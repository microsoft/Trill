// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal class PartitionedStatefulSelectPipe<TKey, TSource, TState, TResult, TPartitionKey> : UnaryPipe<TKey, TSource, TResult>
    {
        private readonly Func<TKey, TPartitionKey> getPartitionKey = GetPartitionExtractor<TPartitionKey, TKey>();
        private readonly string errorMessages;

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Used to avoid creating redundant readonly property.")]
        private readonly MemoryPool<TKey, TResult> pool;

        [SchemaSerialization]
        private Expression<Func<TState>> initialState;
        private readonly Func<TState> initialStateFunc;

        [SchemaSerialization]
        private Expression<Func<long, TState, TSource, TResult>> statefulSelect;
        private readonly Func<long, TState, TSource, TResult> statefulSelectFunc;

        [SchemaSerialization]
        private long stateTimeout;

        /// <summary>
        /// Dictionary of time-ordered colletions of states by substream (partition) key
        /// </summary>
        [DataMember]
        private Dictionary<TPartitionKey, LinkedList<StateWrapper<TKey, TState>>> statesByTimePerPartition;

        [DataMember]
        private long lowWatermark = 0;

        /// <summary>
        /// Dictionary of states by the key. This dictionary is not serialized and will be recomputed after deserialization from statesByTime.
        /// </summary>
        private readonly Dictionary<TKey, LinkedListNode<StateWrapper<TKey, TState>>> statesByKey = new Dictionary<TKey, LinkedListNode<StateWrapper<TKey, TState>>>();

        [Obsolete("Used only by serialization. Do not call directly.")]
        public PartitionedStatefulSelectPipe() { }

        /// <summary>
        /// This operator maintains a state object per group key,
        /// which is updated for every event according to the provided update expressions,
        /// and returns a result for every event according to the provided selector expression.
        /// </summary>
        /// <param name="stream">Parent streamable</param>
        /// <param name="observer">Stream observer</param>
        /// <param name="initialState">An expession to create an initial state.</param>
        /// <param name="statefulSelector">An expression to update the state and project out the result given the state and the current event.</param>
        /// <param name="stateTimeout">The duration in ticks for how long the state will be kept
        /// if there are no new events seen in the corresponding group.</param>
        public PartitionedStatefulSelectPipe(
            Streamable<TKey, TResult> stream,
            IStreamObserver<TKey, TResult> observer,
            Expression<Func<TState>> initialState,
            Expression<Func<long, TState, TSource, TResult>> statefulSelector,
            long stateTimeout)
            : base(stream, observer)
        {
            this.pool = MemoryManager.GetMemoryPool<TKey, TResult>(false);
            this.getPartitionKey = GetPartitionExtractor<TPartitionKey, TKey>();
            this.statesByTimePerPartition = new Dictionary<TPartitionKey, LinkedList<StateWrapper<TKey, TState>>>();
            this.stateTimeout = stateTimeout;
            this.errorMessages = stream.ErrorMessages;

            this.initialState = initialState;
            this.initialStateFunc = initialState.Compile();

            this.statefulSelect = statefulSelector;
            this.statefulSelectFunc = statefulSelector.Compile();
        }

        public override int CurrentlyBufferedOutputCount => 0;
        public override int CurrentlyBufferedInputCount => 0;

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(
                new StatefulSelectPlanNode(
                    previous,
                    pipe: this,
                    keyType: typeof(TKey),
                    inputType: typeof(TSource),
                    payloadType: typeof(TResult),
                    selector: this.statefulSelect,
                    isGenerated: false,
                    errorMessages: this.errorMessages));

        public override unsafe void OnNext(StreamMessage<TKey, TSource> batch)
        {
            this.pool.Get(out StreamMessage<TKey, TResult> outputBatch);

            outputBatch.vsync = batch.vsync;
            outputBatch.vother = batch.vother;
            outputBatch.key = batch.key;
            outputBatch.hash = batch.hash;
            outputBatch.iter = batch.iter;
            this.pool.GetPayload(out outputBatch.payload);
            outputBatch.bitvector = batch.bitvector;

            // Remember max vsync per partition and max low watermark to cleanup once per batch
            var maxVsyncPerPartitionKey = new Dictionary<TPartitionKey, long>();
            var newLowWatermark = false;

            var count = batch.Count;
            var sourceKey = batch.key.col;
            fixed (long* bv = batch.bitvector.col,
                vsync = batch.vsync.col,
                vother = batch.vother.col)
            {
                for (var i = 0; i < count; i++)
                {
                    TPartitionKey partition;
                    if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                    {
                        if (vother[i] == PartitionedStreamEvent.LowWatermarkOtherTime)
                        {
                            this.lowWatermark = Math.Max(this.lowWatermark, batch.vsync.col[i]);
                            newLowWatermark = true;
                        }
                        else if (vother[i] == PartitionedStreamEvent.PunctuationOtherTime)
                        {
                            partition = this.getPartitionKey(batch.key.col[i]);
                            maxVsyncPerPartitionKey[partition] = vsync[i];
                        }

                        continue;
                    }

                    partition = this.getPartitionKey(batch.key.col[i]);
                    maxVsyncPerPartitionKey[partition] = vsync[i];

                    // Retrieve or create the state node
                    LinkedList<StateWrapper<TKey, TState>> statesByTime;
                    if (this.statesByKey.TryGetValue(sourceKey[i], out var stateNode))
                    {
                        statesByTime = this.statesByTimePerPartition[partition];
                        statesByTime.Remove(stateNode); // Node will be reinserted at the end

                        if (stateNode.Value.LastVsync < vsync[i] - this.stateTimeout)
                        {
                            stateNode.Value.State = this.initialStateFunc(); // Reset expired state
                        }
                    }
                    else
                    {
                        statesByTime = new LinkedList<StateWrapper<TKey, TState>>();
                        this.statesByTimePerPartition[partition] = statesByTime;

                        stateNode = new LinkedListNode<StateWrapper<TKey, TState>>(
                            new StateWrapper<TKey, TState> { State = this.initialStateFunc(), Key = sourceKey[i] });
                        this.statesByKey.Add(sourceKey[i], stateNode);
                    }

                    // Update timestamp and add to the end of the time index
                    stateNode.Value.LastVsync = vsync[i];
                    statesByTime.AddLast(stateNode);

                    outputBatch[i] = this.statefulSelectFunc(vsync[i], stateNode.Value.State, batch.payload.col[i]);
                }
            }

            if (newLowWatermark)
            {
                RemoveExpiredStatesOnNewLowWatermark(maxVsyncPerPartitionKey);
            }
            else
            {
                // Cheaper to only update partitions in this batch
                RemoveExpiredStates(maxVsyncPerPartitionKey);
            }

            outputBatch.Count = count;
            batch.payload.Return();
            batch.Return();
            this.Observer.OnNext(outputBatch);
        }

        protected override void UpdatePointers()
        {
            foreach (var statesByTime in this.statesByTimePerPartition.Values)
            {
                var node = statesByTime.First;
                while (node != null)
                {
                    this.statesByKey.Add(node.Value.Key, node);
                    node = node.Next;
                }
            }
        }

        private void RemoveExpiredStatesOnNewLowWatermark(Dictionary<TPartitionKey, long> maxVsyncPerPartition)
        {
            var emptyPartitionKeys = new List<TPartitionKey>();
            foreach (var partitionEntry in this.statesByTimePerPartition)
            {
                var partition = partitionEntry.Key;
                var statesByTime = partitionEntry.Value;
                var maxVsync = maxVsyncPerPartition.TryGetValue(partition, out var maxPartitionVsync)
                    ? Math.Max(maxPartitionVsync, this.lowWatermark)
                    : this.lowWatermark;

                // Delete all expired states from the partition
                while (statesByTime.First != null &&
                    (statesByTime.First.Value.LastVsync < maxVsync - this.stateTimeout))
                {
                    this.statesByKey.Remove(statesByTime.First.Value.Key);
                    statesByTime.RemoveFirst();
                }

                // If there are no keys left delete partition entry
                if (statesByTime.Count == 0)
                {
                    emptyPartitionKeys.Add(partition);
                }
            }

            foreach (var pkey in emptyPartitionKeys)
            {
                this.statesByTimePerPartition.Remove(pkey);
            }
        }

        private void RemoveExpiredStates(Dictionary<TPartitionKey, long> maxVsyncPerPartition)
        {
            foreach (var maxVSyncEntry in maxVsyncPerPartition)
            {
                if (this.statesByTimePerPartition.TryGetValue(maxVSyncEntry.Key, out var statesByTime))
                {
                    // Delete all expired states from the partition
                    while (statesByTime.First != null &&
                        (statesByTime.First.Value.LastVsync < maxVSyncEntry.Value - this.stateTimeout))
                    {
                        this.statesByKey.Remove(statesByTime.First.Value.Key);
                        statesByTime.RemoveFirst();
                    }

                    // If there are no keys left delete partition entry
                    if (statesByTime.First == null)
                    {
                        this.statesByTimePerPartition.Remove(maxVSyncEntry.Key);
                    }
                }
            }
        }
    }
}