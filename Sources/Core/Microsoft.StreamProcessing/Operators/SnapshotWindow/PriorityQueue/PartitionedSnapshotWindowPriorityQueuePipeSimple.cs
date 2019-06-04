// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Aggregates;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Operator that uses a full-fledged priority queue as ECQ
    /// </summary>
    [DataContract]
    internal sealed class PartitionedSnapshotWindowPriorityQueuePipeSimple<TInput, TState, TOutput, TPartitionKey> : UnaryPipe<PartitionKey<TPartitionKey>, TInput, TOutput>
    {
        private static readonly bool hasDisposableState = typeof(IDisposable).GetTypeInfo().IsAssignableFrom(typeof(TState));
        private readonly MemoryPool<PartitionKey<TPartitionKey>, TOutput> pool;
        private readonly string errorMessages;

        private readonly IAggregate<TInput, TState, TOutput> aggregate;
        [SchemaSerialization]
        private readonly Expression<Func<TState>> initialStateExpr;
        private readonly Func<TState> initialState;
        [SchemaSerialization]
        private readonly Expression<Func<TState, long, TInput, TState>> accumulateExpr;
        private readonly Func<TState, long, TInput, TState> accumulate;
        [SchemaSerialization]
        private readonly Expression<Func<TState, long, TInput, TState>> deaccumulateExpr;
        private readonly Func<TState, long, TInput, TState> deaccumulate;
        [SchemaSerialization]
        private readonly Expression<Func<TState, TState, TState>> differenceExpr;
        private readonly Func<TState, TState, TState> difference;
        [SchemaSerialization]
        private readonly Expression<Func<TState, TOutput>> computeResultExpr;
        private readonly Func<TState, TOutput> computeResult;

        [DataMember]
        private StreamMessage<PartitionKey<TPartitionKey>, TOutput> batch;

        [DataMember]
        private FastDictionary2<TPartitionKey, PartitionEntry> partitionData = new FastDictionary2<TPartitionKey, PartitionEntry>();

        [Obsolete("Used only by serialization. Do not call directly.")]
        public PartitionedSnapshotWindowPriorityQueuePipeSimple() { }

        public PartitionedSnapshotWindowPriorityQueuePipeSimple(
            SnapshotWindowStreamable<PartitionKey<TPartitionKey>, TInput, TState, TOutput> stream,
            IStreamObserver<PartitionKey<TPartitionKey>, TOutput> observer)
            : base(stream, observer)
        {
            this.aggregate = stream.Aggregate;
            this.initialStateExpr = this.aggregate.InitialState();
            this.initialState = this.initialStateExpr.Compile();
            this.accumulateExpr = this.aggregate.Accumulate();
            this.accumulate = this.accumulateExpr.Compile();
            this.deaccumulateExpr = this.aggregate.Deaccumulate();
            this.deaccumulate = this.deaccumulateExpr.Compile();
            this.differenceExpr = this.aggregate.Difference();
            this.difference = this.differenceExpr.Compile();
            this.computeResultExpr = this.aggregate.ComputeResult();
            this.computeResult = this.computeResultExpr.Compile();

            this.errorMessages = stream.ErrorMessages;
            this.pool = MemoryManager.GetMemoryPool<PartitionKey<TPartitionKey>, TOutput>(false);
            this.pool.Get(out this.batch);
            this.batch.Allocate();
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new SnapshotWindowPlanNode<TInput, TState, TOutput>(
                previous, this, typeof(PartitionKey<TPartitionKey>), typeof(TInput), typeof(TOutput),
                AggregatePipeType.PriorityQueue, this.aggregate, false, this.errorMessages));

        public override unsafe void OnNext(StreamMessage<PartitionKey<TPartitionKey>, TInput> batch)
        {
            this.batch.iter = batch.iter;

            var count = batch.Count;
            var colkey = batch.key.col;
            var colpayload = batch.payload.col;

            fixed (long* col_vsync = batch.vsync.col)
            fixed (long* col_vother = batch.vother.col)
            fixed (int* col_hash = batch.hash.col)
            fixed (long* col_bv = batch.bitvector.col)
            {
                for (int i = 0; i < count; i++)
                {
                    if ((col_bv[i >> 6] & (1L << (i & 0x3f))) != 0)
                    {
                        if (col_vother[i] == PartitionedStreamEvent.LowWatermarkOtherTime)
                        {
                            OnLowWatermark(col_vsync[i]);

                            int c = this.batch.Count;
                            this.batch.vsync.col[c] = col_vsync[i];
                            this.batch.vother.col[c] = PartitionedStreamEvent.LowWatermarkOtherTime;
                            this.batch.key.col[c] = new PartitionKey<TPartitionKey>(default);
                            this.batch.hash.col[c] = 0;
                            this.batch.bitvector.col[c >> 6] |= 1L << (c & 0x3f);
                            this.batch.Count++;
                            if (this.batch.Count == Config.DataBatchSize) FlushContents();
                        }
                        else if (col_vother[i] == PartitionedStreamEvent.PunctuationOtherTime)
                        {
                            // We have found a row that corresponds to punctuation
                            var p = colkey[i].Key;
                            PartitionEntry partitionEntry;
                            if (!this.partitionData.Lookup(p, out int partitionIndex))
                                this.partitionData.Insert(p, (partitionEntry = new PartitionEntry { lastSyncTime = col_vsync[i], currentKey = colkey[i], currentHash = col_hash[i] }));
                            else partitionEntry = this.partitionData.entries[partitionIndex].value;
                            OnPunctuation(partitionEntry, col_vsync[i]);

                            int c = this.batch.Count;
                            this.batch.vsync.col[c] = col_vsync[i];
                            this.batch.vother.col[c] = long.MinValue;
                            this.batch.key.col[c] = colkey[i];
                            this.batch.hash.col[c] = partitionEntry.currentHash;
                            this.batch.bitvector.col[c >> 6] |= 1L << (c & 0x3f);
                            this.batch.Count++;
                            if (this.batch.Count == Config.DataBatchSize) FlushContents();
                        }
                        continue;
                    }

                    var syncTime = col_vsync[i];
                    var partition = colkey[i].Key;
                    PartitionEntry entry;

                    // Handle time moving forward
                    if (!this.partitionData.Lookup(partition, out int pIndex))
                        this.partitionData.Insert(partition, (entry = new PartitionEntry { lastSyncTime = syncTime, currentKey = colkey[i], currentHash = col_hash[i] }));
                    else if (syncTime > (entry = this.partitionData.entries[pIndex].value).lastSyncTime) AdvanceTime(entry, syncTime);

                    if (entry.currentState == null)
                    {
                        entry.currentState = new HeldState<TState> { state = this.initialState(), timestamp = syncTime };
                        entry.held = true;

                        // No output because initial state is empty
                    }
                    else
                    {
                        if (syncTime > entry.currentState.timestamp)
                        {
                            if (entry.currentState.active > 0)
                            {
                                // Output end edge
                                int c = this.batch.Count;
                                this.batch.vsync.col[c] = syncTime;
                                this.batch.vother.col[c] = entry.currentState.timestamp;
                                this.batch.payload.col[c] = this.computeResult(entry.currentState.state);
                                this.batch.key.col[c] = entry.currentKey;
                                this.batch.hash.col[c] = entry.currentHash;
                                this.batch.Count++;
                                if (this.batch.Count == Config.DataBatchSize)
                                {
                                    this.batch.iter = batch.iter;
                                    FlushContents();
                                    this.batch.iter = batch.iter;
                                }
                            }
                            entry.currentState.timestamp = syncTime;
                            entry.held = true;
                        }
                    }

                    if (col_vsync[i] < col_vother[i]) // insert event
                    {
                        entry.currentState.state = this.accumulate(entry.currentState.state, col_vsync[i], colpayload[i]);
                        entry.currentState.active++;

                        // Update ECQ
                        if (col_vother[i] < StreamEvent.InfinitySyncTime)
                        {
                            StateAndActive<TState> stateAndActive;

                            if (entry.ecq.Count > 0)
                            {
                                if (!entry.ecq.TryGetValue(col_vother[i], out stateAndActive))
                                {
                                    stateAndActive = new StateAndActive<TState> { state = this.initialState() };
                                    entry.ecq.Add(col_vother[i], stateAndActive);
                                }
                            }
                            else
                            {
                                stateAndActive = new StateAndActive<TState> { state = this.initialState() };
                                entry.ecq.Add(col_vother[i], stateAndActive);
                            }

                            stateAndActive.state = this.accumulate(stateAndActive.state, col_vsync[i], colpayload[i]);
                            stateAndActive.active++;
                        }
                    }
                    else // is a retraction
                    {
                        entry.currentState.state = this.deaccumulate(entry.currentState.state, col_vsync[i], colpayload[i]);
                        entry.currentState.active--;
                    }
                }
            }

            batch.Release();
            batch.Return();
        }

        public void OnLowWatermark(long syncTime)
        {
            var deprecated = new List<TPartitionKey>();

            // Handle time moving forward
            int iter = FastDictionary<TPartitionKey, PartitionEntry>.IteratorStart;
            while (this.partitionData.Iterate(ref iter))
            {
                var partition = this.partitionData.entries[iter].value;
                if (syncTime > partition.lastSyncTime)
                {
                    OnPunctuation(partition, syncTime);
                    bool partitionHasValidOutstandingState = false;
                    if (partition.ecq != null)
                    {
                        foreach (var heldState in partition.ecq)
                        {
                            if (heldState.Value.active > 0)
                            {
                                partitionHasValidOutstandingState = true;
                                break;
                            }
                        }
                    }
                    if (!partitionHasValidOutstandingState)
                        deprecated.Add(this.partitionData.entries[iter].key);
                }
            }
            foreach (var d in deprecated) this.partitionData.Remove(d);
        }

        private void OnPunctuation(PartitionEntry partition, long syncTime)
        {
            if (syncTime > partition.lastSyncTime) AdvanceTime(partition, syncTime);
        }

        private void AdvanceTime(PartitionEntry partition, long syncTime)
        {
            /* Issue start edges for held aggregates */
            if (partition.currentState != null && partition.held)
            {
                if (partition.currentState.active > 0)
                {
                    int c = this.batch.Count;
                    this.batch.vsync.col[c] = partition.currentState.timestamp;
                    this.batch.vother.col[c] = StreamEvent.InfinitySyncTime;
                    this.batch.payload.col[c] = this.computeResult(partition.currentState.state);
                    this.batch.key.col[c] = partition.currentKey;
                    this.batch.hash.col[c] = partition.currentHash;
                    this.batch.Count++;
                    if (this.batch.Count == Config.DataBatchSize) FlushContents();
                }
                partition.held = false;
            }

            /* Process the ECQ up until the new sync time */
            while (partition.ecq.Count > 0 && partition.ecq.TryGetFirst(out long ve, out StateAndActive<TState> ecqState) && ve <= syncTime)
            {
                partition.ecq.Remove(ve);
                if (partition.currentState.active > 0)
                {
                    // Issue end edge
                    int c = this.batch.Count;
                    this.batch.vsync.col[c] = ve;
                    this.batch.vother.col[c] = partition.currentState.timestamp;
                    this.batch.payload.col[c] = this.computeResult(partition.currentState.state);
                    this.batch.key.col[c] = partition.currentKey;
                    this.batch.hash.col[c] = partition.currentHash;
                    this.batch.Count++;
                    if (this.batch.Count == Config.DataBatchSize) FlushContents();
                }

                // Update aggregate
                partition.currentState.state = this.difference(partition.currentState.state, ecqState.state);
                partition.currentState.active -= ecqState.active;
                (ecqState.state as IDisposable)?.Dispose();

                if (ve < syncTime)
                {
                    if (partition.currentState.active > 0)
                    {
                        // Issue start edge
                        int c = this.batch.Count;
                        this.batch.vsync.col[c] = ve;
                        this.batch.vother.col[c] = StreamEvent.InfinitySyncTime;
                        this.batch.payload.col[c] = this.computeResult(partition.currentState.state);
                        this.batch.key.col[c] = partition.currentKey;
                        this.batch.hash.col[c] = partition.currentHash;
                        this.batch.Count++;
                        if (this.batch.Count == Config.DataBatchSize) FlushContents();
                    }
                    else
                    {
                        (partition.currentState.state as IDisposable)?.Dispose();
                        partition.currentState = null;
                    }
                }
                else partition.held = true;

                // Update timestamp
                if (partition.currentState != null) partition.currentState.timestamp = ve;
            }

            // Since sync time changed, set lastSyncTime
            partition.lastSyncTime = syncTime;
        }

        protected override void FlushContents()
        {
            if (this.batch == null || this.batch.Count == 0) return;
            this.Observer.OnNext(this.batch);
            this.pool.Get(out this.batch);
            this.batch.Allocate();
        }

        public override int CurrentlyBufferedOutputCount => this.batch.Count;

        public override int CurrentlyBufferedInputCount
        {
            get
            {
                var iter = FastDictionary2<TPartitionKey, PartitionEntry>.IteratorStart;
                var count = 0;
                while (this.partitionData.Iterate(ref iter))
                    count += this.partitionData.entries[iter].value.ecq.Count;
                return count;
            }
        }

        protected override void DisposeState()
        {
            this.batch.Free();
            if (hasDisposableState)
            {
                int index = FastDictionary2<TPartitionKey, PartitionEntry>.IteratorStart;
                while (this.partitionData.Iterate(ref index))
                {
                    (this.partitionData.entries[index].value.currentState.state as IDisposable).Dispose();
                    foreach (var pair in this.partitionData.entries[index].value.ecq)
                        (pair.Value.state as IDisposable).Dispose();
                    this.partitionData.entries[index].value.ecq.Clear();
                }
            }
        }

        [DataContract]
        private sealed class PartitionEntry
        {
            [DataMember]
            public SortedDictionary<long, StateAndActive<TState>> ecq = new SortedDictionary<long, StateAndActive<TState>>();
            [DataMember]
            public long lastSyncTime = long.MinValue;

            [DataMember]
            public HeldState<TState> currentState = null;
            [DataMember]
            public PartitionKey<TPartitionKey> currentKey;
            [DataMember]
            public int currentHash;
            [DataMember]
            public bool held;
        }
    }
}
