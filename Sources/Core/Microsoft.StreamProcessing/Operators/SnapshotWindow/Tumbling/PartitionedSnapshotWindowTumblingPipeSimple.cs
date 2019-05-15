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
    /// Operator for tumbling windows, has no support for ECQ
    /// </summary>
    [DataContract]
    internal sealed class PartitionedSnapshotWindowTumblingPipeSimple<TInput, TState, TOutput, TPartitionKey> : UnaryPipe<PartitionKey<TPartitionKey>, TInput, TOutput>
    {
        private static readonly bool hasDisposableState = typeof(IDisposable).GetTypeInfo().IsAssignableFrom(typeof(TState));
        private readonly MemoryPool<PartitionKey<TPartitionKey>, TOutput> pool;
        private readonly string errorMessages;
        private readonly IAggregate<TInput, TState, TOutput> aggregate;

        [SchemaSerialization]
        private readonly long hop;
        [SchemaSerialization]
        private readonly Expression<Func<TState>> initialStateExpr;
        private readonly Func<TState> initialState;
        [SchemaSerialization]
        private readonly Expression<Func<TState, long, TInput, TState>> accumulateExpr;
        private readonly Func<TState, long, TInput, TState> accumulate;
        [SchemaSerialization]
        private readonly Expression<Func<TState, TOutput>> computeResultExpr;
        private readonly Func<TState, TOutput> computeResult;

        [DataMember]
        private StreamMessage<PartitionKey<TPartitionKey>, TOutput> batch;

        [DataMember]
        private FastDictionary2<TPartitionKey, PartitionEntry> partitionData = new FastDictionary2<TPartitionKey, PartitionEntry>();

        [Obsolete("Used only by serialization. Do not call directly.")]
        public PartitionedSnapshotWindowTumblingPipeSimple() { }

        public PartitionedSnapshotWindowTumblingPipeSimple(
            SnapshotWindowStreamable<PartitionKey<TPartitionKey>, TInput, TState, TOutput> stream,
            IStreamObserver<PartitionKey<TPartitionKey>, TOutput> observer, long hop)
            : base(stream, observer)
        {
            this.aggregate = stream.Aggregate;
            this.initialStateExpr = this.aggregate.InitialState();
            this.initialState = this.initialStateExpr.Compile();
            this.accumulateExpr = this.aggregate.Accumulate();
            this.accumulate = this.accumulateExpr.Compile();
            this.computeResultExpr = this.aggregate.ComputeResult();
            this.computeResult = this.computeResultExpr.Compile();

            this.errorMessages = stream.ErrorMessages;
            this.pool = MemoryManager.GetMemoryPool<PartitionKey<TPartitionKey>, TOutput>(false);
            this.pool.Get(out this.batch);
            this.batch.Allocate();

            this.hop = hop;
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new SnapshotWindowPlanNode<TInput, TState, TOutput>(
                previous, this, typeof(PartitionKey<TPartitionKey>), typeof(TInput), typeof(TOutput),
                AggregatePipeType.StartEdge, this.aggregate, false, this.errorMessages));

        public override unsafe void OnNext(StreamMessage<PartitionKey<TPartitionKey>, TInput> batch)
        {
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
                            var partitionKey = colkey[i].Key;
                            PartitionEntry partitionEntry;
                            bool emitPunctuation;
                            if (!this.partitionData.Lookup(partitionKey, out int partitionKeyIndex))
                            {
                                this.partitionData.Insert(partitionKey, partitionEntry = new PartitionEntry { lastSyncTime = col_vsync[i], key = colkey[i], hash = col_hash[i] });
                                emitPunctuation = true;
                            }
                            else
                            {
                                partitionEntry = this.partitionData.entries[partitionKeyIndex].value;
                                emitPunctuation = partitionEntry.lastSyncTime < col_vsync[i];
                            }
                            OnPunctuation(partitionEntry, col_vsync[i]);

                            if (emitPunctuation)
                            {
                                int c = this.batch.Count;
                                this.batch.vsync.col[c] = col_vsync[i];
                                this.batch.vother.col[c] = long.MinValue;
                                this.batch.key.col[c] = colkey[i];
                                this.batch.hash.col[c] = col_hash[i];
                                this.batch.bitvector.col[c >> 6] |= 1L << (c & 0x3f);
                                this.batch.Count++;
                                if (this.batch.Count == Config.DataBatchSize) FlushContents();
                            }
                        }
                        continue;
                    }

                    var syncTime = col_vsync[i];
                    var partition = colkey[i].Key;
                    PartitionEntry entry;

                    // Handle time moving forward
                    if (!this.partitionData.Lookup(partition, out int partitionIndex))
                        this.partitionData.Insert(partition, entry = new PartitionEntry { lastSyncTime = syncTime, key = colkey[i], hash = col_hash[i] });
                    else if (syncTime > (entry = this.partitionData.entries[partitionIndex].value).lastSyncTime)
                    {
                        if (entry.currentState != null)
                        {
                            var cstate = entry.currentState;
                            int c = this.batch.Count;
                            this.batch.vsync.col[c] = cstate.timestamp;
                            this.batch.vother.col[c] = cstate.timestamp + this.hop;
                            this.batch.payload.col[c] = this.computeResult(cstate.state);
                            this.batch.key.col[c] = colkey[i];
                            this.batch.hash.col[c] = col_hash[i];
                            this.batch.Count++;
                            if (this.batch.Count == Config.DataBatchSize) FlushContents();

                            if (hasDisposableState)
                            {
                                ((IDisposable)entry.currentState.state).Dispose();
                            }

                            entry.currentState = null;
                        }

                        // Since sync time changed, set lastSyncTime
                        entry.lastSyncTime = syncTime;
                    }

                    if (entry.currentState == null)
                    {
                        // First time group is active for this time
                        entry.currentState = new HeldState<TState> { state = this.initialState(), timestamp = syncTime };
                    }
                    else
                    {
                        if (syncTime > entry.currentState.timestamp)
                        {
                            // Reset currentState
                            var stateEntry = entry.currentState;
                            if (hasDisposableState)
                            {
                                ((IDisposable)stateEntry.state).Dispose();
                            }

                            stateEntry.state = this.initialState();
                            stateEntry.timestamp = syncTime;
                        }
                    }

                    var currentState = entry.currentState;
                    currentState.state = this.accumulate(currentState.state, col_vsync[i], colpayload[i]);
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
                    deprecated.Add(this.partitionData.entries[iter].key);
                }
            }
            foreach (var d in deprecated) this.partitionData.Remove(d);
        }

        private void OnPunctuation(PartitionEntry partition, long syncTime)
        {
            long partitionSyncTime = partition.lastSyncTime;
            if (syncTime > partitionSyncTime)
            {
                if (partition.currentState != null)
                {
                    int c = this.batch.Count;
                    this.batch.vsync.col[c] = partition.currentState.timestamp;
                    this.batch.vother.col[c] = partition.currentState.timestamp + this.hop;
                    this.batch.payload.col[c] = this.computeResult(partition.currentState.state);
                    this.batch.key.col[c] = partition.key;
                    this.batch.hash.col[c] = partition.hash;
                    this.batch.Count++;
                    if (this.batch.Count == Config.DataBatchSize) FlushContents();

                    if (hasDisposableState)
                    {
                        ((IDisposable)partition.currentState.state).Dispose();
                    }

                    partition.currentState = null;
                }

                // Since sync time changed, set lastSyncTime
                partition.lastSyncTime = syncTime;
            }
        }

        protected override void FlushContents()
        {
            if (this.batch == null || this.batch.Count == 0) return;
            this.batch.Seal();
            this.Observer.OnNext(this.batch);
            this.pool.Get(out this.batch);
            this.batch.Allocate();
        }

        public override int CurrentlyBufferedOutputCount => this.batch.Count;

        public override int CurrentlyBufferedInputCount => this.partitionData.Count;

        protected override void DisposeState()
        {
            this.batch.Free();
            if (hasDisposableState)
            {
                int index = FastDictionary<TPartitionKey, PartitionEntry>.IteratorStart;
                while (this.partitionData.Iterate(ref index))
                {
                    (this.partitionData.entries[index].value.currentState.state as IDisposable).Dispose();
                }
            }
        }

        [DataContract]
        private sealed class PartitionEntry
        {
            [DataMember]
            public long lastSyncTime;
            [DataMember]
            public HeldState<TState> currentState = default;
            [DataMember]
            public PartitionKey<TPartitionKey> key;
            [DataMember]
            public int hash = 0;
        }
    }
}
