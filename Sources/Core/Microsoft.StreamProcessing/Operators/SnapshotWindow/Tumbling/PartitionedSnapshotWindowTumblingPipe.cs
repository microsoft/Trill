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
    internal sealed class PartitionedSnapshotWindowTumblingPipe<TKey, TInput, TState, TOutput, TPartitionKey> : UnaryPipe<TKey, TInput, TOutput>
    {
        private static readonly bool hasDisposableState = typeof(IDisposable).GetTypeInfo().IsAssignableFrom(typeof(TState));
        private readonly MemoryPool<TKey, TOutput> pool;
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
        [SchemaSerialization]
        private readonly Expression<Func<TKey, TKey, bool>> keyComparerEqualsExpr;
        private readonly Func<TKey, TKey, bool> keyComparerEquals;
        [SchemaSerialization]
        private readonly Expression<Func<TKey, int>> keyComparerGetHashCodeExpr;
        private readonly Func<TKey, int> keyComparerGetHashCode;

        [DataMember]
        private StreamMessage<TKey, TOutput> batch;

        [DataMember]
        private FastDictionary2<TPartitionKey, PartitionEntry> partitionData = new FastDictionary2<TPartitionKey, PartitionEntry>();

        private readonly Func<TKey, TPartitionKey> getPartitionKey = GetPartitionExtractor<TPartitionKey, TKey>();
        private readonly Func<FastDictionary<TKey, TState>> dictionaryGenerator;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public PartitionedSnapshotWindowTumblingPipe() { }

        public PartitionedSnapshotWindowTumblingPipe(
            SnapshotWindowStreamable<TKey, TInput, TState, TOutput> stream,
            IStreamObserver<TKey, TOutput> observer, long hop)
            : base(stream, observer)
        {
            this.aggregate = stream.Aggregate;
            this.initialStateExpr = this.aggregate.InitialState();
            this.initialState = this.initialStateExpr.Compile();
            this.accumulateExpr = this.aggregate.Accumulate();
            this.accumulate = this.accumulateExpr.Compile();
            this.computeResultExpr = this.aggregate.ComputeResult();
            this.computeResult = this.computeResultExpr.Compile();

            var comparer = stream.Properties.KeyEqualityComparer;
            this.keyComparerEqualsExpr = comparer.GetEqualsExpr();
            this.keyComparerEquals = this.keyComparerEqualsExpr.Compile();
            this.keyComparerGetHashCodeExpr = comparer.GetGetHashCodeExpr();
            this.keyComparerGetHashCode = this.keyComparerGetHashCodeExpr.Compile();

            this.errorMessages = stream.ErrorMessages;
            this.pool = MemoryManager.GetMemoryPool<TKey, TOutput>(false);
            this.pool.Get(out this.batch);
            this.batch.Allocate();
            this.dictionaryGenerator = comparer.CreateFastDictionaryGenerator<TKey, TState>(1, this.keyComparerEquals, this.keyComparerGetHashCode, stream.Properties.QueryContainer);

            this.hop = hop;
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new SnapshotWindowPlanNode<TInput, TState, TOutput>(
                previous, this, typeof(TKey), typeof(TInput), typeof(TOutput),
                AggregatePipeType.StartEdge, this.aggregate, false, this.errorMessages));

        public override unsafe void OnNext(StreamMessage<TKey, TInput> batch)
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
                            this.batch.key.col[c] = default;
                            this.batch.hash.col[c] = 0;
                            this.batch.bitvector.col[c >> 6] |= 1L << (c & 0x3f);
                            this.batch.Count++;
                            if (this.batch.Count == Config.DataBatchSize) FlushContents();
                        }
                        else if (col_vother[i] == PartitionedStreamEvent.PunctuationOtherTime)
                        {
                            // We have found a row that corresponds to punctuation
                            var partitionKey = this.getPartitionKey(colkey[i]);
                            bool emitPunctuation = false;
                            PartitionEntry partitionEntry;
                            if (!this.partitionData.Lookup(partitionKey, out int partitionKeyIndex))
                            {
                                this.partitionData.Insert(partitionKey, partitionEntry = new PartitionEntry { lastSyncTime = col_vsync[i], heldAggregates = this.dictionaryGenerator() });
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
                    var partition = this.getPartitionKey(colkey[i]);
                    PartitionEntry entry;

                    // Handle time moving forward
                    if (!this.partitionData.Lookup(partition, out int partitionIndex))
                        this.partitionData.Insert(partition, entry = new PartitionEntry { lastSyncTime = syncTime, heldAggregates = this.dictionaryGenerator() });
                    else if (syncTime > (entry = this.partitionData.entries[partitionIndex].value).lastSyncTime)
                    {
                        int iter1 = FastDictionary<TKey, TState>.IteratorStart;
                        while (entry.heldAggregates.Iterate(ref iter1))
                        {
                            var iter1entry = entry.heldAggregates.entries[iter1];

                            int c = this.batch.Count;
                            this.batch.vsync.col[c] = entry.lastSyncTime;
                            this.batch.vother.col[c] = entry.lastSyncTime + this.hop;
                            this.batch.payload.col[c] = this.computeResult(iter1entry.value);
                            this.batch.key.col[c] = iter1entry.key;
                            this.batch.hash.col[c] = this.keyComparerGetHashCode(iter1entry.key);
                            this.batch.Count++;
                            if (this.batch.Count == Config.DataBatchSize) FlushContents();

                            if (hasDisposableState)
                            {
                                ((IDisposable)iter1entry.value).Dispose();
                            }
                        }
                        entry.heldAggregates.Clear();

                        // Since sync time changed, set lastSyncTime
                        entry.lastSyncTime = syncTime;
                    }

                    // Need to retrieve the key from the dictionary
                    if (!entry.heldAggregates.Lookup(colkey[i], col_hash[i], out int index))
                        entry.heldAggregates.Insert(ref index, colkey[i], this.initialState());

                    var entries = entry.heldAggregates.entries;
                    entries[index].value = this.accumulate(entries[index].value, col_vsync[i], colpayload[i]);
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
                int iter1 = FastDictionary<TKey, TState>.IteratorStart;
                while (partition.heldAggregates.Iterate(ref iter1))
                {
                    var iter1entry = partition.heldAggregates.entries[iter1];

                    int c = this.batch.Count;
                    this.batch.vsync.col[c] = partitionSyncTime;
                    this.batch.vother.col[c] = partitionSyncTime + this.hop;
                    this.batch.payload.col[c] = this.computeResult(iter1entry.value);
                    this.batch.key.col[c] = iter1entry.key;
                    this.batch.hash.col[c] = this.keyComparerGetHashCode(iter1entry.key);
                    this.batch.Count++;
                    if (this.batch.Count == Config.DataBatchSize) FlushContents();

                    if (hasDisposableState)
                    {
                        ((IDisposable)iter1entry.value).Dispose();
                    }
                }

                // Time has moved forward, clear the held aggregates
                partition.heldAggregates.Clear();

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

        public override int CurrentlyBufferedInputCount
        {
            get
            {
                int sum = 0;
                int index1 = FastDictionary<TPartitionKey, PartitionEntry>.IteratorStart;
                while (this.partitionData.Iterate(ref index1))
                    sum += this.partitionData.entries[index1].value.heldAggregates.Count;
                return sum;
            }
        }

        protected override void DisposeState()
        {
            this.batch.Free();
            if (hasDisposableState)
            {
                int index1 = FastDictionary<TPartitionKey, PartitionEntry>.IteratorStart;
                while (this.partitionData.Iterate(ref index1))
                {
                    int index2 = FastDictionary<TKey, TState>.IteratorStart;
                    var partition = this.partitionData.entries[index1].value;
                    while (partition.heldAggregates.Iterate(ref index2))
                        (partition.heldAggregates.entries[index2].value as IDisposable).Dispose();
                }
            }
        }

        [DataContract]
        private sealed class PartitionEntry
        {
            [DataMember]
            public long lastSyncTime;
            [DataMember]
            public FastDictionary<TKey, TState> heldAggregates;
        }
    }
}
