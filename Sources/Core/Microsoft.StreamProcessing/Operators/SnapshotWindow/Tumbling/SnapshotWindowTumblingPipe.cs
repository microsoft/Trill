// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
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
    internal sealed class SnapshotWindowTumblingPipe<TKey, TInput, TState, TOutput> : UnaryPipe<TKey, TInput, TOutput>
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
        private FastDictionary<TKey, TState> heldAggregates;
        [DataMember]
        private long lastSyncTime = long.MinValue;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public SnapshotWindowTumblingPipe() { }

        public SnapshotWindowTumblingPipe(
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
            this.heldAggregates = comparer.CreateFastDictionaryGenerator<TKey, TState>(1, this.keyComparerEquals, this.keyComparerGetHashCode, stream.Properties.QueryContainer).Invoke();

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
                        if (col_vother[i] == StreamEvent.PunctuationOtherTime)
                        {
                            // We have found a row that corresponds to punctuation
                            OnPunctuation(col_vsync[i]);

                            int c = this.batch.Count;
                            this.batch.vsync.col[c] = col_vsync[i];
                            this.batch.vother.col[c] = StreamEvent.PunctuationOtherTime;
                            this.batch.key.col[c] = default;
                            this.batch.hash.col[c] = 0;
                            this.batch.bitvector.col[c >> 6] |= 1L << (c & 0x3f);
                            this.batch.Count++;
                            if (this.batch.Count == Config.DataBatchSize) FlushContents();
                        }
                        continue;
                    }
                    var syncTime = col_vsync[i];

                    // Handle time moving forward
                    if (syncTime > this.lastSyncTime)
                    {
                        int iter1 = FastDictionary<TKey, TState>.IteratorStart;
                        while (this.heldAggregates.Iterate(ref iter1))
                        {
                            var iter1entry = this.heldAggregates.entries[iter1];
                            int c = this.batch.Count;
                            this.batch.vsync.col[c] = this.lastSyncTime;
                            this.batch.vother.col[c] = this.lastSyncTime + this.hop;
                            this.batch.payload.col[c] = this.computeResult(iter1entry.value);
                            this.batch.key.col[c] = iter1entry.key;
                            this.batch.hash.col[c] = this.keyComparerGetHashCode(iter1entry.key);
                            this.batch.Count++;
                            if (this.batch.Count == Config.DataBatchSize) FlushContents();
                        }

                        if (hasDisposableState) DisposeStateLocal();
                        this.heldAggregates.Clear();

                        // Since sync time changed, set lastSyncTime
                        this.lastSyncTime = syncTime;
                    }

                    // Need to retrieve the key from the dictionary
                    if (!this.heldAggregates.Lookup(colkey[i], col_hash[i], out int index)) this.heldAggregates.Insert(ref index, colkey[i], this.initialState());

                    ref var entry = ref this.heldAggregates.entries[index];
                    entry.value = this.accumulate(entry.value, col_vsync[i], colpayload[i]);
                }
            }

            batch.Release();
            batch.Return();
        }

        public void OnPunctuation(long syncTime)
        {
            // Handle time moving forward
            if (syncTime > this.lastSyncTime)
            {
                /* Issue start edges for held aggregates */
                int iter1 = FastDictionary<TKey, TState>.IteratorStart;
                while (this.heldAggregates.Iterate(ref iter1))
                {
                    var iter1entry = this.heldAggregates.entries[iter1];
                    int c = this.batch.Count;
                    this.batch.vsync.col[c] = this.lastSyncTime;
                    this.batch.vother.col[c] = this.lastSyncTime + this.hop;
                    this.batch.payload.col[c] = this.computeResult(iter1entry.value);
                    this.batch.key.col[c] = iter1entry.key;
                    this.batch.hash.col[c] = this.keyComparerGetHashCode(iter1entry.key);
                    this.batch.Count++;
                    if (this.batch.Count == Config.DataBatchSize) FlushContents();
                }

                // Time has moved forward, clear the held aggregates
                if (hasDisposableState) DisposeStateLocal();
                this.heldAggregates.Clear();

                // Since sync time changed, set lastSyncTime
                this.lastSyncTime = syncTime;
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

        public override int CurrentlyBufferedInputCount => this.heldAggregates.Count;

        private void DisposeStateLocal()
        {
            int index = FastDictionary2<TKey, TState>.IteratorStart;
            while (this.heldAggregates.Iterate(ref index))
                (this.heldAggregates.entries[index].value as IDisposable).Dispose();
        }

        protected override void DisposeState()
        {
            this.batch.Free();
            if (hasDisposableState) DisposeStateLocal();
        }
    }
}
