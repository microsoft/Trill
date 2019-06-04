// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Aggregates;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Operator has has no support for ECQ
    /// </summary>
    [DataContract]
    internal sealed class GroupedWindowPipe<TKey, TInput, TState, TOutput, TResult> : UnaryPipe<Empty, TInput, TResult>
    {
        private readonly MemoryPool<Empty, TResult> pool;
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
        private readonly Expression<Func<TState, TOutput>> computeResultExpr;
        private readonly Func<TState, TOutput> computeResult;
        [SchemaSerialization]
        private readonly Expression<Func<TKey, TKey, bool>> keyComparerEqualsExpr;
        private readonly Func<TKey, TKey, bool> keyComparerEquals;
        [SchemaSerialization]
        private readonly Expression<Func<TKey, int>> keyComparerGetHashCodeExpr;
        private readonly Func<TKey, int> keyComparerGetHashCode;
        [SchemaSerialization]
        private readonly Expression<Func<TInput, TKey>> keySelectorExpr;
        private readonly Func<TInput, TKey> keySelector;
        [SchemaSerialization]
        private readonly Expression<Func<TKey, TOutput, TResult>> finalResultSelectorExpr;
        private readonly Func<TKey, TOutput, TResult> finalResultSelector;
        [SchemaSerialization]
        private readonly bool isUngrouped;

        [DataMember]
        private StreamMessage<Empty, TResult> batch;

        [DataMember]
        private FastDictionary2<TKey, HeldState<TState>> aggregateByKey;
        [DataMember]
        private FastDictionary<TKey, HeldState<TState>> heldAggregates;
        [DataMember]
        private long lastSyncTime = long.MinValue;

        private HeldState<TState> currentState;
        private TKey currentKey;
        private int currentHash;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public GroupedWindowPipe() { }

        public GroupedWindowPipe(
            GroupedWindowStreamable<TKey, TInput, TState, TOutput, TResult> stream,
            IStreamObserver<Empty, TResult> observer)
            : base(stream, observer)
        {
            this.aggregate = stream.Aggregate;
            this.initialStateExpr = this.aggregate.InitialState();
            this.initialState = this.initialStateExpr.Compile();
            this.accumulateExpr = this.aggregate.Accumulate();
            this.accumulate = this.accumulateExpr.Compile();
            this.deaccumulateExpr = this.aggregate.Deaccumulate();
            this.deaccumulate = this.deaccumulateExpr.Compile();
            this.computeResultExpr = this.aggregate.ComputeResult();
            this.computeResult = this.computeResultExpr.Compile();

            var comparer = EqualityComparerExpression<TKey>.Default;
            this.keyComparerEqualsExpr = comparer.GetEqualsExpr();
            this.keyComparerEquals = EqualityComparerExpression<TKey>.DefaultEqualsFunction;
            this.keyComparerGetHashCodeExpr = comparer.GetGetHashCodeExpr();
            this.keyComparerGetHashCode = EqualityComparerExpression<TKey>.DefaultGetHashCodeFunction;

            this.keySelectorExpr = stream.KeySelector;
            this.keySelector = this.keySelectorExpr.Compile();
            this.finalResultSelectorExpr = stream.ResultSelector;
            this.finalResultSelector = this.finalResultSelectorExpr.Compile();

            this.pool = MemoryManager.GetMemoryPool<Empty, TResult>(false);
            this.pool.Get(out this.batch);
            this.batch.Allocate();

            this.aggregateByKey = comparer.CreateFastDictionary2Generator<TKey, HeldState<TState>>(1, this.keyComparerEquals, this.keyComparerGetHashCode, stream.Properties.QueryContainer).Invoke();
            this.heldAggregates = comparer.CreateFastDictionaryGenerator<TKey, HeldState<TState>>(1, this.keyComparerEquals, this.keyComparerGetHashCode, stream.Properties.QueryContainer).Invoke();

            this.isUngrouped = typeof(TKey) == typeof(Empty);
            this.errorMessages = stream.ErrorMessages;
        }

        public override unsafe void OnNext(StreamMessage<Empty, TInput> batch)
        {
            this.batch.iter = batch.iter;

            var count = batch.Count;

            fixed (long* col_vsync = batch.vsync.col)
            fixed (long* col_vother = batch.vother.col)
            fixed (long* col_bv = batch.bitvector.col)
            {
                var colpayload = batch.payload.col;

                for (int i = 0; i < count; i++)
                {
                    if ((col_bv[i >> 6] & (1L << (i & 0x3f))) != 0)
                    {
                        if (col_vother[i] == long.MinValue)
                        {
                            // We have found a row that corresponds to punctuation
                            OnPunctuation(col_vsync[i]);

                            int c = this.batch.Count;
                            this.batch.vsync.col[c] = col_vsync[i];
                            this.batch.vother.col[c] = long.MinValue;
                            this.batch.key.col[c] = Empty.Default;
                            this.batch.hash.col[c] = 0;
                            this.batch.bitvector.col[c >> 6] |= 1L << (c & 0x3f);
                            this.batch.Count++;
                            if (this.batch.Count == Config.DataBatchSize) FlushContents();
                        }
                        continue;
                    }

                    var colkey_i = this.keySelector(colpayload[i]);
                    var col_hash_i = this.keyComparerGetHashCode(colkey_i);

                    var syncTime = col_vsync[i];

                    bool cachedState = false;

                    // Handle time moving forward
                    if (syncTime > this.lastSyncTime)
                    {
                        /* Issue start edges for held aggregates */
                        if (this.currentState != null && this.heldAggregates.Count == 1)
                        {
                            // there is just one held aggregate, and currentState is set
                            // so currentState has to be the held aggregate
                            cachedState = true;
                            if (this.currentState.active > 0)
                            {
                                int c = this.batch.Count;
                                this.batch.vsync.col[c] = this.currentState.timestamp;
                                this.batch.vother.col[c] = StreamEvent.InfinitySyncTime;
                                this.batch.payload.col[c] = this.finalResultSelector(this.currentKey, this.computeResult(this.currentState.state));
                                this.batch.hash.col[c] = 0;
                                this.batch.Count++;
                                if (this.batch.Count == Config.DataBatchSize) FlushContents();
                            }
                            else
                            {
                                this.aggregateByKey.Remove(this.currentKey, this.currentHash);
                                this.currentState = null;
                            }
                        }
                        else
                        {
                            int iter1 = FastDictionary<TKey, HeldState<TState>>.IteratorStart;
                            while (this.heldAggregates.Iterate(ref iter1))
                            {
                                var iter1entry = this.heldAggregates.entries[iter1];
                                if (iter1entry.value.active > 0)
                                {
                                    int c = this.batch.Count;
                                    this.batch.vsync.col[c] = iter1entry.value.timestamp;
                                    this.batch.vother.col[c] = StreamEvent.InfinitySyncTime;
                                    this.batch.payload.col[c] = this.finalResultSelector(iter1entry.key, this.computeResult(iter1entry.value.state));
                                    this.batch.hash.col[c] = 0;
                                    this.batch.Count++;
                                    if (this.batch.Count == Config.DataBatchSize) FlushContents();
                                }
                                else
                                {
                                    this.aggregateByKey.Remove(iter1entry.key); // ,  (currentKey, currentHash);
                                    this.currentState = null;
                                }
                            }

                            // Time has moved forward, clear the held aggregates
                            this.heldAggregates.Clear();
                            this.currentState = null;
                        }

                        // Since sync time changed, set lastSyncTime
                        this.lastSyncTime = syncTime;
                    }

                    if (this.currentState == null || ((!this.isUngrouped) && (this.currentHash != col_hash_i || !this.keyComparerEquals(this.currentKey, colkey_i))))
                    {
                        if (cachedState)
                        {
                            cachedState = false;
                            this.heldAggregates.Clear();
                        }

                        // Need to retrieve the key from the dictionary
                        this.currentKey = colkey_i;
                        this.currentHash = col_hash_i;

                        if (!this.heldAggregates.Lookup(this.currentKey, this.currentHash, out int index))
                        {
                            // First time group is active for this time
                            if (!this.aggregateByKey.Lookup(this.currentKey, this.currentHash, out int aggindex))
                            {
                                // New group. Create new state
                                this.currentState = new HeldState<TState> { state = this.initialState(), timestamp = syncTime };
                                this.aggregateByKey.Insert(this.currentKey, this.currentState, this.currentHash);

                                // No output because initial state is empty
                            }
                            else
                            {
                                this.currentState = this.aggregateByKey.entries[aggindex].value;

                                if (syncTime > this.currentState.timestamp)
                                {
                                    if (this.currentState.active > 0)
                                    {
                                        // Output end edge
                                        int c = this.batch.Count;
                                        this.batch.vsync.col[c] = syncTime;
                                        this.batch.vother.col[c] = this.currentState.timestamp;
                                        this.batch.payload.col[c] = this.finalResultSelector(this.currentKey, this.computeResult(this.currentState.state));
                                        this.batch.hash.col[c] = 0;
                                        this.batch.Count++;
                                        if (this.batch.Count == Config.DataBatchSize) FlushContents();
                                    }

                                    this.currentState.timestamp = syncTime;
                                }
                            }

                            this.heldAggregates.Insert(ref index, this.currentKey, this.currentState);
                        }
                        else
                        {
                            // read new currentState from _heldAgg index
                            this.currentState = this.heldAggregates.entries[index].value;
                        }
                    }
                    else
                    {
                        if (syncTime > this.currentState.timestamp)
                        {
                            if (this.currentState.active > 0)
                            {
                                // Output end edge
                                int c = this.batch.Count;
                                this.batch.vsync.col[c] = syncTime;
                                this.batch.vother.col[c] = this.currentState.timestamp;
                                this.batch.payload.col[c] = this.finalResultSelector(this.currentKey, this.computeResult(this.currentState.state));
                                this.batch.hash.col[c] = 0;
                                this.batch.Count++;
                                if (this.batch.Count == Config.DataBatchSize) FlushContents();
                            }

                            this.currentState.timestamp = syncTime;
                        }
                    }

                    if (col_vsync[i] < col_vother[i]) // insert event
                    {
                        this.currentState.state = this.accumulate(this.currentState.state, col_vsync[i], colpayload[i]);
                        this.currentState.active++;
                    }
                    else // is a retraction
                    {
                        this.currentState.state = this.deaccumulate(this.currentState.state, col_vsync[i], colpayload[i]);
                        this.currentState.active--;
                    }
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
                if (this.currentState != null && this.heldAggregates.Count == 1)
                {
                    // there is just one held aggregate, and currentState is set
                    // so currentState has to be the held aggregate
                    if (this.currentState.active > 0)
                    {
                        int c = this.batch.Count;
                        this.batch.vsync.col[c] = this.currentState.timestamp;
                        this.batch.vother.col[c] = StreamEvent.InfinitySyncTime;
                        this.batch.payload.col[c] = this.finalResultSelector(this.currentKey, this.computeResult(this.currentState.state));
                        this.batch.hash.col[c] = 0;
                        this.batch.Count++;
                        if (this.batch.Count == Config.DataBatchSize) FlushContents();
                    }
                    else
                    {
                        this.aggregateByKey.Remove(this.currentKey, this.currentHash);
                        this.currentState = null;
                    }
                }
                else
                {
                    int iter1 = FastDictionary<TKey, HeldState<TState>>.IteratorStart;
                    while (this.heldAggregates.Iterate(ref iter1))
                    {
                        var iter1entry = this.heldAggregates.entries[iter1];

                        if (iter1entry.value.active > 0)
                        {
                            int c = this.batch.Count;
                            this.batch.vsync.col[c] = iter1entry.value.timestamp;
                            this.batch.vother.col[c] = StreamEvent.InfinitySyncTime;
                            this.batch.payload.col[c] = this.finalResultSelector(iter1entry.key, this.computeResult(iter1entry.value.state));
                            this.batch.hash.col[c] = 0;
                            this.batch.Count++;
                            if (this.batch.Count == Config.DataBatchSize) FlushContents();
                        }
                        else
                        {
                            this.aggregateByKey.Remove(iter1entry.key);
                            this.currentState = null;
                        }
                    }
                }

                // Time has moved forward, clear the held aggregates
                this.heldAggregates.Clear();
                this.currentState = null;

                // Since sync time changed, set lastSyncTime
                this.lastSyncTime = syncTime;
            }
        }

        protected override void FlushContents()
        {
            if (this.batch == null || this.batch.Count == 0) return;
            this.Observer.OnNext(this.batch);
            this.pool.Get(out this.batch);
            this.batch.Allocate();
        }

        public override int CurrentlyBufferedOutputCount => this.batch.Count;

        public override int CurrentlyBufferedInputCount => this.aggregateByKey.Count;

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new GroupedWindowPlanNode<TInput, TState, TOutput>(
                previous, this,
                typeof(TKey), typeof(TInput), typeof(TOutput), this.aggregate, this.keySelectorExpr, this.finalResultSelectorExpr,
                false, this.errorMessages));

        protected override void UpdatePointers()
        {
            int iter1 = FastDictionary<TKey, HeldState<TState>>.IteratorStart;
            while (this.heldAggregates.Iterate(ref iter1))
            {
                TKey key = this.heldAggregates.entries[iter1].key;
                bool found = this.aggregateByKey.Lookup(key, out int iter2);
                if (!found) throw new InvalidOperationException();
                this.heldAggregates.entries[iter1].value = this.aggregateByKey.entries[iter2].value;
            }
        }

        protected override void DisposeState() => this.batch.Free();
    }
}
