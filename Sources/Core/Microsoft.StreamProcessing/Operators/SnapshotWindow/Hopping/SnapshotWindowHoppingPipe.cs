﻿// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Aggregates;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Operator that has a simple queue (we know that Ve values are non-decreasing and number of entries is capped)
    /// </summary>
    [DataContract]
    internal sealed class SnapshotWindowHoppingPipe<TKey, TInput, TState, TOutput> : UnaryPipe<TKey, TInput, TOutput>
    {
        private static readonly bool hasDisposableState = typeof(IDisposable).GetTypeInfo().IsAssignableFrom(typeof(TState));
        private readonly MemoryPool<TKey, TOutput> pool;
        private readonly DataStructurePool<FastDictionary<TKey, StateAndActive<TState>>> ecqEntryPool;
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
        [SchemaSerialization]
        private readonly Expression<Func<TKey, TKey, bool>> keyComparerEqualsExpr;
        private readonly Func<TKey, TKey, bool> keyComparerEquals;
        [SchemaSerialization]
        private readonly Expression<Func<TKey, int>> keyComparerGetHashCodeExpr;
        private readonly Func<TKey, int> keyComparerGetHashCode;

        [DataMember]
        private StreamMessage<TKey, TOutput> batch;

        [DataMember]
        private FastDictionary2<TKey, HeldState<TState>> aggregateByKey;
        [DataMember]
        private HashSet<int> heldAggregates = new HashSet<int>();
        [DataMember]
        private CircularBuffer<EcqState> ecq;
        [DataMember]
        private long lastSyncTime = long.MinValue;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public SnapshotWindowHoppingPipe() { }

        public SnapshotWindowHoppingPipe(
            SnapshotWindowStreamable<TKey, TInput, TState, TOutput> stream,
            IStreamObserver<TKey, TOutput> observer)
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

            var comparer = stream.Properties.KeyEqualityComparer;
            this.keyComparerEqualsExpr = comparer.GetEqualsExpr();
            this.keyComparerEquals = this.keyComparerEqualsExpr.Compile();
            this.keyComparerGetHashCodeExpr = comparer.GetGetHashCodeExpr();
            this.keyComparerGetHashCode = this.keyComparerGetHashCodeExpr.Compile();

            this.errorMessages = stream.ErrorMessages;
            this.pool = MemoryManager.GetMemoryPool<TKey, TOutput>(false);
            this.pool.Get(out this.batch);
            this.batch.Allocate();

            var hopsPerDuration = (int)(stream.Source.Properties.ConstantDurationLength.Value / stream.Source.Properties.ConstantHopLength) + 1;
            this.aggregateByKey = comparer.CreateFastDictionary2Generator<TKey, HeldState<TState>>(1, this.keyComparerEquals, this.keyComparerGetHashCode, stream.Properties.QueryContainer).Invoke();
            this.ecq = new CircularBuffer<EcqState>(hopsPerDuration);
            var stateDictGenerator = comparer.CreateFastDictionaryGenerator<TKey, StateAndActive<TState>>(1, this.keyComparerEquals, this.keyComparerGetHashCode, stream.Properties.QueryContainer);
            this.ecqEntryPool = new DataStructurePool<FastDictionary<TKey, StateAndActive<TState>>>(() => stateDictGenerator.Invoke());
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new SnapshotWindowPlanNode<TInput, TState, TOutput>(
                previous, this, typeof(TKey), typeof(TInput), typeof(TOutput),
                AggregatePipeType.Hopping, this.aggregate, false, this.errorMessages));

        public override unsafe void OnNext(StreamMessage<TKey, TInput> batch)
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
                    if (syncTime > this.lastSyncTime) AdvanceTime(syncTime);

                    // Need to retrieve the key from the dictionary
                    HeldState<TState> heldState;
                    if (!this.aggregateByKey.Lookup(colkey[i], col_hash[i], out int aggindex))
                    {
                        // New group. Create new state
                        heldState = new HeldState<TState> { state = this.initialState(), timestamp = syncTime };
                        this.heldAggregates.Add(this.aggregateByKey.Insert(colkey[i], heldState, col_hash[i]));

                        // No output because initial state is empty
                    }
                    else
                    {
                        // Update instance of key in case consumer tracks lifetime of the key object.
                        // Otherwise it may live past the Window lifetime.
                        this.aggregateByKey.entries[aggindex].key = colkey[i];

                        if (this.heldAggregates.Add(aggindex))
                        {
                            // First time group is active for this time
                            heldState = this.aggregateByKey.entries[aggindex].value;
                            if (syncTime > heldState.timestamp)
                            {
                                if (heldState.active > 0)
                                {
                                    // Output end edge
                                    int c = this.batch.Count;
                                    this.batch.vsync.col[c] = syncTime;
                                    this.batch.vother.col[c] = heldState.timestamp;
                                    this.batch.payload.col[c] = this.computeResult(heldState.state);
                                    this.batch.key.col[c] = colkey[i];
                                    this.batch.hash.col[c] = this.keyComparerGetHashCode(colkey[i]);
                                    this.batch.Count++;
                                    if (this.batch.Count == Config.DataBatchSize) FlushContents();
                                }
                                heldState.timestamp = syncTime;
                            }
                        }
                        else
                        {
                            // read new currentState from _heldAgg index
                            heldState = this.aggregateByKey.entries[aggindex].value;
                        }
                    }

                    if (col_vsync[i] < col_vother[i]) // insert event
                    {
                        heldState.state = this.accumulate(heldState.state, col_vsync[i], colpayload[i]);
                        heldState.active++;

                        // Update ECQ
                        if (col_vother[i] < StreamEvent.InfinitySyncTime)
                        {
                            EcqState ecqState;
                            StateAndActive<TState> newState;
                            int index;
                            if (this.ecq.Count > 0)
                            {
                                ecqState = this.ecq.PeekLast();
                                if (ecqState.timestamp != col_vother[i])
                                {
                                    ecqState = new EcqState { timestamp = col_vother[i] };
                                    this.ecqEntryPool.Get(out ecqState.states);

                                    newState = new StateAndActive<TState> { state = this.initialState() };

                                    ecqState.states.Lookup(colkey[i], col_hash[i], out index);
                                    ecqState.states.Insert(ref index, colkey[i], newState);
                                    this.ecq.Enqueue(ref ecqState);
                                }
                                else
                                {
                                    if (!ecqState.states.Lookup(colkey[i], col_hash[i], out index))
                                    {
                                        newState = new StateAndActive<TState> { state = this.initialState() };
                                        ecqState.states.Insert(ref index, colkey[i], newState);
                                    }
                                    else newState = ecqState.states.entries[index].value;
                                }
                            }
                            else
                            {
                                ecqState = new EcqState { timestamp = col_vother[i] };
                                this.ecqEntryPool.Get(out ecqState.states);
                                newState = new StateAndActive<TState> { state = this.initialState() };

                                ecqState.states.Lookup(colkey[i], col_hash[i], out index);
                                ecqState.states.Insert(ref index, colkey[i], newState);
                                this.ecq.Enqueue(ref ecqState);
                            }

                            newState.state = this.accumulate(newState.state, col_vsync[i], colpayload[i]);
                            newState.active++;
                        }
                    }
                    else // is a retraction
                    {
                        heldState.state = this.deaccumulate(heldState.state, col_vsync[i], colpayload[i]);
                        heldState.active--;
                    }
                }
            }

            batch.Release();
            batch.Return();
        }

        public void OnPunctuation(long syncTime)
        {
            // Handle time moving forward
            if (syncTime > this.lastSyncTime) AdvanceTime(syncTime);
        }

        private void AdvanceTime(long syncTime)
        {
            /* Issue start edges for held aggregates */
            HeldState<TState> heldState;
            foreach (int iter1 in this.heldAggregates)
            {
                var iter1entry = this.aggregateByKey.entries[iter1];
                if (iter1entry.value.active > 0)
                {
                    int c = this.batch.Count;
                    this.batch.vsync.col[c] = iter1entry.value.timestamp;
                    this.batch.vother.col[c] = StreamEvent.InfinitySyncTime;
                    this.batch.payload.col[c] = this.computeResult(iter1entry.value.state);
                    this.batch.key.col[c] = iter1entry.key;
                    this.batch.hash.col[c] = this.keyComparerGetHashCode(iter1entry.key);
                    this.batch.Count++;
                    if (this.batch.Count == Config.DataBatchSize) FlushContents();
                }
                else
                    this.aggregateByKey.Remove(iter1entry.key);
            }

            // Time has moved forward, clear the held aggregates
            this.heldAggregates.Clear();

            /* Process the ECQ up until the new sync time */
            while (this.ecq.Count > 0 && this.ecq.PeekFirst().timestamp <= syncTime)
            {
                EcqState ecqState = this.ecq.Dequeue();
                int iter = FastDictionary<TKey, TState>.IteratorStart;

                while (ecqState.states.Iterate(ref iter))
                {
                    this.aggregateByKey.Lookup(ecqState.states.entries[iter].key, out int index);
                    heldState = this.aggregateByKey.entries[index].value;

                    if (heldState.active > 0)
                    {
                        // Issue end edge
                        int c = this.batch.Count;
                        this.batch.vsync.col[c] = ecqState.timestamp;
                        this.batch.vother.col[c] = heldState.timestamp;
                        this.batch.payload.col[c] = this.computeResult(heldState.state);
                        this.batch.key.col[c] = ecqState.states.entries[iter].key;
                        this.batch.hash.col[c] = this.keyComparerGetHashCode(ecqState.states.entries[iter].key);
                        this.batch.Count++;
                        if (this.batch.Count == Config.DataBatchSize) FlushContents();
                    }

                    // Update aggregate
                    heldState.state = this.difference(heldState.state, ecqState.states.entries[iter].value.state);
                    heldState.active -= ecqState.states.entries[iter].value.active;

                    // Dispose state as it is not part of window anymore
                    (ecqState.states.entries[iter].value.state as IDisposable)?.Dispose();

                    if (ecqState.timestamp < syncTime)
                    {
                        if (heldState.active > 0)
                        {
                            // Issue start edge
                            int c = this.batch.Count;
                            this.batch.vsync.col[c] = ecqState.timestamp;
                            this.batch.vother.col[c] = StreamEvent.InfinitySyncTime;
                            this.batch.payload.col[c] = this.computeResult(heldState.state);
                            this.batch.key.col[c] = ecqState.states.entries[iter].key;
                            this.batch.hash.col[c] = this.keyComparerGetHashCode(ecqState.states.entries[iter].key);
                            this.batch.Count++;
                            if (this.batch.Count == Config.DataBatchSize) FlushContents();
                        }
                        else
                            this.aggregateByKey.Remove(ecqState.states.entries[iter].key);
                    }
                    else
                        this.heldAggregates.Add(index);

                    // Update timestamp
                    heldState.timestamp = ecqState.timestamp;
                }
                ecqState.states.Clear();
                this.ecqEntryPool.Return(ecqState.states);
            }

            // Since sync time changed, set lastSyncTime
            this.lastSyncTime = syncTime;
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

        public override int CurrentlyBufferedInputCount => this.aggregateByKey.Count + this.ecq.Iterate().Select(o => o.states.Count).Sum();

        protected override void DisposeState()
        {
            this.batch.Free();
            this.ecqEntryPool.Dispose();
            if (hasDisposableState)
            {
                int index = FastDictionary2<TKey, HeldState<TState>>.IteratorStart;
                while (this.aggregateByKey.Iterate(ref index))
                {
                    (this.aggregateByKey.entries[index].value.state as IDisposable).Dispose();
                }
                foreach (var state in this.ecq.Iterate())
                {
                    var iter = FastDictionary<TKey, StateAndActive<TState>>.IteratorStart;
                    while (state.states.Iterate(ref iter))
                        (state.states.entries[iter].value.state as IDisposable).Dispose();
                }
            }
        }

        [DataContract]
        private sealed class EcqState
        {
            [DataMember]
            public long timestamp;
            [DataMember]
            public FastDictionary<TKey, StateAndActive<TState>> states;
        }
    }
}