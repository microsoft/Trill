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
    /// Operator that uses a full-fledged priority queue as ECQ
    /// </summary>
    [DataContract]
    internal sealed class PartitionedSnapshotWindowPriorityQueuePipe<TKey, TInput, TState, TOutput, TPartitionKey> : UnaryPipe<TKey, TInput, TOutput>
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
        private FastDictionary2<TPartitionKey, PartitionEntry> partitionData = new FastDictionary2<TPartitionKey, PartitionEntry>();

        private readonly Func<TKey, TPartitionKey> getPartitionKey = GetPartitionExtractor<TPartitionKey, TKey>();

        [Obsolete("Used only by serialization. Do not call directly.")]
        public PartitionedSnapshotWindowPriorityQueuePipe() { }

        public PartitionedSnapshotWindowPriorityQueuePipe(
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

            var generator = comparer.CreateFastDictionary2Generator<TKey, HeldState<TState>>(1, this.keyComparerEquals, this.keyComparerGetHashCode, stream.Properties.QueryContainer);
            this.aggregateByKey = generator.Invoke();
            var stateDictGenerator = comparer.CreateFastDictionaryGenerator<TKey, StateAndActive<TState>>(1, this.keyComparerEquals, this.keyComparerGetHashCode, stream.Properties.QueryContainer);
            this.ecqEntryPool = new DataStructurePool<FastDictionary<TKey, StateAndActive<TState>>>(() => stateDictGenerator.Invoke());
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new SnapshotWindowPlanNode<TInput, TState, TOutput>(
                previous, this, typeof(TKey), typeof(TInput), typeof(TOutput),
                AggregatePipeType.PriorityQueue, this.aggregate, false, this.errorMessages));

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
                            var p = this.getPartitionKey(colkey[i]);
                            PartitionEntry partitionEntry;
                            if (!this.partitionData.Lookup(p, out int partitionIndex))
                                this.partitionData.Insert(p, (partitionEntry = new PartitionEntry { lastSyncTime = col_vsync[i] }));
                            else partitionEntry = this.partitionData.entries[partitionIndex].value;
                            OnPunctuation(partitionEntry, col_vsync[i]);

                            int c = this.batch.Count;
                            this.batch.vsync.col[c] = col_vsync[i];
                            this.batch.vother.col[c] = long.MinValue;
                            this.batch.key.col[c] = colkey[i];
                            this.batch.hash.col[c] = this.keyComparerGetHashCode(colkey[i]);
                            this.batch.bitvector.col[c >> 6] |= 1L << (c & 0x3f);
                            this.batch.Count++;
                            if (this.batch.Count == Config.DataBatchSize) FlushContents();
                        }
                        continue;
                    }

                    var syncTime = col_vsync[i];
                    var partition = this.getPartitionKey(colkey[i]);
                    PartitionEntry entry;

                    // Handle time moving forward
                    if (!this.partitionData.Lookup(partition, out int pIndex))
                        this.partitionData.Insert(partition, (entry = new PartitionEntry { lastSyncTime = syncTime }));
                    else if (syncTime > (entry = this.partitionData.entries[pIndex].value).lastSyncTime) AdvanceTime(entry, syncTime);

                    // Need to retrieve the key from the dictionary
                    HeldState<TState> heldState;
                    if (!this.aggregateByKey.Lookup(colkey[i], col_hash[i], out int aggindex))
                    {
                        // New group. Create new state
                        heldState = new HeldState<TState> { state = this.initialState(), timestamp = syncTime };
                        entry.heldAggregates.Add(this.aggregateByKey.Insert(colkey[i], heldState, col_hash[i]));

                        // No output because initial state is empty
                    }

                    // First time group is active for this time
                    else
                    {
                        // Update instance of key in case consumer tracks lifetime of the key object.
                        // Otherwise it may live past the Window lifetime.
                        this.aggregateByKey.entries[aggindex].key = colkey[i];

                        if (entry.heldAggregates.Add(aggindex))
                        {
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
                            FastDictionary<TKey, StateAndActive<TState>> state;
                            int index;
                            if (entry.ecq.Count > 0)
                            {
                                if (!entry.ecq.TryGetValue(col_vother[i], out state))
                                {
                                    this.ecqEntryPool.Get(out state);

                                    state.Lookup(colkey[i], col_hash[i], out index);
                                    state.Insert(ref index, colkey[i], new StateAndActive<TState> { state = this.initialState() });
                                    entry.ecq.Add(col_vother[i], state);
                                }
                                else if (!state.Lookup(colkey[i], col_hash[i], out index))
                                    state.Insert(ref index, colkey[i], new StateAndActive<TState> { state = this.initialState() });
                            }
                            else
                            {
                                this.ecqEntryPool.Get(out state);

                                state.Lookup(colkey[i], col_hash[i], out index);
                                state.Insert(ref index, colkey[i], new StateAndActive<TState> { state = this.initialState() });
                                entry.ecq.Add(col_vother[i], state);
                            }

                            state.entries[index].value.state = this.accumulate(state.entries[index].value.state, col_vsync[i], colpayload[i]);
                            state.entries[index].value.active++;
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

        public void OnLowWatermark(long syncTime)
        {
            var deprecated = new List<TPartitionKey>();

            // Handle time moving forward
            var iter = FastDictionary2<TPartitionKey, PartitionEntry>.IteratorStart;
            while (this.partitionData.Iterate(ref iter))
            {
                var partition = this.partitionData.entries[iter].value;
                if (syncTime > partition.lastSyncTime)
                {
                    OnPunctuation(partition, syncTime);

                    bool partitionHasValidOutstandingState = false;
                    if (partition.ecq != null)
                    {
                        foreach (var ecqState in partition.ecq)
                        {
                            var stateIter = FastDictionary<TKey, StateAndActive<TState>>.IteratorStart;
                            while (ecqState.Value.Iterate(ref stateIter))
                            {
                                if (ecqState.Value.entries[stateIter].value.active > 0)
                                {
                                    partitionHasValidOutstandingState = true;
                                    break;
                                }
                            }
                            if (partitionHasValidOutstandingState) break;
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
            HeldState<TState> heldState;
            foreach (int iter1 in partition.heldAggregates)
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
            partition.heldAggregates.Clear();

            /* Process the ECQ up until the new sync time */
            while (partition.ecq.Count > 0 && partition.ecq.TryGetFirst(out long ve, out FastDictionary<TKey, StateAndActive<TState>> ecqState) && ve <= syncTime)
            {
                partition.ecq.Remove(ve);
                int iter = FastDictionary<TKey, TState>.IteratorStart;

                while (ecqState.Iterate(ref iter))
                {
                    this.aggregateByKey.Lookup(ecqState.entries[iter].key, out int index);
                    heldState = this.aggregateByKey.entries[index].value;

                    if (heldState.active > 0)
                    {
                        // Issue end edge
                        int c = this.batch.Count;
                        this.batch.vsync.col[c] = ve;
                        this.batch.vother.col[c] = heldState.timestamp;
                        this.batch.payload.col[c] = this.computeResult(heldState.state);
                        this.batch.key.col[c] = ecqState.entries[iter].key;
                        this.batch.hash.col[c] = this.keyComparerGetHashCode(ecqState.entries[iter].key);
                        this.batch.Count++;
                        if (this.batch.Count == Config.DataBatchSize) FlushContents();
                    }

                    // Update aggregate
                    heldState.state = this.difference(heldState.state, ecqState.entries[iter].value.state);
                    heldState.active -= ecqState.entries[iter].value.active;

                    // Dispose state as it is not part of window anymore
                    (ecqState.entries[iter].value.state as IDisposable)?.Dispose();

                    if (ve < syncTime)
                    {
                        if (heldState.active > 0)
                        {
                            // Issue start edge
                            int c = this.batch.Count;
                            this.batch.vsync.col[c] = ve;
                            this.batch.vother.col[c] = StreamEvent.InfinitySyncTime;
                            this.batch.payload.col[c] = this.computeResult(heldState.state);
                            this.batch.key.col[c] = ecqState.entries[iter].key;
                            this.batch.hash.col[c] = this.keyComparerGetHashCode(ecqState.entries[iter].key);
                            this.batch.Count++;
                            if (this.batch.Count == Config.DataBatchSize) FlushContents();
                        }
                        else
                            this.aggregateByKey.Remove(ecqState.entries[iter].key);
                    }
                    else partition.heldAggregates.Add(index);

                    // Update timestamp
                    heldState.timestamp = ve;
                }
                ecqState.Clear();
                this.ecqEntryPool.Return(ecqState);
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
                var iter = FastDictionary<TPartitionKey, PartitionEntry>.IteratorStart;
                var count = this.aggregateByKey.Count;
                while (this.partitionData.Iterate(ref iter))
                    count += this.partitionData.entries[iter].value.ecq.Sum(o => o.Value.Count);
                return count;
            }
        }

        protected override void DisposeState()
        {
            this.batch.Free();
            if (hasDisposableState)
            {
                int index = FastDictionary2<TKey, HeldState<TState>>.IteratorStart;
                while (this.aggregateByKey.Iterate(ref index))
                {
                    (this.aggregateByKey.entries[index].value.state as IDisposable).Dispose();
                }
                index = FastDictionary2<TPartitionKey, PartitionEntry>.IteratorStart;
                while (this.partitionData.Iterate(ref index))
                {
                    foreach (var pair in this.partitionData.entries[index].value.ecq)
                    {
                        var iter = FastDictionary<TKey, StateAndActive<TState>>.IteratorStart;
                        while (pair.Value.Iterate(ref iter))
                            (pair.Value.entries[iter].value.state as IDisposable).Dispose();
                    }

                    this.partitionData.entries[index].value.ecq.Clear();
                }
            }
        }

        [DataContract]
        private sealed class PartitionEntry
        {
            [DataMember]
            public HashSet<int> heldAggregates = new HashSet<int>();
            [DataMember]
            public SortedDictionary<long, FastDictionary<TKey, StateAndActive<TState>>> ecq = new SortedDictionary<long, FastDictionary<TKey, StateAndActive<TState>>>();
            [DataMember]
            public long lastSyncTime = long.MinValue;
        }
    }
}
