// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    [KnownType(typeof(EndPointHeap))]
    [KnownType(typeof(EndPointQueue))]
    internal sealed class PartitionedLeftAntiSemiJoinPipe<TKey, TLeft, TRight, TPartitionKey> : BinaryPipe<TKey, TLeft, TRight, TLeft>
    {
        private const long NotActive = long.MaxValue;
        private readonly MemoryPool<TKey, TLeft> pool;
        private readonly Func<TKey, TPartitionKey> getPartitionKey = GetPartitionExtractor<TPartitionKey, TKey>();
        private readonly Func<IEndPointOrderer> leftEndPointGenerator;
        private readonly Func<IEndPointOrderer> rightEndPointGenerator;

        [SchemaSerialization]
        private readonly Expression<Func<TKey, TKey, bool>> keyComparer;
        private readonly Func<TKey, TKey, bool> keyComparerEquals;
        [SchemaSerialization]
        private readonly Expression<Func<TLeft, TLeft, bool>> leftComparer;
        private readonly Func<TLeft, TLeft, bool> leftComparerEquals;

        [DataMember]
        private FastDictionary2<TPartitionKey, PooledElasticCircularBuffer<LEntry>> leftQueue = new FastDictionary2<TPartitionKey, PooledElasticCircularBuffer<LEntry>>();
        [DataMember]
        private FastDictionary2<TPartitionKey, PooledElasticCircularBuffer<REntry>> rightQueue = new FastDictionary2<TPartitionKey, PooledElasticCircularBuffer<REntry>>();
        [DataMember]
        private HashSet<TPartitionKey> processQueue = new HashSet<TPartitionKey>();
        [DataMember]
        private HashSet<TPartitionKey> seenKeys = new HashSet<TPartitionKey>();
        [DataMember]
        private HashSet<TPartitionKey> cleanKeys = new HashSet<TPartitionKey>();

        [DataMember]
        private StreamMessage<TKey, TLeft> output;
        [DataMember]
        private FastDictionary<TPartitionKey, PartitionEntry> partitionData = new FastDictionary<TPartitionKey, PartitionEntry>();
        [DataMember]
        private long lastLeftCTI = long.MinValue;
        [DataMember]
        private long lastRightCTI = long.MinValue;
        [DataMember]
        private bool emitCTI = false;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public PartitionedLeftAntiSemiJoinPipe() { }

        public PartitionedLeftAntiSemiJoinPipe(LeftAntiSemiJoinStreamable<TKey, TLeft, TRight> stream, IStreamObserver<TKey, TLeft> observer)
            : base(stream, observer)
        {
            this.keyComparer = stream.Properties.KeyEqualityComparer.GetEqualsExpr();
            this.keyComparerEquals = this.keyComparer.Compile();

            this.leftComparer = stream.LeftComparer.GetEqualsExpr();
            this.leftComparerEquals = this.leftComparer.Compile();

            if (stream.Left.Properties.IsConstantDuration)
                this.leftEndPointGenerator = () => new EndPointQueue();
            else
                this.leftEndPointGenerator = () => new EndPointHeap();
            if (stream.Right.Properties.IsConstantDuration)
                this.rightEndPointGenerator = () => new EndPointQueue();
            else
                this.rightEndPointGenerator = () => new EndPointHeap();

            this.pool = MemoryManager.GetMemoryPool<TKey, TLeft>(stream.Properties.IsColumnar);
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        protected override void ProduceBinaryQueryPlan(PlanNode left, PlanNode right)
        {
            var node = new JoinPlanNode(
                left, right, this,
                typeof(TLeft), typeof(TRight), typeof(TLeft), typeof(TKey),
                JoinKind.LeftAntiSemiJoin,
                false, null);
            node.AddJoinExpression("key comparer", this.keyComparer);
            node.AddJoinExpression("left key comparer", this.leftComparer);
            this.Observer.ProduceQueryPlan(node);
        }

        private void NewPartition(TPartitionKey pKey)
        {
            this.leftQueue.Insert(pKey, new PooledElasticCircularBuffer<LEntry>());
            this.rightQueue.Insert(pKey, new PooledElasticCircularBuffer<REntry>());

            if (!this.partitionData.Lookup(pKey, out int index)) this.partitionData.Insert(ref index, pKey, new PartitionEntry(this));
        }

        protected override void DisposeState()
        {
            int iter;
            iter = FastDictionary<TPartitionKey, LEntry>.IteratorStart;
            while (this.leftQueue.Iterate(ref iter)) this.leftQueue.entries[iter].value.Dispose();
            iter = FastDictionary<TPartitionKey, REntry>.IteratorStart;
            while (this.leftQueue.Iterate(ref iter)) this.rightQueue.entries[iter].value.Dispose();
            this.output.Free();
        }

        protected override void ProcessBothBatches(StreamMessage<TKey, TLeft> leftBatch, StreamMessage<TKey, TRight> rightBatch, out bool leftBatchDone, out bool rightBatchDone, out bool leftBatchFree, out bool rightBatchFree)
        {
            ProcessLeftBatch(leftBatch, out leftBatchDone, out leftBatchFree);
            ProcessRightBatch(rightBatch, out rightBatchDone, out rightBatchFree);
        }

        protected override void ProcessLeftBatch(StreamMessage<TKey, TLeft> batch, out bool leftBatchDone, out bool leftBatchFree)
        {
            leftBatchDone = true;
            leftBatchFree = true;
            batch.iter = 0;

            PooledElasticCircularBuffer<LEntry> queue = null;
            TPartitionKey previous = default;
            bool first = true;
            for (var i = 0; i < batch.Count; i++)
            {
                if ((batch.bitvector.col[i >> 6] & (1L << (i & 0x3f))) != 0
                    && (batch.vother.col[i] >= 0)) continue;

                if (batch.vother.col[i] == PartitionedStreamEvent.LowWatermarkOtherTime)
                {
                    bool timeAdvanced = this.lastLeftCTI != batch.vsync.col[i];
                    if (timeAdvanced && this.lastLeftCTI < this.lastRightCTI) this.emitCTI = true;
                    this.lastLeftCTI = batch.vsync.col[i];
                    foreach (var p in this.seenKeys) this.processQueue.Add(p);
                    continue;
                }

                var partitionKey = this.getPartitionKey(batch.key.col[i]);
                if (first || !partitionKey.Equals(previous))
                {
                    if (this.seenKeys.Add(partitionKey)) NewPartition(partitionKey);
                    this.leftQueue.Lookup(partitionKey, out int index);
                    queue = this.leftQueue.entries[index].value;
                    this.processQueue.Add(partitionKey);
                }
                var e = new LEntry
                {
                    Key = batch.key.col[i],
                    Sync = batch.vsync.col[i],
                    Other = batch.vother.col[i],
                    Payload = batch.payload.col[i],
                    Hash = batch.hash.col[i],
                };
                queue.Enqueue(ref e);
                first = false;
                previous = partitionKey;
            }

            ProcessPendingEntries();
        }

        protected override void ProcessRightBatch(StreamMessage<TKey, TRight> batch, out bool rightBatchDone, out bool rightBatchFree)
        {
            rightBatchDone = true;
            rightBatchFree = true;
            batch.iter = 0;

            PooledElasticCircularBuffer<REntry> queue = null;
            TPartitionKey previous = default;
            bool first = true;
            for (var i = 0; i < batch.Count; i++)
            {
                if ((batch.bitvector.col[i >> 6] & (1L << (i & 0x3f))) != 0
                    && (batch.vother.col[i] >= 0)) continue;

                if (batch.vother.col[i] == PartitionedStreamEvent.LowWatermarkOtherTime)
                {
                    bool timeAdvanced = this.lastRightCTI != batch.vsync.col[i];
                    if (timeAdvanced && this.lastRightCTI < this.lastLeftCTI) this.emitCTI = true;
                    this.lastRightCTI = batch.vsync.col[i];
                    foreach (var p in this.seenKeys) this.processQueue.Add(p);
                    continue;
                }

                var partitionKey = this.getPartitionKey(batch.key.col[i]);
                if (first || !partitionKey.Equals(previous))
                {
                    if (this.seenKeys.Add(partitionKey)) NewPartition(partitionKey);
                    this.rightQueue.Lookup(partitionKey, out int index);
                    queue = this.rightQueue.entries[index].value;
                    this.processQueue.Add(partitionKey);
                }
                var e = new REntry
                {
                    Key = batch.key.col[i],
                    Sync = batch.vsync.col[i],
                    Other = batch.vother.col[i],
                    Hash = batch.hash.col[i],
                };
                queue.Enqueue(ref e);
                first = false;
                previous = partitionKey;
            }

            ProcessPendingEntries();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessPendingEntries()
        {
            foreach (var pKey in this.processQueue)
            {
                // Partition is no longer clean if we are processing it. If it is still clean, it will be added below.
                this.cleanKeys.Remove(pKey);

                this.leftQueue.Lookup(pKey, out int index);
                var leftWorking = this.leftQueue.entries[index].value;
                var rightWorking = this.rightQueue.entries[index].value;
                this.partitionData.Lookup(pKey, out int fullIndex);
                var partition = this.partitionData.entries[fullIndex].value;

                while (true)
                {
                    var old = partition.currTime;
                    bool hasLeftBatch = leftWorking.TryPeekFirst(out var leftEntry);
                    bool hasRightBatch = rightWorking.TryPeekFirst(out var rightEntry);
                    if (hasLeftBatch && hasRightBatch)
                    {
                        partition.nextLeftTime = leftEntry.Sync;
                        partition.nextRightTime = rightEntry.Sync;

                        if (leftEntry.Sync <= rightEntry.Sync)
                        {
                            UpdateTime(partition, partition.nextLeftTime);

                            if (leftEntry.Other != long.MinValue)
                            {
                                ProcessLeftEvent(
                                    partition,
                                    leftEntry.Sync,
                                    leftEntry.Other,
                                    ref leftEntry.Key,
                                    leftEntry.Payload,
                                    leftEntry.Hash);
                            }
                            else if (partition.currTime > old)
                            {
                                AddToBatch(
                                    partition.currTime,
                                    long.MinValue,
                                    ref leftEntry.Key,
                                    ref leftEntry.Payload,
                                    leftEntry.Hash);
                            }

                            leftWorking.TryDequeue(out leftEntry);
                        }
                        else
                        {
                            UpdateTime(partition, partition.nextRightTime);

                            if (rightEntry.Other != long.MinValue)
                            {
                                ProcessRightEvent(
                                    partition,
                                    rightEntry.Sync,
                                    rightEntry.Other,
                                    ref rightEntry.Key,
                                    rightEntry.Hash);
                            }
                            else if (partition.currTime > old)
                            {
                                var l = default(TLeft);
                                AddToBatch(
                                    partition.currTime,
                                    long.MinValue,
                                    ref rightEntry.Key,
                                    ref l,
                                    rightEntry.Hash);
                            }

                            rightWorking.TryDequeue(out rightEntry);
                        }
                    }
                    else if (hasLeftBatch)
                    {
                        partition.nextLeftTime = leftEntry.Sync;
                        partition.nextRightTime = Math.Max(partition.nextRightTime, this.lastRightCTI);
                        if (leftEntry.Sync > partition.nextRightTime)
                        {
                            // If we have not yet reached the lesser of the two sides (in this case, right), and we don't
                            // have input from that side, reach that time now. This can happen with low watermarks.
                            if (partition.currTime < partition.nextRightTime)
                                UpdateTime(partition, partition.nextRightTime);
                            break;
                        }

                        UpdateTime(partition, leftEntry.Sync);

                        if (leftEntry.Other != long.MinValue)
                        {
                            ProcessLeftEvent(
                                partition,
                                leftEntry.Sync,
                                leftEntry.Other,
                                ref leftEntry.Key,
                                leftEntry.Payload,
                                leftEntry.Hash);
                        }
                        else if (partition.currTime > old)
                        {
                            AddToBatch(
                                partition.currTime,
                                long.MinValue,
                                ref leftEntry.Key,
                                ref leftEntry.Payload,
                                leftEntry.Hash);
                        }

                        leftWorking.TryDequeue(out leftEntry);
                    }
                    else if (hasRightBatch)
                    {
                        partition.nextRightTime = rightEntry.Sync;
                        partition.nextLeftTime = Math.Max(partition.nextLeftTime, this.lastLeftCTI);
                        if (partition.nextLeftTime < rightEntry.Sync)
                        {
                            // If we have not yet reached the lesser of the two sides (in this case, left), and we don't
                            // have input from that side, reach that time now. This can happen with low watermarks.
                            if (partition.currTime < partition.nextLeftTime)
                                UpdateTime(partition, partition.nextLeftTime);
                            break;
                        }

                        UpdateTime(partition, rightEntry.Sync);

                        if (rightEntry.Other != long.MinValue)
                        {
                            ProcessRightEvent(
                                partition,
                                rightEntry.Sync,
                                rightEntry.Other,
                                ref rightEntry.Key,
                                rightEntry.Hash);
                        }
                        else if (partition.currTime > old)
                        {
                            var l = default(TLeft);
                            AddToBatch(
                                partition.currTime,
                                long.MinValue,
                                ref rightEntry.Key,
                                ref l,
                                rightEntry.Hash);
                        }

                        rightWorking.TryDequeue(out rightEntry);
                    }
                    else
                    {
                        if (partition.nextLeftTime < this.lastLeftCTI)
                            partition.nextLeftTime = this.lastLeftCTI;
                        if (partition.nextRightTime < this.lastRightCTI)
                            partition.nextRightTime = this.lastRightCTI;

                        UpdateTime(partition, Math.Min(this.lastLeftCTI, this.lastRightCTI));
                        if (partition.IsClean()) this.cleanKeys.Add(pKey);

                        break;
                    }
                }
            }

            if (this.emitCTI)
            {
                var earliest = Math.Min(this.lastLeftCTI, this.lastRightCTI);
                AddLowWatermarkToBatch(earliest);
                this.emitCTI = false;
                foreach (var p in this.cleanKeys)
                {
                    this.seenKeys.Remove(p);
                    this.leftQueue.Lookup(p, out var index);
                    this.leftQueue.entries[index].value.Dispose();
                    this.leftQueue.Remove(p);
                    this.rightQueue.entries[index].value.Dispose();
                    this.rightQueue.Remove(p);
                }

                this.cleanKeys.Clear();
            }

            this.processQueue.Clear();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateTime(PartitionEntry partition, long time)
        {
            if (time > partition.currTime)
            {
                LeaveTime(partition);
                partition.currTime = time;
                ReachTime(partition);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessLeftEvent(PartitionEntry partition, long start, long end, ref TKey key, TLeft payload, int hash)
        {
            if (start < end)
            {
                // Row is a start edge or interval.
                bool isProcessable = partition.nextRightTime > start;
                bool isInterval = end < StreamEvent.InfinitySyncTime;
                var map = isInterval ? partition.leftIntervalMap : partition.leftEdgeMap;
                var leph = partition.leftEndPointHeap;
                if (isProcessable)
                {
                    if (FindOnRight(partition, ref key, hash, out _))
                    {
                        // Row joins with something on right, so not currently visible.
                        int index = map.Insert(hash);
                        map.Values[index].Populate(start, NotActive, end, ref key, ref payload);
                        if (isInterval) leph.Insert(end, index);
                    }
                    else
                    {
                        // Row does not join (and it is processable).
                        bool isFullyOutputtable = isInterval && partition.nextRightTime >= end;
                        if (isFullyOutputtable)
                        {
                            // Will never join because right has advanced beyond endtime, so output interval.
                            AddToBatch(start, end, ref key, ref payload, hash);
                        }
                        else
                        {
                            // Output start edge.
                            int index = map.Insert(hash);
                            map.Values[index].Populate(start, start, end, ref key, ref payload);
                            AddToBatch(start, StreamEvent.InfinitySyncTime, ref key, ref payload, hash);
                            if (isInterval) leph.Insert(end, index);
                        }
                    }
                }
                else
                {
                    // Row is not yet processable, so insert as invisible.
                    int index = map.InsertInvisible(hash);
                    map.Values[index].Populate(start, NotActive, end, ref key, ref payload);
                }
            }
            else
            {
                // Row is an end edge.

                // Remove from leftEdgeMap.
                var lem = partition.leftEdgeMap;
                var leftEvents = lem.Find(hash);
                while (leftEvents.Next(out int index))
                {
                    if (AreSame(end, StreamEvent.InfinitySyncTime, ref key, ref payload, ref lem.Values[index]))
                    {
                        long currentStart = lem.Values[index].CurrentStart;
                        if (currentStart != NotActive)
                        {
                            // Matching left start edge is currently visible, so output end edge.
                            AddToBatch(start, currentStart, ref key, ref payload, hash);
                        }

                        leftEvents.Remove();
                        break;
                    }
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessRightEvent(PartitionEntry partition, long start, long end, ref TKey key, int hash)
        {
            if (start < end)
            {
                // Row is a start edge or interval.
                var rm = partition.rightMap;
                if (FindOnRight(partition, ref key, hash, out int index))
                {
                    // Corresponding key already exists in map, so any joining on left and already not active.
                    rm.Values[index].Count++;
                }
                else
                {
                    // First instance of this key, so insert and make any joining left entries not active.
                    index = rm.Insert(hash);
                    rm.Values[index].Initialize(ref key);
                    MakeMatchingLeftInvisible(partition, start, ref key, hash);
                }

                if (end != StreamEvent.InfinitySyncTime)
                {
                    // Row is an interval, so schedule removal of interval.
                    var reph = partition.rightEndPointHeap;
                    reph.Insert(end, index);
                }
            }
            else
            {
                // Row is an end edge.

                // Queue for removal when time advances.
                var ree = partition.rightEndEdges;
                int index = ree.Push();
                ree.Values[index].Populate(ref key, hash);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void LeaveTime(PartitionEntry partition)
        {
            // Carry-out all queued end edges for right events.
            int hash;
            var leph = partition.leftEndPointHeap;
            var lem = partition.leftEdgeMap;
            var lim = partition.leftIntervalMap;
            var rm = partition.rightMap;
            var ree = partition.rightEndEdges;
            for (int i = 0; i < ree.Count; i++)
            {
                hash = ree.Values[i].Hash;
                if (FindOnRight(partition, ref ree.Values[i].Key, hash, out int index))
                {
                    int count = rm.Values[index].Count - 1;
                    if (count > 0)
                    {
                        rm.Values[index].Count = count;
                    }
                    else
                    {
                        MakeMatchingLeftVisible(partition, partition.currTime, ref rm.Values[index].Key, hash);
                        rm.Remove(index);
                    }
                }
            }

            ree.Clear();

            // Actually insert all pending left start intervals.
            var leftEvents = lim.TraverseInvisible();
            while (leftEvents.Next(out int index, out hash))
            {
                long end = lim.Values[index].End;
                if (FindOnRight(partition, ref lim.Values[index].Key, hash, out _))
                {
                    leftEvents.MakeVisible();
                    leph.Insert(end, index);
                }
                else
                {
                    // Row does not join.
                    bool isFullyOutputtable = partition.nextRightTime >= end;
                    if (isFullyOutputtable)
                    {
                        AddToBatch(
                            partition.currTime,
                            end,
                            ref lim.Values[index].Key,
                            ref lim.Values[index].Payload,
                            hash);
                        leftEvents.Remove();
                    }
                    else
                    {
                        leftEvents.MakeVisible();
                        lim.Values[index].CurrentStart = partition.currTime;
                        AddToBatch(
                            partition.currTime,
                            StreamEvent.InfinitySyncTime,
                            ref lim.Values[index].Key,
                            ref lim.Values[index].Payload,
                            hash);

                        leph.Insert(end, index);
                    }
                }
            }

            // Actually insert all pending left start edges.
            leftEvents = lem.TraverseInvisible();
            while (leftEvents.Next(out int index, out hash))
            {
                if (!FindOnRight(partition, ref lem.Values[index].Key, hash, out _))
                {
                    // Row does not join, so output start edge.
                    lem.Values[index].CurrentStart = partition.currTime;
                    AddToBatch(
                        partition.currTime,
                        StreamEvent.InfinitySyncTime,
                        ref lem.Values[index].Key,
                        ref lem.Values[index].Payload,
                        hash);
                }

                leftEvents.MakeVisible();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReachTime(PartitionEntry partition)
        {
            // Carry-out all interval endpoints for left intervals that end prior to (or at) new current time.
            var leph = partition.leftEndPointHeap;
            var reph = partition.rightEndPointHeap;
            var lim = partition.leftIntervalMap;
            var rm = partition.rightMap;

            if (!leph.TryPeekNext(out long leftTime, out int leftIndex))
                leftTime = long.MaxValue;
            if (!reph.TryPeekNext(out long rightTime, out int rightIndex))
                rightTime = long.MaxValue;

            while (leftTime <= partition.currTime || rightTime < partition.currTime)
            {
                if (leftTime <= rightTime)
                {
                    // Always process left end-points first as they may end intervals that right end-points may
                    // try to make visible.
                    long currentStart = lim.Values[leftIndex].CurrentStart;
                    if (currentStart != NotActive)
                    {
                        // Matching left start edge is currently visible, so output end edge.
                        AddToBatch(
                            leftTime,
                            currentStart,
                            ref lim.Values[leftIndex].Key,
                            ref lim.Values[leftIndex].Payload,
                            lim.GetHash(leftIndex));
                    }

                    lim.Remove(leftIndex);

                    leph.RemoveTop();
                    if (!leph.TryPeekNext(out leftTime, out leftIndex)) leftTime = long.MaxValue;
                }
                else
                {
                    // Process right end-point up to but not including the current time.
                    int count = rm.Values[rightIndex].Count - 1;
                    if (count > 0)
                    {
                        rm.Values[rightIndex].Count = count;
                    }
                    else
                    {
                        MakeMatchingLeftVisible(partition, rightTime, ref rm.Values[rightIndex].Key, rm.GetHash(rightIndex));
                        rm.Remove(rightIndex);
                    }

                    reph.RemoveTop();
                    if (!reph.TryPeekNext(out rightTime, out rightIndex)) rightTime = long.MaxValue;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool FindOnRight(PartitionEntry partition, ref TKey key, int hash, out int index)
        {
            var rm = partition.rightMap;
            var rightEvents = rm.Find(hash);
            while (rightEvents.Next(out int rightIndex))
            {
                if (this.keyComparerEquals(key, rm.Values[rightIndex].Key))
                {
                    index = rightIndex;
                    return true;
                }
            }

            index = 0;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void MakeMatchingLeftInvisible(PartitionEntry partition, long time, ref TKey key, int hash)
        {
            // Make matching left intervals invisible.
            var lem = partition.leftEdgeMap;
            var lim = partition.leftIntervalMap;
            var leftEvents = lim.Find(hash);
            while (leftEvents.Next(out int index))
            {
                if (this.keyComparerEquals(key, lim.Values[index].Key))
                {
                    // Output end edge.
                    AddToBatch(
                        time,
                        lim.Values[index].CurrentStart,
                        ref lim.Values[index].Key,
                        ref lim.Values[index].Payload,
                        hash);

                    // Mark left event as not visible.
                    lim.Values[index].CurrentStart = NotActive;
                }
            }

            // Make matching left edges invisible.
            leftEvents = lem.Find(hash);
            while (leftEvents.Next(out int index))
            {
                if (this.keyComparerEquals(key, lem.Values[index].Key))
                {
                    // Output end edge.
                    AddToBatch(
                        time,
                        lem.Values[index].CurrentStart,
                        ref lem.Values[index].Key,
                        ref lem.Values[index].Payload,
                        hash);

                    // Mark left event as not visible.
                    lem.Values[index].CurrentStart = NotActive;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void MakeMatchingLeftVisible(PartitionEntry partition, long time, ref TKey key, int hash)
        {
            // Make matching left intervals visible.
            var lem = partition.leftEdgeMap;
            var lim = partition.leftIntervalMap;
            var leftEvents = lim.Find(hash);
            while (leftEvents.Next(out int index))
            {
                if (this.keyComparerEquals(key, lim.Values[index].Key))
                {
                    long end = lim.Values[index].End;
                    bool isFullyOutputtable = partition.nextRightTime >= end;
                    if (isFullyOutputtable)
                    {
                        // Output interval.
                        AddToBatch(
                            time,
                            end,
                            ref lim.Values[index].Key,
                            ref lim.Values[index].Payload,
                            hash);

                        // Mark left event as not visible so that an end-edge is not outputted when the interval actually endss.
                        lim.Values[index].CurrentStart = NotActive;
                    }
                    else
                    {
                        // Output start edge.
                        AddToBatch(
                            time,
                            StreamEvent.InfinitySyncTime,
                            ref lim.Values[index].Key,
                            ref lim.Values[index].Payload,
                            hash);

                        // Mark left event as visible.
                        lim.Values[index].CurrentStart = time;
                    }
                }
            }

            // Make matching left edges visible.
            leftEvents = lem.Find(hash);
            while (leftEvents.Next(out int index))
            {
                if (this.keyComparerEquals(key, lem.Values[index].Key))
                {
                    // Output start edge.
                    AddToBatch(
                        time,
                        StreamEvent.InfinitySyncTime,
                        ref lem.Values[index].Key,
                        ref lem.Values[index].Payload,
                        hash);

                    // Mark left event as visible.
                    lem.Values[index].CurrentStart = time;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AddLowWatermarkToBatch(long start)
        {
            if (start > this.lastCTI)
            {
                this.lastCTI = start;

                int index = this.output.Count++;
                this.output.vsync.col[index] = start;
                this.output.vother.col[index] = PartitionedStreamEvent.LowWatermarkOtherTime;
                this.output.key.col[index] = default;
                this.output[index] = default;
                this.output.hash.col[index] = 0;
                this.output.bitvector.col[index >> 6] |= 1L << (index & 0x3f);

                if (this.output.Count == Config.DataBatchSize) FlushContents();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AddToBatch(long start, long end, ref TKey key, ref TLeft payload, int hash)
        {
            if (start < this.lastCTI)
            {
                throw new StreamProcessingOutOfOrderException("Outputting an event out of order!");
            }

            int index = this.output.Count++;
            this.output.vsync.col[index] = start;
            this.output.vother.col[index] = end;
            this.output.key.col[index] = key;
            this.output[index] = payload;
            this.output.hash.col[index] = hash;

            if (end == long.MinValue) this.output.bitvector.col[index >> 6] |= 1L << (index & 0x3f);
            if (this.output.Count == Config.DataBatchSize) FlushContents();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool AreSame(long start, long end, ref TKey key, ref TLeft payload, ref LeftEvent active)
            => start == active.Start && end == active.End && this.keyComparerEquals(key, active.Key) && this.leftComparerEquals(payload, active.Payload);

        protected override void FlushContents()
        {
            if (this.output.Count == 0) return;
            this.output.Seal();
            this.Observer.OnNext(this.output);
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        public override int CurrentlyBufferedOutputCount => this.output.Count;
        public override int CurrentlyBufferedLeftInputCount
        {
            get
            {
                int count = base.CurrentlyBufferedLeftInputCount;
                int key = FastDictionary2<TPartitionKey, PooledElasticCircularBuffer<LEntry>>.IteratorStart;
                while (this.leftQueue.Iterate(ref key)) count += this.leftQueue.entries[key].value.Count;
                int iter = FastDictionary<TPartitionKey, PartitionEntry>.IteratorStart;
                while (this.partitionData.Iterate(ref iter))
                {
                    var p = this.partitionData.entries[iter].value;
                    count += p.leftEdgeMap.Count + p.leftIntervalMap.Count;
                }
                return count;
            }
        }
        public override int CurrentlyBufferedRightInputCount
        {
            get
            {
                int count = base.CurrentlyBufferedRightInputCount;
                int key = FastDictionary2<TPartitionKey, PooledElasticCircularBuffer<REntry>>.IteratorStart;
                while (this.rightQueue.Iterate(ref key)) count += this.rightQueue.entries[key].value.Count;
                int iter = FastDictionary<TPartitionKey, PartitionEntry>.IteratorStart;
                while (this.partitionData.Iterate(ref iter))
                {
                    var p = this.partitionData.entries[iter].value;
                    count += p.rightEndEdges.Count + p.rightMap.Count;
                }
                return count;
            }
        }
        public override int CurrentlyBufferedLeftKeyCount
        {
            get
            {
                var keys = new HashSet<TPartitionKey>();
                int key = FastDictionary2<TPartitionKey, PooledElasticCircularBuffer<LEntry>>.IteratorStart;
                while (this.leftQueue.Iterate(ref key)) if (this.leftQueue.entries[key].value.Count > 0) keys.Add(this.leftQueue.entries[key].key);
                int iter = FastDictionary<TPartitionKey, PartitionEntry>.IteratorStart;
                while (this.partitionData.Iterate(ref iter))
                {
                    var p = this.partitionData.entries[iter].value;
                    if (p.leftEdgeMap.Count > 0) keys.Add(this.partitionData.entries[iter].key);
                    if (p.leftIntervalMap.Count > 0) keys.Add(this.partitionData.entries[iter].key);
                }
                return keys.Count;
            }
        }
        public override int CurrentlyBufferedRightKeyCount
        {
            get
            {
                var keys = new HashSet<TPartitionKey>();
                int key = FastDictionary2<TPartitionKey, PooledElasticCircularBuffer<REntry>>.IteratorStart;
                while (this.rightQueue.Iterate(ref key)) if (this.rightQueue.entries[key].value.Count > 0) keys.Add(this.rightQueue.entries[key].key);
                int iter = FastDictionary<TPartitionKey, PartitionEntry>.IteratorStart;
                while (this.partitionData.Iterate(ref iter))
                {
                    var p = this.partitionData.entries[iter].value;
                    if (p.rightEndEdges.Count > 0) keys.Add(this.partitionData.entries[iter].key);
                    if (p.rightMap.Count > 0) keys.Add(this.partitionData.entries[iter].key);
                }
                return keys.Count;
            }
        }

        [DataContract]
        private struct LeftEvent
        {
            [DataMember]
            public long Start;
            [DataMember]
            public long CurrentStart;
            [DataMember]
            public long End;
            [DataMember]
            public TKey Key;
            [DataMember]
            public TLeft Payload;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Populate(long start, long currentStart, long end, ref TKey key, ref TLeft payload)
            {
                this.Start = start;
                this.CurrentStart = currentStart;
                this.End = end;
                this.Key = key;
                this.Payload = payload;
            }

            public override string ToString()
                => "[Start=" + this.Start + ", CurrentStart=" + this.CurrentStart + ", End=" + this.End + ", Key='" + this.Key + "', Payload='" + this.Payload + "']";
        }

        [DataContract]
        private struct RightEvent
        {
            [DataMember]
            public TKey Key;
            [DataMember]
            public int Count;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Initialize(ref TKey key)
            {
                this.Key = key;
                this.Count = 1;
            }

            public override string ToString()
                => "[Key='" + this.Key + "', Count=" + this.Count + "]";
        }

        [DataContract]
        private struct QueuedEndEdge
        {
            [DataMember]
            public TKey Key;
            [DataMember]
            public int Hash;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Populate(ref TKey key, int hash)
            {
                this.Key = key;
                this.Hash = hash;
            }

            public override string ToString()
                => "[Key='" + this.Key + "', Hash=" + this.Hash + "]";
        }

        public override bool LeftInputHasState
        {
            get
            {
                int index = FastDictionary<TPartitionKey, HashSet<PartitionedStreamEvent<TKey, TLeft>>>.IteratorStart;
                while (this.leftQueue.Iterate(ref index)) if (this.leftQueue.entries[index].value.Count > 0) return true;
                return false;
            }
        }

        public override bool RightInputHasState
        {
            get
            {
                int index = FastDictionary<TPartitionKey, HashSet<PartitionedStreamEvent<TKey, TRight>>>.IteratorStart;
                while (this.rightQueue.Iterate(ref index)) if (this.rightQueue.entries[index].value.Count > 0) return true;
                return false;
            }
        }

        [DataContract]
        private sealed class LEntry
        {
            [DataMember]
            public long Sync;
            [DataMember]
            public long Other;
            [DataMember]
            public int Hash;
            [DataMember]
            public TKey Key;
            [DataMember]
            public TLeft Payload;
        }

        [DataContract]
        private sealed class REntry
        {
            [DataMember]
            public long Sync;
            [DataMember]
            public long Other;
            [DataMember]
            public int Hash;
            [DataMember]
            public TKey Key;
        }

        [DataContract]
        private sealed class PartitionEntry
        {
            /// <summary>
            /// Stores left intervals starting at <see cref="currTime"/>.
            /// FastMap visibility means that the interval is currently joined by at least one event on the right.
            /// When the interval is not joined, it is "invisible".
            /// </summary>
            [DataMember]
            public FastMap<LeftEvent> leftIntervalMap = new FastMap<LeftEvent>();

            /// <summary>
            /// Stores left start edges at <see cref="currTime"/>
            /// FastMap visibility means that the interval is currently joined by at least one event on the right.
            /// When the interval is not joined, it is "invisible".
            /// </summary>
            [DataMember]
            public FastMap<LeftEvent> leftEdgeMap = new FastMap<LeftEvent>();

            /// <summary>
            /// Stores left end edges at some point in the future, i.e. after <see cref="currTime"/>.
            /// These can originate from edge end events or interval events.
            /// </summary>
            [DataMember]
            public IEndPointOrderer leftEndPointHeap;

            /// <summary>
            /// Stores the right events present for this partition at <see cref="currTime"/>
            /// </summary>
            [DataMember]
            public FastMap<RightEvent> rightMap = new FastMap<RightEvent>();

            /// <summary>
            /// Stores right end edges for <see cref="currTime"/>, excluding interval endpoints
            /// </summary>
            [DataMember]
            public FastStack<QueuedEndEdge> rightEndEdges = new FastStack<QueuedEndEdge>();

            /// <summary>
            /// Stores right endpoints at some point in the future, i.e. after <see cref="currTime"/>, originating
            /// from intervals.
            /// </summary>
            [DataMember]
            public IEndPointOrderer rightEndPointHeap;

            [DataMember]
            public long nextLeftTime = long.MinValue;
            [DataMember]
            public long nextRightTime = long.MinValue;
            [DataMember]
            public long currTime = long.MinValue;

            [Obsolete("Used only by serialization, do not use directly")]
            public PartitionEntry() { }

            public PartitionEntry(PartitionedLeftAntiSemiJoinPipe<TKey, TLeft, TRight, TPartitionKey> parent)
            {
                this.leftEndPointHeap = parent.leftEndPointGenerator();
                this.rightEndPointHeap = parent.rightEndPointGenerator();
            }

            public bool IsClean() => this.leftIntervalMap.IsEmpty &&
                                     this.leftEdgeMap.IsEmpty &&
                                     this.leftEndPointHeap.IsEmpty &&
                                     this.rightMap.IsEmpty &&
                                     this.rightEndEdges.Count == 0 &&
                                     this.rightEndPointHeap.IsEmpty;
        }
    }
}
