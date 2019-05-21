// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
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
    internal sealed class LeftAntiSemiJoinPipe<TKey, TLeft, TRight> : BinaryPipe<TKey, TLeft, TRight, TLeft>
    {
        private const long NotActive = long.MaxValue;
        private readonly MemoryPool<TKey, TLeft> pool;

        [SchemaSerialization]
        private readonly Expression<Func<TKey, TKey, bool>> keyComparer;
        private readonly Func<TKey, TKey, bool> keyComparerEquals;
        [SchemaSerialization]
        private readonly Expression<Func<TLeft, TLeft, bool>> leftComparer;
        private readonly Func<TLeft, TLeft, bool> leftComparerEquals;

        [DataMember]
        private StreamMessage<TKey, TLeft> output;

        /// <summary>
        /// Stores left intervals starting at <see cref="currTime"/>.
        /// FastMap visibility means that the interval is currently joined by at least one event on the right.
        /// When the interval is not joined, it is "invisible".
        /// </summary>
        [DataMember]
        private FastMap<LeftEvent> leftIntervalMap = new FastMap<LeftEvent>();

        /// <summary>
        /// Stores left start edges at <see cref="currTime"/>
        /// FastMap visibility means that the interval is currently joined by at least one event on the right.
        /// When the interval is not joined, it is "invisible".
        /// </summary>
        [DataMember]
        private FastMap<LeftEvent> leftEdgeMap = new FastMap<LeftEvent>();

        /// <summary>
        /// Stores left end edges at some point in the future, i.e. after <see cref="currTime"/>.
        /// These can originate from edge end events or interval events.
        /// </summary>
        [DataMember]
        private IEndPointOrderer leftEndPointHeap;

        /// <summary>
        /// Stores the right events present at <see cref="currTime"/>
        /// </summary>
        [DataMember]
        private FastMap<RightEvent> rightMap = new FastMap<RightEvent>();

        /// <summary>
        /// Stores right end edges for <see cref="currTime"/>, excluding interval endpoints
        /// </summary>
        [DataMember]
        private FastStack<QueuedEndEdge> rightEndEdges = new FastStack<QueuedEndEdge>();

        /// <summary>
        /// Stores right endpoints at some point in the future, i.e. after <see cref="currTime"/>, originating
        /// from intervals.
        /// </summary>
        [DataMember]
        private IEndPointOrderer rightEndPointHeap;

        [DataMember]
        private long nextLeftTime = long.MinValue;
        [DataMember]
        private long nextRightTime = long.MinValue;
        [DataMember]
        private long currTime = long.MinValue;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public LeftAntiSemiJoinPipe() { }

        public LeftAntiSemiJoinPipe(LeftAntiSemiJoinStreamable<TKey, TLeft, TRight> stream, IStreamObserver<TKey, TLeft> observer)
            : base(stream, observer)
        {
            this.keyComparer = stream.Properties.KeyEqualityComparer.GetEqualsExpr();
            this.keyComparerEquals = this.keyComparer.Compile();

            this.leftComparer = stream.LeftComparer.GetEqualsExpr();
            this.leftComparerEquals = this.leftComparer.Compile();

            if (stream.Left.Properties.IsConstantDuration)
            {
                this.leftEndPointHeap = new EndPointQueue();
            }
            else
            {
                this.leftEndPointHeap = new EndPointHeap();
            }
            if (stream.Right.Properties.IsConstantDuration)
            {
                this.rightEndPointHeap = new EndPointQueue();
            }
            else
            {
                this.rightEndPointHeap = new EndPointHeap();
            }

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

        protected override void DisposeState() => this.output.Free();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override void ProcessBothBatches(StreamMessage<TKey, TLeft> leftBatch, StreamMessage<TKey, TRight> rightBatch, out bool leftBatchDone, out bool rightBatchDone, out bool leftBatchFree, out bool rightBatchFree)
        {
            leftBatchFree = rightBatchFree = true;
            if (!GoToVisibleRow(leftBatch))
            {
                leftBatchDone = true;
                rightBatchDone = false;
                return;
            }

            this.nextLeftTime = leftBatch.vsync.col[leftBatch.iter];

            if (!GoToVisibleRow(rightBatch))
            {
                leftBatchDone = false;
                rightBatchDone = true;
                return;
            }

            this.nextRightTime = rightBatch.vsync.col[rightBatch.iter];

            while (true)
            {
                if (this.nextLeftTime <= this.nextRightTime)
                {
                    UpdateTime(this.nextLeftTime);

                    ProcessLeftEvent(
                        this.nextLeftTime,
                        leftBatch.vother.col[leftBatch.iter],
                        ref leftBatch.key.col[leftBatch.iter],
                        leftBatch[leftBatch.iter],
                        leftBatch.hash.col[leftBatch.iter]);

                    leftBatch.iter++;

                    if (!GoToVisibleRow(leftBatch))
                    {
                        leftBatchDone = true;
                        rightBatchDone = false;
                        return;
                    }

                    this.nextLeftTime = leftBatch.vsync.col[leftBatch.iter];
                }
                else
                {
                    UpdateTime(this.nextRightTime);

                    ProcessRightEvent(
                        this.nextRightTime,
                        rightBatch.vother.col[rightBatch.iter],
                        ref rightBatch.key.col[rightBatch.iter],
                        rightBatch.hash.col[rightBatch.iter]);

                    rightBatch.iter++;

                    if (!GoToVisibleRow(rightBatch))
                    {
                        leftBatchDone = false;
                        rightBatchDone = true;
                        return;
                    }

                    this.nextRightTime = rightBatch.vsync.col[rightBatch.iter];
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override void ProcessLeftBatch(StreamMessage<TKey, TLeft> batch, out bool isBatchDone, out bool isBatchFree)
        {
            isBatchFree = true;
            while (true)
            {
                if (!GoToVisibleRow(batch))
                {
                    isBatchDone = true;
                    return;
                }

                this.nextLeftTime = batch.vsync.col[batch.iter];

                if (this.nextLeftTime > this.nextRightTime)
                {
                    isBatchDone = false;
                    return;
                }

                UpdateTime(this.nextLeftTime);

                ProcessLeftEvent(
                    this.nextLeftTime,
                    batch.vother.col[batch.iter],
                    ref batch.key.col[batch.iter],
                    batch[batch.iter],
                    batch.hash.col[batch.iter]);

                batch.iter++;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override void ProcessRightBatch(StreamMessage<TKey, TRight> batch, out bool isBatchDone, out bool isBatchFree)
        {
            isBatchFree = true;
            while (true)
            {
                if (!GoToVisibleRow(batch))
                {
                    isBatchDone = true;
                    return;
                }

                this.nextRightTime = batch.vsync.col[batch.iter];

                if (this.nextRightTime > this.nextLeftTime)
                {
                    isBatchDone = false;
                    return;
                }

                UpdateTime(this.nextRightTime);

                ProcessRightEvent(
                    this.nextRightTime,
                    batch.vother.col[batch.iter],
                    ref batch.key.col[batch.iter],
                    batch.hash.col[batch.iter]);

                batch.iter++;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool GoToVisibleRow<TPayload>(StreamMessage<TKey, TPayload> batch)
        {
            while (batch.iter < batch.Count && (batch.bitvector.col[batch.iter >> 6] & (1L << (batch.iter & 0x3f))) != 0 && batch.vother.col[batch.iter] >= 0)
            {
                batch.iter++;
            }

            return batch.iter != batch.Count;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateTime(long time)
        {
            if (time != this.currTime)
            {
                LeaveTime();
                this.currTime = time;
                ReachTime();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessLeftEvent(long start, long end, ref TKey key, TLeft payload, int hash)
        {
            if (start < end)
            {
                // Row is a start edge or interval.
                bool isProcessable = this.nextRightTime > start;
                bool isInterval = end < StreamEvent.InfinitySyncTime;
                var map = isInterval ? this.leftIntervalMap : this.leftEdgeMap;
                if (isProcessable)
                {
                    if (FindOnRight(ref key, hash, out _))
                    {
                        // Row joins with something on right, so not currently visible.
                        int index = map.Insert(hash);
                        map.Values[index].Populate(start, NotActive, end, ref key, ref payload);

                        if (isInterval)
                        {
                            this.leftEndPointHeap.Insert(end, index);
                        }
                    }
                    else
                    {
                        // Row does not join (and it is processable).
                        bool isFullyOutputtable = isInterval && this.nextRightTime >= end;
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
                            if (isInterval)
                            {
                                this.leftEndPointHeap.Insert(end, index);
                            }
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
            else if (end == StreamEvent.PunctuationOtherTime)
            {
                AddPunctuationToBatch(start);
            }
            else
            {
                // Row is an end edge.
                // Remove from leftEdgeMap.
                var leftEvents = this.leftEdgeMap.Find(hash);
                while (leftEvents.Next(out int index))
                {
                    var temp = this.leftEdgeMap.Values[index];
                    if (AreSame(end, StreamEvent.InfinitySyncTime, ref key, ref payload, ref temp))
                    {
                        long currentStart = this.leftEdgeMap.Values[index].CurrentStart;
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
        private void ProcessRightEvent(long start, long end, ref TKey key, int hash)
        {
            if (start < end)
            {
                // Row is a start edge or interval.
                if (FindOnRight(ref key, hash, out int index))
                {
                    // Corresponding key already exists in map, so any joining on left and already not active.
                    this.rightMap.Values[index].Count++;
                }
                else
                {
                    // First instance of this key, so insert and make any joining left entries not active.
                    index = this.rightMap.Insert(hash);
                    this.rightMap.Values[index].Initialize(ref key);
                    MakeMatchingLeftInvisible(start, ref key, hash);
                }

                if (end != StreamEvent.InfinitySyncTime)
                {
                    // Row is an interval, so schedule removal of interval.
                    this.rightEndPointHeap.Insert(end, index);
                }
            }
            else if (end == StreamEvent.PunctuationOtherTime)
            {
                AddPunctuationToBatch(start);
            }
            else
            {
                // Row is an end edge.

                // Queue for removal when time advances.
                int index = this.rightEndEdges.Push();
                this.rightEndEdges.Values[index].Populate(ref key, hash);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void LeaveTime()
        {
            // Carry-out all queued end edges for right events.
            int hash;
            for (int i = 0; i < this.rightEndEdges.Count; i++)
            {

                hash = this.rightEndEdges.Values[i].Hash;
                var keyTemp = this.rightEndEdges.Values[i].Key;
                if (FindOnRight(ref keyTemp, hash, out int index))
                {
                    int count = this.rightMap.Values[index].Count - 1;
                    if (count > 0)
                        this.rightMap.Values[index].Count = count;
                    else
                    {
                        var key = this.rightMap.Values[index];
                        MakeMatchingLeftVisible(this.currTime, ref key.Key, hash);
                        this.rightMap.Remove(index);
                    }
                }
            }

            this.rightEndEdges.Clear();

            // Actually insert all pending left start intervals.
            var leftEvents = this.leftIntervalMap.TraverseInvisible();
            while (leftEvents.Next(out int index, out hash))
            {
                var leftIntervalItem = this.leftIntervalMap.Values[index];
                long end = leftIntervalItem.End;
                if (FindOnRight(ref this.leftIntervalMap.Values[index].Key, hash, out _))
                {
                    leftEvents.MakeVisible();
                    this.leftEndPointHeap.Insert(end, index);
                }
                else
                {
                    // Row does not join.
                    bool isFullyOutputtable = this.nextRightTime >= end;
                    if (isFullyOutputtable)
                    {
                        AddToBatch(
                            this.currTime,
                            end,
                            ref leftIntervalItem.Key,
                            ref leftIntervalItem.Payload,
                            hash);
                        leftEvents.Remove();

                    }
                    else
                    {
                        leftEvents.MakeVisible();
                        this.leftIntervalMap.Values[index].CurrentStart = this.currTime;
                        AddToBatch(
                            this.currTime,
                            StreamEvent.InfinitySyncTime,
                            ref leftIntervalItem.Key,
                            ref leftIntervalItem.Payload,
                            hash);

                        this.leftEndPointHeap.Insert(end, index);
                    }
                }
            }

            // Actually insert all pending left start edges.
            leftEvents = this.leftEdgeMap.TraverseInvisible();
            while (leftEvents.Next(out int index, out hash))
            {
                if (!FindOnRight(ref this.leftEdgeMap.Values[index].Key, hash, out _))
                {
                    // Row does not join, so output start edge.
                    var leftEdgeItem = this.leftEdgeMap.Values[index];
                    this.leftEdgeMap.Values[index].CurrentStart = this.currTime;
                    AddToBatch(
                        this.currTime,
                        StreamEvent.InfinitySyncTime,
                        ref leftEdgeItem.Key,
                        ref leftEdgeItem.Payload,
                    hash);
                }
                leftEvents.MakeVisible();

            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReachTime()
        {
            // Carry-out all interval endpoints for left intervals that end prior to (or at) new current time.
            if (!this.leftEndPointHeap.TryPeekNext(out long leftTime, out int leftIndex))
            {
                leftTime = long.MaxValue;
            }

            if (!this.rightEndPointHeap.TryPeekNext(out long rightTime, out int rightIndex))
            {
                rightTime = long.MaxValue;
            }

            while (leftTime <= this.currTime || rightTime < this.currTime)
            {
                if (leftTime <= rightTime)
                {
                    // Always process left end-points first as they may end intervals that right end-points may
                    // try to make visible.
                    var leftIntervalItem = this.leftIntervalMap.Values[leftIndex];
                    long currentStart = leftIntervalItem.CurrentStart;
                    if (currentStart != NotActive)
                    {
                        // Matching left start edge is currently visible, so output end edge.
                        AddToBatch(
                            leftTime,
                            currentStart,
                            ref leftIntervalItem.Key,
                            ref leftIntervalItem.Payload, this.leftIntervalMap.GetHash(leftIndex));
                    }

                    this.leftIntervalMap.Remove(leftIndex);

                    this.leftEndPointHeap.RemoveTop();
                    if (!this.leftEndPointHeap.TryPeekNext(out leftTime, out leftIndex))
                    {
                        leftTime = long.MaxValue;
                    }
                }
                else
                {
                    // Process right end-point up to but not including the current time.
                    int count = this.rightMap.Values[rightIndex].Count - 1;
                    if (count > 0)
                        this.rightMap.Values[rightIndex].Count = count;
                    else
                    {
                        var key = this.rightMap.Values[rightIndex].Key;
                        MakeMatchingLeftVisible(rightTime, ref key, this.rightMap.GetHash(rightIndex));
                        this.rightMap.Remove(rightIndex);
                    }

                    this.rightEndPointHeap.RemoveTop();
                    if (!this.rightEndPointHeap.TryPeekNext(out rightTime, out rightIndex))
                    {
                        rightTime = long.MaxValue;
                    }
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool FindOnRight(ref TKey key, int hash, out int index)
        {
            var rightEvents = this.rightMap.Find(hash);
            while (rightEvents.Next(out int rightIndex))
            {
                if (this.keyComparerEquals(key, this.rightMap.Values[rightIndex].Key))
                {
                    index = rightIndex;
                    return true;
                }
            }

            index = 0;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void MakeMatchingLeftInvisible(long time, ref TKey key, int hash)
        {
            // Make matching left intervals invisible.
            int index;
            var leftEvents = this.leftIntervalMap.Find(hash);
            while (leftEvents.Next(out index))
            {
                if (this.keyComparerEquals(key, this.leftIntervalMap.Values[index].Key))
                {
                    // Output end edge.
                    var leftIntervalItem = this.leftIntervalMap.Values[index];
                    AddToBatch(
                        time,
                        leftIntervalItem.CurrentStart,
                        ref leftIntervalItem.Key,
                        ref leftIntervalItem.Payload,
                        hash);

                    // Mark left event as not visible.
                    this.leftIntervalMap.Values[index].CurrentStart = NotActive;
                }
            }

            // Make matching left edges invisible.
            leftEvents = this.leftEdgeMap.Find(hash);
            while (leftEvents.Next(out index))
            {
                if (this.keyComparerEquals(key, this.leftEdgeMap.Values[index].Key))
                {
                    // Output end edge.
                    var leftEdgeItem = this.leftEdgeMap.Values[index];
                    AddToBatch(
                        time,
                        leftEdgeItem.CurrentStart,
                        ref leftEdgeItem.Key,
                        ref leftEdgeItem.Payload,
                        hash);

                    // Mark left event as not visible.
                    this.leftEdgeMap.Values[index].CurrentStart = NotActive;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void MakeMatchingLeftVisible(long time, ref TKey key, int hash)
        {
            // Make matching left intervals visible.
            int index;
            var leftEvents = this.leftIntervalMap.Find(hash);
            while (leftEvents.Next(out index))
            {
                var leftIntervalItem = this.leftIntervalMap.Values[index];
                if (this.keyComparerEquals(key, leftIntervalItem.Key))
                {
                    long end = leftIntervalItem.End;
                    bool isFullyOutputtable = this.nextRightTime >= end;
                    if (isFullyOutputtable)
                    {
                        // Output interval.
                        AddToBatch(
                            time,
                            end,
                            ref leftIntervalItem.Key,
                            ref leftIntervalItem.Payload,
                            hash);

                        // Mark left event as not visible so that an end-edge is not outputted when the interval actually endss.
                        this.leftIntervalMap.Values[index].CurrentStart = NotActive;
                    }
                    else
                    {
                        // Output start edge.
                        AddToBatch(
                            time,
                            StreamEvent.InfinitySyncTime,
                            ref leftIntervalItem.Key,
                            ref leftIntervalItem.Payload,
                            hash);

                        // Mark left event as visible.
                        this.leftIntervalMap.Values[index].CurrentStart = time;
                    }
                }
            }

            // Make matching left edges visible.
            leftEvents = this.leftEdgeMap.Find(hash);
            while (leftEvents.Next(out index))
            {
                var leftEdgeItem = this.leftEdgeMap.Values[index];
                if (this.keyComparerEquals(key, leftEdgeItem.Key))
                {
                    // Output start edge.
                    AddToBatch(
                        time,
                        StreamEvent.InfinitySyncTime,
                        ref leftEdgeItem.Key,
                        ref leftEdgeItem.Payload,
                        hash);

                    // Mark left event as visible.
                    this.leftEdgeMap.Values[index].CurrentStart = time;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AddPunctuationToBatch(long start)
        {
            if (start > this.lastCTI)
            {
                this.lastCTI = start;

                int index = this.output.Count++;
                this.output.vsync.col[index] = start;
                this.output.vother.col[index] = StreamEvent.PunctuationOtherTime;
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
        public override int CurrentlyBufferedLeftInputCount => base.CurrentlyBufferedLeftInputCount + this.leftEdgeMap.Count + this.leftIntervalMap.Count;
        public override int CurrentlyBufferedRightInputCount => base.CurrentlyBufferedRightInputCount + this.rightEndEdges.Count + this.rightMap.Count;

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

            public override string ToString() => "[Key='" + this.Key + "', Count=" + this.Count + "]";
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

            public override string ToString() => "[Key='" + this.Key + "', Hash=" + this.Hash + "]";
        }
    }
}
