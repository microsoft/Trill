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
    internal sealed class EquiJoinPipe<TKey, TLeft, TRight, TResult> : BinaryPipe<TKey, TLeft, TRight, TResult>
    {
        private readonly Func<TLeft, TRight, TResult> selector;
        private readonly MemoryPool<TKey, TResult> pool;

        [SchemaSerialization]
        private readonly Expression<Func<TKey, TKey, bool>> keyComparer;
        private readonly Func<TKey, TKey, bool> keyComparerEquals;
        [SchemaSerialization]
        private readonly Expression<Func<TLeft, TLeft, bool>> leftComparer;
        private readonly Func<TLeft, TLeft, bool> leftComparerEquals;
        [SchemaSerialization]
        private readonly Expression<Func<TRight, TRight, bool>> rightComparer;
        private readonly Func<TRight, TRight, bool> rightComparerEquals;

        [DataMember]
        private StreamMessage<TKey, TResult> output;

        /// <summary>
        /// Stores left intervals starting at <see cref="currTime"/>.
        /// FastMap visibility means that <see cref="nextRightTime"/> is caught up to the left edge time and the
        /// left edge is processable.
        /// </summary>
        [DataMember]
        private FastMap<ActiveInterval<TLeft>> leftIntervalMap = new FastMap<ActiveInterval<TLeft>>();

        /// <summary>
        /// Stores left start edges at <see cref="currTime"/>
        /// FastMap visibility means that <see cref="nextRightTime"/> is caught up to the left edge time and the
        /// left edge is processable.
        /// </summary>
        [DataMember]
        private FastMap<ActiveEdge<TLeft>> leftEdgeMap = new FastMap<ActiveEdge<TLeft>>();

        /// <summary>
        /// Stores end edges for the current join at some point in the future, i.e. after <see cref="currTime"/>.
        /// These can originate from edge end events or interval events.
        /// </summary>
        [DataMember]
        private IEndPointOrderer endPointHeap;

        /// <summary>
        /// Stores right intervals starting at <see cref="currTime"/>.
        /// FastMap visibility means that <see cref="nextRightTime"/> is caught up to the right edge time and the
        /// right edge is processable.
        /// </summary>
        [DataMember]
        private FastMap<ActiveInterval<TRight>> rightIntervalMap = new FastMap<ActiveInterval<TRight>>();

        /// <summary>
        /// Stores right start edges at <see cref="currTime"/>
        /// FastMap visibility means that <see cref="nextRightTime"/> is caught up to the right edge time and the
        /// right edge is processable.
        /// </summary>
        [DataMember]
        private FastMap<ActiveEdge<TRight>> rightEdgeMap = new FastMap<ActiveEdge<TRight>>();

        [DataMember]
        private long nextLeftTime = long.MinValue;
        [DataMember]
        private bool isLeftComplete;
        [DataMember]
        private long nextRightTime = long.MinValue;
        [DataMember]
        private bool isRightComplete;
        [DataMember]
        private long currTime = long.MinValue;
        [Obsolete("Used only by serialization. Do not call directly.")]
        public EquiJoinPipe() { }

        public EquiJoinPipe(
            BinaryStreamable<TKey, TLeft, TRight, TResult> stream,
            Expression<Func<TLeft, TRight, TResult>> selector,
            IStreamObserver<TKey, TResult> observer)
            : base(stream, observer)
        {
            this.selector = selector.Compile();

            this.keyComparer = stream.Properties.KeyEqualityComparer.GetEqualsExpr();
            this.keyComparerEquals = this.keyComparer.Compile();

            this.leftComparer = stream.Left.Properties.PayloadEqualityComparer.GetEqualsExpr();
            this.leftComparerEquals = this.leftComparer.Compile();

            this.rightComparer = stream.Right.Properties.PayloadEqualityComparer.GetEqualsExpr();
            this.rightComparerEquals = this.rightComparer.Compile();

            if (stream.Left.Properties.IsIntervalFree && stream.Right.Properties.IsConstantDuration)
            {
                this.endPointHeap = new EndPointQueue();
            }
            else if (stream.Right.Properties.IsIntervalFree && stream.Left.Properties.IsConstantDuration)
            {
                this.endPointHeap = new EndPointQueue();
            }
            else if (stream.Left.Properties.IsConstantDuration && stream.Right.Properties.IsConstantDuration &&
                     stream.Left.Properties.ConstantDurationLength == stream.Right.Properties.ConstantDurationLength)
            {
                this.endPointHeap = new EndPointQueue();
            }
            else
            {
                this.endPointHeap = new EndPointHeap();
            }

            this.pool = MemoryManager.GetMemoryPool<TKey, TResult>(stream.Properties.IsColumnar);
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        protected override void ProduceBinaryQueryPlan(PlanNode left, PlanNode right)
        {
            var node = new JoinPlanNode(
                left, right, this,
                typeof(TLeft), typeof(TRight), typeof(TLeft), typeof(TKey),
                JoinKind.EquiJoin,
                false, null);
            node.AddJoinExpression("key comparer", this.keyComparer);
            node.AddJoinExpression("left key comparer", this.leftComparer);
            node.AddJoinExpression("right key comparer", this.rightComparer);
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

            UpdateNextLeftTime(leftBatch.vsync.col[leftBatch.iter]);
            if (!GoToVisibleRow(rightBatch))
            {
                leftBatchDone = false;
                rightBatchDone = true;
                return;
            }

            UpdateNextRightTime(rightBatch.vsync.col[rightBatch.iter]);

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

                    UpdateNextLeftTime(leftBatch.vsync.col[leftBatch.iter]);
                }
                else
                {
                    UpdateTime(this.nextRightTime);
                    ProcessRightEvent(
                        this.nextRightTime,
                        rightBatch.vother.col[rightBatch.iter],
                        ref rightBatch.key.col[rightBatch.iter],
                        rightBatch[rightBatch.iter],
                        rightBatch.hash.col[rightBatch.iter]);

                    rightBatch.iter++;

                    if (!GoToVisibleRow(rightBatch))
                    {
                        leftBatchDone = false;
                        rightBatchDone = true;
                        return;
                    }

                    UpdateNextRightTime(rightBatch.vsync.col[rightBatch.iter]);
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

                UpdateNextLeftTime(batch.vsync.col[batch.iter]);

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

                UpdateNextRightTime(batch.vsync.col[batch.iter]);

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
                    batch[batch.iter],
                    batch.hash.col[batch.iter]);

                batch.iter++;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool GoToVisibleRow<TPayload>(StreamMessage<TKey, TPayload> batch)
        {
            while (batch.iter < batch.Count && (batch.bitvector.col[batch.iter >> 6] & (1L << (batch.iter & 0x3f))) != 0 && batch.vother.col[batch.iter] >= 0)
                batch.iter++;

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
        private void UpdateNextLeftTime(long time)
        {
            this.nextLeftTime = time;
            if (this.nextLeftTime == StreamEvent.InfinitySyncTime && this.leftEdgeMap.IsInvisibleEmpty && this.leftIntervalMap.IsInvisibleEmpty && this.leftIntervalMap.IsEmpty)
            {
                this.isLeftComplete = true;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateNextRightTime(long time)
        {
            this.nextRightTime = time;
            if (this.nextRightTime == StreamEvent.InfinitySyncTime && this.rightEdgeMap.IsInvisibleEmpty && this.rightIntervalMap.IsInvisibleEmpty && this.rightIntervalMap.IsEmpty)
            {
                this.isRightComplete = true;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessLeftEvent(long start, long end, ref TKey key, TLeft payload, int hash)
        {
            if (start < end)
            {
                // Row is a start edge or interval.
                bool processable = this.nextRightTime > start || this.rightEdgeMap.IsEmpty;
                if (end == StreamEvent.InfinitySyncTime)
                {
                    // Row is a start edge.
                    if (processable)
                    {
                        if (!this.isRightComplete)
                        {
                            int index = this.leftEdgeMap.Insert(hash);
                            this.leftEdgeMap.Values[index].Populate(start, ref key, ref payload);
                        }

                        CreateOutputForStartEdge(start, ref key, ref payload, hash);
                    }
                    else
                    {
                        int index = this.leftEdgeMap.InsertInvisible(hash);
                        this.leftEdgeMap.Values[index].Populate(start, ref key, ref payload);
                    }
                }
                else
                {
                    // Row is an interval.
                    if (processable)
                    {
                        int index = this.leftIntervalMap.Insert(hash);
                        this.leftIntervalMap.Values[index].Populate(start, end, ref key, ref payload);
                        CreateOutputForStartInterval(start, end, ref key, ref payload, hash);
                        this.endPointHeap.Insert(end, index);
                    }
                    else
                    {
                        int index = this.leftIntervalMap.InsertInvisible(hash);
                        this.leftIntervalMap.Values[index].Populate(start, end, ref key, ref payload);
                    }
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
                if (!this.isRightComplete)
                {
                    var edges = this.leftEdgeMap.Find(hash);
                    while (edges.Next(out int index))
                    {
                        var temp = this.leftEdgeMap.Values[index];
                        if (AreSame(end, ref key, ref payload, ref temp))
                        {
                            edges.Remove();
                            break;
                        }
                    }
                }

                // Output end edges.
                CreateOutputForEndEdge(start, end, ref key, ref payload, hash);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessRightEvent(long start, long end, ref TKey key, TRight payload, int hash)
        {
            if (start < end)
            {
                // Row is a start edge or interval.
                bool processable = this.nextLeftTime > start || this.leftEdgeMap.IsEmpty;
                if (end == StreamEvent.InfinitySyncTime)
                {
                    // Row is a start edge.
                    if (processable)
                    {
                        if (!this.isLeftComplete)
                        {
                            int index = this.rightEdgeMap.Insert(hash);
                            this.rightEdgeMap.Values[index].Populate(start, ref key, ref payload);
                        }

                        CreateOutputForStartEdge(start, ref key, ref payload, hash);
                    }
                    else
                    {
                        int index = this.rightEdgeMap.InsertInvisible(hash);
                        this.rightEdgeMap.Values[index].Populate(start, ref key, ref payload);

                    }
                }
                else
                {
                    // Row is an interval.
                    if (processable)
                    {
                        int index = this.rightIntervalMap.Insert(hash);
                        this.rightIntervalMap.Values[index].Populate(start, end, ref key, ref payload);

                        CreateOutputForStartInterval(start, end, ref key, ref payload, hash);
                        this.endPointHeap.Insert(end, ~index);
                    }
                    else
                    {
                        int index = this.rightIntervalMap.InsertInvisible(hash);
                        this.rightIntervalMap.Values[index].Populate(start, end, ref key, ref payload);
                    }
                }
            }
            else if (end == StreamEvent.PunctuationOtherTime)
            {
                AddPunctuationToBatch(start);
            }
            else
            {
                // Row is an end edge.

                // Remove from rightEdgeMap.
                if (!this.isLeftComplete)
                {
                    var edges = this.rightEdgeMap.Find(hash);
                    while (edges.Next(out int index))
                    {
                        var temp = this.rightEdgeMap.Values[index];
                        if (AreSame(end, ref key, ref payload, ref temp))
                        {

                            edges.Remove();
                            break;
                        }
                    }
                }

                // Output end edges.
                CreateOutputForEndEdge(start, end, ref key, ref payload, hash);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void LeaveTime()
        {

            int hash;
            int index;
            var leftEdges = this.leftEdgeMap.TraverseInvisible();
            while (leftEdges.Next(out index, out hash))
            {

                var ed = this.leftEdgeMap.Values[index];
                CreateOutputForStartEdge(
                    this.currTime,
                    ref ed.Key,
                    ref ed.Payload,
                    hash);

                leftEdges.MakeVisible();
            }

            var leftIntervals = this.leftIntervalMap.TraverseInvisible();
            while (leftIntervals.Next(out index, out hash))
            {

                var intrvl = this.leftIntervalMap.Values[index];
                long end = intrvl.End;
                CreateOutputForStartInterval(
                    this.currTime,
                    end,
                    ref intrvl.Key,
                    ref intrvl.Payload,
                    hash);

                leftIntervals.MakeVisible();
                this.endPointHeap.Insert(end, index);
            }

            var rightEdges = this.rightEdgeMap.TraverseInvisible();
            while (rightEdges.Next(out index, out hash))
            {
                var ed = this.rightEdgeMap.Values[index];
                CreateOutputForStartEdge(
                    this.currTime,
                    ref ed.Key,
                    ref ed.Payload,
                    hash);

                rightEdges.MakeVisible();
            }

            var rightIntervals = this.rightIntervalMap.TraverseInvisible();
            while (rightIntervals.Next(out index, out hash))
            {
                var intrvl = this.rightIntervalMap.Values[index];
                long end = intrvl.End;
                CreateOutputForStartInterval(
                    this.currTime,
                    end,
                    ref intrvl.Key,
                    ref intrvl.Payload,
                    hash);

                rightIntervals.MakeVisible();
                this.endPointHeap.Insert(end, ~index);
            }

            if (this.nextLeftTime == StreamEvent.InfinitySyncTime && this.leftIntervalMap.IsEmpty)
            {
                this.isLeftComplete = true;
            }

            if (this.nextRightTime == StreamEvent.InfinitySyncTime && this.rightIntervalMap.IsEmpty)
            {
                this.isRightComplete = true;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReachTime()
        {
            while (this.endPointHeap.TryGetNextInclusive(this.currTime, out long endPointTime, out int index))
            {
                if (index >= 0)
                {
                    // Endpoint is left interval ending.
                    var intrvl = this.leftIntervalMap.Values[index];
                    CreateOutputForEndInterval(
                        endPointTime,
                        intrvl.Start,
                        ref intrvl.Key,
                        ref intrvl.Payload, this.leftIntervalMap.GetHash(index));

                    this.leftIntervalMap.Remove(index);
                }
                else
                {
                    // Endpoint is right interval ending.
                    index = ~index;
                    var intrvl = this.rightIntervalMap.Values[index];

                    CreateOutputForEndInterval(
                        endPointTime,
                        intrvl.Start,
                        ref intrvl.Key,
                        ref intrvl.Payload, this.rightIntervalMap.GetHash(index));
                    this.rightIntervalMap.Remove(index);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CreateOutputForEndEdge(long currentTime, long start, ref TKey key, ref TLeft payload, int hash)
        {
            // Create end edges for all joined right edges.
            int index;
            var edges = this.rightEdgeMap.Find(hash);
            while (edges.Next(out index))
            {
                var ed = this.rightEdgeMap.Values[index];
                if (this.keyComparerEquals(key, ed.Key))
                {
                    long rightStart = ed.Start;
                    AddToBatch(
                        currentTime,
                        start > rightStart ? start : rightStart,
                        ref key,
                        ref payload,
                        ref ed.Payload,
                        hash);
                }
            }

            // Create end edges for all joined right intervals.
            var intervals = this.rightIntervalMap.Find(hash);
            while (intervals.Next(out index))
            {
                var ed = this.rightIntervalMap.Values[index];
                if (this.keyComparerEquals(key, ed.Key))
                {
                    long rightStart = ed.Start;
                    AddToBatch(
                        currentTime,
                        start > rightStart ? start : rightStart,
                        ref key,
                        ref payload,
                        ref ed.Payload,
                        hash);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CreateOutputForEndEdge(long currentTime, long start, ref TKey key, ref TRight payload, int hash)
        {
            // Create end edges for all joined left edges.
            int index;
            var edges = this.leftEdgeMap.Find(hash);
            while (edges.Next(out index))
            {
                var ld = this.leftEdgeMap.Values[index];
                if (this.keyComparerEquals(key, ld.Key))
                {
                    long leftStart = ld.Start;
                    AddToBatch(
                        currentTime,
                        start > leftStart ? start : leftStart,
                        ref key,
                        ref ld.Payload,
                        ref payload,
                        hash);
                }
            }

            // Create end edges for all joined left intervals.
            var intervals = this.leftIntervalMap.Find(hash);
            while (intervals.Next(out index))
            {
                var li = this.leftIntervalMap.Values[index];
                if (this.keyComparerEquals(key, li.Key))
                {
                    long leftStart = li.Start;
                    AddToBatch(
                        currentTime,
                        start > leftStart ? start : leftStart,
                        ref key,
                        ref li.Payload,
                        ref payload,
                        hash);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CreateOutputForStartEdge(long currentTime, ref TKey key, ref TLeft payload, int hash)
        {
            // Create end edges for all joined right edges.
            var edges = this.rightEdgeMap.Find(hash);
            int index;
            while (edges.Next(out index))
            {
                var red = this.rightEdgeMap.Values[index];
                if (this.keyComparerEquals(key, red.Key))
                {
                    AddToBatch(
                        currentTime,
                        StreamEvent.InfinitySyncTime,
                        ref key,
                        ref payload,
                        ref red.Payload,
                        hash);
                }
            }

            // Create end edges for all joined right intervals.
            var intervals = this.rightIntervalMap.Find(hash);
            while (intervals.Next(out index))
            {
                var rin = this.rightIntervalMap.Values[index];
                if (this.keyComparerEquals(key, rin.Key))
                {
                    AddToBatch(
                        currentTime,
                        StreamEvent.InfinitySyncTime,
                        ref key,
                        ref payload,
                        ref rin.Payload,
                        hash);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CreateOutputForStartEdge(long currentTime, ref TKey key, ref TRight payload, int hash)
        {
            // Create end edges for all joined left edges.
            var edges = this.leftEdgeMap.Find(hash);
            int index;
            while (edges.Next(out index))
            {
                var led = this.leftEdgeMap.Values[index];
                if (this.keyComparerEquals(key, led.Key))
                {
                    AddToBatch(
                        currentTime,
                        StreamEvent.InfinitySyncTime,
                        ref key,
                        ref led.Payload,
                        ref payload,
                        hash);
                }
            }

            // Create end edges for all joined left intervals.
            var intervals = this.leftIntervalMap.Find(hash);
            while (intervals.Next(out index))
            {
                var lin = this.leftIntervalMap.Values[index];
                if (this.keyComparerEquals(key, lin.Key))
                {
                    AddToBatch(
                        currentTime,
                        StreamEvent.InfinitySyncTime,
                        ref key,
                        ref lin.Payload,
                        ref payload,
                        hash);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CreateOutputForStartInterval(long currentTime, long end, ref TKey key, ref TLeft payload, int hash)
        {
            // Create end edges for all joined right edges.
            var edges = this.rightEdgeMap.Find(hash);
            int index;
            while (edges.Next(out index))
            {
                var red = this.rightEdgeMap.Values[index];
                if (this.keyComparerEquals(key, red.Key))
                {
                    AddToBatch(
                        currentTime,
                        StreamEvent.InfinitySyncTime,
                        ref key,
                        ref payload,
                        ref red.Payload,
                        hash);
                }
            }

            // Create end edges for all joined right intervals.
            var intervals = this.rightIntervalMap.Find(hash);
            while (intervals.Next(out index))
            {
                var rin = this.rightIntervalMap.Values[index];
                if (this.keyComparerEquals(key, rin.Key))
                {
                    long rightEnd = rin.End;
                    AddToBatch(
                        currentTime,
                        end < rightEnd ? end : rightEnd,
                        ref key,
                        ref payload,
                        ref rin.Payload,
                        hash);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CreateOutputForStartInterval(long currentTime, long end, ref TKey key, ref TRight payload, int hash)
        {
            // Create end edges for all joined left edges.
            var edges = this.leftEdgeMap.Find(hash);
            int index;
            while (edges.Next(out index))
            {
                var led = this.leftEdgeMap.Values[index];
                if (this.keyComparerEquals(key, led.Key))
                {
                    AddToBatch(
                        currentTime,
                        StreamEvent.InfinitySyncTime,
                        ref key,
                        ref led.Payload,
                        ref payload,
                        hash);
                }
            }

            // Create end edges for all joined left intervals.
            var intervals = this.leftIntervalMap.Find(hash);
            while (intervals.Next(out index))
            {
                var lin = this.leftIntervalMap.Values[index];
                if (this.keyComparerEquals(key, lin.Key))
                {
                    long leftEnd = lin.End;
                    AddToBatch(
                        currentTime,
                        end < leftEnd ? end : leftEnd,
                        ref key,
                        ref lin.Payload,
                        ref payload,
                        hash);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CreateOutputForEndInterval(long currentTime, long start, ref TKey key, ref TLeft payload, int hash)
        {
            // Create end edges for all joined right edges.
            var edges = this.rightEdgeMap.Find(hash);
            while (edges.Next(out int index))
            {
                var red = this.rightEdgeMap.Values[index];
                if (this.keyComparerEquals(key, red.Key))
                {
                    long rightStart = red.Start;
                    AddToBatch(
                        currentTime,
                        start > rightStart ? start : rightStart,
                        ref key,
                        ref payload,
                        ref red.Payload,
                        hash);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CreateOutputForEndInterval(long currentTime, long start, ref TKey key, ref TRight payload, int hash)
        {
            // Create end edges for all joined left edges.
            var edges = this.leftEdgeMap.Find(hash);
            while (edges.Next(out int index))
            {
                var led = this.leftEdgeMap.Values[index];
                if (this.keyComparerEquals(key, led.Key))
                {
                    long leftStart = led.Start;
                    AddToBatch(
                        currentTime,
                        start > leftStart ? start : leftStart,
                        ref key,
                        ref led.Payload,
                        ref payload,
                        hash);
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
                this.output.bitvector.col[index >> 6] |= (1L << (index & 0x3f));

                if (this.output.Count == Config.DataBatchSize) FlushContents();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AddToBatch(long start, long end, ref TKey key, ref TLeft leftPayload, ref TRight rightPayload, int hash)
        {
            if (start < this.lastCTI)
            {
                throw new StreamProcessingOutOfOrderException("Outputting an event out of order!");
            }

            int index = this.output.Count++;
            this.output.vsync.col[index] = start;
            this.output.vother.col[index] = end;
            this.output.key.col[index] = key;
            this.output[index] = this.selector(leftPayload, rightPayload);
            this.output.hash.col[index] = hash;

            if (this.output.Count == Config.DataBatchSize) FlushContents();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool AreSame(long start, ref TKey key, ref TLeft payload, ref ActiveEdge<TLeft> active)
            => start == active.Start && this.keyComparerEquals(key, active.Key) && this.leftComparerEquals(payload, active.Payload);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool AreSame(long start, ref TKey key, ref TRight payload, ref ActiveEdge<TRight> active)
            => start == active.Start && this.keyComparerEquals(key, active.Key) && this.rightComparerEquals(payload, active.Payload);

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
        public override int CurrentlyBufferedRightInputCount => base.CurrentlyBufferedRightInputCount + this.rightEdgeMap.Count + this.rightIntervalMap.Count;

        [DataContract]
        private struct ActiveInterval<TPayload>
        {
            [DataMember]
            public long Start;
            [DataMember]
            public long End;
            [DataMember]
            public TKey Key;
            [DataMember]
            public TPayload Payload;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Populate(long start, long end, ref TKey key, ref TPayload payload)
            {
                this.Start = start;
                this.End = end;
                this.Key = key;
                this.Payload = payload;
            }

            public override string ToString() => "[Start=" + this.Start + ", End=" + this.End + ", Key='" + this.Key + "', Payload='" + this.Payload + "']";
        }

        [DataContract]
        private struct ActiveEdge<TPayload>
        {
            [DataMember]
            public long Start;
            [DataMember]
            public TKey Key;
            [DataMember]
            public TPayload Payload;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Populate(long start, ref TKey key, ref TPayload payload)
            {
                this.Start = start;
                this.Key = key;
                this.Payload = payload;
            }

            public override string ToString() => "[Start=" + this.Start + ", Key='" + this.Key + "', Payload='" + this.Payload + "']";
        }
    }
}
