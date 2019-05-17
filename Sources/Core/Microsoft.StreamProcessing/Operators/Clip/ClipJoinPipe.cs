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
    internal sealed class ClipJoinPipe<TKey, TLeft, TRight> : BinaryPipe<TKey, TLeft, TRight, TLeft>
    {
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
        /// Stores intervals for active left events.
        /// </summary>
        [DataMember]
        private FastMap<LeftInterval> leftIntervalMap = new FastMap<LeftInterval>();

        /// <summary>
        /// Stores left start edges at <see cref="currTime"/>
        /// </summary>
        [DataMember]
        private FastMap<LeftEdge> leftEdgeMap = new FastMap<LeftEdge>();

        /// <summary>
        /// Stores left end edges at some point in the future, i.e. after <see cref="currTime"/>.
        /// These can originate from edge end events or interval events.
        /// </summary>
        [DataMember]
        private RemovableEndPointHeap leftEndPointHeap;

        [DataMember]
        private long nextLeftTime = long.MinValue;
        [DataMember]
        private long nextRightTime = long.MinValue;
        [DataMember]
        private long currTime = long.MinValue;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public ClipJoinPipe() { }

        public ClipJoinPipe(ClipJoinStreamable<TKey, TLeft, TRight> stream, IStreamObserver<TKey, TLeft> observer)
            : base(stream, observer)
        {
            this.keyComparer = stream.Properties.KeyEqualityComparer.GetEqualsExpr();
            this.keyComparerEquals = this.keyComparer.Compile();

            this.leftComparer = stream.LeftComparer.GetEqualsExpr();
            this.leftComparerEquals = this.leftComparer.Compile();

            this.leftEndPointHeap = new RemovableEndPointHeap();
            this.pool = MemoryManager.GetMemoryPool<TKey, TLeft>(stream.Properties.IsColumnar);
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        protected override void ProduceBinaryQueryPlan(PlanNode left, PlanNode right)
        {
            var node = new JoinPlanNode(
                left, right, this,
                typeof(TLeft), typeof(TRight), typeof(TLeft), typeof(TKey), JoinKind.Clip, false, null);
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
                batch.iter++;

            return (batch.iter != batch.Count);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateTime(long time)
        {
            if (time != this.currTime)
            {
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
                bool isInterval = end < StreamEvent.InfinitySyncTime;
                if (isInterval)
                {
                    bool isFullyOutputtable = this.nextRightTime >= end;
                    if (isFullyOutputtable)
                    {
                        // Output full interval.
                        AddToBatch(start, end, ref key, ref payload, hash);
                    }
                    else
                    {
                        // Insert into map to remember interval.
                        int mapIndex = this.leftIntervalMap.Insert(hash);

                        // Insert into heap to schedule removal at endpoint.
                        int heapIndex = this.leftEndPointHeap.Insert(end, mapIndex);

                        // Set value in map, also remembering heap's index.
                        this.leftIntervalMap.Values[mapIndex].Initialize(start, ref key, ref payload, heapIndex);

                        // Output start edge.
                        AddToBatch(start, StreamEvent.InfinitySyncTime, ref key, ref payload, hash);
                    }
                }
                else
                {
                    int index = this.leftEdgeMap.Insert(hash);
                    this.leftEdgeMap.Values[index].Populate(start, ref key, ref payload);

                    // Output start edge.
                    AddToBatch(start, StreamEvent.InfinitySyncTime, ref key, ref payload, hash);
                }
            }
            else if (end == StreamEvent.PunctuationOtherTime)
            {
                AddPunctuationToBatch(start);
            }
            else
            {
                // Row is an end edge.
                var leftEvents = this.leftEdgeMap.Find(hash);
                while (leftEvents.Next(out int index))
                {
                    if (AreSame(end, ref key, ref payload, ref this.leftEdgeMap.Values[index]))
                    {
                        // Output end edge.
                        AddToBatch(start, end, ref key, ref payload, hash);

                        // Remove from leftMap.
                        leftEvents.Remove();
                        break;
                    }
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessRightEvent(long start, long end, ref TKey key, int hash)
        {
            if (end == StreamEvent.PunctuationOtherTime)
            {
                AddPunctuationToBatch(start);
                return;
            }
            else if (start >= end)
            {
                // Row is an end edge, which we don't care about because the start edge would have already
                // removed all joining left events.
                return;
            }

            // Mark any matching left intervals as no longer active.
            int index;
            var leftIntervals = this.leftIntervalMap.Find(hash);
            while (leftIntervals.Next(out index))
            {
                long leftStart = this.leftIntervalMap.Values[index].Start;
                if (leftStart < start && this.keyComparerEquals(key, this.leftIntervalMap.Values[index].Key))
                {
                    // Output end edge.
                    AddToBatch(
                        start,
                        leftStart,
                        ref this.leftIntervalMap.Values[index].Key,
                        ref this.leftIntervalMap.Values[index].Payload,
                        hash);

                    // Remove from heap and map.
                    this.leftEndPointHeap.Remove(this.leftIntervalMap.Values[index].HeapIndex);
                    leftIntervals.Remove();
                }
            }

            // Remove any matching left edges.
            var leftEdges = this.leftEdgeMap.Find(hash);
            while (leftEdges.Next(out index))
            {
                long leftStart = this.leftEdgeMap.Values[index].Start;
                if (leftStart < start && this.keyComparerEquals(key, this.leftEdgeMap.Values[index].Key))
                {
                    // Output end edge.
                    AddToBatch(
                        start,
                        leftStart,
                        ref this.leftEdgeMap.Values[index].Key,
                        ref this.leftEdgeMap.Values[index].Payload,
                        hash);

                    // Remove left event.
                    leftEdges.Remove();
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReachTime()
        {
            // Carry-out all interval endpoints for left intervals that end prior or at new current time.
            while (this.leftEndPointHeap.TryGetNextInclusive(this.currTime, out long endPointTime, out int index))
            {
                // Output end edge.
                AddToBatch(
                    endPointTime, this.leftIntervalMap.Values[index].Start,
                    ref this.leftIntervalMap.Values[index].Key,
                    ref this.leftIntervalMap.Values[index].Payload, this.leftIntervalMap.GetHash(index));

                // Remove from leftMap.
                this.leftIntervalMap.Remove(index);
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
        private bool AreSame(long start, ref TKey key, ref TLeft payload, ref LeftEdge active)
            => start == active.Start && this.keyComparerEquals(key, active.Key) && this.leftComparerEquals(payload, active.Payload);

        protected override void FlushContents()
        {
            if (this.output.Count == 0) return;
            this.Observer.OnNext(this.output);
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        public override int CurrentlyBufferedOutputCount => this.output.Count;
        public override int CurrentlyBufferedLeftInputCount => base.CurrentlyBufferedLeftInputCount + this.leftEdgeMap.Count + this.leftIntervalMap.Count;

        [DataContract]
        private struct LeftInterval
        {
            [DataMember]
            public long Start;
            [DataMember]
            public TKey Key;
            [DataMember]
            public TLeft Payload;
            [DataMember]
            public int HeapIndex;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Initialize(long start, ref TKey key, ref TLeft payload, int heapIndex)
            {
                this.Start = start;
                this.Key = key;
                this.Payload = payload;
                this.HeapIndex = heapIndex;
            }

            public override string ToString()
                => "[Start=" + this.Start + ", Key='" + this.Key + "', Payload='" + this.Payload + "', HeapIndex=" + this.HeapIndex + "]";
        }

        [DataContract]
        private struct LeftEdge
        {
            [DataMember]
            public long Start;
            [DataMember]
            public TKey Key;
            [DataMember]
            public TLeft Payload;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Populate(long start, ref TKey key, ref TLeft payload)
            {
                this.Start = start;
                this.Key = key;
                this.Payload = payload;
            }

            public override string ToString()
                => "[Start=" + this.Start + ", Key='" + this.Key + "', Payload='" + this.Payload + "]";
        }
    }
}
