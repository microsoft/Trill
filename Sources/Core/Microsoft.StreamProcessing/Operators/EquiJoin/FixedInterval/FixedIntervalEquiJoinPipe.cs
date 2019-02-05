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
    internal sealed class FixedIntervalEquiJoinPipe<TKey, TLeft, TRight, TResult> : BinaryPipe<TKey, TLeft, TRight, TResult>
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
        [DataMember]
        private FastMap<ActiveInterval<TLeft>> leftIntervalMap = new FastMap<ActiveInterval<TLeft>>();
        [DataMember]
        private IEndPointOrderer endPointHeap;
        [DataMember]
        private FastMap<ActiveInterval<TRight>> rightIntervalMap = new FastMap<ActiveInterval<TRight>>();
        [DataMember]
        private long nextLeftTime = long.MinValue;
        [DataMember]
        private long nextRightTime = long.MinValue;
        [DataMember]
        private long currTime = long.MinValue;
        [Obsolete("Used only by serialization. Do not call directly.")]
        public FixedIntervalEquiJoinPipe() { }

        public FixedIntervalEquiJoinPipe(
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

            if (stream.Left.Properties.IsConstantDuration && stream.Right.Properties.IsConstantDuration &&
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
                JoinKind.FixedIntervalEquiJoin,
                false, null, false);
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

            return (batch.iter != batch.Count);
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
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateNextRightTime(long time)
        {
            this.nextRightTime = time;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessLeftEvent(long start, long end, ref TKey key, TLeft payload, int hash)
        {
            if (start < end)
            {
                // Row is a start edge or interval.
                bool processable = this.nextRightTime > start;
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
            else if (end == StreamEvent.PunctuationOtherTime)
            {
                AddPunctuationToBatch(start);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessRightEvent(long start, long end, ref TKey key, TRight payload, int hash)
        {
            if (start < end)
            {
                // Row is a start edge or interval.
                bool processable = this.nextLeftTime > start;
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
            else if (end == StreamEvent.PunctuationOtherTime)
            {
                AddPunctuationToBatch(start);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void LeaveTime()
        {
            int index;
            int hash;

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
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReachTime()
        {
            while (this.endPointHeap.TryGetNextInclusive(this.currTime, out long endPointTime, out int index))
            {
                if (index >= 0)
                {
                    // Endpoint is left interval ending.
                    this.leftIntervalMap.Remove(index);
                }
                else
                {
                    // Endpoint is right interval ending.
                    index = ~index;
                    this.rightIntervalMap.Remove(index);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CreateOutputForStartInterval(long currentTime, long end, ref TKey key, ref TLeft payload, int hash)
        {
            // Create end edges for all joined right intervals.
            var intervals = this.rightIntervalMap.Find(hash);
            while (intervals.Next(out var index))
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
            // Create end edges for all joined left intervals.
            var intervals = this.leftIntervalMap.Find(hash);
            while (intervals.Next(out var index))
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
        private void AddToBatch(long start, long end, ref TKey key, ref TLeft leftPayload, ref TRight rightPayload, int hash)
        {
            int index = this.output.Count++;
            this.output.vsync.col[index] = start;
            this.output.vother.col[index] = end;
            this.output.key.col[index] = key;
            this.output[index] = this.selector(leftPayload, rightPayload);
            this.output.hash.col[index] = hash;

            if (this.output.Count == Config.DataBatchSize) FlushContents();
        }

        protected override void FlushContents()
        {
            if (this.output.Count == 0) return;
            this.output.Seal();
            this.Observer.OnNext(this.output);
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        public override int CurrentlyBufferedOutputCount => this.output.Count;
        public override int CurrentlyBufferedLeftInputCount => base.CurrentlyBufferedLeftInputCount + this.leftIntervalMap.Count;
        public override int CurrentlyBufferedRightInputCount => base.CurrentlyBufferedRightInputCount + this.rightIntervalMap.Count;

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
    }
}
