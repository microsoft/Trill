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
        private readonly long leftDuration;
        [SchemaSerialization]
        private readonly long rightDuration;

        [DataMember]
        private StreamMessage<TKey, TResult> output;

        /// <summary>
        /// Stores left intervals valid for <see cref="currTime"/>.
        /// FastMap visibility is not used.
        /// </summary>
        [DataMember]
        private FastMap<ActiveInterval<TLeft>> leftIntervalMap = new FastMap<ActiveInterval<TLeft>>();

        /// <summary>
        /// Stores right intervals valid for <see cref="currTime"/>.
        /// FastMap visibility is not used.
        /// </summary>
        [DataMember]
        private FastMap<ActiveInterval<TRight>> rightIntervalMap = new FastMap<ActiveInterval<TRight>>();

        /// <summary>
        /// Stores end edges for the current join intervals, i.e. after <see cref="currTime"/>.
        /// Left intervals have a positive index, right intervals have a negative index.
        /// </summary>
        [DataMember]
        private IEndPointOrderer endPointHeap;

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
            this.leftDuration = stream.Left.Properties.ConstantDurationLength.Value;
            this.rightDuration = stream.Right.Properties.ConstantDurationLength.Value;

            this.keyComparer = stream.Properties.KeyEqualityComparer.GetEqualsExpr();
            this.keyComparerEquals = this.keyComparer.Compile();

            if (this.leftDuration == this.rightDuration)
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
                false, null);
            node.AddJoinExpression("key comparer", this.keyComparer);
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
                    if (leftBatch.vother.col[leftBatch.iter] == StreamEvent.PunctuationOtherTime)
                    {
                        AddPunctuationToBatch(this.nextLeftTime);
                    }
                    else
                    {
                        ProcessLeftEvent(
                            this.nextLeftTime,
                            ref leftBatch.key.col[leftBatch.iter],
                            leftBatch[leftBatch.iter],
                            leftBatch.hash.col[leftBatch.iter]);
                    }

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
                    if (rightBatch.vother.col[rightBatch.iter] == StreamEvent.PunctuationOtherTime)
                    {
                        AddPunctuationToBatch(this.nextRightTime);
                    }
                    else
                    {
                        ProcessRightEvent(
                            this.nextRightTime,
                            ref rightBatch.key.col[rightBatch.iter],
                            rightBatch[rightBatch.iter],
                            rightBatch.hash.col[rightBatch.iter]);
                    }

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

                if (batch.vother.col[batch.iter] == StreamEvent.PunctuationOtherTime)
                {
                    AddPunctuationToBatch(this.nextLeftTime);
                }
                else
                {
                    ProcessLeftEvent(
                        this.nextLeftTime,
                        ref batch.key.col[batch.iter],
                        batch[batch.iter],
                        batch.hash.col[batch.iter]);
                }

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

                if (batch.vother.col[batch.iter] == StreamEvent.PunctuationOtherTime)
                {
                    AddPunctuationToBatch(this.nextRightTime);
                }
                else
                {
                    ProcessRightEvent(
                        this.nextRightTime,
                        ref batch.key.col[batch.iter],
                        batch[batch.iter],
                        batch.hash.col[batch.iter]);
                }

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
                this.currTime = time;
                ReachTime();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessLeftEvent(long start, ref TKey key, TLeft payload, int hash)
        {
            int index = this.leftIntervalMap.Insert(hash);
            this.leftIntervalMap.Values[index].Populate(start, ref key, ref payload);
            CreateOutputForStartInterval(start, ref key, ref payload, hash);
            this.endPointHeap.Insert(start + this.leftDuration, index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessRightEvent(long start, ref TKey key, TRight payload, int hash)
        {
            int index = this.rightIntervalMap.Insert(hash);
            this.rightIntervalMap.Values[index].Populate(start, ref key, ref payload);
            CreateOutputForStartInterval(start, ref key, ref payload, hash);
            this.endPointHeap.Insert(start + this.rightDuration, ~index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReachTime()
        {
            while (this.endPointHeap.TryGetNextInclusive(this.currTime, out long endTime, out int index))
            {
                if (index >= 0)
                {
                    this.leftIntervalMap.Remove(index);
                }
                else
                {
                    this.rightIntervalMap.Remove(~index);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CreateOutputForStartInterval(long currentTime, ref TKey key, ref TLeft payload, int hash)
        {
            // Create end edges for all joined right intervals.
            var intervals = this.rightIntervalMap.Find(hash);
            while (intervals.Next(out var index))
            {
                var rin = this.rightIntervalMap.Values[index];
                if (this.keyComparerEquals(key, rin.Key))
                {
                    long leftEnd = currentTime + this.leftDuration;
                    long rightEnd = rin.Start + this.rightDuration;
                    AddToBatch(
                        currentTime,
                        leftEnd < rightEnd ? leftEnd : rightEnd,
                        ref key,
                        ref payload,
                        ref rin.Payload,
                        hash);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CreateOutputForStartInterval(long currentTime, ref TKey key, ref TRight payload, int hash)
        {
            // Create end edges for all joined left intervals.
            var intervals = this.leftIntervalMap.Find(hash);
            while (intervals.Next(out var index))
            {
                var lin = this.leftIntervalMap.Values[index];
                if (this.keyComparerEquals(key, lin.Key))
                {
                    long rightEnd = currentTime + this.rightDuration;
                    long leftEnd = lin.Start + this.leftDuration;
                    AddToBatch(
                        currentTime,
                        rightEnd < leftEnd ? rightEnd : leftEnd,
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