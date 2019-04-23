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
    internal sealed class StartEdgeEquiJoinPipe<TKey, TLeft, TRight, TResult> : BinaryPipe<TKey, TLeft, TRight, TResult>
    {
        private const int DefaultCapacity = 64;
        private readonly MemoryPool<TKey, TResult> pool;
        private readonly string errorMessages;

        [SchemaSerialization]
        private readonly Expression<Func<TLeft, TRight, TResult>> selectorExpr;
        private readonly Func<TLeft, TRight, TResult> selector;
        [SchemaSerialization]
        private readonly Expression<Func<TKey, TKey, bool>> keyComparerExpr;
        private readonly Func<TKey, TKey, bool> keyComparer;

        [DataMember]
        private StreamMessage<TKey, TResult> output;

        [DataMember]
        private FastMap<ActiveEvent<TLeft>> leftEdgeMap = new FastMap<ActiveEvent<TLeft>>(DefaultCapacity);
        [DataMember]
        private FastMap<ActiveEvent<TRight>> rightEdgeMap = new FastMap<ActiveEvent<TRight>>(DefaultCapacity);
        [DataMember]
        private long nextLeftTime = long.MinValue;
        [DataMember]
        private bool isLeftComplete;
        [DataMember]
        private long nextRightTime = long.MinValue;
        [DataMember]
        private bool isRightComplete;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public StartEdgeEquiJoinPipe() { }

        public StartEdgeEquiJoinPipe(
            BinaryStreamable<TKey, TLeft, TRight, TResult> stream,
            Expression<Func<TLeft, TRight, TResult>> selector,
            IStreamObserver<TKey, TResult> observer)
            : base(stream, observer)
        {
            this.selectorExpr = selector;
            this.selector = this.selectorExpr.Compile();

            this.keyComparerExpr = stream.Properties.KeyEqualityComparer.GetEqualsExpr();
            this.keyComparer = this.keyComparerExpr.Compile();

            this.errorMessages = stream.ErrorMessages;

            this.pool = MemoryManager.GetMemoryPool<TKey, TResult>(stream.Properties.IsColumnar);
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        protected override void ProduceBinaryQueryPlan(PlanNode left, PlanNode right)
        {
            var node = new JoinPlanNode(
                left, right, this,
                typeof(TLeft), typeof(TRight), typeof(TResult), typeof(TKey),
                JoinKind.StartEdgeEquijoin,
                false, this.errorMessages);
            node.AddJoinExpression("selector", this.selectorExpr);
            node.AddJoinExpression("key comparer", this.keyComparerExpr);
            this.Observer.ProduceQueryPlan(node);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

            FastMap<ActiveEvent<TRight>>.FindTraverser rightEdges = default;
            FastMap<ActiveEvent<TLeft>>.FindTraverser leftEdges = default;
            while (true)
            {
                bool leftPunctuation = leftBatch.vother.col[leftBatch.iter] == StreamEvent.PunctuationOtherTime;
                bool rightPunctuation = rightBatch.vother.col[rightBatch.iter] == StreamEvent.PunctuationOtherTime;

                if (this.nextLeftTime <= this.nextRightTime)
                {
                    if (leftPunctuation)
                    {
                        AddPunctuationToBatch(this.nextLeftTime);
                    }
                    else
                    {
                        bool first = true;
                        TKey key = default;
                        TLeft payload;

                        var hash = leftBatch.hash.col[leftBatch.iter];
                        if (this.rightEdgeMap.Find(hash, ref rightEdges))
                        {
                            while (rightEdges.Next(out int rightIndex))
                            {

                                if (first) { key = leftBatch.key.col[leftBatch.iter]; first = false; }
                                if (this.keyComparer(key, this.rightEdgeMap.Values[rightIndex].Key))
                                {
                                    payload = leftBatch[leftBatch.iter];
                                    OutputStartEdge(this.nextLeftTime, ref key, ref payload, ref this.rightEdgeMap.Values[rightIndex].Payload, hash);
                                }
                            }
                        }

                        if (!this.isRightComplete)
                        {
                            if (first) key = leftBatch.key.col[leftBatch.iter];
                            payload = leftBatch[leftBatch.iter]; // potential rare recomputation

                            int newIndex = this.leftEdgeMap.Insert(hash);
                            this.leftEdgeMap.Values[newIndex].Populate(ref key, ref payload);

                        }
                    }

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
                    if (rightPunctuation)
                    {
                        AddPunctuationToBatch(this.nextRightTime);
                    }
                    else
                    {
                        bool first = true;
                        TKey key = default;
                        TRight payload;

                        var hash = rightBatch.hash.col[rightBatch.iter];
                        if (this.leftEdgeMap.Find(hash, ref leftEdges))
                        {
                            while (leftEdges.Next(out int leftIndex))
                            {

                                if (first) { key = rightBatch.key.col[rightBatch.iter]; first = false; }
                                if (this.keyComparer(key, this.leftEdgeMap.Values[leftIndex].Key))
                                {
                                    payload = rightBatch[rightBatch.iter];
                                    OutputStartEdge(this.nextRightTime, ref key, ref this.leftEdgeMap.Values[leftIndex].Payload, ref payload, hash);
                                }
                            }
                        }
                        if (!this.isLeftComplete)
                        {

                            if (first) key = rightBatch.key.col[rightBatch.iter];
                            payload = rightBatch[rightBatch.iter]; // potential rare recomputation
                            int newIndex = this.rightEdgeMap.Insert(hash);
                            this.rightEdgeMap.Values[newIndex].Populate(ref key, ref payload);
                        }
                    }

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

                if (batch.vother.col[batch.iter] == StreamEvent.PunctuationOtherTime)
                {
                    AddPunctuationToBatch(batch.vsync.col[batch.iter]);
                    batch.iter++;
                    continue;
                }

                FastMap<ActiveEvent<TRight>>.FindTraverser rightEdges = default;
                bool first = true;
                TKey key = default;
                TLeft payload;

                var hash = batch.hash.col[batch.iter];
                if (this.rightEdgeMap.Find(hash, ref rightEdges))
                {
                    while (rightEdges.Next(out int rightIndex))
                    {

                        if (first) { key = batch.key.col[batch.iter]; first = false; }
                        if (this.keyComparer(key, this.rightEdgeMap.Values[rightIndex].Key))
                        {
                            payload = batch[batch.iter];
                            OutputStartEdge(this.nextLeftTime, ref key, ref payload, ref this.rightEdgeMap.Values[rightIndex].Payload, hash);
                        }
                    }
                }
                if (!this.isRightComplete)
                {
                    if (first) key = batch.key.col[batch.iter];
                    payload = batch[batch.iter]; // potential rare recomputation

                    int newIndex = this.leftEdgeMap.Insert(hash);
                    this.leftEdgeMap.Values[newIndex].Populate(ref key, ref payload);

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

                UpdateNextRightTime(batch.vsync.col[batch.iter]);

                if (this.nextRightTime > this.nextLeftTime)
                {
                    isBatchDone = false;
                    return;
                }

                if (batch.vother.col[batch.iter] == StreamEvent.PunctuationOtherTime)
                {
                    AddPunctuationToBatch(batch.vsync.col[batch.iter]);
                    batch.iter++;
                    continue;
                }

                FastMap<ActiveEvent<TLeft>>.FindTraverser leftEdges = default;

                bool first = true;
                TKey key = default;
                TRight payload;

                var hash = batch.hash.col[batch.iter];
                if (this.leftEdgeMap.Find(hash, ref leftEdges))
                {
                    while (leftEdges.Next(out int leftIndex))
                    {
                        if (first) { key = batch.key.col[batch.iter]; first = false; }
                        if (this.keyComparer(key, this.leftEdgeMap.Values[leftIndex].Key))
                        {
                            payload = batch[batch.iter];
                            OutputStartEdge(this.nextRightTime, ref key, ref this.leftEdgeMap.Values[leftIndex].Payload, ref payload, hash);
                        }
                    }
                }
                if (!this.isLeftComplete)
                {
                    if (first) key = batch.key.col[batch.iter];
                    payload = batch[batch.iter]; // potential rare recomputation

                    int newIndex = this.rightEdgeMap.Insert(hash);
                    this.rightEdgeMap.Values[newIndex].Populate(ref key, ref payload);

                }

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

            return (batch.iter != batch.Count);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateNextLeftTime(long time)
        {
            this.nextLeftTime = time;
            if (this.nextLeftTime == StreamEvent.InfinitySyncTime)
            {
                this.isLeftComplete = true;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateNextRightTime(long time)
        {
            this.nextRightTime = time;
            if (this.nextRightTime == StreamEvent.InfinitySyncTime)
            {
                this.isRightComplete = true;
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
        private void OutputStartEdge(long start, ref TKey key, ref TLeft leftPayload, ref TRight rightPayload, int hash)
        {
            int index = this.output.Count++;
            this.output.vsync.col[index] = start;
            this.output.vother.col[index] = StreamEvent.InfinitySyncTime;
            this.output.key.col[index] = key;
            this.output[index] = this.selector(leftPayload, rightPayload);
            this.output.hash.col[index] = hash;

            if (this.output.Count == Config.DataBatchSize) FlushContents();
        }

        protected override void FlushContents()
        {
            if (this.output.Count == 0) return;
            this.Observer.OnNext(this.output);
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        public override int CurrentlyBufferedOutputCount => this.output.Count;
        public override int CurrentlyBufferedLeftInputCount => base.CurrentlyBufferedLeftInputCount + this.leftEdgeMap.Count;
        public override int CurrentlyBufferedRightInputCount => base.CurrentlyBufferedRightInputCount + this.rightEdgeMap.Count;

        private struct ActiveEvent<TPayload>
        {
            public TKey Key;

            public TPayload Payload;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Populate(ref TKey key, ref TPayload payload)
            {
                this.Key = key;
                this.Payload = payload;
            }

            public override string ToString()
            {
                return "[Key='" + this.Key + "', Payload='" + this.Payload + "']";
            }
        }
    }
}
