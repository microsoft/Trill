// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class IncreasingOrderEquiJoinPipe<TKey, TLeft, TRight, TResult> : BinaryPipe<TKey, TLeft, TRight, TResult>
    {
        private const int DefaultCapacity = 64;
        private readonly MemoryPool<TKey, TResult> pool;
        private readonly string errorMessages;

        private readonly Expression<Func<TLeft, TRight, TResult>> selectorExpr;
        private readonly Func<TLeft, TRight, TResult> selector;
        /* Comparer for ordering of join key - should eventually be a stream property */
        [SchemaSerialization]
        private readonly Expression<Comparison<TKey>> joinKeyOrderComparerExpression;
        private readonly Comparison<TKey> joinKeyOrderComparer;

        [DataMember]
        private StreamMessage<TKey, TResult> output;

        [DataMember]
        private long nextLeftTime = long.MinValue;
        [DataMember]
        private long nextRightTime = long.MinValue;
        [DataMember]
        private TKey nextLeftKey;
        [DataMember]
        private TKey nextRightKey;
        [DataMember]
        private TKey currentRightKey;
        [DataMember]
        private List<ActiveEvent<TRight>> currentRightList;
        [DataMember]
        private TKey currentLeftKey;
        [DataMember]
        private List<ActiveEvent<TLeft>> currentLeftList;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public IncreasingOrderEquiJoinPipe() { }

        public IncreasingOrderEquiJoinPipe(
            BinaryStreamable<TKey, TLeft, TRight, TResult> stream,
            Expression<Func<TLeft, TRight, TResult>> selector,
            IStreamObserver<TKey, TResult> observer)
            : base(stream, observer)
        {
            this.selectorExpr = selector;
            this.selector = this.selectorExpr.Compile();

            this.joinKeyOrderComparerExpression = stream.Left.Properties.KeyComparer.GetCompareExpr();
            this.joinKeyOrderComparer = this.joinKeyOrderComparerExpression.Compile();

            this.currentLeftList = new List<ActiveEvent<TLeft>>();
            this.currentRightList = new List<ActiveEvent<TRight>>();

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
                JoinKind.IncreasingOrderEquiJoin,
                false, this.errorMessages);
            node.AddJoinExpression("selector", this.selectorExpr);
            node.AddJoinExpression("join key order comparer", this.joinKeyOrderComparerExpression);
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
            this.nextLeftKey = leftBatch.key.col[leftBatch.iter];

            if (!GoToVisibleRow(rightBatch))
            {
                leftBatchDone = false;
                rightBatchDone = true;
                return;
            }

            UpdateNextRightTime(rightBatch.vsync.col[rightBatch.iter]);
            this.nextRightKey = rightBatch.key.col[rightBatch.iter];

            while (true)
            {
                bool leftPunctuation = leftBatch.vother.col[leftBatch.iter] == StreamEvent.PunctuationOtherTime;
                bool rightPunctuation = rightBatch.vother.col[rightBatch.iter] == StreamEvent.PunctuationOtherTime;

                int compare = (leftPunctuation || rightPunctuation) ? 0 : this.joinKeyOrderComparer(this.nextLeftKey, this.nextRightKey);

                if (compare == 0)
                {
                    if (this.nextLeftTime <= this.nextRightTime)
                    {
                        // process left
                        if (leftPunctuation)
                        {
                            AddPunctuationToBatch(this.nextLeftTime);
                        }
                        else
                        {
                            #region ProcessLeftStartEdge
                            /*
                            ProcessLeftStartEdge(
                                nextLeftTime,
                                ref leftBatch.key.col[leftBatch.iter],
                                leftBatch[leftBatch.iter],
                                leftBatch.hash.col[leftBatch.iter], compare);
                            */
                            {
                                var payload = leftBatch[leftBatch.iter];

                                if (this.currentRightList.Count > 0)
                                {
                                    int compare2 = this.joinKeyOrderComparer(this.nextLeftKey, this.currentRightKey);

                                    Contract.Assert(compare2 >= 0, "Unexpected comparison in sort-ordered join");

                                    if (compare2 == 0)
                                    {
                                        // perform the join
                                        for (int i = 0; i < this.currentRightList.Count; i++)
                                        {
                                            ActiveEvent<TRight> t = this.currentRightList[i];
                                            var nextLeftKeyTemp = this.nextLeftKey;
                                            OutputStartEdge(this.nextLeftTime > t.Timestamp ? this.nextLeftTime : t.Timestamp, ref nextLeftKeyTemp, ref payload, ref t.Payload, leftBatch.hash.col[leftBatch.iter]);
                                        }
                                    }
                                    else
                                    {
                                        // clear the right array
                                        this.currentRightList.Clear();
                                    }
                                }

                                if (compare >= 0)
                                {
                                    // update the left array
                                    if ((this.currentLeftList.Count != 0) && (this.joinKeyOrderComparer(this.nextLeftKey, this.currentLeftKey) != 0))
                                    {
                                        Contract.Assert(this.joinKeyOrderComparer(this.nextLeftKey, this.currentLeftKey) > 0);
                                        this.currentLeftList.Clear();
                                    }
                                    var temp = this.nextLeftKey;
                                    this.currentLeftKey = temp;
                                    var leftAE = new ActiveEvent<TLeft> { Payload = payload, Timestamp = this.nextLeftTime };
                                    this.currentLeftList.Add(leftAE);
                                }
                            }
                            #endregion
                        }

                        leftBatch.iter++;

                        if (!GoToVisibleRow(leftBatch))
                        {
                            leftBatchDone = true;
                            rightBatchDone = false;
                            return;
                        }

                        this.nextLeftTime = leftBatch.vsync.col[leftBatch.iter];
                        this.nextLeftKey = leftBatch.key.col[leftBatch.iter];
                    }
                    else
                    {
                        // process right
                        if (rightPunctuation)
                        {
                            AddPunctuationToBatch(this.nextRightTime);
                        }
                        else
                        {
                            #region ProcessRightStartEdge
                            /* Inlined version of:
                            ProcessRightStartEdge(
                                nextRightTime,
                                ref rightBatch.key.col[rightBatch.iter],
                                rightBatch[rightBatch.iter],
                                rightBatch.hash.col[rightBatch.iter], compare);
                            */
                            if (this.currentLeftList.Count > 0)
                            {
                                int compare2 = this.joinKeyOrderComparer(this.nextRightKey, this.currentLeftKey);

                                Contract.Assert(compare2 >= 0, "Unexpected comparison in sort-ordered join");

                                if (compare2 == 0)
                                {
                                    // perform the join
                                    var rightP = rightBatch[rightBatch.iter];
                                    for (int i = 0; i < this.currentLeftList.Count; i++)
                                    {
                                        ActiveEvent<TLeft> t = this.currentLeftList[i];

                                        #region OutputStartEdge
                                        /* OutputStartEdge(nextRightTime > t.Timestamp ? nextRightTime : t.Timestamp,
                                            ref nextRightKey, ref t.Payload, ref rightP, rightBatch.hash.col[rightBatch.iter]);*/
                                        int index = this.output.Count++;
                                        this.output.vsync.col[index] = this.nextRightTime > t.Timestamp ? this.nextRightTime : t.Timestamp;
                                        this.output.vother.col[index] = StreamEvent.InfinitySyncTime;
                                        this.output.key.col[index] = this.nextRightKey;
                                        this.output[index] = this.selector(t.Payload, rightP);
                                        this.output.hash.col[index] = rightBatch.hash.col[rightBatch.iter];

                                        if (this.output.Count == Config.DataBatchSize) FlushContents();
                                        #endregion
                                    }
                                }
                                else
                                {
                                    // clear the left array
                                    this.currentLeftList.Clear();
                                }
                            }

                            if (compare <= 0)
                            {
                                // update the right array
                                if ((this.currentRightList.Count != 0) && (this.joinKeyOrderComparer(rightBatch.key.col[rightBatch.iter], this.currentRightKey) != 0))
                                {
                                    Contract.Assert(this.joinKeyOrderComparer(rightBatch.key.col[rightBatch.iter], this.currentRightKey) > 0);
                                    this.currentRightList.Clear();
                                }

                                this.currentRightKey = rightBatch.key.col[rightBatch.iter];
                                ActiveEvent<TRight> rightAE = default;
                                var payload = rightBatch[rightBatch.iter];

                                rightAE.Populate(ref this.nextRightTime, ref payload);
                                this.currentRightList.Add(rightAE);
                            }
                            #endregion
                        }

                        rightBatch.iter++;

                        #region GoToVisibleRow
                        /* Inlined version of:
                        if (!GoToVisibleRow(rightBatch))
                        {
                            leftBatchDone = false;
                            rightBatchDone = true;
                            return;
                        }*/
                        while (rightBatch.iter < rightBatch.Count &&
                            (rightBatch.bitvector.col[rightBatch.iter >> 6] & (1L << (rightBatch.iter & 0x3f))) != 0 &&
                            rightBatch.vother.col[rightBatch.iter] >= 0)
                        {
                            rightBatch.iter++;
                        }
                        if (rightBatch.iter == rightBatch.Count)
                        {
                            leftBatchDone = false;
                            rightBatchDone = true;
                            return;
                        }
                        #endregion

                        this.nextRightTime = rightBatch.vsync.col[rightBatch.iter];
                        this.nextRightKey = rightBatch.key.col[rightBatch.iter];
                    }
                }
                else if (compare < 0)
                {
                    // process left
                    #region ProcessLeftStartEdge
                    /*
                        ProcessLeftStartEdge(
                            nextLeftTime,
                            ref leftBatch.key.col[leftBatch.iter],
                            leftBatch[leftBatch.iter],
                            leftBatch.hash.col[leftBatch.iter], compare);
                        */
                    {

                        if (this.currentRightList.Count > 0)
                        {
                            int compare2 = this.joinKeyOrderComparer(this.nextLeftKey, this.currentRightKey);

                            Contract.Assert(compare2 >= 0, "Unexpected comparison in sort-ordered join");

                            if (compare2 == 0)
                            {
                                // perform the join
                                var payload = leftBatch[leftBatch.iter];

                                for (int i = 0; i < this.currentRightList.Count; i++)
                                {
                                    ActiveEvent<TRight> t = this.currentRightList[i];
                                    var temp = this.nextLeftKey;
                                    OutputStartEdge(this.nextLeftTime > t.Timestamp ? this.nextLeftTime : t.Timestamp, ref temp, ref payload, ref t.Payload, leftBatch.hash.col[leftBatch.iter]);
                                }
                            }
                            else
                            {
                                // clear the right array
                                this.currentRightList.Clear();
                            }
                        }

                        /*if (compare >= 0)
                        {
                            // update the left array
                            if ((currentLeftList.Count != 0) && (joinKeyOrderComparer(nextLeftKey, currentLeftKey) != 0))
                            {
                                Contract.Assert(joinKeyOrderComparer(nextLeftKey, currentLeftKey) > 0);
                                currentLeftList.Clear();
                            }
                            currentLeftKey = nextLeftKey;
                            ActiveEvent<TLeft> leftAE = new ActiveEvent<TLeft> { Payload = payload, Timestamp = nextLeftTime };
                            currentLeftList.Add(leftAE);
                        }*/
                    }
                    #endregion

                    leftBatch.iter++;

                    #region GoToVisibleRow
                    /* Inlined version of:
                    if (!GoToVisibleRow(leftBatch))
                    {
                        leftBatchDone = true;
                        rightBatchDone = false;
                        return;
                    }*/
                    while (leftBatch.iter < leftBatch.Count &&
                        (leftBatch.bitvector.col[leftBatch.iter >> 6] & (1L << (leftBatch.iter & 0x3f))) != 0 &&
                        leftBatch.vother.col[leftBatch.iter] >= 0)
                    {
                        leftBatch.iter++;
                    }
                    if (leftBatch.iter == leftBatch.Count)
                    {
                        leftBatchDone = true;
                        rightBatchDone = false;
                        return;
                    }
                    #endregion

                    #region UpdateNextLeftTime

                    this.nextLeftTime = leftBatch.vsync.col[leftBatch.iter];
                        #endregion

                    this.nextLeftKey = leftBatch.key.col[leftBatch.iter];

                }
                else // hot path if larger right side of join matches very few things on the left side
                {
                    // process right
                    #region ProcessRightStartEdge
                    /* Inlined version of:
                    ProcessRightStartEdge(
                        nextRightTime,
                        ref rightBatch.key.col[rightBatch.iter],
                        rightBatch.payload.col[rightBatch.iter],
                        rightBatch.hash.col[rightBatch.iter], compare);
                    */
                    if (this.currentLeftList.Count > 0)
                    {
                        int compare2 = this.joinKeyOrderComparer(this.nextRightKey, this.currentLeftKey);

                        Contract.Assert(compare2 >= 0, "Unexpected comparison in sort-ordered join");

                        if (compare2 == 0)
                        {
                            // perform the join
                            var rightP = rightBatch[rightBatch.iter];
                            for (int i = 0; i < this.currentLeftList.Count; i++)
                            {
                                ActiveEvent<TLeft> t = this.currentLeftList[i];
                                #region OutputStartEdge
                                /* OutputStartEdge(nextRightTime > t.Timestamp ? nextRightTime : t.Timestamp,
                                        ref nextRightKey, ref t.Payload, ref rightP, rightBatch.hash.col[rightBatch.iter]);*/
                                int index = this.output.Count++;
                                this.output.vsync.col[index] = this.nextRightTime > t.Timestamp ? this.nextRightTime : t.Timestamp;
                                this.output.vother.col[index] = StreamEvent.InfinitySyncTime;
                                this.output.key.col[index] = this.nextRightKey;
                                this.output[index] = this.selector(t.Payload, rightP);
                                this.output.hash.col[index] = rightBatch.hash.col[rightBatch.iter];

                                if (this.output.Count == Config.DataBatchSize) FlushContents();
                                #endregion
                            }
                        }
                        else
                        {
                            // clear the left array
                            this.currentLeftList.Clear();
                        }
                    }

                    /*
                    if (compare <= 0)
                    {
                        // update the right array
                        if ((currentRightList.Count != 0) && (joinKeyOrderComparer(rightBatch.key.col[rightBatch.iter], currentRightKey) != 0))
                        {
                            Contract.Assert(joinKeyOrderComparer(rightBatch.key.col[rightBatch.iter], currentRightKey) > 0);
                            currentRightList.Clear();
                        }
                        currentRightKey = rightBatch.key.col[rightBatch.iter];
                        ActiveEvent<TRight> rightAE = new ActiveEvent<TRight>();
                        rightAE.Populate(ref nextRightTime, ref rightBatch.payload.col[rightBatch.iter]);
                        currentRightList.Add(rightAE);
                    }*/
                    #endregion

                    rightBatch.iter++;

                    #region GoToVisibleRow
                    /* Inlined version of:
                    if (!GoToVisibleRow(rightBatch))
                    {
                        leftBatchDone = false;
                        rightBatchDone = true;
                        return;
                    }*/
                    while (rightBatch.iter < rightBatch.Count &&
                        (rightBatch.bitvector.col[rightBatch.iter >> 6] & (1L << (rightBatch.iter & 0x3f))) != 0 &&
                        rightBatch.vother.col[rightBatch.iter] >= 0)
                    {
                        rightBatch.iter++;
                    }
                    if (rightBatch.iter == rightBatch.Count)
                    {
                        leftBatchDone = false;
                        rightBatchDone = true;
                        return;
                    }
                    #endregion

                    #region UpdateNextRightTime
                        /* Inlined version of: UpdateNextRightTime(rightBatch.vsync.col[rightBatch.iter]); */
                    this.nextRightTime = rightBatch.vsync.col[rightBatch.iter];
                        #endregion

                    this.nextRightKey = rightBatch.key.col[rightBatch.iter];
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

                if (batch.vother.col[batch.iter] == StreamEvent.PunctuationOtherTime)
                {
                    if (this.nextLeftTime > this.nextRightTime)
                    {
                        isBatchDone = false;
                        return;
                    }

                    AddPunctuationToBatch(batch.vsync.col[batch.iter]);

                    batch.iter++;
                    continue;
                }

                this.nextLeftKey = batch.key.col[batch.iter];

                int compare = this.joinKeyOrderComparer(this.nextLeftKey, this.nextRightKey);
                if ((compare == 0) && (this.nextLeftTime <= this.nextRightTime))
                {
                    ProcessLeftStartEdge(
                        this.nextLeftTime,
                        ref batch.key.col[batch.iter],
                        batch[batch.iter],
                        batch.hash.col[batch.iter], compare);

                    batch.iter++;
                }
                else
                {
                    isBatchDone = false;
                    return;
                }
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

                if (batch.vother.col[batch.iter] == StreamEvent.PunctuationOtherTime)
                {
                    if (this.nextRightTime > this.nextLeftTime)
                    {
                        isBatchDone = false;
                        return;
                    }

                    AddPunctuationToBatch(batch.vsync.col[batch.iter]);

                    batch.iter++;
                    continue;
                }

                this.nextRightKey = batch.key.col[batch.iter];

                int compare = this.joinKeyOrderComparer(this.nextLeftKey, this.nextRightKey);
                if ((compare == 0) && (this.nextRightTime <= this.nextLeftTime))
                {
                    ProcessRightStartEdge(
                        this.nextRightTime,
                        ref batch.key.col[batch.iter],
                        batch[batch.iter],
                        batch.hash.col[batch.iter], compare);

                    batch.iter++;
                }
                else
                {
                    isBatchDone = false;
                    return;
                }
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
        private void UpdateNextLeftTime(long time) => this.nextLeftTime = time;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateNextRightTime(long time) => this.nextRightTime = time;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessLeftStartEdge(long start, ref TKey key, TLeft payload, int hash, int nextLeftRightCompareResult)
        {
            if (this.currentRightList.Count > 0)
            {
                int compare = this.joinKeyOrderComparer(key, this.currentRightKey);

                Contract.Assert(compare >= 0, "Unexpected comparison in sort-ordered join");

                if (compare == 0)
                {
                    // perform the join
                    for (int i = 0; i < this.currentRightList.Count; i++)
                    {
                        ActiveEvent<TRight> t = this.currentRightList[i];
                        OutputStartEdge(start > t.Timestamp ? start : t.Timestamp, ref key, ref payload, ref t.Payload, hash);
                    }
                }
                else
                {
                    // clear the right array
                    this.currentRightList.Clear();
                }
            }

            if (nextLeftRightCompareResult >= 0)
            {
                // update the left array
                if ((this.currentLeftList.Count != 0) && (this.joinKeyOrderComparer(key, this.currentLeftKey) != 0))
                {
                    Contract.Assert(this.joinKeyOrderComparer(key, this.currentLeftKey) > 0);
                    this.currentLeftList.Clear();
                }

                this.currentLeftKey = key;
                ActiveEvent<TLeft> leftAE = default;
                leftAE.Populate(ref start, ref payload);
                this.currentLeftList.Add(leftAE);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessRightStartEdge(long start, ref TKey key, TRight payload, int hash, int nextLeftRightCompareResult)
        {
            if (this.currentLeftList.Count > 0)
            {
                int compare = this.joinKeyOrderComparer(key, this.currentLeftKey);

                Contract.Assert(compare >= 0, "Unexpected comparison in sort-ordered join");

                if (compare == 0)
                {
                    // perform the join
                    for (int i = 0; i < this.currentLeftList.Count; i++)
                    {
                        ActiveEvent<TLeft> t = this.currentLeftList[i];
                        OutputStartEdge(start > t.Timestamp ? start : t.Timestamp, ref key, ref t.Payload, ref payload, hash);
                    }
                }
                else
                {
                    // clear the left array
                    this.currentLeftList.Clear();
                }
            }

            if (nextLeftRightCompareResult <= 0)
            {
                // update the right array
                if ((this.currentRightList.Count != 0) && (this.joinKeyOrderComparer(key, this.currentRightKey) != 0))
                {
                    Contract.Assert(this.joinKeyOrderComparer(key, this.currentRightKey) > 0);
                    this.currentRightList.Clear();
                }

                this.currentRightKey = key;
                ActiveEvent<TRight> rightAE = default;
                rightAE.Populate(ref start, ref payload);
                this.currentRightList.Add(rightAE);
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
            this.output.Seal();
            this.Observer.OnNext(this.output);
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        public override int CurrentlyBufferedOutputCount => this.output.Count;
        public override int CurrentlyBufferedLeftInputCount => base.CurrentlyBufferedLeftInputCount + this.currentLeftList.Count;
        public override int CurrentlyBufferedRightInputCount => base.CurrentlyBufferedRightInputCount + this.currentRightList.Count;

        [DataContract]
        private struct ActiveEvent<TPayload>
        {
            [DataMember]
            public long Timestamp;
            [DataMember]
            public TPayload Payload;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Populate(ref long timestamp, ref TPayload payload)
            {
                this.Timestamp = timestamp;
                this.Payload = payload;
            }

            public override string ToString() => "[Timestamp='" + this.Timestamp + "', Payload='" + this.Payload + "']";
        }
    }
}
