// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class DisjointUnionPipe<TKey, TPayload> : BinaryPipe<TKey, TPayload, TPayload, TPayload>
    {
        private static readonly long GlobalPunctuationOtherTime = typeof(TKey).GetPartitionType() != null ? PartitionedStreamEvent.LowWatermarkOtherTime : StreamEvent.PunctuationOtherTime;
        private readonly MemoryPool<TKey, TPayload> pool;

        [DataMember]
        private long leftGlobalPunctuation = 0;
        [DataMember]
        private long rightGlobalPunctuation = 0;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public DisjointUnionPipe() { }

        public DisjointUnionPipe(Streamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer)
            : base(stream, observer)
        {
            this.pool = MemoryManager.GetMemoryPool<TKey, TPayload>(stream.Properties.IsColumnar);
        }

        protected override void ProcessBothBatches(StreamMessage<TKey, TPayload> leftBatch, StreamMessage<TKey, TPayload> rightBatch, out bool leftBatchDone, out bool rightBatchDone, out bool leftBatchFree, out bool rightBatchFree)
        {
            leftBatchDone = true;
            leftBatchFree = false;
            rightBatchDone = true;
            rightBatchFree = false;

            var newLeftGlobalPunctuation = Math.Max(this.leftGlobalPunctuation, ExtractGlobalPunctuations(leftBatch));
            var newRightGlobalPunctuation = Math.Max(this.rightGlobalPunctuation, ExtractGlobalPunctuations(rightBatch));
            this.Observer.OnNext(leftBatch);
            this.Observer.OnNext(rightBatch);

            var newGlobalPunctuation = Math.Min(newLeftGlobalPunctuation, newRightGlobalPunctuation);
            EmitGlobalPunctuationIfNecessary(newGlobalPunctuation);

            this.leftGlobalPunctuation = newLeftGlobalPunctuation;
            this.rightGlobalPunctuation = newRightGlobalPunctuation;
        }

        protected override void ProcessLeftBatch(StreamMessage<TKey, TPayload> leftBatch, out bool leftBatchDone, out bool leftBatchFree)
        {
            leftBatchDone = true;
            leftBatchFree = false;

            var newLeftGlobalPunctuation = Math.Max(this.leftGlobalPunctuation, ExtractGlobalPunctuations(leftBatch));
            this.Observer.OnNext(leftBatch);

            var newGlobalPunctuation = Math.Min(newLeftGlobalPunctuation, this.rightGlobalPunctuation);
            EmitGlobalPunctuationIfNecessary(newGlobalPunctuation);

            this.leftGlobalPunctuation = newLeftGlobalPunctuation;
        }

        protected override void ProcessRightBatch(StreamMessage<TKey, TPayload> rightBatch, out bool rightBatchDone, out bool rightBatchFree)
        {
            rightBatchDone = true;
            rightBatchFree = false;

            var newRightGlobalPunctuation = Math.Max(this.rightGlobalPunctuation, ExtractGlobalPunctuations(rightBatch));
            this.Observer.OnNext(rightBatch);

            var newGlobalPunctuation = Math.Min(this.leftGlobalPunctuation, newRightGlobalPunctuation);
            EmitGlobalPunctuationIfNecessary(newGlobalPunctuation);

            this.rightGlobalPunctuation = newRightGlobalPunctuation;
        }

        protected override void ProduceBinaryQueryPlan(PlanNode left, PlanNode right)
            => this.Observer.ProduceQueryPlan(new UnionPlanNode(left, right, this, typeof(TKey), typeof(TPayload), true, false, null));

        public override bool LeftInputHasState => false;
        public override bool RightInputHasState => false;
        public override int CurrentlyBufferedOutputCount => 0;

        private void EmitGlobalPunctuationIfNecessary(long time)
        {
            if (time > Math.Min(this.leftGlobalPunctuation, this.rightGlobalPunctuation))
            {
                this.pool.Get(out StreamMessage<TKey, TPayload> batch);
                batch.Allocate();
                batch.Add(vsync: time, vother: GlobalPunctuationOtherTime, key: default, payload: default);
                this.Observer.OnNext(batch);
            }
        }

        /// <summary>
        /// Removes all global punctuations from the batch, and returns the maximum found
        /// </summary>
        private long ExtractGlobalPunctuations(StreamMessage<TKey, TPayload> batch)
        {
            var count = batch.Count;
            long max = -1;
            bool writable = false;
            for (int i = 0; i < count; i++)
            {
                if (batch.vother.col[i] == GlobalPunctuationOtherTime)
                {
                    max = Math.Max(max, batch.vsync.col[i]);

                    // Remove the low watermark/punctuation by converting to a deleted data event
                    if (!writable)
                    {
                        batch.vother = batch.vother.MakeWritable(this.pool.longPool);
                        batch.bitvector = batch.bitvector.MakeWritable(this.pool.bitvectorPool);
                        writable = true;
                    }

                    batch.vother.col[i] = 0;
                    batch.bitvector.col[i >> 6] |= (1L << (i & 0x3f));
                }
            }

            return max;
        }
    }
}
