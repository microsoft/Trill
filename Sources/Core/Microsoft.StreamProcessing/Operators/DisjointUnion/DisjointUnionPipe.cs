// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class DisjointUnionPipe<TKey, TPayload> : BinaryPipe<TKey, TPayload, TPayload, TPayload>
    {
        [Obsolete("Used only by serialization. Do not call directly.")]
        public DisjointUnionPipe() { }

        public DisjointUnionPipe(Streamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer)
            : base(stream, observer)
        {
        }

        protected override void ProcessBothBatches(StreamMessage<TKey, TPayload> leftBatch, StreamMessage<TKey, TPayload> rightBatch, out bool leftBatchDone, out bool rightBatchDone, out bool leftBatchFree, out bool rightBatchFree)
        {
            ProcessLeftBatch(leftBatch, out leftBatchDone, out leftBatchFree);
            ProcessRightBatch(rightBatch, out rightBatchDone, out rightBatchFree);
        }

        protected override void ProcessLeftBatch(StreamMessage<TKey, TPayload> leftBatch, out bool leftBatchDone, out bool leftBatchFree)
        {
            leftBatchDone = true;
            leftBatchFree = false;
            this.Observer.OnNext(leftBatch);
        }

        protected override void ProcessRightBatch(StreamMessage<TKey, TPayload> rightBatch, out bool rightBatchDone, out bool rightBatchFree)
        {
            rightBatchDone = true;
            rightBatchFree = false;
            this.Observer.OnNext(rightBatch);
        }

        protected override void ProduceBinaryQueryPlan(PlanNode left, PlanNode right)
        {
            var node = new UnionPlanNode(
                left, right, this, typeof(TKey), typeof(TPayload), true, false, null);
            this.Observer.ProduceQueryPlan(node);
        }

        public override bool LeftInputHasState => false;
        public override bool RightInputHasState => false;
        public override int CurrentlyBufferedOutputCount => 0;
    }
}
