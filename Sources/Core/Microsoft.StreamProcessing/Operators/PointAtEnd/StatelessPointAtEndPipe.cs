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
    internal sealed class StatelessPointAtEndPipe<TKey, TPayload> : UnaryPipe<TKey, TPayload, TPayload>
    {
        private readonly MemoryPool<TKey, TPayload> pool;
        private readonly bool isPartitioned;
        private readonly string errorMessages;
        private readonly long constantDuration;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public StatelessPointAtEndPipe() { }

        public StatelessPointAtEndPipe(IStreamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer, long constantDuration)
            : base(stream, observer)
        {
            this.pool = MemoryManager.GetMemoryPool<TKey, TPayload>(stream.Properties.IsColumnar);
            this.errorMessages = stream.ErrorMessages;
            this.constantDuration = constantDuration;
            this.isPartitioned = typeof(TKey).GetPartitionType() != null;
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new PointAtEndPlanNode(
                previous, this,
                typeof(TKey), typeof(TPayload),
                false, this.errorMessages));

        public override unsafe void OnNext(StreamMessage<TKey, TPayload> batch)
        {
            if (this.constantDuration == StreamEvent.InfinitySyncTime)
            {
                batch.Free();
                return;
            }

            var count = batch.Count;
            batch.vsync = batch.vsync.MakeWritable(this.pool.longPool);
            batch.vother = batch.vother.MakeWritable(this.pool.longPool);
            batch.bitvector = batch.bitvector.MakeWritable(this.pool.bitvectorPool);

            fixed (long* vsync = batch.vsync.col)
            fixed (long* vother = batch.vother.col)
            fixed (long* bv = batch.bitvector.col)
            {
                for (int i = 0; i < count; i++)
                {
                    if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                    {
                        if (vother[i] == StreamEvent.InfinitySyncTime) // start edge
                        {
                            bv[i >> 6] |= (1L << (i & 0x3f));
                        }
                        else if (vother[i] < vsync[i]) // end edge (converts to interval of length 1)
                        {
                            vother[i] = vsync[i] + 1;
                        }
                        else // interval
                        {
                            vsync[i] = vother[i];
                            vother[i]++;
                        }
                    }
                }
            }
            if (batch.RefreshCount() && !this.isPartitioned && (batch.Count == 0))
                batch.Free();
            else
                this.Observer.OnNext(batch);
        }

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }

}
