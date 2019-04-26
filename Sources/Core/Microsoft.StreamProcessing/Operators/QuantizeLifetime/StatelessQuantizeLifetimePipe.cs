// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class StatelessQuantizeLifetimePipe<TKey, TPayload> : UnaryPipe<TKey, TPayload, TPayload>
    {
        private readonly MemoryPool<TKey, TPayload> pool;

        [SchemaSerialization]
        private readonly long width;
        [SchemaSerialization]
        private readonly long skip;
        [SchemaSerialization]
        private readonly long progress;
        [SchemaSerialization]
        private readonly long offset;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public StatelessQuantizeLifetimePipe() { }

        public StatelessQuantizeLifetimePipe(IStreamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer, long width, long skip, long progress, long offset)
            : base(stream, observer)
        {
            this.pool = MemoryManager.GetMemoryPool<TKey, TPayload>(stream.Properties.IsColumnar);
            this.width = width;
            this.skip = skip;
            this.progress = progress;
            this.offset = offset;
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new QuantizeLifetimePlanNode(
                previous, this,
                typeof(TKey), typeof(TPayload), this.width, this.skip, this.progress, this.offset,
                false, null));

        public override unsafe void OnNext(StreamMessage<TKey, TPayload> batch)
        {
            var count = batch.Count;
            batch.vsync = batch.vsync.MakeWritable(this.pool.longPool);
            batch.vother = batch.vother.MakeWritable(this.pool.longPool);

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
                            vsync[i] = vsync[i] - ((vsync[i] - this.offset) % this.progress + this.progress) % this.progress;
                        }
                        else if (vother[i] < vsync[i]) // end edge
                        {
                            var temp = Math.Max(vsync[i] + this.skip - 1, vother[i] + this.width);
                            vsync[i] = temp - ((temp - (this.offset + this.width)) % this.skip + this.skip) % this.skip;
                            vother[i] = vother[i] - ((vother[i] - this.offset) % this.progress + this.progress) % this.progress;
                        }
                        else // interval
                        {
                            var temp = Math.Max(vother[i] + this.skip - 1, vsync[i] + this.width);
                            vother[i] = temp - ((temp - (this.offset + this.width)) % this.skip + this.skip) % this.skip;
                            vsync[i] = vsync[i] - ((vsync[i] - this.offset) % this.progress + this.progress) % this.progress;
                        }
                    }
                    else if (vother[i] == long.MinValue) // Punctuation
                    {
                        vsync[i] = vsync[i] - ((vsync[i] - this.offset) % this.progress + this.progress) % this.progress;
                    }
                }
            }

            this.Observer.OnNext(batch);
        }

        public override int CurrentlyBufferedOutputCount => 0;
        public override int CurrentlyBufferedInputCount => 0;
    }
}
