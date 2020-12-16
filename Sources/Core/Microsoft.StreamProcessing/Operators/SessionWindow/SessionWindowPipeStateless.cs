// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class SessionWindowPipeStateless<TKey, TPayload> : UnaryPipe<TKey, TPayload, TPayload>
    {
        private readonly MemoryPool<TKey, TPayload> pool;
        private readonly string errorMessages;

        [SchemaSerialization]
        private readonly long sessionDuration;

        private long currentWindowEndTime = 0;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public SessionWindowPipeStateless() { }

        public SessionWindowPipeStateless(IStreamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer, long sessionDuration)
            : base(stream, observer)
        {
            this.sessionDuration = sessionDuration;

            this.errorMessages = stream.ErrorMessages;
            this.pool = MemoryManager.GetMemoryPool<TKey, TPayload>(stream.Properties.IsColumnar);
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(
                    new SessionWindowPlanNode(
                        previous,
                        pipe: this,
                        keyType: typeof(TKey),
                        payloadType: typeof(TPayload),
                        sessionDuration: this.sessionDuration,
                        isGenerated: false,
                        errorMessages: this.errorMessages));

        public override int CurrentlyBufferedInputCount => 0;

        public override int CurrentlyBufferedOutputCount => 0;

        public override unsafe void OnNext(StreamMessage<TKey, TPayload> batch)
        {
            var count = batch.Count;

            // We reuse the incoming batch, making vother and bitvectore writable
            batch.vother = batch.vother.MakeWritable(this.pool.longPool);
            batch.bitvector = batch.bitvector.MakeWritable(this.pool.bitvectorPool);
            fixed (long* bv = batch.bitvector.col)
            fixed (long* vsync = batch.vsync.col)
            fixed (long* vother = batch.vother.col)
            {
                for (int i = 0; i < count; i++)
                {
                    // No action needed on punctuation, let them pass through
                    if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                    {
                        if (vsync[i] > vother[i])
                        {
                            // Remove end edges, they are no longer relevant
                            bv[i >> 6] |= (1L << (i & 0x3f));
                        }
                        else
                        {
                            // Start edge or interval - update vother in place.
                            if (vsync[i] >= this.currentWindowEndTime)
                            {
                                this.currentWindowEndTime = vsync[i] + this.sessionDuration;
                            }

                            vother[i] = this.currentWindowEndTime;
                        }
                    }
                }
            }

            this.Observer.OnNext(batch);
        }

        protected override void DisposeState()
        {
            // No action needed?
        }
    }
}