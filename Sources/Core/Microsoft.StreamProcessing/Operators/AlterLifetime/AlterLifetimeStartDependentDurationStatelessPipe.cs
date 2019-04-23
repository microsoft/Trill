// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    // Version of stateless AlterLifetime with vother func as (a) => c
    [DataContract]
    internal sealed class AlterLifetimeStartDependentDurationStatelessPipe<TKey, TPayload> : UnaryPipe<TKey, TPayload, TPayload>
    {
        private readonly MemoryPool<TKey, TPayload> pool;

        [SchemaSerialization]
        private readonly Expression<Func<long, long>> startTimeDurationSelector;
        private readonly Func<long, long> startTimeDurationSelectorCompiled;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public AlterLifetimeStartDependentDurationStatelessPipe() { }

        public AlterLifetimeStartDependentDurationStatelessPipe(AlterLifetimeStreamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer)
            : base(stream, observer)
        {
            Contract.Requires(stream != null);

            this.startTimeDurationSelector = (Expression<Func<long, long>>)stream.DurationSelector;
            this.startTimeDurationSelectorCompiled = this.startTimeDurationSelector.Compile();
            this.pool = MemoryManager.GetMemoryPool<TKey, TPayload>(stream.Properties.IsColumnar);
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new AlterLifetimePlanNode(
                previous, this,
                typeof(TKey), typeof(TPayload),
                null, this.startTimeDurationSelector,
                true));

        public unsafe override void OnNext(StreamMessage<TKey, TPayload> batch)
        {
            batch.vother = batch.vother.MakeWritable(this.pool.longPool);
            batch.bitvector = batch.bitvector.MakeWritable(this.pool.bitvectorPool);

            var count = batch.Count;

            fixed (long* vsync = batch.vsync.col)
            fixed (long* vother = batch.vother.col)
            fixed (long* bv = batch.bitvector.col)
            {
                for (int i = 0; i < count; i++)
                {
                    if ((bv[i >> 6] & (1L << (i & 0x3f))) != 0) continue;

                    if (vsync[i] < vother[i])
                    {
                        // insert event
                        vother[i] = vsync[i] + this.startTimeDurationSelectorCompiled(vsync[i]);
                        if (vother[i] > StreamEvent.MaxSyncTime) throw new ArgumentOutOfRangeException();
                        if (vother[i] <= vsync[i]) bv[i >> 6] |= (1L << (i & 0x3f));
                    }
                    else
                    {
                        if (vother[i] != long.MinValue) // not a CTI
                        {
                            // drop the retract
                            bv[i >> 6] |= (1L << (i & 0x3f));
                            continue;
                        }
                    }
                }
            }

            this.Observer.OnNext(batch);
        }

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }
}
