// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.Linq.Expressions;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    // Version of AlterLifetime with vother func as () => c
    [DataContract]
    internal sealed class AlterLifetimeConstantDurationPipe<TKey, TPayload> : UnaryPipe<TKey, TPayload, TPayload>, IStreamObserver<TKey, TPayload>
    {
        private readonly MemoryPool<TKey, TPayload> pool;

        [SchemaSerialization]
        private readonly long constantDurationSelector;
        [SchemaSerialization]
        private readonly Expression<Func<long, long>> startTimeSelector;
        private readonly Func<long, long> startTimeSelectorCompiled;

        [DataMember]
        private long lastSync = StreamEvent.MinSyncTime;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public AlterLifetimeConstantDurationPipe() { }

        public AlterLifetimeConstantDurationPipe(AlterLifetimeStreamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer)
            : base(stream, observer)
        {
            Contract.Requires(stream != null);

            this.constantDurationSelector = (long)((ConstantExpression)stream.DurationSelector.Body).Value;
            this.startTimeSelector = (Expression<Func<long, long>>)stream.StartTimeSelector;
            this.startTimeSelectorCompiled = this.startTimeSelector.Compile();
            this.pool = MemoryManager.GetMemoryPool<TKey, TPayload>(stream.Properties.IsColumnar);
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new AlterLifetimePlanNode(
                previous, this,
                typeof(TKey), typeof(TPayload), this.startTimeSelector, Expression.Constant(this.constantDurationSelector),
                true));

        public unsafe override void OnNext(StreamMessage<TKey, TPayload> batch)
        {
            batch.vsync = batch.vsync.MakeWritable(this.pool.longPool);
            batch.vother = batch.vother.MakeWritable(this.pool.longPool);
            batch.bitvector = batch.bitvector.MakeWritable(this.pool.bitvectorPool);

            var count = batch.Count;

            fixed (long* vsync = batch.vsync.col)
            fixed (long* vother = batch.vother.col)
            fixed (long* bv = batch.bitvector.col)
            {
                for (int i = 0; i < count; i++)
                {
                    if ((bv[i >> 6] & (1L << (i & 0x3f))) != 0)
                    {
                        if (vother[i] == long.MinValue)
                        {
                            if ((this.startTimeSelector != null) && (vsync[i] < StreamEvent.InfinitySyncTime))
                            {
                                // create a new punctuation for the modified timestamp
                                vsync[i] = this.startTimeSelectorCompiled(vsync[i]);
                                if (vsync[i] > StreamEvent.MaxSyncTime) throw new ArgumentOutOfRangeException();
                            }

                            this.lastSync = Math.Max(vsync[i], this.lastSync);
                        }
                        continue;
                    }

                    if (vsync[i] < vother[i])
                    {
                        // insert event
                        if (this.startTimeSelector != null)
                        {
                            vsync[i] = this.startTimeSelectorCompiled(vsync[i]);
                        }

                        if (this.constantDurationSelector == StreamEvent.InfinitySyncTime)
                        {
                            vother[i] = StreamEvent.InfinitySyncTime;
                        }
                        else
                        {
                            vother[i] = vsync[i] + this.constantDurationSelector;
                            if (vother[i] > StreamEvent.MaxSyncTime) throw new ArgumentOutOfRangeException();
                        }
                    }
                    else
                    {
                        if (vother[i] != long.MinValue) // not a CTI
                        {
                            // drop the retract
                            bv[i >> 6] |= (1L << (i & 0x3f));
                            continue;
                        }
                        else
                        {
                            if (this.startTimeSelector != null)
                            {
                                vsync[i] = this.startTimeSelectorCompiled(vsync[i]);
                            }
                        }
                    }
                    if (vsync[i] < this.lastSync)
                    {
                        throw new StreamProcessingOutOfOrderException(
                            "The operator AlterLifetime produced output out of sync-time order on an input event. The current internal sync time is " + this.lastSync.ToString(CultureInfo.InvariantCulture)
                            + ". The event's sync time is " + vsync[i].ToString(CultureInfo.InvariantCulture)
                            + ". The event's value is " + batch[i].ToString() + ".");
                    }

                    this.lastSync = vsync[i];
                }
            }

            this.Observer.OnNext(batch);
        }

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }
}
