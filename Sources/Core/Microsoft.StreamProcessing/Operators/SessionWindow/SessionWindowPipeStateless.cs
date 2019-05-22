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
        private readonly long sessionTimeout;
        [SchemaSerialization]
        private readonly long maximumDuration;

        [DataMember]
        private Queue<StreamMessage<TKey, TPayload>> batches = new Queue<StreamMessage<TKey, TPayload>>();

        [DataMember]
        private int windowStartIdx = 0;
        [DataMember]
        private long windowEndTime = StreamEvent.InfinitySyncTime;
        [DataMember]
        private long lastDataTime = long.MinValue;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public SessionWindowPipeStateless() { }

        public SessionWindowPipeStateless(IStreamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer, long sessionTimeout, long maximumDuration)
            : base(stream, observer)
        {
            this.sessionTimeout = sessionTimeout;
            this.maximumDuration = maximumDuration;

            this.errorMessages = stream.ErrorMessages;
            this.pool = MemoryManager.GetMemoryPool<TKey, TPayload>(stream.Properties.IsColumnar);

        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new SessionWindowPlanNode(
                previous, this,
                typeof(TKey), typeof(TPayload), this.sessionTimeout, this.maximumDuration,
                false, this.errorMessages));

        public override int CurrentlyBufferedInputCount => this.batches.Select(o => o.Count).Sum();

        public override int CurrentlyBufferedOutputCount => 0;

        public override unsafe void OnNext(StreamMessage<TKey, TPayload> batch)
        {
            var count = batch.Count;

            this.batches.Enqueue(batch);
            batch.vother = batch.vother.MakeWritable(this.pool.longPool);
            batch.bitvector = batch.bitvector.MakeWritable(this.pool.bitvectorPool);
            fixed (long* bv = batch.bitvector.col)
            fixed (long* vsync = batch.vsync.col)
            fixed (long* vother = batch.vother.col)
            {
                for (int i = 0; i < count; i++)
                {
                    if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                    {
                        if (vsync[i] > vother[i]) // We have an end edge
                        {
                            bv[i >> 6] |= (1L << (i & 0x3f));
                            ReachTime(vsync[i], false);
                        }
                        else
                        {
                            ReachTime(vsync[i], true);

                            this.lastDataTime = vsync[i];
                        }
                    }
                    else if (vother[i] == long.MinValue)
                    {
                        ReachTime(vsync[i], false);
                    }
                }
            }
        }

        private unsafe void ReachTime(long timestamp, bool isData)
        {
            if (isData && this.maximumDuration < StreamEvent.InfinitySyncTime)
            {

                if (this.windowEndTime == StreamEvent.InfinitySyncTime)
                {
                    long mod = timestamp % this.maximumDuration;
                    this.windowEndTime = timestamp - mod + ((mod == 0 ? 1 : 2) * this.maximumDuration);
                }
                else if (this.windowEndTime == StreamEvent.MaxSyncTime)
                {
                    this.windowEndTime = timestamp - (timestamp % this.maximumDuration) + this.maximumDuration;
                }

            }

            var threshhold = this.lastDataTime == long.MinValue
                ? this.windowEndTime
                : Math.Min(this.lastDataTime + this.sessionTimeout, this.windowEndTime);
            if (timestamp >= threshhold)
            {
                StreamMessage<TKey, TPayload> batch;
                while (this.batches.Any())
                {

                    batch = this.batches.Peek();

                    var count = batch.Count;
                    fixed (long* bv = batch.bitvector.col)
                    fixed (long* vsync = batch.vsync.col)
                    fixed (long* vother = batch.vother.col)
                    {
                        for (; this.windowStartIdx < count; this.windowStartIdx++)
                        {
                            if (vsync[this.windowStartIdx] >= threshhold)
                            {
                                this.windowEndTime = (timestamp < this.lastDataTime + this.sessionTimeout) ? StreamEvent.MaxSyncTime : StreamEvent.InfinitySyncTime;
                                if (vother[this.windowStartIdx] != StreamEvent.PunctuationOtherTime)
                                    return;
                            }
                            if ((bv[this.windowStartIdx >> 6] & (1L << (this.windowStartIdx & 0x3f))) == 0)
                                vother[this.windowStartIdx] = threshhold;
                        }
                        if (this.windowStartIdx == count)
                        {
                            this.windowStartIdx = 0;

                            this.Observer.OnNext(batch);

                            this.batches.Dequeue();
                        }
                    }
                }
            }
        }

        protected override void DisposeState()
        {
            foreach (var b in this.batches) b.Free();
            this.batches.Clear();
        }
    }
}