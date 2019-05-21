// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class MonotonicEgressPipe<TPayload> : EgressBoundary<Empty, TPayload, TPayload>
    {
        [DataMember]
        private SortedDictionary<long, List<TPayload>> toDelete = new SortedDictionary<long, List<TPayload>>();

        [Obsolete("Used only by serialization. Do not call directly.")]
        public MonotonicEgressPipe() { }

        public MonotonicEgressPipe(
            IObserver<TPayload> observer,
            QueryContainer container)
            : base(observer, container) { }

        public override void OnNext(StreamMessage<Empty, TPayload> batch)
        {
            var col_bv = batch.bitvector.col;
            var col_vsync = batch.vsync.col;
            var col_vother = batch.vother.col;

            for (int i = 0; i < batch.Count; i++)
            {
                var currentSync = col_vsync[i];
                ProcessDeletions(currentSync);
                if ((col_bv[i >> 6] & (1L << (i & 0x3f))) != 0) continue;
                if (col_vother[i] == StreamEvent.InfinitySyncTime)
                {
                    // Start edge: create an insertion event
                    this.observer.OnNext(batch[i]);
                }
                else if (currentSync < col_vother[i])
                {
                    // Interval: create an insertion event now, and a deletion later when time progresses
                    this.observer.OnNext(batch[i]);
                    EnqueueDelete(col_vother[i], batch[i]);
                }
                else
                {
                    // End edge: throw, because we expect the data to be monotonic
                    throw new StreamProcessingException("The query has encountered either an end edge or an interval, while the egress point expects only start edges.");
                }
            }
            batch.Free();
        }

        private void ProcessDeletions(long timestamp)
        {
            while (true)
            {
                if (!this.toDelete.TryGetFirst(out long currentTime, out List<TPayload> queue)) return;

                if (currentTime <= timestamp)
                {
                    // End edge: throw, because we expect the data to be monotonic
                    if (queue.Any())
                        throw new StreamProcessingException("The query has encountered either an end edge or an interval, while the egress point expects only start edges.");
                    this.toDelete.Remove(currentTime);
                }
                else return;
            }
        }

        private void EnqueueDelete(long currentTime, TPayload payload)
        {
            if (!this.toDelete.TryGetValue(currentTime, out List<TPayload> queue))
            {
                queue = new List<TPayload>();
                this.toDelete.Add(currentTime, queue);
            }
            queue.Add(payload);
        }

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => this.toDelete.Values.Select(o => o.Count).Sum();
    }
    [DataContract]
    internal sealed class ReactiveEgressPipe<TPayload> : EgressBoundary<Empty, TPayload, TPayload>
    {

        [Obsolete("Used only by serialization. Do not call directly.")]
        public ReactiveEgressPipe() { }

        public ReactiveEgressPipe(
            IObserver<TPayload> observer,
            QueryContainer container)
            : base(observer, container) { }

        public override void OnNext(StreamMessage<Empty, TPayload> batch)
        {
            var col_bv = batch.bitvector.col;
            var col_vsync = batch.vsync.col;
            var col_vother = batch.vother.col;

            for (int i = 0; i < batch.Count; i++)
            {
                var currentSync = col_vsync[i];
                if ((col_bv[i >> 6] & (1L << (i & 0x3f))) != 0) continue;
                if (col_vother[i] == StreamEvent.InfinitySyncTime)
                {
                    // Start edge: create an insertion event
                    this.observer.OnNext(batch[i]);
                }
                else if (currentSync < col_vother[i])
                {
                    // Interval: create an insertion event now, and a deletion later when time progresses
                    this.observer.OnNext(batch[i]);
                }
            }
            batch.Free();
        }

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }
}