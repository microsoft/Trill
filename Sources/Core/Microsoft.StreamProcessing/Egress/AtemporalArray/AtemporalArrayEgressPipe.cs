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
    internal sealed class MonotonicArrayEgressPipe<TPayload> : EgressBoundary<Empty, TPayload, ArraySegment<TPayload>>
    {
        [DataMember]
        private SortedDictionary<long, List<TPayload>> toDelete = new SortedDictionary<long, List<TPayload>>();

        private readonly Func<TPayload[]> generator;
        [DataMember]
        private TPayload[] array;
        [DataMember]
        private int arrayLength;
        [DataMember]
        private int populationCount = 0;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public MonotonicArrayEgressPipe() { }

        public MonotonicArrayEgressPipe(
            Func<TPayload[]> generator,
            IObserver<ArraySegment<TPayload>> observer,
            QueryContainer container)
            : base(observer, container)
        {
            this.generator = generator;
            this.array = this.generator();
            this.arrayLength = this.array.Length;
        }

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
                    this.array[this.populationCount++] = batch[i];
                    if (this.populationCount == this.arrayLength)
                    {
                        this.observer.OnNext(new ArraySegment<TPayload>(this.array, 0, this.arrayLength));
                        this.populationCount = 0;
                        this.array = this.generator();
                        this.arrayLength = this.array.Length;
                    }
                }
                else if (currentSync < col_vother[i])
                {
                    // Interval: create an insertion event now, and a deletion later when time progresses
                    this.array[this.populationCount++] = batch[i];
                    if (this.populationCount == this.arrayLength)
                    {
                        this.observer.OnNext(new ArraySegment<TPayload>(this.array, 0, this.arrayLength));
                        this.populationCount = 0;
                        this.array = this.generator();
                        this.arrayLength = this.array.Length;
                    }
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

        public override void OnCompleted()
        {
            OnFlush();
            base.OnCompleted();
        }

        public override void OnFlush()
        {
            if (this.populationCount > 0)
            {
                this.observer.OnNext(new ArraySegment<TPayload>(this.array, 0, this.arrayLength));
                this.populationCount = 0;
                this.array = this.generator();
                this.arrayLength = this.array.Length;
            }
        }

        public override int CurrentlyBufferedOutputCount => this.populationCount;

        public override int CurrentlyBufferedInputCount => this.toDelete.Values.Select(o => o.Count).Sum();
    }
}