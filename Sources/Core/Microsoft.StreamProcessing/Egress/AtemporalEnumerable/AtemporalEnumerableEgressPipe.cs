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
    internal sealed class AtemporalEnumerableEgressPipe<TPayload> : EgressBoundary<Empty, TPayload, IEnumerable<ChangeListEvent<TPayload>>>
    {
        [DataMember]
        private long currentTimestamp = long.MinValue;
        [DataMember]
        private SortedDictionary<long, List<TPayload>> toDelete = new SortedDictionary<long, List<TPayload>>();
        [DataMember]
        private List<ChangeListEvent<TPayload>> currentVersion = new List<ChangeListEvent<TPayload>>();

        [Obsolete("Used only by serialization. Do not call directly.")]
        public AtemporalEnumerableEgressPipe() { }

        public AtemporalEnumerableEgressPipe(IObserver<IEnumerable<ChangeListEvent<TPayload>>> observer, QueryContainer container)
            : base(observer, container) { }

        public override void OnNext(StreamMessage<Empty, TPayload> batch)
        {
            var col_bv = batch.bitvector.col;
            var col_vsync = batch.vsync.col;
            var col_vother = batch.vother.col;

            for (int i = 0; i < batch.Count; i++)
            {
                var currentSync = col_vsync[i];
                Process(currentSync);

                if ((col_bv[i >> 6] & (1L << (i & 0x3f))) != 0) continue;
                if (col_vother[i] == StreamEvent.InfinitySyncTime)
                {
                    // Start edge: create an insertion event
                    this.currentVersion.Add(ChangeListEvent.CreateInsertion(batch[i]));
                }
                else if (currentSync < col_vother[i])
                {
                    // Interval: create an insertion event now, and a deletion later when time progresses
                    this.currentVersion.Add(ChangeListEvent.CreateInsertion(batch[i]));
                    EnqueueDelete(col_vother[i], batch[i]);
                }
                else
                {
                    // End edge: create a deletion event
                    this.currentVersion.Add(ChangeListEvent.CreateDeletion(batch[i]));
                }
            }

            batch.Free();
        }

        private void Process(long timestamp)
        {
            while (true)
            {
                if (!this.toDelete.TryGetFirst(out long currentTime, out List<TPayload> queue)) break;

                if (currentTime < timestamp)
                {
                    foreach (var item in queue)
                    {
                        this.currentVersion.Add(ChangeListEvent.CreateDeletion(item));
                    }

                    this.toDelete.Remove(currentTime);
                }
                else
                {
                    break;
                }
            }

            if (timestamp > this.currentTimestamp)
            {
                this.currentTimestamp = timestamp;
                if (this.currentVersion.Count > 0)
                {
                    this.observer.OnNext(this.currentVersion);
                    this.currentVersion = new List<ChangeListEvent<TPayload>>();
                }
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
}
