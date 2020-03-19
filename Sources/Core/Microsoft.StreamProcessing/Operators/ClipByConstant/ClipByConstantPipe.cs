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
    internal sealed class ClipByConstantPipe<TKey, TPayload> : UnaryPipe<TKey, TPayload, TPayload>
    {
        private readonly MemoryPool<TKey, TPayload> pool;
        private readonly string errorMessages;

        [SchemaSerialization]
        private readonly long limit;

        [DataMember]
        private StreamMessage<TKey, TPayload> output;

        [DataMember]
        private SortedDictionary<long, MultiSet<ActiveEvent>> syncTimeMap = new SortedDictionary<long, MultiSet<ActiveEvent>>();

        [Obsolete("Used only by serialization. Do not call directly.")]
        public ClipByConstantPipe() { }

        public ClipByConstantPipe(ClipByConstantStreamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer, long limit)
            : base(stream, observer)
        {
            this.limit = limit;
            this.pool = MemoryManager.GetMemoryPool<TKey, TPayload>(stream.Properties.IsColumnar);
            this.errorMessages = stream.ErrorMessages;
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new ClipByConstantPlanNode(
                previous, this,
                typeof(TKey), typeof(TPayload),
                false, this.errorMessages));

        private void ReachTime(long timestamp)
        {
            var toDelete = new List<long>();
            foreach (var kvp in this.syncTimeMap)
            {
                if (timestamp < kvp.Key + this.limit) break;

                foreach (var ae in kvp.Value.GetEnumerable())
                {
                    int ind = this.output.Count++;
                    this.output.vsync.col[ind] = kvp.Key + this.limit;
                    this.output.vother.col[ind] = kvp.Key;
                    this.output.key.col[ind] = ae.Key;
                    this.output[ind] = ae.Payload;
                    this.output.hash.col[ind] = ae.Hash;

                    if (this.output.Count == Config.DataBatchSize) FlushContents();
                }

                toDelete.Add(kvp.Key);
            }

            foreach (var l in toDelete) this.syncTimeMap.Remove(l);
        }

        public override unsafe void OnNext(StreamMessage<TKey, TPayload> batch)
        {
            var count = batch.Count;

            fixed (long* bv = batch.bitvector.col)
            {
                for (int i = 0; i < count; i++)
                {
                    if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                    {
                        ReachTime(batch.vsync.col[i]);

                        if (batch.vother.col[i] == StreamEvent.InfinitySyncTime)
                        {
                            // For start events, we copy directly to the output batch
                            // and add them to the list of events that may need to be clipped
                            int ind = this.output.Count++;
                            var sync = batch.vsync.col[i];
                            this.output.vsync.col[ind] = sync;
                            this.output.vother.col[ind] = StreamEvent.InfinitySyncTime;
                            this.output.key.col[ind] = batch.key.col[i];
                            this.output[ind] = batch[i];
                            this.output.hash.col[ind] = batch.hash.col[i];

                            if (this.output.Count == Config.DataBatchSize) FlushContents();

                            if (!this.syncTimeMap.TryGetValue(sync, out var multiSet))
                            {
                                multiSet = new MultiSet<ActiveEvent>();
                                this.syncTimeMap.Add(sync, multiSet);
                            }
                            multiSet.Add(new ActiveEvent { Hash = batch.hash.col[i], Key = batch.key.col[i], Payload = batch[i] });
                        }
                        else if (batch.vother.col[i] > batch.vsync.col[i])
                        {
                            // For intervals, we clip the limit and copy to the output batch
                            int ind = this.output.Count++;
                            this.output.vsync.col[ind] = batch.vsync.col[i];
                            this.output.vother.col[ind] = Math.Min(batch.vother.col[i], batch.vsync.col[i] + this.limit);
                            this.output.key.col[ind] = batch.key.col[i];
                            this.output[ind] = batch[i];
                            this.output.hash.col[ind] = batch.hash.col[i];

                            if (this.output.Count == Config.DataBatchSize) FlushContents();
                        }
                        else
                        {
                            var sync = batch.vsync.col[i];
                            var other = batch.vother.col[i];

                            // For end edges, if the delta is greater than the limit, then ignore,
                            // otherwise copy directly over
                            if (other + this.limit <= sync) continue;

                            var payload = batch[i];
                            int ind = this.output.Count++;
                            this.output.vsync.col[ind] = sync;
                            this.output.vother.col[ind] = other;
                            this.output.key.col[ind] = batch.key.col[i];
                            this.output[ind] = payload;
                            this.output.hash.col[ind] = batch.hash.col[i];

                            if (this.output.Count == Config.DataBatchSize) FlushContents();

                            // Remove the corresponding start edge from the waiting list
                            this.syncTimeMap[other].Remove(new ActiveEvent { Payload = payload, Key = batch.key.col[i], Hash = batch.hash.col[i] });
                            if (this.syncTimeMap[other].IsEmpty) this.syncTimeMap.Remove(other);
                        }
                    }
                    else if (batch.vother.col[i] == StreamEvent.PunctuationOtherTime)
                    {
                        ReachTime(batch.vsync.col[i]);

                        int ind = this.output.Count++;
                        this.output.vsync.col[ind] = batch.vsync.col[i];
                        this.output.vother.col[ind] = StreamEvent.PunctuationOtherTime;
                        this.output.key.col[ind] = batch.key.col[i];
                        this.output[ind] = default;
                        this.output.hash.col[ind] = batch.hash.col[i];
                        this.output.bitvector.col[ind >> 6] |= 1L << (ind & 0x3f);

                        if (this.output.Count == Config.DataBatchSize) FlushContents();
                    }
                }
            }
            batch.Free();
        }

        protected override void FlushContents()
        {
            if (this.output.Count == 0) return;
            this.output.Seal();
            this.Observer.OnNext(this.output);
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        protected override void DisposeState() => this.output.Free();

        public override int CurrentlyBufferedOutputCount => this.output.Count;

        public override int CurrentlyBufferedInputCount => this.syncTimeMap.Values.Select(o => (int)o.Count).Sum();

        [DataContract]
        private struct ActiveEvent
        {
            [DataMember]
            public TPayload Payload;
            [DataMember]
            public TKey Key;
            [DataMember]
            public int Hash;

            public override string ToString() => "Key='" + this.Key + "', Payload='" + this.Payload;
        }
    }

}