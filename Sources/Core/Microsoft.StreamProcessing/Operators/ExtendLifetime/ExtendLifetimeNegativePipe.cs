// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class ExtendLifetimeNegativePipe<TKey, TPayload> : UnaryPipe<TKey, TPayload, TPayload>
    {
        private readonly MemoryPool<TKey, TPayload> pool;
        private readonly string errorMessages;

        [SchemaSerialization]
        private readonly long duration;

        [DataMember]
        private StreamMessage<TKey, TPayload> output;

        [DataMember]
        private FastMap<ActiveEvent> syncTimeMap = new FastMap<ActiveEvent>();
        [DataMember]
        private EndPointHeap endPointHeap = new EndPointHeap();
        [DataMember]
        private Dictionary<long, List<ActiveEvent>> contractedToZero = new Dictionary<long, List<ActiveEvent>>();

        [SchemaSerialization]
        private readonly Expression<Func<TKey, TKey, bool>> keyComparerExpr;
        private readonly Func<TKey, TKey, bool> keyComparer;
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, TPayload, bool>> payloadComparerExpr;
        private readonly Func<TPayload, TPayload, bool> payloadComparer;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public ExtendLifetimeNegativePipe() { }

        public ExtendLifetimeNegativePipe(ExtendLifetimeStreamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer, long duration)
            : base(stream, observer)
        {
            this.duration = duration;

            this.keyComparerExpr = stream.Properties.KeyEqualityComparer.GetEqualsExpr();
            this.keyComparer = this.keyComparerExpr.Compile();

            this.payloadComparerExpr = stream.Properties.PayloadEqualityComparer.GetEqualsExpr();
            this.payloadComparer = this.payloadComparerExpr.Compile();

            this.pool = MemoryManager.GetMemoryPool<TKey, TPayload>(stream.Properties.IsColumnar);
            this.errorMessages = stream.ErrorMessages;
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new ExtendLifetimePlanNode(
                previous, this,
                typeof(TKey), typeof(TPayload),
                false, this.errorMessages, true));

        private void ReachTime(long timestamp)
        {
            while (this.endPointHeap.TryGetNextExclusive(timestamp, out long endPointTime, out int index))
            {
                var activeEvent = this.syncTimeMap.Values[index];
                bool sendToOutput = true;
                if (this.contractedToZero.TryGetValue(endPointTime, out var found))
                {
                    for (int i = 0; i < found.Count; i++)
                    {
                        if (activeEvent.Other != StreamEvent.InfinitySyncTime) continue;
                        var f = found[i];
                        if (this.payloadComparer(f.Payload, activeEvent.Payload)
                         && this.keyComparer(f.Key, activeEvent.Key)
                         && f.Other == activeEvent.Sync)
                        {
                            // We have a start edge that has been marked for deletion
                            found.RemoveAt(i);
                            if (found.Count == 0) this.contractedToZero.Remove(endPointTime);
                            sendToOutput = false;
                            break;
                        }
                    }
                }

                if (!sendToOutput) continue;
                int ind = this.output.Count++;
                {
                    this.output.vsync.col[ind] = activeEvent.Sync;
                    this.output.vother.col[ind] = activeEvent.Other;
                    this.output.key.col[ind] = activeEvent.Key;
                    this.output[ind] = activeEvent.Payload;
                    this.output.hash.col[ind] = activeEvent.Hash;
                }

                if (this.output.Count == Config.DataBatchSize) FlushContents();

                this.syncTimeMap.Remove(index);
            }
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
                        if (batch.vsync.col[i] >= StreamEvent.MinSyncTime + this.duration) ReachTime(batch.vsync.col[i] - this.duration);

                        if (batch.vother.col[i] == StreamEvent.InfinitySyncTime)
                        {
                            // For start events, we queue them up
                            // until we are [duration] time past to make sure that
                            // no end edges have been moved earlier
                            int index = this.syncTimeMap.Insert(batch.hash.col[i]);
                            this.syncTimeMap.Values[index].Populate(batch.key.col[i], batch[i], batch.vsync.col[i], StreamEvent.InfinitySyncTime, batch.hash.col[i]);
                            this.endPointHeap.Insert(batch.vsync.col[i], index);
                        }
                        else if (batch.vother.col[i] > batch.vsync.col[i])
                        {
                            // For intervals, we clip the duration as well before queueing
                            // Events with durations of zero or less are dropped
                            var sync = batch.vsync.col[i];
                            var other = batch.vother.col[i] - this.duration;
                            if (other <= sync) continue;
                            int index = this.syncTimeMap.Insert(batch.hash.col[i]);
                            this.syncTimeMap.Values[index].Populate(batch.key.col[i], batch[i], sync, other, batch.hash.col[i]);
                            this.endPointHeap.Insert(batch.vsync.col[i], index);
                        }
                        else
                        {
                            var sync = batch.vsync.col[i] - this.duration;
                            var other = batch.vother.col[i];

                            if (other >= sync)
                            {
                                // If we have a contracted event, do not add the end edge to the batch.
                                // Also, add the payload to a list of events to be purged.
                                this.contractedToZero.Add(other, new ActiveEvent
                                {
                                    Key = batch.key.col[i],
                                    Payload = batch[i],
                                    Sync = sync,
                                    Other = other,
                                    Hash = batch.hash.col[i]
                                });
                            }
                            else
                            {
                                int ind = this.output.Count++;
                                this.output.vsync.col[ind] = sync;
                                this.output.vother.col[ind] = other;
                                this.output.key.col[ind] = batch.key.col[i];
                                this.output[ind] = batch[i];
                                this.output.hash.col[ind] = batch.hash.col[i];

                                if (this.output.Count == Config.DataBatchSize) FlushContents();
                            }
                        }
                    }
                    else if (batch.vother.col[i] == long.MinValue)
                    {
                        long syncTime = (batch.vsync.col[i] == StreamEvent.InfinitySyncTime ? StreamEvent.InfinitySyncTime : batch.vsync.col[i] - this.duration);
                        ReachTime(syncTime);

                        int ind = this.output.Count++;
                        this.output.vsync.col[ind] = syncTime;
                        this.output.vother.col[ind] = long.MinValue;
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

        public override int CurrentlyBufferedInputCount => this.syncTimeMap.Count + this.contractedToZero.Values.Select(o => o.Count).Sum();

        [DataContract]
        private struct ActiveEvent
        {
            [DataMember]
            public TPayload Payload;
            [DataMember]
            public TKey Key;
            [DataMember]
            public long Sync;
            [DataMember]
            public long Other;
            [DataMember]
            public int Hash;

            public void Populate(TKey key, TPayload payload, long sync, long other, int hash)
            {
                this.Key = key;
                this.Payload = payload;
                this.Sync = sync;
                this.Other = other;
                this.Hash = hash;
            }

            public override string ToString() => "Key='" + this.Key + "', Payload='" + this.Payload;
        }
    }
}