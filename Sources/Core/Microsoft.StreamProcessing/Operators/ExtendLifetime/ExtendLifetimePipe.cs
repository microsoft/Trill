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
    internal sealed class ExtendLifetimePipe<TKey, TPayload> : UnaryPipe<TKey, TPayload, TPayload>
    {
        private readonly MemoryPool<TKey, TPayload> pool;
        private readonly string errorMessages;

        [SchemaSerialization]
        private readonly long duration;

        [DataMember]
        private StreamMessage<TKey, TPayload> output;

        [DataMember]
        private long lastSyncTime = long.MinValue;
        [DataMember]
        private FastMap<ActiveEvent> endPointMap = new FastMap<ActiveEvent>();
        [DataMember]
        private EndPointHeap endPointHeap = new EndPointHeap();


        [Obsolete("Used only by serialization. Do not call directly.")]
        public ExtendLifetimePipe() { }

        public ExtendLifetimePipe(ExtendLifetimeStreamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer, long duration)
            : base(stream, observer)
        {
            this.duration = duration;
            this.pool = MemoryManager.GetMemoryPool<TKey, TPayload>(stream.Properties.IsColumnar);
            this.errorMessages = stream.ErrorMessages;
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new ExtendLifetimePlanNode(
                previous, this,
                typeof(TKey), typeof(TPayload),
                false, this.errorMessages, false));

        private void ReachTime(long timestamp)
        {
            while (this.endPointHeap.TryGetNextInclusive(timestamp, out long endPointTime, out int index))
            {
                int ind = this.output.Count++;
                var activeEvent = this.endPointMap.Values[index];
                this.output.vsync.col[ind] = endPointTime;
                this.output.vother.col[ind] = activeEvent.StartEdge;
                this.output.key.col[ind] = activeEvent.Key;
                this.output[ind] = activeEvent.Payload;
                this.output.hash.col[ind] = activeEvent.Hash;

                if (this.output.Count == Config.DataBatchSize) FlushContents();

                this.endPointMap.Remove(index);
            }

            this.lastSyncTime = timestamp;
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
                        if (batch.vsync.col[i] > this.lastSyncTime) ReachTime(batch.vsync.col[i]);

                        if (batch.vother.col[i] == StreamEvent.InfinitySyncTime) // For start events, copy directly across
                        {
                            int ind = this.output.Count++;
                            this.output.vsync.col[ind] = batch.vsync.col[i];
                            this.output.vother.col[ind] = StreamEvent.InfinitySyncTime;
                            this.output.key.col[ind] = batch.key.col[i];
                            this.output[ind] = batch[i];
                            this.output.hash.col[ind] = batch.hash.col[i];

                            if (this.output.Count == Config.DataBatchSize) FlushContents();
                        }
                        else if (batch.vother.col[i] > batch.vsync.col[i]) // For intervals, just extend the duration
                        {
                            int ind = this.output.Count++;
                            this.output.vsync.col[ind] = batch.vsync.col[i];
                            this.output.vother.col[ind] = batch.vother.col[i] + this.duration;
                            this.output.key.col[ind] = batch.key.col[i];
                            this.output[ind] = batch[i];
                            this.output.hash.col[ind] = batch.hash.col[i];

                            if (this.output.Count == Config.DataBatchSize) FlushContents();
                        }
                        else
                        {
                            int index = this.endPointMap.Insert(batch.hash.col[i]);
                            this.endPointMap.Values[index].Populate(batch.key.col[i], batch[i], batch.vother.col[i], batch.hash.col[i]);
                            this.endPointHeap.Insert(batch.vsync.col[i] + this.duration, index);

                        }
                    }
                    else if (batch.vother.col[i] == long.MinValue)
                    {
                        ReachTime(batch.vsync.col[i]);

                        int ind = this.output.Count++;
                        this.output.vsync.col[ind] = batch.vsync.col[i];
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

        public override int CurrentlyBufferedInputCount => this.endPointMap.Count;

        [DataContract]
        private struct ActiveEvent
        {
            [DataMember]
            public TPayload Payload;
            [DataMember]
            public TKey Key;
            [DataMember]
            public long StartEdge;
            [DataMember]
            public int Hash;

            public void Populate(TKey key, TPayload payload, long startEdge, int hash)
            {
                this.Key = key;
                this.Payload = payload;
                this.StartEdge = startEdge;
                this.Hash = hash;
            }

            public override string ToString() => "Key='" + this.Key + "', Payload='" + this.Payload;
        }
    }
}