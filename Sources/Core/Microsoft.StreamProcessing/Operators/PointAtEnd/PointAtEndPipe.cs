// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class PointAtEndPipe<TKey, TPayload> : UnaryPipe<TKey, TPayload, TPayload>
    {
        private readonly MemoryPool<TKey, TPayload> pool;
        private readonly string errorMessages;

        [DataMember]
        private StreamMessage<TKey, TPayload> output;

        [DataMember]
        private long lastSyncTime = long.MinValue;
        [DataMember]
        private EndPointHeap endPointHeap = new EndPointHeap();
        [DataMember]
        private FastMap<ActiveEvent> intervalMap = new FastMap<ActiveEvent>();

        [Obsolete("Used only by serialization. Do not call directly.")]
        public PointAtEndPipe() { }

        public PointAtEndPipe(PointAtEndStreamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer)
            : base(stream, observer)
        {
            this.pool = MemoryManager.GetMemoryPool<TKey, TPayload>(stream.Properties.IsColumnar);
            this.errorMessages = stream.ErrorMessages;
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new PointAtEndPlanNode(
                previous, this,
                typeof(TKey), typeof(TPayload),
                false, this.errorMessages));

        private void ReachTime(long timestamp)
        {
            while (this.endPointHeap.TryGetNextInclusive(timestamp, out long endPointTime, out int index))
            {
                int ind = this.output.Count++;
                this.output.vsync.col[ind] = endPointTime;
                this.output.vother.col[ind] = endPointTime + 1;
                this.output.key.col[ind] = this.intervalMap.Values[index].Key;
                this.output[ind] = this.intervalMap.Values[index].Payload;
                this.output.hash.col[ind] = this.intervalMap.Values[index].Hash;

                if (this.output.Count == Config.DataBatchSize) FlushContents();

                this.intervalMap.Remove(index);
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

                        if (batch.vother.col[i] == StreamEvent.InfinitySyncTime)
                        {

                        }
                        else if (batch.vother.col[i] < batch.vsync.col[i])
                        {
                            int ind = this.output.Count++;
                            this.output.vsync.col[ind] = batch.vsync.col[i];
                            this.output.vother.col[ind] = batch.vsync.col[i] + 1;
                            this.output.key.col[ind] = batch.key.col[i];
                            this.output[ind] = batch[i];
                            this.output.hash.col[ind] = batch.hash.col[i];

                            if (this.output.Count == Config.DataBatchSize) FlushContents();
                        }
                        else
                        {
                            int index = this.intervalMap.Insert(batch.hash.col[i]);
                            this.intervalMap.Values[index].Populate(batch.key.col[i], batch[i], batch.hash.col[i]);
                            this.endPointHeap.Insert(batch.vother.col[i], index);
                        }
                    }
                    else if (batch.vother.col[i] == StreamEvent.PunctuationOtherTime)
                    {
                        ReachTime(batch.vsync.col[i]);

                        int ind = this.output.Count++;
                        this.output.vsync.col[ind] = batch.vsync.col[i];
                        this.output.vother.col[ind] = batch.vother.col[i];
                        this.output.key.col[ind] = batch.key.col[i];
                        this.output.payload.col[ind] = default;
                        this.output.hash.col[ind] = batch.hash.col[i];
                        this.output.bitvector.col[ind >> 6] |= (1L << (ind & 0x3f));

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

        public override int CurrentlyBufferedInputCount => this.intervalMap.Count;

        [DataContract]
        private struct ActiveEvent
        {
            [DataMember]
            public TPayload Payload;
            [DataMember]
            public TKey Key;
            [DataMember]
            public int Hash;

            public void Populate(TKey key, TPayload payload, int hash)
            {
                this.Key = key;
                this.Payload = payload;
                this.Hash = hash;
            }

            public override string ToString() => "Key='" + this.Key + "', Payload='" + this.Payload;
        }

    }
}