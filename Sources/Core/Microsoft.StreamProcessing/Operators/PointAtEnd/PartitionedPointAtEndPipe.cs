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
    internal sealed class PartitionedPointAtEndPipe<TKey, TPayload, TPartitionKey> : UnaryPipe<TKey, TPayload, TPayload>
    {
        private readonly MemoryPool<TKey, TPayload> pool;
        private readonly string errorMessages;
        private readonly Func<TKey, TPartitionKey> getPartitionKey = GetPartitionExtractor<TPartitionKey, TKey>();

        [DataMember]
        private StreamMessage<TKey, TPayload> output;

        [DataMember]
        private FastDictionary<TPartitionKey, long> lastSyncTime = new FastDictionary<TPartitionKey, long>();
        [DataMember]
        private FastDictionary<TPartitionKey, EndPointHeap> endPointHeapDictionary = new FastDictionary<TPartitionKey, EndPointHeap>();
        [DataMember]
        private FastDictionary<TPartitionKey, FastMap<ActiveEvent>> intervalMapDictionary = new FastDictionary<TPartitionKey, FastMap<ActiveEvent>>();

        [Obsolete("Used only by serialization. Do not call directly.")]
        public PartitionedPointAtEndPipe() { }

        public PartitionedPointAtEndPipe(Streamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer)
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
            int index = FastDictionary<TPartitionKey, long>.IteratorStart;
            while (this.lastSyncTime.Iterate(ref index)) ReachTime(index, timestamp);
        }

        private void ReachTime(int pIndex, long timestamp)
        {
            var endPointHeap = this.endPointHeapDictionary.entries[pIndex].value;
            var intervalMap = this.intervalMapDictionary.entries[pIndex].value;
            while (endPointHeap.TryGetNextInclusive(timestamp, out long endPointTime, out int index))
            {
                int ind = this.output.Count++;
                this.output.vsync.col[ind] = endPointTime;
                this.output.vother.col[ind] = endPointTime + 1;
                this.output.key.col[ind] = intervalMap.Values[index].Key;
                this.output[ind] = intervalMap.Values[index].Payload;
                this.output.hash.col[ind] = intervalMap.Values[index].Hash;

                if (this.output.Count == Config.DataBatchSize) FlushContents();
                intervalMap.Remove(index);
            }

            this.lastSyncTime.entries[pIndex].value = timestamp;
        }

        private int AllocatePartition(TPartitionKey pKey, long timestamp)
        {
            this.endPointHeapDictionary.Lookup(pKey, out int heapIndex);
            this.endPointHeapDictionary.Insert(ref heapIndex, pKey, new EndPointHeap());
            this.intervalMapDictionary.Lookup(pKey, out int mapIndex);
            this.intervalMapDictionary.Insert(ref mapIndex, pKey, new FastMap<ActiveEvent>());
            this.lastSyncTime.Lookup(pKey, out int timeIndex);
            this.lastSyncTime.Insert(ref timeIndex, pKey, timestamp);
            return timeIndex;
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
                        var partition = this.getPartitionKey(batch.key.col[i]);
                        if (!this.lastSyncTime.Lookup(partition, out int timeIndex)) timeIndex = AllocatePartition(partition, batch.vsync.col[i]);
                        else if (batch.vsync.col[i] > this.lastSyncTime.entries[timeIndex].value) ReachTime(timeIndex, batch.vsync.col[i]);

                        if (batch.vother.col[i] == StreamEvent.InfinitySyncTime) continue;
                        else if (batch.vother.col[i] < batch.vsync.col[i]) // End edge
                        {
                            int ind = this.output.Count++;
                            this.output.vsync.col[ind] = batch.vsync.col[i];
                            this.output.vother.col[ind] = batch.vsync.col[i] + 1;
                            this.output.key.col[ind] = batch.key.col[i];
                            this.output[ind] = batch[i];
                            this.output.hash.col[ind] = batch.hash.col[i];

                            if (this.output.Count == Config.DataBatchSize) FlushContents();
                        }
                        else // Interval
                        {
                            var endPointHeap = this.endPointHeapDictionary.entries[timeIndex].value;
                            var intervalMap = this.intervalMapDictionary.entries[timeIndex].value;

                            int index = intervalMap.Insert(batch.hash.col[i]);
                            intervalMap.Values[index].Populate(batch.key.col[i], batch[i], batch.hash.col[i]);
                            endPointHeap.Insert(batch.vother.col[i], index);
                        }
                    }
                    else if (batch.vother.col[i] == PartitionedStreamEvent.LowWatermarkOtherTime)
                    {
                        ReachTime(batch.vsync.col[i]);

                        int ind = this.output.Count++;
                        this.output.vsync.col[ind] = batch.vsync.col[i];
                        this.output.vother.col[ind] = PartitionedStreamEvent.LowWatermarkOtherTime;
                        this.output.key.col[ind] = default;
                        this.output.payload.col[ind] = default;
                        this.output.hash.col[ind] = 0;
                        this.output.bitvector.col[ind >> 6] |= (1L << (ind & 0x3f));

                        if (this.output.Count == Config.DataBatchSize) FlushContents();
                    }
                    else if (batch.vother.col[i] == PartitionedStreamEvent.PunctuationOtherTime)
                    {
                        var partition = this.getPartitionKey(batch.key.col[i]);
                        if (!this.lastSyncTime.Lookup(partition, out int timeIndex)) timeIndex = AllocatePartition(partition, batch.vsync.col[i]);
                        else if (batch.vsync.col[i] > this.lastSyncTime.entries[timeIndex].value) ReachTime(timeIndex, batch.vsync.col[i]);

                        int ind = this.output.Count++;
                        this.output.vsync.col[ind] = batch.vsync.col[i];
                        this.output.vother.col[ind] = PartitionedStreamEvent.PunctuationOtherTime;
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

        public override int CurrentlyBufferedInputCount
        {
            get
            {
                int count = 0;
                int iter = FastDictionary<TPartitionKey, FastMap<ActiveEvent>>.IteratorStart;
                while (this.intervalMapDictionary.Iterate(ref iter)) count += this.intervalMapDictionary.entries[iter].value.Count;
                return count;
            }
        }

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