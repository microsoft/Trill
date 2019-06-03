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
    internal sealed class PartitionedExtendLifetimePipe<TKey, TPayload, TPartitionKey> : UnaryPipe<TKey, TPayload, TPayload>
    {
        private readonly MemoryPool<TKey, TPayload> pool;
        private readonly string errorMessages;
        private readonly Func<TKey, TPartitionKey> getPartitionKey = GetPartitionExtractor<TPartitionKey, TKey>();

        [SchemaSerialization]
        private readonly long duration;

        [DataMember]
        private StreamMessage<TKey, TPayload> output;

        [DataMember]
        private FastDictionary<TPartitionKey, long> lastSyncTimeDictionary = new FastDictionary<TPartitionKey, long>();
        [DataMember]
        private FastDictionary<TPartitionKey, FastMap<ActiveEvent>> endPointMapDictionary = new FastDictionary<TPartitionKey, FastMap<ActiveEvent>>();
        [DataMember]
        private FastDictionary<TPartitionKey, EndPointHeap> endPointHeapDictionary = new FastDictionary<TPartitionKey, EndPointHeap>();

        [Obsolete("Used only by serialization. Do not call directly.")]
        public PartitionedExtendLifetimePipe() { }

        public PartitionedExtendLifetimePipe(Streamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer, long duration)
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
            int partitionKey = FastDictionary<TPartitionKey, long>.IteratorStart;
            while (this.lastSyncTimeDictionary.Iterate(ref partitionKey)) ReachTime(this.lastSyncTimeDictionary.entries[partitionKey].key, timestamp);
        }

        private void ReachTime(TPartitionKey pKey, long timestamp)
        {
            this.endPointHeapDictionary.Lookup(pKey, out int heapIndex);
            this.endPointMapDictionary.Lookup(pKey, out int mapIndex);
            this.lastSyncTimeDictionary.Lookup(pKey, out int timeIndex);

            var endPointHeap = this.endPointHeapDictionary.entries[heapIndex].value;
            var endPointMap = this.endPointMapDictionary.entries[mapIndex].value;
            while (endPointHeap.TryGetNextInclusive(timestamp, out long endPointTime, out int index))
            {
                int ind = this.output.Count++;
                this.output.vsync.col[ind] = endPointTime;
                this.output.vother.col[ind] = endPointMap.Values[index].StartEdge;
                this.output.key.col[ind] = endPointMap.Values[index].Key;
                this.output[ind] = endPointMap.Values[index].Payload;
                this.output.hash.col[ind] = endPointMap.Values[index].Hash;

                if (this.output.Count == Config.DataBatchSize) FlushContents();

                endPointMap.Remove(index);
            }

            this.lastSyncTimeDictionary.entries[timeIndex].value = timestamp;
        }

        private void AllocatePartition(TPartitionKey pKey, long timestamp)
        {
            this.endPointHeapDictionary.Lookup(pKey, out int heapIndex);
            this.endPointHeapDictionary.Insert(ref heapIndex, pKey, new EndPointHeap());
            this.endPointMapDictionary.Lookup(pKey, out int mapIndex);
            this.endPointMapDictionary.Insert(ref mapIndex, pKey, new FastMap<ActiveEvent>());
            this.lastSyncTimeDictionary.Lookup(pKey, out int timeIndex);
            this.lastSyncTimeDictionary.Insert(ref timeIndex, pKey, timestamp);
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
                        if (!this.lastSyncTimeDictionary.Lookup(partition, out int timeIndex))
                        {
                            AllocatePartition(partition, batch.vsync.col[i]);
                        }
                        else if (batch.vsync.col[i] > this.lastSyncTimeDictionary.entries[timeIndex].value) ReachTime(batch.vsync.col[i]);

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
                            this.endPointHeapDictionary.Lookup(partition, out int heapIndex);
                            var endPointHeap = this.endPointHeapDictionary.entries[heapIndex].value;

                            this.endPointMapDictionary.Lookup(partition, out int mapIndex);
                            var endPointMap = this.endPointMapDictionary.entries[mapIndex].value;

                            int index = endPointMap.Insert(batch.hash.col[i]);
                            endPointMap.Values[index].Populate(batch.key.col[i], batch[i], batch.vother.col[i], batch.hash.col[i]);
                            endPointHeap.Insert(batch.vsync.col[i] + this.duration, index);
                        }
                    }
                    else if (batch.vother.col[i] == PartitionedStreamEvent.LowWatermarkOtherTime)
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
                    else if (batch.vother.col[i] == PartitionedStreamEvent.PunctuationOtherTime)
                    {
                        var partition = this.getPartitionKey(batch.key.col[i]);
                        ReachTime(partition, batch.vsync.col[i]);

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

        public override int CurrentlyBufferedInputCount
        {
            get
            {
                int count = 0;
                int iter = FastDictionary<TPartitionKey, FastMap<ActiveEvent>>.IteratorStart;
                while (this.endPointMapDictionary.Iterate(ref iter)) count += this.endPointMapDictionary.entries[iter].value.Count;
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