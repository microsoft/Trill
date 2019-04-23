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
    internal sealed class PartitionedQuantizeLifetimePipe<TKey, TPayload, TPartitionKey> : UnaryPipe<TKey, TPayload, TPayload>
    {
        private readonly MemoryPool<TKey, TPayload> pool;
        private readonly string errorMessages;
        private readonly Func<TKey, TPartitionKey> getPartitionKey = GetPartitionExtractor<TPartitionKey, TKey>();

        [SchemaSerialization]
        private readonly long width;
        [SchemaSerialization]
        private readonly long skip;
        [SchemaSerialization]
        private readonly long progress;
        [SchemaSerialization]
        private readonly long offset;

        [DataMember]
        private StreamMessage<TKey, TPayload> output;

        [DataMember]
        private FastDictionary<TPartitionKey, PartitionEntry> partitionData = new FastDictionary<TPartitionKey, PartitionEntry>();

        [Obsolete("Used only by serialization. Do not call directly.")]
        public PartitionedQuantizeLifetimePipe() { }

        public PartitionedQuantizeLifetimePipe(Streamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer, long width, long skip, long progress, long offset)
            : base(stream, observer)
        {
            this.pool = MemoryManager.GetMemoryPool<TKey, TPayload>(stream.Properties.IsColumnar);
            this.errorMessages = stream.ErrorMessages;
            this.pool.Get(out this.output);
            this.output.Allocate();

            this.width = width;
            this.skip = skip;
            this.progress = progress;
            this.offset = offset;
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new QuantizeLifetimePlanNode(
                previous, this,
                typeof(TKey), typeof(TPayload), this.width, this.skip, this.progress, this.offset,
                false, this.errorMessages));

        private int AllocatePartition(TPartitionKey pKey)
        {
            this.partitionData.Lookup(pKey, out int index);
            this.partitionData.Insert(ref index, pKey, new PartitionEntry());
            return index;
        }

        private void ReachTime(long timestamp)
        {
            int index = FastDictionary<TPartitionKey, PartitionEntry>.IteratorStart;
            while (this.partitionData.Iterate(ref index)) ReachTime(index, timestamp);
        }

        private void ReachTime(int pIndex, long timestamp)
        {
            var endPointHeap = this.partitionData.entries[pIndex].value.endPointHeap;
            var intervalMap = this.partitionData.entries[pIndex].value.intervalMap;
            while (endPointHeap.TryGetNextInclusive(timestamp, out long endPointTime, out int index))
            {
                int ind = this.output.Count++;
                this.output.vsync.col[ind] = endPointTime;
                this.output.vother.col[ind] = intervalMap.Values[index].Other;
                this.output.key.col[ind] = intervalMap.Values[index].Key;
                this.output[ind] = intervalMap.Values[index].Payload;
                this.output.hash.col[ind] = intervalMap.Values[index].Hash;

                if (this.output.Count == Config.DataBatchSize) FlushContents();

                intervalMap.Remove(index);
            }

            this.partitionData.entries[pIndex].value.lastSyncTime = timestamp;
        }

        public override unsafe void OnNext(StreamMessage<TKey, TPayload> batch)
        {
            var count = batch.Count;

            fixed (long* bv = batch.bitvector.col)
            fixed (long* vsync = batch.vsync.col)
            fixed (long* vother = batch.vother.col)
            {
                for (int i = 0; i < count; i++)
                {
                    if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                    {
                        var partition = this.getPartitionKey(batch.key.col[i]);
                        if (!this.partitionData.Lookup(partition, out int timeIndex)) timeIndex = AllocatePartition(partition);
                        else if (batch.vsync.col[i] > this.partitionData.entries[timeIndex].value.lastSyncTime) ReachTime(batch.vsync.col[i]);

                        if (batch.vother.col[i] == StreamEvent.InfinitySyncTime) // Start edge
                        {
                            int ind = this.output.Count++;
                            this.output.vsync.col[ind] = vsync[i] - ((vsync[i] - this.offset) % this.progress + this.progress) % this.progress;
                            this.output.vother.col[ind] = StreamEvent.InfinitySyncTime;
                            this.output.key.col[ind] = batch.key.col[i];
                            this.output[ind] = batch[i];
                            this.output.hash.col[ind] = batch.hash.col[i];

                            if (this.output.Count == Config.DataBatchSize) FlushContents();
                        }
                        else if (batch.vother.col[i] > batch.vsync.col[i]) // Interval
                        {
                            int ind = this.output.Count++;
                            this.output.vsync.col[ind] = vsync[i] - ((vsync[i] - this.offset) % this.progress + this.progress) % this.progress;
                            var temp = Math.Max(vother[i] + this.skip - 1, vsync[i] + this.width);
                            this.output.vother.col[ind] = temp - ((temp - (this.offset + this.width)) % this.skip + this.skip) % this.skip;
                            this.output.key.col[ind] = batch.key.col[i];
                            this.output[ind] = batch[i];
                            this.output.hash.col[ind] = batch.hash.col[i];

                            if (this.output.Count == Config.DataBatchSize) FlushContents();
                        }
                        else // End edge
                        {
                            var endPointHeap = this.partitionData.entries[timeIndex].value.endPointHeap;
                            var intervalMap = this.partitionData.entries[timeIndex].value.intervalMap;

                            var temp = Math.Max(vsync[i] + this.skip - 1, vother[i] + this.width);
                            int index = intervalMap.Insert(batch.hash.col[i]);
                            intervalMap.Values[index].Populate(batch.key.col[i], batch[i], batch.hash.col[i], vother[i] - ((vother[i] - this.offset) % this.progress + this.progress) % this.progress);
                            endPointHeap.Insert(temp - ((temp - (this.offset + this.width)) % this.skip + this.skip) % this.skip, index);
                        }
                    }
                    else if (batch.vother.col[i] == PartitionedStreamEvent.LowWatermarkOtherTime)
                    {
                        ReachTime(batch.vsync.col[i]);

                        int ind = this.output.Count++;
                        this.output.vsync.col[ind] = vsync[i] - ((vsync[i] - this.offset) % this.progress + this.progress) % this.progress;
                        this.output.vother.col[ind] = PartitionedStreamEvent.LowWatermarkOtherTime;
                        this.output.key.col[ind] = default;
                        this.output[ind] = default;
                        this.output.hash.col[ind] = 0;
                        this.output.bitvector.col[ind >> 6] |= 1L << (ind & 0x3f);

                        if (this.output.Count == Config.DataBatchSize) FlushContents();
                    }
                    else if (batch.vother.col[i] == PartitionedStreamEvent.PunctuationOtherTime)
                    {
                        var partition = this.getPartitionKey(batch.key.col[i]);
                        if (!this.partitionData.Lookup(partition, out int timeIndex)) timeIndex = AllocatePartition(partition);
                        ReachTime(timeIndex, batch.vsync.col[i]);

                        int ind = this.output.Count++;
                        this.output.vsync.col[ind] = vsync[i] - ((vsync[i] - this.offset) % this.progress + this.progress) % this.progress;
                        this.output.vother.col[ind] = long.MinValue;
                        this.output.key.col[ind] = batch.key.col[i];
                        this.output[ind] = default;
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
                int iter = FastDictionary<TPartitionKey, PartitionEntry>.IteratorStart;
                while (this.partitionData.Iterate(ref iter)) count += this.partitionData.entries[iter].value.intervalMap.Count;
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
            [DataMember]
            public long Other;

            public void Populate(TKey key, TPayload payload, int hash, long other)
            {
                this.Key = key;
                this.Payload = payload;
                this.Hash = hash;
                this.Other = other;
            }

            public override string ToString() => "Key='" + this.Key + "', Payload='" + this.Payload;
        }

        [DataContract]
        private sealed class PartitionEntry
        {
            [DataMember]
            public long lastSyncTime = long.MinValue;
            [DataMember]
            public EndPointHeap endPointHeap = new EndPointHeap();
            [DataMember]
            public FastMap<ActiveEvent> intervalMap = new FastMap<ActiveEvent>();

        }
    }
}
