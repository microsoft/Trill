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
    internal sealed class PartitionedExtendLifetimeNegativePipe<TKey, TPayload, TPartitionKey> : UnaryPipe<TKey, TPayload, TPayload>
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
        private FastDictionary<TPartitionKey, FastMap<ActiveEvent>> syncTimeMapDictionary = new FastDictionary<TPartitionKey, FastMap<ActiveEvent>>();
        [DataMember]
        private FastDictionary<TPartitionKey, EndPointHeap> endPointHeapDictionary = new FastDictionary<TPartitionKey, EndPointHeap>();
        [DataMember]
        private FastDictionary<TPartitionKey, Dictionary<long, List<ActiveEvent>>> contractedToZeroDictionary = new FastDictionary<TPartitionKey, Dictionary<long, List<ActiveEvent>>>();

        [Obsolete("Used only by serialization. Do not call directly.")]
        public PartitionedExtendLifetimeNegativePipe() { }

        public PartitionedExtendLifetimeNegativePipe(Streamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer, long duration)
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
                false, this.errorMessages, true));

        private void ReachTime(long timestamp)
        {
            int partitionKey = FastDictionary<TPartitionKey, long>.IteratorStart;
            while (this.lastSyncTimeDictionary.Iterate(ref partitionKey)) ReachTime(this.lastSyncTimeDictionary.entries[partitionKey].key, timestamp);
        }

        private void ReachTime(TPartitionKey pKey, long timestamp)
        {
            this.endPointHeapDictionary.Lookup(pKey, out int heapIndex);
            var endPointHeap = this.endPointHeapDictionary.entries[heapIndex].value;

            this.syncTimeMapDictionary.Lookup(pKey, out int mapIndex);
            var syncTimeMap = this.syncTimeMapDictionary.entries[mapIndex].value;

            this.lastSyncTimeDictionary.Lookup(pKey, out int timeIndex);

            this.contractedToZeroDictionary.Lookup(pKey, out int contractedIndex);
            var contractedToZero = this.contractedToZeroDictionary.entries[contractedIndex].value;

            while (endPointHeap.TryGetNextExclusive(timestamp, out long endPointTime, out int index))
            {
                bool sendToOutput = true;
                if (contractedToZero.TryGetValue(endPointTime, out List<ActiveEvent> found))
                {
                    for (int i = 0; i < found.Count; i++)
                    {
                        if (syncTimeMap.Values[index].Other != StreamEvent.InfinitySyncTime) continue;
                        var f = found[i];
                        if (f.Payload.Equals(syncTimeMap.Values[index].Payload)
                         && f.Key.Equals(syncTimeMap.Values[index].Key)
                         && f.Other == syncTimeMap.Values[index].Sync)
                        {
                            // We have a start edge that has been marked for deletion
                            found.RemoveAt(i);
                            if (found.Count == 0) contractedToZero.Remove(endPointTime);
                            sendToOutput = false;
                            break;
                        }
                    }
                }

                if (!sendToOutput) continue;
                int ind = this.output.Count++;
                this.output.vsync.col[ind] = syncTimeMap.Values[index].Sync;
                this.output.vother.col[ind] = syncTimeMap.Values[index].Other;
                this.output.key.col[ind] = syncTimeMap.Values[index].Key;
                this.output[ind] = syncTimeMap.Values[index].Payload;
                this.output.hash.col[ind] = syncTimeMap.Values[index].Hash;

                if (this.output.Count == Config.DataBatchSize) FlushContents();

                syncTimeMap.Remove(index);
            }

            this.lastSyncTimeDictionary.entries[timeIndex].value = timestamp;
        }

        private void AllocatePartition(TPartitionKey pKey, long timestamp)
        {
            this.endPointHeapDictionary.Lookup(pKey, out int heapIndex);
            this.endPointHeapDictionary.Insert(ref heapIndex, pKey, new EndPointHeap());
            this.syncTimeMapDictionary.Lookup(pKey, out int mapIndex);
            this.syncTimeMapDictionary.Insert(ref mapIndex, pKey, new FastMap<ActiveEvent>());
            this.lastSyncTimeDictionary.Lookup(pKey, out int timeIndex);
            this.lastSyncTimeDictionary.Insert(ref timeIndex, pKey, timestamp);
            this.contractedToZeroDictionary.Lookup(pKey, out int collapseIndex);
            this.contractedToZeroDictionary.Insert(ref collapseIndex, pKey, new Dictionary<long, List<ActiveEvent>>());
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
                        else if (batch.vsync.col[i] >= StreamEvent.MinSyncTime + this.duration
                            && batch.vsync.col[i] > this.lastSyncTimeDictionary.entries[timeIndex].value) ReachTime(partition, batch.vsync.col[i] - this.duration);

                        if (batch.vother.col[i] == StreamEvent.InfinitySyncTime)
                        {
                            this.endPointHeapDictionary.Lookup(partition, out int heapIndex);
                            var endPointHeap = this.endPointHeapDictionary.entries[heapIndex].value;

                            this.syncTimeMapDictionary.Lookup(partition, out int mapIndex);
                            var syncTimeMap = this.syncTimeMapDictionary.entries[mapIndex].value;

                            // For start events, we queue them up
                            // until we are [duration] time past to make sure that
                            // no end edges have been moved earlier
                            int index = syncTimeMap.Insert(batch.hash.col[i]);
                            syncTimeMap.Values[index].Populate(batch.key.col[i], batch[i], batch.vsync.col[i], StreamEvent.InfinitySyncTime, batch.hash.col[i]);
                            endPointHeap.Insert(batch.vsync.col[i], index);
                        }
                        else if (batch.vother.col[i] > batch.vsync.col[i])
                        {
                            // For intervals, we clip the duration as well before queueing
                            // Events with durations of zero or less are dropped
                            var sync = batch.vsync.col[i];
                            var other = batch.vother.col[i] - this.duration;
                            if (other <= sync) continue;

                            this.endPointHeapDictionary.Lookup(partition, out int heapIndex);
                            var endPointHeap = this.endPointHeapDictionary.entries[heapIndex].value;

                            this.syncTimeMapDictionary.Lookup(partition, out int mapIndex);
                            var syncTimeMap = this.syncTimeMapDictionary.entries[mapIndex].value;

                            int index = syncTimeMap.Insert(batch.hash.col[i]);
                            syncTimeMap.Values[index].Populate(batch.key.col[i], batch[i], sync, other, batch.hash.col[i]);
                            endPointHeap.Insert(batch.vsync.col[i], index);
                        }
                        else
                        {
                            var sync = batch.vsync.col[i] - this.duration;
                            var other = batch.vother.col[i];

                            if (other >= sync)
                            {
                                this.contractedToZeroDictionary.Lookup(partition, out int contractedIndex);
                                var contractedToZero = this.contractedToZeroDictionary.entries[contractedIndex].value;

                                // If we have a contracted event, do not add the end edge to the batch.
                                // Also, add the payload to a list of events to be purged.
                                contractedToZero.Add(other, new ActiveEvent
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
                    else if (batch.vother.col[i] == PartitionedStreamEvent.LowWatermarkOtherTime)
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
                    else if (batch.vother.col[i] == PartitionedStreamEvent.PunctuationOtherTime)
                    {
                        var partition = this.getPartitionKey(batch.key.col[i]);
                        long syncTime = batch.vsync.col[i] == StreamEvent.InfinitySyncTime ? StreamEvent.InfinitySyncTime : batch.vsync.col[i] - this.duration;
                        ReachTime(partition, syncTime);

                        int ind = this.output.Count++;
                        this.output.vsync.col[ind] = syncTime;
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
                int iter = FastDictionary<TPartitionKey, FastMap<ActiveEvent>>.IteratorStart;
                while (this.syncTimeMapDictionary.Iterate(ref iter)) count += this.syncTimeMapDictionary.entries[iter].value.Count;
                iter = FastDictionary<TPartitionKey, Dictionary<long, List<ActiveEvent>>>.IteratorStart;
                while (this.contractedToZeroDictionary.Iterate(ref iter)) count += this.contractedToZeroDictionary.entries[iter].value.Values.Select(o => o.Count).Sum();
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