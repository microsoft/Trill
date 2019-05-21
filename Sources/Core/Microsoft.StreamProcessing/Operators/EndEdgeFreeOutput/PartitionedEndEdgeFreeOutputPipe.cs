// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class PartitionedEndEdgeFreeOutputPipe<TKey, TPayload, TPartitionKey> : UnaryPipe<TKey, TPayload, TPayload>
    {
        private readonly MemoryPool<TKey, TPayload> pool;
        private readonly Func<TKey, TPartitionKey> getPartitionKey = GetPartitionExtractor<TPartitionKey, TKey>();
        private readonly string errorMessages;

        [DataMember]
        private StreamMessage<TKey, TPayload> output;

        [DataMember]
        private FastDictionary<TPartitionKey, PartitionEntry> partitionData = new FastDictionary<TPartitionKey, PartitionEntry>();

        private readonly DataStructurePool<FastDictionary2<ActiveEvent, int>> dictPool;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public PartitionedEndEdgeFreeOutputPipe() { }

        public PartitionedEndEdgeFreeOutputPipe(IStreamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer)
            : base(stream, observer)
        {
            this.pool = MemoryManager.GetMemoryPool<TKey, TPayload>(stream.Properties.IsColumnar);
            this.errorMessages = stream.ErrorMessages;

            var compoundEqualityExpr = EqualityComparerExtensions.GetCompoundEqualityComparerExpression<ActiveEvent, long, TKey, TPayload>(
                e => e.End, EqualityComparerExpression<long>.Default, e => e.Key, stream.Properties.KeyEqualityComparer, e => e.Payload, stream.Properties.PayloadEqualityComparer);
            var equals = compoundEqualityExpr.GetEqualsExpr().Compile();
            var getHashCode = compoundEqualityExpr.GetGetHashCodeExpr().Compile();
            var generator = compoundEqualityExpr.CreateFastDictionary2Generator<ActiveEvent, int>(1, equals, getHashCode, stream.Properties.QueryContainer);
            this.dictPool = new DataStructurePool<FastDictionary2<ActiveEvent, int>>(() => generator.Invoke());
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        protected override void DisposeState()
        {
            this.output.Free();
            this.dictPool.Dispose();
        }

        private void OutputCompletedIntervals(PartitionEntry partition)
        {
            // Output all completed intervals
            var delList = new List<long>();
            foreach (var kvp in partition.eventMap)
            {
                var index = FastDictionary2<ActiveEvent, int>.IteratorStart;
                while (kvp.Value.Iterate(ref index))
                {
                    var outevt = kvp.Value.entries[index].key;
                    if (outevt.End == StreamEvent.InfinitySyncTime)
                    {
                        foreach (var key in delList)
                            partition.eventMap.Remove(key);
                        return;
                    }

                    for (int j = 0; j < kvp.Value.entries[index].value; j++)
                    {
                        var ind = this.output.Count++;
                        this.output.vsync.col[ind] = kvp.Key;
                        this.output.vother.col[ind] = outevt.End;
                        this.output.key.col[ind] = outevt.Key;
                        this.output[ind] = outevt.Payload;
                        this.output.hash.col[ind] = outevt.Hash;
                        partition.lastSyncTime = kvp.Key;

                        if (this.output.Count == Config.DataBatchSize) FlushContents();
                    }
                    kvp.Value.Remove(outevt);
                }

                this.dictPool.Return(kvp.Value);
                delList.Add(kvp.Key);
            }
            foreach (var key in delList)
                partition.eventMap.Remove(key);
        }

        private void OutputAllEvents(PartitionEntry partition)
        {
            // Output all collected intervals
            foreach (var kvp in partition.eventMap)
            {
                var index = FastDictionary2<ActiveEvent, int>.IteratorStart;
                while (kvp.Value.Iterate(ref index))
                {
                    var outevt = kvp.Value.entries[index].key;

                    for (int j = 0; j < kvp.Value.entries[index].value; j++)
                    {
                        var ind = this.output.Count++;
                        this.output.vsync.col[ind] = kvp.Key;
                        this.output.vother.col[ind] = outevt.End;
                        this.output.key.col[ind] = outevt.Key;
                        this.output[ind] = outevt.Payload;
                        this.output.hash.col[ind] = outevt.Hash;

                        if (this.output.Count == Config.DataBatchSize) FlushContents();
                    }
                }
            }
        }

        public override unsafe void OnNext(StreamMessage<TKey, TPayload> batch)
        {
            var count = batch.Count;
            batch.bitvector = batch.bitvector.MakeWritable(this.pool.bitvectorPool);

            fixed (long* bv = batch.bitvector.col)
            {
                for (int i = 0; i < count; i++)
                {
                    if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0 || batch.vother.col[i] < 0)
                    {
                        if (batch.vother.col[i] == PartitionedStreamEvent.LowWatermarkOtherTime)
                        {
                            int index = FastDictionary<TPartitionKey, PartitionEntry>.IteratorStart;
                            while (this.partitionData.Iterate(ref index))
                            {
                                var p = this.partitionData.entries[index].value;
                                if (batch.vsync.col[i] == StreamEvent.InfinitySyncTime)
                                    OutputAllEvents(p);
                                else
                                    OutputCompletedIntervals(p);

                                p.lastCti = Math.Max(batch.vsync.col[i], p.lastCti);
                                p.lastSyncTime = Math.Max(batch.vsync.col[i], p.lastSyncTime);
                            }

                            AddLowWatermarkToBatch(batch.vother.col[i]);
                            continue;
                        }

                        var partitionKey = this.getPartitionKey(batch.key.col[i]);
                        PartitionEntry partition;
                        if (!this.partitionData.Lookup(partitionKey, out int tempIndex))
                            this.partitionData.Insert(ref tempIndex, partitionKey, partition = new PartitionEntry());
                        else partition = this.partitionData.entries[tempIndex].value;

                        if (batch.vother.col[i] == PartitionedStreamEvent.PunctuationOtherTime)
                        {
                            partition.lastCti = batch.vsync.col[i];
                            partition.lastSyncTime = batch.vsync.col[i];
                        }
                        else if (batch.vsync.col[i] < batch.vother.col[i]) // Start edge or interval
                        {
                            var evt = new ActiveEvent
                            {
                                End = batch.vother.col[i],
                                Payload = batch[i],
                                Key = batch.key.col[i],
                                Hash = batch.hash.col[i]
                            };

                            if (!partition.eventMap.TryGetValue(batch.vsync.col[i], out FastDictionary2<ActiveEvent, int> entry))
                            {
                                this.dictPool.Get(out entry);
                                partition.eventMap.Add(batch.vsync.col[i], entry);
                            }
                            if (!entry.Lookup(evt, out int index))
                            {
                                entry.Insert(evt, 1);
                            }
                            else
                                entry.entries[index].value++;
                        }
                        else // end edge
                        {
                            // lookup corresponding start edge
                            var lookupevt = new ActiveEvent
                            {
                                End = StreamEvent.InfinitySyncTime,
                                Payload = batch[i],
                                Key = batch.key.col[i],
                                Hash = batch.hash.col[i]
                            };

                            if (!partition.eventMap.TryGetValue(batch.vother.col[i], out FastDictionary2<ActiveEvent, int> entry))
                                throw new InvalidOperationException("Found end edge without corresponding start edge");

                            if (!entry.Lookup(lookupevt, out int index))
                                throw new InvalidOperationException("Found end edge without corresponding start edge");

                            // Set interval payload to the payload of the original start-edge
                            // (in case they are different due to an optimized payload equality comparer)
                            lookupevt.Payload = entry.entries[index].key.Payload;

                            // delete the start edge
                            entry.entries[index].value--;
                            if (entry.entries[index].value == 0)
                            {
                                entry.Remove(lookupevt);
                            }

                            // insert interval
                            lookupevt.End = batch.vsync.col[i];

                            if (!entry.Lookup(lookupevt, out index))
                            {
                                entry.Insert(lookupevt, 1);
                            }
                            else
                            {
                                entry.entries[index].value++;
                            }
                            OutputCompletedIntervals(partition); // Can make this more efficient by trying only if the first event in index got completed
                        }
                    }
                }
            }

            batch.Free();
        }

        private void AddLowWatermarkToBatch(long start)
        {
            int index = this.output.Count++;
            this.output.vsync.col[index] = start;
            this.output.vother.col[index] = PartitionedStreamEvent.LowWatermarkOtherTime;
            this.output.key.col[index] = default;
            this.output[index] = default;
            this.output.hash.col[index] = 0;
            this.output.bitvector.col[index >> 6] |= (1L << (index & 0x3f));

            if (this.output.Count == Config.DataBatchSize) FlushContents();
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new EndEdgeFreeOutputPlanNode(
                previous, this, typeof(TKey), typeof(TPayload), false, this.errorMessages));

        protected override void FlushContents()
        {
            if (this.output.Count == 0) return;
            this.output.Seal();
            this.Observer.OnNext(this.output);
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        public override int CurrentlyBufferedOutputCount => this.output.Count;

        public override int CurrentlyBufferedInputCount
        {
            get
            {
                var count = 0;
                var outer = FastDictionary<TPartitionKey, PartitionEntry>.IteratorStart;
                while (this.partitionData.Iterate(ref outer))
                {
                    foreach (var tuple in this.partitionData.entries[outer].value.eventMap)
                    {
                        var iter = FastDictionary2<ActiveEvent, int>.IteratorStart;
                        while (tuple.Value.Iterate(ref iter)) count += tuple.Value.entries[iter].value;
                    }
                }
                return count;
            }
        }

        [DataContract]
        private struct ActiveEvent
        {
            [DataMember]
            public long End;
            [DataMember]
            public TPayload Payload;
            [DataMember]
            public TKey Key;
            [DataMember]
            public int Hash;

            public override string ToString()
                => "[End=" + this.End + ", Key='" + this.Key + "', Payload='" + this.Payload + "']";
        }

        [DataContract]
        private sealed class PartitionEntry
        {
            [DataMember]
            public SortedDictionary<long, FastDictionary2<ActiveEvent, int>> eventMap = new SortedDictionary<long, FastDictionary2<ActiveEvent, int>>();
            [DataMember]
            public long lastSyncTime = StreamEvent.MinSyncTime;
            [DataMember]
            public long lastCti = StreamEvent.MinSyncTime;
        }
    }
}
