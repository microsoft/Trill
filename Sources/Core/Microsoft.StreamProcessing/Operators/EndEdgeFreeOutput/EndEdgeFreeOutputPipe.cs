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
    internal sealed class EndEdgeFreeOutputPipe<TKey, TPayload> : UnaryPipe<TKey, TPayload, TPayload>
    {
        private readonly MemoryPool<TKey, TPayload> pool;
        private readonly string errorMessages;

        [DataMember]
        private StreamMessage<TKey, TPayload> output;

        [DataMember]
        private SortedDictionary<long, FastDictionary2<ActiveEvent, int>> eventMap;
        [DataMember]
        private long lastSyncTime;
        [DataMember]
        private long lastCti;

        private DataStructurePool<FastDictionary2<ActiveEvent, int>> dictPool;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public EndEdgeFreeOutputPipe() { }

        public EndEdgeFreeOutputPipe(EndEdgeFreeOutputStreamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer)
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

            this.eventMap = new SortedDictionary<long, FastDictionary2<ActiveEvent, int>>();
            this.lastSyncTime = StreamEvent.MinSyncTime;
            this.lastCti = StreamEvent.MinSyncTime;
        }

        protected override void DisposeState()
        {
            this.output.Free();
            this.dictPool.Dispose();
        }
        private void OutputCompletedIntervals()
        {
            // Output all completed intervals
            var delList = new List<long>();
            foreach (var kvp in this.eventMap)
            {
                var index = FastDictionary2<ActiveEvent, int>.IteratorStart;
                while (kvp.Value.Iterate(ref index))
                {
                    var outevt = kvp.Value.entries[index].key;
                    if (outevt.End == StreamEvent.InfinitySyncTime)
                    {
                        foreach (var key in delList) this.eventMap.Remove(key);
                        return;
                    }

                    for (int j = 0; j < kvp.Value.entries[index].value; j++)
                    {
                        int ind = this.output.Count++;
                        this.output.vsync.col[ind] = kvp.Key;
                        this.output.vother.col[ind] = outevt.End;
                        this.lastSyncTime = kvp.Key;
                        this.output.key.col[ind] = outevt.Key;
                        this.output[ind] = outevt.Payload;
                        this.output.hash.col[ind] = outevt.Hash;

                        if (this.output.Count == Config.DataBatchSize) FlushContents();
                    }
                    kvp.Value.Remove(outevt);
                }

                this.dictPool.Return(kvp.Value);
                delList.Add(kvp.Key);
            }
            foreach (var key in delList) this.eventMap.Remove(key);
        }

        private void OutputAllEvents()
        {
            // Output all collected intervals
            foreach (var kvp in this.eventMap)
            {
                var index = FastDictionary2<ActiveEvent, int>.IteratorStart;
                while (kvp.Value.Iterate(ref index))
                {
                    var outevt = kvp.Value.entries[index].key;

                    for (int j = 0; j < kvp.Value.entries[index].value; j++)
                    {
                        int ind = this.output.Count++;
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

            fixed (long* vsync = batch.vsync.col)
            fixed (long* vother = batch.vother.col)
            fixed (long* bv = batch.bitvector.col)
            {
                for (int i = 0; i < count; i++)
                {
                    if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0 || vother[i] == long.MinValue)
                    {
                        if (batch.vother.col[i] == long.MinValue) // Punctuation
                        {
                            if (vsync[i] == StreamEvent.InfinitySyncTime)
                                OutputAllEvents();
                            else
                                OutputCompletedIntervals();

                            this.lastCti = Math.Max(vsync[i], this.lastCti);
                            this.lastSyncTime = Math.Max(vsync[i], this.lastSyncTime);
                            AddPunctuationToBatch(batch.vsync.col[i]);
                        }
                        else if (vsync[i] < vother[i]) // Start edge or interval
                        {
                            var evt = new ActiveEvent
                            {
                                End = vother[i],
                                Payload = batch[i],
                                Key = batch.key.col[i],
                                Hash = batch.hash.col[i]
                            };

                            if (!this.eventMap.TryGetValue(vsync[i], out var entry))
                            {
                                this.dictPool.Get(out entry);
                                this.eventMap.Add(vsync[i], entry);
                            }
                            if (!entry.Lookup(evt, out int index))
                                entry.Insert(evt, 1);
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

                            if (!this.eventMap.TryGetValue(vother[i], out FastDictionary2<ActiveEvent, int> entry))
                                throw new InvalidOperationException("Found end edge without corresponding start edge");

                            if (!entry.Lookup(lookupevt, out int index))
                                throw new InvalidOperationException("Found end edge without corresponding start edge");

                            // Set interval payload to the payload of the original start-edge
                            // (in case they are different due to an optimized payload equality comparer)
                            lookupevt.Payload = entry.entries[index].key.Payload;

                            // delete the start edge
                            entry.entries[index].value--;
                            if (entry.entries[index].value == 0)
                                entry.Remove(lookupevt);

                            // insert interval
                            lookupevt.End = batch.vsync.col[i];

                            if (!entry.Lookup(lookupevt, out index))
                                entry.Insert(lookupevt, 1);
                            else
                                entry.entries[index].value++;
                            OutputCompletedIntervals(); // Can make this more efficient by trying only if the first event in index got completed
                        }
                    }
                }
            }

            batch.Free();
        }

        private void AddPunctuationToBatch(long start)
        {
            int index = this.output.Count++;
            this.output.vsync.col[index] = start;
            this.output.vother.col[index] = StreamEvent.PunctuationOtherTime;
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
                int count = 0;
                foreach (var tuple in this.eventMap)
                {
                    int iter = FastDictionary2<ActiveEvent, int>.IteratorStart;
                    while (tuple.Value.Iterate(ref iter)) count += tuple.Value.entries[iter].value;
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

            public override string ToString() => "[End=" + this.End + ", Key='" + this.Key + "', Payload='" + this.Payload + "']";
        }
    }
}
