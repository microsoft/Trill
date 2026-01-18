// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class SessionWindowPipe<TKey, TPayload> : UnaryPipe<TKey, TPayload, TPayload>
    {
        private readonly MemoryPool<TKey, TPayload> pool;
        private readonly string errorMessages;

        [SchemaSerialization]
        private readonly long sessionTimeout;
        [SchemaSerialization]
        private readonly long maximumDuration;

        [DataMember]
        private StreamMessage<TKey, TPayload> output;

        private Queue<SessionThreshold> orderedKeys = new Queue<SessionThreshold>();
        [DataMember]
        private FastDictionary2<TKey, long> windowEndTimeDictionary = new FastDictionary2<TKey, long>();
        [DataMember]
        private FastDictionary2<TKey, long> lastDataTimeDictionary = new FastDictionary2<TKey, long>();
        [DataMember]
        private FastDictionary2<TKey, Queue<ActiveEvent>> stateDictionary = new FastDictionary2<TKey, Queue<ActiveEvent>>();

        [Obsolete("Used only by serialization. Do not call directly.")]
        public SessionWindowPipe() { }

        public SessionWindowPipe(IStreamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer, long sessionTimeout, long maximumDuration)
            : base(stream, observer)
        {
            this.sessionTimeout = sessionTimeout;
            this.maximumDuration = maximumDuration;
            this.pool = MemoryManager.GetMemoryPool<TKey, TPayload>(stream.Properties.IsColumnar);
            this.errorMessages = stream.ErrorMessages;
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new SessionWindowPlanNode(
                previous, this,
                typeof(TKey), typeof(TPayload), this.sessionTimeout, this.maximumDuration,
                false, this.errorMessages));

        private void ReachTime(int pIndex, long timestamp)
        {
            if (pIndex != -1 && this.maximumDuration < StreamEvent.InfinitySyncTime)
            {
                if (this.windowEndTimeDictionary.entries[pIndex].value == StreamEvent.InfinitySyncTime)
                {
                    long mod = timestamp % this.maximumDuration;
                    this.windowEndTimeDictionary.entries[pIndex].value = timestamp - mod + ((mod == 0 ? 1 : 2) * this.maximumDuration);
                }
                else if (this.windowEndTimeDictionary.entries[pIndex].value == StreamEvent.MaxSyncTime)
                {
                    this.windowEndTimeDictionary.entries[pIndex].value = timestamp - (timestamp % this.maximumDuration) + this.maximumDuration;
                }
            }

            while (this.orderedKeys.Count > 0)
            {
                var current = this.orderedKeys.Peek();
                this.lastDataTimeDictionary.Lookup(current.Key, out int cIndex);
                var threshold = this.lastDataTimeDictionary.entries[cIndex].value == long.MinValue
                    ? this.windowEndTimeDictionary.entries[cIndex].value
                    : CalculateThreshold(this.lastDataTimeDictionary.entries[cIndex].value, cIndex);

                // Skip stale threshold entries (can happen when threshold is updated and new entry is enqueued)
                if (current.Threshold != threshold)
                {
                    this.orderedKeys.Dequeue();
                    continue;
                }

                if (timestamp >= threshold)
                {
                    var queue = this.stateDictionary.entries[cIndex].value;
                    while (queue.Any())
                    {
                        var active = queue.Dequeue();

                        int ind = this.output.Count++;
                        this.output.vsync.col[ind] = threshold;
                        this.output.vother.col[ind] = active.Sync;
                        this.output.key.col[ind] = active.Key;
                        this.output[ind] = active.Payload;
                        this.output.hash.col[ind] = active.Hash;

                        if (this.output.Count == Config.DataBatchSize) FlushContents();
                    }
                    if (timestamp < this.lastDataTimeDictionary.entries[cIndex].value + this.sessionTimeout)
                        this.windowEndTimeDictionary.entries[cIndex].value = StreamEvent.MaxSyncTime;
                    else
                    {
                        this.windowEndTimeDictionary.Remove(current.Key);
                        this.lastDataTimeDictionary.Remove(current.Key);
                        this.stateDictionary.Remove(current.Key);
                    }

                    this.orderedKeys.Dequeue();
                }
                else break;
            }
        }

        private int AllocatePartition(TKey pKey)
        {
            this.windowEndTimeDictionary.Insert(pKey, StreamEvent.InfinitySyncTime);
            this.lastDataTimeDictionary.Insert(pKey, long.MinValue);
            return this.stateDictionary.Insert(pKey, new Queue<ActiveEvent>());
        }

        public override unsafe void OnNext(StreamMessage<TKey, TPayload> batch)
        {
            var count = batch.Count;

            fixed (long* bv = batch.bitvector.col)
            fixed (long* vsync = batch.vsync.col)
            fixed (long* vother = batch.vother.col)
            fixed (int* hash = batch.hash.col)
            {
                for (int i = 0; i < count; i++)
                {
                    if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                    {
                        if (vsync[i] > vother[i]) // We have an end edge
                        {
                            ReachTime(-1, vsync[i]);
                        }
                        else
                        {
                            // Check to see if the key is already being tracked
                            if (!this.lastDataTimeDictionary.Lookup(batch.key.col[i], out int keyIndex))
                                keyIndex = AllocatePartition(batch.key.col[i]);
                            ReachTime(keyIndex, vsync[i]);

                            // Check to see if advancing time removed the key
                            if (!this.lastDataTimeDictionary.Lookup(batch.key.col[i], out keyIndex))
                                keyIndex = AllocatePartition(batch.key.col[i]);

                            var newThreshold = CalculateThreshold(vsync[i], keyIndex);

                            if (!this.stateDictionary.entries[keyIndex].value.Any())
                            {
                                this.orderedKeys.Enqueue(new SessionThreshold { Key = batch.key.col[i], Threshold = newThreshold });
                            }
                            else
                            {
                                var oldThreshold = CalculateThreshold(this.lastDataTimeDictionary.entries[keyIndex].value, keyIndex);
                                if (newThreshold > oldThreshold)
                                {
                                    this.orderedKeys.Enqueue(new SessionThreshold { Key = batch.key.col[i], Threshold = newThreshold });
                                }
                            }

                            this.lastDataTimeDictionary.entries[keyIndex].value = vsync[i];
                            this.stateDictionary.entries[keyIndex].value.Enqueue(new ActiveEvent
                            {
                                Key = batch.key.col[i],
                                Sync = vsync[i],
                                Hash = hash[i],
                                Payload = batch.payload.col[i],
                            });

                            int ind = this.output.Count++;
                            this.output.vsync.col[ind] = vsync[i];
                            this.output.vother.col[ind] = StreamEvent.InfinitySyncTime;
                            this.output.key.col[ind] = batch.key.col[i];
                            this.output[ind] = batch.payload.col[i];
                            this.output.hash.col[ind] = hash[i];

                            if (this.output.Count == Config.DataBatchSize) FlushContents();
                        }
                    }
                    else if (vother[i] == long.MinValue)
                    {
                        ReachTime(-1, vsync[i]);

                        int ind = this.output.Count++;
                        this.output.vsync.col[ind] = vsync[i];
                        this.output.vother.col[ind] = long.MinValue;
                        this.output.key.col[ind] = batch.key.col[i];
                        this.output[ind] = batch.payload.col[i];
                        this.output.hash.col[ind] = hash[i];
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

        public override int CurrentlyBufferedOutputCount => this.output.Count;

        public override int CurrentlyBufferedInputCount
        {
            get
            {
                int count = 0;
                int iter = FastDictionary<TKey, Queue<ActiveEvent>>.IteratorStart;
                while (this.stateDictionary.Iterate(ref iter)) count += this.stateDictionary.entries[iter].value.Count();
                return count;
            }
        }

        protected override void UpdatePointers()
        {
            int iter = FastDictionary<TKey, long>.IteratorStart;
            var temp = new List<Tuple<TKey, long>>();
            while (this.lastDataTimeDictionary.Iterate(ref iter))
            {
                if (this.stateDictionary.entries[iter].value.Any())
                {
                    temp.Add(Tuple.Create(
                        this.windowEndTimeDictionary.entries[iter].key,
                        CalculateThreshold(this.lastDataTimeDictionary.entries[iter].value, iter)));
                }
            }
            foreach (var item in temp.OrderBy(o => o.Item2)) this.orderedKeys.Enqueue(new SessionThreshold { Key = item.Item1, Threshold = item.Item2 });
            base.UpdatePointers();
        }

        protected override void DisposeState()
        {
            this.output.Free();
            this.windowEndTimeDictionary.Clear();
            this.lastDataTimeDictionary.Clear();
            this.stateDictionary.Clear();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long CalculateThreshold(long lastDataTime, int keyIndex) =>
            Math.Min(lastDataTime + this.sessionTimeout, this.windowEndTimeDictionary.entries[keyIndex].value);

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
            public long Sync;

            public override string ToString() => "Key='" + this.Key + "', Payload='" + this.Payload;
        }

        [DataContract]
        private struct SessionThreshold
        {
            [DataMember]
            public long Threshold;

            [DataMember]
            public TKey Key;
        }
    }
}