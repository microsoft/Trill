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
    internal sealed class PartitionedStitchPipe<TKey, TPayload, TPartitionKey> : UnaryPipe<TKey, TPayload, TPayload>
    {
        private readonly string errorMessages;
        private readonly Func<TKey, TPartitionKey> getPartitionKey = GetPartitionExtractor<TPartitionKey, TKey>();

        // transient; don't need to contract it
        private readonly DataStructurePool<FastDictionary2<KHP, List<ActiveEvent>>> dictPool;
        private readonly MemoryPool<TKey, TPayload> pool;

        [DataMember]
        private StreamMessage<TKey, TPayload> batch;
        [DataMember]
        private int outputCount;

        [DataMember]
        private FastDictionary<TPartitionKey, FastDictionary2<KHP, int>> CurrentTimeOpenEventBuffer =
            new FastDictionary<TPartitionKey, FastDictionary2<KHP, int>>();
        private readonly Func<FastDictionary2<KHP, int>> CurrentTimeOpenEventBufferGenerator;
        [DataMember]
        private int CurrentTimeOpenEventBufferIndex;

        [DataMember]
        private FastDictionary<TPartitionKey, long> CurrentTimeOpenEventBufferTime =
            new FastDictionary<TPartitionKey, long>();
        [DataMember]
        private int CurrentTimeOpenEventBufferTimeIndex;

        [DataMember]
        private FastDictionary<TPartitionKey, long> now =
            new FastDictionary<TPartitionKey, long>();
        [DataMember]
        private int nowIndex;

        // ok, to deal with multisets:
        // SCENARIO: we see an incoming payload. It's the same as an existing payload. We should clearly be matching
        // on PAYLOAD, KEY.
        // The VALUE version in this dictionary has the ORIGINAL, EARLY Start Time
        [DataMember]
        private FastDictionary<TPartitionKey, FastDictionary2<KHP, List<ActiveEventExt>>> OpenEvents =
            new FastDictionary<TPartitionKey, FastDictionary2<KHP, List<ActiveEventExt>>>();
        private readonly Func<FastDictionary2<KHP, List<ActiveEventExt>>> OpenEventsGenerator;
        [DataMember]
        private int OpenEventsIndex;

        // This is a dictionary by time: It contains only elements that will expire in the future
        // The Purge event clears any events from the 'past' out
        // The End event moves an item from the OpenEvent to the ClosedEvent set
        [DataMember]
        private FastDictionary<TPartitionKey, SortedDictionary<long, FastDictionary2<KHP, List<ActiveEvent>>>> ClosedEvents =
            new FastDictionary<TPartitionKey, SortedDictionary<long, FastDictionary2<KHP, List<ActiveEvent>>>>();
        [DataMember]
        private int ClosedEventsIndex;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public PartitionedStitchPipe() { }

        public PartitionedStitchPipe(IStreamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer)
            : base(stream, observer)
        {
            this.errorMessages = stream.ErrorMessages;
            this.outputCount = 0;
            this.pool = MemoryManager.GetMemoryPool<TKey, TPayload>(stream.Properties.IsColumnar);
            this.pool.Get(out this.batch);
            this.batch.Allocate();

            var khpcomparer = EqualityComparerExtensions.GetCompoundEqualityComparerExpression<KHP, TKey, TPayload>(
                                    e => e.Key, stream.Properties.KeyEqualityComparer,
                                    e => e.Payload, stream.Properties.PayloadEqualityComparer);
            var equals = khpcomparer.GetEqualsExpr().Compile();
            var getHashCode = khpcomparer.GetGetHashCodeExpr().Compile();

            var generator1 = khpcomparer.CreateFastDictionary2Generator<KHP, List<ActiveEvent>>(1, equals, getHashCode, stream.Properties.QueryContainer);
            this.dictPool = new DataStructurePool<FastDictionary2<KHP, List<ActiveEvent>>>(() => generator1.Invoke());

            var generator2 = khpcomparer.CreateFastDictionary2Generator<KHP, int>(1, equals, getHashCode, stream.Properties.QueryContainer);
            this.CurrentTimeOpenEventBufferGenerator = () => generator2.Invoke();

            var generator3 = khpcomparer.CreateFastDictionary2Generator<KHP, List<ActiveEventExt>>(1, equals, getHashCode, stream.Properties.QueryContainer);
            this.OpenEventsGenerator = () => generator3.Invoke();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void InsertOrAppend<K, V>(FastDictionary2<K, List<V>> events, K key, V value)
        {
            List<V> lst;
            if (events.Lookup(key, out int indx))
            {
                lst = events.entries[indx].value;
                lst.Add(value);
            }
            else
            {
                lst = new List<V>();
                events.Insert(key, lst);
                lst.Add(value);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ActiveEvent RemoveOne(FastDictionary2<KHP, List<ActiveEvent>> events, KHP key)
        {
            if (!events.Lookup(key, out int indx))
                throw new InvalidOperationException("Can't remove if it's not already there!");

            var lst = events.entries[indx].value;
            var rv = lst[0];
            lst.RemoveAt(0);
            return rv;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ActiveEventExt RemoveOne(FastDictionary2<KHP, List<ActiveEventExt>> events, KHP key, long startMatch)
        {
            if (!events.Lookup(key, out int indx))
                throw new InvalidOperationException("Can't remove if it's not already there!");

            var lst = events.entries[indx].value;
            var itemIndex = lst.FindIndex(s => s.Start == startMatch);
            if (itemIndex > -1)
            {
                var item = lst[itemIndex];
                lst.RemoveAt(itemIndex);
                return item;
            }
            throw new InvalidOperationException("Can't remove if it's not in the item list!");
        }

        protected override void DisposeState()
        {
            this.batch.Free();
            this.dictPool.Dispose();
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new StitchPlanNode(
                previous, this,
                typeof(TKey), typeof(TPayload), false, this.errorMessages));

        private struct ActiveEvent
        {
            public long Start;
            public long End;
            public TPayload Payload;
            public TKey Key;
            public int Hash;

            public static ActiveEvent FromExt(ActiveEventExt item)
            {
                var rv = new ActiveEvent()
                {
                    Start = item.OriginalStart,
                    End = item.End,
                    Hash = item.Hash,
                    Key = item.Key,
                    Payload = item.Payload
                };
                return rv;
            }

            public override string ToString()
                => "[Start=" + this.Start + ", End=" + this.End + ", Key='" + this.Key + "', Payload='" + this.Payload + "']";
        }

        private struct ActiveEventExt
        {
            public long OriginalStart;
            public long Start;
            public long End;
            public TPayload Payload;
            public TKey Key;
            public int Hash;

            public override string ToString()
                => "[OriginalStart=" + this.OriginalStart + ", Start=" + this.Start + ", End=" + this.End + ", Key='" + this.Key + "', Payload='" + this.Payload + "']";
        }

        private struct KHP
        {
            public TPayload Payload;
            public TKey Key;
            public int Hash;

            public override string ToString()
                => "[Key='" + this.Key + "', Payload='" + this.Payload + "']";
        }

        protected override void FlushContents()
        {
            if (this.outputCount == 0) return;
            this.batch.Count = this.outputCount;
            this.Observer.OnNext(this.batch);
            this.pool.Get(out this.batch);
            this.batch.Allocate();
            this.outputCount = 0;
        }

        public override unsafe void OnNext(StreamMessage<TKey, TPayload> input)
        {
            var count = input.Count;

            fixed (long* src_bv = input.bitvector.col, src_vsync = input.vsync.col, src_vother = input.vother.col)
            fixed (int* src_hash = input.hash.col)
            {
                long* vsync = src_vsync;
                long* vother = src_vother;
                int* hash = src_hash;

                for (int i = 0; i < count; i++)
                {
                    if ((src_bv[i >> 6] & (1L << (i & 0x3f))) == 0 || *vother < 0)
                    {
                        var partitionKey = this.getPartitionKey(input.key.col[i]);
                        if (!this.ClosedEvents.Lookup(partitionKey, out this.ClosedEventsIndex)) this.ClosedEvents.Insert(ref this.ClosedEventsIndex, partitionKey, new SortedDictionary<long, FastDictionary2<KHP, List<ActiveEvent>>>());
                        if (!this.OpenEvents.Lookup(partitionKey, out this.OpenEventsIndex)) this.OpenEvents.Insert(ref this.OpenEventsIndex, partitionKey, this.OpenEventsGenerator());
                        if (!this.now.Lookup(partitionKey, out this.nowIndex)) this.now.Insert(ref this.nowIndex, partitionKey, StreamEvent.MinSyncTime);
                        if (!this.CurrentTimeOpenEventBufferTime.Lookup(partitionKey, out this.CurrentTimeOpenEventBufferTimeIndex)) this.CurrentTimeOpenEventBufferTime.Insert(ref this.CurrentTimeOpenEventBufferTimeIndex, partitionKey, StreamEvent.MinSyncTime);
                        if (!this.CurrentTimeOpenEventBuffer.Lookup(partitionKey, out this.CurrentTimeOpenEventBufferIndex)) this.CurrentTimeOpenEventBuffer.Insert(ref this.CurrentTimeOpenEventBufferIndex, partitionKey, this.CurrentTimeOpenEventBufferGenerator());

                        var sync = input.vsync.col[i];
                        if (this.now.entries[this.nowIndex].value < sync)
                        {
                            this.now.entries[this.nowIndex].value = sync;
                            Purge(this.now.entries[this.nowIndex].value);
                        }

                        if (*vother == StreamEvent.InfinitySyncTime)
                        {
                            ActOnStart(input.payload.col[i], input.key.col[i], *hash, *vsync);
                        }
                        else if (*vother == PartitionedStreamEvent.LowWatermarkOtherTime)
                        {
                            PurgeGlobal(*vsync);

                            this.batch.vsync.col[this.outputCount] = *vsync;
                            this.batch.vother.col[this.outputCount] = *vother;
                            this.batch[this.outputCount] = default;
                            this.batch.key.col[this.outputCount] = default;
                            this.batch.hash.col[this.outputCount] = 0;
                            this.batch.bitvector.col[this.outputCount >> 6] |= 1L << (this.outputCount & 0x3f);
                            this.outputCount++;

                            if (this.outputCount == Config.DataBatchSize) FlushContents();
                        }
                        else if (*vother == PartitionedStreamEvent.PunctuationOtherTime)
                        {
                            Purge(*vsync);
                        }
                        else if (*vsync < *vother)
                        {
                            ActOnStart(input.payload.col[i], input.key.col[i], *hash, *vsync);
                            ActOnEnd(input.payload.col[i], input.key.col[i], *hash, *vsync, *vother);
                        }
                        else
                        {
                            ActOnEnd(input.payload.col[i], input.key.col[i], *hash, *vother, *vsync);
                        }
                    }

                    vsync++; vother++; hash++;
                }
            }

            input.Free();
        }

        // The "SkipBuffering" tells us that we're at the end of a round, called from "purge". Rather than buffering this, we should
        // just issue it as a begin event.
        private void ActOnStart(TPayload payload, TKey key, int hash, long eventstart, bool skipBuffering = false)
        {
            var lookupStart = new KHP // we look up start events by Hash, Key, Payload
            {
                Hash = hash,
                Key = key,
                Payload = payload
            };

            // We should match ONLY on
            bool foundInClosedEvents = this.ClosedEvents.entries[this.ClosedEventsIndex].value.ContainsKey(eventstart) && this.ClosedEvents.entries[this.ClosedEventsIndex].value[eventstart].Lookup(lookupStart, out _);

            if (foundInClosedEvents)
            {
                // reopen it! Make sure that we squirrel away the ORIGINAL start time as VALUE, and the MOST RECENT as the Key
                var original = RemoveOne(this.ClosedEvents.entries[this.ClosedEventsIndex].value[eventstart], lookupStart); //  .entries[indx].value; // this has the original start time
                var originalExt = new ActiveEventExt
                {
                    OriginalStart = original.Start,
                    Start = eventstart,
                    End = StreamEvent.InfinitySyncTime,
                    Hash = hash,
                    Key = key,
                    Payload = payload
                };

                InsertOrAppend(this.OpenEvents.entries[this.OpenEventsIndex].value, lookupStart, originalExt);
            }
            else
            {
                // We MIGHT match an open event. We need to search through anything that might be in
                // OpenEvents[ lookupStart ] to see if it's there.
                // Matching an "open" event means that we've already got something like
                // BEGIN( Payload = P, Start = 0 )
                // BEGIN( Payload = P, Start = 1 )
                // END  ( Payload = P, End = 1, Start = 0)
                // In this case, we don't want to issue a new Start on the second Begin--instead, we
                // want to wait for the Begin and get rid of it.
                bool candidatesInOpenEvents = this.OpenEvents.entries[this.OpenEventsIndex].value.Lookup(lookupStart, out var indx);

                if (candidatesInOpenEvents && !skipBuffering && this.OpenEvents.entries[this.OpenEventsIndex].value.entries[indx].value.Count > 0)
                {
                    // We found a matching event. What we need to do is squirrel this away until the
                    // end of the time step.
                    // Every time we see an END event, we'll check if it matches this squirreled-away
                    // event. If it does, well and good; we'll process the END , then this
                    // If it doesn't, we should issue a brand new Start, as below
                    if (this.CurrentTimeOpenEventBuffer.entries[this.CurrentTimeOpenEventBufferIndex].value.Lookup(lookupStart, out int ctoindx))
                        this.CurrentTimeOpenEventBuffer.entries[this.CurrentTimeOpenEventBufferIndex].value.entries[ctoindx].value++;
                    else
                        this.CurrentTimeOpenEventBuffer.entries[this.CurrentTimeOpenEventBufferIndex].value.Insert(lookupStart, 1);
                    this.CurrentTimeOpenEventBufferTime.entries[this.CurrentTimeOpenEventBufferTimeIndex].value = eventstart;
                }
                else
                { // issue a brand new start

                    var activeEventExt = new ActiveEventExt
                    {
                        Hash = hash,
                        Key = key,
                        Payload = payload,
                        Start = eventstart,
                        OriginalStart = eventstart,
                        End = StreamEvent.InfinitySyncTime
                    };

                    // brand new event! Issue a public version
                    Emit(ActiveEvent.FromExt(activeEventExt));
                    InsertOrAppend(this.OpenEvents.entries[this.OpenEventsIndex].value, lookupStart, activeEventExt);
                }
            }
        }

        private void ActOnEnd(TPayload payload, TKey key, int hash, long start, long endTime)
        {
            var matchSmall = new KHP
            {
                Hash = hash,
                Key = key,
                Payload = payload
            };

            var item = RemoveOne(this.OpenEvents.entries[this.OpenEventsIndex].value, matchSmall, start);

            // and add it to the End list
            item.End = endTime;

            if (!this.ClosedEvents.entries[this.ClosedEventsIndex].value.ContainsKey(endTime))
            {
                // For performance, we pull this out of a pool rather than consing a new one.
                // Make very sure to reset the object before restoring it to the pool, or it'll carry garbage
                this.dictPool.Get(out FastDictionary2<KHP, List<ActiveEvent>> entry);
                this.ClosedEvents.entries[this.ClosedEventsIndex].value[endTime] = entry;
            }

            var activeEvt = ActiveEvent.FromExt(item);

            InsertOrAppend(this.ClosedEvents.entries[this.ClosedEventsIndex].value[endTime], matchSmall, activeEvt);
        }

        private void PurgeGlobal(long time)
        {
            this.ClosedEventsIndex = FastDictionary<TPartitionKey, long>.IteratorStart;
            while (this.ClosedEvents.Iterate(ref this.ClosedEventsIndex))
            {
                var partitionKey = this.ClosedEvents.entries[this.ClosedEventsIndex].key;
                this.CurrentTimeOpenEventBufferTime.Lookup(partitionKey, out this.CurrentTimeOpenEventBufferTimeIndex);
                this.CurrentTimeOpenEventBuffer.Lookup(partitionKey, out this.CurrentTimeOpenEventBufferIndex);
                Purge(time);
            }
        }

        private void Purge(long time)
        {
            // anything left in the Buffer?
            if (this.CurrentTimeOpenEventBuffer.entries[this.CurrentTimeOpenEventBufferIndex].value.Size > 0)
            {
                var it = FastDictionary2<KHP, long>.IteratorStart;
                while (this.CurrentTimeOpenEventBuffer.entries[this.CurrentTimeOpenEventBufferIndex].value.Iterate(ref it))
                {
                    var e = this.CurrentTimeOpenEventBuffer.entries[this.CurrentTimeOpenEventBufferIndex].value.entries[it].key;
                    for (int i = 0; i < this.CurrentTimeOpenEventBuffer.entries[this.CurrentTimeOpenEventBufferIndex].value.entries[it].value; i++)
                        ActOnStart(e.Payload, e.Key, e.Hash, this.CurrentTimeOpenEventBufferTime.entries[this.CurrentTimeOpenEventBufferTimeIndex].value, true);

                }

                this.CurrentTimeOpenEventBuffer.entries[this.CurrentTimeOpenEventBufferIndex].value.Initialize();
            }

            foreach (var closed in this.ClosedEvents.entries[this.ClosedEventsIndex].value.Where(k => k.Key < time).ToArray())
            {
                var iterator = FastDictionary2<TPayload, ActiveEvent>.IteratorStart;
                while (closed.Value.Iterate(ref iterator))
                {
                    foreach (var v in closed.Value.entries[iterator].value) Emit(v);
                }
                closed.Value.Initialize();
                this.ClosedEvents.entries[this.ClosedEventsIndex].value.Remove(closed.Key);

                this.dictPool.Return(closed.Value);
            }
        }

        // Optimally, this would be inline
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Emit(ActiveEvent evt)
        {
            var dest_vsync = this.batch.vsync.col;
            var dest_vother = this.batch.vother.col;
            var destkey = this.batch.key.col;
            var dest_hash = this.batch.hash.col;

            if (evt.End == StreamEvent.InfinitySyncTime)
            { // start event
                dest_vsync[this.outputCount] = evt.Start;
                dest_vother[this.outputCount] = evt.End;
            }
            else // issue an end event
            {
                dest_vsync[this.outputCount] = evt.End;
                dest_vother[this.outputCount] = evt.Start;
            }

            this.batch[this.outputCount] = evt.Payload;
            destkey[this.outputCount] = evt.Key;
            dest_hash[this.outputCount] = evt.Hash;
            this.outputCount++;

            if (this.outputCount == Config.DataBatchSize) FlushContents();
        }

        public override int CurrentlyBufferedOutputCount => this.outputCount;

        public override int CurrentlyBufferedInputCount
        {
            get
            {
                int count = 0;
                int iterator = FastDictionary<TPartitionKey, FastDictionary2<KHP, int>>.IteratorStart;
                while (this.OpenEvents.Iterate(ref iterator))
                {
                    count += this.OpenEvents.entries[iterator].value.Count;
                }
                return count;
            }
        }
    }
}
