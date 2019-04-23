// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class PartitionedBeatPipe<TKey, TPayload, TPartitionKey> : UnaryPipe<TKey, TPayload, TPayload>
    {
        private readonly MemoryPool<TKey, TPayload> pool;
        private readonly Func<TKey, TPartitionKey> getPartitionKey = GetPartitionExtractor<TPartitionKey, TKey>();
        private readonly string errorMessages;

        [SchemaSerialization]
        private readonly long offset;
        [SchemaSerialization]
        private readonly long period;
        [SchemaSerialization]
        private readonly Expression<Func<TKey, TKey, bool>> keyComparerExpr;
        private readonly Func<TKey, TKey, bool> keyComparer;
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, TPayload, bool>> payloadComparerExpr;
        private readonly Func<TPayload, TPayload, bool> payloadComparer;

        [DataMember]
        private StreamMessage<TKey, TPayload> output;

        [DataMember]
        private FastDictionary<TPartitionKey, FastMap<ActiveInterval>> intervals = new FastDictionary<TPartitionKey, FastMap<ActiveInterval>>();
        [DataMember]
        private FastDictionary<TPartitionKey, FastMap<ActiveEdge>> edges = new FastDictionary<TPartitionKey, FastMap<ActiveEdge>>();
        [DataMember]
        private int intervalIndex;
        [DataMember]
        private int edgeIndex;
        [DataMember]
        private FastDictionary<TPartitionKey, long> currBeatTime = new FastDictionary<TPartitionKey, long>();
        [DataMember]
        private FastDictionary<TPartitionKey, long> lastTime = new FastDictionary<TPartitionKey, long>();
        [DataMember]
        private int currBeatIndex;
        [DataMember]
        private int lastIndex;
        [DataMember]
        private int batchIter;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public PartitionedBeatPipe() { }

        public PartitionedBeatPipe(BeatStreamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer)
            : base(stream, observer)
        {
            this.offset = stream.Offset;
            this.period = stream.Period;
            this.errorMessages = stream.ErrorMessages;

            this.keyComparerExpr = stream.Properties.KeyEqualityComparer.GetEqualsExpr();
            this.keyComparer = this.keyComparerExpr.Compile();

            this.payloadComparerExpr = stream.Properties.PayloadEqualityComparer.GetEqualsExpr();
            this.payloadComparer = this.payloadComparerExpr.Compile();

            this.pool = MemoryManager.GetMemoryPool<TKey, TPayload>(stream.Properties.IsColumnar);
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        public override unsafe void OnNext(StreamMessage<TKey, TPayload> batch)
        {
            this.batchIter = batch.iter;
            TPayload[] sourcePayload = batch.payload.col;
            TKey[] sourceKey = batch.key.col;
            fixed (int* sourceHash = batch.hash.col)
            fixed (long* sourceBitVector = batch.bitvector.col)
            fixed (long* sourceVSync = batch.vsync.col)
            fixed (long* sourceVOther = batch.vother.col)
            {
                int count = batch.Count;
                int* sourceHashPtr = sourceHash;
                long* sourceVSyncPtr = sourceVSync;
                long* sourceVOtherPtr = sourceVOther;
                for (int row = 0; row < count; row++)
                {
                    if ((sourceBitVector[row >> 6] & (1L << (row & 0x3f))) == 0 || *sourceVOtherPtr == long.MinValue)
                    {
                        long startTime = *sourceVSyncPtr;
                        long endTime = *sourceVOtherPtr;
                        int hash = *sourceHashPtr;
                        TPartitionKey partitionKey = this.getPartitionKey(batch.key.col[row]);
                        if (!this.edges.Lookup(partitionKey, out this.edgeIndex)) this.edges.Insert(ref this.edgeIndex, partitionKey, new FastMap<ActiveEdge>());
                        if (!this.intervals.Lookup(partitionKey, out this.intervalIndex)) this.intervals.Insert(ref this.intervalIndex, partitionKey, new FastMap<ActiveInterval>());
                        if (!this.currBeatTime.Lookup(partitionKey, out this.currBeatIndex)) this.currBeatTime.Insert(ref this.currBeatIndex, partitionKey, long.MinValue);
                        if (!this.lastTime.Lookup(partitionKey, out this.lastIndex)) this.lastTime.Insert(ref this.lastIndex, partitionKey, long.MinValue);

                        bool isPunctuation = endTime == PartitionedStreamEvent.PunctuationOtherTime;
                        bool isLowWatermark = endTime == PartitionedStreamEvent.LowWatermarkOtherTime;
                        bool isInsert = startTime < endTime;
                        bool isStartEdge = isInsert && endTime == StreamEvent.InfinitySyncTime;
                        bool isEndEdge = !isInsert;

                        if (isLowWatermark) AdvanceGlobalTime(startTime);
                        else AdvanceTime(startTime);

                        if (isPunctuation || isLowWatermark)
                        {
                            AddToBatch(startTime, endTime, ref sourceKey[row], ref sourcePayload[row], hash);
                        }
                        else if (isStartEdge)
                        {
                            // Add starting edge { vSync = startTime, vOther = StreamEvent.InfinitySyncTime }.
                            AddToBatch(startTime, StreamEvent.InfinitySyncTime, ref sourceKey[row], ref sourcePayload[row], hash);

                            // Add to active edges list to handle repeat at beats (and waiting for closing edge).
                            int index = this.edges.entries[this.edgeIndex].value.Insert(hash);
                            this.edges.entries[this.edgeIndex].value.Values[index].Populate(
                                startTime,
                                ref sourceKey[row],
                                ref sourcePayload[row]);
                        }
                        else if (isEndEdge)
                        {
                            bool notCurrentlyOnBeat = startTime != this.currBeatTime.entries[this.currBeatIndex].value;
                            long edgeStartTime = endTime;
                            long edgeEndTime = startTime;
                            if (notCurrentlyOnBeat)
                            {
                                // Edges are only open if not on a beat.
                                long lastBeatTime = this.currBeatTime.entries[this.currBeatIndex].value - this.period;
                                bool edgeStartedBeforeLastBeat = edgeStartTime < lastBeatTime;

                                if (edgeStartedBeforeLastBeat)
                                {
                                    // Add closing edge { vSync = edgeEndTime, vOther = lastBeatTime }.
                                    AddToBatch(edgeEndTime, lastBeatTime, ref sourceKey[row], ref sourcePayload[row], hash);
                                }
                                else
                                {
                                    // Add closing edge { vSync = edgeEndTime, vOther = edgeStartTime }.
                                    AddToBatch(edgeEndTime, edgeStartTime, ref sourceKey[row], ref sourcePayload[row], hash);
                                }
                            }

                            // Remove from active edges list.
                            var edgesTraversal = this.edges.entries[this.edgeIndex].value.Find(hash);
                            while (edgesTraversal.Next(out int index))
                            {
                                if (AreSame(edgeStartTime, ref sourceKey[row], ref sourcePayload[row], ref this.edges.entries[this.edgeIndex].value.Values[index]))
                                {
                                    edgesTraversal.Remove();
                                    break;
                                }
                            }
                        }
                        else
                        {
                            long nextBeatTime = startTime == this.currBeatTime.entries[this.currBeatIndex].value ? this.currBeatTime.entries[this.currBeatIndex].value + this.period : this.currBeatTime.entries[this.currBeatIndex].value;
                            bool isLastBeatForInterval = endTime <= nextBeatTime;

                            if (isLastBeatForInterval)
                            {
                                // Add interval { vSync = startTime, vOther = endTime }.
                                AddToBatch(startTime, endTime, ref sourceKey[row], ref sourcePayload[row], hash);

                                // No need to add to active list as interval ends <= nextBeatTime.
                            }
                            else
                            {
                                // Add interval { vSync = startTime, vOther = nextBeatTime }.
                                AddToBatch(startTime, nextBeatTime, ref sourceKey[row], ref sourcePayload[row], hash);

                                // Add to active list to handle repeat at beats.
                                int index = this.intervals.entries[this.intervalIndex].value.Insert(hash);
                                this.intervals.entries[this.intervalIndex].value.Values[index].Populate(endTime, ref sourceKey[row], ref sourcePayload[row]);
                            }
                        }
                    }

                    // Advance pointers.
                    sourceHashPtr++;
                    sourceVSyncPtr++;
                    sourceVOtherPtr++;
                }
            }

            batch.Free();
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new BeatPlanNode(
                previous, this, typeof(TKey), typeof(TPayload), this.offset, this.period, false, this.errorMessages));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AdvanceGlobalTime(long time)
        {
            this.intervalIndex = FastDictionary<TPartitionKey, FastMap<ActiveInterval>>.IteratorStart;
            while (this.intervals.Iterate(ref this.intervalIndex))
            {
                TPartitionKey partitionKey = this.intervals.entries[this.intervalIndex].key;
                this.edges.Lookup(partitionKey, out this.edgeIndex);
                this.currBeatTime.Lookup(partitionKey, out this.currBeatIndex);
                this.lastTime.Lookup(partitionKey, out this.lastIndex);
                AdvanceTime(time);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AdvanceTime(long time)
        {
            if (time == this.lastTime.entries[this.lastIndex].value || time < this.currBeatTime.entries[this.currBeatIndex].value)
            {
                // Time has not changed or has not yet reached beat.
                return;
            }

            if (time >= StreamEvent.InfinitySyncTime)
            {
                // If advancing time to infinity, verify no open edges.
                if (!this.edges.entries[this.edgeIndex].value.IsEmpty)
                {
                    throw new InvalidOperationException("Cannot advance time to infinity if unclosed open edges");
                }
            }

            if (this.edges.entries[this.edgeIndex].value.IsEmpty && this.intervals.entries[this.intervalIndex].value.IsEmpty)
            {
                // No elements to track, so just advance time to next beat.
                this.currBeatTime.entries[this.currBeatIndex].value = FindNextBeatGreaterThanOrEqualTo(time);
                this.lastTime.entries[this.lastIndex].value = time;
                return;
            }

            // currTime must be >= currBeatTime AND lastTime must be <= currBeatTime, so we have definitely
            // reached or surpassed a beat.
            if (this.lastTime.entries[this.lastIndex].value < this.currBeatTime.entries[this.currBeatIndex].value)
            {
                // This is the first time reaching the time currBeatTime, so handle reaching a beat.
                ReachBeat(this.currBeatTime.entries[this.currBeatIndex].value);

                if (this.edges.entries[this.edgeIndex].value.IsEmpty && this.intervals.entries[this.intervalIndex].value.IsEmpty)
                {
                    // No elements to track, so just advance time to next beat.
                    this.currBeatTime.entries[this.currBeatIndex].value = FindNextBeatGreaterThanOrEqualTo(time);
                    this.lastTime.entries[this.currBeatIndex].value = time;
                    return;
                }
            }

            // By this point, we have definitely reached currBeatTime although may or may not have surpassed it.
            while (time >= this.currBeatTime.entries[this.currBeatIndex].value + this.period)
            {
                // We are guaranteed there are no events within (currBeatTime, currBeatTime + period) because
                // lastTime must be <= currBeatTime and time is >= currBeatTime + period. Note there could have
                // been edges at currBeatTime or edges to still come at currBeatTime + period, however.

                // Regardless, we can optimize edges to output as intervals for (currBeatTime, currBeatTime + period).
                LeaveBeatContinuousToNext(this.currBeatTime.entries[this.currBeatIndex].value);
                this.currBeatTime.entries[this.currBeatIndex].value += this.period;
                ReachBeatContinuousFromLast(this.currBeatTime.entries[this.currBeatIndex].value);

                if (this.edges.entries[this.edgeIndex].value.IsEmpty && this.intervals.entries[this.intervalIndex].value.IsEmpty)
                {
                    // No elements to track, so just advance time to next beat.
                    this.currBeatTime.entries[this.currBeatIndex].value = FindNextBeatGreaterThanOrEqualTo(time);
                    this.lastTime.entries[this.lastIndex].value = time;
                    return;
                }
            }

            // By this point, the loop guarantees that: currBeatTime <= time < currBeatTime + period
            if (time > this.currBeatTime.entries[this.currBeatIndex].value)
            {
                // time has passed the beat at currBeatTime, so handle the beat.
                LeaveBeat(this.currBeatTime.entries[this.currBeatIndex].value);
                this.currBeatTime.entries[this.currBeatIndex].value += this.period;
            }

            // By this point, time must be <= currBeatTime.
            this.lastTime.entries[this.lastIndex].value = time;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReachBeat(long beatTime)
        {
            long lastBeatTime = beatTime - this.period;
            long nextBeatTime = beatTime + this.period;

            // Close all active edges.
            var edgeTraverser = this.edges.entries[this.edgeIndex].value.Traverse();
            int index;
            int hash;
            while (edgeTraverser.Next(out index, out hash))
            {
                long edgeStartTime = this.edges.entries[this.edgeIndex].value.Values[index].Start;
                bool edgeStartedBeforeLastBeat = edgeStartTime < lastBeatTime;

                if (edgeStartedBeforeLastBeat)
                {
                    // Add closing edge { vSync = beatTime, vOther = lastBeatTime }.
                    AddToBatch(beatTime, lastBeatTime, ref this.edges.entries[this.edgeIndex].value.Values[index].Key, ref this.edges.entries[this.edgeIndex].value.Values[index].Payload, hash);
                }
                else
                {
                    // Add closing edge { vSync = beatTime, vOther = edge.Start }.
                    AddToBatch(beatTime, edgeStartTime, ref this.edges.entries[this.edgeIndex].value.Values[index].Key, ref this.edges.entries[this.edgeIndex].value.Values[index].Payload, hash);
                }
            }

            // Add all active intervals.
            var intervalTraverser = this.intervals.entries[this.intervalIndex].value.Traverse();
            while (intervalTraverser.Next(out index, out hash))
            {
                long intervalEndTime = this.intervals.entries[this.intervalIndex].value.Values[index].End;
                bool isLastBeatForInterval = intervalEndTime <= nextBeatTime;

                if (isLastBeatForInterval)
                {
                    // Add interval { vSync = beatTime, vOther = interval.End }.
                    AddToBatch(beatTime, intervalEndTime, ref this.intervals.entries[this.intervalIndex].value.Values[index].Key, ref this.intervals.entries[this.intervalIndex].value.Values[index].Payload, hash);

                    // Remove from active list as no longer need to output.
                    intervalTraverser.Remove();
                }
                else
                {
                    // Add interval { vSync = beatTime, vOther = nextBeatTime }.
                    AddToBatch(beatTime, nextBeatTime, ref this.intervals.entries[this.intervalIndex].value.Values[index].Key, ref this.intervals.entries[this.intervalIndex].value.Values[index].Payload, hash);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void LeaveBeat(long beatTime)
        {
            // Open all active edges (that weren't added during this beat).
            var edgeTraverser = this.edges.entries[this.edgeIndex].value.Traverse();
            while (edgeTraverser.Next(out int index, out int hash))
            {
                bool edgeWasAddedPriorToBeat = this.edges.entries[this.edgeIndex].value.Values[index].Start < beatTime;

                if (edgeWasAddedPriorToBeat)
                {
                    // Add starting edge { vSync = beatTime, vOther = StreamEvent.InfinitySyncTime }.
                    AddToBatch(beatTime, StreamEvent.InfinitySyncTime, ref this.edges.entries[this.edgeIndex].value.Values[index].Key, ref this.edges.entries[this.edgeIndex].value.Values[index].Payload, hash);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void LeaveBeatContinuousToNext(long beatTime)
        {
            long nextBeatTime = beatTime + this.period;

            // Make intervals for all active edges (that weren't added during this beat).
            var edgeTraverser = this.edges.entries[this.edgeIndex].value.Traverse();
            int index;
            int hash;
            while (edgeTraverser.Next(out index, out hash))
            {
                bool edgeWasAddedPriorToBeat = this.edges.entries[this.edgeIndex].value.Values[index].Start < beatTime;

                if (edgeWasAddedPriorToBeat)
                {
                    // Add interval for edge { vSync = beatTime, vOther = nextBeatTime }.
                    AddToBatch(beatTime, nextBeatTime, ref this.edges.entries[this.edgeIndex].value.Values[index].Key, ref this.edges.entries[this.edgeIndex].value.Values[index].Payload, hash);
                }
                else
                {
                    // Denote edges that were added at beat because they have already outputted a start edge.
                    edgeTraverser.MakeInvisible();
                }
            }

            // Output corresponding end edges for all start edges.
            var invisibleTraverser = this.edges.entries[this.edgeIndex].value.TraverseInvisible();
            while (invisibleTraverser.Next(out index, out hash))
            {
                // Add closing edge { vSync = nextBeatTime, vOther = beatTime }.
                AddToBatch(nextBeatTime, beatTime, ref this.edges.entries[this.edgeIndex].value.Values[index].Key, ref this.edges.entries[this.edgeIndex].value.Values[index].Payload, hash);
                invisibleTraverser.MakeVisible();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReachBeatContinuousFromLast(long beatTime)
        {
            long nextBeatTime = beatTime + this.period;

            // Add all active intervals.
            var intervalTraverser = this.intervals.entries[this.intervalIndex].value.Traverse();
            while (intervalTraverser.Next(out int index, out int hash))
            {
                long intervalEndTime = this.intervals.entries[this.intervalIndex].value.Values[index].End;
                bool isLastBeatForInterval = intervalEndTime <= nextBeatTime;

                if (isLastBeatForInterval)
                {
                    // Add interval { vSync = beatTime, vOther = interval.End }.
                    AddToBatch(beatTime, intervalEndTime, ref this.intervals.entries[this.intervalIndex].value.Values[index].Key, ref this.intervals.entries[this.intervalIndex].value.Values[index].Payload, hash);

                    // Remove from active list as no longer need to output.
                    intervalTraverser.Remove();
                }
                else
                {
                    // Add interval { vSync = beatTime, vOther = nextBeatTime }.
                    AddToBatch(beatTime, nextBeatTime, ref this.intervals.entries[this.intervalIndex].value.Values[index].Key, ref this.intervals.entries[this.intervalIndex].value.Values[index].Payload, hash);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long FindNextBeatGreaterThanOrEqualTo(long time)
        {
            long multiple = (time - this.offset + this.period - 1) / this.period;
            return (multiple * this.period) + this.offset;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AddToBatch(long start, long end, ref TKey key, ref TPayload payload, int hash)
        {
            int index = this.output.Count++;
            this.output.vsync.col[index] = start;
            this.output.vother.col[index] = end;
            this.output.key.col[index] = key;
            this.output.payload.col[index] = payload;
            this.output.hash.col[index] = hash;
            if (end == long.MinValue) this.output.bitvector.col[index >> 6] |= (1L << (index & 0x3f));

            if (this.output.Count == Config.DataBatchSize) FlushContents();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool AreSame(long start, ref TKey key, ref TPayload payload, ref ActiveEdge active)
            => start == active.Start && this.keyComparer(key, active.Key) && this.payloadComparer(payload, active.Payload);

        protected override void FlushContents()
        {
            if (this.output.Count == 0) return;
            this.output.iter = this.batchIter;
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
                int iter = FastDictionary<TPartitionKey, FastMap<ActiveEdge>>.IteratorStart;
                while (this.edges.Iterate(ref iter)) count += this.edges.entries[iter].value.Count;
                iter = FastDictionary<TPartitionKey, FastMap<ActiveInterval>>.IteratorStart;
                while (this.intervals.Iterate(ref iter)) count += this.intervals.entries[iter].value.Count;
                return count;
            }
        }

        [DataContract]
        private struct ActiveInterval
        {
            [DataMember]
            public long End;
            [DataMember]
            public TKey Key;
            [DataMember]
            public TPayload Payload;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Populate(long end, ref TKey key, ref TPayload payload)
            {
                this.End = end;
                this.Key = key;
                this.Payload = payload;
            }

            public override string ToString()
                => "[End=" + this.End + ", Key='" + this.Key + "', Payload='" + this.Payload + "']";
        }

        [DataContract]
        private struct ActiveEdge
        {
            [DataMember]
            public long Start;
            [DataMember]
            public TKey Key;
            [DataMember]
            public TPayload Payload;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Populate(long start, ref TKey key, ref TPayload payload)
            {
                this.Start = start;
                this.Key = key;
                this.Payload = payload;
            }

            public override string ToString()
                => "[Start=" + this.Start + ", Key='" + this.Key + "', Payload='" + this.Payload + "']";
        }
    }
}
