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
    internal sealed class BeatPipe<TKey, TPayload> : UnaryPipe<TKey, TPayload, TPayload>
    {
        private readonly MemoryPool<TKey, TPayload> pool;
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
        private FastMap<ActiveInterval> intervals = new FastMap<ActiveInterval>();
        [DataMember]
        private FastMap<ActiveEdge> edges = new FastMap<ActiveEdge>();
        [DataMember]
        private long currBeatTime = long.MinValue;
        [DataMember]
        private long lastTime = long.MinValue;
        [DataMember]
        private int batchIter;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public BeatPipe() { }

        public BeatPipe(BeatStreamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer)
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

                        AdvanceTime(startTime);

                        bool isPunctuation = endTime == long.MinValue;
                        bool isInsert = startTime < endTime;
                        bool isStartEdge = isInsert && endTime == StreamEvent.InfinitySyncTime;
                        bool isEndEdge = !isInsert;
                        if (isPunctuation)
                        {
                            AddToBatch(startTime, long.MinValue, ref sourceKey[row], ref sourcePayload[row], hash);
                        }
                        else if (isStartEdge)
                        {
                            // Add starting edge { vSync = startTime, vOther = StreamEvent.InfinitySyncTime }.
                            AddToBatch(
                                startTime,
                                StreamEvent.InfinitySyncTime,
                                ref sourceKey[row],
                                ref sourcePayload[row],
                                hash);

                            // Add to active edges list to handle repeat at beats (and waiting for closing edge).
                            int index = this.edges.Insert(hash);
                            this.edges.Values[index].Populate(
                                startTime,
                                ref sourceKey[row],
                                ref sourcePayload[row]);

                        }
                        else if (isEndEdge)
                        {
                            bool notCurrentlyOnBeat = startTime != this.currBeatTime;
                            long edgeStartTime = endTime;
                            long edgeEndTime = startTime;
                            if (notCurrentlyOnBeat)
                            {
                                // Edges are only open if not on a beat.
                                long lastBeatTime = this.currBeatTime - this.period;
                                bool edgeStartedBeforeLastBeat = edgeStartTime < lastBeatTime;

                                if (edgeStartedBeforeLastBeat)
                                {
                                    // Add closing edge { vSync = edgeEndTime, vOther = lastBeatTime }.
                                    AddToBatch(
                                        edgeEndTime,
                                        lastBeatTime,
                                        ref sourceKey[row],
                                        ref sourcePayload[row],
                                        hash);
                                }
                                else
                                {
                                    // Add closing edge { vSync = edgeEndTime, vOther = edgeStartTime }.
                                    AddToBatch(
                                        edgeEndTime,
                                        edgeStartTime,
                                        ref sourceKey[row],
                                        ref sourcePayload[row],
                                        hash);
                                }
                            }

                            // Remove from active edges list.
                            var edgesTraversal = this.edges.Find(hash);
                            while (edgesTraversal.Next(out int index))
                            {
                                var temp = this.edges.Values[index];
                                if (AreSame(edgeStartTime, ref sourceKey[row], ref sourcePayload[row], ref temp))
                                {
                                    edgesTraversal.Remove();

                                    break;
                                }
                            }
                        }
                        else
                        {
                            long nextBeatTime = startTime == this.currBeatTime ? this.currBeatTime + this.period : this.currBeatTime;
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
                                int index = this.intervals.Insert(hash);
                                this.intervals.Values[index].Populate(endTime, ref sourceKey[row], ref sourcePayload[row]);
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
        private void AdvanceTime(long time)
        {
            if (time == this.lastTime || time < this.currBeatTime)
            {
                // Time has not changed or has not yet reached beat.
                return;
            }

            if (time >= StreamEvent.InfinitySyncTime)
            {
                // If advancing time to infinity, verify no open edges.
                if (!this.edges.IsEmpty)
                {
                    throw new InvalidOperationException("Cannot advance time to infinity if unclosed open edges");
                }
            }

            if (this.edges.IsEmpty && this.intervals.IsEmpty)
            {
                // No elements to track, so just advance time to next beat.
                this.currBeatTime = FindNextBeatGreaterThanOrEqualTo(time);
                this.lastTime = time;
                return;
            }

            // currTime must be >= currBeatTime AND lastTime must be <= currBeatTime, so we have definitely
            // reached or surpassed a beat.
            if (this.lastTime < this.currBeatTime)
            {
                // This is the first time reaching the time currBeatTime, so handle reaching a beat.
                ReachBeat(this.currBeatTime);

                if (this.edges.IsEmpty && this.intervals.IsEmpty)
                {
                    // No elements to track, so just advance time to next beat.
                    this.currBeatTime = FindNextBeatGreaterThanOrEqualTo(time);
                    this.lastTime = time;
                    return;
                }
            }

            // By this point, we have definitely reached currBeatTime although may or may not have surpassed it.
            while (time >= this.currBeatTime + this.period)
            {
                // We are guaranteed there are no events within (currBeatTime, currBeatTime + period) because
                // lastTime must be <= currBeatTime and time is >= currBeatTime + period. Note there could have
                // been edges at currBeatTime or edges to still come at currBeatTime + period, however.

                // Regardless, we can optimize edges to output as intervals for (currBeatTime, currBeatTime + period).
                LeaveBeatContinuousToNext(this.currBeatTime);
                this.currBeatTime += this.period;
                ReachBeatContinuousFromLast(this.currBeatTime);

                if (this.edges.IsEmpty && this.intervals.IsEmpty)
                {
                    // No elements to track, so just advance time to next beat.
                    this.currBeatTime = FindNextBeatGreaterThanOrEqualTo(time);
                    this.lastTime = time;
                    return;
                }
            }

            // By this point, the loop guarantees that: currBeatTime <= time < currBeatTime + period
            if (time > this.currBeatTime)
            {
                // time has passed the beat at currBeatTime, so handle the beat.
                LeaveBeat(this.currBeatTime);
                this.currBeatTime += this.period;
            }

            // By this point, time must be <= currBeatTime.
            this.lastTime = time;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReachBeat(long beatTime)
        {
            long lastBeatTime = beatTime - this.period;
            long nextBeatTime = beatTime + this.period;

            // Close all active edges.
            int index;
            int hash;

            var edgesTraversal = this.edges.Traverse();
            while (edgesTraversal.Next(out index, out hash))
            {

                var activeEdge = this.edges.Values[index];
                bool edgeStartedBeforeLastBeat = activeEdge.Start < lastBeatTime;

                if (edgeStartedBeforeLastBeat)
                {
                    // Add closing edge { vSync = beatTime, vOther = lastBeatTime }.
                    AddToBatch(beatTime, lastBeatTime, ref activeEdge.Key, ref activeEdge.Payload, hash);
                }
                else
                {
                    // Add closing edge { vSync = beatTime, vOther = edge.Start }.
                    AddToBatch(beatTime, activeEdge.Start, ref activeEdge.Key, ref activeEdge.Payload, hash);
                }
            }

            // Add all active intervals.
            var intervalTraverser = this.intervals.Traverse();
            while (intervalTraverser.Next(out index, out hash))
            {
                var activeInterval = this.intervals.Values[index];
                bool isLastBeatForInterval = activeInterval.End <= nextBeatTime;

                if (isLastBeatForInterval)
                {
                    // Add interval { vSync = beatTime, vOther = interval.End }.
                    AddToBatch(beatTime, activeInterval.End, ref activeInterval.Key, ref activeInterval.Payload, hash);

                    // Remove from active list as no longer need to output.
                    intervalTraverser.Remove();

                }
                else
                {
                    // Add interval { vSync = beatTime, vOther = nextBeatTime }.
                    AddToBatch(beatTime, nextBeatTime, ref activeInterval.Key, ref activeInterval.Payload, hash);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void LeaveBeat(long beatTime)
        {
            // Open all active edges (that weren't added during this beat).
            var edgesTraversal = this.edges.Traverse();
            while (edgesTraversal.Next(out int index, out int hash))
            {
                var activeEdge = this.edges.Values[index];
                bool edgeWasAddedPriorToBeat = activeEdge.Start < beatTime;

                if (edgeWasAddedPriorToBeat)
                {
                    // Add starting edge { vSync = beatTime, vOther = StreamEvent.InfinitySyncTime }.
                    AddToBatch(beatTime, StreamEvent.InfinitySyncTime, ref activeEdge.Key, ref activeEdge.Payload, hash);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void LeaveBeatContinuousToNext(long beatTime)
        {
            long nextBeatTime = beatTime + this.period;

            // Make intervals for all active edges (that weren't added during this beat).
            var edgesTraversal = this.edges.Traverse();
            while (edgesTraversal.Next(out int index, out int hash))
            {
                var activeEdge = this.edges.Values[index];
                bool edgeWasAddedPriorToBeat = activeEdge.Start < beatTime;

                if (edgeWasAddedPriorToBeat)
                {
                    // Add interval for edge { vSync = beatTime, vOther = nextBeatTime }.
                    AddToBatch(beatTime, nextBeatTime, ref activeEdge.Key, ref activeEdge.Payload, hash);
                }
                else
                {
                    // Denote edges that were added at beat because they have already outputted a start edge.
                    edgesTraversal.MakeInvisible();

                }
            }

            // Output corresponding end edges for all start edges.
            var invisibleTraverser = this.edges.TraverseInvisible();
            while (invisibleTraverser.Next(out int index, out int hash))
            {

                // Add closing edge { vSync = nextBeatTime, vOther = beatTime }.
                var activeEdge = this.edges.Values[index];
                AddToBatch(nextBeatTime, beatTime, ref activeEdge.Key, ref activeEdge.Payload, hash);
                invisibleTraverser.MakeVisible();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReachBeatContinuousFromLast(long beatTime)
        {
            long nextBeatTime = beatTime + this.period;

            // Add all active intervals.
            var intervalTraverser = this.intervals.Traverse();
            while (intervalTraverser.Next(out int index, out int hash))
            {
                var activeInterval = this.intervals.Values[index];
                bool isLastBeatForInterval = activeInterval.End <= nextBeatTime;

                if (isLastBeatForInterval)
                {
                    // Add interval { vSync = beatTime, vOther = interval.End }.
                    AddToBatch(beatTime, activeInterval.End, ref activeInterval.Key, ref activeInterval.Payload, hash);

                    // Remove from active list as no longer need to output.
                    intervalTraverser.Remove();
                }
                else
                {
                    // Add interval { vSync = beatTime, vOther = nextBeatTime }.
                    AddToBatch(beatTime, nextBeatTime, ref activeInterval.Key, ref activeInterval.Payload, hash);
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
            if (end == StreamEvent.PunctuationOtherTime) this.output.bitvector.col[index >> 6] |= (1L << (index & 0x3f));

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

        public override int CurrentlyBufferedOutputCount => this.output.Count;

        public override int CurrentlyBufferedInputCount => this.edges.Count + this.intervals.Count;

        protected override void DisposeState() => this.output.Free();

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

            public override string ToString() => "[Start=" + this.Start + ", Key='" + this.Key + "', Payload='" + this.Payload + "']";
        }
    }
}
