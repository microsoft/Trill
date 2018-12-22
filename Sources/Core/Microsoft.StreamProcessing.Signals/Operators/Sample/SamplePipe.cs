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

namespace Microsoft.StreamProcessing.Signal
{
    internal sealed class SamplePipe<TKey, TSource, TResult> : UnaryPipe<TKey, TSource, TResult>
    {
        private readonly MemoryPool<TKey, TResult> pool;

        [SchemaSerialization]
        private readonly long offset;
        [SchemaSerialization]
        private readonly long period;

        [SchemaSerialization]
        private readonly Expression<Func<TKey, TKey, bool>> keyComparerExpr;
        private readonly Func<TKey, TKey, bool> keyComparer;
        [SchemaSerialization]
        private readonly Expression<Func<TSource, TSource, bool>> payloadComparerExpr;
        private readonly Func<TSource, TSource, bool> payloadComparer;
        [SchemaSerialization]
        private readonly Expression<Func<long, TSource, TResult>> signalFunctionExpr;
        private readonly Func<long, TSource, TResult> signalFunction;

        [DataMember]
        private StreamMessage<TKey, TResult> output;

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

        public SamplePipe() { }

        public SamplePipe(SampleStreamable<TKey, TSource, TResult> stream, IStreamObserver<TKey, TResult> observer)
            : base(stream, observer)
        {
            this.offset = stream.offset;
            this.period = stream.period;

            this.keyComparerExpr = stream.Properties.KeyEqualityComparer.GetEqualsExpr();
            this.keyComparer = this.keyComparerExpr.Compile();

            this.payloadComparerExpr = stream.sourcePayloadEqualityComparer.GetEqualsExpr();
            this.payloadComparer = this.payloadComparerExpr.Compile();

            this.signalFunctionExpr = stream.signalFunction;
            this.signalFunction = this.signalFunctionExpr.Compile();

            this.pool = MemoryManager.GetMemoryPool<TKey, TResult>(stream.Properties.IsColumnar);
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        public override unsafe void OnNext(StreamMessage<TKey, TSource> batch)
        {
            this.batchIter = batch.iter;
            TKey[] sourceKey = batch.key.col;
            TSource[] sourcePayload = batch.payload.col;
            fixed (long* sourceVSync = batch.vsync.col)
            fixed (long* sourceVOther = batch.vother.col)
            fixed (int* sourceHash = batch.hash.col)
            fixed (long* sourceBitVector = batch.bitvector.col)
            {
                int count = batch.Count;
                int* sourceHashPtr = sourceHash;
                long* sourceVSyncPtr = sourceVSync;
                long* sourceVOtherPtr = sourceVOther;

                for (int row = 0; row < count; row++)
                {
                    if ((sourceBitVector[row >> 6] & (1L << (row & 0x3f))) == 0)
                    {
                        long startTime = *sourceVSyncPtr;
                        long endTime = *sourceVOtherPtr;
                        int hash = *sourceHashPtr;

                        AdvanceTime(startTime);

                        bool isInsert = startTime < endTime;
                        bool isStartEdge = isInsert && endTime == StreamEvent.InfinitySyncTime;
                        bool isEndEdge = !isInsert;
                        if (isStartEdge)
                        {
                            // Add to active edges list to handle repeat at beats (and waiting for closing edge).
                            int index = this.edges.Insert(hash);
                            this.edges.Values[index].Populate(
                                startTime,
                                ref sourceKey[row],
                                ref sourcePayload[row]);
                        }
                        else if (isEndEdge)
                        {
                            long edgeStartTime = endTime;

                            // Remove from active edges list.
                            var edgesTraversal = this.edges.Find(hash);
                            while (edgesTraversal.Next(out int index))
                            {
                                if (AreSame(edgeStartTime, ref sourceKey[row], ref sourcePayload[row], ref this.edges.Values[index]))
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

                            if (startTime == this.currBeatTime)
                            {
                                // Add interval { vSync = startTime, vOther = startTime + 1 }.
                                AddToBatch(startTime, startTime + 1, ref sourceKey[row], ref sourcePayload[row], hash);
                            }

                            if (isLastBeatForInterval)
                            {
                                // No need to add to active list as interval ends <= nextBeatTime.
                            }
                            else
                            {
                                // Add to active list to handle repeat at beats.
                                int index = this.intervals.Insert(hash);
                                this.intervals.Values[index].Populate(endTime, ref sourceKey[row], ref sourcePayload[row]);
                            }
                        }
                    }
                    else if (*sourceVOtherPtr == StreamEvent.PunctuationOtherTime) AdvanceTime(*sourceVSyncPtr);

                    // Advance pointers.
                    sourceHashPtr++;
                    sourceVSyncPtr++;
                    sourceVOtherPtr++;
                }
            }

            batch.Free();
        }

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
                LeaveBeat(this.currBeatTime);
                this.currBeatTime += period;
                ReachBeat(this.currBeatTime);

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
                this.currBeatTime += period;
            }

            // By this point, time must be <= currBeatTime.
            this.lastTime = time;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReachBeat(long beatTime)
        {
            long nextBeatTime = beatTime + this.period;

            // Add all active intervals.
            var intervalTraverser = this.intervals.Traverse();
            while (intervalTraverser.Next(out int index, out int hash))
            {
                long intervalEndTime = this.intervals.Values[index].end;
                bool isLastBeatForInterval = intervalEndTime <= nextBeatTime;

                // Add interval { vSync = beatTime, vOther = beatTime + 1 }.
                AddToBatch(beatTime, beatTime + 1, ref this.intervals.Values[index].key, ref this.intervals.Values[index].payload, hash);

                if (isLastBeatForInterval)
                {
                    // Remove from active list as no longer need to output.
                    intervalTraverser.Remove();
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void LeaveBeat(long beatTime)
        {
            // Open all active edges (that weren't added during this beat).
            var edgeTraverser = this.edges.Traverse();
            while (edgeTraverser.Next(out int index, out int hash))
            {
                // Add interval { vSync = beatTime, vOther = beatTime + 1 }.
                AddToBatch(beatTime, beatTime + 1, ref this.edges.Values[index].key, ref this.edges.Values[index].payload, hash);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long FindNextBeatGreaterThanOrEqualTo(long time)
        {
            long multiple = (time - this.offset + this.period - 1) / this.period;
            return (multiple * this.period) + this.offset;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AddToBatch(long start, long end, ref TKey key, ref TSource sourcePayload, int hash)
        {
            // Evaluate function
            TResult payload = signalFunction(start, sourcePayload);

            int index = this.output.Count++;
            this.output.vsync.col[index] = start;
            this.output.vother.col[index] = end;
            this.output.key.col[index] = key;
            this.output.payload.col[index] = payload;
            this.output.hash.col[index] = hash;

            if (this.output.Count == Config.DataBatchSize) FlushContents();
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => Observer.ProduceQueryPlan(new SamplePlanNode(
                previous, this, typeof(TKey), typeof(TSource), typeof(TResult), this.offset, this.period));

        protected override void FlushContents()
        {
            if (output.Count == 0) return;
            this.output.iter = this.batchIter;
            this.output.Seal();
            this.Observer.OnNext(this.output);
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        protected override void DisposeState() => this.output.Free();

        public override int CurrentlyBufferedOutputCount => this.output.Count;

        public override int CurrentlyBufferedInputCount => this.edges.Count + this.intervals.Count;


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool AreSame(long start, ref TKey key, ref TSource payload, ref ActiveEdge active)
            => start == active.start && this.keyComparer(key, active.key) && this.payloadComparer(payload, active.payload);

        [DataContract]
        private struct ActiveInterval
        {
            [DataMember]
            public long end;
            [DataMember]
            public TKey key;
            [DataMember]
            public TSource payload;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Populate(long end, ref TKey key, ref TSource payload)
            {
                this.end = end;
                this.key = key;
                this.payload = payload;
            }

            public override string ToString() => "[End=" + end + ", Key='" + key + "', Payload='" + payload + "']";
        }

        [DataContract]
        private struct ActiveEdge
        {
            [DataMember]
            public long start;
            [DataMember]
            public TKey key;
            [DataMember]
            public TSource payload;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Populate(long start, ref TKey key, ref TSource payload)
            {
                this.start = start;
                this.key = key;
                this.payload = payload;
            }

            public override string ToString() => "[Start=" + start + ", Key='" + key + "', Payload='" + payload + "']";
        }
    }
}
