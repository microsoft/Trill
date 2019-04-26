// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing.Signal
{
    [DataContract]
    internal sealed class InterpolatePipe<TKey, TSource, TResult> : UnaryPipe<TKey, TSource, TResult>, IDisposable
    {
        private readonly MemoryPool<TKey, TResult> pool;
        private readonly DataStructurePool<List<StreamMessage<TKey, TResult>>> batchListPool;

        [SchemaSerialization]
        private readonly long offset;
        [SchemaSerialization]
        private readonly long period;
        [SchemaSerialization]
        private readonly InterpolationPolicy<TResult> interpolationPolicy;
        [SchemaSerialization]
        private readonly long interpolationWindow;

        [SchemaSerialization]
        private readonly bool isUngrouped;

        [SchemaSerialization]
        private readonly Expression<Func<TKey, TKey, bool>> keyComparerExpr;
        private readonly Func<TKey, TKey, bool> keyComparer;
        [SchemaSerialization]
        private readonly Expression<Func<TSource, TSource, bool>> payloadComparerExpr;
        private readonly Func<TSource, TSource, bool> payloadComparer;
        [SchemaSerialization]
        private readonly Expression<Func<long, TSource, TResult>> signalFunctionExpr;
        private readonly Func<long, TSource, TResult> signalFunction;

        [SchemaSerialization]
        private readonly Expression<Func<Interpolator<TResult>>> createInterpolatorExpr;
        private readonly Func<Interpolator<TResult>> createInterpolator;

        [DataMember]
        private StreamMessage<TKey, TResult> output;

        [DataMember]
        private FastMap<ActiveInterval> intervals = new FastMap<ActiveInterval>();
        [DataMember]
        private FastMap<ActiveEdge> edges = new FastMap<ActiveEdge>();
        [DataMember]
        private FastMap<ActiveEdge> newEdges = new FastMap<ActiveEdge>();
        [DataMember]
        private long currBeatTime = long.MinValue;
        [DataMember]
        private long lastTime = long.MinValue;
        [DataMember]
        private int batchIter;

        [DataMember]
        private FastDictionary2<TKey, Interpolator<TResult>> interpolators;
        [DataMember]
        private FastDictionary2<long, List<StreamMessage<TKey, TResult>>> interpolationBuffer;

        private Interpolator<TResult> currentInterpolator;
        private TKey currentKey;
        private int currentHash;

        public InterpolatePipe() { }

        public InterpolatePipe(InterpolateStreamable<TKey, TSource, TResult> stream, IStreamObserver<TKey, TResult> observer)
            : base(stream, observer)
        {
            this.offset = stream.offset;
            this.period = stream.period;
            this.interpolationPolicy = stream.interpolationPolicy;

            // Adjust interpolation window size to be a multiple of period
            this.interpolationWindow = (interpolationPolicy.windowSize + period - 1) / period * period;

            this.isUngrouped = typeof(TKey) == typeof(Empty);

            var keyEqualityComparer = stream.Properties.KeyEqualityComparer;
            this.keyComparerExpr = keyEqualityComparer.GetEqualsExpr();
            this.keyComparer = this.keyComparerExpr.Compile();
            var getHashCode = keyEqualityComparer.GetGetHashCodeExpr().Compile();

            this.payloadComparerExpr = stream.sourcePayloadEqualityComparer.GetEqualsExpr();
            this.payloadComparer = this.payloadComparerExpr.Compile();

            this.signalFunctionExpr = stream.signalFunction;
            this.signalFunction = signalFunctionExpr.Compile();

            this.createInterpolatorExpr = interpolationPolicy.NewInterpolator();
            this.createInterpolator = createInterpolatorExpr.Compile();

            this.batchListPool = new DataStructurePool<List<StreamMessage<TKey, TResult>>>();
            this.pool = MemoryManager.GetMemoryPool<TKey, TResult>(stream.Properties.IsColumnar);
            this.pool.Get(out this.output);
            this.output.Allocate();

            this.interpolators = keyEqualityComparer.CreateFastDictionary2Generator<TKey, Interpolator<TResult>>(1, this.keyComparer, getHashCode, stream.Properties.QueryContainer).Invoke();
            var defaultcomparer = EqualityComparerExpression<long>.Default;
            this.interpolationBuffer = defaultcomparer.CreateFastDictionary2Generator<long, List<StreamMessage<TKey, TResult>>>(1, defaultcomparer.GetEqualsExpr().Compile(), defaultcomparer.GetGetHashCodeExpr().Compile(), stream.Properties.QueryContainer).Invoke();
            this.currentInterpolator = null;
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
                        TResult payload = this.signalFunction(startTime, sourcePayload[row]);

                        AdvanceTime(startTime);

                        if (this.currentInterpolator == null || ((!this.isUngrouped) && (this.currentHash != hash || !this.keyComparer(this.currentKey, sourceKey[row]))))
                        {
                            this.currentKey = sourceKey[row];
                            this.currentHash = hash;

                            if (!this.interpolators.Lookup(sourceKey[row], hash, out int index))
                            {
                                // First time group is active for this time
                                this.currentInterpolator = createInterpolator();
                                this.interpolators.Insert(sourceKey[row], this.currentInterpolator, hash);
                            }
                            else
                            {
                                // Read new currentInterpolator from _interpolators
                                this.currentInterpolator = this.interpolators.entries[index].value;
                            }
                        }

                        bool isInsert = startTime < endTime;
                        bool isStartEdge = isInsert && endTime == StreamEvent.InfinitySyncTime;
                        bool isEndEdge = !isInsert;
                        if (isStartEdge)
                        {
                            // Add to active edges list to handle repeat at beats (and waiting for closing edge).
                            int indexEdge = this.edges.Insert(hash);
                            this.edges.Values[indexEdge].Populate(
                                startTime,
                                ref sourceKey[row],
                                ref sourcePayload[row],
                                this.currentInterpolator);

                            // Add edge to _newEdges for processing on ReachBeat(currBeatTime) if startTime is
                            // before the current beat; otherwise, it is processed on LeaveBeat(currBeatTime)
                            if (startTime < currBeatTime)
                            {
                                indexEdge = this.newEdges.Insert(hash);
                                this.newEdges.Values[indexEdge].Populate(
                                    startTime,
                                    ref sourceKey[row],
                                    ref sourcePayload[row],
                                    this.currentInterpolator);
                            }
                        }
                        else if (isEndEdge)
                        {
                            long edgeStartTime = endTime;
                            long edgeEndTime = startTime;

                            // Each event interval [a, b), in this case defined by a pair of start and end edges,
                            // gets transformed into at least two samples: a start sample [a, a + 1) if a < b,
                            // an end sample [b - 1, b) if a < b - 1, and any additional samples created at
                            // sampling beats.

                            long previousBeatTime = currBeatTime - period;
                            long sampleTime = edgeEndTime - 1;

                            // Create sample at sampleTime if differs from edgeStartTime and previousBeatTime
                            if (edgeStartTime < sampleTime && previousBeatTime < sampleTime)
                            {
                                // The interpolation window size is at least one tick,
                                // so adding samples at sampleTime will not cause out of ordering
                                ProcessNewPoint(this.currentInterpolator, sampleTime, ref sourceKey[row], ref sourcePayload[row], hash, currBeatTime);
                            }

                            // Remove from active edges list.
                            var edgesTraversal = edges.Find(hash);
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
                            // Each event interval [a, b) gets transformed into at least two samples:
                            // a start sample [a, a + 1) if a < b, an end sample [b - 1, b) if a < b - 1,
                            // and any additional samples created at sampling beats.
                            ProcessNewPoint(this.currentInterpolator, startTime, ref sourceKey[row], ref sourcePayload[row], hash, currBeatTime);

                            // Add to active list only if duration greater than one
                            if (endTime > startTime + 1)
                            {
                                // Add to active list to handle repeat at beats.
                                int indexInterval = intervals.Insert(hash);
                                intervals.Values[indexInterval].Populate(
                                    endTime,
                                    ref sourceKey[row],
                                    ref sourcePayload[row],
                                    this.currentInterpolator);
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
        private void ProcessNewPoint(Interpolator<TResult> interpolator, long time, ref TKey key, ref TSource sourcePayload, int hash, long nextBeatTime)
        {
            TResult payload = signalFunction(time, sourcePayload);

            // Invariant: we interpolated all samples up to and including interpolator.RightTime.
            // Now we need to interpolate samples from (interpolator.RightTime, time], if any.

            // Buffer new point if falling on nextBeatTime
            if (time == nextBeatTime)
            {
                AddSampleToBuffer(time, ref key, ref payload, hash);
            }

            // Now interpolate samples from (interpolator.RightTime, nextBeatTime - period)
            long interpolationStartTime = interpolator.RightTime;
            long interpolationEndTime = nextBeatTime - period;

            interpolator.AddPoint(time, ref payload);

            if (interpolator.CanInterpolate(interpolationEndTime))
            {
                // Interpolator can interpolate all samples from (interpolationStartTime, interpolationEndTime)

                // We know that interpolationEndTime is aligned with beatTime,
                // so we generate samples backwards to avoid beat recomputation.
                while (interpolationStartTime < interpolationEndTime)
                {
                    interpolator.Interpolate(interpolationEndTime, out TResult interpolatedSample);
                    AddSampleToBuffer(interpolationEndTime, ref key, ref interpolatedSample, hash);
                    interpolationEndTime -= period;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AddSampleToBuffer(long time, ref TKey key, ref TResult payload, int hash)
        {
            StreamMessage<TKey, TResult> batch;
            List<StreamMessage<TKey, TResult>> batchList;

            // Lookup/create the buffer for a given timestamp
            if (!this.interpolationBuffer.Lookup(time, out int index))
            {
                // Allocate new batch
                this.pool.Get(out batch);
                batch.Allocate();

                // Create new batch list
                this.batchListPool.Get(out batchList);
                batchList.Add(batch);

                // Insert into interpolation buffer
                this.interpolationBuffer.Insert(time, batchList);
            }
            else
            {
                batchList = this.interpolationBuffer.entries[index].value;
                batch = batchList[batchList.Count - 1];
            }

            int batchIndex = batch.Count++;
            batch.vsync.col[batchIndex] = time;
            batch.vother.col[batchIndex] = time + 1;
            batch.key.col[batchIndex] = key;
            batch.payload.col[batchIndex] = payload;
            batch.hash.col[batchIndex] = hash;

            if (batch.Count == Config.DataBatchSize)
            {
                // Allocate new batch
                pool.Get(out batch);
                batch.Allocate();

                // Add batch to list
                batchList.Add(batch);
            }
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

            if (this.edges.IsEmpty && this.intervals.IsEmpty && this.interpolationBuffer.Count == 0 && this.newEdges.IsEmpty)
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

                if (this.edges.IsEmpty && this.intervals.IsEmpty && this.interpolationBuffer.Count == 0 && this.newEdges.IsEmpty)
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

                LeaveBeat(this.currBeatTime);
                this.currBeatTime += period;
                ReachBeat(this.currBeatTime);

                if (this.edges.IsEmpty && this.intervals.IsEmpty && this.interpolationBuffer.Count == 0 && this.newEdges.IsEmpty)
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
            // Process all active edges that started between two last beats.
            var edgeTraverser = this.newEdges.Traverse();
            int index;
            int hash;
            while (edgeTraverser.Next(out index, out hash))
            {
                var interpolator = this.newEdges.Values[index].Interpolator;

                // Each event interval [vs, vo), in this case defined by a pair of start and end edges,
                // gets transformed into at least two samples: a start sample [vs, vs + 1) if vs < vo,
                // an end sample [vo - 1, vo) if vs < vo - 1, and any additional samples created at
                // sampling beats.

                // Create a sample at edgeStartTime if the event started between two last beats
                long edgeStartTime = this.newEdges.Values[index].Start;
                // if (previousBeatTime < edgeStartTime && edgeStartTime < beatTime)
                {
                    // Add sample at edgeStartTime
                    ProcessNewPoint(interpolator, edgeStartTime, ref this.newEdges.Values[index].Key, ref this.newEdges.Values[index].Payload, hash, beatTime);
                }
            }
            // Clear new edges as no longer need to output.
            this.newEdges.Clear();

            // Add all active intervals.
            var intervalTraverser = this.intervals.Traverse();
            while (intervalTraverser.Next(out index, out hash))
            {
                var interpolator = this.intervals.Values[index].interpolator;

                // Each event interval [vs, vo) gets transformed into at least two samples:
                // a start sample [vs, vs + 1) if vs < vo, an end sample [vo - 1, vo) if vs < vo - 1,
                // and any additional samples created at sampling beats.

                long intervalEndTime = this.intervals.Values[index].end;

                // Create sample and remove interval if ends before or at (beatTime + 1).
                if (intervalEndTime <= beatTime + 1)
                {
                    // We know that intervalEndTime > (previousBeatTime + 1)
                    // (otherwise, the interval would have been removed),
                    // so the sample cannot overlap with the previous beat.
                    ProcessNewPoint(interpolator, intervalEndTime - 1, ref this.intervals.Values[index].key, ref this.intervals.Values[index].payload, hash, beatTime);

                    // Remove from active list as no longer need to output.
                    intervalTraverser.Remove();
                }
                else
                {
                    // Add sample at beatTime
                    ProcessNewPoint(interpolator, beatTime, ref this.intervals.Values[index].key, ref this.intervals.Values[index].payload, hash, beatTime);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void LeaveBeat(long beatTime)
        {
            // Process all active edges (that weren't added during this beat).
            var edgeTraverser = this.edges.Traverse();
            while (edgeTraverser.Next(out int index, out int hash))
            {
                var interpolator = this.edges.Values[index].Interpolator;
                ProcessNewPoint(interpolator, beatTime, ref this.edges.Values[index].Key, ref this.edges.Values[index].Payload, hash, beatTime);
            }

            LeaveWatermark(beatTime - this.interpolationWindow);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void LeaveWatermark(long watermarkTime)
        {
            if (watermarkTime < StreamEvent.MinSyncTime)
            {
                // Time has not progressed enough to active watermark.
                return;
            }

            if (this.interpolationBuffer.Lookup(watermarkTime, out int index))
            {
                // Release batches at watermark
                var batchList = this.interpolationBuffer.entries[index].value;
                foreach (var batch in batchList)
                {
                    AddToOutput(batch);
                    batch.Free();
                }

                // Clear list
                batchList.Clear();
                this.batchListPool.Return(batchList);

                // Remove list from buffer
                this.interpolationBuffer.Remove(watermarkTime);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long FindNextBeatGreaterThanOrEqualTo(long time)
        {
            long multiple = (time - this.offset + this.period - 1) / this.period;
            return (multiple * this.period) + this.offset;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe void AddToOutput(StreamMessage<TKey, TResult> input)
        {
            // Bulk copy from input to output, avoid branching inside loop
            int availableSize = Config.DataBatchSize - this.output.Count;        // always greater than zero
            int smallerSize = availableSize < input.Count ? availableSize : input.Count;

            Array.Copy(input.key.col, 0, this.output.key.col, this.output.Count, smallerSize);
            Array.Copy(input.payload.col, 0, this.output.payload.col, this.output.Count, smallerSize);
            Array.Copy(input.vsync.col, 0, this.output.vsync.col, this.output.Count, smallerSize);
            Array.Copy(input.vother.col, 0, this.output.vother.col, this.output.Count, smallerSize);
            Array.Copy(input.hash.col, 0, this.output.hash.col, this.output.Count, smallerSize);
            this.output.Count += smallerSize;

            if (this.output.Count == Config.DataBatchSize)
            {
                FlushContents();

                var remainingSize = input.Count - smallerSize;
                if (remainingSize > 0)
                {
                    Array.Copy(input.key.col, 0, this.output.key.col, 0, remainingSize);
                    Array.Copy(input.payload.col, 0, this.output.payload.col, 0, remainingSize);
                    Array.Copy(input.vsync.col, 0, this.output.vsync.col, 0, remainingSize);
                    Array.Copy(input.vother.col, 0, this.output.vother.col, 0, remainingSize);
                    Array.Copy(input.hash.col, 0, this.output.hash.col, 0, remainingSize);
                    this.output.Count += remainingSize;
                }
            }
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new InterpolatePlanNode(
                previous, this, typeof(TKey), typeof(TSource), typeof(TResult), this.offset, this.period));

        protected override void FlushContents()
        {
            if (output.Count == 0) return;
            this.output.iter = batchIter;
            this.output.Seal();
            this.Observer.OnNext(this.output);
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        public override int CurrentlyBufferedOutputCount => this.output.Count;

        public override int CurrentlyBufferedInputCount => this.intervals.Count + this.edges.Count + this.newEdges.Count;

        protected override void DisposeState() => this.batchListPool.Dispose();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool AreSame(long start, ref TKey key, ref TSource payload, ref ActiveEdge active)
            => start == active.Start && this.keyComparer(key, active.Key) && this.payloadComparer(payload, active.Payload);

        [DataContract]
        private struct ActiveInterval
        {
            [DataMember]
            public long end;
            [DataMember]
            public TKey key;
            [DataMember]
            public TSource payload;
            [DataMember]
            public Interpolator<TResult> interpolator;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Populate(long end, ref TKey key, ref TSource payload, Interpolator<TResult> interpolator)
            {
                this.end = end;
                this.key = key;
                this.payload = payload;
                this.interpolator = interpolator;
            }

            public override string ToString()
                => "[End=" + this.end + ", Key='" + this.key + "', Payload='" + this.payload + "', Interpolator='" + this.interpolator + "']";
        }

        [DataContract]
        private struct ActiveEdge
        {
            [DataMember]
            public long Start;
            [DataMember]
            public TKey Key;
            [DataMember]
            public TSource Payload;
            [DataMember]
            public Interpolator<TResult> Interpolator;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Populate(long start, ref TKey key, ref TSource payload, Interpolator<TResult> interpolator)
            {
                Start = start;
                Key = key;
                Payload = payload;
                Interpolator = interpolator;
            }

            public override string ToString() => "[Start=" + Start + ", Key='" + Key + "', Payload='" + Payload + "', Interpolator='" + Interpolator + "']";
        }
    }
}
