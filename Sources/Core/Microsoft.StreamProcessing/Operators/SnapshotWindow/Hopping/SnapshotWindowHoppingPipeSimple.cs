// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Aggregates;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Operator that has a simple queue (we know that Ve values are non-decreasing and number of entries is capped)
    /// </summary>
    [DataContract]
    internal sealed class SnapshotWindowHoppingPipeSimple<TInput, TState, TOutput> : UnaryPipe<Empty, TInput, TOutput>
    {
        private static readonly bool hasDisposableState = typeof(IDisposable).GetTypeInfo().IsAssignableFrom(typeof(TState));
        private readonly MemoryPool<Empty, TOutput> pool;
        private readonly string errorMessages;

        private readonly IAggregate<TInput, TState, TOutput> aggregate;
        [SchemaSerialization]
        private readonly Expression<Func<TState>> initialStateExpr;
        private readonly Func<TState> initialState;
        [SchemaSerialization]
        private readonly Expression<Func<TState, long, TInput, TState>> accumulateExpr;
        private readonly Func<TState, long, TInput, TState> accumulate;
        [SchemaSerialization]
        private readonly Expression<Func<TState, long, TInput, TState>> deaccumulateExpr;
        private readonly Func<TState, long, TInput, TState> deaccumulate;
        [SchemaSerialization]
        private readonly Expression<Func<TState, TState, TState>> differenceExpr;
        private readonly Func<TState, TState, TState> difference;
        [SchemaSerialization]
        private readonly Expression<Func<TState, TOutput>> computeResultExpr;
        private readonly Func<TState, TOutput> computeResult;

        [DataMember]
        private StreamMessage<Empty, TOutput> batch;

        [DataMember]
        private CircularBuffer<HeldState<TState>> ecq;
        [DataMember]
        private long lastSyncTime = long.MinValue;

        [DataMember]
        private HeldState<TState> currentState;
        private HeldState<TState> currentEcqHeldState;
        [DataMember]
        private bool held;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public SnapshotWindowHoppingPipeSimple() { }

        public SnapshotWindowHoppingPipeSimple(
            SnapshotWindowStreamable<Empty, TInput, TState, TOutput> stream,
            IStreamObserver<Empty, TOutput> observer)
            : base(stream, observer)
        {
            this.aggregate = stream.Aggregate;
            this.initialStateExpr = this.aggregate.InitialState();
            this.initialState = this.initialStateExpr.Compile();
            this.accumulateExpr = this.aggregate.Accumulate();
            this.accumulate = this.accumulateExpr.Compile();
            this.deaccumulateExpr = this.aggregate.Deaccumulate();
            this.deaccumulate = this.deaccumulateExpr.Compile();
            this.differenceExpr = this.aggregate.Difference();
            this.difference = this.differenceExpr.Compile();
            this.computeResultExpr = this.aggregate.ComputeResult();
            this.computeResult = this.computeResultExpr.Compile();

            this.errorMessages = stream.ErrorMessages;
            this.pool = MemoryManager.GetMemoryPool<Empty, TOutput>(false);
            this.pool.Get(out this.batch);
            this.batch.Allocate();

            var hopsPerDuration = (int)(stream.Source.Properties.ConstantDurationLength.Value / stream.Source.Properties.ConstantHopLength) + 1;
            this.ecq = new CircularBuffer<HeldState<TState>>(hopsPerDuration);
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new SnapshotWindowPlanNode<TInput, TState, TOutput>(
                previous, this, typeof(Empty), typeof(TInput), typeof(TOutput),
                AggregatePipeType.Hopping, this.aggregate, false, this.errorMessages));

        public override unsafe void OnNext(StreamMessage<Empty, TInput> batch)
        {
            this.batch.iter = batch.iter;

            var count = batch.Count;
            var colpayload = batch.payload.col;

            fixed (long* col_vsync = batch.vsync.col)
            fixed (long* col_vother = batch.vother.col)
            fixed (int* col_hash = batch.hash.col)
            fixed (long* col_bv = batch.bitvector.col)
            {
                for (int i = 0; i < count; i++)
                {
                    if ((col_bv[i >> 6] & (1L << (i & 0x3f))) != 0)
                    {
                        if (col_vother[i] == StreamEvent.PunctuationOtherTime)
                        {
                            // We have found a row that corresponds to punctuation
                            OnPunctuation(col_vsync[i]);

                            int c = this.batch.Count;
                            this.batch.vsync.col[c] = col_vsync[i];
                            this.batch.vother.col[c] = StreamEvent.PunctuationOtherTime;
                            this.batch.key.col[c] = Empty.Default;
                            this.batch.hash.col[c] = 0;
                            this.batch.bitvector.col[c >> 6] |= 1L << (c & 0x3f);
                            this.batch.Count++;
                            if (this.batch.Count == Config.DataBatchSize) FlushContents();
                        }
                        continue;
                    }

                    var syncTime = col_vsync[i];

                    // Handle time moving forward
                    if (syncTime > this.lastSyncTime) AdvanceTime(syncTime);

                    if (this.currentState == null)
                    {
                        this.currentEcqHeldState = null;
                        this.currentState = new HeldState<TState> { state = this.initialState(), timestamp = syncTime };
                        this.held = true;

                        // No output because initial state is empty
                    }
                    else
                    {
                        if (syncTime > this.currentState.timestamp)
                        {
                            if (this.currentState.active > 0)
                            {
                                // Output end edge
                                int c = this.batch.Count;
                                this.batch.vsync.col[c] = syncTime;
                                this.batch.vother.col[c] = this.currentState.timestamp;
                                this.batch.payload.col[c] = this.computeResult(this.currentState.state);
                                this.batch.key.col[c] = Empty.Default;
                                this.batch.hash.col[c] = 0;
                                this.batch.Count++;
                                if (this.batch.Count == Config.DataBatchSize) FlushContents();
                            }

                            this.currentState.timestamp = syncTime;
                            this.held = true;
                        }
                    }

                    if (col_vsync[i] < col_vother[i]) // insert event
                    {
                        this.currentState.state = this.accumulate(this.currentState.state, col_vsync[i], colpayload[i]);
                        this.currentState.active++;

                        // Update ECQ
                        if (col_vother[i] < StreamEvent.InfinitySyncTime)
                        {
                            if ((this.currentEcqHeldState == null) || (this.currentEcqHeldState.timestamp != col_vother[i]))
                            {
                                if (this.ecq.Count > 0)
                                {
                                    this.currentEcqHeldState = this.ecq.PeekLast();
                                    if (this.currentEcqHeldState.timestamp != col_vother[i])
                                    {
                                        this.currentEcqHeldState = new HeldState<TState> { state = this.initialState(), timestamp = col_vother[i] };
                                        this.ecq.Enqueue(ref this.currentEcqHeldState);
                                    }
                                }
                                else
                                {
                                    this.currentEcqHeldState = new HeldState<TState> { state = this.initialState(), timestamp = col_vother[i] };
                                    this.ecq.Enqueue(ref this.currentEcqHeldState);
                                }
                            }

                            this.currentEcqHeldState.state = this.accumulate(this.currentEcqHeldState.state, col_vsync[i], colpayload[i]);
                            this.currentEcqHeldState.active++;
                        }
                    }
                    else // is a retraction
                    {
                        this.currentState.state = this.deaccumulate(this.currentState.state, col_vsync[i], colpayload[i]);
                        this.currentState.active--;
                    }
                }
            }

            batch.Release();
            batch.Return();
        }

        public void OnPunctuation(long syncTime)
        {
            // Handle time moving forward
            if (syncTime > this.lastSyncTime) AdvanceTime(syncTime);
        }

        private void AdvanceTime(long syncTime)
        {
            /* Issue start edges for held aggregates */
            if (this.currentState != null && this.held)
            {
                if (this.currentState.active > 0)
                {
                    int c = this.batch.Count;
                    this.batch.vsync.col[c] = this.currentState.timestamp;
                    this.batch.vother.col[c] = StreamEvent.InfinitySyncTime;
                    this.batch.payload.col[c] = this.computeResult(this.currentState.state);
                    this.batch.key.col[c] = Empty.Default;
                    this.batch.hash.col[c] = 0;
                    this.batch.Count++;
                    if (this.batch.Count == Config.DataBatchSize) FlushContents();
                }

                this.held = false;
            }

            /* Process the ECQ up until the new sync time */
            while (this.ecq.Count > 0 && this.ecq.PeekFirst().timestamp <= syncTime)
            {
                HeldState<TState> ecqState = this.ecq.Dequeue();
                if (this.currentState.active > 0)
                {
                    // Issue end edge
                    int c = this.batch.Count;
                    this.batch.vsync.col[c] = ecqState.timestamp;
                    this.batch.vother.col[c] = this.currentState.timestamp;
                    this.batch.payload.col[c] = this.computeResult(this.currentState.state);
                    this.batch.key.col[c] = Empty.Default;
                    this.batch.hash.col[c] = 0;
                    this.batch.Count++;
                    if (this.batch.Count == Config.DataBatchSize) FlushContents();
                }

                // Update aggregate
                this.currentState.state = this.difference(this.currentState.state, ecqState.state);
                this.currentState.active -= ecqState.active;
                (ecqState.state as IDisposable)?.Dispose();

                if (ecqState.timestamp < syncTime)
                {
                    if (this.currentState.active > 0)
                    {
                        // Issue start edge
                        int c = this.batch.Count;
                        this.batch.vsync.col[c] = ecqState.timestamp;
                        this.batch.vother.col[c] = StreamEvent.InfinitySyncTime;
                        this.batch.payload.col[c] = this.computeResult(this.currentState.state);
                        this.batch.key.col[c] = Empty.Default;
                        this.batch.hash.col[c] = 0;
                        this.batch.Count++;
                        if (this.batch.Count == Config.DataBatchSize) FlushContents();
                    }
                    else
                    {
                        (this.currentState.state as IDisposable)?.Dispose();
                        this.currentState = null;
                    }
                }
                else
                    this.held = true;

                // Update timestamp
                if (this.currentState != null) this.currentState.timestamp = ecqState.timestamp;
            }

            // Since sync time changed, set lastSyncTime
            this.lastSyncTime = syncTime;
        }

        protected override void FlushContents()
        {
            if (this.batch == null || this.batch.Count == 0) return;
            this.batch.Seal();
            this.Observer.OnNext(this.batch);
            this.pool.Get(out this.batch);
            this.batch.Allocate();
        }

        public override int CurrentlyBufferedOutputCount => this.batch.Count;

        public override int CurrentlyBufferedInputCount => this.ecq.Count;

        protected override void DisposeState()
        {
            this.batch.Free();
            if (hasDisposableState)
            {
                if (this.currentState != null)
                {
                    ((IDisposable)this.currentState.state).Dispose();
                }

                while (!this.ecq.IsEmpty())
                {
                    var e = this.ecq.Dequeue();
                    ((IDisposable)e.state).Dispose();
                }
            }
        }
    }
}