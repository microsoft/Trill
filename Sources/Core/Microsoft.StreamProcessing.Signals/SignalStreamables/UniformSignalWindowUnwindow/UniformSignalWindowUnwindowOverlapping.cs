// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Aggregates;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing.Signal.UDO
{
    [DataContract]
    internal sealed class OverlappingPipe<TKey, TSource, TState, TResult> : UnaryPipe<TKey, TSource, TResult>
    {
        private readonly MemoryPool<TKey, TResult> pool;
        private readonly string errorMessages;

        [SchemaSerialization]
        private readonly Expression<Func<TKey, TKey, bool>> keyComparerEqualsExpr;
        private readonly Func<TKey, TKey, bool> keyComparerEquals;

        [SchemaSerialization]
        private readonly long periodTicks;
        [SchemaSerialization]
        private readonly long offsetTicks;
        [SchemaSerialization]
        private readonly int inputWindowSize;
        [SchemaSerialization]
        private readonly int outputWindowSize;
        [SchemaSerialization]
        private readonly int hopSize;
        [SchemaSerialization]
        private readonly long inputWindowSizeTicks;
        [SchemaSerialization]
        private readonly long outputWindowSizeTicks;
        [SchemaSerialization]
        private readonly long hopSizeTicks;
        [SchemaSerialization]
        private readonly bool setMissingDataToNull;
        [SchemaSerialization]
        private readonly bool isUngrouped;

        [SchemaSerialization]
        private readonly Func<ISignalWindowObservable<TSource>, ISignalWindowObservable<TResult>> operatorPipeline;
        [SchemaSerialization]
        private readonly IAggregate<TResult, TState, TResult> aggregate;
        [SchemaSerialization]
        private readonly Expression<Func<TState>> initialStateExpr;
        private readonly Func<TState> initialState;
        [SchemaSerialization]
        private readonly Expression<Func<TState, long, TResult, TState>> accumulateExpr;
        private readonly Func<TState, long, TResult, TState> accumulate;
        [SchemaSerialization]
        private readonly Expression<Func<TState, TResult>> computeResultExpr;
        private readonly Func<TState, TResult> computeResult;

        [SchemaSerialization]
        private readonly WindowPipeline<TSource, TResult> windowPipeline;
        private readonly bool windowPipelineIsEmpty;

        [DataMember]
        private StreamMessage<TKey, TResult> output;

        [DataMember]
        private FastDictionary3<TKey, WindowState> windowStates;
        [DataMember]
        private int numberOfActiveInputStates;
        [DataMember]
        private int numberOfActiveOutputStates;
        [DataMember]
        private long lastSyncTime = long.MinValue;
        [DataMember]
        private int batchIter;

        private WindowState currentState;
        private TKey currentKey;
        private int currentHash;

        public override int CurrentlyBufferedOutputCount => output.Count;

        public override int CurrentlyBufferedInputCount => windowStates.Count;

        public OverlappingPipe() { }

        public OverlappingPipe(UniformSignalWindowUnwindowStreamable<TKey, TSource, TState, TResult> stream, IStreamObserver<TKey, TResult> observer)
            : base(stream, observer)
        {
            this.pool = MemoryManager.GetMemoryPool<TKey, TResult>(stream.Properties.IsColumnar);
            this.pool.Get(out this.output);
            this.output.Allocate();
            this.errorMessages = stream.ErrorMessages;

            var comparer = stream.Properties.KeyEqualityComparer;
            this.keyComparerEqualsExpr = comparer.GetEqualsExpr();
            this.keyComparerEquals = this.keyComparerEqualsExpr.Compile();
            var getHashCode = comparer.GetGetHashCodeExpr().Compile();

            this.periodTicks = stream.PeriodTicks;
            this.offsetTicks = stream.OffsetTicks;
            this.inputWindowSize = stream.WindowSize;
            this.outputWindowSize = stream.OutputWindowSize;
            this.hopSize = stream.HopSize;
            this.inputWindowSizeTicks = stream.WindowSize * periodTicks;
            this.outputWindowSizeTicks = stream.OutputWindowSize * periodTicks;
            this.hopSizeTicks = stream.HopSize * periodTicks;

            this.setMissingDataToNull = stream.SetMissingDataToNull;
            this.operatorPipeline = stream.OperatorPipeline;

            this.isUngrouped = typeof(TKey) == typeof(Empty);

            this.aggregate = stream.Aggregate;
            this.initialStateExpr = this.aggregate.InitialState();
            this.initialState = this.initialStateExpr.Compile();
            this.accumulateExpr = aggregate.Accumulate();
            this.accumulate = this.accumulateExpr.Compile();
            this.computeResultExpr = aggregate.ComputeResult();
            this.computeResult = this.computeResultExpr.Compile();

            this.windowPipeline = new WindowPipeline<TSource, TResult>(this.operatorPipeline, this.inputWindowSize);
            this.windowPipelineIsEmpty = this.windowPipeline.IsEmpty;

            this.windowStates = comparer.CreateFastDictionary3Generator<TKey, WindowState>(1, this.keyComparerEquals, getHashCode, stream.Properties.QueryContainer).Invoke();
            this.numberOfActiveInputStates = 0;
            this.numberOfActiveOutputStates = 0;
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => Observer.ProduceQueryPlan(new UniformSignalWindowToArrayPlanNode<TSource, TResult>(
                previous, this, typeof(TKey), typeof(TSource), typeof(TResult), this.operatorPipeline, false, this.errorMessages));

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
                        int hash = *sourceHashPtr;

                        AdvanceTime(startTime);

                        if (this.currentState == null || ((!this.isUngrouped) && (this.currentHash != hash || !this.keyComparerEquals(this.currentKey, sourceKey[row]))))
                        {
                            this.currentKey = sourceKey[row];
                            this.currentHash = hash;

                            if (!this.windowStates.Lookup(this.currentKey, this.currentHash, out int index))
                            {
                                // First time group is active for this time.
                                // Create window pipeline and wire together observers and observables.

                                var resultObserver = new OverlappingOutputObserver(this, ref this.currentKey, this.currentHash);

                                ISignalWindowObserver<TSource> firstPipelineObserver;
                                if (this.windowPipelineIsEmpty)
                                {
                                    // When the pipeline is empty (the pipeline lambda is identity),
                                    // there is no transformation and we can send directly to resultObserver
                                    firstPipelineObserver = (ISignalWindowObserver<TSource>)(object)resultObserver;
                                }
                                else
                                {
                                    this.windowPipeline.CreateNewPipeline(out firstPipelineObserver, out var lastPipelineObservable);
                                    lastPipelineObservable.Subscribe(resultObserver);
                                }

                                var inputWindow =
                                    SignalWindow<TSource>.GetInstance(
                                        firstPipelineObserver,
                                        this.periodTicks,
                                        this.offsetTicks,
                                        this.inputWindowSize,
                                        this.hopSize,
                                        this.setMissingDataToNull);
                                this.currentState = new WindowState(this, inputWindow, resultObserver);
                                this.windowStates.Insert(ref index, this.currentKey, this.currentState, this.currentHash);
                            }
                            else
                            {
                                // Read new currentState from _filterStates
                                this.currentState = this.windowStates.entries[index].value;
                            }
                        }

                        int beforeInput = this.currentState.inputWindow.numberOfActiveItems == 0 ? 0 : 1;
                        int beforeOutput = this.currentState.outputObserver.numberOfActiveItems == 0 ? 0 : 1;

                        // Enqueue items may increase or decrease the number of active items
                        this.currentState.inputWindow.Enqueue(startTime, ref sourcePayload[row]);

                        int afterInput = this.currentState.inputWindow.numberOfActiveItems == 0 ? 0 : 1;
                        int afterOutput = this.currentState.outputObserver.numberOfActiveItems == 0 ? 0 : 1;

                        this.numberOfActiveInputStates += afterInput - beforeInput;
                        this.numberOfActiveOutputStates += afterOutput - beforeOutput;
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

        protected override void FlushContents()
        {
            if (this.output.Count == 0) return;
            this.output.iter = this.batchIter;
            this.output.Seal();
            this.Observer.OnNext(this.output);
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        protected override void DisposeState() => this.output.Free();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AdvanceTime(long time)
        {
            if (time == this.lastSyncTime) return;

            bool hasActiveStates = (this.numberOfActiveInputStates | this.numberOfActiveOutputStates) != 0;

            if (hasActiveStates)
            {
                if (this.isUngrouped)
                {
                    if (this.currentState.inputWindow.numberOfActiveItems > 0)
                    {
                        this.currentState.inputWindow.AdvanceTime(time);
                        if (this.currentState.inputWindow.numberOfActiveItems == 0)
                        {
                            this.numberOfActiveInputStates--;
                        }
                    }
                    if (this.currentState.outputObserver.numberOfActiveItems > 0)
                    {
                        this.currentState.outputObserver.AdvanceTime(time);
                        if (this.currentState.outputObserver.numberOfActiveItems == 0)
                        {
                            this.numberOfActiveOutputStates--;
                        }
                    }
                    this.lastSyncTime = time;
                    return;
                }

                var nextTime = this.lastSyncTime + this.periodTicks;
                while (hasActiveStates && time >= nextTime)
                {
                    // Iterate over all active windows and advance time
                    int iter1 = FastDictionary3<TKey, WindowState>.IteratorStart;
                    while (hasActiveStates && this.windowStates.Iterate(ref iter1))
                    {
                        var state = this.windowStates.entries[iter1].value;

                        if (state.inputWindow.numberOfActiveItems > 0)
                        {
                            state.inputWindow.AdvanceTime(time);
                            if (state.inputWindow.numberOfActiveItems == 0)
                            {
                                this.numberOfActiveInputStates--;
                                hasActiveStates = (this.numberOfActiveInputStates | this.numberOfActiveOutputStates) != 0;
                            }
                        }

                        if (state.outputObserver.numberOfActiveItems > 0)
                        {
                            state.outputObserver.AdvanceTime(time);
                            if (state.outputObserver.numberOfActiveItems == 0)
                            {
                                this.numberOfActiveOutputStates--;
                                hasActiveStates = (this.numberOfActiveInputStates | this.numberOfActiveOutputStates) != 0;
                            }
                        }
                    }
                    nextTime += this.periodTicks;
                }
            }

            this.lastSyncTime = time;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AddToOutput(long time, ref TKey key, ref TResult payload, int hash)
        {
            int index = this.output.Count++;
            this.output.vsync.col[index] = time;
            this.output.vother.col[index] = time + 1;
            this.output.key.col[index] = key;
            this.output.payload.col[index] = payload;
            this.output.hash.col[index] = hash;

            if (this.output.Count == Config.DataBatchSize) FlushContents();
        }

        [DataContract]
        private sealed class WindowState
        {
            [DataMember]
            public readonly OverlappingPipe<TKey, TSource, TState, TResult> parent;
            [DataMember]
            public readonly SignalWindow<TSource> inputWindow;
            [DataMember]
            public readonly OverlappingOutputObserver outputObserver;

            public WindowState(OverlappingPipe<TKey, TSource, TState, TResult> parent, SignalWindow<TSource> inputWindow, OverlappingOutputObserver outputObserver)
            {
                this.parent = parent;
                this.inputWindow = inputWindow;
                this.outputObserver = outputObserver;
            }
        }

        [DataContract]
        private sealed class OverlappingOutputObserver : ISignalWindowObserver<TResult>
        {
            [DataMember]
            public readonly OverlappingPipe<TKey, TSource, TState, TResult> parent;
            [DataMember]
            public TKey key;
            [DataMember]
            public readonly int hash;
            [DataMember]
            public readonly TState zero;

            [DataMember]
            public readonly TState[] buffer;
            [DataMember]
            public readonly int indexMask;
            [DataMember]
            public int head;
            [DataMember]
            public long headTime = long.MinValue;
            [DataMember]
            public int numberOfActiveItems;

            public OverlappingOutputObserver(OverlappingPipe<TKey, TSource, TState, TResult> parent, ref TKey key, int hash)
            {
                this.parent = parent;
                this.key = key;
                this.hash = hash;
                this.zero = parent.initialState();

                var size = Utility.Power2Ceil(this.parent.outputWindowSize + this.parent.hopSize + 1);
                this.buffer = new TState[size];
                this.indexMask = size - 1;
                this.numberOfActiveItems = 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnInit(long time, BaseWindow<TResult> window)
            {
                // Release all items before the watermark time: watermarkTime = headTime
                AdvanceTime(time);

                var range = window.CurrentRange();
                var index = head;

                for (int i = range.First.Head; i < range.First.Tail; i++)
                {
                    this.buffer[index] = parent.accumulate(this.buffer[index], time, window.Items[i]);
                    index = (index + 1) & this.indexMask;
                }
                for (int i = range.Second.Head; i < range.Second.Tail; i++)
                {
                    this.buffer[index] = parent.accumulate(this.buffer[index], time, window.Items[i]);
                    index = (index + 1) & this.indexMask;
                }
                this.numberOfActiveItems = this.parent.outputWindowSize;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnHop(long time, BaseWindow<TResult> window) => OnInit(time, window);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public object Clone(ISignalWindowObservable<TResult> source) => null;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void AdvanceTime(long time)
            {
                // One array is 'unwindowed' at time T by laying out
                // its items starting from (T - outputWindowSizeTicks).
                // So, all items before this point are safe to release.
                var watermarkTime = time - this.parent.outputWindowSizeTicks;

                if (this.numberOfActiveItems != 0)
                {
                    // We know that (headTime < watermarkTime) and our goal is to ensure (watermarkTime = headTime)
                    while (this.numberOfActiveItems > 0 && this.headTime < watermarkTime)
                    {
                        var result = this.parent.computeResult(this.buffer[this.head]);

                        this.parent.AddToOutput(this.headTime, ref this.key, ref result, this.hash);

                        this.buffer[this.head] = this.zero;
                        this.head = (this.head + 1) & this.indexMask;
                        this.headTime += this.parent.periodTicks;
                        this.numberOfActiveItems--;
                    }
                }

                headTime = watermarkTime;
            }
        }
    }
}