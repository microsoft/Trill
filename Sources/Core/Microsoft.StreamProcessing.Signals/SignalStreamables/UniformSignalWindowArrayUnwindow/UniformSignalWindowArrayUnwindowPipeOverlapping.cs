// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Aggregates;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing.Signal.UDO
{
    [DataContract]
    internal sealed class UniformSignalWindowArrayUnwindowPipeOverlapping<TKey, TSource, TState, TResult> : UnaryPipe<TKey, TSource[], TResult>, IDisposable
    {
        private readonly MemoryPool<TKey, TResult> pool;
        private readonly string errorMessages;

        [SchemaSerialization]
        private readonly Expression<Func<TKey, TKey, bool>> keyComparerEqualsExpr;
        private readonly Func<TKey, TKey, bool> keyComparerEquals;

        [SchemaSerialization]
        private readonly long sourcePeriodTicks;
        [SchemaSerialization]
        private readonly int windowSize;
        [SchemaSerialization]
        private readonly int outputWindowSize;
        [SchemaSerialization]
        private readonly long outputWindowSizeTicks;

        [SchemaSerialization]
        private readonly int hopSize;
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

        [DataMember]
        private StreamMessage<TKey, TResult> output;

        [DataMember]
        private FastDictionary3<TKey, WindowState> windowStates;
        [DataMember]
        private int numberOfActiveOutputStates;
        [DataMember]
        private long lastSyncTime = long.MinValue;
        [DataMember]
        private int batchIter;
        [SchemaSerialization]
        private readonly BaseWindow<TSource> window;

        [SchemaSerialization]
        private readonly WindowPipeline<TSource, TResult> windowPipeline;
        private readonly bool windowPipelineIsEmpty;

        private WindowState currentState;
        private TKey currentKey;
        private int currentHash;

        public UniformSignalWindowArrayUnwindowPipeOverlapping() { }

        public UniformSignalWindowArrayUnwindowPipeOverlapping(UniformSignalWindowArrayUnwindowStreamable<TKey, TSource, TState, TResult> stream, IStreamObserver<TKey, TResult> observer)
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

            this.sourcePeriodTicks = stream.sourcePeriodTicks;
            this.hopSize = stream.hopSize;
            this.windowSize = stream.windowSize;
            this.outputWindowSize = stream.outputWindowSize;
            this.outputWindowSizeTicks = outputWindowSize * sourcePeriodTicks;
            this.operatorPipeline = stream.operatorPipeline;

            this.isUngrouped = typeof(TKey) == typeof(Empty);

            this.aggregate = stream.aggregate;
            this.initialStateExpr = this.aggregate.InitialState();
            this.initialState = this.initialStateExpr.Compile();
            this.accumulateExpr = this.aggregate.Accumulate();
            this.accumulate = this.accumulateExpr.Compile();
            this.computeResultExpr = this.aggregate.ComputeResult();
            this.computeResult = this.computeResultExpr.Compile();

            this.window = new BaseWindow<TSource>();
            this.windowStates = comparer.CreateFastDictionary3Generator<TKey, WindowState>(1, this.keyComparerEquals, getHashCode, stream.Properties.QueryContainer).Invoke();
            this.numberOfActiveOutputStates = 0;

            this.windowPipeline = new WindowPipeline<TSource, TResult>(this.operatorPipeline, this.windowSize);
            this.windowPipelineIsEmpty = this.windowPipeline.IsEmpty;
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => Observer.ProduceQueryPlan(new UniformSignalWindowArrayUnwindowPlanNode<TSource, TResult>(
                previous, this, typeof(TKey), typeof(TSource), typeof(TResult), this.operatorPipeline, false, this.errorMessages));

        public override unsafe void OnNext(StreamMessage<TKey, TSource[]> batch)
        {
            this.batchIter = batch.iter;
            TKey[] sourceKey = batch.key.col;
            TSource[][] sourcePayload = batch.payload.col;
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
                                var resultObserver = new OverlappingArrayOutputObserver(this, ref this.currentKey, this.currentHash);

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

                                this.currentState = new WindowState(this, firstPipelineObserver, resultObserver);
                                this.windowStates.Insert(ref index, this.currentKey, this.currentState, this.currentHash);
                            }
                            else
                            {
                                // Read new currentState from _filterStates
                                this.currentState = this.windowStates.entries[index].value;
                            }
                        }

                        this.window.SetItems(sourcePayload[row], this.windowSize);

                        int before = this.currentState.outputObserver.numberOfActiveItems == 0 ? 0 : 1;
                        this.currentState.inputObserver.OnInit(startTime, this.window);
                        int after = this.currentState.outputObserver.numberOfActiveItems == 0 ? 0 : 1;
                        this.numberOfActiveOutputStates += after - before;
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
            if (output.Count == 0) return;
            this.output.iter = batchIter;
            this.output.Seal();
            this.Observer.OnNext(this.output);
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        public override int CurrentlyBufferedOutputCount => this.output.Count;

        public override int CurrentlyBufferedInputCount => this.windowStates.Count;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AdvanceTime(long time)
        {
            if (time == this.lastSyncTime) return;

            if (this.numberOfActiveOutputStates != 0)
            {
                if (this.isUngrouped)
                {
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

                var nextTime = this.lastSyncTime + this.sourcePeriodTicks;
                while (this.numberOfActiveOutputStates > 0 && time >= nextTime)
                {
                    // Iterate over all active windows and advance time
                    int iter1 = FastDictionary3<TKey, WindowState>.IteratorStart;
                    while (this.numberOfActiveOutputStates > 0 && windowStates.Iterate(ref iter1))
                    {
                        var state = this.windowStates.entries[iter1].value;

                        if (state.outputObserver.numberOfActiveItems > 0)
                        {
                            state.outputObserver.AdvanceTime(time);
                            if (state.outputObserver.numberOfActiveItems == 0)
                            {
                                this.numberOfActiveOutputStates--;
                            }
                        }
                    }
                    nextTime += this.sourcePeriodTicks;
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
            public readonly UniformSignalWindowArrayUnwindowPipeOverlapping<TKey, TSource, TState, TResult> parent;
            [DataMember]
            public readonly ISignalWindowObserver<TSource> inputObserver;
            [DataMember]
            public readonly OverlappingArrayOutputObserver outputObserver;

            public WindowState(UniformSignalWindowArrayUnwindowPipeOverlapping<TKey, TSource, TState, TResult> parent, ISignalWindowObserver<TSource> inputObserver, OverlappingArrayOutputObserver outputObserver)
            {
                this.parent = parent;
                this.inputObserver = inputObserver;
                this.outputObserver = outputObserver;
            }
        }

        [DataContract]
        private sealed class OverlappingArrayOutputObserver : ISignalWindowObserver<TResult>
        {
            [DataMember]
            public readonly UniformSignalWindowArrayUnwindowPipeOverlapping<TKey, TSource, TState, TResult> parent;
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

            public OverlappingArrayOutputObserver(UniformSignalWindowArrayUnwindowPipeOverlapping<TKey, TSource, TState, TResult> parent, ref TKey key, int hash)
            {
                this.parent = parent;
                this.key = key;
                this.hash = hash;
                this.zero = parent.initialState();

                var size = Utility.Power2Ceil(parent.outputWindowSize + parent.hopSize + 1);
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
                    this.buffer[index] = this.parent.accumulate(this.buffer[index], time, window.Items[i]);
                    index = (index + 1) & this.indexMask;
                }
                for (int i = range.Second.Head; i < range.Second.Tail; i++)
                {
                    buffer[index] = this.parent.accumulate(buffer[index], time, window.Items[i]);
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

                if (this.numberOfActiveItems == 0)
                {
                    // No active items, so just advance time
                    this.headTime = watermarkTime;
                    return;
                }

                // We know that (headTime < watermarkTime) and our goal is to ensure (watermarkTime = headTime)
                while (this.numberOfActiveItems > 0 && this.headTime < watermarkTime)
                {
                    var result = this.parent.computeResult(this.buffer[this.head]);

                    this.parent.AddToOutput(this.headTime, ref this.key, ref result, this.hash);

                    this.buffer[this.head] = zero;
                    this.head = (this.head + 1) & indexMask;
                    this.headTime += this.parent.sourcePeriodTicks;
                    this.numberOfActiveItems--;
                }

                this.headTime = watermarkTime;
            }
        }

        protected override void DisposeState() => this.window.Dispose();
    }
}