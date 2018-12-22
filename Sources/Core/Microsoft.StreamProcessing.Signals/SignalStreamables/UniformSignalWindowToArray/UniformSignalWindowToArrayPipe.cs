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

namespace Microsoft.StreamProcessing.Signal.UDO
{
    [DataContract]
    internal sealed class UniformSignalWindowToArrayPipe<TKey, TSource, TResult> : UnaryPipe<TKey, TSource, TResult[]>
    {
        private readonly MemoryPool<TKey, TResult[]> pool;
        private readonly string errorMessages;

        [SchemaSerialization]
        private readonly Expression<Func<TKey, TKey, bool>> keyComparerEqualsExpr;
        private readonly Func<TKey, TKey, bool> keyComparerEquals;

        [SchemaSerialization]
        private readonly long periodTicks;
        [SchemaSerialization]
        private readonly long offsetTicks;
        [SchemaSerialization]
        private readonly int windowSize;
        [SchemaSerialization]
        private readonly int hopSize;

        [SchemaSerialization]
        private readonly Func<ISignalWindowObservable<TSource>, ISignalWindowObservable<TResult>> operatorPipeline;
        [SchemaSerialization]
        private readonly bool setMissingDataToNull;
        [SchemaSerialization]
        private readonly bool isUngrouped;

        [DataMember]
        private StreamMessage<TKey, TResult[]> output;

        [DataMember]
        private FastDictionary3<TKey, WindowState> windowStates;
        [DataMember]
        private int numberOfActiveStates;
        [DataMember]
        private long lastSyncTime = long.MinValue;
        [DataMember]
        private int batchIter;

        private WindowState currentState;
        private TKey currentKey;
        private int currentHash;

        [SchemaSerialization]
        private readonly WindowPipeline<TSource, TResult> windowPipeline;
        private readonly bool windowPipelineIsEmpty;

        public UniformSignalWindowToArrayPipe() { }

        public UniformSignalWindowToArrayPipe(UniformSignalWindowToArrayStreamable<TKey, TSource, TResult> stream, IStreamObserver<TKey, TResult[]> observer)
            : base(stream, observer)
        {
            this.pool = MemoryManager.GetMemoryPool<TKey, TResult[]>(stream.Properties.IsColumnar);
            this.pool.Get(out output);
            this.output.Allocate();
            this.errorMessages = stream.ErrorMessages;

            var comparer = stream.Properties.KeyEqualityComparer;
            this.keyComparerEqualsExpr = comparer.GetEqualsExpr();
            this.keyComparerEquals = this.keyComparerEqualsExpr.Compile();
            var getHashCode = comparer.GetGetHashCodeExpr().Compile();

            this.periodTicks = stream.PeriodTicks;
            this.offsetTicks = stream.OffsetTicks;
            this.windowSize = stream.WindowSize;
            this.hopSize = stream.HopSize;
            this.setMissingDataToNull = stream.SetMissingDataToNull;
            this.operatorPipeline = stream.OperatorPipeline;

            this.isUngrouped = typeof(TKey) == typeof(Empty);

            this.windowStates = comparer.CreateFastDictionary3Generator<TKey, WindowState>(1, keyComparerEquals, getHashCode, stream.Properties.QueryContainer).Invoke();
            this.numberOfActiveStates = 0;

            this.windowPipeline = new WindowPipeline<TSource, TResult>(operatorPipeline, windowSize);
            this.windowPipelineIsEmpty = windowPipeline.IsEmpty;
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

                        if (this.currentState == null || ((!this.isUngrouped) && (this.currentHash != hash || !keyComparerEquals(this.currentKey, sourceKey[row]))))
                        {
                            this.currentKey = sourceKey[row];
                            this.currentHash = hash;

                            if (!this.windowStates.Lookup(this.currentKey, this.currentHash, out int index))
                            {
                                // First time group is active for this time.

                                // Create window pipeline and wire together observers and observables.
                                this.windowPipeline.CreateNewPipeline(out var firstPipelineObserver, out var lastPipelineObservable);

                                this.currentState = new WindowState(this, ref this.currentKey, this.currentHash);
                                this.currentState.Subscribe(firstPipelineObserver);
                                lastPipelineObservable.Subscribe(this.currentState);
                                this.windowStates.Insert(ref index, this.currentKey, this.currentState, this.currentHash);
                            }
                            else
                            {
                                // Read new currentState from _filterStates
                                this.currentState = this.windowStates.entries[index].value;
                            }
                        }

                        // Enqueue items may increase or decrease the number of active items
                        int before = this.currentState.window.numberOfActiveItems == 0 ? 0 : 1;
                        this.currentState.window.Enqueue(startTime, ref sourcePayload[row]);
                        int after = this.currentState.window.numberOfActiveItems == 0 ? 0 : 1;
                        this.numberOfActiveStates += after - before;
                    }
                    else if (*sourceVOther == StreamEvent.PunctuationOtherTime) AdvanceTime(*sourceVSyncPtr);

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
            this.output.iter = batchIter;
            this.output.Seal();
            this.Observer.OnNext(this.output);
            this.pool.Get(out output);
            this.output.Allocate();
        }

        protected override void DisposeState() => this.output.Free();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AdvanceTime(long time)
        {
            if (time == this.lastSyncTime) return;

            if (this.numberOfActiveStates != 0)
            {
                if (this.isUngrouped)
                {
                    if (this.currentState.window.numberOfActiveItems > 0)
                    {
                        this.currentState.window.AdvanceTime(time);
                        if (this.currentState.window.numberOfActiveItems == 0)
                        {
                            this.numberOfActiveStates--;
                        }
                    }
                    this.lastSyncTime = time;
                    return;
                }

                var nextTime = this.lastSyncTime + this.periodTicks;
                while (this.numberOfActiveStates > 0 && time >= nextTime)
                {
                    // Iterate over all active windows and advance time
                    int iter1 = FastDictionary3<TKey, WindowState>.IteratorStart;
                    while (this.numberOfActiveStates > 0 && this.windowStates.Iterate(ref iter1))
                    {
                        var state = this.windowStates.entries[iter1].value;
                        if (state.window.numberOfActiveItems > 0)
                        {
                            state.window.AdvanceTime(nextTime);
                            if (state.window.numberOfActiveItems == 0)
                            {
                                this.numberOfActiveStates--;
                            }
                        }
                    }
                    nextTime += periodTicks;
                }
            }

            this.lastSyncTime = time;
        }

        public override int CurrentlyBufferedOutputCount => this.output.Count;

        public override int CurrentlyBufferedInputCount => this.windowStates.Count;

        [DataContract]
        private sealed class WindowState : ISignalWindowObserver<TResult>, ISignalWindowObservable<TSource>
        {
            [DataMember]
            public readonly UniformSignalWindowToArrayPipe<TKey, TSource, TResult> parent;
            [DataMember]
            public readonly TKey key;
            [DataMember]
            public readonly int hash;
            [DataMember]
            public SignalWindow<TSource> window;

            public WindowState(UniformSignalWindowToArrayPipe<TKey, TSource, TResult> parent, ref TKey key, int hash)
            {
                this.parent = parent;
                this.key = key;
                this.hash = hash;
            }

            public int WindowSize
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => this.parent.windowSize;
            }

            public void Subscribe(ISignalWindowObserver<TSource> observer)
                => this.window =
                    SignalWindow<TSource>.GetInstance(
                        observer,
                        this.parent.periodTicks,
                        this.parent.offsetTicks,
                        this.parent.windowSize,
                        this.parent.hopSize,
                        this.parent.setMissingDataToNull);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnInit(long time, BaseWindow<TResult> window)
            {
                var array = new TResult[window.Capacity];

                var range = window.CurrentRange();
                var firstLength = range.First.Length;
                var secondLength = range.Second.Length;

                Array.Copy(window.Items, range.First.Head, array, 0, firstLength);
                Array.Copy(window.Items, range.Second.Head, array, firstLength, secondLength);

                int index = this.parent.output.Count++;
                this.parent.output.vsync.col[index] = time;
                this.parent.output.vother.col[index] = time + 1;
                this.parent.output.key.col[index] = key;
                this.parent.output.payload.col[index] = array;
                this.parent.output.hash.col[index] = hash;

                if (parent.output.Count == Config.DataBatchSize) this.parent.FlushContents();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnHop(long time, BaseWindow<TResult> window) => OnInit(time, window);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public object Clone(ISignalWindowObservable<TResult> source) => null;
        }
    }
}