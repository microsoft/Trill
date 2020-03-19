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
    internal sealed class UngroupedNonOverlappingPipe<TKey, TSource, TState, TResult> : UnaryPipe<TKey, TSource, TResult>, ISignalWindowObserver<TResult>
    {
        private readonly MemoryPool<TKey, TResult> pool;
        private readonly string errorMessages;

        [SchemaSerialization]
        private readonly long periodTicks;
        [SchemaSerialization]
        private readonly long offsetTicks;
        [SchemaSerialization]
        private readonly int windowSize;
        [SchemaSerialization]
        private readonly int hopSize;
        [SchemaSerialization]
        private readonly bool setMissingDataToNull;

        [SchemaSerialization]
        private readonly Func<ISignalWindowObservable<TSource>, ISignalWindowObservable<TResult>> operatorPipeline;

        [DataMember]
        private StreamMessage<TKey, TResult> output;

        [DataMember]
        private long lastSyncTime = long.MinValue;
        [DataMember]
        private int batchIter;
        [DataMember]
        private int numberOfActiveStates;

        [SchemaSerialization]
        private readonly WindowPipeline<TSource, TResult> windowPipeline;
        private readonly bool windowPipelineIsEmpty;

        private SignalWindow<TSource> currentWindow;
        private TKey currentKey;
        private int currentHash;

        public UngroupedNonOverlappingPipe() { }

        public UngroupedNonOverlappingPipe(UniformSignalWindowUnwindowStreamable<TKey, TSource, TState, TResult> stream, IStreamObserver<TKey, TResult> observer)
            : base(stream, observer)
        {
            this.pool = MemoryManager.GetMemoryPool<TKey, TResult>(stream.Properties.IsColumnar);
            this.pool.Get(out this.output);
            this.output.Allocate();
            this.errorMessages = stream.ErrorMessages;

            this.periodTicks = stream.PeriodTicks;
            this.offsetTicks = stream.OffsetTicks;
            this.windowSize = stream.WindowSize;
            this.hopSize = stream.HopSize;
            this.setMissingDataToNull = stream.SetMissingDataToNull;
            this.operatorPipeline = stream.OperatorPipeline;
            this.numberOfActiveStates = 0;

            this.windowPipeline = new WindowPipeline<TSource, TResult>(this.operatorPipeline, this.windowSize);
            this.windowPipelineIsEmpty = this.windowPipeline.IsEmpty;
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

                        if (this.currentWindow == null)
                        {
                            this.currentKey = sourceKey[row];
                            this.currentHash = hash;

                            // First time group is active for this time.
                            // Create window pipeline and wire together observers and observables.
                            var resultObserver = this as ISignalWindowObserver<TResult>;

                            ISignalWindowObserver<TSource> firstPipelineObserver;
                            if (this.windowPipelineIsEmpty)
                            {
                                // When the pipeline is empty (the pipeline lambda is identity),
                                // there is no transformation and we can send directly to resultObserver
                                firstPipelineObserver = (ISignalWindowObserver<TSource>)resultObserver;
                            }
                            else
                            {
                                this.windowPipeline.CreateNewPipeline(out firstPipelineObserver, out var lastPipelineObservable);
                                lastPipelineObservable.Subscribe(resultObserver);
                            }

                            this.currentWindow = SignalWindow<TSource>.GetInstance(
                                firstPipelineObserver,
                                this.periodTicks,
                                this.offsetTicks,
                                this.windowSize,
                                this.hopSize,
                                this.setMissingDataToNull);
                        }

                        // Enqueue items may increase or decrease the number of active items
                        int before = this.currentWindow.numberOfActiveItems == 0 ? 0 : 1;
                        this.currentWindow.Enqueue(startTime, ref sourcePayload[row]);
                        int after = this.currentWindow.numberOfActiveItems == 0 ? 0 : 1;
                        this.numberOfActiveStates += after - before;
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

            if (this.numberOfActiveStates != 0)
            {
                if (this.currentWindow.numberOfActiveItems > 0)
                {
                    this.currentWindow.AdvanceTime(time);
                    if (this.currentWindow.numberOfActiveItems == 0) this.numberOfActiveStates--;
                }
            }

            this.lastSyncTime = time;
            return;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnInit(long time, BaseWindow<TResult> window)
        {
            var range = window.CurrentRange();
            long sampleTime = time - range.Length * this.periodTicks;
            AddToOutput(sampleTime, window.Items, range.First.Head, range.First.Length);
            AddToOutput(sampleTime, window.Items, range.Second.Head, range.Second.Length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnHop(long time, BaseWindow<TResult> window) => OnInit(time, window);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public object Clone(ISignalWindowObservable<TResult> source) => null;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe void AddToOutput(long time, TResult[] items, int head, int length)
        {
            // Bulk copy from input to output, avoid branching inside loop
            int availableSize = Config.DataBatchSize - this.output.Count;        // always greater than zero
            int smallerSize = availableSize < length ? availableSize : length;
            var smallerTail = head + smallerSize;
            var tail = head + length;

            var castedKey = (Empty)(object)this.currentKey;
            TResult[] outputPayload = this.output.payload.col;
            fixed (long* outputVSync = this.output.vsync.col)
            fixed (long* outputVOther = this.output.vother.col)
            fixed (int* outputHash = this.output.hash.col)
            fixed (Empty* outputKey = (Empty[])(object)this.output.key.col)
            {
                Empty* outputKeyPtr = outputKey + this.output.Count;
                long* outputVSyncPtr = outputVSync + this.output.Count;
                long* outputVOtherPtr = outputVOther + this.output.Count;
                int* outputHashPtr = outputHash + this.output.Count;

                Array.Copy(items, head, outputPayload, this.output.Count, smallerSize);

                for (; head < smallerTail; head++)
                {
                    this.output.Count++;
                    *outputKeyPtr = castedKey;                  // This only works for ungrouped signals
                    *outputVSyncPtr = time;
                    *outputVOtherPtr = time + 1;
                    *outputHashPtr = this.currentHash;               // This only works for ungrouped signals

                    // Advance pointers
                    outputKeyPtr++;
                    outputVSyncPtr++;
                    outputVOtherPtr++;
                    outputHashPtr++;

                    time += this.periodTicks;
                }
            }

            if (this.output.Count == Config.DataBatchSize) FlushContents();
            if (head == tail) return;

            outputPayload = this.output.payload.col;
            fixed (long* outputVSync = this.output.vsync.col)
            fixed (long* outputVOther = this.output.vother.col)
            fixed (int* outputHash = this.output.hash.col)
            fixed (Empty* outputKey = (Empty[])(object)this.output.key.col)
            {
                Empty* outputKeyPtr = outputKey;
                long* outputVSyncPtr = outputVSync;
                long* outputVOtherPtr = outputVOther;
                int* outputHashPtr = outputHash;

                Array.Copy(items, head, outputPayload, 0, tail - head);

                for (; head < tail; head++)
                {
                    output.Count++;
                    *outputKeyPtr = castedKey;                  // This only works for ungrouped signals
                    *outputVSyncPtr = time;
                    *outputVOtherPtr = time + 1;
                    *outputHashPtr = this.currentHash;               // This only works for ungrouped signals

                    // Advance pointers
                    outputKeyPtr++;
                    outputVSyncPtr++;
                    outputVOtherPtr++;
                    outputHashPtr++;

                    time += this.periodTicks;
                }
            }
        }

        public override int CurrentlyBufferedOutputCount => this.output.Count;

        public override int CurrentlyBufferedInputCount => this.numberOfActiveStates;
    }
}