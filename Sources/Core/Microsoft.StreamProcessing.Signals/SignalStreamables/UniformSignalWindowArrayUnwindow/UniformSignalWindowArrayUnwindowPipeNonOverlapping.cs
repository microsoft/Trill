// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing.Signal.UDO
{
    [DataContract]
    internal sealed class UniformSignalWindowArrayUnwindowPipeNonOverlapping<TKey, TSource, TState, TResult> : UnaryPipe<TKey, TSource[], TResult>, ISignalWindowObserver<TResult>, IDisposable
    {
        private readonly MemoryPool<TKey, TResult> pool;
        private readonly string errorMessages;

        [SchemaSerialization]
        private readonly long sourcePeriodTicks;
        [SchemaSerialization]
        private readonly int windowSize;
        [SchemaSerialization]
        private readonly Func<ISignalWindowObservable<TSource>, ISignalWindowObservable<TResult>> operatorPipeline;
        [SchemaSerialization]
        private readonly BaseWindow<TSource> window;

        [SchemaSerialization]
        private readonly WindowPipeline<TSource, TResult> windowPipeline;
        private readonly bool windowPipelineIsEmpty;

        [DataMember]
        private StreamMessage<TKey, TResult> output;
        [DataMember]
        private int batchIter;

        private ISignalWindowObserver<TSource> currentObserver;
        private TKey currentKey;
        private int currentHash;

        public UniformSignalWindowArrayUnwindowPipeNonOverlapping() { }

        public UniformSignalWindowArrayUnwindowPipeNonOverlapping(UniformSignalWindowArrayUnwindowStreamable<TKey, TSource, TState, TResult> stream, IStreamObserver<TKey, TResult> observer)
            : base(stream, observer)
        {
            this.pool = MemoryManager.GetMemoryPool<TKey, TResult>(stream.Properties.IsColumnar);
            this.pool.Get(out this.output);
            this.output.Allocate();
            this.errorMessages = stream.ErrorMessages;

            this.sourcePeriodTicks = stream.sourcePeriodTicks;
            this.windowSize = stream.windowSize;
            this.operatorPipeline = stream.operatorPipeline;
            this.window = new BaseWindow<TSource>();

            this.windowPipeline = new WindowPipeline<TSource, TResult>(this.operatorPipeline, this.windowSize);
            this.windowPipelineIsEmpty = this.windowPipeline.IsEmpty;
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => Observer.ProduceQueryPlan(new UniformSignalWindowToArrayPlanNode<TSource, TResult>(
                previous, this, typeof(TKey), typeof(TSource), typeof(TResult), this.operatorPipeline, false, this.errorMessages));

        public override unsafe void OnNext(StreamMessage<TKey, TSource[]> batch)
        {
            this.batchIter = batch.iter;
            TKey[] sourceKey = batch.key.col;
            TSource[][] sourcePayload = batch.payload.col;
            fixed (long* sourceVSync = batch.vsync.col)
            fixed (int* sourceHash = batch.hash.col)
            fixed (long* sourceBitVector = batch.bitvector.col)
            {
                int count = batch.Count;
                int* sourceHashPtr = sourceHash;
                long* sourceVSyncPtr = sourceVSync;

                for (int row = 0; row < count; row++)
                {
                    if ((sourceBitVector[row >> 6] & (1L << (row & 0x3f))) == 0)
                    {
                        long startTime = *sourceVSyncPtr;
                        int hash = *sourceHashPtr;

                        if (this.currentObserver == null)
                        {
                            this.currentKey = sourceKey[row];
                            this.currentHash = hash;

                            // First time group is active for this time
                            var resultObserver = this as ISignalWindowObserver<TResult>;

                            if (this.windowPipelineIsEmpty)
                            {
                                // When the pipeline is empty (the pipeline lambda is identity),
                                // there is no transformation and we can send directly to resultObserver
                                this.currentObserver = (ISignalWindowObserver<TSource>)resultObserver;
                            }
                            else
                            {
                                this.windowPipeline.CreateNewPipeline(out this.currentObserver, out var lastPipelineObservable);
                                lastPipelineObservable.Subscribe(resultObserver);
                            }
                        }

                        this.window.SetItems(sourcePayload[row], this.windowSize);
                        this.currentObserver.OnInit(startTime, this.window);
                    }

                    // Advance pointers.
                    sourceHashPtr++;
                    sourceVSyncPtr++;
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

        public override int CurrentlyBufferedOutputCount => this.output.Count;

        public override int CurrentlyBufferedInputCount => 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnInit(long time, BaseWindow<TResult> window)
        {
            var range = window.CurrentRange();
            long sampleTime = time - range.Length * this.sourcePeriodTicks;
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

                    time += this.sourcePeriodTicks;
                }
            }

            if (this.output.Count == Config.DataBatchSize)
            {
                FlushContents();

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
                        this.output.Count++;
                        *outputKeyPtr = castedKey;                  // This only works for ungrouped signals
                        *outputVSyncPtr = time;
                        *outputVOtherPtr = time + 1;
                        *outputHashPtr = this.currentHash;          // This only works for ungrouped signals

                        // Advance pointers
                        outputKeyPtr++;
                        outputVSyncPtr++;
                        outputVOtherPtr++;
                        outputHashPtr++;

                        time += this.sourcePeriodTicks;
                    }
                }
            }
        }

        protected override void DisposeState() => this.window.Dispose();
    }
}