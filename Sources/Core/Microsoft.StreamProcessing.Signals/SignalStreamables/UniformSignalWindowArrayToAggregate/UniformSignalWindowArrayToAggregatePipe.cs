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
    internal sealed class UniformSignalWindowArrayToAggregatePipe<TKey, TSource, TResult> : UnaryPipe<TKey, TSource[], TResult>, ISignalObserver<TResult>, IDisposable
    {
        private readonly MemoryPool<TKey, TResult> pool;
        private readonly string errorMessages;

        [SchemaSerialization]
        private readonly Expression<Func<TKey, TKey, bool>> keyComparerEqualsExpr;
        private readonly Func<TKey, TKey, bool> keyComparerEquals;

        [SchemaSerialization]
        private readonly int windowSize;
        [SchemaSerialization]
        private readonly Func<ISignalWindowObservable<TSource>, ISignalObservable<TResult>> operatorPipeline;
        [SchemaSerialization]
        private readonly bool isUngrouped;
        [SchemaSerialization]
        private readonly BaseWindow<TSource> window;

        [DataMember]
        private FastDictionary3<TKey, ISignalWindowObserver<TSource>> windowObservables;

        [DataMember]
        private StreamMessage<TKey, TResult> output;
        private int row;

        private ISignalWindowObserver<TSource> currentObserver;
        private TKey currentKey;
        private int currentHash;

        [SchemaSerialization]
        private readonly WindowAggregatePipeline<TSource, TResult> windowPipeline;

        public UniformSignalWindowArrayToAggregatePipe() { }

        public UniformSignalWindowArrayToAggregatePipe(
            UniformSignalWindowArrayToAggregateStreamable<TKey, TSource, TResult> stream,
            IStreamObserver<TKey, TResult> observer)
            : base(stream, observer)
        {
            this.pool = MemoryManager.GetMemoryPool<TKey, TResult>(stream.Properties.IsColumnar);
            this.errorMessages = stream.ErrorMessages;

            var comparer = stream.Properties.KeyEqualityComparer;
            this.keyComparerEqualsExpr = comparer.GetEqualsExpr();
            this.keyComparerEquals = keyComparerEqualsExpr.Compile();
            var getHashCode = comparer.GetGetHashCodeExpr().Compile();

            this.windowSize = stream.WindowSize;
            this.operatorPipeline = stream.OperatorPipeline;
            this.isUngrouped = typeof(TKey) == typeof(Empty);
            this.window = new BaseWindow<TSource>();

            this.windowPipeline = new WindowAggregatePipeline<TSource, TResult>(this.operatorPipeline, this.windowSize);

            this.windowObservables = comparer.CreateFastDictionary3Generator<TKey, ISignalWindowObserver<TSource>>(1, keyComparerEquals, getHashCode, stream.Properties.QueryContainer).Invoke();
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => Observer.ProduceQueryPlan(new UniformSignalWindowArrayToAggregatePlanNode<TSource, TResult>(
                previous, this, typeof(TKey), typeof(TSource), typeof(TResult), this.operatorPipeline, false, this.errorMessages));

        public override unsafe void OnNext(StreamMessage<TKey, TSource[]> batch)
        {
            this.pool.Get(out this.output);

            var count = batch.Count;
            this.output.vsync = batch.vsync;
            this.output.vother = batch.vother;
            this.output.key = batch.key;
            this.output.hash = batch.hash;
            this.output.iter = batch.iter;
            this.pool.GetPayload(out output.payload);
            this.output.bitvector = batch.bitvector;

            TKey[] sourceKey = batch.key.col;
            TSource[][] sourcePayload = batch.payload.col;
            fixed (long* sourceVSync = batch.vsync.col)
            fixed (int* sourceHash = batch.hash.col)
            fixed (long* sourceBitVector = batch.bitvector.col)
            {
                int* sourceHashPtr = sourceHash;
                long* sourceVSyncPtr = sourceVSync;

                for (row = 0; row < count; row++)
                {
                    if ((sourceBitVector[row >> 6] & (1L << (row & 0x3f))) == 0)
                    {
                        long startTime = *sourceVSyncPtr;
                        int hash = *sourceHashPtr;

                        if (this.currentObserver == null || ((!this.isUngrouped) && (this.currentHash != hash || !this.keyComparerEquals(this.currentKey, sourceKey[row]))))
                        {
                            this.currentKey = sourceKey[row];
                            this.currentHash = hash;

                            if (!this.windowObservables.Lookup(this.currentKey, this.currentHash, out int index))
                            {
                                // First time group is active for this time
                                this.windowPipeline.CreateNewPipeline(out this.currentObserver, out var lastPipelineObservable);
                                lastPipelineObservable.Subscribe(this);
                                this.windowObservables.Insert(ref index, this.currentKey, this.currentObserver, this.currentHash);
                            }
                            else
                            {
                                // Read new currentState from _filterStates
                                this.currentObserver = this.windowObservables.entries[index].value;
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
            this.window.Dispose();
            this.output.Count = count;
            batch.payload.Return();
            batch.Return();
            Observer.OnNext(this.output);
        }

        protected override void FlushContents() { }

        public override int CurrentlyBufferedOutputCount => this.output.Count;

        public override int CurrentlyBufferedInputCount => this.windowObservables.Count;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(long time, ref TResult result) => this.output.payload.col[row] = result;

        protected override void DisposeState() => this.window.Dispose();
    }
}