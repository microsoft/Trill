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
using Microsoft.StreamProcessing.Signal.UDO;

namespace Microsoft.StreamProcessing.Signal
{
    [DataContract]
    internal sealed class DigitalFilterPipe<TKey, TPayload> : UnaryPipe<TKey, TPayload, TPayload>, IDisposable
    {
        private readonly MemoryPool<TKey, TPayload> pool;
        private readonly string errorMessages;

        [SchemaSerialization]
        private readonly Expression<Func<TKey, TKey, bool>> keyComparerEqualsExpr;
        private readonly Func<TKey, TKey, bool> keyComparerEquals;

        [SchemaSerialization]
        private readonly long period;
        [SchemaSerialization]
        private readonly long offset;

        [SchemaSerialization]
        private readonly IDigitalFilter<TPayload> filter;
        [SchemaSerialization]
        private readonly Expression<Func<BaseWindow<TPayload>, BaseWindow<TPayload>, TPayload>> computeExpr;
        private readonly Func<BaseWindow<TPayload>, BaseWindow<TPayload>, TPayload> computeFunc;
        [SchemaSerialization]
        private readonly bool setMissingDataToNull;
        [SchemaSerialization]
        private readonly int feedForwardSize;
        [SchemaSerialization]
        private readonly int feedBackwardSize;

        [SchemaSerialization]
        private readonly bool isUngrouped;

        [DataMember]
        private StreamMessage<TKey, TPayload> output;

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
        private bool currentStateExists;

        public DigitalFilterPipe() { }

        public DigitalFilterPipe(DigitalFilterStreamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer)
            : base(stream, observer)
        {
            this.pool = MemoryManager.GetMemoryPool<TKey, TPayload>(stream.Properties.IsColumnar);
            this.pool.Get(out this.output);
            this.output.Allocate();
            this.errorMessages = stream.ErrorMessages;

            var comparer = stream.Properties.KeyEqualityComparer;
            this.keyComparerEqualsExpr = comparer.GetEqualsExpr();
            this.keyComparerEquals = this.keyComparerEqualsExpr.Compile();
            var getHashCode = comparer.GetGetHashCodeExpr().Compile();

            this.period = stream.period;
            this.offset = stream.offset;

            this.filter = stream.filter;
            this.computeExpr = this.filter.Compute();
            this.computeFunc = computeExpr.Compile();
            this.setMissingDataToNull = this.filter.SetMissingDataToNull().Compile().Invoke();
            this.feedForwardSize = this.filter.GetFeedForwardSize().Compile().Invoke();
            this.feedBackwardSize = this.filter.GetFeedBackwardSize().Compile().Invoke();

            this.isUngrouped = typeof(TKey) == typeof(Empty);
            this.windowStates = comparer.CreateFastDictionary3Generator<TKey, WindowState>(1, this.keyComparerEquals, getHashCode, stream.Properties.QueryContainer).Invoke();
            this.numberOfActiveStates = 0;
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => Observer.ProduceQueryPlan(new DigitalFilterPlanNode<TPayload>(
                previous, this, typeof(TKey), typeof(TPayload), this.filter, false, this.errorMessages));

        public override unsafe void OnNext(StreamMessage<TKey, TPayload> batch)
        {
            this.batchIter = batch.iter;
            TKey[] sourceKey = batch.key.col;
            TPayload[] sourcePayload = batch.payload.col;
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

                        if (!this.currentStateExists || ((!this.isUngrouped) && (this.currentHash != hash || !this.keyComparerEquals(this.currentKey, sourceKey[row]))))
                        {
                            this.currentKey = sourceKey[row];
                            this.currentHash = hash;

                            if (!this.windowStates.Lookup(this.currentKey, this.currentHash, out int index))
                            {
                                // First time group is active for this time
                                this.currentState = new WindowState(this, ref this.currentKey, this.currentHash);
                                this.windowStates.Insert(ref index, this.currentKey, this.currentState, this.currentHash);
                            }
                            else
                            {
                                // Read new currentState from _filterStates
                                this.currentState = this.windowStates.entries[index].value;
                            }
                            this.currentStateExists = true;
                        }

                        // Enqueue items may increase or decrease the number of active items
                        int before = this.currentState.feedForwardWindow.numberOfActiveItems == 0 ? 0 : 1;
                        this.currentState.feedForwardWindow.Enqueue(startTime, ref sourcePayload[row]);
                        int after = this.currentState.feedForwardWindow.numberOfActiveItems > 0 ? 1 : 0;
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

            if (this.numberOfActiveStates != 0)
            {
                if (this.isUngrouped)
                {
                    if (this.currentState.feedForwardWindow.numberOfActiveItems > 0)
                    {
                        this.currentState.feedForwardWindow.AdvanceTime(time);
                        if (this.currentState.feedForwardWindow.numberOfActiveItems == 0) this.numberOfActiveStates--;
                    }
                    this.lastSyncTime = time;
                    return;
                }

                var nextTime = this.lastSyncTime + this.period;
                while (this.numberOfActiveStates > 0 && time >= nextTime)
                {
                    // Iterate over all active windows and advance time
                    int iter1 = FastDictionary3<TKey, WindowState>.IteratorStart;
                    while (this.numberOfActiveStates > 0 && this.windowStates.Iterate(ref iter1))
                    {
                        var state = this.windowStates.entries[iter1].value;

                        if (state.feedForwardWindow.numberOfActiveItems > 0)
                        {
                            state.feedForwardWindow.AdvanceTime(nextTime);
                            if (state.feedForwardWindow.numberOfActiveItems == 0) this.numberOfActiveStates--;
                        }
                    }
                    nextTime += period;
                }
            }

            this.lastSyncTime = time;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AddToOutput(long start, long end, ref TKey key, ref TPayload payload, int hash)
        {
            int index = this.output.Count++;
            this.output.vsync.col[index] = start;
            this.output.vother.col[index] = end;
            this.output.key.col[index] = key;
            this.output.payload.col[index] = payload;
            this.output.hash.col[index] = hash;

            if (this.output.Count == Config.DataBatchSize) FlushContents();
        }

        [DataContract]
        internal sealed class WindowState : ISignalWindowObserver<TPayload>, IDisposable
        {
            [DataMember]
            public DigitalFilterPipe<TKey, TPayload> parent;
            [DataMember]
            public TKey key;
            [DataMember]
            public int hash;
            [DataMember]
            public SignalWindow<TPayload> feedForwardWindow;
            [DataMember]
            public BaseWindow<TPayload> feedBackwardWindow;

            public WindowState(DigitalFilterPipe<TKey, TPayload> parent, ref TKey key, int hash)
            {
                this.parent = parent;
                this.key = key;
                this.hash = hash;
                this.feedForwardWindow = parent.setMissingDataToNull ?
                    (SignalWindow<TPayload>)new HoppingSignalWindow<TPayload>(this, parent.period, parent.offset, parent.feedForwardSize, 1) :
                    new PaddedHoppingSignalWindow<TPayload>(this, parent.period, parent.offset, parent.feedForwardSize, 1);
                this.feedBackwardWindow = new BaseWindow<TPayload>(parent.feedBackwardSize, 1);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnInit(long time, BaseWindow<TPayload> window)
            {
                this.feedBackwardWindow.Clear();
                TPayload result = this.parent.computeFunc(this.feedForwardWindow, this.feedBackwardWindow);
                this.feedBackwardWindow.Enqueue(ref result);
                this.parent.AddToOutput(time, time + 1, ref key, ref result, hash);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnHop(long time, BaseWindow<TPayload> window)
            {
                TPayload result = this.parent.computeFunc(this.feedForwardWindow, this.feedBackwardWindow);
                this.feedBackwardWindow.Enqueue(ref result);
                this.parent.AddToOutput(time, time + 1, ref key, ref result, hash);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public object Clone(ISignalWindowObservable<TPayload> source) => null;

            public void Dispose()
            {
                this.feedForwardWindow.Dispose();
                this.feedBackwardWindow.Dispose();
            }
        }

        protected override void DisposeState()
        {
            this.output.Free();
            var iter = FastDictionary3<TKey, WindowState>.IteratorStart;
            while (this.windowStates.Iterate(ref iter)) this.windowStates.entries[iter].value.Dispose();
            this.currentState.Dispose();
        }
    }
}
