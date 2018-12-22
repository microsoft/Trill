// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing.Signal
{
    [DataContract]
    internal sealed class FilterIIRPipe<TKey, TPayload> : UnaryPipe<TKey, TPayload, TPayload>
    {
        private readonly MemoryPool<TKey, TPayload> pool;
        private readonly string errorMessages;

        [SchemaSerialization]
        private readonly Expression<Func<TKey, TKey, bool>> keyComparerEqualsExpr;
        private readonly Func<TKey, TKey, bool> keyComparerEquals;

        [SchemaSerialization]
        private readonly long period;

        [SchemaSerialization]
        private readonly double factor;
        private readonly double antifactor;

        [SchemaSerialization]
        private readonly bool isUngrouped;

        [DataMember]
        private FastDictionary<TKey, FilterState> filterStates;

        private FilterState currentState;
        private TKey currentKey;
        private int currentHash;

        public FilterIIRPipe() { }

        public FilterIIRPipe(FilterIIRStreamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer)
            : base(stream, observer)
        {
            var comparer = stream.Properties.KeyEqualityComparer;
            this.keyComparerEqualsExpr = comparer.GetEqualsExpr();
            this.keyComparerEquals = this.keyComparerEqualsExpr.Compile();
            var equalsComp = comparer.GetEqualsExpr().Compile();
            var getHashCode = comparer.GetGetHashCodeExpr().Compile();

            this.pool = MemoryManager.GetMemoryPool<TKey, TPayload>(stream.Properties.IsColumnar);
            this.errorMessages = stream.ErrorMessages;

            this.period = stream.period;
            this.factor = stream.factor;
            this.antifactor = 1 - factor;

            this.isUngrouped = typeof(TKey) == typeof(Empty);
            this.filterStates = comparer.CreateFastDictionaryGenerator<TKey, FilterState>(1, equalsComp, getHashCode, stream.Properties.QueryContainer).Invoke();
            this.currentState = null;
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => Observer.ProduceQueryPlan(new FilterIIRPlanNode(
                previous, this, typeof(TKey), typeof(TPayload), this.period, this.factor, false, this.errorMessages));

        public override unsafe void OnNext(StreamMessage<TKey, TPayload> sourceBatch)
        {
            this.pool.Get(out StreamMessage<TKey, TPayload> destBatch);

            var count = sourceBatch.Count;
            destBatch.vsync = sourceBatch.vsync;
            destBatch.vother = sourceBatch.vother;
            destBatch.key = sourceBatch.key;
            destBatch.hash = sourceBatch.hash;
            destBatch.iter = sourceBatch.iter;
            this.pool.GetPayload(out destBatch.payload);
            destBatch.bitvector = sourceBatch.bitvector;

            TKey[] sourceKey = sourceBatch.key.col;
            TPayload[] sourcePayload = sourceBatch.payload.col;
            TPayload[] destPayload = destBatch.payload.col;
            fixed (long* sourceVSync = sourceBatch.vsync.col)
            fixed (long* sourceVOther = sourceBatch.vother.col)
            fixed (int* sourceHash = sourceBatch.hash.col)
            fixed (long* sourceBitVector = sourceBatch.bitvector.col)
            {
                long* sourceVSyncPtr = sourceVSync;

                for (int row = 0; row < count; row++)
                {
                    if ((sourceBitVector[row >> 6] & (1L << (row & 0x3f))) == 0)
                    {
                        long startTime = *sourceVSyncPtr;

                        if (this.currentState == null || ((!this.isUngrouped) && (this.currentHash != sourceHash[row] || !this.keyComparerEquals(this.currentKey, sourceKey[row]))))
                        {
                            this.currentKey = sourceKey[row];
                            this.currentHash = sourceHash[row];

                            if (!this.filterStates.Lookup(this.currentKey, this.currentHash, out int index))
                            {
                                // First time group is active for this time
                                this.currentState = new FilterState();
                                this.filterStates.Insert(ref index, this.currentKey, this.currentState);
                            }
                            else
                            {
                                // Read new currentState from _filterStates
                                this.currentState = this.filterStates.entries[index].value;
                            }
                        }

                        // TODO: Fix casting
                        this.currentState.value =
                            startTime > this.currentState.time + this.period
                                ? (TPayload)(object)(this.antifactor * (double)(object)sourcePayload[row])
                                : (TPayload)(object)(this.factor * (double)(object)this.currentState.value + this.antifactor * (double)(object)sourcePayload[row]);
                        this.currentState.time = startTime;

                        destPayload[row] = this.currentState.value;
                    }

                    // Advance pointers.
                    sourceVSyncPtr++;
                }
            }
            sourceBatch.payload.Return();
            sourceBatch.Return();
            destBatch.Count = count;
            Observer.OnNext(destBatch);
        }

        protected override void FlushContents() { }

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => this.filterStates.Count;

        [DataContract]
        internal sealed class FilterState
        {
            [DataMember]
            public TPayload value;

            [DataMember]
            public long time;
        }
    }
}
