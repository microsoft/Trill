// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class WherePipe<TKey, TPayload> : UnaryPipe<TKey, TPayload, TPayload>
    {
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, bool>> predicate;
        private readonly bool isPartitioned;
        private readonly Func<TPayload, bool> predicateFunc;
        private readonly MemoryPool<TKey, TPayload> pool;
        private readonly string errorMessages;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public WherePipe() { }

        public WherePipe(WhereStreamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer)
            : base(stream, observer)
        {
            this.predicate = stream.Predicate;
            this.predicateFunc = this.predicate.Compile();
            this.pool = MemoryManager.GetMemoryPool<TKey, TPayload>(stream.Properties.IsColumnar);
            this.errorMessages = stream.ErrorMessages;
            this.isPartitioned = typeof(TKey).GetPartitionType() != null;
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new WherePlanNode(
                previous, this,
                typeof(TKey), typeof(TPayload), this.predicate,
                false, this.errorMessages));

        public override unsafe void OnNext(StreamMessage<TKey, TPayload> batch)
        {
            var count = batch.Count;
            batch.bitvector = batch.bitvector.MakeWritable(this.pool.bitvectorPool);

            fixed (long* bv = batch.bitvector.col)
            {
                var pay = batch.payload.col;
                for (int i = 0; i < count; i++)
                {
                    if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                    {
                        if (!this.predicateFunc(pay[i]))
                        {
                            bv[i >> 6] |= (1L << (i & 0x3f));
                        }
                    }
                }
            }
            if (!this.isPartitioned && batch.RefreshCount() && (batch.Count == 0))
                batch.Free();
            else
                this.Observer.OnNext(batch);
        }

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }
}
