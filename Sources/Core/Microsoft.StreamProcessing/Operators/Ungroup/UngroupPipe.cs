#define PARALLELMERGE
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
    internal sealed class PartitionedUngroupNestedPipe<TOuterKey, TInnerKey, TInnerResult, TResult>
        : Pipe<TOuterKey, TResult>, IStreamObserver<CompoundGroupKey<TOuterKey, TInnerKey>, TInnerResult>
    {
        private readonly MemoryPool<TOuterKey, TResult> outPool;
        private readonly string errorMessages;

        [SchemaSerialization]
        private readonly Expression<Func<TInnerKey, TInnerResult, TResult>> resultSelectorExpr;
        private readonly Func<TInnerKey, TInnerResult, TResult> resultSelector;
        [SchemaSerialization]
        private readonly Expression<Func<TOuterKey, int>> keyComparer;
        private readonly Func<TOuterKey, int> outerHashCode;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public PartitionedUngroupNestedPipe() { }

        public PartitionedUngroupNestedPipe(
            UngroupStreamable<TOuterKey, TInnerKey, TInnerResult, TResult> stream,
            IStreamObserver<TOuterKey, TResult> observer)
            : base(stream, observer)
        {
            this.resultSelectorExpr = stream.ResultSelector;
            this.resultSelector = this.resultSelectorExpr.Compile();
            this.outPool = MemoryManager.GetMemoryPool<TOuterKey, TResult>(stream.Properties.IsColumnar);
            this.errorMessages = stream.ErrorMessages;
            this.keyComparer = stream.Properties.KeyEqualityComparer.GetGetHashCodeExpr();
            this.outerHashCode = this.keyComparer.Compile();
        }

        public unsafe void OnNext(StreamMessage<CompoundGroupKey<TOuterKey, TInnerKey>, TInnerResult> batch)
        {
            outPool.Get(out StreamMessage<TOuterKey, TResult> tmp);
            tmp.AllocatePayload();
            outPool.GetKey(out tmp.key);
            tmp.hash = batch.hash.MakeWritable(outPool.intPool);
            var count = batch.Count;

            tmp.vsync = batch.vsync;
            tmp.vother = batch.vother;
            tmp.bitvector = batch.bitvector;

            fixed (long* srcbv = batch.bitvector.col)
            fixed (int* desthash = tmp.hash.col)
            {
                var srckey = batch.key.col;
                var destkey = tmp.key.col;

                for (int i = 0; i < count; i++)
                {
                    if ((srcbv[i >> 6] & (1L << (i & 0x3f))) != 0)
                    {
                        if (batch.vother.col[i] == long.MinValue)
                        {
                            destkey[i] = srckey[i].outerGroup;
                            desthash[i] = batch.hash.col[i];
                            tmp.bitvector.col[i >> 6] |= 1L << (i & 0x3f);
                        }
                        continue;
                    }
                    destkey[i] = srckey[i].outerGroup;
                    tmp[i] = resultSelector(srckey[i].innerGroup, batch[i]);
                    desthash[i] = this.outerHashCode(destkey[i]);
                }
            }

            tmp.Count = count;
            tmp.Seal();

            Observer.OnNext(tmp);

            batch.ReleasePayload();
            batch.key.Return();
            batch.Return();
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => Observer.ProduceQueryPlan(new UngroupPlanNode(
                previous,
                this,
                typeof(CompoundGroupKey<TOuterKey, TInnerKey>),
                typeof(TOuterKey),
                typeof(TInnerResult),
                typeof(TResult),
                resultSelectorExpr,
                false,
                errorMessages));

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }

    [DataContract]
    internal sealed class UngroupNestedPipe<TOuterKey, TInnerKey, TInnerResult, TResult>
        : Pipe<TOuterKey, TResult>, IStreamObserver<CompoundGroupKey<TOuterKey, TInnerKey>, TInnerResult>
    {
        private readonly MemoryPool<TOuterKey, TResult> outPool;
        private readonly string errorMessages;

        [SchemaSerialization]
        private readonly Expression<Func<TInnerKey, TInnerResult, TResult>> resultSelectorExpr;
        private readonly Func<TInnerKey, TInnerResult, TResult> resultSelector;
        [SchemaSerialization]
        private readonly Expression<Func<TOuterKey, int>> keyComparer;
        private readonly Func<TOuterKey, int> outerHashCode;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public UngroupNestedPipe() { }

        public UngroupNestedPipe(
            UngroupStreamable<TOuterKey, TInnerKey, TInnerResult, TResult> stream,
            IStreamObserver<TOuterKey, TResult> observer)
            : base(stream, observer)
        {
            this.resultSelectorExpr = stream.ResultSelector;
            this.resultSelector = this.resultSelectorExpr.Compile();
            this.outPool = MemoryManager.GetMemoryPool<TOuterKey, TResult>(stream.Properties.IsColumnar);
            this.errorMessages = stream.ErrorMessages;
            this.keyComparer = stream.Properties.KeyEqualityComparer.GetGetHashCodeExpr();
            this.outerHashCode = this.keyComparer.Compile();
        }

        public unsafe void OnNext(StreamMessage<CompoundGroupKey<TOuterKey, TInnerKey>, TInnerResult> batch)
        {
            outPool.Get(out StreamMessage<TOuterKey, TResult> tmp);
            tmp.AllocatePayload();
            outPool.GetKey(out tmp.key);
            tmp.hash = batch.hash.MakeWritable(outPool.intPool);
            var count = batch.Count;

            tmp.vsync = batch.vsync;
            tmp.vother = batch.vother;
            tmp.bitvector = batch.bitvector;

            fixed (long* srcbv = batch.bitvector.col)
            fixed (int* desthash = tmp.hash.col)
            {
                var srckey = batch.key.col;
                var destkey = tmp.key.col;

                for (int i = 0; i < count; i++)
                {
                    if ((srcbv[i >> 6] & (1L << (i & 0x3f))) != 0) continue;
                    destkey[i] = srckey[i].outerGroup;
                    tmp[i] = resultSelector(srckey[i].innerGroup, batch[i]);
                    desthash[i] = this.outerHashCode(destkey[i]);
                }
            }

            tmp.Count = count;
            tmp.Seal();

            Observer.OnNext(tmp);

            batch.ReleasePayload();
            batch.key.Return();
            batch.Return();
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => Observer.ProduceQueryPlan(new UngroupPlanNode(
                previous,
                this,
                typeof(CompoundGroupKey<TOuterKey, TInnerKey>),
                typeof(TOuterKey),
                typeof(TInnerResult),
                typeof(TResult),
                resultSelectorExpr,
                false,
                errorMessages));

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }

    [DataContract]
    internal sealed class UngroupPipe<TInnerKey, TInnerResult, TResult>
        : Pipe<Empty, TResult>, IStreamObserver<TInnerKey, TInnerResult>
    {
        private readonly MemoryPool<Empty, TResult> outPool;
        private readonly string errorMessages;

        [SchemaSerialization]
        private readonly Expression<Func<TInnerKey, TInnerResult, TResult>> resultSelectorExpr;
        private readonly Func<TInnerKey, TInnerResult, TResult> resultSelector;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public UngroupPipe() { }

        public UngroupPipe(
            UngroupStreamable<TInnerKey, TInnerResult, TResult> stream,
            IStreamObserver<Empty, TResult> observer)
            : base(stream, observer)
        {
            this.resultSelectorExpr = stream.ResultSelector;
            this.resultSelector = this.resultSelectorExpr.Compile();
            this.outPool = MemoryManager.GetMemoryPool<Empty, TResult>(stream.Properties.IsColumnar);
            this.errorMessages = stream.ErrorMessages;
        }

        public unsafe void OnNext(StreamMessage<TInnerKey, TInnerResult> batch)
        {
            outPool.Get(out StreamMessage<Empty, TResult> tmp);
            tmp.AllocatePayload();
            outPool.GetKey(out tmp.key);
            tmp.hash = batch.hash.MakeWritable(outPool.intPool);
            var count = batch.Count;

            tmp.vsync = batch.vsync;
            tmp.vother = batch.vother;
            tmp.bitvector = batch.bitvector;

            fixed (long* srcbv = batch.bitvector.col)
            fixed (int* desthash = tmp.hash.col)
            {
                var srckey = batch.key.col;
                var destkey = tmp.key.col;

                Array.Clear(tmp.hash.col, 0, count);

                var unit = Empty.Default;

                for (int i = 0; i < count; i++)
                {
                    if ((srcbv[i >> 6] & (1L << (i & 0x3f))) != 0) continue;
                    destkey[i] = unit;
                    tmp[i] = resultSelector(srckey[i], batch[i]);
                }
            }

            tmp.Count = count;
            tmp.Seal();

            Observer.OnNext(tmp);

            batch.ReleasePayload();
            batch.key.Return();
            batch.Return();
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => Observer.ProduceQueryPlan(new UngroupPlanNode(
                previous,
                this,
                typeof(TInnerKey),
                typeof(Empty),
                typeof(TInnerResult),
                typeof(TResult),
                resultSelectorExpr,
                false,
                errorMessages));

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }
}