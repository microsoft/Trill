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
    internal sealed class PartitionedGroupNestedPipe<TOuterKey, TSource, TInnerKey> :
        Pipe<CompoundGroupKey<TOuterKey, TInnerKey>, TSource>, IStreamObserver<TOuterKey, TSource>
    {
        [SchemaSerialization]
        private readonly Expression<Func<TSource, TInnerKey>> keySelector;
        private readonly Func<TSource, TInnerKey> keySelectorFunc;
        [SchemaSerialization]
        private readonly Expression<Func<TInnerKey, int>> keyComparer;
        private readonly Func<TInnerKey, int> innerHashCode;

        private readonly MemoryPool<CompoundGroupKey<TOuterKey, TInnerKey>, TSource> l1Pool;
        private readonly string errorMessages;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public PartitionedGroupNestedPipe() { }

        public PartitionedGroupNestedPipe(
            GroupNestedStreamable<TOuterKey, TSource, TInnerKey> stream,
            IStreamObserver<CompoundGroupKey<TOuterKey, TInnerKey>, TSource> observer)
            : base(stream, observer)
        {
            this.keySelector = stream.KeySelector;
            this.keySelectorFunc = this.keySelector.Compile();
            this.keyComparer = ((CompoundGroupKeyEqualityComparer<TOuterKey, TInnerKey>)stream.Properties.KeyEqualityComparer).innerComparer.GetGetHashCodeExpr();
            this.innerHashCode = this.keyComparer.Compile();

            this.errorMessages = stream.ErrorMessages;
            this.l1Pool = MemoryManager.GetMemoryPool<CompoundGroupKey<TOuterKey, TInnerKey>, TSource>(stream.Properties.IsColumnar);
        }

        public unsafe void OnNext(StreamMessage<TOuterKey, TSource> batch)
        {
            this.l1Pool.Get(out StreamMessage<CompoundGroupKey<TOuterKey, TInnerKey>, TSource> outputBatch);
            outputBatch.vsync = batch.vsync;
            outputBatch.vother = batch.vother;
            outputBatch.payload = batch.payload;
            outputBatch.hash = batch.hash.MakeWritable(this.l1Pool.intPool);
            outputBatch.bitvector = batch.bitvector;
            this.l1Pool.GetKey(out outputBatch.key);

            outputBatch.Count = batch.Count;

            var count = batch.Count;

            var srckey = batch.key.col;
            var destkey = outputBatch.key.col;
            var destpayload = outputBatch;
            fixed (long* srcbv = batch.bitvector.col)
            {
                fixed (int* desthash = outputBatch.hash.col)
                {
                    for (int i = 0; i < count; i++)
                    {
                        if ((srcbv[i >> 6] & (1L << (i & 0x3f))) != 0)
                        {
                            if (batch.vother.col[i] == long.MinValue)
                            {
                                destkey[i].outerGroup = srckey[i];
                                destkey[i].innerGroup = default;
                                desthash[i] = batch.hash.col[i];
                                outputBatch.bitvector.col[i >> 6] |= (1L << (i & 0x3f));
                            }
                            continue;
                        }
                        var key = this.keySelectorFunc(destpayload[i]);
                        var innerHash = this.innerHashCode(key);
                        var hash = desthash[i] ^ innerHash;

                        destkey[i].outerGroup = srckey[i];
                        destkey[i].innerGroup = key;
                        destkey[i].hashCode = hash;
                        desthash[i] = hash;
                    }
                }
            }

            batch.key.Return();
            batch.Return();
            this.Observer.OnNext(outputBatch);
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new GroupPlanNode(
                previous,
                this,
                typeof(TOuterKey),
                typeof(CompoundGroupKey<TOuterKey, TInnerKey>),
                typeof(TSource),
                this.keySelector,
                int.MinValue,
                1,
                false,
                false,
                this.errorMessages));

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }

    [DataContract]
    internal sealed class GroupNestedPipe<TOuterKey, TSource, TInnerKey> :
        Pipe<CompoundGroupKey<TOuterKey, TInnerKey>, TSource>, IStreamObserver<TOuterKey, TSource>
    {
        [SchemaSerialization]
        private readonly Expression<Func<TSource, TInnerKey>> keySelector;
        private readonly Func<TSource, TInnerKey> keySelectorFunc;
        [SchemaSerialization]
        private readonly Expression<Func<TInnerKey, int>> keyComparer;
        private readonly Func<TInnerKey, int> innerHashCode;

        private readonly MemoryPool<CompoundGroupKey<TOuterKey, TInnerKey>, TSource> l1Pool;
        private readonly string errorMessages;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public GroupNestedPipe() { }

        public GroupNestedPipe(
            GroupNestedStreamable<TOuterKey, TSource, TInnerKey> stream,
            IStreamObserver<CompoundGroupKey<TOuterKey, TInnerKey>, TSource> observer)
            : base(stream, observer)
        {
            this.keySelector = stream.KeySelector;
            this.keySelectorFunc = this.keySelector.Compile();
            this.keyComparer = ((CompoundGroupKeyEqualityComparer<TOuterKey, TInnerKey>)stream.Properties.KeyEqualityComparer).innerComparer.GetGetHashCodeExpr();
            this.innerHashCode = this.keyComparer.Compile();

            this.errorMessages = stream.ErrorMessages;
            this.l1Pool = MemoryManager.GetMemoryPool<CompoundGroupKey<TOuterKey, TInnerKey>, TSource>(stream.Properties.IsColumnar);
        }

        public unsafe void OnNext(StreamMessage<TOuterKey, TSource> batch)
        {
            this.l1Pool.Get(out StreamMessage<CompoundGroupKey<TOuterKey, TInnerKey>, TSource> outputBatch);
            outputBatch.vsync = batch.vsync;
            outputBatch.vother = batch.vother;
            outputBatch.payload = batch.payload;
            outputBatch.hash = batch.hash.MakeWritable(this.l1Pool.intPool);
            outputBatch.bitvector = batch.bitvector;
            this.l1Pool.GetKey(out outputBatch.key);

            outputBatch.Count = batch.Count;

            var count = batch.Count;

            var srckey = batch.key.col;
            var destkey = outputBatch.key.col;
            var destpayload = outputBatch;
            fixed (long* srcbv = batch.bitvector.col)
            {
                fixed (int* desthash = outputBatch.hash.col)
                {
                    for (int i = 0; i < count; i++)
                    {
                        if ((srcbv[i >> 6] & (1L << (i & 0x3f))) != 0) continue;
                        var key = this.keySelectorFunc(destpayload[i]);
                        var innerHash = this.innerHashCode(key);
                        var hash = desthash[i] ^ innerHash;

                        destkey[i].outerGroup = srckey[i];
                        destkey[i].innerGroup = key;
                        destkey[i].hashCode = hash;
                        desthash[i] = hash;
                    }
                }
            }

            batch.key.Return();
            batch.Return();
            this.Observer.OnNext(outputBatch);
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new GroupPlanNode(
                previous,
                this,
                typeof(TOuterKey),
                typeof(CompoundGroupKey<TOuterKey, TInnerKey>),
                typeof(TSource),
                this.keySelector,
                int.MinValue,
                1,
                false,
                false,
                this.errorMessages));

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }

    [DataContract]
    internal sealed class GroupPipe<TOuterKey, TSource, TInnerKey> :
        Pipe<TInnerKey, TSource>, IStreamObserver<TOuterKey, TSource>
    {
        [SchemaSerialization]
        private readonly Expression<Func<TSource, TInnerKey>> keySelector;
        private readonly Func<TSource, TInnerKey> keySelectorFunc;
        [SchemaSerialization]
        private readonly Expression<Func<TInnerKey, int>> keyComparer;
        private readonly Func<TInnerKey, int> innerHashCode;

        private readonly MemoryPool<TInnerKey, TSource> l1Pool;
        private readonly string errorMessages;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public GroupPipe() { }

        public GroupPipe(
            GroupStreamable<TOuterKey, TSource, TInnerKey> stream,
            IStreamObserver<TInnerKey, TSource> observer)
            : base(stream, observer)
        {
            this.keySelector = stream.KeySelector;
            this.keySelectorFunc = this.keySelector.Compile();
            this.keyComparer = stream.Properties.KeyEqualityComparer.GetGetHashCodeExpr();
            this.innerHashCode = this.keyComparer.Compile();

            this.errorMessages = stream.ErrorMessages;
            this.l1Pool = MemoryManager.GetMemoryPool<TInnerKey, TSource>(stream.Properties.IsColumnar);
        }

        public unsafe void OnNext(StreamMessage<TOuterKey, TSource> batch)
        {
            this.l1Pool.Get(out StreamMessage<TInnerKey, TSource> outputBatch);
            outputBatch.vsync = batch.vsync;
            outputBatch.vother = batch.vother;
            outputBatch.payload = batch.payload;
            outputBatch.hash = batch.hash.MakeWritable(this.l1Pool.intPool);
            outputBatch.bitvector = batch.bitvector;
            this.l1Pool.GetKey(out outputBatch.key);

            outputBatch.Count = batch.Count;

            var count = batch.Count;

            var destkey = outputBatch.key.col;
            var destpayload = outputBatch;
            fixed (long* srcbv = batch.bitvector.col)
            {
                fixed (int* desthash = outputBatch.hash.col)
                {
                    for (int i = 0; i < count; i++)
                    {
                        if ((srcbv[i >> 6] & (1L << (i & 0x3f))) != 0) continue;
                        var key = this.keySelectorFunc(destpayload[i]);
                        destkey[i] = key;
                        desthash[i] = this.innerHashCode(key);
                    }
                }
            }

            batch.key.Return();
            batch.Return();
            this.Observer.OnNext(outputBatch);
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new GroupPlanNode(
                previous,
                this,
                typeof(TOuterKey),
                typeof(TInnerKey),
                typeof(TSource),
                this.keySelector,
                int.MinValue,
                1,
                false,
                false,
                this.errorMessages));

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }
}