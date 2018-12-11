// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{

    [DataContract]
    internal sealed class SelectManyPipe<TKey, TSource, TResult> : UnaryPipe<TKey, TSource, TResult>
    {
        private readonly MemoryPool<TKey, TResult> pool;
        private readonly string errorMessages;

        [SchemaSerialization]
        private readonly Expression<Func<TSource, IEnumerable<TResult>>> selector;
        private readonly Func<TSource, IEnumerable<TResult>> selectorFunc;

        [DataMember]
        private StreamMessage<TKey, TResult> batch;

        [DataMember]
        private int iter;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public SelectManyPipe() { }

        public SelectManyPipe(SelectManyStreamable<TKey, TSource, TResult> stream, IStreamObserver<TKey, TResult> observer)
            : base(stream, observer)
        {
            this.selector = (Expression<Func<TSource, IEnumerable<TResult>>>)stream.Selector;
            this.selectorFunc = this.selector.Compile();
            this.pool = MemoryManager.GetMemoryPool<TKey, TResult>(stream.Properties.IsColumnar);
            this.pool.Get(out this.batch);
            this.batch.Allocate();

            this.iter = 0;
            this.errorMessages = stream.ErrorMessages;
        }

        public override void ProduceQueryPlan(PlanNode previous)
        {
            this.Observer.ProduceQueryPlan(
                new SelectManyPlanNode(
                    previous, this,
                    typeof(TKey), typeof(TSource), typeof(TResult),
                    this.selector, false, false,
                    false, this.errorMessages));
        }

        protected override void DisposeState() => this.batch.Free();

        public override unsafe void OnNext(StreamMessage<TKey, TSource> batch)
        {
            var count = batch.Count;
            this.batch.iter = batch.iter;

            var dest_vsync = this.batch.vsync.col;
            var dest_vother = this.batch.vother.col;
            var destkey = this.batch.key.col;
            var dest_hash = this.batch.hash.col;

            var srckey = batch.key.col;

            fixed (long* src_bv = batch.bitvector.col, src_vsync = batch.vsync.col, src_vother = batch.vother.col)
            fixed (int* src_hash = batch.hash.col)
            {
                for (int i = 0; i < count; i++)
                {
                    if ((src_bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                    {
                        var enumerator = this.selectorFunc(batch[i]).GetEnumerator();
                        while (enumerator.MoveNext())
                        {
                            dest_vsync[this.iter] = src_vsync[i];
                            dest_vother[this.iter] = src_vother[i];
                            this.batch[this.iter] = enumerator.Current;
                            destkey[this.iter] = srckey[i];
                            dest_hash[this.iter] = src_hash[i];

                            this.iter++;

                            if (this.iter == Config.DataBatchSize)
                            {
                                FlushContents();
                                this.batch.iter = batch.iter;
                                dest_vsync = this.batch.vsync.col;
                                dest_vother = this.batch.vother.col;
                                destkey = this.batch.key.col;
                                dest_hash = this.batch.hash.col;
                            }
                        }
                        enumerator.Dispose();
                    }
                    else if (src_vother[i] < 0)
                    {
                        dest_vsync[this.iter] = src_vsync[i];
                        dest_vother[this.iter] = src_vother[i];
                        destkey[this.iter] = srckey[i];
                        dest_hash[this.iter] = src_hash[i];
                        this.batch.bitvector.col[(this.iter) >> 6] |= (1L << ((this.iter) & 0x3f));

                        this.iter++;

                        if (this.iter == Config.DataBatchSize)
                        {
                            FlushContents();
                            this.batch.iter = batch.iter;
                            dest_vsync = this.batch.vsync.col;
                            dest_vother = this.batch.vother.col;
                            destkey = this.batch.key.col;
                            dest_hash = this.batch.hash.col;
                        }
                    }
                }
            }

            batch.Free();
        }

        protected override void FlushContents()
        {
            if (this.iter == 0) return;
            this.batch.Count = this.iter;
            this.batch.Seal();
            this.Observer.OnNext(this.batch);
            this.iter = 0;
            this.pool.Get(out this.batch);
            this.batch.Allocate();
        }

        public override int CurrentlyBufferedOutputCount => this.iter;

        public override int CurrentlyBufferedInputCount => 0;
    }

    [DataContract]
    internal sealed class SelectManyPipeWithStartEdge<TKey, TSource, TResult> : UnaryPipe<TKey, TSource, TResult>
    {
        private readonly MemoryPool<TKey, TResult> pool;
        private readonly string errorMessages;

        [SchemaSerialization]
        private readonly Expression<Func<long, TSource, IEnumerable<TResult>>> selector;
        private readonly Func<long, TSource, IEnumerable<TResult>> selectorFunc;

        [DataMember]
        private StreamMessage<TKey, TResult> batch;

        [DataMember]
        private int iter;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public SelectManyPipeWithStartEdge() { }

        public SelectManyPipeWithStartEdge(SelectManyStreamable<TKey, TSource, TResult> stream, IStreamObserver<TKey, TResult> observer)
            : base(stream, observer)
        {
            this.selector = (Expression<Func<long, TSource, IEnumerable<TResult>>>)stream.Selector;
            this.selectorFunc = this.selector.Compile();
            this.pool = MemoryManager.GetMemoryPool<TKey, TResult>(stream.Properties.IsColumnar);
            this.pool.Get(out this.batch);
            this.batch.Allocate();

            this.iter = 0;
            this.errorMessages = stream.ErrorMessages;
        }

        public override void ProduceQueryPlan(PlanNode previous)
        {
            this.Observer.ProduceQueryPlan(
                new SelectManyPlanNode(
                    previous, this,
                    typeof(TKey), typeof(TSource), typeof(TResult),
                    this.selector, false, true,
                    false, this.errorMessages));
        }

        protected override void DisposeState() => this.batch.Free();

        public override unsafe void OnNext(StreamMessage<TKey, TSource> batch)
        {
            var count = batch.Count;
            this.batch.iter = batch.iter;

            var dest_vsync = this.batch.vsync.col;
            var dest_vother = this.batch.vother.col;
            var destkey = this.batch.key.col;
            var dest_hash = this.batch.hash.col;

            var srckey = batch.key.col;

            fixed (long* src_bv = batch.bitvector.col, src_vsync = batch.vsync.col, src_vother = batch.vother.col)
            fixed (int* src_hash = batch.hash.col)
            {
                for (int i = 0; i < count; i++)
                {
                    if ((src_bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                    {
                        var enumerator = this.selectorFunc(src_vsync[i] < src_vother[i] ? src_vsync[i] : src_vother[i], batch[i]).GetEnumerator();
                        while (enumerator.MoveNext())
                        {
                            dest_vsync[this.iter] = src_vsync[i];
                            dest_vother[this.iter] = src_vother[i];
                            this.batch[this.iter] = enumerator.Current;
                            destkey[this.iter] = srckey[i];
                            dest_hash[this.iter] = src_hash[i];

                            this.iter++;

                            if (this.iter == Config.DataBatchSize)
                            {
                                FlushContents();
                                this.batch.iter = batch.iter;
                                dest_vsync = this.batch.vsync.col;
                                dest_vother = this.batch.vother.col;
                                destkey = this.batch.key.col;
                                dest_hash = this.batch.hash.col;
                            }
                        }
                        enumerator.Dispose();
                    }
                    else if (src_vother[i] < 0)
                    {
                        dest_vsync[this.iter] = src_vsync[i];
                        dest_vother[this.iter] = src_vother[i];
                        destkey[this.iter] = srckey[i];
                        dest_hash[this.iter] = src_hash[i];
                        this.batch.bitvector.col[(this.iter) >> 6] |= (1L << ((this.iter) & 0x3f));

                        this.iter++;

                        if (this.iter == Config.DataBatchSize)
                        {
                            FlushContents();
                            this.batch.iter = batch.iter;
                            dest_vsync = this.batch.vsync.col;
                            dest_vother = this.batch.vother.col;
                            destkey = this.batch.key.col;
                            dest_hash = this.batch.hash.col;
                        }
                    }
                }
            }

            batch.Free();
        }

        protected override void FlushContents()
        {
            if (this.iter == 0) return;
            this.batch.Count = this.iter;
            this.batch.Seal();
            this.Observer.OnNext(this.batch);
            this.iter = 0;
            this.pool.Get(out this.batch);
            this.batch.Allocate();
        }

        public override int CurrentlyBufferedOutputCount => this.iter;

        public override int CurrentlyBufferedInputCount => 0;
    }

    [DataContract]
    internal sealed class SelectManyKeyPipe<TKey, TSource, TResult> : UnaryPipe<TKey, TSource, TResult>
    {
        private readonly MemoryPool<TKey, TResult> pool;
        private readonly string errorMessages;

        [SchemaSerialization]
        private readonly Expression<Func<TKey, TSource, IEnumerable<TResult>>> selector;
        private readonly Func<TKey, TSource, IEnumerable<TResult>> selectorFunc;

        [DataMember]
        private StreamMessage<TKey, TResult> batch;

        [DataMember]
        private int iter;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public SelectManyKeyPipe() { }

        public SelectManyKeyPipe(SelectManyStreamable<TKey, TSource, TResult> stream, IStreamObserver<TKey, TResult> observer)
            : base(stream, observer)
        {
            this.selector = (Expression<Func<TKey, TSource, IEnumerable<TResult>>>)stream.Selector;
            this.selectorFunc = this.selector.Compile();
            this.pool = MemoryManager.GetMemoryPool<TKey, TResult>(stream.Properties.IsColumnar);
            this.pool.Get(out this.batch);
            this.batch.Allocate();

            this.iter = 0;
            this.errorMessages = stream.ErrorMessages;
        }

        public override void ProduceQueryPlan(PlanNode previous)
        {
            this.Observer.ProduceQueryPlan(
                new SelectManyPlanNode(
                    previous, this,
                    typeof(TKey), typeof(TSource), typeof(TResult),
                    this.selector, true, false,
                    false, this.errorMessages));
        }

        protected override void DisposeState() => this.batch.Free();

        public override unsafe void OnNext(StreamMessage<TKey, TSource> batch)
        {
            var count = batch.Count;
            this.batch.iter = batch.iter;

            var dest_vsync = this.batch.vsync.col;
            var dest_vother = this.batch.vother.col;
            var destkey = this.batch.key.col;
            var dest_hash = this.batch.hash.col;

            var srckey = batch.key.col;

            fixed (long* src_bv = batch.bitvector.col, src_vsync = batch.vsync.col, src_vother = batch.vother.col)
            fixed (int* src_hash = batch.hash.col)
            {
                for (int i = 0; i < count; i++)
                {
                    if ((src_bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                    {
                        var enumerator = this.selectorFunc(srckey[i], batch[i]).GetEnumerator();
                        while (enumerator.MoveNext())
                        {
                            dest_vsync[this.iter] = src_vsync[i];
                            dest_vother[this.iter] = src_vother[i];
                            this.batch[this.iter] = enumerator.Current;
                            destkey[this.iter] = srckey[i];
                            dest_hash[this.iter] = src_hash[i];

                            this.iter++;

                            if (this.iter == Config.DataBatchSize)
                            {
                                FlushContents();
                                this.batch.iter = batch.iter;
                                dest_vsync = this.batch.vsync.col;
                                dest_vother = this.batch.vother.col;
                                destkey = this.batch.key.col;
                                dest_hash = this.batch.hash.col;
                            }
                        }
                        enumerator.Dispose();
                    }
                    else if (src_vother[i] < 0)
                    {
                        dest_vsync[this.iter] = src_vsync[i];
                        dest_vother[this.iter] = src_vother[i];
                        destkey[this.iter] = srckey[i];
                        dest_hash[this.iter] = src_hash[i];
                        this.batch.bitvector.col[(this.iter) >> 6] |= (1L << ((this.iter) & 0x3f));

                        this.iter++;

                        if (this.iter == Config.DataBatchSize)
                        {
                            FlushContents();
                            this.batch.iter = batch.iter;
                            dest_vsync = this.batch.vsync.col;
                            dest_vother = this.batch.vother.col;
                            destkey = this.batch.key.col;
                            dest_hash = this.batch.hash.col;
                        }
                    }
                }
            }

            batch.Free();
        }

        protected override void FlushContents()
        {
            if (this.iter == 0) return;
            this.batch.Count = this.iter;
            this.batch.Seal();
            this.Observer.OnNext(this.batch);
            this.iter = 0;
            this.pool.Get(out this.batch);
            this.batch.Allocate();
        }

        public override int CurrentlyBufferedOutputCount => this.iter;

        public override int CurrentlyBufferedInputCount => 0;
    }

    [DataContract]
    internal sealed class SelectManyKeyPipeWithStartEdge<TKey, TSource, TResult> : UnaryPipe<TKey, TSource, TResult>
    {
        private readonly MemoryPool<TKey, TResult> pool;
        private readonly string errorMessages;

        [SchemaSerialization]
        private readonly Expression<Func<long, TKey, TSource, IEnumerable<TResult>>> selector;
        private readonly Func<long, TKey, TSource, IEnumerable<TResult>> selectorFunc;

        [DataMember]
        private StreamMessage<TKey, TResult> batch;

        [DataMember]
        private int iter;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public SelectManyKeyPipeWithStartEdge() { }

        public SelectManyKeyPipeWithStartEdge(SelectManyStreamable<TKey, TSource, TResult> stream, IStreamObserver<TKey, TResult> observer)
            : base(stream, observer)
        {
            this.selector = (Expression<Func<long, TKey, TSource, IEnumerable<TResult>>>)stream.Selector;
            this.selectorFunc = this.selector.Compile();
            this.pool = MemoryManager.GetMemoryPool<TKey, TResult>(stream.Properties.IsColumnar);
            this.pool.Get(out this.batch);
            this.batch.Allocate();

            this.iter = 0;
            this.errorMessages = stream.ErrorMessages;
        }

        public override void ProduceQueryPlan(PlanNode previous)
        {
            this.Observer.ProduceQueryPlan(
                new SelectManyPlanNode(
                    previous, this,
                    typeof(TKey), typeof(TSource), typeof(TResult),
                    this.selector, true, true,
                    false, this.errorMessages));
        }

        protected override void DisposeState() => this.batch.Free();

        public override unsafe void OnNext(StreamMessage<TKey, TSource> batch)
        {
            var count = batch.Count;
            this.batch.iter = batch.iter;

            var dest_vsync = this.batch.vsync.col;
            var dest_vother = this.batch.vother.col;
            var destkey = this.batch.key.col;
            var dest_hash = this.batch.hash.col;

            var srckey = batch.key.col;

            fixed (long* src_bv = batch.bitvector.col, src_vsync = batch.vsync.col, src_vother = batch.vother.col)
            fixed (int* src_hash = batch.hash.col)
            {
                for (int i = 0; i < count; i++)
                {
                    if ((src_bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                    {
                        var enumerator = this.selectorFunc(src_vsync[i] < src_vother[i] ? src_vsync[i] : src_vother[i], srckey[i], batch[i]).GetEnumerator();
                        while (enumerator.MoveNext())
                        {
                            dest_vsync[this.iter] = src_vsync[i];
                            dest_vother[this.iter] = src_vother[i];
                            this.batch[this.iter] = enumerator.Current;
                            destkey[this.iter] = srckey[i];
                            dest_hash[this.iter] = src_hash[i];

                            this.iter++;

                            if (this.iter == Config.DataBatchSize)
                            {
                                FlushContents();
                                this.batch.iter = batch.iter;
                                dest_vsync = this.batch.vsync.col;
                                dest_vother = this.batch.vother.col;
                                destkey = this.batch.key.col;
                                dest_hash = this.batch.hash.col;
                            }
                        }
                        enumerator.Dispose();
                    }
                    else if (src_vother[i] < 0)
                    {
                        dest_vsync[this.iter] = src_vsync[i];
                        dest_vother[this.iter] = src_vother[i];
                        destkey[this.iter] = srckey[i];
                        dest_hash[this.iter] = src_hash[i];
                        this.batch.bitvector.col[(this.iter) >> 6] |= (1L << ((this.iter) & 0x3f));

                        this.iter++;

                        if (this.iter == Config.DataBatchSize)
                        {
                            FlushContents();
                            this.batch.iter = batch.iter;
                            dest_vsync = this.batch.vsync.col;
                            dest_vother = this.batch.vother.col;
                            destkey = this.batch.key.col;
                            dest_hash = this.batch.hash.col;
                        }
                    }
                }
            }

            batch.Free();
        }

        protected override void FlushContents()
        {
            if (this.iter == 0) return;
            this.batch.Count = this.iter;
            this.batch.Seal();
            this.Observer.OnNext(this.batch);
            this.iter = 0;
            this.pool.Get(out this.batch);
            this.batch.Allocate();
        }

        public override int CurrentlyBufferedOutputCount => this.iter;

        public override int CurrentlyBufferedInputCount => 0;
    }
}