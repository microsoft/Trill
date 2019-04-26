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
    internal sealed class SelectPipe<TKey, TPayload, TResult> : UnaryPipe<TKey, TPayload, TResult>
    {
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, TResult>> selector;
        private readonly Func<TPayload, TResult> selectorFunc;
        private readonly MemoryPool<TKey, TResult> pool;
        private readonly string errorMessages;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public SelectPipe() { }

        public SelectPipe(SelectStreamable<TKey, TPayload, TResult> stream, IStreamObserver<TKey, TResult> observer)
            : base(stream, observer)
        {
            this.selector = (Expression<Func<TPayload, TResult>>)stream.Selector;
            this.selectorFunc = this.selector.Compile();
            this.pool = MemoryManager.GetMemoryPool<TKey, TResult>(stream.Properties.IsColumnar);
            this.errorMessages = stream.ErrorMessages;
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new SelectPlanNode(
                previous, this,
                typeof(TKey), typeof(TPayload), typeof(TResult),
                this.selector, false, false, false, this.errorMessages));

        public override unsafe void OnNext(StreamMessage<TKey, TPayload> batch)
        {
            this.pool.Get(out StreamMessage<TKey, TResult> outputBatch);

            var count = batch.Count;
            outputBatch.vsync = batch.vsync;
            outputBatch.vother = batch.vother;
            outputBatch.key = batch.key;
            outputBatch.hash = batch.hash;
            outputBatch.iter = batch.iter;
            this.pool.GetPayload(out outputBatch.payload);
            outputBatch.bitvector = batch.bitvector;

            var dest = outputBatch.payload.col;
            var src = batch.payload.col;
            fixed (long* bv = batch.bitvector.col)
            {
                for (int i = 0; i < count; i++)
                {
                    if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                        dest[i] = this.selectorFunc(src[i]);
                }
            }
            outputBatch.Count = count;
            batch.payload.Return();
            batch.Return();
            this.Observer.OnNext(outputBatch);
        }

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }

    [DataContract]
    internal sealed class SelectPipeWithStartEdge<TKey, TPayload, TResult> : UnaryPipe<TKey, TPayload, TResult>
    {
        [SchemaSerialization]
        private readonly Expression<Func<long, TPayload, TResult>> selector;
        private readonly Func<long, TPayload, TResult> selectorFunc;
        private readonly MemoryPool<TKey, TResult> pool;
        private readonly string errorMessages;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public SelectPipeWithStartEdge() { }

        public SelectPipeWithStartEdge(SelectStreamable<TKey, TPayload, TResult> stream, IStreamObserver<TKey, TResult> observer)
            : base(stream, observer)
        {
            this.selector = (Expression<Func<long, TPayload, TResult>>)stream.Selector;
            this.selectorFunc = this.selector.Compile();
            this.pool = MemoryManager.GetMemoryPool<TKey, TResult>(stream.Properties.IsColumnar);
            this.errorMessages = stream.ErrorMessages;
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new SelectPlanNode(
                previous, this,
                typeof(TKey), typeof(TPayload), typeof(TResult),
                this.selector, false, true, false, this.errorMessages));

        public override unsafe void OnNext(StreamMessage<TKey, TPayload> batch)
        {
            this.pool.Get(out StreamMessage<TKey, TResult> outputBatch);

            var count = batch.Count;
            outputBatch.vsync = batch.vsync;
            outputBatch.vother = batch.vother;
            outputBatch.key = batch.key;
            outputBatch.hash = batch.hash;
            outputBatch.iter = batch.iter;
            this.pool.GetPayload(out outputBatch.payload);
            outputBatch.bitvector = batch.bitvector;

            var dest = outputBatch.payload.col;
            var src = batch.payload.col;
            var vsync = batch.vsync.col;
            var vother = batch.vother.col;
            fixed (long* bv = batch.bitvector.col)
            {
                for (int i = 0; i < count; i++)
                {
                    if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                        dest[i] = this.selectorFunc(vsync[i] < vother[i] ? vsync[i] : vother[i], src[i]);
                }
            }
            outputBatch.Count = count;
            batch.payload.Return();
            batch.Return();
            this.Observer.OnNext(outputBatch);
        }

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }

    [DataContract]
    internal sealed class SelectKeyPipe<TKey, TPayload, TResult> : UnaryPipe<TKey, TPayload, TResult>
    {
        [SchemaSerialization]
        private readonly Expression<Func<TKey, TPayload, TResult>> selector;
        private readonly Func<TKey, TPayload, TResult> selectorFunc;
        private readonly MemoryPool<TKey, TResult> pool;
        private readonly string errorMessages;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public SelectKeyPipe() { }

        public SelectKeyPipe(SelectStreamable<TKey, TPayload, TResult> stream, IStreamObserver<TKey, TResult> observer)
            : base(stream, observer)
        {
            this.selector = (Expression<Func<TKey, TPayload, TResult>>)stream.Selector;
            this.selectorFunc = this.selector.Compile();
            this.pool = MemoryManager.GetMemoryPool<TKey, TResult>(stream.Properties.IsColumnar);
            this.errorMessages = stream.ErrorMessages;
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new SelectPlanNode(
                previous, this,
                typeof(TKey), typeof(TPayload), typeof(TResult),
                this.selector, true, false, false, this.errorMessages));

        public override unsafe void OnNext(StreamMessage<TKey, TPayload> batch)
        {
            this.pool.Get(out StreamMessage<TKey, TResult> outputBatch);

            var count = batch.Count;
            outputBatch.vsync = batch.vsync;
            outputBatch.vother = batch.vother;
            outputBatch.key = batch.key;
            outputBatch.hash = batch.hash;
            outputBatch.iter = batch.iter;
            this.pool.GetPayload(out outputBatch.payload);
            outputBatch.bitvector = batch.bitvector;

            var dest = outputBatch.payload.col;
            var src = batch.payload.col;
            var srckey = batch.key.col;
            fixed (long* bv = batch.bitvector.col)
            {
                for (int i = 0; i < count; i++)
                {
                    if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                        dest[i] = this.selectorFunc(srckey[i], src[i]);
                }
            }
            outputBatch.Count = count;
            batch.payload.Return();
            batch.Return();
            this.Observer.OnNext(outputBatch);
        }

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }

    [DataContract]
    internal sealed class SelectKeyPipeWithStartEdge<TKey, TPayload, TResult> : UnaryPipe<TKey, TPayload, TResult>
    {
        [SchemaSerialization]
        private readonly Expression<Func<long, TKey, TPayload, TResult>> selector;
        private readonly Func<long, TKey, TPayload, TResult> selectorFunc;
        private readonly MemoryPool<TKey, TResult> pool;
        private readonly string errorMessages;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public SelectKeyPipeWithStartEdge() { }

        public SelectKeyPipeWithStartEdge(SelectStreamable<TKey, TPayload, TResult> stream, IStreamObserver<TKey, TResult> observer)
            : base(stream, observer)
        {
            this.selector = (Expression<Func<long, TKey, TPayload, TResult>>)stream.Selector;
            this.selectorFunc = this.selector.Compile();
            this.pool = MemoryManager.GetMemoryPool<TKey, TResult>(stream.Properties.IsColumnar);
            this.errorMessages = stream.ErrorMessages;
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new SelectPlanNode(
                previous, this,
                typeof(TKey), typeof(TPayload), typeof(TResult),
                this.selector, true, true, false, this.errorMessages));

        public override unsafe void OnNext(StreamMessage<TKey, TPayload> batch)
        {
            this.pool.Get(out StreamMessage<TKey, TResult> outputBatch);

            var count = batch.Count;
            outputBatch.vsync = batch.vsync;
            outputBatch.vother = batch.vother;
            outputBatch.key = batch.key;
            outputBatch.hash = batch.hash;
            outputBatch.iter = batch.iter;
            this.pool.GetPayload(out outputBatch.payload);
            outputBatch.bitvector = batch.bitvector;

            var dest = outputBatch.payload.col;
            var src = batch.payload.col;
            var vsync = batch.vsync.col;
            var vother = batch.vother.col;
            var srckey = batch.key.col;
            fixed (long* bv = batch.bitvector.col)
            {
                for (int i = 0; i < count; i++)
                {
                    if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                        dest[i] = this.selectorFunc(vsync[i] < vother[i] ? vsync[i] : vother[i], srckey[i], src[i]);
                }
            }
            outputBatch.Count = count;
            batch.payload.Return();
            batch.Return();
            this.Observer.OnNext(outputBatch);
        }

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }
}