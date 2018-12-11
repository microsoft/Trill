// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class StreamEventEgressPipe<TPayload> : EgressBoundary<Empty, TPayload, StreamEvent<TPayload>>
    {

        [Obsolete("Used only by serialization. Do not call directly.")]
        public StreamEventEgressPipe() { }

        public StreamEventEgressPipe(
            IObserver<StreamEvent<TPayload>> observer,
            QueryContainer container)
            : base(observer, container)
        {
        }

        public override void OnNext(StreamMessage<Empty, TPayload> batch)
        {
            var col_bv = batch.bitvector.col;
            var col_vsync = batch.vsync.col;
            var col_vother = batch.vother.col;

            for (int i = 0; i < batch.Count; i++)
            {
                if ((col_bv[i >> 6] & (1L << (i & 0x3f))) != 0 && col_vother[i] >= 0)
                    continue;
                else if (col_vother[i] == StreamEvent.PunctuationOtherTime)
                    this.observer.OnNext(StreamEvent.CreatePunctuation<TPayload>(col_vsync[i]));
                else
                    this.observer.OnNext(new StreamEvent<TPayload>(col_vsync[i], col_vother[i], col_vother[i] >= 0 ? batch[i] : default));
            }
            batch.Free();
        }

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }
    [DataContract]
    internal sealed class StartEdgeEgressPipe<TPayload, TResult> : EgressBoundary<Empty, TPayload, TResult>
    {
        private readonly Func<long, TPayload, TResult> constructor;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public StartEdgeEgressPipe() { }

        public StartEdgeEgressPipe(
                 Expression<Func<long, TPayload, TResult>> constructor,
            IObserver<TResult> observer,
            QueryContainer container)
            : base(observer, container)
        {
            this.constructor = constructor.Compile();
        }

        public override void OnNext(StreamMessage<Empty, TPayload> batch)
        {
            var col_bv = batch.bitvector.col;
            var col_vsync = batch.vsync.col;
            var col_vother = batch.vother.col;

            for (int i = 0; i < batch.Count; i++)
            {
                if ((col_bv[i >> 6] & (1L << (i & 0x3f))) != 0)
                    continue;
                else if (col_vother[i] == StreamEvent.InfinitySyncTime)
                    this.observer.OnNext(this.constructor(col_vsync[i], batch[i]));
                else
                    throw new StreamProcessingException("The query has encountered either an end edge or an interval, while the egress point expects only start edges.");
            }
            batch.Free();
        }

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }
    [DataContract]
    internal sealed class IntervalEgressPipe<TPayload, TResult> : EgressBoundary<Empty, TPayload, TResult>
    {
        private readonly Func<long, long, TPayload, TResult> constructor;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public IntervalEgressPipe() { }

        public IntervalEgressPipe(
                 Expression<Func<long, long, TPayload, TResult>> constructor,
            IObserver<TResult> observer,
            QueryContainer container)
            : base(observer, container)
        {
            this.constructor = constructor.Compile();
        }

        public override void OnNext(StreamMessage<Empty, TPayload> batch)
        {
            var col_bv = batch.bitvector.col;
            var col_vsync = batch.vsync.col;
            var col_vother = batch.vother.col;

            for (int i = 0; i < batch.Count; i++)
            {
                if ((col_bv[i >> 6] & (1L << (i & 0x3f))) != 0)
                    continue;
                else
                    this.observer.OnNext(this.constructor(col_vsync[i], col_vother[i], batch[i]));
            }
            batch.Free();
        }

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }
    [DataContract]
    internal sealed class PartitionedStreamEventEgressPipe<TKey, TPayload> : EgressBoundary<PartitionKey<TKey>, TPayload, PartitionedStreamEvent<TKey, TPayload>>
    {

        [Obsolete("Used only by serialization. Do not call directly.")]
        public PartitionedStreamEventEgressPipe() { }

        public PartitionedStreamEventEgressPipe(
            IObserver<PartitionedStreamEvent<TKey, TPayload>> observer,
            QueryContainer container)
            : base(observer, container)
        {
        }

        public override void OnNext(StreamMessage<PartitionKey<TKey>, TPayload> batch)
        {
            var colkey = batch.key.col;
            var col_bv = batch.bitvector.col;
            var col_vsync = batch.vsync.col;
            var col_vother = batch.vother.col;

            for (int i = 0; i < batch.Count; i++)
            {
                if ((col_bv[i >> 6] & (1L << (i & 0x3f))) != 0 && col_vother[i] >= 0)
                    continue;
                else if (col_vother[i] == StreamEvent.PunctuationOtherTime)
                    this.observer.OnNext(PartitionedStreamEvent.CreatePunctuation<TKey, TPayload>(colkey[i].Key, col_vsync[i]));
                else if (col_vother[i] == PartitionedStreamEvent.LowWatermarkOtherTime)
                    this.observer.OnNext(PartitionedStreamEvent.CreateLowWatermark<TKey, TPayload>(col_vsync[i]));
                else
                    this.observer.OnNext(new PartitionedStreamEvent<TKey, TPayload>(colkey[i].Key, col_vsync[i], col_vother[i], col_vother[i] >= 0 ? batch[i] : default));
            }
            batch.Free();
        }

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }
    [DataContract]
    internal sealed class PartitionedStartEdgeEgressPipe<TKey, TPayload, TResult> : EgressBoundary<PartitionKey<TKey>, TPayload, TResult>
    {
        private readonly Func<TKey, long, TPayload, TResult> constructor;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public PartitionedStartEdgeEgressPipe() { }

        public PartitionedStartEdgeEgressPipe(
                 Expression<Func<TKey, long, TPayload, TResult>> constructor,
            IObserver<TResult> observer,
            QueryContainer container)
            : base(observer, container)
        {
            this.constructor = constructor.Compile();
        }

        public override void OnNext(StreamMessage<PartitionKey<TKey>, TPayload> batch)
        {
            var colkey = batch.key.col;
            var col_bv = batch.bitvector.col;
            var col_vsync = batch.vsync.col;
            var col_vother = batch.vother.col;

            for (int i = 0; i < batch.Count; i++)
            {
                if ((col_bv[i >> 6] & (1L << (i & 0x3f))) != 0)
                    continue;
                else if (col_vother[i] == StreamEvent.InfinitySyncTime)
                    this.observer.OnNext(this.constructor(colkey[i].Key, col_vsync[i], batch[i]));
                else
                    throw new StreamProcessingException("The query has encountered either an end edge or an interval, while the egress point expects only start edges.");
            }
            batch.Free();
        }

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }
    [DataContract]
    internal sealed class PartitionedIntervalEgressPipe<TKey, TPayload, TResult> : EgressBoundary<PartitionKey<TKey>, TPayload, TResult>
    {
        private readonly Func<TKey, long, long, TPayload, TResult> constructor;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public PartitionedIntervalEgressPipe() { }

        public PartitionedIntervalEgressPipe(
                 Expression<Func<TKey, long, long, TPayload, TResult>> constructor,
            IObserver<TResult> observer,
            QueryContainer container)
            : base(observer, container)
        {
            this.constructor = constructor.Compile();
        }

        public override void OnNext(StreamMessage<PartitionKey<TKey>, TPayload> batch)
        {
            var colkey = batch.key.col;
            var col_bv = batch.bitvector.col;
            var col_vsync = batch.vsync.col;
            var col_vother = batch.vother.col;

            for (int i = 0; i < batch.Count; i++)
            {
                if ((col_bv[i >> 6] & (1L << (i & 0x3f))) != 0)
                    continue;
                else
                    this.observer.OnNext(this.constructor(colkey[i].Key, col_vsync[i], col_vother[i], batch[i]));
            }
            batch.Free();
        }

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }
}