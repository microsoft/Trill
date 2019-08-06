// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Runtime.CompilerServices;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// For Advanced users. Replaces the Trill ingress with a custom ingress implementation.
    /// This allows ingress to bypass the creation of StreamEvent objects, and any unwanted policies.
    /// The output of the ingress operator should still be strictly in-order, or results may be undefined.
    /// </summary>
    /// <typeparam name="TSource"></typeparam>
    /// <typeparam name="TPayload"></typeparam>
    public sealed class CustomIngressStreamable<TSource, TPayload> : Streamable<Empty, TPayload>, IIngressStreamable<Empty, TPayload>
    {
        private readonly IObservable<TSource> source;
        private readonly QueryContainer queryContainer;
        private readonly MemoryPool<Empty, TPayload> pool;
        private readonly Func<CustomIngressStreamable<TSource, TPayload>, IStreamObserver<Empty, TPayload>, CustomIngressPipe> pipeCreator;

        /// <summary>
        /// Creates a custom ingress streamable
        /// </summary>
        /// <param name="source">Source observable that will be the provider for this streamable's pipe(s)</param>
        /// <param name="pipeCreator">Creates an instance of a <see cref="CustomIngressPipe"/></param>
        /// <param name="identifier">Identifier string for the streamable and its pipe(s)</param>
        /// <param name="queryContainer">Optional query container to register the ingress pipe(s)</param>
        public CustomIngressStreamable(
            IObservable<TSource> source,
            Func<CustomIngressStreamable<TSource, TPayload>, IStreamObserver<Empty, TPayload>, CustomIngressPipe> pipeCreator,
            QueryContainer queryContainer = null,
            string identifier = null)
            : base(StreamProperties())
        {
            this.source = source;
            this.pipeCreator = pipeCreator;
            this.queryContainer = queryContainer;
            this.IngressSiteIdentifier = identifier ?? $"{nameof(CustomIngressStreamable<object, object>)}.{Guid.NewGuid()}";
            this.pool = MemoryManager.GetMemoryPool<Empty, TPayload>();
        }

        /// <summary>
        /// Streamable identifier
        /// </summary>
        public string IngressSiteIdentifier { get; }

        private static StreamProperties<Empty, TPayload> StreamProperties()
        {
            // TODO: SetQueryContainer? or leave to implementer?
            var props = StreamProperties<Empty, TPayload>.Default;
            props.IsColumnar = false;
            return props;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="observer"></param>
        /// <returns></returns>
        public override sealed IDisposable Subscribe(IStreamObserver<Empty, TPayload> observer)
        {
            var pipe = this.pipeCreator.Invoke(this, observer);
            this.queryContainer?.RegisterIngressPipe(this.IngressSiteIdentifier, pipe);
            return Utility.CreateDisposable(this.source.Subscribe(pipe));
        }

        /// <summary>
        /// Abstract pipe implementation. Implementers need to override OnNext, and should call the Add* APIs to populate the current batch,
        /// and optionally FlushContents to flush.
        /// </summary>
        public abstract class CustomIngressPipe : Pipe<Empty, TPayload>, IObserver<TSource>, IIngressStreamObserver
        {
            private readonly MemoryPool<Empty, TPayload> pool;
            private long currentTime;
            private bool punctuationGeneratedAtCurrentTime;
            private StreamMessage<Empty, TPayload> currentBatch;

            /// <summary/>
            /// <param name="streamable"/>
            /// <param name="observer"/>
            public CustomIngressPipe(
                CustomIngressStreamable<TSource, TPayload> streamable,
                IStreamObserver<Empty, TPayload> observer)
                : base(streamable, observer)
            {
                this.IngressSiteIdentifier = streamable.IngressSiteIdentifier;
                this.pool = streamable.pool;
                this.pool.Get(out this.currentBatch);
                this.currentBatch.Allocate();
            }

            /// <summary>
            /// Number of output events currently buffered.
            /// </summary>
            public override sealed int CurrentlyBufferedOutputCount => this.currentBatch.Count;

            /// <summary>
            /// Number of input events currently buffered. Implementers should override if any input is buffered.
            /// </summary>
            public override int CurrentlyBufferedInputCount => 0;

            /// <summary>
            /// Currently for internal use only - do not use directly. Implementers should override CurrentlyBufferedInputCount if buffering input.
            /// </summary>
            public int CurrentlyBufferedReorderCount => 0;

            /// <summary>
            /// Currently for internal use only - do not use directly. Implementers should override CurrentlyBufferedInputCount if buffering input.
            /// </summary>
            public int CurrentlyBufferedStartEdgeCount => 0;

            /// <summary>
            /// Streamable identifier
            /// </summary>
            public string IngressSiteIdentifier { get; }

            /// <summary>
            /// Delayed subscriptions not supported
            /// </summary>
            public IDisposable DelayedDisposable => throw new NotImplementedException();

            /// <summary>
            /// Delayed subscriptions not supported
            /// </summary>
            public void Enable() => throw new NotImplementedException();

            /// <summary>
            /// Get a query plan of the actively running query rooted at this query node.
            /// </summary>
            /// <param name="previous">The previous node in the query plan.</param>
            /// <returns>The query plan node representing the current query artifact.</returns>
            public override void ProduceQueryPlan(PlanNode previous) =>
                this.Observer.ProduceQueryPlan(
                    new IngressPlanNode(
                        observer: this,
                        keyType: typeof(Empty),
                        payloadType: typeof(TPayload),
                        isGenerated: false,
                        compileErrors: null));

            /// <summary>
            /// Disposes any state associated with this ingress pipe. Implementers should call this base implementation if overriding.
            /// </summary>
            protected override void DisposeState() => this.currentBatch.Free();

            /// <summary>
            /// Flushes any buffered state. Implementers should call this base implementation if overriding.
            /// </summary>
            public override void OnFlush()
            {
                FlushContents();
                this.Observer.OnFlush();
            }

            /// <summary>
            /// Implementers should call the Add* APIs to populate the current batch, and optionally FlushContents to flush.
            /// </summary>
            /// <param name="value">Source value</param>
            public abstract void OnNext(TSource value);

            /// <summary>
            /// Adds a punctuation to the buffered output, and flushes if the batch is full.
            /// </summary>
            /// <param name="punctuationTime">Time of punctuation</param>
            protected void AddPunctuation(long punctuationTime)
            {
                ValidateAndUpdatePunctuationTime(punctuationTime);
                if (this.currentTime < punctuationTime || !this.punctuationGeneratedAtCurrentTime)
                {
                    this.currentTime = punctuationTime;
                    this.punctuationGeneratedAtCurrentTime = true;
                    if (this.currentBatch.AddPunctuation(punctuationTime))
                    {
                        FlushContents();
                    }
                }
            }

            /// <summary>
            /// Adds an interval to the buffered output, and flushes if the batch is full.
            /// </summary>
            /// <param name="startTime"></param>
            /// <param name="endTime"></param>
            /// <param name="payload"></param>
            protected void AddInterval(long startTime, long endTime, TPayload payload)
            {
                ValidateAndUpdateInputTime(startTime);
                if (this.currentBatch.Add(vsync: startTime, vother: endTime, key: default, payload: payload))
                {
                    FlushContents();
                }
            }

            /// <summary>
            /// Adds a start edge to the buffered output, and flushes if the batch is full.
            /// </summary>
            /// <param name="startTime"></param>
            /// <param name="payload"></param>
            protected void AddStartEdge(long startTime, TPayload payload)
            {
                ValidateAndUpdateInputTime(startTime);
                if (this.currentBatch.Add(vsync: startTime, vother: StreamEvent.InfinitySyncTime, key: default, payload: payload))
                {
                    FlushContents();
                }
            }

            /// <summary>
            /// Adds an end edge to the buffered output, and flushes if the batch is full.
            /// </summary>
            /// <param name="endTime"></param>
            /// <param name="originalStartTime">Original start time specified in the corresponding <see cref="AddStartEdge(long, TPayload)"/> call</param>
            /// <param name="payload">Original payload specified in the corresponding <see cref="AddStartEdge(long, TPayload)"/> call</param>
            protected void AddEndEdge(long endTime, long originalStartTime, TPayload payload)
            {
                ValidateAndUpdateInputTime(endTime);
                if (this.currentBatch.Add(vsync: endTime, vother: originalStartTime, key: default, payload: payload))
                {
                    FlushContents();
                }
            }

            /// <summary>
            /// Flushes the currently batched output
            /// </summary>
            protected override sealed void FlushContents()
            {
                if (this.currentBatch.Count != 0)
                {
                    this.currentBatch.Seal();
                    this.Observer.OnNext(this.currentBatch);
                    this.pool.Get(out this.currentBatch);
                    this.currentBatch.Allocate();
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private bool ValidateAndUpdatePunctuationTime(long time)
            {
                if (time < this.currentTime)
                {
                    throw new StreamProcessingOutOfOrderException($"Custom ingress operator produced out of order punctuation at time {time}, current time {this.currentTime}");
                }

                if (time > this.currentTime || !this.punctuationGeneratedAtCurrentTime)
                {
                    this.punctuationGeneratedAtCurrentTime = true;
                    this.currentTime = time;
                    return true;
                }

                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void ValidateAndUpdateInputTime(long time)
            {
                if (time < this.currentTime)
                {
                    throw new StreamProcessingOutOfOrderException($"Custom ingress operator produced out of order event at time {time}, current time {this.currentTime}");
                }

                if (time > this.currentTime)
                {
                    this.currentTime = time;
                    this.punctuationGeneratedAtCurrentTime = false;
                }
            }
        }
    }

    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TSource"></typeparam>
    /// <typeparam name="TPayload"></typeparam>
    public sealed class PartitionedCustomIngressStreamable<TSource, TKey, TPayload> : Streamable<PartitionKey<TKey>, TPayload>, IIngressStreamable<PartitionKey<TKey>, TPayload>
    {
        private readonly IObservable<TSource> source;
        private readonly QueryContainer queryContainer;
        private readonly MemoryPool<PartitionKey<TKey>, TPayload> pool;
        private readonly Func<PartitionedCustomIngressStreamable<TSource, TKey, TPayload>, IStreamObserver<PartitionKey<TKey>, TPayload>, CustomIngressPipe> pipeCreator;

        /// <summary>
        ///
        /// </summary>
        /// <param name="source"></param>
        /// <param name="pipeCreator"></param>
        /// <param name="queryContainer"></param>
        /// <param name="identifier"></param>
        public PartitionedCustomIngressStreamable(
            IObservable<TSource> source,
            Func<PartitionedCustomIngressStreamable<TSource, TKey, TPayload>, IStreamObserver<PartitionKey<TKey>, TPayload>, CustomIngressPipe> pipeCreator,
            QueryContainer queryContainer = null,
            string identifier = null)
            : base(StreamProperties())
        {
            this.source = source;
            this.pipeCreator = pipeCreator;
            this.queryContainer = queryContainer;
            this.IngressSiteIdentifier = identifier ?? $"{nameof(PartitionedCustomIngressStreamable<object, object, object>)}.{Guid.NewGuid()}";
            this.pool = MemoryManager.GetMemoryPool<PartitionKey<TKey>, TPayload>();
        }

        private static StreamProperties<PartitionKey<TKey>, TPayload> StreamProperties()
        {
            // TODO: SetQueryContainer? or leave to implementer?
            var props = StreamProperties<PartitionKey<TKey>, TPayload>.Default;
            props.IsColumnar = false;
            return props;
        }

        /// <summary>
        ///
        /// </summary>
        public string IngressSiteIdentifier { get; }

        /// <summary>
        ///
        /// </summary>
        /// <param name="observer"></param>
        /// <returns></returns>
        public override IDisposable Subscribe(IStreamObserver<PartitionKey<TKey>, TPayload> observer)
        {
            var pipe = this.pipeCreator.Invoke(this, observer);
            this.queryContainer?.RegisterIngressPipe(this.IngressSiteIdentifier, pipe);
            return Utility.CreateDisposable(this.source.Subscribe(pipe));
        }

        /// <summary>
        ///
        /// </summary>
        public abstract class CustomIngressPipe : Pipe<PartitionKey<TKey>, TPayload>, IObserver<TSource>, IIngressStreamObserver
        {
            private readonly MemoryPool<PartitionKey<TKey>, TPayload> pool;
            private long currentTime;
            private bool lowWatermarkGeneratedAtCurrentTime;
            private StreamMessage<PartitionKey<TKey>, TPayload> currentBatch;

            /// <summary/>
            /// <param name="streamable"/>
            /// <param name="observer"/>
            public CustomIngressPipe(
                PartitionedCustomIngressStreamable<TSource, TKey, TPayload> streamable,
                IStreamObserver<PartitionKey<TKey>, TPayload> observer)
                : base(streamable, observer)
            {
                this.IngressSiteIdentifier = streamable.IngressSiteIdentifier;
                this.pool = streamable.pool;
                this.pool.Get(out this.currentBatch);
                this.currentBatch.Allocate();
            }

            /// <summary>
            /// Number of output events currently buffered.
            /// </summary>
            public override sealed int CurrentlyBufferedOutputCount => this.currentBatch.Count;

            /// <summary>
            /// Number of input events currently buffered. Implementers should override if any input is buffered.
            /// </summary>
            public override int CurrentlyBufferedInputCount => 0;

            /// <summary>
            /// Currently for internal use only - do not use directly. Implementers should override CurrentlyBufferedInputCount if buffering input.
            /// </summary>
            public int CurrentlyBufferedReorderCount => 0;

            /// <summary>
            /// Currently for internal use only - do not use directly. Implementers should override CurrentlyBufferedInputCount if buffering input.
            /// </summary>
            public int CurrentlyBufferedStartEdgeCount => 0;

            /// <summary>
            /// Streamable identifier
            /// </summary>
            public string IngressSiteIdentifier { get; }

            /// <summary>
            /// Delayed subscriptions not supported
            /// </summary>
            public IDisposable DelayedDisposable => throw new NotImplementedException();

            /// <summary>
            /// Delayed subscriptions not supported
            /// </summary>
            public void Enable() => throw new NotImplementedException();

            /// <summary>
            /// Get a query plan of the actively running query rooted at this query node.
            /// </summary>
            /// <param name="previous">The previous node in the query plan.</param>
            /// <returns>The query plan node representing the current query artifact.</returns>
            public override void ProduceQueryPlan(PlanNode previous) =>
                this.Observer.ProduceQueryPlan(
                    new IngressPlanNode(
                        observer: this,
                        keyType: typeof(PartitionKey<TKey>),
                        payloadType: typeof(TPayload),
                        isGenerated: false,
                        compileErrors: null));

            /// <summary>
            /// Implementers should call the Add* APIs to populate the current batch, and optionally FlushContents to flush.
            /// </summary>
            /// <param name="value">Source value</param>
            public abstract void OnNext(TSource value);

            /// <summary>
            /// Adds a punctuation to the buffered output, and flushes if the batch is full.
            /// </summary>
            /// <param name="key">Partition key</param>
            /// <param name="punctuationTime">Time of punctuation</param>
            protected void AddPunctuation(TKey key, long punctuationTime)
            {
                ValidateAndUpdateInputTime(punctuationTime);
                if (this.currentBatch.Add(
                    vsync: punctuationTime,
                    vother: StreamEvent.PunctuationOtherTime,
                    key: new PartitionKey<TKey>(key),
                    payload: default))
                {
                    FlushContents();
                }
            }

            /// <summary>
            /// Adds a punctuation to the buffered output, and flushes if the batch is full.
            /// </summary>
            /// <param name="lowWatermark">Time of punctuation</param>
            protected void AddLowWatermark(long lowWatermark)
            {
                ValidateAndUpdateLowWatermarkTime(lowWatermark);
                if (this.currentTime < lowWatermark || !this.lowWatermarkGeneratedAtCurrentTime)
                {
                    this.currentTime = lowWatermark;
                    this.lowWatermarkGeneratedAtCurrentTime = true;
                    if (this.currentBatch.AddLowWatermark(lowWatermark))
                    {
                        FlushContents();
                    }
                }
            }

            /// <summary>
            /// Adds an interval to the buffered output, and flushes if the batch is full.
            /// </summary>
            /// <param name="key"></param>
            /// <param name="startTime"></param>
            /// <param name="endTime"></param>
            /// <param name="payload"></param>
            protected void AddInterval(TKey key, long startTime, long endTime, TPayload payload)
            {
                ValidateAndUpdateInputTime(startTime);
                if (this.currentBatch.Add(vsync: startTime, vother: endTime, key: new PartitionKey<TKey>(key), payload: payload))
                {
                    FlushContents();
                }
            }

            /// <summary>
            /// Adds a start edge to the buffered output, and flushes if the batch is full.
            /// </summary>
            /// <param name="key"></param>
            /// <param name="startTime"></param>
            /// <param name="payload"></param>
            protected void AddStartEdge(TKey key, long startTime, TPayload payload)
            {
                ValidateAndUpdateInputTime(startTime);
                if (this.currentBatch.Add(vsync: startTime, vother: StreamEvent.InfinitySyncTime, key: new PartitionKey<TKey>(key), payload: payload))
                {
                    FlushContents();
                }
            }

            /// <summary>
            /// Adds an end edge to the buffered output, and flushes if the batch is full.
            /// </summary>
            /// <param name="key"></param>
            /// <param name="endTime"></param>
            /// <param name="originalStartTime">Original start time specified in the corresponding <see cref="AddStartEdge(TKey, long, TPayload)"/> call</param>
            /// <param name="payload">Original payload specified in the corresponding <see cref="AddStartEdge(TKey, long, TPayload)"/> call</param>
            protected void AddEndEdge(TKey key, long endTime, long originalStartTime, TPayload payload)
            {
                ValidateAndUpdateInputTime(endTime);
                if (this.currentBatch.Add(vsync: endTime, vother: originalStartTime, key: new PartitionKey<TKey>(key), payload: payload))
                {
                    FlushContents();
                }
            }

            /// <summary>
            /// Flushes the currently batched output
            /// </summary>
            protected override sealed void FlushContents()
            {
                if (this.currentBatch.Count != 0)
                {
                    this.currentBatch.Seal();
                    this.Observer.OnNext(this.currentBatch);
                    this.pool.Get(out this.currentBatch);
                    this.currentBatch.Allocate();
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private bool ValidateAndUpdateLowWatermarkTime(long time)
            {
                if (time < this.currentTime)
                {
                    throw new StreamProcessingOutOfOrderException($"Custom ingress operator produced out of order punctuation at time {time}, current time {this.currentTime}");
                }

                if (time > this.currentTime || !this.lowWatermarkGeneratedAtCurrentTime)
                {
                    this.lowWatermarkGeneratedAtCurrentTime = true;
                    this.currentTime = time;
                    return true;
                }

                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void ValidateAndUpdateInputTime(long time)
            {
                if (time < this.currentTime)
                {
                    throw new StreamProcessingOutOfOrderException($"Custom ingress operator produced out of order event at time {time}, current time {this.currentTime}");
                }

                if (time > this.currentTime)
                {
                    this.currentTime = time;
                    this.lowWatermarkGeneratedAtCurrentTime = false;
                }
            }
        }
    }
}
