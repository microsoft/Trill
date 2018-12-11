// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    public static partial class Streamable
    {
        /// <summary>
        /// Converts a sequence of StreamEvents to an IStreamable.
        /// The completion policy specifies what to do when the resulting stream completes.
        /// The disorder policy specifies what to do with out of order events.
        /// The punctuation policy specifies whether and how punctuations are created and injected
        /// into the resulting stream. Since punctuations force output, this exposes a throughput/latency tradeoff.
        /// </summary>
        /// <typeparam name="TPayload">The type of data for the stream.</typeparam>
        /// <param name="streamEvents">A sequence of stream events created by the client.</param>
        /// <param name="disorderPolicy">How to handle events that are not in time order.</param>
        /// <param name="flushPolicy">When to flush batched output events.</param>
        /// <param name="periodicPunctuationPolicy">Whether to add periodic punctuations to the resulting stream.</param>
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        /// <exception cref="IngressException">
        /// Throws an exception if the <paramref name="disorderPolicy"/> is to throw and
        /// an out-of-order stream event is encountered.
        /// Also, an exception is thrown if any payload is null.
        /// </exception>
        public static IObservableIngressStreamable<TPayload> ToStreamable<TPayload>(
            this IObservable<StreamEvent<TPayload>> streamEvents,
            DisorderPolicy disorderPolicy = null,
            FlushPolicy flushPolicy = FlushPolicy.FlushOnPunctuation,
            PeriodicPunctuationPolicy periodicPunctuationPolicy = null,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.EndOfStream)
        {
            return ToCheckpointableStreamable(
                streamEvents,
                null,
                Guid.NewGuid().ToString(),
                disorderPolicy,
                flushPolicy,
                periodicPunctuationPolicy,
                onCompletedPolicy);
        }

        /// <summary>
        /// Converts a sequence of StreamEvents to an IStreamable.
        /// The completion policy specifies what to do when the resulting stream completes.
        /// The disorder policy specifies what to do with out of order events.
        /// The punctuation policy specifies whether and how punctuations are created and injected
        /// into the resulting stream. Since punctuations force output, this exposes a throughput/latency tradeoff.
        /// </summary>
        /// <param name="container">The query container to which to register the ingress point.</param>
        /// <typeparam name="TPayload">The type of data for the stream.</typeparam>
        /// <param name="streamEvents">A sequence of stream events created by the client.</param>
        /// <param name="disorderPolicy">How to handle events that are not in time order.</param>
        /// <param name="flushPolicy">When to flush batched output events.</param>
        /// <param name="periodicPunctuationPolicy">Whether to add periodic punctuations to the resulting stream.</param>
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <param name="identifier">If provided, a unique name to identify to point of ingress in the query.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        /// <exception cref="IngressException">
        /// Throws an exception if the <paramref name="disorderPolicy"/> is to throw and
        /// an out-of-order stream event is encountered.
        /// Also, an exception is thrown if any payload is null.
        /// </exception>
        public static IObservableIngressStreamable<TPayload> RegisterInput<TPayload>(
            this QueryContainer container,
            IObservable<StreamEvent<TPayload>> streamEvents,
            DisorderPolicy disorderPolicy = null,
            FlushPolicy flushPolicy = FlushPolicy.FlushOnPunctuation,
            PeriodicPunctuationPolicy periodicPunctuationPolicy = null,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.EndOfStream,
            string identifier = null)
        {
            return ToCheckpointableStreamable(
                streamEvents,
                container,
                identifier ?? Guid.NewGuid().ToString(),
                disorderPolicy,
                flushPolicy,
                periodicPunctuationPolicy,
                onCompletedPolicy);
        }

        internal static IObservableIngressStreamable<TPayload> ToCheckpointableStreamable<TPayload>(
            this IObservable<StreamEvent<TPayload>> streamEvents,
            QueryContainer container,
            string identifier,
            DisorderPolicy disorderPolicy,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy periodicPunctuationPolicy,
            OnCompletedPolicy onCompletedPolicy)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            if (disorderPolicy == null)
                disorderPolicy = DisorderPolicy.Throw();

            if (periodicPunctuationPolicy == null)
                periodicPunctuationPolicy = PeriodicPunctuationPolicy.None();

            var a = new StreamEventIngressStreamable<TPayload>(
                streamEvents,
                disorderPolicy,
                flushPolicy,
                periodicPunctuationPolicy,
                onCompletedPolicy,
                container,
                identifier);

            return a;
        }

        /// <summary>
        /// Converts a sequence of data elements to an IStreamable, start-edge only.
        /// The completion policy specifies what to do when the resulting stream completes.
        /// The disorder policy specifies what to do with out of order events.
        /// The punctuation policy specifies whether and how punctuations are created and injected
        /// into the resulting stream. Since punctuations force output, this exposes a throughput/latency tradeoff.
        /// </summary>
        /// <typeparam name="TPayload">The type of data for the stream.</typeparam>
        /// <param name="streamEvents">A sequence of stream events created by the client.</param>
        /// <param name="startEdgeExtractor">An expresion that describes how to interpret the start time for each data value.</param>
        /// <param name="disorderPolicy">How to handle events that are not in time order.</param>
        /// <param name="flushPolicy">When to flush batched output events.</param>
        /// <param name="periodicPunctuationPolicy">Whether to add periodic punctuations to the resulting stream.</param>
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        /// <exception cref="IngressException">
        /// Throws an exception if the <paramref name="disorderPolicy"/> is to throw and
        /// an out-of-order stream event is encountered.
        /// Also, an exception is thrown if any payload is null.
        /// </exception>
        public static IObservableIngressStreamable<TPayload> ToTemporalStreamable<TPayload>(
            this IObservable<TPayload> streamEvents,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            DisorderPolicy disorderPolicy = null,
            FlushPolicy flushPolicy = FlushPolicy.FlushOnPunctuation,
            PeriodicPunctuationPolicy periodicPunctuationPolicy = null,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.EndOfStream)
        {
            return ToCheckpointableTemporalStreamable(
                streamEvents,
                startEdgeExtractor,
                null,
                Guid.NewGuid().ToString(),
                disorderPolicy,
                flushPolicy,
                periodicPunctuationPolicy,
                onCompletedPolicy);
        }

        /// <summary>
        /// Converts a sequence of data elements to an IStreamable, start-edge only.
        /// The completion policy specifies what to do when the resulting stream completes.
        /// The disorder policy specifies what to do with out of order events.
        /// The punctuation policy specifies whether and how punctuations are created and injected
        /// into the resulting stream. Since punctuations force output, this exposes a throughput/latency tradeoff.
        /// </summary>
        /// <param name="container">The query container to which to register the ingress point.</param>
        /// <typeparam name="TPayload">The type of data for the stream.</typeparam>
        /// <param name="streamEvents">A sequence of stream events created by the client.</param>
        /// <param name="startEdgeExtractor">An expresion that describes how to interpret the start time for each data value.</param>
        /// <param name="disorderPolicy">How to handle events that are not in time order.</param>
        /// <param name="flushPolicy">When to flush batched output events.</param>
        /// <param name="periodicPunctuationPolicy">Whether to add periodic punctuations to the resulting stream.</param>
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <param name="identifier">If provided, a unique name to identify to point of ingress in the query.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        /// <exception cref="IngressException">
        /// Throws an exception if the <paramref name="disorderPolicy"/> is to throw and
        /// an out-of-order stream event is encountered.
        /// Also, an exception is thrown if any payload is null.
        /// </exception>
        public static IObservableIngressStreamable<TPayload> RegisterTemporalInput<TPayload>(
            this QueryContainer container,
            IObservable<TPayload> streamEvents,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            DisorderPolicy disorderPolicy = null,
            FlushPolicy flushPolicy = FlushPolicy.FlushOnPunctuation,
            PeriodicPunctuationPolicy periodicPunctuationPolicy = null,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.EndOfStream,
            string identifier = null)
        {
            return ToCheckpointableTemporalStreamable(
                streamEvents,
                startEdgeExtractor,
                container,
                identifier ?? Guid.NewGuid().ToString(),
                disorderPolicy,
                flushPolicy,
                periodicPunctuationPolicy,
                onCompletedPolicy);
        }

        internal static IObservableIngressStreamable<TPayload> ToCheckpointableTemporalStreamable<TPayload>(
            this IObservable<TPayload> streamEvents,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            QueryContainer container,
            string identifier,
            DisorderPolicy disorderPolicy,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy periodicPunctuationPolicy,
            OnCompletedPolicy onCompletedPolicy)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            if (disorderPolicy == null)
                disorderPolicy = DisorderPolicy.Throw();

            if (periodicPunctuationPolicy == null)
                periodicPunctuationPolicy = PeriodicPunctuationPolicy.None();

            var a = new IntervalIngressStreamable<TPayload>(
                streamEvents,
                startEdgeExtractor,
                (p) => StreamEvent.InfinitySyncTime,
                disorderPolicy,
                flushPolicy,
                periodicPunctuationPolicy,
                onCompletedPolicy,
                container,
                identifier);

            return a;
        }

        /// <summary>
        /// Converts a sequence of data elements to an IStreamable, intervals and start-edges only.
        /// The completion policy specifies what to do when the resulting stream completes.
        /// The disorder policy specifies what to do with out of order events.
        /// The punctuation policy specifies whether and how punctuations are created and injected
        /// into the resulting stream. Since punctuations force output, this exposes a throughput/latency tradeoff.
        /// </summary>
        /// <typeparam name="TPayload">The type of data for the stream.</typeparam>
        /// <param name="streamEvents">A sequence of stream events created by the client.</param>
        /// <param name="startEdgeExtractor">An expresion that describes how to interpret the start time for each data value.</param>
        /// <param name="endEdgeExtractor">An expresion that describes how to interpret the end time for each data value.  Return StreamEvent.InfinitySyncTime to indicate an event with no end time.</param>
        /// <param name="disorderPolicy">How to handle events that are not in time order.</param>
        /// <param name="flushPolicy">When to flush batched output events.</param>
        /// <param name="periodicPunctuationPolicy">Whether to add periodic punctuations to the resulting stream.</param>
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        /// <exception cref="IngressException">
        /// Throws an exception if the <paramref name="disorderPolicy"/> is to throw and
        /// an out-of-order stream event is encountered.
        /// Also, an exception is thrown if any payload is null.
        /// </exception>
        public static IObservableIngressStreamable<TPayload> ToTemporalStreamable<TPayload>(
            this IObservable<TPayload> streamEvents,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            DisorderPolicy disorderPolicy = null,
            FlushPolicy flushPolicy = FlushPolicy.FlushOnPunctuation,
            PeriodicPunctuationPolicy periodicPunctuationPolicy = null,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.EndOfStream)
        {
            return ToCheckpointableTemporalStreamable(
                streamEvents,
                startEdgeExtractor,
                endEdgeExtractor,
                null,
                Guid.NewGuid().ToString(),
                disorderPolicy,
                flushPolicy,
                periodicPunctuationPolicy,
                onCompletedPolicy);
        }

        /// <summary>
        /// Converts a sequence of data elements to an IStreamable, intervals and start-edges only.
        /// The completion policy specifies what to do when the resulting stream completes.
        /// The disorder policy specifies what to do with out of order events.
        /// The punctuation policy specifies whether and how punctuations are created and injected
        /// into the resulting stream. Since punctuations force output, this exposes a throughput/latency tradeoff.
        /// </summary>
        /// <param name="container">The query container to which to register the ingress point.</param>
        /// <typeparam name="TPayload">The type of data for the stream.</typeparam>
        /// <param name="streamEvents">A sequence of stream events created by the client.</param>
        /// <param name="startEdgeExtractor">An expresion that describes how to interpret the start time for each data value.</param>
        /// <param name="endEdgeExtractor">An expresion that describes how to interpret the end time for each data value.  Return StreamEvent.InfinitySyncTime to indicate an event with no end time.</param>
        /// <param name="disorderPolicy">How to handle events that are not in time order.</param>
        /// <param name="flushPolicy">When to flush batched output events.</param>
        /// <param name="periodicPunctuationPolicy">Whether to add periodic punctuations to the resulting stream.</param>
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <param name="identifier">If provided, a unique name to identify to point of ingress in the query.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        /// <exception cref="IngressException">
        /// Throws an exception if the <paramref name="disorderPolicy"/> is to throw and
        /// an out-of-order stream event is encountered.
        /// Also, an exception is thrown if any payload is null.
        /// </exception>
        public static IObservableIngressStreamable<TPayload> RegisterTemporalInput<TPayload>(
            this QueryContainer container,
            IObservable<TPayload> streamEvents,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            DisorderPolicy disorderPolicy = null,
            FlushPolicy flushPolicy = FlushPolicy.FlushOnPunctuation,
            PeriodicPunctuationPolicy periodicPunctuationPolicy = null,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.EndOfStream,
            string identifier = null)
        {
            return ToCheckpointableTemporalStreamable(
                streamEvents,
                startEdgeExtractor,
                endEdgeExtractor,
                container,
                identifier ?? Guid.NewGuid().ToString(),
                disorderPolicy,
                flushPolicy,
                periodicPunctuationPolicy,
                onCompletedPolicy);
        }

        internal static IObservableIngressStreamable<TPayload> ToCheckpointableTemporalStreamable<TPayload>(
            this IObservable<TPayload> streamEvents,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            QueryContainer container,
            string identifier,
            DisorderPolicy disorderPolicy,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy periodicPunctuationPolicy,
            OnCompletedPolicy onCompletedPolicy)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            if (disorderPolicy == null)
                disorderPolicy = DisorderPolicy.Throw();

            if (periodicPunctuationPolicy == null)
                periodicPunctuationPolicy = PeriodicPunctuationPolicy.None();

            var a = new IntervalIngressStreamable<TPayload>(
                streamEvents,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                periodicPunctuationPolicy,
                onCompletedPolicy,
                container,
                identifier);

            return a;
        }

        /// <summary>
        /// Converts a sequence of PartitionedStreamEvents to an IStreamable.
        /// The completion policy specifies what to do when the resulting stream completes.
        /// The disorder policy specifies what to do with out of order events.
        /// The punctuation policy specifies whether and how punctuations are created and injected
        /// into the resulting stream. Since punctuations force output, this exposes a throughput/latency tradeoff.
        /// </summary>
        /// <typeparam name="TPartitionKey">The type of partition key for the stream.</typeparam>
        /// <typeparam name="TPayload">The type of data for the stream.</typeparam>
        /// <param name="streamEvents">A sequence of stream events created by the client.</param>
        /// <param name="disorderPolicy">How to handle events that are not in time order.</param>
        /// <param name="flushPolicy">When to flush batched output events.</param>
        /// <param name="periodicPunctuationPolicy">Whether to add periodic punctuations to the resulting stream.</param>
        /// <param name="periodicLowWatermarkPolicy">Whether to add periodic low watermarks to the resulting stream.</param>
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        /// <exception cref="IngressException">
        /// Throws an exception if the <paramref name="disorderPolicy"/> is to throw and
        /// an out-of-order stream event is encountered.
        /// Also, an exception is thrown if any payload is null.
        /// </exception>
        public static IPartitionedIngressStreamable<TPartitionKey, TPayload> ToStreamable<TPartitionKey, TPayload>(
            this IObservable<PartitionedStreamEvent<TPartitionKey, TPayload>> streamEvents,
            DisorderPolicy disorderPolicy = null,
            PartitionedFlushPolicy flushPolicy = PartitionedFlushPolicy.FlushOnLowWatermark,
            PeriodicPunctuationPolicy periodicPunctuationPolicy = null,
            PeriodicLowWatermarkPolicy periodicLowWatermarkPolicy = null,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.EndOfStream)
        {
            return ToCheckpointableStreamable(
                streamEvents,
                null,
                Guid.NewGuid().ToString(),
                disorderPolicy,
                flushPolicy,
                periodicPunctuationPolicy,
                periodicLowWatermarkPolicy,
                onCompletedPolicy);
        }

        /// <summary>
        /// Converts a sequence of PartitionedStreamEvents to an IStreamable.
        /// The completion policy specifies what to do when the resulting stream completes.
        /// The disorder policy specifies what to do with out of order events.
        /// The punctuation policy specifies whether and how punctuations are created and injected
        /// into the resulting stream. Since punctuations force output, this exposes a throughput/latency tradeoff.
        /// </summary>
        /// <param name="container">The query container to which to register the ingress point.</param>
        /// <typeparam name="TPartitionKey">The type of partition key for the stream.</typeparam>
        /// <typeparam name="TPayload">The type of data for the stream.</typeparam>
        /// <param name="streamEvents">A sequence of stream events created by the client.</param>
        /// <param name="disorderPolicy">How to handle events that are not in time order.</param>
        /// <param name="flushPolicy">When to flush batched output events.</param>
        /// <param name="periodicPunctuationPolicy">Whether to add periodic punctuations to the resulting stream.</param>
        /// <param name="periodicLowWatermarkPolicy">Whether to add periodic low watermarks to the resulting stream.</param>
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <param name="identifier">If provided, a unique name to identify to point of ingress in the query.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        /// <exception cref="IngressException">
        /// Throws an exception if the <paramref name="disorderPolicy"/> is to throw and
        /// an out-of-order stream event is encountered.
        /// Also, an exception is thrown if any payload is null.
        /// </exception>
        public static IPartitionedIngressStreamable<TPartitionKey, TPayload> RegisterInput<TPartitionKey, TPayload>(
            this QueryContainer container,
            IObservable<PartitionedStreamEvent<TPartitionKey, TPayload>> streamEvents,
            DisorderPolicy disorderPolicy = null,
            PartitionedFlushPolicy flushPolicy = PartitionedFlushPolicy.FlushOnLowWatermark,
            PeriodicPunctuationPolicy periodicPunctuationPolicy = null,
            PeriodicLowWatermarkPolicy periodicLowWatermarkPolicy = null,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.EndOfStream,
            string identifier = null)
        {
            return ToCheckpointableStreamable(
                streamEvents,
                container,
                identifier ?? Guid.NewGuid().ToString(),
                disorderPolicy,
                flushPolicy,
                periodicPunctuationPolicy,
                periodicLowWatermarkPolicy,
                onCompletedPolicy);
        }

        internal static IPartitionedIngressStreamable<TPartitionKey, TPayload> ToCheckpointableStreamable<TPartitionKey, TPayload>(
            this IObservable<PartitionedStreamEvent<TPartitionKey, TPayload>> streamEvents,
            QueryContainer container,
            string identifier,
            DisorderPolicy disorderPolicy,
            PartitionedFlushPolicy flushPolicy,
            PeriodicPunctuationPolicy periodicPunctuationPolicy,
            PeriodicLowWatermarkPolicy lowWatermarkPolicy,
            OnCompletedPolicy onCompletedPolicy)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            if (disorderPolicy == null)
                disorderPolicy = DisorderPolicy.Throw();

            if (periodicPunctuationPolicy == null)
                periodicPunctuationPolicy = PeriodicPunctuationPolicy.None();

            if (lowWatermarkPolicy == null)
                lowWatermarkPolicy = PeriodicLowWatermarkPolicy.None();

            var a = new PartitionedStreamEventIngressStreamable<TPartitionKey, TPayload>(
                streamEvents,
                disorderPolicy,
                flushPolicy,
                periodicPunctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                identifier);

            return a;
        }

        /// <summary>
        /// Converts a sequence of data elements to an IStreamable, start-edge only, with a partition key.
        /// The completion policy specifies what to do when the resulting stream completes.
        /// The disorder policy specifies what to do with out of order events.
        /// The punctuation policy specifies whether and how punctuations are created and injected
        /// into the resulting stream. Since punctuations force output, this exposes a throughput/latency tradeoff.
        /// </summary>
        /// <typeparam name="TPartitionKey">The type of partition key for the stream.</typeparam>
        /// <typeparam name="TPayload">The type of data for the stream.</typeparam>
        /// <param name="streamEvents">A sequence of stream events created by the client.</param>
        /// <param name="partitionExtractor">An expresion that describes how to interpret the partition identifier for each data value.</param>
        /// <param name="startEdgeExtractor">An expresion that describes how to interpret the start time for each data value.</param>
        /// <param name="disorderPolicy">How to handle events that are not in time order.</param>
        /// <param name="flushPolicy">When to flush batched output events.</param>
        /// <param name="periodicPunctuationPolicy">Whether to add periodic punctuations to the resulting stream.</param>
        /// <param name="periodicLowWatermarkPolicy">Whether to add periodic low watermarks to the resulting stream.</param>
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        /// <exception cref="IngressException">
        /// Throws an exception if the <paramref name="disorderPolicy"/> is to throw and
        /// an out-of-order stream event is encountered.
        /// Also, an exception is thrown if any payload is null.
        /// </exception>
        public static IPartitionedIngressStreamable<TPartitionKey, TPayload> ToPartitionedStreamable<TPartitionKey, TPayload>(
            this IObservable<TPayload> streamEvents,
            Expression<Func<TPayload, TPartitionKey>> partitionExtractor,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            DisorderPolicy disorderPolicy = null,
            PartitionedFlushPolicy flushPolicy = PartitionedFlushPolicy.FlushOnLowWatermark,
            PeriodicPunctuationPolicy periodicPunctuationPolicy = null,
            PeriodicLowWatermarkPolicy periodicLowWatermarkPolicy = null,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.EndOfStream)
        {
            return ToCheckpointablePartitionedStreamable(
                streamEvents,
                partitionExtractor,
                startEdgeExtractor,
                null,
                Guid.NewGuid().ToString(),
                disorderPolicy,
                flushPolicy,
                periodicPunctuationPolicy,
                periodicLowWatermarkPolicy,
                onCompletedPolicy);
        }

        /// <summary>
        /// Converts a sequence of data elements to an IStreamable, start-edge only, with a partition key.
        /// The completion policy specifies what to do when the resulting stream completes.
        /// The disorder policy specifies what to do with out of order events.
        /// The punctuation policy specifies whether and how punctuations are created and injected
        /// into the resulting stream. Since punctuations force output, this exposes a throughput/latency tradeoff.
        /// </summary>
        /// <param name="container">The query container to which to register the ingress point.</param>
        /// <typeparam name="TPartitionKey">The type of partition key for the stream.</typeparam>
        /// <typeparam name="TPayload">The type of data for the stream.</typeparam>
        /// <param name="streamEvents">A sequence of stream events created by the client.</param>
        /// <param name="partitionExtractor">An expresion that describes how to interpret the partition identifier for each data value.</param>
        /// <param name="startEdgeExtractor">An expresion that describes how to interpret the start time for each data value.</param>
        /// <param name="disorderPolicy">How to handle events that are not in time order.</param>
        /// <param name="flushPolicy">When to flush batched output events.</param>
        /// <param name="periodicPunctuationPolicy">Whether to add periodic punctuations to the resulting stream.</param>
        /// <param name="periodicLowWatermarkPolicy">Whether to add periodic low watermarks to the resulting stream.</param>
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <param name="identifier">If provided, a unique name to identify to point of ingress in the query.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        /// <exception cref="IngressException">
        /// Throws an exception if the <paramref name="disorderPolicy"/> is to throw and
        /// an out-of-order stream event is encountered.
        /// Also, an exception is thrown if any payload is null.
        /// </exception>
        public static IPartitionedIngressStreamable<TPartitionKey, TPayload> RegisterPartitionedInput<TPartitionKey, TPayload>(
            this QueryContainer container,
            IObservable<TPayload> streamEvents,
            Expression<Func<TPayload, TPartitionKey>> partitionExtractor,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            DisorderPolicy disorderPolicy = null,
            PartitionedFlushPolicy flushPolicy = PartitionedFlushPolicy.FlushOnLowWatermark,
            PeriodicPunctuationPolicy periodicPunctuationPolicy = null,
            PeriodicLowWatermarkPolicy periodicLowWatermarkPolicy = null,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.EndOfStream,
            string identifier = null)
        {
            return ToCheckpointablePartitionedStreamable(
                streamEvents,
                partitionExtractor,
                startEdgeExtractor,
                container,
                identifier ?? Guid.NewGuid().ToString(),
                disorderPolicy,
                flushPolicy,
                periodicPunctuationPolicy,
                periodicLowWatermarkPolicy,
                onCompletedPolicy);
        }

        internal static IPartitionedIngressStreamable<TPartitionKey, TPayload> ToCheckpointablePartitionedStreamable<TPartitionKey, TPayload>(
            this IObservable<TPayload> streamEvents,
            Expression<Func<TPayload, TPartitionKey>> partitionExtractor,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            QueryContainer container,
            string identifier,
            DisorderPolicy disorderPolicy,
            PartitionedFlushPolicy flushPolicy,
            PeriodicPunctuationPolicy periodicPunctuationPolicy,
            PeriodicLowWatermarkPolicy lowWatermarkPolicy,
            OnCompletedPolicy onCompletedPolicy)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            if (disorderPolicy == null)
                disorderPolicy = DisorderPolicy.Throw();

            if (periodicPunctuationPolicy == null)
                periodicPunctuationPolicy = PeriodicPunctuationPolicy.None();

            if (lowWatermarkPolicy == null)
                lowWatermarkPolicy = PeriodicLowWatermarkPolicy.None();

            var a = new PartitionedIntervalIngressStreamable<TPartitionKey, TPayload>(
                streamEvents,
                partitionExtractor,
                startEdgeExtractor,
                (p) => StreamEvent.InfinitySyncTime,
                disorderPolicy,
                flushPolicy,
                periodicPunctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                identifier);

            return a;
        }

        /// <summary>
        /// Converts a sequence of data elements to an IStreamable, intervals and start-edges only, with a partition key.
        /// The completion policy specifies what to do when the resulting stream completes.
        /// The disorder policy specifies what to do with out of order events.
        /// The punctuation policy specifies whether and how punctuations are created and injected
        /// into the resulting stream. Since punctuations force output, this exposes a throughput/latency tradeoff.
        /// </summary>
        /// <typeparam name="TPartitionKey">The type of partition key for the stream.</typeparam>
        /// <typeparam name="TPayload">The type of data for the stream.</typeparam>
        /// <param name="streamEvents">A sequence of stream events created by the client.</param>
        /// <param name="partitionExtractor">An expresion that describes how to interpret the partition identifier for each data value.</param>
        /// <param name="startEdgeExtractor">An expresion that describes how to interpret the start time for each data value.</param>
        /// <param name="endEdgeExtractor">An expresion that describes how to interpret the end time for each data value.  Return StreamEvent.InfinitySyncTime to indicate an event with no end time.</param>
        /// <param name="disorderPolicy">How to handle events that are not in time order.</param>
        /// <param name="flushPolicy">When to flush batched output events.</param>
        /// <param name="periodicPunctuationPolicy">Whether to add periodic punctuations to the resulting stream.</param>
        /// <param name="periodicLowWatermarkPolicy">Whether to add periodic low watermarks to the resulting stream.</param>
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        /// <exception cref="IngressException">
        /// Throws an exception if the <paramref name="disorderPolicy"/> is to throw and
        /// an out-of-order stream event is encountered.
        /// Also, an exception is thrown if any payload is null.
        /// </exception>
        public static IPartitionedIngressStreamable<TPartitionKey, TPayload> ToPartitionedStreamable<TPartitionKey, TPayload>(
            this IObservable<TPayload> streamEvents,
            Expression<Func<TPayload, TPartitionKey>> partitionExtractor,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            DisorderPolicy disorderPolicy = null,
            PartitionedFlushPolicy flushPolicy = PartitionedFlushPolicy.FlushOnLowWatermark,
            PeriodicPunctuationPolicy periodicPunctuationPolicy = null,
            PeriodicLowWatermarkPolicy periodicLowWatermarkPolicy = null,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.EndOfStream)
        {
            return ToCheckpointablePartitionedStreamable(
                streamEvents,
                partitionExtractor,
                startEdgeExtractor,
                endEdgeExtractor,
                null,
                Guid.NewGuid().ToString(),
                disorderPolicy,
                flushPolicy,
                periodicPunctuationPolicy,
                periodicLowWatermarkPolicy,
                onCompletedPolicy);
        }

        /// <summary>
        /// Converts a sequence of data elements to an IStreamable, intervals and start-edges only, with a partition key.
        /// The completion policy specifies what to do when the resulting stream completes.
        /// The disorder policy specifies what to do with out of order events.
        /// The punctuation policy specifies whether and how punctuations are created and injected
        /// into the resulting stream. Since punctuations force output, this exposes a throughput/latency tradeoff.
        /// </summary>
        /// <param name="container">The query container to which to register the ingress point.</param>
        /// <typeparam name="TPartitionKey">The type of partition key for the stream.</typeparam>
        /// <typeparam name="TPayload">The type of data for the stream.</typeparam>
        /// <param name="streamEvents">A sequence of stream events created by the client.</param>
        /// <param name="partitionExtractor">An expresion that describes how to interpret the partition identifier for each data value.</param>
        /// <param name="startEdgeExtractor">An expresion that describes how to interpret the start time for each data value.</param>
        /// <param name="endEdgeExtractor">An expresion that describes how to interpret the end time for each data value.  Return StreamEvent.InfinitySyncTime to indicate an event with no end time.</param>
        /// <param name="disorderPolicy">How to handle events that are not in time order.</param>
        /// <param name="flushPolicy">When to flush batched output events.</param>
        /// <param name="periodicPunctuationPolicy">Whether to add periodic punctuations to the resulting stream.</param>
        /// <param name="periodicLowWatermarkPolicy">Whether to add periodic low watermarks to the resulting stream.</param>
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <param name="identifier">If provided, a unique name to identify to point of ingress in the query.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        /// <exception cref="IngressException">
        /// Throws an exception if the <paramref name="disorderPolicy"/> is to throw and
        /// an out-of-order stream event is encountered.
        /// Also, an exception is thrown if any payload is null.
        /// </exception>
        public static IPartitionedIngressStreamable<TPartitionKey, TPayload> RegisterPartitionedInput<TPartitionKey, TPayload>(
            this QueryContainer container,
            IObservable<TPayload> streamEvents,
            Expression<Func<TPayload, TPartitionKey>> partitionExtractor,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            DisorderPolicy disorderPolicy = null,
            PartitionedFlushPolicy flushPolicy = PartitionedFlushPolicy.FlushOnLowWatermark,
            PeriodicPunctuationPolicy periodicPunctuationPolicy = null,
            PeriodicLowWatermarkPolicy periodicLowWatermarkPolicy = null,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.EndOfStream,
            string identifier = null)
        {
            return ToCheckpointablePartitionedStreamable(
                streamEvents,
                partitionExtractor,
                startEdgeExtractor,
                endEdgeExtractor,
                container,
                identifier ?? Guid.NewGuid().ToString(),
                disorderPolicy,
                flushPolicy,
                periodicPunctuationPolicy,
                periodicLowWatermarkPolicy,
                onCompletedPolicy);
        }

        internal static IPartitionedIngressStreamable<TPartitionKey, TPayload> ToCheckpointablePartitionedStreamable<TPartitionKey, TPayload>(
            this IObservable<TPayload> streamEvents,
            Expression<Func<TPayload, TPartitionKey>> partitionExtractor,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            QueryContainer container,
            string identifier,
            DisorderPolicy disorderPolicy,
            PartitionedFlushPolicy flushPolicy,
            PeriodicPunctuationPolicy periodicPunctuationPolicy,
            PeriodicLowWatermarkPolicy lowWatermarkPolicy,
            OnCompletedPolicy onCompletedPolicy)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            if (disorderPolicy == null)
                disorderPolicy = DisorderPolicy.Throw();

            if (periodicPunctuationPolicy == null)
                periodicPunctuationPolicy = PeriodicPunctuationPolicy.None();

            if (lowWatermarkPolicy == null)
                lowWatermarkPolicy = PeriodicLowWatermarkPolicy.None();

            var a = new PartitionedIntervalIngressStreamable<TPartitionKey, TPayload>(
                streamEvents,
                partitionExtractor,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                periodicPunctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                identifier);

            return a;
        }

    }
}