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
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        /// <exception cref="IngressException">
        /// Throws an exception if an out-of-order stream event is encountered.
        /// Also, an exception is thrown if any payload is null.
        /// </exception>
        public static IObservableIngressStreamable<TPayload> ToStreamable<TPayload>(
            this IObservable<ArraySegment<StreamEvent<TPayload>>> streamEvents,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.EndOfStream)
        {
            return ToCheckpointableStreamable(
                streamEvents,
                null,
                Guid.NewGuid().ToString(),
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
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <param name="identifier">If provided, a unique name to identify to point of ingress in the query.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        /// <exception cref="IngressException">
        /// Throws an exception if an out-of-order stream event is encountered.
        /// Also, an exception is thrown if any payload is null.
        /// </exception>
        public static IObservableIngressStreamable<TPayload> RegisterInput<TPayload>(
            this QueryContainer container,
            IObservable<ArraySegment<StreamEvent<TPayload>>> streamEvents,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.EndOfStream,
            string identifier = null)
        {
            return ToCheckpointableStreamable(
                streamEvents,
                container,
                identifier ?? Guid.NewGuid().ToString(),
                onCompletedPolicy);
        }

        internal static IObservableIngressStreamable<TPayload> ToCheckpointableStreamable<TPayload>(
            this IObservable<ArraySegment<StreamEvent<TPayload>>> streamEvents,
            QueryContainer container,
            string identifier,
            OnCompletedPolicy onCompletedPolicy)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            var a = new StreamEventArrayIngressStreamable<TPayload>(streamEvents, onCompletedPolicy, container, identifier);
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
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        /// <exception cref="IngressException">
        /// Throws an exception if an out-of-order stream event is encountered.
        /// Also, an exception is thrown if any payload is null.
        /// </exception>
        public static IObservableIngressStreamable<TPayload> ToTemporalArrayStreamable<TPayload>(
            this IObservable<ArraySegment<TPayload>> streamEvents,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.EndOfStream)
        {
            return ToCheckpointableTemporalArrayStreamable(
                streamEvents,
                startEdgeExtractor,
                null,
                Guid.NewGuid().ToString(),
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
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <param name="identifier">If provided, a unique name to identify to point of ingress in the query.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        /// <exception cref="IngressException">
        /// Throws an exception if an out-of-order stream event is encountered.
        /// Also, an exception is thrown if any payload is null.
        /// </exception>
        public static IObservableIngressStreamable<TPayload> RegisterTemporalArrayInput<TPayload>(
            this QueryContainer container,
            IObservable<ArraySegment<TPayload>> streamEvents,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.EndOfStream,
            string identifier = null)
        {
            return ToCheckpointableTemporalArrayStreamable(
                streamEvents,
                startEdgeExtractor,
                container,
                identifier ?? Guid.NewGuid().ToString(),
                onCompletedPolicy);
        }

        internal static IObservableIngressStreamable<TPayload> ToCheckpointableTemporalArrayStreamable<TPayload>(
            this IObservable<ArraySegment<TPayload>> streamEvents,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            QueryContainer container,
            string identifier,
            OnCompletedPolicy onCompletedPolicy)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            var a = new IntervalArrayIngressStreamable<TPayload>(streamEvents, startEdgeExtractor, (o) => StreamEvent.InfinitySyncTime, onCompletedPolicy, container, identifier);
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
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        /// <exception cref="IngressException">
        /// Throws an exception if an out-of-order stream event is encountered.
        /// Also, an exception is thrown if any payload is null.
        /// </exception>
        public static IObservableIngressStreamable<TPayload> ToTemporalArrayStreamable<TPayload>(
            this IObservable<ArraySegment<TPayload>> streamEvents,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.EndOfStream)
        {
            return ToCheckpointableTemporalArrayStreamable(
                streamEvents,
                startEdgeExtractor,
                endEdgeExtractor,
                null,
                Guid.NewGuid().ToString(),
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
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <param name="identifier">If provided, a unique name to identify to point of ingress in the query.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        /// <exception cref="IngressException">
        /// Throws an exception if an out-of-order stream event is encountered.
        /// Also, an exception is thrown if any payload is null.
        /// </exception>
        public static IObservableIngressStreamable<TPayload> RegisterTemporalArrayInput<TPayload>(
            this QueryContainer container,
            IObservable<ArraySegment<TPayload>> streamEvents,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.EndOfStream,
            string identifier = null)
        {
            return ToCheckpointableTemporalArrayStreamable(
                streamEvents,
                startEdgeExtractor,
                endEdgeExtractor,
                container,
                identifier ?? Guid.NewGuid().ToString(),
                onCompletedPolicy);
        }

        internal static IObservableIngressStreamable<TPayload> ToCheckpointableTemporalArrayStreamable<TPayload>(
            this IObservable<ArraySegment<TPayload>> streamEvents,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            QueryContainer container,
            string identifier,
            OnCompletedPolicy onCompletedPolicy)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            var a = new IntervalArrayIngressStreamable<TPayload>(streamEvents, startEdgeExtractor, endEdgeExtractor, onCompletedPolicy, container, identifier);
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
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        /// <exception cref="IngressException">
        /// Throws an exception if an out-of-order stream event is encountered.
        /// Also, an exception is thrown if any payload is null.
        /// </exception>
        public static IPartitionedIngressStreamable<TPartitionKey, TPayload> ToStreamable<TPartitionKey, TPayload>(
            this IObservable<ArraySegment<PartitionedStreamEvent<TPartitionKey, TPayload>>> streamEvents,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.EndOfStream)
        {
            return ToCheckpointableStreamable(
                streamEvents,
                null,
                Guid.NewGuid().ToString(),
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
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <param name="identifier">If provided, a unique name to identify to point of ingress in the query.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        /// <exception cref="IngressException">
        /// Throws an exception if an out-of-order stream event is encountered.
        /// Also, an exception is thrown if any payload is null.
        /// </exception>
        public static IPartitionedIngressStreamable<TPartitionKey, TPayload> RegisterInput<TPartitionKey, TPayload>(
            this QueryContainer container,
            IObservable<ArraySegment<PartitionedStreamEvent<TPartitionKey, TPayload>>> streamEvents,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.EndOfStream,
            string identifier = null)
        {
            return ToCheckpointableStreamable(
                streamEvents,
                container,
                identifier ?? Guid.NewGuid().ToString(),
                onCompletedPolicy);
        }

        internal static IPartitionedIngressStreamable<TPartitionKey, TPayload> ToCheckpointableStreamable<TPartitionKey, TPayload>(
            this IObservable<ArraySegment<PartitionedStreamEvent<TPartitionKey, TPayload>>> streamEvents,
            QueryContainer container,
            string identifier,
            OnCompletedPolicy onCompletedPolicy)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            var a = new PartitionedStreamEventArrayIngressStreamable<TPartitionKey, TPayload>(streamEvents, onCompletedPolicy, container, identifier);
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
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        /// <exception cref="IngressException">
        /// Throws an exception if an out-of-order stream event is encountered.
        /// Also, an exception is thrown if any payload is null.
        /// </exception>
        public static IPartitionedIngressStreamable<TPartitionKey, TPayload> ToPartitionedArrayStreamable<TPartitionKey, TPayload>(
            this IObservable<ArraySegment<TPayload>> streamEvents,
            Expression<Func<TPayload, TPartitionKey>> partitionExtractor,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.EndOfStream)
        {
            return ToCheckpointablePartitionedArrayStreamable(
                streamEvents,
                partitionExtractor,
                startEdgeExtractor,
                null,
                Guid.NewGuid().ToString(),
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
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <param name="identifier">If provided, a unique name to identify to point of ingress in the query.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        /// <exception cref="IngressException">
        /// Throws an exception if an out-of-order stream event is encountered.
        /// Also, an exception is thrown if any payload is null.
        /// </exception>
        public static IPartitionedIngressStreamable<TPartitionKey, TPayload> RegisterPartitionedArrayInput<TPartitionKey, TPayload>(
            this QueryContainer container,
            IObservable<ArraySegment<TPayload>> streamEvents,
            Expression<Func<TPayload, TPartitionKey>> partitionExtractor,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.EndOfStream,
            string identifier = null)
        {
            return ToCheckpointablePartitionedArrayStreamable(
                streamEvents,
                partitionExtractor,
                startEdgeExtractor,
                container,
                identifier ?? Guid.NewGuid().ToString(),
                onCompletedPolicy);
        }

        internal static IPartitionedIngressStreamable<TPartitionKey, TPayload> ToCheckpointablePartitionedArrayStreamable<TPartitionKey, TPayload>(
            this IObservable<ArraySegment<TPayload>> streamEvents,
            Expression<Func<TPayload, TPartitionKey>> partitionExtractor,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            QueryContainer container,
            string identifier,
            OnCompletedPolicy onCompletedPolicy)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            var a = new PartitionedIntervalArrayIngressStreamable<TPartitionKey, TPayload>(streamEvents, partitionExtractor, startEdgeExtractor, (o) => StreamEvent.InfinitySyncTime, onCompletedPolicy, container, identifier);
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
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        /// <exception cref="IngressException">
        /// Throws an exception if an out-of-order stream event is encountered.
        /// Also, an exception is thrown if any payload is null.
        /// </exception>
        public static IPartitionedIngressStreamable<TPartitionKey, TPayload> ToPartitionedArrayStreamable<TPartitionKey, TPayload>(
            this IObservable<ArraySegment<TPayload>> streamEvents,
            Expression<Func<TPayload, TPartitionKey>> partitionExtractor,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.EndOfStream)
        {
            return ToCheckpointablePartitionedArrayStreamable(
                streamEvents,
                partitionExtractor,
                startEdgeExtractor,
                endEdgeExtractor,
                null,
                Guid.NewGuid().ToString(),
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
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <param name="identifier">If provided, a unique name to identify to point of ingress in the query.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        /// <exception cref="IngressException">
        /// Throws an exception if an out-of-order stream event is encountered.
        /// Also, an exception is thrown if any payload is null.
        /// </exception>
        public static IPartitionedIngressStreamable<TPartitionKey, TPayload> RegisterPartitionedArrayInput<TPartitionKey, TPayload>(
            this QueryContainer container,
            IObservable<ArraySegment<TPayload>> streamEvents,
            Expression<Func<TPayload, TPartitionKey>> partitionExtractor,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.EndOfStream,
            string identifier = null)
        {
            return ToCheckpointablePartitionedArrayStreamable(
                streamEvents,
                partitionExtractor,
                startEdgeExtractor,
                endEdgeExtractor,
                container,
                identifier ?? Guid.NewGuid().ToString(),
                onCompletedPolicy);
        }

        internal static IPartitionedIngressStreamable<TPartitionKey, TPayload> ToCheckpointablePartitionedArrayStreamable<TPartitionKey, TPayload>(
            this IObservable<ArraySegment<TPayload>> streamEvents,
            Expression<Func<TPayload, TPartitionKey>> partitionExtractor,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            QueryContainer container,
            string identifier,
            OnCompletedPolicy onCompletedPolicy)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            var a = new PartitionedIntervalArrayIngressStreamable<TPartitionKey, TPayload>(streamEvents, partitionExtractor, startEdgeExtractor, endEdgeExtractor, onCompletedPolicy, container, identifier);
            return a;
        }

    }
}