// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Streamable extension methods.
    /// </summary>
    public static partial class Streamable
    {
        /// <summary>
        /// Exports a streamable as an observable of events. Produces events that are sync time ordered.
        /// </summary>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="stream"></param>
        /// <param name="reshapingPolicy">Policy that specifies whether and how events are reshaped at egress. Default passes events through unmodified.</param>
        /// <returns></returns>
        public static IObservable<ArraySegment<StreamEvent<TPayload>>> ToStreamEventArrayObservable<TPayload>(
            this IStreamable<Empty, TPayload> stream,
            ReshapingPolicy reshapingPolicy = ReshapingPolicy.None)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToStreamEventArrayObservable(
                () => new StreamEvent<TPayload>[Config.DataBatchSize],
                null,
                Guid.NewGuid().ToString(),
                reshapingPolicy);
        }

        /// <summary>
        /// Exports a streamable as an observable of events. Produces events that are sync time ordered.
        /// </summary>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="stream"></param>
        /// <param name="generator">A function that returns an array that will be populated with stream results.</param>
        /// <param name="reshapingPolicy">Policy that specifies whether and how events are reshaped at egress. Default passes events through unmodified.</param>
        /// <returns></returns>
        public static IObservable<ArraySegment<StreamEvent<TPayload>>> ToStreamEventArrayObservable<TPayload>(
            this IStreamable<Empty, TPayload> stream,
            Func<StreamEvent<TPayload>[]> generator,
            ReshapingPolicy reshapingPolicy = ReshapingPolicy.None)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToStreamEventArrayObservable(
                generator,
                null,
                Guid.NewGuid().ToString(),
                reshapingPolicy);
        }

        /// <summary>
        /// Exports a streamable as an observable of events. Produces events that are sync time ordered.
        /// </summary>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="container">The query container to which an egress point is being added.</param>
        /// <param name="identifier">A string that can uniquely identify the point of egress in the query.</param>
        /// <param name="stream"></param>
        /// <param name="reshapingPolicy">Policy that specifies whether and how events are reshaped at egress. Default passes events through unmodified.</param>
        /// <returns></returns>
        public static IObservable<ArraySegment<StreamEvent<TPayload>>> RegisterArrayOutput<TPayload>(
            this QueryContainer container,
            IStreamable<Empty, TPayload> stream,
            ReshapingPolicy reshapingPolicy = ReshapingPolicy.None,
            string identifier = null)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToStreamEventArrayObservable(
                () => new StreamEvent<TPayload>[Config.DataBatchSize],
                container,
                identifier ?? Guid.NewGuid().ToString(),
                reshapingPolicy);
        }

        /// <summary>
        /// Exports a streamable as an observable of events. Produces events that are sync time ordered.
        /// </summary>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="container">The query container to which an egress point is being added.</param>
        /// <param name="identifier">A string that can uniquely identify the point of egress in the query.</param>
        /// <param name="stream"></param>
        /// <param name="generator">A function that returns an array that will be populated with stream results.</param>
        /// <param name="reshapingPolicy">Policy that specifies whether and how events are reshaped at egress. Default passes events through unmodified.</param>
        /// <returns></returns>
        public static IObservable<ArraySegment<StreamEvent<TPayload>>> RegisterArrayOutput<TPayload>(
            this QueryContainer container,
            IStreamable<Empty, TPayload> stream,
            Func<StreamEvent<TPayload>[]> generator,
            ReshapingPolicy reshapingPolicy = ReshapingPolicy.None,
            string identifier = null)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToStreamEventArrayObservable(
                generator,
                container,
                identifier ?? Guid.NewGuid().ToString(),
                reshapingPolicy);
        }

        internal static IObservable<ArraySegment<StreamEvent<TPayload>>> ToStreamEventArrayObservable<TPayload>(
            this IStreamable<Empty, TPayload> stream,
            Func<StreamEvent<TPayload>[]> generator,
            QueryContainer container,
            string identifer,
            ReshapingPolicy reshapingPolicy)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return (reshapingPolicy == ReshapingPolicy.None)
                ? new StreamEventArrayObservable<TPayload>(stream, generator, container, identifer)
                : new StreamEventArrayObservable<TPayload>(stream.ToEndEdgeFreeStream(), generator, container, identifer);
        }

        /// <summary>
        /// Exports a streamable as an observable of events. Produces events that are sync time ordered.
        /// Expects only start-edge events in the stream, and constructs user-defined payloads as a result.
        /// </summary>
        /// <typeparam name="TPayload"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="stream"></param>
        /// <param name="constructor">Method description that constructs result data from the start time and the payload of each event.</param>
        /// <returns></returns>
        public static IObservable<ArraySegment<TResult>> ToTemporalArrayObservable<TPayload, TResult>(
            this IStreamable<Empty, TPayload> stream,
            Expression<Func<long, TPayload, TResult>> constructor)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToTemporalArrayObservable(
                () => new TResult[Config.DataBatchSize],
                constructor, null,
                Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Exports a streamable as an observable of events. Produces events that are sync time ordered.
        /// Expects only start-edge events in the stream, and constructs user-defined payloads as a result.
        /// </summary>
        /// <typeparam name="TPayload"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="stream"></param>
        /// <param name="generator">A function that returns an array that will be populated with stream results.</param>
        /// <param name="constructor">Method description that constructs result data from the start time and the payload of each event.</param>
        /// <returns></returns>
        public static IObservable<ArraySegment<TResult>> ToTemporalArrayObservable<TPayload, TResult>(
            this IStreamable<Empty, TPayload> stream,
            Func<TResult[]> generator,
            Expression<Func<long, TPayload, TResult>> constructor)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToTemporalArrayObservable(
                generator,
                constructor, null,
                Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Exports a streamable as an observable of events. Produces events that are sync time ordered.
        /// Expects only start-edge events in the stream, and constructs user-defined payloads as a result.
        /// </summary>
        /// <typeparam name="TPayload"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="container">The query container to which an egress point is being added.</param>
        /// <param name="identifier">A string that can uniquely identify the point of egress in the query.</param>
        /// <param name="stream"></param>
        /// <param name="constructor">Method description that constructs result data from the start time and the payload of each event.</param>
        /// <returns></returns>
        public static IObservable<ArraySegment<TResult>> RegisterTemporalArrayOutput<TPayload, TResult>(
            this QueryContainer container,
            IStreamable<Empty, TPayload> stream,
            Expression<Func<long, TPayload, TResult>> constructor,
            string identifier = null)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToTemporalArrayObservable(
                () => new TResult[Config.DataBatchSize],
                constructor, container,
                identifier ?? Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Exports a streamable as an observable of events. Produces events that are sync time ordered.
        /// Expects only start-edge events in the stream, and constructs user-defined payloads as a result.
        /// </summary>
        /// <typeparam name="TPayload"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="container">The query container to which an egress point is being added.</param>
        /// <param name="identifier">A string that can uniquely identify the point of egress in the query.</param>
        /// <param name="stream"></param>
        /// <param name="generator">A function that returns an array that will be populated with stream results.</param>
        /// <param name="constructor">Method description that constructs result data from the start time and the payload of each event.</param>
        /// <returns></returns>
        public static IObservable<ArraySegment<TResult>> RegisterTemporalArrayOutput<TPayload, TResult>(
            this QueryContainer container,
            IStreamable<Empty, TPayload> stream,
            Func<TResult[]> generator,
            Expression<Func<long, TPayload, TResult>> constructor,
            string identifier = null)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToTemporalArrayObservable(
                generator,
                constructor, container,
                identifier ?? Guid.NewGuid().ToString());
        }

        internal static IObservable<ArraySegment<TResult>> ToTemporalArrayObservable<TPayload, TResult>(
            this IStreamable<Empty, TPayload> stream,
            Func<TResult[]> generator,
                 Expression<Func<long, TPayload, TResult>> constructor,
            QueryContainer container,
            string identifer)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return new StartEdgeArrayObservable<TPayload, TResult>(stream, generator, constructor, container, identifer);
        }

        /// <summary>
        /// Exports a streamable as an observable of events. Produces events that are sync time ordered.
        /// Expects only start-edge and interval events in the stream, and constructs user-defined payloads as a result.
        /// </summary>
        /// <typeparam name="TPayload"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="stream"></param>
        /// <param name="constructor">Method description that constructs result data from the start time, end time, and the payload of each event.</param>
        /// <returns></returns>
        public static IObservable<ArraySegment<TResult>> ToTemporalArrayObservable<TPayload, TResult>(
            this IStreamable<Empty, TPayload> stream,
            Expression<Func<long, long, TPayload, TResult>> constructor)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToTemporalArrayObservable(
                () => new TResult[Config.DataBatchSize],
                constructor, null,
                Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Exports a streamable as an observable of events. Produces events that are sync time ordered.
        /// Expects only start-edge and interval events in the stream, and constructs user-defined payloads as a result.
        /// </summary>
        /// <typeparam name="TPayload"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="stream"></param>
        /// <param name="generator">A function that returns an array that will be populated with stream results.</param>
        /// <param name="constructor">Method description that constructs result data from the start time, end time, and the payload of each event.</param>
        /// <returns></returns>
        public static IObservable<ArraySegment<TResult>> ToTemporalArrayObservable<TPayload, TResult>(
            this IStreamable<Empty, TPayload> stream,
            Func<TResult[]> generator,
            Expression<Func<long, long, TPayload, TResult>> constructor)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToTemporalArrayObservable(
                generator,
                constructor, null,
                Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Exports a streamable as an observable of events. Produces events that are sync time ordered.
        /// Expects only start-edge and interval events in the stream, and constructs user-defined payloads as a result.
        /// </summary>
        /// <typeparam name="TPayload"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="container">The query container to which an egress point is being added.</param>
        /// <param name="identifier">A string that can uniquely identify the point of egress in the query.</param>
        /// <param name="stream"></param>
        /// <param name="constructor">Method description that constructs result data from the start time, end time, and the payload of each event.</param>
        /// <returns></returns>
        public static IObservable<ArraySegment<TResult>> RegisterTemporalArrayOutput<TPayload, TResult>(
            this QueryContainer container,
            IStreamable<Empty, TPayload> stream,
            Expression<Func<long, long, TPayload, TResult>> constructor,
            string identifier = null)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToTemporalArrayObservable(
                () => new TResult[Config.DataBatchSize],
                constructor, container,
                identifier ?? Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Exports a streamable as an observable of events. Produces events that are sync time ordered.
        /// Expects only start-edge and interval events in the stream, and constructs user-defined payloads as a result.
        /// </summary>
        /// <typeparam name="TPayload"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="container">The query container to which an egress point is being added.</param>
        /// <param name="identifier">A string that can uniquely identify the point of egress in the query.</param>
        /// <param name="stream"></param>
        /// <param name="generator">A function that returns an array that will be populated with stream results.</param>
        /// <param name="constructor">Method description that constructs result data from the start time, end time, and the payload of each event.</param>
        /// <returns></returns>
        public static IObservable<ArraySegment<TResult>> RegisterTemporalArrayOutput<TPayload, TResult>(
            this QueryContainer container,
            IStreamable<Empty, TPayload> stream,
            Func<TResult[]> generator,
            Expression<Func<long, long, TPayload, TResult>> constructor,
            string identifier = null)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToTemporalArrayObservable(
                generator,
                constructor, container,
                identifier ?? Guid.NewGuid().ToString());
        }

        internal static IObservable<ArraySegment<TResult>> ToTemporalArrayObservable<TPayload, TResult>(
            this IStreamable<Empty, TPayload> stream,
            Func<TResult[]> generator,
                 Expression<Func<long, long, TPayload, TResult>> constructor,
            QueryContainer container,
            string identifer)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return new IntervalArrayObservable<TPayload, TResult>(stream.ToEndEdgeFreeStream(), generator, constructor, container, identifer);
        }

        /// <summary>
        /// Exports a streamable as an observable of events. Produces events that are sync time ordered.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="stream"></param>
        /// <param name="reshapingPolicy">Policy that specifies whether and how events are reshaped at egress. Default passes events through unmodified.</param>
        /// <returns></returns>
        public static IObservable<ArraySegment<PartitionedStreamEvent<TKey, TPayload>>> ToStreamEventArrayObservable<TKey, TPayload>(
            this IStreamable<PartitionKey<TKey>, TPayload> stream,
            ReshapingPolicy reshapingPolicy = ReshapingPolicy.None)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToStreamEventArrayObservable(
                () => new PartitionedStreamEvent<TKey, TPayload>[Config.DataBatchSize],
                null,
                Guid.NewGuid().ToString(),
                reshapingPolicy);
        }

        /// <summary>
        /// Exports a streamable as an observable of events. Produces events that are sync time ordered.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="stream"></param>
        /// <param name="generator">A function that returns an array that will be populated with stream results.</param>
        /// <param name="reshapingPolicy">Policy that specifies whether and how events are reshaped at egress. Default passes events through unmodified.</param>
        /// <returns></returns>
        public static IObservable<ArraySegment<PartitionedStreamEvent<TKey, TPayload>>> ToStreamEventArrayObservable<TKey, TPayload>(
            this IStreamable<PartitionKey<TKey>, TPayload> stream,
            Func<PartitionedStreamEvent<TKey, TPayload>[]> generator,
            ReshapingPolicy reshapingPolicy = ReshapingPolicy.None)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToStreamEventArrayObservable(
                generator,
                null,
                Guid.NewGuid().ToString(),
                reshapingPolicy);
        }

        /// <summary>
        /// Exports a streamable as an observable of events. Produces events that are sync time ordered.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="container">The query container to which an egress point is being added.</param>
        /// <param name="identifier">A string that can uniquely identify the point of egress in the query.</param>
        /// <param name="stream"></param>
        /// <param name="reshapingPolicy">Policy that specifies whether and how events are reshaped at egress. Default passes events through unmodified.</param>
        /// <returns></returns>
        public static IObservable<ArraySegment<PartitionedStreamEvent<TKey, TPayload>>> RegisterArrayOutput<TKey, TPayload>(
            this QueryContainer container,
            IStreamable<PartitionKey<TKey>, TPayload> stream,
            ReshapingPolicy reshapingPolicy = ReshapingPolicy.None,
            string identifier = null)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToStreamEventArrayObservable(
                () => new PartitionedStreamEvent<TKey, TPayload>[Config.DataBatchSize],
                container,
                identifier ?? Guid.NewGuid().ToString(),
                reshapingPolicy);
        }

        /// <summary>
        /// Exports a streamable as an observable of events. Produces events that are sync time ordered.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="container">The query container to which an egress point is being added.</param>
        /// <param name="identifier">A string that can uniquely identify the point of egress in the query.</param>
        /// <param name="stream"></param>
        /// <param name="generator">A function that returns an array that will be populated with stream results.</param>
        /// <param name="reshapingPolicy">Policy that specifies whether and how events are reshaped at egress. Default passes events through unmodified.</param>
        /// <returns></returns>
        public static IObservable<ArraySegment<PartitionedStreamEvent<TKey, TPayload>>> RegisterArrayOutput<TKey, TPayload>(
            this QueryContainer container,
            IStreamable<PartitionKey<TKey>, TPayload> stream,
            Func<PartitionedStreamEvent<TKey, TPayload>[]> generator,
            ReshapingPolicy reshapingPolicy = ReshapingPolicy.None,
            string identifier = null)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToStreamEventArrayObservable(
                generator,
                container,
                identifier ?? Guid.NewGuid().ToString(),
                reshapingPolicy);
        }

        internal static IObservable<ArraySegment<PartitionedStreamEvent<TKey, TPayload>>> ToStreamEventArrayObservable<TKey, TPayload>(
            this IStreamable<PartitionKey<TKey>, TPayload> stream,
            Func<PartitionedStreamEvent<TKey, TPayload>[]> generator,
            QueryContainer container,
            string identifer,
            ReshapingPolicy reshapingPolicy)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return (reshapingPolicy == ReshapingPolicy.None)
                ? new PartitionedStreamEventArrayObservable<TKey, TPayload>(stream, generator, container, identifer)
                : new PartitionedStreamEventArrayObservable<TKey, TPayload>(stream.ToEndEdgeFreeStream(), generator, container, identifer);
        }

        /// <summary>
        /// Exports a streamable as an observable of events. Produces events that are sync time ordered.
        /// Expects only start-edge events in the stream, and constructs user-defined payloads as a result.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TPayload"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="stream"></param>
        /// <param name="constructor">Method description that constructs result data from the start time and the payload of each event.</param>
        /// <returns></returns>
        public static IObservable<ArraySegment<TResult>> ToTemporalArrayObservable<TKey, TPayload, TResult>(
            this IStreamable<PartitionKey<TKey>, TPayload> stream,
            Expression<Func<TKey, long, TPayload, TResult>> constructor)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToTemporalArrayObservable(
                () => new TResult[Config.DataBatchSize],
                constructor, null,
                Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Exports a streamable as an observable of events. Produces events that are sync time ordered.
        /// Expects only start-edge events in the stream, and constructs user-defined payloads as a result.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TPayload"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="stream"></param>
        /// <param name="generator">A function that returns an array that will be populated with stream results.</param>
        /// <param name="constructor">Method description that constructs result data from the start time and the payload of each event.</param>
        /// <returns></returns>
        public static IObservable<ArraySegment<TResult>> ToTemporalArrayObservable<TKey, TPayload, TResult>(
            this IStreamable<PartitionKey<TKey>, TPayload> stream,
            Func<TResult[]> generator,
            Expression<Func<TKey, long, TPayload, TResult>> constructor)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToTemporalArrayObservable(
                generator,
                constructor, null,
                Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Exports a streamable as an observable of events. Produces events that are sync time ordered.
        /// Expects only start-edge events in the stream, and constructs user-defined payloads as a result.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TPayload"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="container">The query container to which an egress point is being added.</param>
        /// <param name="identifier">A string that can uniquely identify the point of egress in the query.</param>
        /// <param name="stream"></param>
        /// <param name="constructor">Method description that constructs result data from the start time and the payload of each event.</param>
        /// <returns></returns>
        public static IObservable<ArraySegment<TResult>> RegisterTemporalArrayOutput<TKey, TPayload, TResult>(
            this QueryContainer container,
            IStreamable<PartitionKey<TKey>, TPayload> stream,
            Expression<Func<TKey, long, TPayload, TResult>> constructor,
            string identifier = null)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToTemporalArrayObservable(
                () => new TResult[Config.DataBatchSize],
                constructor, container,
                identifier ?? Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Exports a streamable as an observable of events. Produces events that are sync time ordered.
        /// Expects only start-edge events in the stream, and constructs user-defined payloads as a result.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TPayload"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="container">The query container to which an egress point is being added.</param>
        /// <param name="identifier">A string that can uniquely identify the point of egress in the query.</param>
        /// <param name="stream"></param>
        /// <param name="generator">A function that returns an array that will be populated with stream results.</param>
        /// <param name="constructor">Method description that constructs result data from the start time and the payload of each event.</param>
        /// <returns></returns>
        public static IObservable<ArraySegment<TResult>> RegisterTemporalArrayOutput<TKey, TPayload, TResult>(
            this QueryContainer container,
            IStreamable<PartitionKey<TKey>, TPayload> stream,
            Func<TResult[]> generator,
            Expression<Func<TKey, long, TPayload, TResult>> constructor,
            string identifier = null)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToTemporalArrayObservable(
                generator,
                constructor, container,
                identifier ?? Guid.NewGuid().ToString());
        }

        internal static IObservable<ArraySegment<TResult>> ToTemporalArrayObservable<TKey, TPayload, TResult>(
            this IStreamable<PartitionKey<TKey>, TPayload> stream,
            Func<TResult[]> generator,
                 Expression<Func<TKey, long, TPayload, TResult>> constructor,
            QueryContainer container,
            string identifer)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return new PartitionedStartEdgeArrayObservable<TKey, TPayload, TResult>(stream, generator, constructor, container, identifer);
        }

        /// <summary>
        /// Exports a streamable as an observable of events. Produces events that are sync time ordered.
        /// Expects only start-edge and interval events in the stream, and constructs user-defined payloads as a result.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TPayload"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="stream"></param>
        /// <param name="constructor">Method description that constructs result data from the start time, end time, and the payload of each event.</param>
        /// <returns></returns>
        public static IObservable<ArraySegment<TResult>> ToTemporalArrayObservable<TKey, TPayload, TResult>(
            this IStreamable<PartitionKey<TKey>, TPayload> stream,
            Expression<Func<TKey, long, long, TPayload, TResult>> constructor)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToTemporalArrayObservable(
                () => new TResult[Config.DataBatchSize],
                constructor, null,
                Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Exports a streamable as an observable of events. Produces events that are sync time ordered.
        /// Expects only start-edge and interval events in the stream, and constructs user-defined payloads as a result.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TPayload"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="stream"></param>
        /// <param name="generator">A function that returns an array that will be populated with stream results.</param>
        /// <param name="constructor">Method description that constructs result data from the start time, end time, and the payload of each event.</param>
        /// <returns></returns>
        public static IObservable<ArraySegment<TResult>> ToTemporalArrayObservable<TKey, TPayload, TResult>(
            this IStreamable<PartitionKey<TKey>, TPayload> stream,
            Func<TResult[]> generator,
            Expression<Func<TKey, long, long, TPayload, TResult>> constructor)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToTemporalArrayObservable(
                generator,
                constructor, null,
                Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Exports a streamable as an observable of events. Produces events that are sync time ordered.
        /// Expects only start-edge and interval events in the stream, and constructs user-defined payloads as a result.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TPayload"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="container">The query container to which an egress point is being added.</param>
        /// <param name="identifier">A string that can uniquely identify the point of egress in the query.</param>
        /// <param name="stream"></param>
        /// <param name="constructor">Method description that constructs result data from the start time, end time, and the payload of each event.</param>
        /// <returns></returns>
        public static IObservable<ArraySegment<TResult>> RegisterTemporalArrayOutput<TKey, TPayload, TResult>(
            this QueryContainer container,
            IStreamable<PartitionKey<TKey>, TPayload> stream,
            Expression<Func<TKey, long, long, TPayload, TResult>> constructor,
            string identifier = null)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToTemporalArrayObservable(
                () => new TResult[Config.DataBatchSize],
                constructor, container,
                identifier ?? Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Exports a streamable as an observable of events. Produces events that are sync time ordered.
        /// Expects only start-edge and interval events in the stream, and constructs user-defined payloads as a result.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TPayload"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="container">The query container to which an egress point is being added.</param>
        /// <param name="identifier">A string that can uniquely identify the point of egress in the query.</param>
        /// <param name="stream"></param>
        /// <param name="generator">A function that returns an array that will be populated with stream results.</param>
        /// <param name="constructor">Method description that constructs result data from the start time, end time, and the payload of each event.</param>
        /// <returns></returns>
        public static IObservable<ArraySegment<TResult>> RegisterTemporalArrayOutput<TKey, TPayload, TResult>(
            this QueryContainer container,
            IStreamable<PartitionKey<TKey>, TPayload> stream,
            Func<TResult[]> generator,
            Expression<Func<TKey, long, long, TPayload, TResult>> constructor,
            string identifier = null)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToTemporalArrayObservable(
                generator,
                constructor, container,
                identifier ?? Guid.NewGuid().ToString());
        }

        internal static IObservable<ArraySegment<TResult>> ToTemporalArrayObservable<TKey, TPayload, TResult>(
            this IStreamable<PartitionKey<TKey>, TPayload> stream,
            Func<TResult[]> generator,
                 Expression<Func<TKey, long, long, TPayload, TResult>> constructor,
            QueryContainer container,
            string identifer)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return new PartitionedIntervalArrayObservable<TKey, TPayload, TResult>(stream.ToEndEdgeFreeStream(), generator, constructor, container, identifer);
        }
    }
}