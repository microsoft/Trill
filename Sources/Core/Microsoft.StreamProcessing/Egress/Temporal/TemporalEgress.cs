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
        public static IObservable<StreamEvent<TPayload>> ToStreamEventObservable<TPayload>(
            this IStreamable<Empty, TPayload> stream,
            ReshapingPolicy reshapingPolicy = ReshapingPolicy.None)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToStreamEventObservable(
                null, Guid.NewGuid().ToString(), reshapingPolicy);
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
        public static IObservable<StreamEvent<TPayload>> RegisterOutput<TPayload>(
            this QueryContainer container,
            IStreamable<Empty, TPayload> stream,
            ReshapingPolicy reshapingPolicy = ReshapingPolicy.None,
            string identifier = null)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToStreamEventObservable(
                container, identifier ?? Guid.NewGuid().ToString(), reshapingPolicy);
        }

        internal static IObservable<StreamEvent<TPayload>> ToStreamEventObservable<TPayload>(
            this IStreamable<Empty, TPayload> stream,
            QueryContainer container,
            string identifier,
            ReshapingPolicy reshapingPolicy)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            if (stream is IFusibleStreamable<Empty, TPayload> f && f.CanFuseEgressObservable)
            {
                if (reshapingPolicy == ReshapingPolicy.None)
                {
                    return f.FuseEgressObservable((s, e, p, k) => new StreamEvent<TPayload>(s, e, p), container, identifier);
                }
            }

            return (reshapingPolicy == ReshapingPolicy.None)
                ? new StreamEventObservable<TPayload>(stream, container, identifier)
                : new StreamEventObservable<TPayload>(stream.ToEndEdgeFreeStream(), container, identifier);
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
        public static IObservable<TResult> ToTemporalObservable<TPayload, TResult>(
            this IStreamable<Empty, TPayload> stream,
            Expression<Func<long, TPayload, TResult>> constructor)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToTemporalObservable(
                constructor, null, Guid.NewGuid().ToString());
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
        public static IObservable<TResult> RegisterTemporalOutput<TPayload, TResult>(
            this QueryContainer container,
            IStreamable<Empty, TPayload> stream,
            Expression<Func<long, TPayload, TResult>> constructor,
            string identifier = null)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToTemporalObservable(
                constructor, container, identifier ?? Guid.NewGuid().ToString());
        }

        internal static IObservable<TResult> ToTemporalObservable<TPayload, TResult>(
            this IStreamable<Empty, TPayload> stream,
            Expression<Func<long, TPayload, TResult>> constructor,
            QueryContainer container,
            string identifier)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            if (stream is IFusibleStreamable<Empty, TPayload> f && f.CanFuseEgressObservable)
            {
                if (stream.Properties.IsStartEdgeOnly)
                {
                    return f.FuseEgressObservable(Expression.Lambda<Func<long, long, TPayload, Empty, TResult>>(constructor.Body, constructor.Parameters[0], Expression.Parameter(typeof(long)), constructor.Parameters[1], Expression.Parameter(typeof(Empty))), container, identifier);
                }
            }

            return new StartEdgeObservable<TPayload, TResult>(stream, constructor, container, identifier);
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
        public static IObservable<TResult> ToTemporalObservable<TPayload, TResult>(
            this IStreamable<Empty, TPayload> stream,
            Expression<Func<long, long, TPayload, TResult>> constructor)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToTemporalObservable(
                constructor, null, Guid.NewGuid().ToString());
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
        public static IObservable<TResult> RegisterTemporalOutput<TPayload, TResult>(
            this QueryContainer container,
            IStreamable<Empty, TPayload> stream,
            Expression<Func<long, long, TPayload, TResult>> constructor,
            string identifier = null)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToTemporalObservable(
                constructor, container, identifier ?? Guid.NewGuid().ToString());
        }

        internal static IObservable<TResult> ToTemporalObservable<TPayload, TResult>(
            this IStreamable<Empty, TPayload> stream,
            Expression<Func<long, long, TPayload, TResult>> constructor,
            QueryContainer container,
            string identifier)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            if (stream is IFusibleStreamable<Empty, TPayload> f && f.CanFuseEgressObservable)
            {
                // Optimization for the case where the user is going to drop the temporal aspects
                // or constant duration tells us that we only have intervals
                if (stream.Properties.IsConstantDuration
                    || (typeof(TPayload) == typeof(TResult)
                    && constructor.Body.NodeType == ExpressionType.Parameter
                    && constructor.Body.Equals(constructor.Parameters[2])))
                    return f.FuseEgressObservable(Expression.Lambda<Func<long, long, TPayload, Empty, TResult>>(constructor.Body, constructor.Parameters[0], constructor.Parameters[1], constructor.Parameters[2], Expression.Parameter(typeof(Empty))), container, identifier);

            }

            // Optimization for the case where the user is going to drop the temporal aspects
            if (typeof(TPayload) == typeof(TResult)
                && constructor.Body.NodeType == ExpressionType.Parameter
                && constructor.Body.Equals(constructor.Parameters[2]))
                return new ReactiveObservable<TPayload>(stream, container, identifier) as IObservable<TResult>;

            return new IntervalObservable<TPayload, TResult>(stream.ToEndEdgeFreeStream(), constructor, container, identifier);
        }

        /// <summary>
        /// Exports a streamable as an observable of events. Produces events that are sync time ordered.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="stream"></param>
        /// <param name="reshapingPolicy">Policy that specifies whether and how events are reshaped at egress. Default passes events through unmodified.</param>
        /// <returns></returns>
        public static IObservable<PartitionedStreamEvent<TKey, TPayload>> ToStreamEventObservable<TKey, TPayload>(
            this IStreamable<PartitionKey<TKey>, TPayload> stream,
            ReshapingPolicy reshapingPolicy = ReshapingPolicy.None)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToStreamEventObservable(
                null, Guid.NewGuid().ToString(), reshapingPolicy);
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
        public static IObservable<PartitionedStreamEvent<TKey, TPayload>> RegisterOutput<TKey, TPayload>(
            this QueryContainer container,
            IStreamable<PartitionKey<TKey>, TPayload> stream,
            ReshapingPolicy reshapingPolicy = ReshapingPolicy.None,
            string identifier = null)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToStreamEventObservable(
                container, identifier ?? Guid.NewGuid().ToString(), reshapingPolicy);
        }

        internal static IObservable<PartitionedStreamEvent<TKey, TPayload>> ToStreamEventObservable<TKey, TPayload>(
            this IStreamable<PartitionKey<TKey>, TPayload> stream,
            QueryContainer container,
            string identifier,
            ReshapingPolicy reshapingPolicy)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            if (stream is IFusibleStreamable<PartitionKey<TKey>, TPayload> f && f.CanFuseEgressObservable)
            {
                if (reshapingPolicy == ReshapingPolicy.None)
                {
                    return f.FuseEgressObservable((s, e, p, k) => new PartitionedStreamEvent<TKey, TPayload>(k.Key, s, e, p), container, identifier);
                }
            }

            return (reshapingPolicy == ReshapingPolicy.None)
                ? new PartitionedStreamEventObservable<TKey, TPayload>(stream, container, identifier)
                : new PartitionedStreamEventObservable<TKey, TPayload>(stream.ToEndEdgeFreeStream(), container, identifier);
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
        public static IObservable<TResult> ToTemporalObservable<TKey, TPayload, TResult>(
            this IStreamable<PartitionKey<TKey>, TPayload> stream,
            Expression<Func<TKey, long, TPayload, TResult>> constructor)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToTemporalObservable(
                constructor, null, Guid.NewGuid().ToString());
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
        public static IObservable<TResult> RegisterTemporalOutput<TKey, TPayload, TResult>(
            this QueryContainer container,
            IStreamable<PartitionKey<TKey>, TPayload> stream,
            Expression<Func<TKey, long, TPayload, TResult>> constructor,
            string identifier = null)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToTemporalObservable(
                constructor, container, identifier ?? Guid.NewGuid().ToString());
        }

        internal static IObservable<TResult> ToTemporalObservable<TKey, TPayload, TResult>(
            this IStreamable<PartitionKey<TKey>, TPayload> stream,
            Expression<Func<TKey, long, TPayload, TResult>> constructor,
            QueryContainer container,
            string identifier)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            if (stream is IFusibleStreamable<PartitionKey<TKey>, TPayload> f && f.CanFuseEgressObservable)
            {
                if (stream.Properties.IsStartEdgeOnly)
                {
                    Expression<Func<PartitionKey<TKey>, TKey>> lower = (o) => o.Key;
                    var newBody = constructor.ReplaceParametersInBody(lower.Body);
                    var newFunc = Expression.Lambda<Func<long, long, TPayload, PartitionKey<TKey>, TResult>>(
                        newBody,
                        constructor.Parameters[1],
                        Expression.Parameter(typeof(long)),
                        constructor.Parameters[2],
                        lower.Parameters[0]);
                    return f.FuseEgressObservable(newFunc, container, identifier);
                }
            }

            return new PartitionedStartEdgeObservable<TKey, TPayload, TResult>(stream, constructor, container, identifier);
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
        public static IObservable<TResult> ToTemporalObservable<TKey, TPayload, TResult>(
            this IStreamable<PartitionKey<TKey>, TPayload> stream,
            Expression<Func<TKey, long, long, TPayload, TResult>> constructor)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToTemporalObservable(
                constructor, null, Guid.NewGuid().ToString());
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
        public static IObservable<TResult> RegisterTemporalOutput<TKey, TPayload, TResult>(
            this QueryContainer container,
            IStreamable<PartitionKey<TKey>, TPayload> stream,
            Expression<Func<TKey, long, long, TPayload, TResult>> constructor,
            string identifier = null)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToTemporalObservable(
                constructor, container, identifier ?? Guid.NewGuid().ToString());
        }

        internal static IObservable<TResult> ToTemporalObservable<TKey, TPayload, TResult>(
            this IStreamable<PartitionKey<TKey>, TPayload> stream,
            Expression<Func<TKey, long, long, TPayload, TResult>> constructor,
            QueryContainer container,
            string identifier)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            if (stream is IFusibleStreamable<PartitionKey<TKey>, TPayload> f && f.CanFuseEgressObservable)
            {
                if (stream.Properties.IsConstantDuration)
                {
                    Expression<Func<PartitionKey<TKey>, TKey>> lower = (o) => o.Key;
                    var newBody = constructor.ReplaceParametersInBody(lower.Body);
                    var newFunc = Expression.Lambda<Func<long, long, TPayload, PartitionKey<TKey>, TResult>>(
                        newBody,
                        constructor.Parameters[1],
                        constructor.Parameters[2],
                        constructor.Parameters[3],
                        lower.Parameters[0]);
                    return f.FuseEgressObservable(newFunc, container, identifier);
                }
            }

            return new PartitionedIntervalObservable<TKey, TPayload, TResult>(stream.ToEndEdgeFreeStream(), constructor, container, identifier);
        }

    }
}