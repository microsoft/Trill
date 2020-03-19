// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using System.Reflection;

namespace Microsoft.StreamProcessing.Provider
{
    /// <summary>
    /// The extension methods over interface IQStreamable
    /// </summary>
    public static partial class QStreamableStatic
    {
        /// <summary>
        /// Divides long-lasting events into smaller, repeated events of the given period with the given offset.
        /// </summary>
        /// <typeparam name="TSource">Type of payload in the stream</typeparam>
        /// <param name="source">Input stream</param>
        /// <param name="period">The new duration for each event.</param>
        /// <param name="offset">The offset at which to apply the temporal division.</param>
        /// <returns>Result (output) stream</returns>
        public static IQStreamable<TSource> Chop<TSource>(this IQStreamable<TSource> source, long period, long offset = 0)
            => source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)),
                    (source ?? throw new ArgumentNullException(nameof(source))).Expression,
                    Expression.Constant(period, typeof(long)),
                    Expression.Constant(offset, typeof(long))));

        /// <summary>
        /// Truncates the duration of each event in the stream to a maximum of the given value.
        /// </summary>
        /// <typeparam name="TSource">Type of payload in the stream</typeparam>
        /// <param name="source">Input stream</param>
        /// <param name="maximumDuration">The maximum duration of each event</param>
        /// <returns>Result (output) stream</returns>
        public static IQStreamable<TSource> ClipDuration<TSource>(this IQStreamable<TSource> source, long maximumDuration)
            => source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)),
                    (source ?? throw new ArgumentNullException(nameof(source))).Expression,
                    Expression.Constant(maximumDuration, typeof(long))));

        /// <summary>
        /// Extends (or contracts) the durations of all events in the stream by a given amount. Durations that contract to zero or less are deleted.
        /// </summary>
        /// <typeparam name="TSource">Type of payload in the stream</typeparam>
        /// <param name="source">Input stream</param>
        /// <param name="duration">The amount to extend (or contract) the duration for each event</param>
        /// <returns>Result (output) stream</returns>
        public static IQStreamable<TSource> ExtendDuration<TSource>(this IQStreamable<TSource> source, long duration)
            => source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)),
                    (source ?? throw new ArgumentNullException(nameof(source))).Expression,
                    Expression.Constant(duration, typeof(long))));

        /// <summary>
        /// Edits the lifespan of each event to be a single point (interval of length one) whose start time is the input's end time.
        /// </summary>
        /// <typeparam name="TSource">Type of payload in the stream</typeparam>
        /// <param name="source">Input stream</param>
        /// <returns>Result (output) stream</returns>
        public static IQStreamable<TSource> PointAtEnd<TSource>(this IQStreamable<TSource> source)
            => source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)),
                    (source ?? throw new ArgumentNullException(nameof(source))).Expression));

        /// <summary>
        /// Adjusts the lifetime of incoming events to snap the start and end time of each event to quantized boundaries.
        /// The function is similar to a hopping lifetime expression, except that all start edges are either moved
        /// earlier or stay the same, and all end edges either move later or stay the same.
        /// </summary>
        /// <typeparam name="TSource">Type of payload in the stream</typeparam>
        /// <param name="source">Input stream</param>
        /// <param name="windowSize">Window size</param>
        /// <param name="period">Period (or hop size)</param>
        /// <param name="offset">Offset from the start of time</param>
        /// <returns>Result (output) stream</returns>
        public static IQStreamable<TSource> QuantizeLifetime<TSource>(this IQStreamable<TSource> source, long windowSize, long period, long offset = 0)
            => source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)),
                    (source ?? throw new ArgumentNullException(nameof(source))).Expression,
                    Expression.Constant(windowSize, typeof(long)),
                    Expression.Constant(period, typeof(long)),
                    Expression.Constant(offset, typeof(long))));

        /// <summary>
        /// Sets the lifetime of incoming events to the specified duration.
        /// </summary>
        /// <typeparam name="TSource">Type of payload in the stream</typeparam>
        /// <param name="source">Input stream</param>
        /// <param name="duration">The new duration for each event</param>
        /// <returns>Result (output) stream</returns>
        public static IQStreamable<TSource> SetDuration<TSource>(this IQStreamable<TSource> source, long duration)
            => source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)),
                    (source ?? throw new ArgumentNullException(nameof(source))).Expression,
                    Expression.Constant(duration, typeof(long))));

        /// <summary>
        /// Unifies adjacent identical payloads into a single consistent lifespan.
        /// </summary>
        /// <typeparam name="TSource">Type of payload in the stream</typeparam>
        /// <param name="source">Input stream</param>
        /// <returns>Result (output) stream</returns>
        public static IQStreamable<TSource> Stitch<TSource>(this IQStreamable<TSource> source)
            => source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)),
                    (source ?? throw new ArgumentNullException(nameof(source))).Expression));
    }
}
