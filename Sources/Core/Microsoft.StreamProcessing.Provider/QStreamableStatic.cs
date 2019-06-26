// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace Microsoft.StreamProcessing.Provider
{
    /// <summary>
    ///
    /// </summary>
    public struct Empty
    { }

    /// <summary>
    /// The extension methods over interface IQStreamable
    /// </summary>
    public static class QStreamableStatic
    {
        /// <summary>
        /// Projects each element of a stream into a new form.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements in the source stream.</typeparam>
        /// <typeparam name="TResult">The type of the elements in the result stream, obtained by running the selector function for each element in the source sequence.</typeparam>
        /// <param name="source">A stream of elements to invoke a transform function on.</param>
        /// <param name="selector">A transform function to apply to each source element.</param>
        /// <returns>A stream whose elements are the result of invoking the transform function on each element of the source.</returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="source" /> or <paramref name="selector" /> is null.</exception>
        public static IQStreamable<TResult> Select<TSource, TResult>(this IQStreamable<TSource> source, Expression<Func<TSource, TResult>> selector)
            => source.Provider.CreateQuery<TResult>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TResult)),
                    (source ?? throw new ArgumentNullException(nameof(source))).Expression,
                    selector ?? throw new ArgumentNullException(nameof(selector))));

        /// <summary>
        /// Projects each element of a stream into a new form where each input element returns zero to many output elements.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements in the source stream.</typeparam>
        /// <typeparam name="TResult">The type of the elements in the result stream, obtained by running the selector function for each element in the source sequence.</typeparam>
        /// <param name="source">A stream of elements to invoke a transform function on.</param>
        /// <param name="selector">A transform function to apply to each source element.</param>
        /// <returns>A stream whose elements are the result of invoking the transform function on each element of the source.</returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="source" /> or <paramref name="selector" /> is null.</exception>
        public static IQStreamable<IEnumerable<TResult>> SelectMany<TSource, TResult>(
            this IQStreamable<TSource> source,
            Expression<Func<TSource, IEnumerable<TResult>>> selector)
            => source.Provider.CreateQuery<IEnumerable<TResult>>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TResult)),
                    (source ?? throw new ArgumentNullException(nameof(source))).Expression,
                    selector ?? throw new ArgumentNullException(nameof(selector))));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TLeft"></typeparam>
        /// <typeparam name="TRight"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="resultSelector"></param>
        /// <returns></returns>
        public static IQStreamable<TResult> SelectMany<TLeft, TRight, TResult>(
           this IQStreamable<TLeft> left,
           Expression<Func<Empty, IQStreamable<TRight>>> right,
           Expression<Func<TLeft, TRight, TResult>> resultSelector)
            => left.Provider.CreateQuery<TResult>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TLeft), typeof(TRight), typeof(TResult)),
                    (left ?? throw new ArgumentNullException(nameof(left))).Expression,
                    right ?? throw new ArgumentNullException(nameof(right)),
                    resultSelector ?? throw new ArgumentNullException(nameof(resultSelector))));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TLeft"></typeparam>
        /// <typeparam name="TRight"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="resultSelector"></param>
        /// <returns></returns>
        public static IQStreamable<TResult> SelectMany<TLeft, TRight, TResult>(
           this IQStreamable<TLeft> left,
           Expression<Func<TLeft, IEnumerable<TRight>>> right,
           Expression<Func<TLeft, TRight, TResult>> resultSelector)
            => left.Provider.CreateQuery<TResult>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TLeft), typeof(TRight), typeof(TResult)),
                    (left ?? throw new ArgumentNullException(nameof(left))).Expression,
                    right ?? throw new ArgumentNullException(nameof(right)),
                    resultSelector ?? throw new ArgumentNullException(nameof(resultSelector))));

        /// <summary>
        /// Filters the elements of a stream based on a predicate.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements in the source stream.</typeparam>
        /// <param name="source">A stream whose elements to filter.</param>
        /// <param name="predicate">A function to test each source element for a condition.</param>
        /// <returns>A stream that contains elements from the input stream that satisfy the condition.</returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="source" /> or <paramref name="predicate" /> is null.</exception>
        public static IQStreamable<TSource> Where<TSource>(this IQStreamable<TSource> source, Expression<Func<TSource, bool>> predicate)
            => source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)),
                    (source ?? throw new ArgumentNullException(nameof(source))).Expression,
                    predicate ?? throw new ArgumentNullException(nameof(predicate))));

        /// <summary>
        /// Groups the elements of a stream according to a specified key selector function.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements in the source stream.</typeparam>
        /// <typeparam name="TKey">The type of the grouping key computed for each element in the source stream.</typeparam>
        /// <param name="source">A stream whose elements to group.</param>
        /// <param name="keySelector">A function to extract the key for each element.</param>
        /// <returns>A stream of windowed groups, each of which corresponds to a unique key value, containing all elements that share that same key value.</returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="source" /> or <paramref name="keySelector" /> is null.</exception>
        public static IQStreamable<IGroupedWindow<TKey, TSource>> GroupBy<TSource, TKey>(
            this IQStreamable<TSource> source,
            Expression<Func<TSource, TKey>> keySelector)
            => source.Provider.CreateQuery<IGroupedWindow<TKey, TSource>>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TKey)),
                    (source ?? throw new ArgumentNullException(nameof(source))).Expression,
                    keySelector ?? throw new ArgumentNullException(nameof(keySelector))));

        /// <summary>
        /// Groups the elements of a stream according to a specified key selector function.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements in the source stream.</typeparam>
        /// <typeparam name="TKey">The type of the grouping key computed for each element in the source stream.</typeparam>
        /// <typeparam name="TElement">The type of the grouped elements contained in each window.</typeparam>
        /// <param name="source">A stream whose elements to group.</param>
        /// <param name="keySelector">A function to extract the key for each element.</param>
        /// <param name="elementSelector">A function to determine the element type of the window.</param>
        /// <returns>A stream of windowed groups, each of which corresponds to a unique key value, containing all projected elements that share that same key value.</returns>
        public static IQStreamable<IGroupedWindow<TKey, TElement>> GroupBy<TSource, TKey, TElement>(
            this IQStreamable<TSource> source,
            Expression<Func<TSource, TKey>> keySelector,
            Expression<Func<TSource, TElement>> elementSelector)
            => source.Provider.CreateQuery<IGroupedWindow<TKey, TElement>>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TKey), typeof(TElement)),
                    (source ?? throw new ArgumentNullException(nameof(source))).Expression,
                    elementSelector ?? throw new ArgumentNullException(nameof(elementSelector)),
                    keySelector ?? throw new ArgumentNullException(nameof(keySelector))));

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
        /// Adjusts the lifetime of incoming events to have the specified duration.
        /// </summary>
        /// <typeparam name="TSource">Type of payload in the stream</typeparam>
        /// <param name="source">Input stream</param>
        /// <param name="duration">The new duration for each event</param>
        /// <returns>Result (output) stream</returns>
        public static IQStreamable<TSource> AlterEventDuration<TSource>(this IQStreamable<TSource> source, long duration)
            => source.Provider.CreateQuery<TSource>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource)),
                    (source ?? throw new ArgumentNullException(nameof(source))).Expression,
                    Expression.Constant(duration, typeof(long))));

        /// <summary>
        /// Joins the elements of two streams based on overlapping lifespans.
        /// </summary>
        /// <typeparam name="TLeft">The type of the elements in the left source stream.</typeparam>
        /// <typeparam name="TRight">The type of the elements in the right source stream.</typeparam>
        /// <typeparam name="TKey">The type of the elements forming the join key.</typeparam>
        /// <typeparam name="TResult">The type of the elements in the result sequence, obtained by invoking the result selector function for source elements with overlapping duration.</typeparam>
        /// <param name="left">The left stream to join elements for.</param>
        /// <param name="right">The right stream to join elements for.</param>
        /// <param name="leftSelector">A function to select the join key of each element of the left stream.</param>
        /// <param name="rightSelector">A function to select the join key of each element of the right stream.</param>
        /// <param name="resultSelector">A function invoked to compute a result element for any two overlapping elements of the left and right observable sequences.</param>
        /// <returns>A stream that contains result elements computed from source elements that have an overlapping lifespan.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="left" /> or <paramref name="right" /> or <paramref name="leftSelector" /> or <paramref name="rightSelector" /> or <paramref name="resultSelector" /> is null.</exception>
        public static IQStreamable<TResult> Join<TLeft, TRight, TKey, TResult>(
            this IQStreamable<TLeft> left,
            IQStreamable<TRight> right,
            Expression<Func<TLeft, TKey>> leftSelector,
            Expression<Func<TRight, TKey>> rightSelector,
            Expression<Func<TLeft, TRight, TResult>> resultSelector)
            => left.Provider.CreateQuery<TResult>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TLeft), typeof(TRight), typeof(TKey), typeof(TResult)),
                    (left ?? throw new ArgumentNullException(nameof(left))).Expression,
                    (right ?? throw new ArgumentNullException(nameof(right))).Expression,
                    leftSelector ?? throw new ArgumentNullException(nameof(leftSelector)),
                    rightSelector ?? throw new ArgumentNullException(nameof(rightSelector)),
                    resultSelector ?? throw new ArgumentNullException(nameof(resultSelector))));
    }
}
