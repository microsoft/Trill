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
    /// The extension methods over interface IQStreamable
    /// </summary>
    public static partial class QStreamableStatic
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
        public static IQStreamable<TResult> SelectMany<TSource, TResult>(
            this IQStreamable<TSource> source,
            Expression<Func<TSource, IEnumerable<TResult>>> selector)
            => source.Provider.CreateQuery<TResult>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TSource), typeof(TResult)),
                    (source ?? throw new ArgumentNullException(nameof(source))).Expression,
                    selector ?? throw new ArgumentNullException(nameof(selector))));

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
    }
}
