// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

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
    public static partial class QStreamableStatic
    {
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
            Func<Empty, IQStreamable<TRight>> right,
            Expression<Func<TLeft, TRight, TResult>> resultSelector)
            => Join(left, right(default), o => true, o => true, resultSelector);

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
        {
            if (typeof(TRight) == typeof(TResult) && resultSelector.Body is ParameterExpression p && p == resultSelector.Parameters[1])
            {
                return SelectMany(left, (Expression<Func<TLeft, IEnumerable<TResult>>>)(object)right);
            }

            throw new NotImplementedException();
        }

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
            => GroupBy(source, keySelector, element => element);
    }
}
