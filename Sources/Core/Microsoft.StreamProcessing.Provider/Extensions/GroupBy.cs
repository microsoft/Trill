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
    /// The extension methods over interface IQStreamable
    /// </summary>
    public static partial class QStreamableStatic
    {
        /// <summary>
        /// Groups the elements of a stream according to a specified key selector function.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements in the source stream.</typeparam>
        /// <typeparam name="TKey">The type of the grouping key computed for each element in the source stream returned by <paramref name="keySelector"/>.</typeparam>
        /// <typeparam name="TElement">The type of the grouped elements contained in each window returned by <paramref name="elementSelector"/>.</typeparam>
        /// <typeparam name="TResult">The type of the value returned by the <paramref name="resultSelector"/>.</typeparam>
        /// <param name="source">A stream whose elements are to be grouped.</param>
        /// <param name="keySelector">A function to extract the key for each element.</param>
        /// <param name="elementSelector">A function to map each source element to an element in the group.</param>
        /// <param name="resultSelector">A function to create a result value from each group.</param>
        /// <returns>A stream of windowed groups, each of which corresponds to a unique key value, containing all projected elements that share that same key value.</returns>
        public static IQStreamable<IGroupedWindow<TKey, TElement>> GroupBy<TSource, TKey, TElement, TResult>(
            this IQStreamable<TSource> source,
            Expression<Func<TSource, TKey>> keySelector,
            Expression<Func<TSource, TElement>> elementSelector,
            Expression<Func<TKey, IWindow<TElement>, TResult>> resultSelector)

            // => source.GroupBy(keySelector, elementSelector).Select(g => resultSelector(g.Key, g.Window));
            => throw new NotImplementedException();
    }
}
