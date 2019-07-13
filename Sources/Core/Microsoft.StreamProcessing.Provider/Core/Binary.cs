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
        /// Passes a truncated version of each event on the left, where the left event is truncated by the first event on the right
        /// whose Vs occurs after the event on the left, whose join condition is met, and where the keys for both streams match. A
        /// fast join key comparer is passed in for efficiency.
        /// </summary>
        /// <typeparam name="TLeft">The type of the elements in the left source stream.</typeparam>
        /// <typeparam name="TRight">The type of the elements in the right source stream.</typeparam>
        /// <typeparam name="TKey">The type of the elements forming the join key.</typeparam>
        /// <param name="left">The left stream to join elements for.</param>
        /// <param name="right">The right stream to join elements for.</param>
        /// <param name="leftSelector">A function to select the join key of each element of the left stream.</param>
        /// <param name="rightSelector">A function to select the join key of each element of the right stream.</param>
        /// <returns>A stream that contains result elements from the left whose timelines are truncated by elements from the right.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="left" /> or <paramref name="right" /> or <paramref name="leftSelector" /> or <paramref name="rightSelector" /> is null.</exception>
        public static IQStreamable<TLeft> ClipDuration<TLeft, TRight, TKey>(
            this IQStreamable<TLeft> left,
            IQStreamable<TRight> right,
            Expression<Func<TLeft, TKey>> leftSelector,
            Expression<Func<TRight, TKey>> rightSelector)
            => left.Provider.CreateQuery<TLeft>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TLeft), typeof(TRight), typeof(TKey)),
                    (left ?? throw new ArgumentNullException(nameof(left))).Expression,
                    (right ?? throw new ArgumentNullException(nameof(right))).Expression,
                    leftSelector ?? throw new ArgumentNullException(nameof(leftSelector)),
                    rightSelector ?? throw new ArgumentNullException(nameof(rightSelector))));

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

        /// <summary>
        /// Performs a union of the elements of two streams.
        /// </summary>
        /// <typeparam name="TElement">The type of the elements in each source stream.</typeparam>
        /// <param name="left">The left stream to union elements from.</param>
        /// <param name="right">The right stream to union elements from.</param>
        /// <returns>A stream that contains all elements from each stream.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="left" /> or <paramref name="right" /> is null.</exception>
        public static IQStreamable<TElement> Union<TElement>(
            this IQStreamable<TElement> left,
            IQStreamable<TElement> right)
            => left.Provider.CreateQuery<TElement>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TElement)),
                    (left ?? throw new ArgumentNullException(nameof(left))).Expression,
                    (right ?? throw new ArgumentNullException(nameof(right))).Expression));

        /// <summary>
        /// Performs a left anti-semijoin on the elements of two streams based on overlapping lifespans.
        /// </summary>
        /// <typeparam name="TLeft">The type of the elements in the left source stream.</typeparam>
        /// <typeparam name="TRight">The type of the elements in the right source stream.</typeparam>
        /// <typeparam name="TKey">The type of the elements forming the join key.</typeparam>
        /// <param name="left">The left stream to join elements for.</param>
        /// <param name="right">The right stream to join elements for.</param>
        /// <param name="leftSelector">A function to select the join key of each element of the left stream.</param>
        /// <param name="rightSelector">A function to select the join key of each element of the right stream.</param>
        /// <returns>A stream that contains result elements computed from source elements that have left but not right elements at each lifespan.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="left" /> or <paramref name="right" /> or <paramref name="leftSelector" /> or <paramref name="rightSelector" /> is null.</exception>
        public static IQStreamable<TLeft> WhereNotExists<TLeft, TRight, TKey>(
            this IQStreamable<TLeft> left,
            IQStreamable<TRight> right,
            Expression<Func<TLeft, TKey>> leftSelector,
            Expression<Func<TRight, TKey>> rightSelector)
            => left.Provider.CreateQuery<TLeft>(
                Expression.Call(
                    null,
                    ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(TLeft), typeof(TRight), typeof(TKey)),
                    (left ?? throw new ArgumentNullException(nameof(left))).Expression,
                    (right ?? throw new ArgumentNullException(nameof(right))).Expression,
                    leftSelector ?? throw new ArgumentNullException(nameof(leftSelector)),
                    rightSelector ?? throw new ArgumentNullException(nameof(rightSelector))));
    }
}
