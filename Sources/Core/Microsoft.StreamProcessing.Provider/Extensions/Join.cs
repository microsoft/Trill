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
        {
            var parameter = Expression.Parameter(typeof(ValueTuple<TLeft, TRight>));
            Expression<Func<ValueTuple<TLeft, TRight>, TLeft>> leftItem = (v) => v.Item1;
            Expression<Func<ValueTuple<TLeft, TRight>, TRight>> rightItem = (v) => v.Item2;
            var newBody = resultSelector.ReplaceParametersInBody(
                leftItem.ReplaceParametersInBody(parameter),
                rightItem.ReplaceParametersInBody(parameter));
            return left.Join(right, leftSelector, rightSelector)
                .Select(Expression.Lambda<Func<ValueTuple<TLeft, TRight>, TResult>>(newBody, parameter));
        }
    }
}
