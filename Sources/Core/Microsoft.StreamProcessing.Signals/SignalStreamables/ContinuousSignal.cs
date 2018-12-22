// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Signal
{
    /// <summary>
    /// Wrapper class for holding a signal comprising a function and inputs to that function
    /// </summary>
    /// <typeparam name="TKey">Key type</typeparam>
    /// <typeparam name="TSource">Source data type</typeparam>
    /// <typeparam name="TResult">Result data type</typeparam>
    public sealed class ContinuousSignal<TKey, TSource, TResult>
    {
        internal IStreamable<TKey, TSource> Stream { get; private set; }
        internal Expression<Func<long, TSource, TResult>> Function { get; private set; }

        internal ContinuousSignal(IStreamable<TKey, TSource> stream, Expression<Func<long, TSource, TResult>> function)
        {
            Contract.Requires(stream != null);

            this.Stream = stream;
            this.Function = function;
        }

        /// <summary>
        /// Performs a project over a signal.
        /// </summary>
        public ContinuousSignal<TKey, TSource, TNewResult> Select<TNewResult>(Expression<Func<TResult, TNewResult>> selector)
        {
            var newBody = selector.ReplaceParametersInBody(Function.Body);
            var newFunction = Expression.Lambda<Func<long, TSource, TNewResult>>(newBody, Function.Parameters);

            return new ContinuousSignal<TKey, TSource, TNewResult>(Stream, newFunction);
        }

        /// <summary>
        /// Performs a scaling operation over a signal
        /// </summary>
        /// <param name="factor">The factor by which the signal should be scaled</param>
        /// <returns>The scaled signal</returns>
        public ContinuousSignal<TKey, TSource, TResult> Scale(double factor)
        {
            Invariant.IsTrue(typeof(TResult).SupportsOperator("*d"), "Type " + typeof(TResult) + " does not support the multiplication operator with second argument " + typeof(double) + ".");
            if (factor == 1.0) { return this; }

            var leftParam = Expression.Parameter(typeof(TResult));
            var rightParam = Expression.Constant(factor);
            var multiply = Expression.Multiply(leftParam, rightParam);
            return this.Select(Expression.Lambda<Func<TResult, TResult>>(multiply, leftParam));
        }
    }
}
