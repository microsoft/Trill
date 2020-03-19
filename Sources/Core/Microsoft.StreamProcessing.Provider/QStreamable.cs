// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Provider
{
    /// <summary>
    /// A concrete implementation of the IQStreamable interface for building streaming query expressions.
    /// </summary>
    /// <typeparam name="TPayload">The type of the payload of the underlying stream query.</typeparam>
    public sealed class QStreamable<TPayload> : IQStreamable<TPayload>
    {
        internal QStreamable(Expression expression, IQStreamableProvider provider)
        {
            this.Expression = expression;
            this.Provider = provider;
        }

        /// <summary>
        /// The expression representing the full query so far in the pipeline.
        /// </summary>
        public Expression Expression { get; }

        /// <summary>
        /// The assigned provider whose job it is to evaluate the query once it is constructed.
        /// </summary>
        public IQStreamableProvider Provider { get; }

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="expression"></param>
        /// <returns></returns>
        public IObservable<TResult> ToTemporalObservable<TResult>(Expression<Func<long, long, TPayload, TResult>> expression)
            => throw new System.NotImplementedException();
    }
}
