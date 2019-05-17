// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Provider
{
    /// <summary>
    /// Provides the base implementation for a Stream provider.
    /// </summary>
    public abstract class QStreamableProviderBase : IQStreamableProvider
    {
        /// <summary>
        /// This method represents a link in the chain for building new IQStreamable queries.
        /// </summary>
        /// <typeparam name="TElement">The type of the underlying payload.</typeparam>
        /// <param name="expression">The expression representing the streaming query that has been created.</param>
        /// <returns>A new IQStreamable object from which the query can continue to be built or evaluated.</returns>
        public IQStreamable<TElement> CreateQuery<TElement>(Expression expression)
            => new QStreamable<TElement>(expression, this);

        /// <summary>
        /// The method that is called when it is time to evaluate the constructed query.
        /// </summary>
        /// <typeparam name="TResult">The type of the result of the streaming query.</typeparam>
        /// <param name="expression">The expression to be evaluated and run.</param>
        /// <returns>The result of evaluating the query expression.</returns>
        public TResult Execute<TResult>(Expression expression) => throw new NotImplementedException();
    }
}
