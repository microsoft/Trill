// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Provider
{
    /// <summary>
    /// Provides the base interface for the implementation of a stream provider.
    /// </summary>
    public interface IQStreamableProvider
    {
        /// <summary>
        /// This method represents a link in the chain for building new IQStreamable queries.
        /// </summary>
        /// <typeparam name="TElement">The type of the underlying payload.</typeparam>
        /// <param name="expression">The expression representing the streaming query that has been created.</param>
        /// <returns>A new IQStreamable object from which the query can continue to be built or evaluated.</returns>
        IQStreamable<TElement> CreateQuery<TElement>(Expression expression);

        /// <summary>
        /// The method that is called when it is time to evaluate the constructed query.
        /// </summary>
        /// <typeparam name="TResult">The type of the result of the streaming query.</typeparam>
        /// <param name="expression">The expression to be evaluated and run.</param>
        /// <returns>The result of evaluating the query expression.</returns>
        TResult Execute<TResult>(Expression expression);
    }
}
