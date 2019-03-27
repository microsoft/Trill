// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Provider
{
    /// <summary>
    ///
    /// </summary>
    public abstract class QStreamableProviderBase : IQStreamableProvider
    {
        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TElement"></typeparam>
        /// <param name="expression"></param>
        /// <returns></returns>
        public IQStreamable<TElement> CreateQuery<TElement>(Expression expression)
            => new QStreamable<TElement>(expression, this);

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="expression"></param>
        /// <returns></returns>
        public TResult Execute<TResult>(Expression expression) => throw new NotImplementedException();
    }
}
