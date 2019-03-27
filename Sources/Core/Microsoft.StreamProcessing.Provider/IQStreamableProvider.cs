// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Provider
{
    /// <summary>
    ///
    /// </summary>
    public interface IQStreamableProvider
    {
        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TElement"></typeparam>
        /// <param name="expression"></param>
        /// <returns></returns>
        IQStreamable<TElement> CreateQuery<TElement>(Expression expression);

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="expression"></param>
        /// <returns></returns>
        TResult Execute<TResult>(Expression expression);
    }
}
