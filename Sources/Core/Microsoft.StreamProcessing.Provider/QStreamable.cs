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
    /// <typeparam name="TPayload"></typeparam>
    public sealed class QStreamable<TPayload> : IQStreamable<TPayload>
    {
        internal QStreamable(Expression expression, IQStreamableProvider provider)
        {
            this.Expression = expression;
            this.Provider = provider;
        }

        /// <summary>
        ///
        /// </summary>
        public Expression Expression { get; }

        /// <summary>
        ///
        /// </summary>
        public IQStreamableProvider Provider { get; }
    }
}
