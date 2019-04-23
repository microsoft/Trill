// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Provider
{
    /// <summary>
    /// The core interface of the streaming provider API, representing a temporal or time-series stream of data
    /// </summary>
    /// <typeparam name="TPayload">The type of the payload of the underlying stream query.</typeparam>
    public interface IQStreamable<out TPayload>
    {
        /// <summary>
        /// The expression representing the full query so far in the pipeline.
        /// </summary>
        Expression Expression { get; }

        /// <summary>
        /// The assigned provider whose job it is to evaluate the query once it is constructed.
        /// </summary>
        IQStreamableProvider Provider { get; }
    }
}