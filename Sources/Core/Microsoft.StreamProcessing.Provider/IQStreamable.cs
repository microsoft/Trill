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
    public interface IQStreamable<out TPayload>
    {
        /// <summary>
        ///
        /// </summary>
        Expression Expression { get; }

        /// <summary>
        ///
        /// </summary>
        IQStreamableProvider Provider { get; }
    }
}