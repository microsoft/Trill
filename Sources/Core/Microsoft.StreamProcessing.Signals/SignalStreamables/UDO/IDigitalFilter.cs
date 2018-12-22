// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Signal.UDO
{
    /// <summary>
    /// Interface used by uniform signal digital filters.
    /// </summary>
    public interface IDigitalFilter<T>
    {
        /// <summary>
        /// Create an expression that evaluates to a boolean stating whether missing data should be initialized to null
        /// </summary>
        /// <returns>Expression that evaluates to a boolean stating whether missing data should be initialized to null</returns>
        Expression<Func<bool>> SetMissingDataToNull();

        /// <summary>
        /// Create an expression that returns the feed forward size of the digital filter
        /// </summary>
        /// <returns>Expression that returns the feed forward size of the digital filter</returns>
        Expression<Func<int>> GetFeedForwardSize();

        /// <summary>
        /// Create an expression that returns the feed backward size of the digital filter
        /// </summary>
        /// <returns>Expression that returns the feed backward size of the digital filter</returns>
        Expression<Func<int>> GetFeedBackwardSize();

        /// <summary>
        /// Create an expression that performs the computation over a window
        /// </summary>
        /// <returns>Expression that performs the computation over a window</returns>
        Expression<Func<BaseWindow<T>, BaseWindow<T>, T>> Compute();
    }
}