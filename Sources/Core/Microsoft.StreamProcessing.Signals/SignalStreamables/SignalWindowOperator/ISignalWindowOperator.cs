// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Signal.UDO
{
    /// <summary>
    /// Interface used by uniform signal window operators.
    /// </summary>
    /// <typeparam name="TSource">Source data type for the window operator</typeparam>
    /// <typeparam name="TResult">Result data type for the window operator</typeparam>
    public interface ISignalWindowOperator<TSource, TResult>
    {
        /// <summary>
        /// Factory for generating an expression that provides the output window size
        /// </summary>
        /// <returns>An expression that provides the output window size</returns>
        Expression<Func<int>> GetOutputWindowSize();

        /// <summary>
        /// Factory for generating an expression that describes window updating
        /// </summary>
        /// <returns>An expression that describes window updating</returns>
        Expression<Func<BaseWindow<TSource>, BaseWindow<TResult>, BaseWindow<TResult>>> Update();

        /// <summary>
        /// Factory for generating an expression that describes window recomputation
        /// </summary>
        /// <returns>An expression that describes window recomputation</returns>
        Expression<Func<BaseWindow<TSource>, BaseWindow<TResult>, BaseWindow<TResult>>> Recompute();
    }

}
