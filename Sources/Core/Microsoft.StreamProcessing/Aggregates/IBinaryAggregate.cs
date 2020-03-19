// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Aggregates
{
    /// <summary>
    /// Interface used by all aggregates over two inputs.
    /// </summary>
    /// <typeparam name="TLeft">The type of the left input being aggregated.</typeparam>
    /// <typeparam name="TRight">The type of the right input being aggregated.</typeparam>
    /// <typeparam name="TState">The type of the state object used for intermediate computation.</typeparam>
    /// <typeparam name="TResult">The type of the computed result of the aggregation.</typeparam>
    public interface IBinaryAggregate<TLeft, TRight, TState, TResult>
    {
        /// <summary>
        /// Provides an expression that creates the initial state for the aggregate computation.
        /// </summary>
        /// <returns>An expression that creates the initial state for the aggregate computation.</returns>
        Expression<Func<TState>> InitialState();

        /// <summary>
        /// Provides an expression that describes how to take the aggregate state and a new data object from the left input and compute a new aggregate state.
        /// </summary>
        /// <returns>An expression that describes how to take the aggregate state and a new data object from the left input and compute a new aggregate state.</returns>
        Expression<Func<TState, long, TLeft, TState>> AccumulateLeft();

        /// <summary>
        /// Provides an expression that describes how to take the aggregate state and a new data object from the right input and compute a new aggregate state.
        /// </summary>
        /// <returns>An expression that describes how to take the aggregate state and a new data object from the right input and compute a new aggregate state.</returns>
        Expression<Func<TState, long, TRight, TState>> AccumulateRight();

        /// <summary>
        /// Provides an expression that describes how to take the aggregate state and a retracted data object from the left input and compute a new aggregate state.
        /// </summary>
        /// <returns>An expression that describes how to take the aggregate state and a retracted data object from the left input and compute a new aggregate state.</returns>
        Expression<Func<TState, long, TLeft, TState>> DeaccumulateLeft();

        /// <summary>
        /// Provides an expression that describes how to take the aggregate state and a retracted data object from the right input and compute a new aggregate state.
        /// </summary>
        /// <returns>An expression that describes how to take the aggregate state and a retracted data object from the right input and compute a new aggregate state.</returns>
        Expression<Func<TState, long, TRight, TState>> DeaccumulateRight();

        /// <summary>
        /// Provides an expression that describes how to take two different aggregate states and subtract one from the other.
        /// </summary>
        /// <returns>An expression that describes how to take two different aggregate states and subtract one from the other.</returns>
        Expression<Func<TState, TState, TState>> Difference();

        /// <summary>
        /// Provides an expression that describes how to compute a final result from an aggregate state.
        /// </summary>
        /// <returns>An expression that describes how to compute a final result from an aggregate state.</returns>
        Expression<Func<TState, TResult>> ComputeResult();
    }
}
