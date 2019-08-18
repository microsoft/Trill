// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Interface used by all aggregates.
    /// </summary>
    /// <typeparam name="TInput">The type of the input being aggregated.</typeparam>
    /// <typeparam name="TState">The type of the state object used for intermediate computation.</typeparam>
    /// <typeparam name="TResult">The type of the computed result of the aggregation.</typeparam>
    public interface IAggregate<TInput, TState, TResult>
    {
        /// <summary>
        /// Provides an expression that creates the initial state for the aggregate computation.
        /// </summary>
        /// <returns>An expression that creates the initial state for the aggregate computation.</returns>
        Expression<Func<TState>> InitialState();

        /// <summary>
        /// Provides an expression that describes how to take the aggregate state and a new data object and compute a new aggregate state.
        /// </summary>
        /// <returns>An expression that describes how to take the aggregate state and a new data object and compute a new aggregate state.</returns>
        Expression<Func<TState, long, TInput, TState>> Accumulate();

        /// <summary>
        /// Provides an expression that describes how to take the aggregate state and a retracted data object and compute a new aggregate state.
        /// </summary>
        /// <returns>An expression that describes how to take the aggregate state and a retracted data object and compute a new aggregate state.</returns>
        Expression<Func<TState, long, TInput, TState>> Deaccumulate();

        /// <summary>
        /// Provides an expression that describes how to take two different aggregate states and combine them.
        /// </summary>
        /// <returns>An expression that describes how to take two different aggregate states and combine them.</returns>
        Expression<Func<TState, TState, TState>> Sum();

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

    /// <summary>
    /// TODO: Determine if this class should be made internal
    /// </summary>
    public static class AggregateExtensions
    {
        /// <summary>
        /// Given two aggregates, determines whether they are identical in how they manage state. Specifically:
        /// - The have the same state type
        /// - They have the same expressions for state creation, accumulation, deaccumulation, summation, and difference
        /// </summary>
        /// <typeparam name="TInput"></typeparam>
        /// <typeparam name="TState1">The state type of the left aggregate</typeparam>
        /// <typeparam name="TState2">The state type of the right aggregate</typeparam>
        /// <typeparam name="TResult1">The result type of the left aggregate</typeparam>
        /// <typeparam name="TResult2">The result type of the right aggregate</typeparam>
        /// <param name="left">The first of two aggregates to test for identical state management</param>
        /// <param name="right">The second of two aggregates to test for identical state management</param>
        /// <returns>True if the two aggregates have the same state management operations (are the same aggregate except for the logic for returning values), false otherwise</returns>
        public static bool HasSameStateAs<TInput, TState1, TState2, TResult1, TResult2>(this IAggregate<TInput, TState1, TResult1> left, IAggregate<TInput, TState2, TResult2> right)
            => typeof(TState1) == typeof(TState2)
                && left.Accumulate().ExpressionEquals(right.Accumulate())
                && left.Deaccumulate().ExpressionEquals(right.Deaccumulate())
                && left.Sum().ExpressionEquals(right.Sum())
                && left.Difference().ExpressionEquals(right.Difference())
                && left.InitialState().ExpressionEquals(right.InitialState());
    }
}
