// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Aggregates
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
    /// Interface used by aggregates that support the ability to "sum" two states.
    /// </summary>
    public interface ISummableAggregate<TInput, TState, TResult> : IAggregate<TInput, TState, TResult>
    {
        /// <summary>
        /// Provides an expression that describes how to take two different aggregate states and combine them.
        /// </summary>
        /// <returns>An expression that describes how to take two different aggregate states and combine them.</returns>
        Expression<Func<TState, TState, TState>> Sum();
    }

    internal static class AggregateExtensions
    {
        public static bool HasSameStateAs<TInput, TState1, TState2, TResult1, TResult2>(this IAggregate<TInput, TState1, TResult1> left, IAggregate<TInput, TState2, TResult2> right)
            => typeof(TState1) == typeof(TState2)
                && left.Accumulate().ExpressionEquals(right.Accumulate())
                && left.Deaccumulate().ExpressionEquals(right.Deaccumulate())
                && left.Difference().ExpressionEquals(right.Difference())
                && left.InitialState().ExpressionEquals(right.InitialState());
    }
}
