// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************

using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;
using Microsoft.StreamProcessing.Internal;

namespace Microsoft.StreamProcessing.Aggregates
{
    /// <summary>
    /// Methods to combine individual aggregates into a single, state-efficient one
    /// </summary>
    public static partial class AggregateFunctions
    {

        /// <summary>
        /// Performs multiple aggregations simultaneously
        /// </summary>
        /// <typeparam name="TInput">The input stream to aggregate</typeparam>
        /// <typeparam name="TState1">Aggregation state type for aggregate number 1</typeparam>
        /// <typeparam name="TResult1">Result type for aggregate number 1</typeparam>
        /// <typeparam name="TResult">Result type of the merged aggregation</typeparam>
        /// <param name="aggregate1">Aggregation specification number 1</param>
        /// <param name="merger">Function to take the result of all aggregations and merge them into a single result</param>
        /// <returns></returns>
        public static IAggregate<TInput, StructTuple<TState1>, TResult> Combine<TInput, TState1, TResult1, TResult>(
            IAggregate<TInput, TState1, TResult1> aggregate1,
            Expression<Func<TResult1, TResult>> merger)
        {
            Contract.Requires(aggregate1 != null);
            Contract.Requires(merger != null);

            var duplicate = new bool[1];
            Expression<Func<StructTuple<TState1>, TState1>> target1 = state => state.Item1;
            var target = new System.Collections.Generic.List<LambdaExpression> { target1 };

            Expression<Func<StructTuple<TState1>>> newInitialState =
                () => new StructTuple<TState1>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.InitialState() : () => default)
                };

            Expression<Func<StructTuple<TState1>, long, TInput, StructTuple<TState1>>> newAccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Accumulate() : (s, a, b) => s, oldState.Item1, timestamp, input)
                };

            Expression<Func<StructTuple<TState1>, long, TInput, StructTuple<TState1>>> newDeaccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Deaccumulate() : (s, a, b) => s, oldState.Item1, timestamp, input)
                };

            Expression<Func<StructTuple<TState1>, StructTuple<TState1>, StructTuple<TState1>>> newDifference =
                (leftState, rightState) => new StructTuple<TState1>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Difference() : (l, r) => l, leftState.Item1, rightState.Item1)
                };

            Expression<Func<StructTuple<TState1>, TResult>> newComputeResult =
                state =>
                CallInliner.Call(
                    merger,
                    CallInliner.Call(aggregate1.ComputeResult(), CallInliner.Call(target[0] as Expression<Func<StructTuple<TState1>, TState1>>, state)));

            return GeneratedAggregate.Create(
                initialState: newInitialState.InlineCalls(),
                accumulate: newAccumulate.InlineCalls(),
                deaccumulate: newDeaccumulate.InlineCalls(),
                difference: newDifference.InlineCalls(),
                computeResult: newComputeResult.InlineCalls());
        }

        /// <summary>
        /// Performs multiple aggregations simultaneously
        /// </summary>
        /// <typeparam name="TInput">The input stream to aggregate</typeparam>
        /// <typeparam name="TState1">Aggregation state type for aggregate number 1</typeparam>,
        /// <typeparam name="TState2">Aggregation state type for aggregate number 2</typeparam>
        /// <typeparam name="TResult1">Result type for aggregate number 1</typeparam>,
        /// <typeparam name="TResult2">Result type for aggregate number 2</typeparam>
        /// <typeparam name="TResult">Result type of the merged aggregation</typeparam>
        /// <param name="aggregate1">Aggregation specification number 1</param>,
        /// <param name="aggregate2">Aggregation specification number 2</param>
        /// <param name="merger">Function to take the result of all aggregations and merge them into a single result</param>
        /// <returns></returns>
        public static IAggregate<TInput, StructTuple<TState1, TState2>, TResult> Combine<TInput, TState1, TResult1, TState2, TResult2, TResult>(
            IAggregate<TInput, TState1, TResult1> aggregate1,
            IAggregate<TInput, TState2, TResult2> aggregate2,
            Expression<Func<TResult1, TResult2, TResult>> merger)
        {
            Contract.Requires(aggregate1 != null);
            Contract.Requires(aggregate2 != null);
            Contract.Requires(merger != null);

            var duplicate = new bool[2];
            Expression<Func<StructTuple<TState1, TState2>, TState1>> target1 = state => state.Item1;
            Expression<Func<StructTuple<TState1, TState2>, TState2>> target2 = state => state.Item2;
            var target = new System.Collections.Generic.List<LambdaExpression> { target1, target2 };
            if (aggregate2.HasSameStateAs(aggregate1))
            {
                duplicate[1] = true; target[1] = target[0];
            }

            Expression<Func<StructTuple<TState1, TState2>>> newInitialState =
                () => new StructTuple<TState1, TState2>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.InitialState() : () => default),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.InitialState() : () => default)
                };

            Expression<Func<StructTuple<TState1, TState2>, long, TInput, StructTuple<TState1, TState2>>> newAccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Accumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Accumulate() : (s, a, b) => s, oldState.Item2, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2>, long, TInput, StructTuple<TState1, TState2>>> newDeaccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Deaccumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Deaccumulate() : (s, a, b) => s, oldState.Item2, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2>, StructTuple<TState1, TState2>, StructTuple<TState1, TState2>>> newDifference =
                (leftState, rightState) => new StructTuple<TState1, TState2>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Difference() : (l, r) => l, leftState.Item1, rightState.Item1),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Difference() : (l, r) => l, leftState.Item2, rightState.Item2)
                };

            Expression<Func<StructTuple<TState1, TState2>, TResult>> newComputeResult =
                state =>
                CallInliner.Call(
                    merger,
                    CallInliner.Call(aggregate1.ComputeResult(), CallInliner.Call(target[0] as Expression<Func<StructTuple<TState1, TState2>, TState1>>, state)),
                    CallInliner.Call(aggregate2.ComputeResult(), CallInliner.Call(target[1] as Expression<Func<StructTuple<TState1, TState2>, TState2>>, state)));

            return GeneratedAggregate.Create(
                initialState: newInitialState.InlineCalls(),
                accumulate: newAccumulate.InlineCalls(),
                deaccumulate: newDeaccumulate.InlineCalls(),
                difference: newDifference.InlineCalls(),
                computeResult: newComputeResult.InlineCalls());
        }

        /// <summary>
        /// Performs multiple aggregations simultaneously
        /// </summary>
        /// <typeparam name="TInput">The input stream to aggregate</typeparam>
        /// <typeparam name="TState1">Aggregation state type for aggregate number 1</typeparam>,
        /// <typeparam name="TState2">Aggregation state type for aggregate number 2</typeparam>,
        /// <typeparam name="TState3">Aggregation state type for aggregate number 3</typeparam>
        /// <typeparam name="TResult1">Result type for aggregate number 1</typeparam>,
        /// <typeparam name="TResult2">Result type for aggregate number 2</typeparam>,
        /// <typeparam name="TResult3">Result type for aggregate number 3</typeparam>
        /// <typeparam name="TResult">Result type of the merged aggregation</typeparam>
        /// <param name="aggregate1">Aggregation specification number 1</param>,
        /// <param name="aggregate2">Aggregation specification number 2</param>,
        /// <param name="aggregate3">Aggregation specification number 3</param>
        /// <param name="merger">Function to take the result of all aggregations and merge them into a single result</param>
        /// <returns></returns>
        public static IAggregate<TInput, StructTuple<TState1, TState2, TState3>, TResult> Combine<TInput, TState1, TResult1, TState2, TResult2, TState3, TResult3, TResult>(
            IAggregate<TInput, TState1, TResult1> aggregate1,
            IAggregate<TInput, TState2, TResult2> aggregate2,
            IAggregate<TInput, TState3, TResult3> aggregate3,
            Expression<Func<TResult1, TResult2, TResult3, TResult>> merger)
        {
            Contract.Requires(aggregate1 != null);
            Contract.Requires(aggregate2 != null);
            Contract.Requires(aggregate3 != null);
            Contract.Requires(merger != null);

            var duplicate = new bool[3];
            Expression<Func<StructTuple<TState1, TState2, TState3>, TState1>> target1 = state => state.Item1;
            Expression<Func<StructTuple<TState1, TState2, TState3>, TState2>> target2 = state => state.Item2;
            Expression<Func<StructTuple<TState1, TState2, TState3>, TState3>> target3 = state => state.Item3;
            var target = new System.Collections.Generic.List<LambdaExpression> { target1, target2, target3 };
            if (aggregate2.HasSameStateAs(aggregate1))
            {
                duplicate[1] = true; target[1] = target[0];
            }
            if (aggregate3.HasSameStateAs(aggregate1))
            {
                duplicate[2] = true; target[2] = target[0];
            }
            else if (aggregate3.HasSameStateAs(aggregate2))
            {
                duplicate[2] = true; target[2] = target[1];
            }

            Expression<Func<StructTuple<TState1, TState2, TState3>>> newInitialState =
                () => new StructTuple<TState1, TState2, TState3>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.InitialState() : () => default),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.InitialState() : () => default),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.InitialState() : () => default)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3>, long, TInput, StructTuple<TState1, TState2, TState3>>> newAccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2, TState3>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Accumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Accumulate() : (s, a, b) => s, oldState.Item2, timestamp, input),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Accumulate() : (s, a, b) => s, oldState.Item3, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3>, long, TInput, StructTuple<TState1, TState2, TState3>>> newDeaccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2, TState3>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Deaccumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Deaccumulate() : (s, a, b) => s, oldState.Item2, timestamp, input),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Deaccumulate() : (s, a, b) => s, oldState.Item3, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3>, StructTuple<TState1, TState2, TState3>, StructTuple<TState1, TState2, TState3>>> newDifference =
                (leftState, rightState) => new StructTuple<TState1, TState2, TState3>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Difference() : (l, r) => l, leftState.Item1, rightState.Item1),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Difference() : (l, r) => l, leftState.Item2, rightState.Item2),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Difference() : (l, r) => l, leftState.Item3, rightState.Item3)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3>, TResult>> newComputeResult =
                state =>
                CallInliner.Call(
                    merger,
                    CallInliner.Call(aggregate1.ComputeResult(), CallInliner.Call(target[0] as Expression<Func<StructTuple<TState1, TState2, TState3>, TState1>>, state)),
                    CallInliner.Call(aggregate2.ComputeResult(), CallInliner.Call(target[1] as Expression<Func<StructTuple<TState1, TState2, TState3>, TState2>>, state)),
                    CallInliner.Call(aggregate3.ComputeResult(), CallInliner.Call(target[2] as Expression<Func<StructTuple<TState1, TState2, TState3>, TState3>>, state)));

            return GeneratedAggregate.Create(
                initialState: newInitialState.InlineCalls(),
                accumulate: newAccumulate.InlineCalls(),
                deaccumulate: newDeaccumulate.InlineCalls(),
                difference: newDifference.InlineCalls(),
                computeResult: newComputeResult.InlineCalls());
        }

        /// <summary>
        /// Performs multiple aggregations simultaneously
        /// </summary>
        /// <typeparam name="TInput">The input stream to aggregate</typeparam>
        /// <typeparam name="TState1">Aggregation state type for aggregate number 1</typeparam>,
        /// <typeparam name="TState2">Aggregation state type for aggregate number 2</typeparam>,
        /// <typeparam name="TState3">Aggregation state type for aggregate number 3</typeparam>,
        /// <typeparam name="TState4">Aggregation state type for aggregate number 4</typeparam>
        /// <typeparam name="TResult1">Result type for aggregate number 1</typeparam>,
        /// <typeparam name="TResult2">Result type for aggregate number 2</typeparam>,
        /// <typeparam name="TResult3">Result type for aggregate number 3</typeparam>,
        /// <typeparam name="TResult4">Result type for aggregate number 4</typeparam>
        /// <typeparam name="TResult">Result type of the merged aggregation</typeparam>
        /// <param name="aggregate1">Aggregation specification number 1</param>,
        /// <param name="aggregate2">Aggregation specification number 2</param>,
        /// <param name="aggregate3">Aggregation specification number 3</param>,
        /// <param name="aggregate4">Aggregation specification number 4</param>
        /// <param name="merger">Function to take the result of all aggregations and merge them into a single result</param>
        /// <returns></returns>
        public static IAggregate<TInput, StructTuple<TState1, TState2, TState3, TState4>, TResult> Combine<TInput, TState1, TResult1, TState2, TResult2, TState3, TResult3, TState4, TResult4, TResult>(
            IAggregate<TInput, TState1, TResult1> aggregate1,
            IAggregate<TInput, TState2, TResult2> aggregate2,
            IAggregate<TInput, TState3, TResult3> aggregate3,
            IAggregate<TInput, TState4, TResult4> aggregate4,
            Expression<Func<TResult1, TResult2, TResult3, TResult4, TResult>> merger)
        {
            Contract.Requires(aggregate1 != null);
            Contract.Requires(aggregate2 != null);
            Contract.Requires(aggregate3 != null);
            Contract.Requires(aggregate4 != null);
            Contract.Requires(merger != null);

            var duplicate = new bool[4];
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4>, TState1>> target1 = state => state.Item1;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4>, TState2>> target2 = state => state.Item2;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4>, TState3>> target3 = state => state.Item3;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4>, TState4>> target4 = state => state.Item4;
            var target = new System.Collections.Generic.List<LambdaExpression> { target1, target2, target3, target4 };
            if (aggregate2.HasSameStateAs(aggregate1))
            {
                duplicate[1] = true; target[1] = target[0];
            }
            if (aggregate3.HasSameStateAs(aggregate1))
            {
                duplicate[2] = true; target[2] = target[0];
            }
            else if (aggregate3.HasSameStateAs(aggregate2))
            {
                duplicate[2] = true; target[2] = target[1];
            }
            if (aggregate4.HasSameStateAs(aggregate1))
            {
                duplicate[3] = true; target[3] = target[0];
            }
            else if (aggregate4.HasSameStateAs(aggregate2))
            {
                duplicate[3] = true; target[3] = target[1];
            }
            else if (aggregate4.HasSameStateAs(aggregate3))
            {
                duplicate[3] = true; target[3] = target[2];
            }

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4>>> newInitialState =
                () => new StructTuple<TState1, TState2, TState3, TState4>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.InitialState() : () => default),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.InitialState() : () => default),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.InitialState() : () => default),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.InitialState() : () => default)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4>, long, TInput, StructTuple<TState1, TState2, TState3, TState4>>> newAccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2, TState3, TState4>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Accumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Accumulate() : (s, a, b) => s, oldState.Item2, timestamp, input),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Accumulate() : (s, a, b) => s, oldState.Item3, timestamp, input),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Accumulate() : (s, a, b) => s, oldState.Item4, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4>, long, TInput, StructTuple<TState1, TState2, TState3, TState4>>> newDeaccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2, TState3, TState4>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Deaccumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Deaccumulate() : (s, a, b) => s, oldState.Item2, timestamp, input),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Deaccumulate() : (s, a, b) => s, oldState.Item3, timestamp, input),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Deaccumulate() : (s, a, b) => s, oldState.Item4, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4>, StructTuple<TState1, TState2, TState3, TState4>, StructTuple<TState1, TState2, TState3, TState4>>> newDifference =
                (leftState, rightState) => new StructTuple<TState1, TState2, TState3, TState4>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Difference() : (l, r) => l, leftState.Item1, rightState.Item1),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Difference() : (l, r) => l, leftState.Item2, rightState.Item2),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Difference() : (l, r) => l, leftState.Item3, rightState.Item3),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Difference() : (l, r) => l, leftState.Item4, rightState.Item4)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4>, TResult>> newComputeResult =
                state =>
                CallInliner.Call(
                    merger,
                    CallInliner.Call(aggregate1.ComputeResult(), CallInliner.Call(target[0] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4>, TState1>>, state)),
                    CallInliner.Call(aggregate2.ComputeResult(), CallInliner.Call(target[1] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4>, TState2>>, state)),
                    CallInliner.Call(aggregate3.ComputeResult(), CallInliner.Call(target[2] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4>, TState3>>, state)),
                    CallInliner.Call(aggregate4.ComputeResult(), CallInliner.Call(target[3] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4>, TState4>>, state)));

            return GeneratedAggregate.Create(
                initialState: newInitialState.InlineCalls(),
                accumulate: newAccumulate.InlineCalls(),
                deaccumulate: newDeaccumulate.InlineCalls(),
                difference: newDifference.InlineCalls(),
                computeResult: newComputeResult.InlineCalls());
        }

        /// <summary>
        /// Performs multiple aggregations simultaneously
        /// </summary>
        /// <typeparam name="TInput">The input stream to aggregate</typeparam>
        /// <typeparam name="TState1">Aggregation state type for aggregate number 1</typeparam>,
        /// <typeparam name="TState2">Aggregation state type for aggregate number 2</typeparam>,
        /// <typeparam name="TState3">Aggregation state type for aggregate number 3</typeparam>,
        /// <typeparam name="TState4">Aggregation state type for aggregate number 4</typeparam>,
        /// <typeparam name="TState5">Aggregation state type for aggregate number 5</typeparam>
        /// <typeparam name="TResult1">Result type for aggregate number 1</typeparam>,
        /// <typeparam name="TResult2">Result type for aggregate number 2</typeparam>,
        /// <typeparam name="TResult3">Result type for aggregate number 3</typeparam>,
        /// <typeparam name="TResult4">Result type for aggregate number 4</typeparam>,
        /// <typeparam name="TResult5">Result type for aggregate number 5</typeparam>
        /// <typeparam name="TResult">Result type of the merged aggregation</typeparam>
        /// <param name="aggregate1">Aggregation specification number 1</param>,
        /// <param name="aggregate2">Aggregation specification number 2</param>,
        /// <param name="aggregate3">Aggregation specification number 3</param>,
        /// <param name="aggregate4">Aggregation specification number 4</param>,
        /// <param name="aggregate5">Aggregation specification number 5</param>
        /// <param name="merger">Function to take the result of all aggregations and merge them into a single result</param>
        /// <returns></returns>
        public static IAggregate<TInput, StructTuple<TState1, TState2, TState3, TState4, TState5>, TResult> Combine<TInput, TState1, TResult1, TState2, TResult2, TState3, TResult3, TState4, TResult4, TState5, TResult5, TResult>(
            IAggregate<TInput, TState1, TResult1> aggregate1,
            IAggregate<TInput, TState2, TResult2> aggregate2,
            IAggregate<TInput, TState3, TResult3> aggregate3,
            IAggregate<TInput, TState4, TResult4> aggregate4,
            IAggregate<TInput, TState5, TResult5> aggregate5,
            Expression<Func<TResult1, TResult2, TResult3, TResult4, TResult5, TResult>> merger)
        {
            Contract.Requires(aggregate1 != null);
            Contract.Requires(aggregate2 != null);
            Contract.Requires(aggregate3 != null);
            Contract.Requires(aggregate4 != null);
            Contract.Requires(aggregate5 != null);
            Contract.Requires(merger != null);

            var duplicate = new bool[5];
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5>, TState1>> target1 = state => state.Item1;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5>, TState2>> target2 = state => state.Item2;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5>, TState3>> target3 = state => state.Item3;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5>, TState4>> target4 = state => state.Item4;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5>, TState5>> target5 = state => state.Item5;
            var target = new System.Collections.Generic.List<LambdaExpression> { target1, target2, target3, target4, target5 };
            if (aggregate2.HasSameStateAs(aggregate1))
            {
                duplicate[1] = true; target[1] = target[0];
            }
            if (aggregate3.HasSameStateAs(aggregate1))
            {
                duplicate[2] = true; target[2] = target[0];
            }
            else if (aggregate3.HasSameStateAs(aggregate2))
            {
                duplicate[2] = true; target[2] = target[1];
            }
            if (aggregate4.HasSameStateAs(aggregate1))
            {
                duplicate[3] = true; target[3] = target[0];
            }
            else if (aggregate4.HasSameStateAs(aggregate2))
            {
                duplicate[3] = true; target[3] = target[1];
            }
            else if (aggregate4.HasSameStateAs(aggregate3))
            {
                duplicate[3] = true; target[3] = target[2];
            }
            if (aggregate5.HasSameStateAs(aggregate1))
            {
                duplicate[4] = true; target[4] = target[0];
            }
            else if (aggregate5.HasSameStateAs(aggregate2))
            {
                duplicate[4] = true; target[4] = target[1];
            }
            else if (aggregate5.HasSameStateAs(aggregate3))
            {
                duplicate[4] = true; target[4] = target[2];
            }
            else if (aggregate5.HasSameStateAs(aggregate4))
            {
                duplicate[4] = true; target[4] = target[3];
            }

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5>>> newInitialState =
                () => new StructTuple<TState1, TState2, TState3, TState4, TState5>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.InitialState() : () => default),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.InitialState() : () => default),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.InitialState() : () => default),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.InitialState() : () => default),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.InitialState() : () => default)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5>, long, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5>>> newAccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2, TState3, TState4, TState5>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Accumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Accumulate() : (s, a, b) => s, oldState.Item2, timestamp, input),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Accumulate() : (s, a, b) => s, oldState.Item3, timestamp, input),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Accumulate() : (s, a, b) => s, oldState.Item4, timestamp, input),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Accumulate() : (s, a, b) => s, oldState.Item5, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5>, long, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5>>> newDeaccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2, TState3, TState4, TState5>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Deaccumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Deaccumulate() : (s, a, b) => s, oldState.Item2, timestamp, input),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Deaccumulate() : (s, a, b) => s, oldState.Item3, timestamp, input),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Deaccumulate() : (s, a, b) => s, oldState.Item4, timestamp, input),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Deaccumulate() : (s, a, b) => s, oldState.Item5, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5>, StructTuple<TState1, TState2, TState3, TState4, TState5>, StructTuple<TState1, TState2, TState3, TState4, TState5>>> newDifference =
                (leftState, rightState) => new StructTuple<TState1, TState2, TState3, TState4, TState5>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Difference() : (l, r) => l, leftState.Item1, rightState.Item1),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Difference() : (l, r) => l, leftState.Item2, rightState.Item2),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Difference() : (l, r) => l, leftState.Item3, rightState.Item3),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Difference() : (l, r) => l, leftState.Item4, rightState.Item4),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Difference() : (l, r) => l, leftState.Item5, rightState.Item5)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5>, TResult>> newComputeResult =
                state =>
                CallInliner.Call(
                    merger,
                    CallInliner.Call(aggregate1.ComputeResult(), CallInliner.Call(target[0] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5>, TState1>>, state)),
                    CallInliner.Call(aggregate2.ComputeResult(), CallInliner.Call(target[1] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5>, TState2>>, state)),
                    CallInliner.Call(aggregate3.ComputeResult(), CallInliner.Call(target[2] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5>, TState3>>, state)),
                    CallInliner.Call(aggregate4.ComputeResult(), CallInliner.Call(target[3] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5>, TState4>>, state)),
                    CallInliner.Call(aggregate5.ComputeResult(), CallInliner.Call(target[4] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5>, TState5>>, state)));

            return GeneratedAggregate.Create(
                initialState: newInitialState.InlineCalls(),
                accumulate: newAccumulate.InlineCalls(),
                deaccumulate: newDeaccumulate.InlineCalls(),
                difference: newDifference.InlineCalls(),
                computeResult: newComputeResult.InlineCalls());
        }

        /// <summary>
        /// Performs multiple aggregations simultaneously
        /// </summary>
        /// <typeparam name="TInput">The input stream to aggregate</typeparam>
        /// <typeparam name="TState1">Aggregation state type for aggregate number 1</typeparam>,
        /// <typeparam name="TState2">Aggregation state type for aggregate number 2</typeparam>,
        /// <typeparam name="TState3">Aggregation state type for aggregate number 3</typeparam>,
        /// <typeparam name="TState4">Aggregation state type for aggregate number 4</typeparam>,
        /// <typeparam name="TState5">Aggregation state type for aggregate number 5</typeparam>,
        /// <typeparam name="TState6">Aggregation state type for aggregate number 6</typeparam>
        /// <typeparam name="TResult1">Result type for aggregate number 1</typeparam>,
        /// <typeparam name="TResult2">Result type for aggregate number 2</typeparam>,
        /// <typeparam name="TResult3">Result type for aggregate number 3</typeparam>,
        /// <typeparam name="TResult4">Result type for aggregate number 4</typeparam>,
        /// <typeparam name="TResult5">Result type for aggregate number 5</typeparam>,
        /// <typeparam name="TResult6">Result type for aggregate number 6</typeparam>
        /// <typeparam name="TResult">Result type of the merged aggregation</typeparam>
        /// <param name="aggregate1">Aggregation specification number 1</param>,
        /// <param name="aggregate2">Aggregation specification number 2</param>,
        /// <param name="aggregate3">Aggregation specification number 3</param>,
        /// <param name="aggregate4">Aggregation specification number 4</param>,
        /// <param name="aggregate5">Aggregation specification number 5</param>,
        /// <param name="aggregate6">Aggregation specification number 6</param>
        /// <param name="merger">Function to take the result of all aggregations and merge them into a single result</param>
        /// <returns></returns>
        public static IAggregate<TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>, TResult> Combine<TInput, TState1, TResult1, TState2, TResult2, TState3, TResult3, TState4, TResult4, TState5, TResult5, TState6, TResult6, TResult>(
            IAggregate<TInput, TState1, TResult1> aggregate1,
            IAggregate<TInput, TState2, TResult2> aggregate2,
            IAggregate<TInput, TState3, TResult3> aggregate3,
            IAggregate<TInput, TState4, TResult4> aggregate4,
            IAggregate<TInput, TState5, TResult5> aggregate5,
            IAggregate<TInput, TState6, TResult6> aggregate6,
            Expression<Func<TResult1, TResult2, TResult3, TResult4, TResult5, TResult6, TResult>> merger)
        {
            Contract.Requires(aggregate1 != null);
            Contract.Requires(aggregate2 != null);
            Contract.Requires(aggregate3 != null);
            Contract.Requires(aggregate4 != null);
            Contract.Requires(aggregate5 != null);
            Contract.Requires(aggregate6 != null);
            Contract.Requires(merger != null);

            var duplicate = new bool[6];
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>, TState1>> target1 = state => state.Item1;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>, TState2>> target2 = state => state.Item2;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>, TState3>> target3 = state => state.Item3;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>, TState4>> target4 = state => state.Item4;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>, TState5>> target5 = state => state.Item5;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>, TState6>> target6 = state => state.Item6;
            var target = new System.Collections.Generic.List<LambdaExpression> { target1, target2, target3, target4, target5, target6 };
            if (aggregate2.HasSameStateAs(aggregate1))
            {
                duplicate[1] = true; target[1] = target[0];
            }
            if (aggregate3.HasSameStateAs(aggregate1))
            {
                duplicate[2] = true; target[2] = target[0];
            }
            else if (aggregate3.HasSameStateAs(aggregate2))
            {
                duplicate[2] = true; target[2] = target[1];
            }
            if (aggregate4.HasSameStateAs(aggregate1))
            {
                duplicate[3] = true; target[3] = target[0];
            }
            else if (aggregate4.HasSameStateAs(aggregate2))
            {
                duplicate[3] = true; target[3] = target[1];
            }
            else if (aggregate4.HasSameStateAs(aggregate3))
            {
                duplicate[3] = true; target[3] = target[2];
            }
            if (aggregate5.HasSameStateAs(aggregate1))
            {
                duplicate[4] = true; target[4] = target[0];
            }
            else if (aggregate5.HasSameStateAs(aggregate2))
            {
                duplicate[4] = true; target[4] = target[1];
            }
            else if (aggregate5.HasSameStateAs(aggregate3))
            {
                duplicate[4] = true; target[4] = target[2];
            }
            else if (aggregate5.HasSameStateAs(aggregate4))
            {
                duplicate[4] = true; target[4] = target[3];
            }
            if (aggregate6.HasSameStateAs(aggregate1))
            {
                duplicate[5] = true; target[5] = target[0];
            }
            else if (aggregate6.HasSameStateAs(aggregate2))
            {
                duplicate[5] = true; target[5] = target[1];
            }
            else if (aggregate6.HasSameStateAs(aggregate3))
            {
                duplicate[5] = true; target[5] = target[2];
            }
            else if (aggregate6.HasSameStateAs(aggregate4))
            {
                duplicate[5] = true; target[5] = target[3];
            }
            else if (aggregate6.HasSameStateAs(aggregate5))
            {
                duplicate[5] = true; target[5] = target[4];
            }

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>>> newInitialState =
                () => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.InitialState() : () => default),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.InitialState() : () => default),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.InitialState() : () => default),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.InitialState() : () => default),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.InitialState() : () => default),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.InitialState() : () => default)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>, long, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>>> newAccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Accumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Accumulate() : (s, a, b) => s, oldState.Item2, timestamp, input),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Accumulate() : (s, a, b) => s, oldState.Item3, timestamp, input),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Accumulate() : (s, a, b) => s, oldState.Item4, timestamp, input),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Accumulate() : (s, a, b) => s, oldState.Item5, timestamp, input),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Accumulate() : (s, a, b) => s, oldState.Item6, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>, long, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>>> newDeaccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Deaccumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Deaccumulate() : (s, a, b) => s, oldState.Item2, timestamp, input),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Deaccumulate() : (s, a, b) => s, oldState.Item3, timestamp, input),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Deaccumulate() : (s, a, b) => s, oldState.Item4, timestamp, input),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Deaccumulate() : (s, a, b) => s, oldState.Item5, timestamp, input),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Deaccumulate() : (s, a, b) => s, oldState.Item6, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>>> newDifference =
                (leftState, rightState) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Difference() : (l, r) => l, leftState.Item1, rightState.Item1),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Difference() : (l, r) => l, leftState.Item2, rightState.Item2),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Difference() : (l, r) => l, leftState.Item3, rightState.Item3),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Difference() : (l, r) => l, leftState.Item4, rightState.Item4),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Difference() : (l, r) => l, leftState.Item5, rightState.Item5),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Difference() : (l, r) => l, leftState.Item6, rightState.Item6)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>, TResult>> newComputeResult =
                state =>
                CallInliner.Call(
                    merger,
                    CallInliner.Call(aggregate1.ComputeResult(), CallInliner.Call(target[0] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>, TState1>>, state)),
                    CallInliner.Call(aggregate2.ComputeResult(), CallInliner.Call(target[1] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>, TState2>>, state)),
                    CallInliner.Call(aggregate3.ComputeResult(), CallInliner.Call(target[2] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>, TState3>>, state)),
                    CallInliner.Call(aggregate4.ComputeResult(), CallInliner.Call(target[3] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>, TState4>>, state)),
                    CallInliner.Call(aggregate5.ComputeResult(), CallInliner.Call(target[4] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>, TState5>>, state)),
                    CallInliner.Call(aggregate6.ComputeResult(), CallInliner.Call(target[5] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>, TState6>>, state)));

            return GeneratedAggregate.Create(
                initialState: newInitialState.InlineCalls(),
                accumulate: newAccumulate.InlineCalls(),
                deaccumulate: newDeaccumulate.InlineCalls(),
                difference: newDifference.InlineCalls(),
                computeResult: newComputeResult.InlineCalls());
        }

        /// <summary>
        /// Performs multiple aggregations simultaneously
        /// </summary>
        /// <typeparam name="TInput">The input stream to aggregate</typeparam>
        /// <typeparam name="TState1">Aggregation state type for aggregate number 1</typeparam>,
        /// <typeparam name="TState2">Aggregation state type for aggregate number 2</typeparam>,
        /// <typeparam name="TState3">Aggregation state type for aggregate number 3</typeparam>,
        /// <typeparam name="TState4">Aggregation state type for aggregate number 4</typeparam>,
        /// <typeparam name="TState5">Aggregation state type for aggregate number 5</typeparam>,
        /// <typeparam name="TState6">Aggregation state type for aggregate number 6</typeparam>,
        /// <typeparam name="TState7">Aggregation state type for aggregate number 7</typeparam>
        /// <typeparam name="TResult1">Result type for aggregate number 1</typeparam>,
        /// <typeparam name="TResult2">Result type for aggregate number 2</typeparam>,
        /// <typeparam name="TResult3">Result type for aggregate number 3</typeparam>,
        /// <typeparam name="TResult4">Result type for aggregate number 4</typeparam>,
        /// <typeparam name="TResult5">Result type for aggregate number 5</typeparam>,
        /// <typeparam name="TResult6">Result type for aggregate number 6</typeparam>,
        /// <typeparam name="TResult7">Result type for aggregate number 7</typeparam>
        /// <typeparam name="TResult">Result type of the merged aggregation</typeparam>
        /// <param name="aggregate1">Aggregation specification number 1</param>,
        /// <param name="aggregate2">Aggregation specification number 2</param>,
        /// <param name="aggregate3">Aggregation specification number 3</param>,
        /// <param name="aggregate4">Aggregation specification number 4</param>,
        /// <param name="aggregate5">Aggregation specification number 5</param>,
        /// <param name="aggregate6">Aggregation specification number 6</param>,
        /// <param name="aggregate7">Aggregation specification number 7</param>
        /// <param name="merger">Function to take the result of all aggregations and merge them into a single result</param>
        /// <returns></returns>
        public static IAggregate<TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>, TResult> Combine<TInput, TState1, TResult1, TState2, TResult2, TState3, TResult3, TState4, TResult4, TState5, TResult5, TState6, TResult6, TState7, TResult7, TResult>(
            IAggregate<TInput, TState1, TResult1> aggregate1,
            IAggregate<TInput, TState2, TResult2> aggregate2,
            IAggregate<TInput, TState3, TResult3> aggregate3,
            IAggregate<TInput, TState4, TResult4> aggregate4,
            IAggregate<TInput, TState5, TResult5> aggregate5,
            IAggregate<TInput, TState6, TResult6> aggregate6,
            IAggregate<TInput, TState7, TResult7> aggregate7,
            Expression<Func<TResult1, TResult2, TResult3, TResult4, TResult5, TResult6, TResult7, TResult>> merger)
        {
            Contract.Requires(aggregate1 != null);
            Contract.Requires(aggregate2 != null);
            Contract.Requires(aggregate3 != null);
            Contract.Requires(aggregate4 != null);
            Contract.Requires(aggregate5 != null);
            Contract.Requires(aggregate6 != null);
            Contract.Requires(aggregate7 != null);
            Contract.Requires(merger != null);

            var duplicate = new bool[7];
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>, TState1>> target1 = state => state.Item1;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>, TState2>> target2 = state => state.Item2;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>, TState3>> target3 = state => state.Item3;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>, TState4>> target4 = state => state.Item4;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>, TState5>> target5 = state => state.Item5;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>, TState6>> target6 = state => state.Item6;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>, TState7>> target7 = state => state.Item7;
            var target = new System.Collections.Generic.List<LambdaExpression> { target1, target2, target3, target4, target5, target6, target7 };
            if (aggregate2.HasSameStateAs(aggregate1))
            {
                duplicate[1] = true; target[1] = target[0];
            }
            if (aggregate3.HasSameStateAs(aggregate1))
            {
                duplicate[2] = true; target[2] = target[0];
            }
            else if (aggregate3.HasSameStateAs(aggregate2))
            {
                duplicate[2] = true; target[2] = target[1];
            }
            if (aggregate4.HasSameStateAs(aggregate1))
            {
                duplicate[3] = true; target[3] = target[0];
            }
            else if (aggregate4.HasSameStateAs(aggregate2))
            {
                duplicate[3] = true; target[3] = target[1];
            }
            else if (aggregate4.HasSameStateAs(aggregate3))
            {
                duplicate[3] = true; target[3] = target[2];
            }
            if (aggregate5.HasSameStateAs(aggregate1))
            {
                duplicate[4] = true; target[4] = target[0];
            }
            else if (aggregate5.HasSameStateAs(aggregate2))
            {
                duplicate[4] = true; target[4] = target[1];
            }
            else if (aggregate5.HasSameStateAs(aggregate3))
            {
                duplicate[4] = true; target[4] = target[2];
            }
            else if (aggregate5.HasSameStateAs(aggregate4))
            {
                duplicate[4] = true; target[4] = target[3];
            }
            if (aggregate6.HasSameStateAs(aggregate1))
            {
                duplicate[5] = true; target[5] = target[0];
            }
            else if (aggregate6.HasSameStateAs(aggregate2))
            {
                duplicate[5] = true; target[5] = target[1];
            }
            else if (aggregate6.HasSameStateAs(aggregate3))
            {
                duplicate[5] = true; target[5] = target[2];
            }
            else if (aggregate6.HasSameStateAs(aggregate4))
            {
                duplicate[5] = true; target[5] = target[3];
            }
            else if (aggregate6.HasSameStateAs(aggregate5))
            {
                duplicate[5] = true; target[5] = target[4];
            }
            if (aggregate7.HasSameStateAs(aggregate1))
            {
                duplicate[6] = true; target[6] = target[0];
            }
            else if (aggregate7.HasSameStateAs(aggregate2))
            {
                duplicate[6] = true; target[6] = target[1];
            }
            else if (aggregate7.HasSameStateAs(aggregate3))
            {
                duplicate[6] = true; target[6] = target[2];
            }
            else if (aggregate7.HasSameStateAs(aggregate4))
            {
                duplicate[6] = true; target[6] = target[3];
            }
            else if (aggregate7.HasSameStateAs(aggregate5))
            {
                duplicate[6] = true; target[6] = target[4];
            }
            else if (aggregate7.HasSameStateAs(aggregate6))
            {
                duplicate[6] = true; target[6] = target[5];
            }

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>>> newInitialState =
                () => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.InitialState() : () => default),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.InitialState() : () => default),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.InitialState() : () => default),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.InitialState() : () => default),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.InitialState() : () => default),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.InitialState() : () => default),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.InitialState() : () => default)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>, long, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>>> newAccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Accumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Accumulate() : (s, a, b) => s, oldState.Item2, timestamp, input),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Accumulate() : (s, a, b) => s, oldState.Item3, timestamp, input),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Accumulate() : (s, a, b) => s, oldState.Item4, timestamp, input),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Accumulate() : (s, a, b) => s, oldState.Item5, timestamp, input),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Accumulate() : (s, a, b) => s, oldState.Item6, timestamp, input),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.Accumulate() : (s, a, b) => s, oldState.Item7, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>, long, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>>> newDeaccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Deaccumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Deaccumulate() : (s, a, b) => s, oldState.Item2, timestamp, input),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Deaccumulate() : (s, a, b) => s, oldState.Item3, timestamp, input),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Deaccumulate() : (s, a, b) => s, oldState.Item4, timestamp, input),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Deaccumulate() : (s, a, b) => s, oldState.Item5, timestamp, input),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Deaccumulate() : (s, a, b) => s, oldState.Item6, timestamp, input),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.Deaccumulate() : (s, a, b) => s, oldState.Item7, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>>> newDifference =
                (leftState, rightState) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Difference() : (l, r) => l, leftState.Item1, rightState.Item1),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Difference() : (l, r) => l, leftState.Item2, rightState.Item2),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Difference() : (l, r) => l, leftState.Item3, rightState.Item3),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Difference() : (l, r) => l, leftState.Item4, rightState.Item4),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Difference() : (l, r) => l, leftState.Item5, rightState.Item5),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Difference() : (l, r) => l, leftState.Item6, rightState.Item6),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.Difference() : (l, r) => l, leftState.Item7, rightState.Item7)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>, TResult>> newComputeResult =
                state =>
                CallInliner.Call(
                    merger,
                    CallInliner.Call(aggregate1.ComputeResult(), CallInliner.Call(target[0] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>, TState1>>, state)),
                    CallInliner.Call(aggregate2.ComputeResult(), CallInliner.Call(target[1] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>, TState2>>, state)),
                    CallInliner.Call(aggregate3.ComputeResult(), CallInliner.Call(target[2] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>, TState3>>, state)),
                    CallInliner.Call(aggregate4.ComputeResult(), CallInliner.Call(target[3] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>, TState4>>, state)),
                    CallInliner.Call(aggregate5.ComputeResult(), CallInliner.Call(target[4] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>, TState5>>, state)),
                    CallInliner.Call(aggregate6.ComputeResult(), CallInliner.Call(target[5] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>, TState6>>, state)),
                    CallInliner.Call(aggregate7.ComputeResult(), CallInliner.Call(target[6] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>, TState7>>, state)));

            return GeneratedAggregate.Create(
                initialState: newInitialState.InlineCalls(),
                accumulate: newAccumulate.InlineCalls(),
                deaccumulate: newDeaccumulate.InlineCalls(),
                difference: newDifference.InlineCalls(),
                computeResult: newComputeResult.InlineCalls());
        }

        /// <summary>
        /// Performs multiple aggregations simultaneously
        /// </summary>
        /// <typeparam name="TInput">The input stream to aggregate</typeparam>
        /// <typeparam name="TState1">Aggregation state type for aggregate number 1</typeparam>,
        /// <typeparam name="TState2">Aggregation state type for aggregate number 2</typeparam>,
        /// <typeparam name="TState3">Aggregation state type for aggregate number 3</typeparam>,
        /// <typeparam name="TState4">Aggregation state type for aggregate number 4</typeparam>,
        /// <typeparam name="TState5">Aggregation state type for aggregate number 5</typeparam>,
        /// <typeparam name="TState6">Aggregation state type for aggregate number 6</typeparam>,
        /// <typeparam name="TState7">Aggregation state type for aggregate number 7</typeparam>,
        /// <typeparam name="TState8">Aggregation state type for aggregate number 8</typeparam>
        /// <typeparam name="TResult1">Result type for aggregate number 1</typeparam>,
        /// <typeparam name="TResult2">Result type for aggregate number 2</typeparam>,
        /// <typeparam name="TResult3">Result type for aggregate number 3</typeparam>,
        /// <typeparam name="TResult4">Result type for aggregate number 4</typeparam>,
        /// <typeparam name="TResult5">Result type for aggregate number 5</typeparam>,
        /// <typeparam name="TResult6">Result type for aggregate number 6</typeparam>,
        /// <typeparam name="TResult7">Result type for aggregate number 7</typeparam>,
        /// <typeparam name="TResult8">Result type for aggregate number 8</typeparam>
        /// <typeparam name="TResult">Result type of the merged aggregation</typeparam>
        /// <param name="aggregate1">Aggregation specification number 1</param>,
        /// <param name="aggregate2">Aggregation specification number 2</param>,
        /// <param name="aggregate3">Aggregation specification number 3</param>,
        /// <param name="aggregate4">Aggregation specification number 4</param>,
        /// <param name="aggregate5">Aggregation specification number 5</param>,
        /// <param name="aggregate6">Aggregation specification number 6</param>,
        /// <param name="aggregate7">Aggregation specification number 7</param>,
        /// <param name="aggregate8">Aggregation specification number 8</param>
        /// <param name="merger">Function to take the result of all aggregations and merge them into a single result</param>
        /// <returns></returns>
        public static IAggregate<TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>, TResult> Combine<TInput, TState1, TResult1, TState2, TResult2, TState3, TResult3, TState4, TResult4, TState5, TResult5, TState6, TResult6, TState7, TResult7, TState8, TResult8, TResult>(
            IAggregate<TInput, TState1, TResult1> aggregate1,
            IAggregate<TInput, TState2, TResult2> aggregate2,
            IAggregate<TInput, TState3, TResult3> aggregate3,
            IAggregate<TInput, TState4, TResult4> aggregate4,
            IAggregate<TInput, TState5, TResult5> aggregate5,
            IAggregate<TInput, TState6, TResult6> aggregate6,
            IAggregate<TInput, TState7, TResult7> aggregate7,
            IAggregate<TInput, TState8, TResult8> aggregate8,
            Expression<Func<TResult1, TResult2, TResult3, TResult4, TResult5, TResult6, TResult7, TResult8, TResult>> merger)
        {
            Contract.Requires(aggregate1 != null);
            Contract.Requires(aggregate2 != null);
            Contract.Requires(aggregate3 != null);
            Contract.Requires(aggregate4 != null);
            Contract.Requires(aggregate5 != null);
            Contract.Requires(aggregate6 != null);
            Contract.Requires(aggregate7 != null);
            Contract.Requires(aggregate8 != null);
            Contract.Requires(merger != null);

            var duplicate = new bool[8];
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>, TState1>> target1 = state => state.Item1;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>, TState2>> target2 = state => state.Item2;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>, TState3>> target3 = state => state.Item3;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>, TState4>> target4 = state => state.Item4;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>, TState5>> target5 = state => state.Item5;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>, TState6>> target6 = state => state.Item6;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>, TState7>> target7 = state => state.Item7;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>, TState8>> target8 = state => state.Item8;
            var target = new System.Collections.Generic.List<LambdaExpression> { target1, target2, target3, target4, target5, target6, target7, target8 };
            if (aggregate2.HasSameStateAs(aggregate1))
            {
                duplicate[1] = true; target[1] = target[0];
            }
            if (aggregate3.HasSameStateAs(aggregate1))
            {
                duplicate[2] = true; target[2] = target[0];
            }
            else if (aggregate3.HasSameStateAs(aggregate2))
            {
                duplicate[2] = true; target[2] = target[1];
            }
            if (aggregate4.HasSameStateAs(aggregate1))
            {
                duplicate[3] = true; target[3] = target[0];
            }
            else if (aggregate4.HasSameStateAs(aggregate2))
            {
                duplicate[3] = true; target[3] = target[1];
            }
            else if (aggregate4.HasSameStateAs(aggregate3))
            {
                duplicate[3] = true; target[3] = target[2];
            }
            if (aggregate5.HasSameStateAs(aggregate1))
            {
                duplicate[4] = true; target[4] = target[0];
            }
            else if (aggregate5.HasSameStateAs(aggregate2))
            {
                duplicate[4] = true; target[4] = target[1];
            }
            else if (aggregate5.HasSameStateAs(aggregate3))
            {
                duplicate[4] = true; target[4] = target[2];
            }
            else if (aggregate5.HasSameStateAs(aggregate4))
            {
                duplicate[4] = true; target[4] = target[3];
            }
            if (aggregate6.HasSameStateAs(aggregate1))
            {
                duplicate[5] = true; target[5] = target[0];
            }
            else if (aggregate6.HasSameStateAs(aggregate2))
            {
                duplicate[5] = true; target[5] = target[1];
            }
            else if (aggregate6.HasSameStateAs(aggregate3))
            {
                duplicate[5] = true; target[5] = target[2];
            }
            else if (aggregate6.HasSameStateAs(aggregate4))
            {
                duplicate[5] = true; target[5] = target[3];
            }
            else if (aggregate6.HasSameStateAs(aggregate5))
            {
                duplicate[5] = true; target[5] = target[4];
            }
            if (aggregate7.HasSameStateAs(aggregate1))
            {
                duplicate[6] = true; target[6] = target[0];
            }
            else if (aggregate7.HasSameStateAs(aggregate2))
            {
                duplicate[6] = true; target[6] = target[1];
            }
            else if (aggregate7.HasSameStateAs(aggregate3))
            {
                duplicate[6] = true; target[6] = target[2];
            }
            else if (aggregate7.HasSameStateAs(aggregate4))
            {
                duplicate[6] = true; target[6] = target[3];
            }
            else if (aggregate7.HasSameStateAs(aggregate5))
            {
                duplicate[6] = true; target[6] = target[4];
            }
            else if (aggregate7.HasSameStateAs(aggregate6))
            {
                duplicate[6] = true; target[6] = target[5];
            }
            if (aggregate8.HasSameStateAs(aggregate1))
            {
                duplicate[7] = true; target[7] = target[0];
            }
            else if (aggregate8.HasSameStateAs(aggregate2))
            {
                duplicate[7] = true; target[7] = target[1];
            }
            else if (aggregate8.HasSameStateAs(aggregate3))
            {
                duplicate[7] = true; target[7] = target[2];
            }
            else if (aggregate8.HasSameStateAs(aggregate4))
            {
                duplicate[7] = true; target[7] = target[3];
            }
            else if (aggregate8.HasSameStateAs(aggregate5))
            {
                duplicate[7] = true; target[7] = target[4];
            }
            else if (aggregate8.HasSameStateAs(aggregate6))
            {
                duplicate[7] = true; target[7] = target[5];
            }
            else if (aggregate8.HasSameStateAs(aggregate7))
            {
                duplicate[7] = true; target[7] = target[6];
            }

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>>> newInitialState =
                () => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.InitialState() : () => default),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.InitialState() : () => default),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.InitialState() : () => default),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.InitialState() : () => default),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.InitialState() : () => default),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.InitialState() : () => default),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.InitialState() : () => default),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.InitialState() : () => default)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>, long, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>>> newAccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Accumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Accumulate() : (s, a, b) => s, oldState.Item2, timestamp, input),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Accumulate() : (s, a, b) => s, oldState.Item3, timestamp, input),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Accumulate() : (s, a, b) => s, oldState.Item4, timestamp, input),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Accumulate() : (s, a, b) => s, oldState.Item5, timestamp, input),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Accumulate() : (s, a, b) => s, oldState.Item6, timestamp, input),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.Accumulate() : (s, a, b) => s, oldState.Item7, timestamp, input),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.Accumulate() : (s, a, b) => s, oldState.Item8, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>, long, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>>> newDeaccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Deaccumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Deaccumulate() : (s, a, b) => s, oldState.Item2, timestamp, input),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Deaccumulate() : (s, a, b) => s, oldState.Item3, timestamp, input),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Deaccumulate() : (s, a, b) => s, oldState.Item4, timestamp, input),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Deaccumulate() : (s, a, b) => s, oldState.Item5, timestamp, input),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Deaccumulate() : (s, a, b) => s, oldState.Item6, timestamp, input),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.Deaccumulate() : (s, a, b) => s, oldState.Item7, timestamp, input),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.Deaccumulate() : (s, a, b) => s, oldState.Item8, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>>> newDifference =
                (leftState, rightState) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Difference() : (l, r) => l, leftState.Item1, rightState.Item1),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Difference() : (l, r) => l, leftState.Item2, rightState.Item2),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Difference() : (l, r) => l, leftState.Item3, rightState.Item3),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Difference() : (l, r) => l, leftState.Item4, rightState.Item4),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Difference() : (l, r) => l, leftState.Item5, rightState.Item5),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Difference() : (l, r) => l, leftState.Item6, rightState.Item6),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.Difference() : (l, r) => l, leftState.Item7, rightState.Item7),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.Difference() : (l, r) => l, leftState.Item8, rightState.Item8)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>, TResult>> newComputeResult =
                state =>
                CallInliner.Call(
                    merger,
                    CallInliner.Call(aggregate1.ComputeResult(), CallInliner.Call(target[0] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>, TState1>>, state)),
                    CallInliner.Call(aggregate2.ComputeResult(), CallInliner.Call(target[1] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>, TState2>>, state)),
                    CallInliner.Call(aggregate3.ComputeResult(), CallInliner.Call(target[2] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>, TState3>>, state)),
                    CallInliner.Call(aggregate4.ComputeResult(), CallInliner.Call(target[3] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>, TState4>>, state)),
                    CallInliner.Call(aggregate5.ComputeResult(), CallInliner.Call(target[4] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>, TState5>>, state)),
                    CallInliner.Call(aggregate6.ComputeResult(), CallInliner.Call(target[5] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>, TState6>>, state)),
                    CallInliner.Call(aggregate7.ComputeResult(), CallInliner.Call(target[6] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>, TState7>>, state)),
                    CallInliner.Call(aggregate8.ComputeResult(), CallInliner.Call(target[7] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>, TState8>>, state)));

            return GeneratedAggregate.Create(
                initialState: newInitialState.InlineCalls(),
                accumulate: newAccumulate.InlineCalls(),
                deaccumulate: newDeaccumulate.InlineCalls(),
                difference: newDifference.InlineCalls(),
                computeResult: newComputeResult.InlineCalls());
        }

        /// <summary>
        /// Performs multiple aggregations simultaneously
        /// </summary>
        /// <typeparam name="TInput">The input stream to aggregate</typeparam>
        /// <typeparam name="TState1">Aggregation state type for aggregate number 1</typeparam>,
        /// <typeparam name="TState2">Aggregation state type for aggregate number 2</typeparam>,
        /// <typeparam name="TState3">Aggregation state type for aggregate number 3</typeparam>,
        /// <typeparam name="TState4">Aggregation state type for aggregate number 4</typeparam>,
        /// <typeparam name="TState5">Aggregation state type for aggregate number 5</typeparam>,
        /// <typeparam name="TState6">Aggregation state type for aggregate number 6</typeparam>,
        /// <typeparam name="TState7">Aggregation state type for aggregate number 7</typeparam>,
        /// <typeparam name="TState8">Aggregation state type for aggregate number 8</typeparam>,
        /// <typeparam name="TState9">Aggregation state type for aggregate number 9</typeparam>
        /// <typeparam name="TResult1">Result type for aggregate number 1</typeparam>,
        /// <typeparam name="TResult2">Result type for aggregate number 2</typeparam>,
        /// <typeparam name="TResult3">Result type for aggregate number 3</typeparam>,
        /// <typeparam name="TResult4">Result type for aggregate number 4</typeparam>,
        /// <typeparam name="TResult5">Result type for aggregate number 5</typeparam>,
        /// <typeparam name="TResult6">Result type for aggregate number 6</typeparam>,
        /// <typeparam name="TResult7">Result type for aggregate number 7</typeparam>,
        /// <typeparam name="TResult8">Result type for aggregate number 8</typeparam>,
        /// <typeparam name="TResult9">Result type for aggregate number 9</typeparam>
        /// <typeparam name="TResult">Result type of the merged aggregation</typeparam>
        /// <param name="aggregate1">Aggregation specification number 1</param>,
        /// <param name="aggregate2">Aggregation specification number 2</param>,
        /// <param name="aggregate3">Aggregation specification number 3</param>,
        /// <param name="aggregate4">Aggregation specification number 4</param>,
        /// <param name="aggregate5">Aggregation specification number 5</param>,
        /// <param name="aggregate6">Aggregation specification number 6</param>,
        /// <param name="aggregate7">Aggregation specification number 7</param>,
        /// <param name="aggregate8">Aggregation specification number 8</param>,
        /// <param name="aggregate9">Aggregation specification number 9</param>
        /// <param name="merger">Function to take the result of all aggregations and merge them into a single result</param>
        /// <returns></returns>
        public static IAggregate<TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>, TResult> Combine<TInput, TState1, TResult1, TState2, TResult2, TState3, TResult3, TState4, TResult4, TState5, TResult5, TState6, TResult6, TState7, TResult7, TState8, TResult8, TState9, TResult9, TResult>(
            IAggregate<TInput, TState1, TResult1> aggregate1,
            IAggregate<TInput, TState2, TResult2> aggregate2,
            IAggregate<TInput, TState3, TResult3> aggregate3,
            IAggregate<TInput, TState4, TResult4> aggregate4,
            IAggregate<TInput, TState5, TResult5> aggregate5,
            IAggregate<TInput, TState6, TResult6> aggregate6,
            IAggregate<TInput, TState7, TResult7> aggregate7,
            IAggregate<TInput, TState8, TResult8> aggregate8,
            IAggregate<TInput, TState9, TResult9> aggregate9,
            Expression<Func<TResult1, TResult2, TResult3, TResult4, TResult5, TResult6, TResult7, TResult8, TResult9, TResult>> merger)
        {
            Contract.Requires(aggregate1 != null);
            Contract.Requires(aggregate2 != null);
            Contract.Requires(aggregate3 != null);
            Contract.Requires(aggregate4 != null);
            Contract.Requires(aggregate5 != null);
            Contract.Requires(aggregate6 != null);
            Contract.Requires(aggregate7 != null);
            Contract.Requires(aggregate8 != null);
            Contract.Requires(aggregate9 != null);
            Contract.Requires(merger != null);

            var duplicate = new bool[9];
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>, TState1>> target1 = state => state.Item1;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>, TState2>> target2 = state => state.Item2;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>, TState3>> target3 = state => state.Item3;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>, TState4>> target4 = state => state.Item4;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>, TState5>> target5 = state => state.Item5;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>, TState6>> target6 = state => state.Item6;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>, TState7>> target7 = state => state.Item7;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>, TState8>> target8 = state => state.Item8;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>, TState9>> target9 = state => state.Item9;
            var target = new System.Collections.Generic.List<LambdaExpression> { target1, target2, target3, target4, target5, target6, target7, target8, target9 };
            if (aggregate2.HasSameStateAs(aggregate1))
            {
                duplicate[1] = true; target[1] = target[0];
            }
            if (aggregate3.HasSameStateAs(aggregate1))
            {
                duplicate[2] = true; target[2] = target[0];
            }
            else if (aggregate3.HasSameStateAs(aggregate2))
            {
                duplicate[2] = true; target[2] = target[1];
            }
            if (aggregate4.HasSameStateAs(aggregate1))
            {
                duplicate[3] = true; target[3] = target[0];
            }
            else if (aggregate4.HasSameStateAs(aggregate2))
            {
                duplicate[3] = true; target[3] = target[1];
            }
            else if (aggregate4.HasSameStateAs(aggregate3))
            {
                duplicate[3] = true; target[3] = target[2];
            }
            if (aggregate5.HasSameStateAs(aggregate1))
            {
                duplicate[4] = true; target[4] = target[0];
            }
            else if (aggregate5.HasSameStateAs(aggregate2))
            {
                duplicate[4] = true; target[4] = target[1];
            }
            else if (aggregate5.HasSameStateAs(aggregate3))
            {
                duplicate[4] = true; target[4] = target[2];
            }
            else if (aggregate5.HasSameStateAs(aggregate4))
            {
                duplicate[4] = true; target[4] = target[3];
            }
            if (aggregate6.HasSameStateAs(aggregate1))
            {
                duplicate[5] = true; target[5] = target[0];
            }
            else if (aggregate6.HasSameStateAs(aggregate2))
            {
                duplicate[5] = true; target[5] = target[1];
            }
            else if (aggregate6.HasSameStateAs(aggregate3))
            {
                duplicate[5] = true; target[5] = target[2];
            }
            else if (aggregate6.HasSameStateAs(aggregate4))
            {
                duplicate[5] = true; target[5] = target[3];
            }
            else if (aggregate6.HasSameStateAs(aggregate5))
            {
                duplicate[5] = true; target[5] = target[4];
            }
            if (aggregate7.HasSameStateAs(aggregate1))
            {
                duplicate[6] = true; target[6] = target[0];
            }
            else if (aggregate7.HasSameStateAs(aggregate2))
            {
                duplicate[6] = true; target[6] = target[1];
            }
            else if (aggregate7.HasSameStateAs(aggregate3))
            {
                duplicate[6] = true; target[6] = target[2];
            }
            else if (aggregate7.HasSameStateAs(aggregate4))
            {
                duplicate[6] = true; target[6] = target[3];
            }
            else if (aggregate7.HasSameStateAs(aggregate5))
            {
                duplicate[6] = true; target[6] = target[4];
            }
            else if (aggregate7.HasSameStateAs(aggregate6))
            {
                duplicate[6] = true; target[6] = target[5];
            }
            if (aggregate8.HasSameStateAs(aggregate1))
            {
                duplicate[7] = true; target[7] = target[0];
            }
            else if (aggregate8.HasSameStateAs(aggregate2))
            {
                duplicate[7] = true; target[7] = target[1];
            }
            else if (aggregate8.HasSameStateAs(aggregate3))
            {
                duplicate[7] = true; target[7] = target[2];
            }
            else if (aggregate8.HasSameStateAs(aggregate4))
            {
                duplicate[7] = true; target[7] = target[3];
            }
            else if (aggregate8.HasSameStateAs(aggregate5))
            {
                duplicate[7] = true; target[7] = target[4];
            }
            else if (aggregate8.HasSameStateAs(aggregate6))
            {
                duplicate[7] = true; target[7] = target[5];
            }
            else if (aggregate8.HasSameStateAs(aggregate7))
            {
                duplicate[7] = true; target[7] = target[6];
            }
            if (aggregate9.HasSameStateAs(aggregate1))
            {
                duplicate[8] = true; target[8] = target[0];
            }
            else if (aggregate9.HasSameStateAs(aggregate2))
            {
                duplicate[8] = true; target[8] = target[1];
            }
            else if (aggregate9.HasSameStateAs(aggregate3))
            {
                duplicate[8] = true; target[8] = target[2];
            }
            else if (aggregate9.HasSameStateAs(aggregate4))
            {
                duplicate[8] = true; target[8] = target[3];
            }
            else if (aggregate9.HasSameStateAs(aggregate5))
            {
                duplicate[8] = true; target[8] = target[4];
            }
            else if (aggregate9.HasSameStateAs(aggregate6))
            {
                duplicate[8] = true; target[8] = target[5];
            }
            else if (aggregate9.HasSameStateAs(aggregate7))
            {
                duplicate[8] = true; target[8] = target[6];
            }
            else if (aggregate9.HasSameStateAs(aggregate8))
            {
                duplicate[8] = true; target[8] = target[7];
            }

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>>> newInitialState =
                () => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.InitialState() : () => default),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.InitialState() : () => default),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.InitialState() : () => default),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.InitialState() : () => default),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.InitialState() : () => default),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.InitialState() : () => default),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.InitialState() : () => default),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.InitialState() : () => default),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.InitialState() : () => default)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>, long, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>>> newAccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Accumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Accumulate() : (s, a, b) => s, oldState.Item2, timestamp, input),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Accumulate() : (s, a, b) => s, oldState.Item3, timestamp, input),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Accumulate() : (s, a, b) => s, oldState.Item4, timestamp, input),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Accumulate() : (s, a, b) => s, oldState.Item5, timestamp, input),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Accumulate() : (s, a, b) => s, oldState.Item6, timestamp, input),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.Accumulate() : (s, a, b) => s, oldState.Item7, timestamp, input),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.Accumulate() : (s, a, b) => s, oldState.Item8, timestamp, input),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.Accumulate() : (s, a, b) => s, oldState.Item9, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>, long, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>>> newDeaccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Deaccumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Deaccumulate() : (s, a, b) => s, oldState.Item2, timestamp, input),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Deaccumulate() : (s, a, b) => s, oldState.Item3, timestamp, input),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Deaccumulate() : (s, a, b) => s, oldState.Item4, timestamp, input),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Deaccumulate() : (s, a, b) => s, oldState.Item5, timestamp, input),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Deaccumulate() : (s, a, b) => s, oldState.Item6, timestamp, input),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.Deaccumulate() : (s, a, b) => s, oldState.Item7, timestamp, input),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.Deaccumulate() : (s, a, b) => s, oldState.Item8, timestamp, input),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.Deaccumulate() : (s, a, b) => s, oldState.Item9, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>>> newDifference =
                (leftState, rightState) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Difference() : (l, r) => l, leftState.Item1, rightState.Item1),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Difference() : (l, r) => l, leftState.Item2, rightState.Item2),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Difference() : (l, r) => l, leftState.Item3, rightState.Item3),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Difference() : (l, r) => l, leftState.Item4, rightState.Item4),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Difference() : (l, r) => l, leftState.Item5, rightState.Item5),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Difference() : (l, r) => l, leftState.Item6, rightState.Item6),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.Difference() : (l, r) => l, leftState.Item7, rightState.Item7),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.Difference() : (l, r) => l, leftState.Item8, rightState.Item8),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.Difference() : (l, r) => l, leftState.Item9, rightState.Item9)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>, TResult>> newComputeResult =
                state =>
                CallInliner.Call(
                    merger,
                    CallInliner.Call(aggregate1.ComputeResult(), CallInliner.Call(target[0] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>, TState1>>, state)),
                    CallInliner.Call(aggregate2.ComputeResult(), CallInliner.Call(target[1] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>, TState2>>, state)),
                    CallInliner.Call(aggregate3.ComputeResult(), CallInliner.Call(target[2] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>, TState3>>, state)),
                    CallInliner.Call(aggregate4.ComputeResult(), CallInliner.Call(target[3] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>, TState4>>, state)),
                    CallInliner.Call(aggregate5.ComputeResult(), CallInliner.Call(target[4] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>, TState5>>, state)),
                    CallInliner.Call(aggregate6.ComputeResult(), CallInliner.Call(target[5] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>, TState6>>, state)),
                    CallInliner.Call(aggregate7.ComputeResult(), CallInliner.Call(target[6] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>, TState7>>, state)),
                    CallInliner.Call(aggregate8.ComputeResult(), CallInliner.Call(target[7] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>, TState8>>, state)),
                    CallInliner.Call(aggregate9.ComputeResult(), CallInliner.Call(target[8] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>, TState9>>, state)));

            return GeneratedAggregate.Create(
                initialState: newInitialState.InlineCalls(),
                accumulate: newAccumulate.InlineCalls(),
                deaccumulate: newDeaccumulate.InlineCalls(),
                difference: newDifference.InlineCalls(),
                computeResult: newComputeResult.InlineCalls());
        }

        /// <summary>
        /// Performs multiple aggregations simultaneously
        /// </summary>
        /// <typeparam name="TInput">The input stream to aggregate</typeparam>
        /// <typeparam name="TState1">Aggregation state type for aggregate number 1</typeparam>,
        /// <typeparam name="TState2">Aggregation state type for aggregate number 2</typeparam>,
        /// <typeparam name="TState3">Aggregation state type for aggregate number 3</typeparam>,
        /// <typeparam name="TState4">Aggregation state type for aggregate number 4</typeparam>,
        /// <typeparam name="TState5">Aggregation state type for aggregate number 5</typeparam>,
        /// <typeparam name="TState6">Aggregation state type for aggregate number 6</typeparam>,
        /// <typeparam name="TState7">Aggregation state type for aggregate number 7</typeparam>,
        /// <typeparam name="TState8">Aggregation state type for aggregate number 8</typeparam>,
        /// <typeparam name="TState9">Aggregation state type for aggregate number 9</typeparam>,
        /// <typeparam name="TState10">Aggregation state type for aggregate number 10</typeparam>
        /// <typeparam name="TResult1">Result type for aggregate number 1</typeparam>,
        /// <typeparam name="TResult2">Result type for aggregate number 2</typeparam>,
        /// <typeparam name="TResult3">Result type for aggregate number 3</typeparam>,
        /// <typeparam name="TResult4">Result type for aggregate number 4</typeparam>,
        /// <typeparam name="TResult5">Result type for aggregate number 5</typeparam>,
        /// <typeparam name="TResult6">Result type for aggregate number 6</typeparam>,
        /// <typeparam name="TResult7">Result type for aggregate number 7</typeparam>,
        /// <typeparam name="TResult8">Result type for aggregate number 8</typeparam>,
        /// <typeparam name="TResult9">Result type for aggregate number 9</typeparam>,
        /// <typeparam name="TResult10">Result type for aggregate number 10</typeparam>
        /// <typeparam name="TResult">Result type of the merged aggregation</typeparam>
        /// <param name="aggregate1">Aggregation specification number 1</param>,
        /// <param name="aggregate2">Aggregation specification number 2</param>,
        /// <param name="aggregate3">Aggregation specification number 3</param>,
        /// <param name="aggregate4">Aggregation specification number 4</param>,
        /// <param name="aggregate5">Aggregation specification number 5</param>,
        /// <param name="aggregate6">Aggregation specification number 6</param>,
        /// <param name="aggregate7">Aggregation specification number 7</param>,
        /// <param name="aggregate8">Aggregation specification number 8</param>,
        /// <param name="aggregate9">Aggregation specification number 9</param>,
        /// <param name="aggregate10">Aggregation specification number 10</param>
        /// <param name="merger">Function to take the result of all aggregations and merge them into a single result</param>
        /// <returns></returns>
        public static IAggregate<TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, TResult> Combine<TInput, TState1, TResult1, TState2, TResult2, TState3, TResult3, TState4, TResult4, TState5, TResult5, TState6, TResult6, TState7, TResult7, TState8, TResult8, TState9, TResult9, TState10, TResult10, TResult>(
            IAggregate<TInput, TState1, TResult1> aggregate1,
            IAggregate<TInput, TState2, TResult2> aggregate2,
            IAggregate<TInput, TState3, TResult3> aggregate3,
            IAggregate<TInput, TState4, TResult4> aggregate4,
            IAggregate<TInput, TState5, TResult5> aggregate5,
            IAggregate<TInput, TState6, TResult6> aggregate6,
            IAggregate<TInput, TState7, TResult7> aggregate7,
            IAggregate<TInput, TState8, TResult8> aggregate8,
            IAggregate<TInput, TState9, TResult9> aggregate9,
            IAggregate<TInput, TState10, TResult10> aggregate10,
            Expression<Func<TResult1, TResult2, TResult3, TResult4, TResult5, TResult6, TResult7, TResult8, TResult9, TResult10, TResult>> merger)
        {
            Contract.Requires(aggregate1 != null);
            Contract.Requires(aggregate2 != null);
            Contract.Requires(aggregate3 != null);
            Contract.Requires(aggregate4 != null);
            Contract.Requires(aggregate5 != null);
            Contract.Requires(aggregate6 != null);
            Contract.Requires(aggregate7 != null);
            Contract.Requires(aggregate8 != null);
            Contract.Requires(aggregate9 != null);
            Contract.Requires(aggregate10 != null);
            Contract.Requires(merger != null);

            var duplicate = new bool[10];
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, TState1>> target1 = state => state.Item1;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, TState2>> target2 = state => state.Item2;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, TState3>> target3 = state => state.Item3;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, TState4>> target4 = state => state.Item4;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, TState5>> target5 = state => state.Item5;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, TState6>> target6 = state => state.Item6;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, TState7>> target7 = state => state.Item7;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, TState8>> target8 = state => state.Item8;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, TState9>> target9 = state => state.Item9;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, TState10>> target10 = state => state.Item10;
            var target = new System.Collections.Generic.List<LambdaExpression> { target1, target2, target3, target4, target5, target6, target7, target8, target9, target10 };
            if (aggregate2.HasSameStateAs(aggregate1))
            {
                duplicate[1] = true; target[1] = target[0];
            }
            if (aggregate3.HasSameStateAs(aggregate1))
            {
                duplicate[2] = true; target[2] = target[0];
            }
            else if (aggregate3.HasSameStateAs(aggregate2))
            {
                duplicate[2] = true; target[2] = target[1];
            }
            if (aggregate4.HasSameStateAs(aggregate1))
            {
                duplicate[3] = true; target[3] = target[0];
            }
            else if (aggregate4.HasSameStateAs(aggregate2))
            {
                duplicate[3] = true; target[3] = target[1];
            }
            else if (aggregate4.HasSameStateAs(aggregate3))
            {
                duplicate[3] = true; target[3] = target[2];
            }
            if (aggregate5.HasSameStateAs(aggregate1))
            {
                duplicate[4] = true; target[4] = target[0];
            }
            else if (aggregate5.HasSameStateAs(aggregate2))
            {
                duplicate[4] = true; target[4] = target[1];
            }
            else if (aggregate5.HasSameStateAs(aggregate3))
            {
                duplicate[4] = true; target[4] = target[2];
            }
            else if (aggregate5.HasSameStateAs(aggregate4))
            {
                duplicate[4] = true; target[4] = target[3];
            }
            if (aggregate6.HasSameStateAs(aggregate1))
            {
                duplicate[5] = true; target[5] = target[0];
            }
            else if (aggregate6.HasSameStateAs(aggregate2))
            {
                duplicate[5] = true; target[5] = target[1];
            }
            else if (aggregate6.HasSameStateAs(aggregate3))
            {
                duplicate[5] = true; target[5] = target[2];
            }
            else if (aggregate6.HasSameStateAs(aggregate4))
            {
                duplicate[5] = true; target[5] = target[3];
            }
            else if (aggregate6.HasSameStateAs(aggregate5))
            {
                duplicate[5] = true; target[5] = target[4];
            }
            if (aggregate7.HasSameStateAs(aggregate1))
            {
                duplicate[6] = true; target[6] = target[0];
            }
            else if (aggregate7.HasSameStateAs(aggregate2))
            {
                duplicate[6] = true; target[6] = target[1];
            }
            else if (aggregate7.HasSameStateAs(aggregate3))
            {
                duplicate[6] = true; target[6] = target[2];
            }
            else if (aggregate7.HasSameStateAs(aggregate4))
            {
                duplicate[6] = true; target[6] = target[3];
            }
            else if (aggregate7.HasSameStateAs(aggregate5))
            {
                duplicate[6] = true; target[6] = target[4];
            }
            else if (aggregate7.HasSameStateAs(aggregate6))
            {
                duplicate[6] = true; target[6] = target[5];
            }
            if (aggregate8.HasSameStateAs(aggregate1))
            {
                duplicate[7] = true; target[7] = target[0];
            }
            else if (aggregate8.HasSameStateAs(aggregate2))
            {
                duplicate[7] = true; target[7] = target[1];
            }
            else if (aggregate8.HasSameStateAs(aggregate3))
            {
                duplicate[7] = true; target[7] = target[2];
            }
            else if (aggregate8.HasSameStateAs(aggregate4))
            {
                duplicate[7] = true; target[7] = target[3];
            }
            else if (aggregate8.HasSameStateAs(aggregate5))
            {
                duplicate[7] = true; target[7] = target[4];
            }
            else if (aggregate8.HasSameStateAs(aggregate6))
            {
                duplicate[7] = true; target[7] = target[5];
            }
            else if (aggregate8.HasSameStateAs(aggregate7))
            {
                duplicate[7] = true; target[7] = target[6];
            }
            if (aggregate9.HasSameStateAs(aggregate1))
            {
                duplicate[8] = true; target[8] = target[0];
            }
            else if (aggregate9.HasSameStateAs(aggregate2))
            {
                duplicate[8] = true; target[8] = target[1];
            }
            else if (aggregate9.HasSameStateAs(aggregate3))
            {
                duplicate[8] = true; target[8] = target[2];
            }
            else if (aggregate9.HasSameStateAs(aggregate4))
            {
                duplicate[8] = true; target[8] = target[3];
            }
            else if (aggregate9.HasSameStateAs(aggregate5))
            {
                duplicate[8] = true; target[8] = target[4];
            }
            else if (aggregate9.HasSameStateAs(aggregate6))
            {
                duplicate[8] = true; target[8] = target[5];
            }
            else if (aggregate9.HasSameStateAs(aggregate7))
            {
                duplicate[8] = true; target[8] = target[6];
            }
            else if (aggregate9.HasSameStateAs(aggregate8))
            {
                duplicate[8] = true; target[8] = target[7];
            }
            if (aggregate10.HasSameStateAs(aggregate1))
            {
                duplicate[9] = true; target[9] = target[0];
            }
            else if (aggregate10.HasSameStateAs(aggregate2))
            {
                duplicate[9] = true; target[9] = target[1];
            }
            else if (aggregate10.HasSameStateAs(aggregate3))
            {
                duplicate[9] = true; target[9] = target[2];
            }
            else if (aggregate10.HasSameStateAs(aggregate4))
            {
                duplicate[9] = true; target[9] = target[3];
            }
            else if (aggregate10.HasSameStateAs(aggregate5))
            {
                duplicate[9] = true; target[9] = target[4];
            }
            else if (aggregate10.HasSameStateAs(aggregate6))
            {
                duplicate[9] = true; target[9] = target[5];
            }
            else if (aggregate10.HasSameStateAs(aggregate7))
            {
                duplicate[9] = true; target[9] = target[6];
            }
            else if (aggregate10.HasSameStateAs(aggregate8))
            {
                duplicate[9] = true; target[9] = target[7];
            }
            else if (aggregate10.HasSameStateAs(aggregate9))
            {
                duplicate[9] = true; target[9] = target[8];
            }

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>>> newInitialState =
                () => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.InitialState() : () => default),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.InitialState() : () => default),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.InitialState() : () => default),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.InitialState() : () => default),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.InitialState() : () => default),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.InitialState() : () => default),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.InitialState() : () => default),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.InitialState() : () => default),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.InitialState() : () => default),
                    Item10 = CallInliner.Call(!duplicate[9] ? aggregate10.InitialState() : () => default)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, long, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>>> newAccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Accumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Accumulate() : (s, a, b) => s, oldState.Item2, timestamp, input),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Accumulate() : (s, a, b) => s, oldState.Item3, timestamp, input),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Accumulate() : (s, a, b) => s, oldState.Item4, timestamp, input),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Accumulate() : (s, a, b) => s, oldState.Item5, timestamp, input),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Accumulate() : (s, a, b) => s, oldState.Item6, timestamp, input),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.Accumulate() : (s, a, b) => s, oldState.Item7, timestamp, input),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.Accumulate() : (s, a, b) => s, oldState.Item8, timestamp, input),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.Accumulate() : (s, a, b) => s, oldState.Item9, timestamp, input),
                    Item10 = CallInliner.Call(!duplicate[9] ? aggregate10.Accumulate() : (s, a, b) => s, oldState.Item10, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, long, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>>> newDeaccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Deaccumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Deaccumulate() : (s, a, b) => s, oldState.Item2, timestamp, input),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Deaccumulate() : (s, a, b) => s, oldState.Item3, timestamp, input),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Deaccumulate() : (s, a, b) => s, oldState.Item4, timestamp, input),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Deaccumulate() : (s, a, b) => s, oldState.Item5, timestamp, input),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Deaccumulate() : (s, a, b) => s, oldState.Item6, timestamp, input),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.Deaccumulate() : (s, a, b) => s, oldState.Item7, timestamp, input),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.Deaccumulate() : (s, a, b) => s, oldState.Item8, timestamp, input),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.Deaccumulate() : (s, a, b) => s, oldState.Item9, timestamp, input),
                    Item10 = CallInliner.Call(!duplicate[9] ? aggregate10.Deaccumulate() : (s, a, b) => s, oldState.Item10, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>>> newDifference =
                (leftState, rightState) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Difference() : (l, r) => l, leftState.Item1, rightState.Item1),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Difference() : (l, r) => l, leftState.Item2, rightState.Item2),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Difference() : (l, r) => l, leftState.Item3, rightState.Item3),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Difference() : (l, r) => l, leftState.Item4, rightState.Item4),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Difference() : (l, r) => l, leftState.Item5, rightState.Item5),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Difference() : (l, r) => l, leftState.Item6, rightState.Item6),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.Difference() : (l, r) => l, leftState.Item7, rightState.Item7),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.Difference() : (l, r) => l, leftState.Item8, rightState.Item8),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.Difference() : (l, r) => l, leftState.Item9, rightState.Item9),
                    Item10 = CallInliner.Call(!duplicate[9] ? aggregate10.Difference() : (l, r) => l, leftState.Item10, rightState.Item10)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, TResult>> newComputeResult =
                state =>
                CallInliner.Call(
                    merger,
                    CallInliner.Call(aggregate1.ComputeResult(), CallInliner.Call(target[0] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, TState1>>, state)),
                    CallInliner.Call(aggregate2.ComputeResult(), CallInliner.Call(target[1] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, TState2>>, state)),
                    CallInliner.Call(aggregate3.ComputeResult(), CallInliner.Call(target[2] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, TState3>>, state)),
                    CallInliner.Call(aggregate4.ComputeResult(), CallInliner.Call(target[3] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, TState4>>, state)),
                    CallInliner.Call(aggregate5.ComputeResult(), CallInliner.Call(target[4] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, TState5>>, state)),
                    CallInliner.Call(aggregate6.ComputeResult(), CallInliner.Call(target[5] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, TState6>>, state)),
                    CallInliner.Call(aggregate7.ComputeResult(), CallInliner.Call(target[6] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, TState7>>, state)),
                    CallInliner.Call(aggregate8.ComputeResult(), CallInliner.Call(target[7] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, TState8>>, state)),
                    CallInliner.Call(aggregate9.ComputeResult(), CallInliner.Call(target[8] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, TState9>>, state)),
                    CallInliner.Call(aggregate10.ComputeResult(), CallInliner.Call(target[9] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, TState10>>, state)));

            return GeneratedAggregate.Create(
                initialState: newInitialState.InlineCalls(),
                accumulate: newAccumulate.InlineCalls(),
                deaccumulate: newDeaccumulate.InlineCalls(),
                difference: newDifference.InlineCalls(),
                computeResult: newComputeResult.InlineCalls());
        }

        /// <summary>
        /// Performs multiple aggregations simultaneously
        /// </summary>
        /// <typeparam name="TInput">The input stream to aggregate</typeparam>
        /// <typeparam name="TState1">Aggregation state type for aggregate number 1</typeparam>,
        /// <typeparam name="TState2">Aggregation state type for aggregate number 2</typeparam>,
        /// <typeparam name="TState3">Aggregation state type for aggregate number 3</typeparam>,
        /// <typeparam name="TState4">Aggregation state type for aggregate number 4</typeparam>,
        /// <typeparam name="TState5">Aggregation state type for aggregate number 5</typeparam>,
        /// <typeparam name="TState6">Aggregation state type for aggregate number 6</typeparam>,
        /// <typeparam name="TState7">Aggregation state type for aggregate number 7</typeparam>,
        /// <typeparam name="TState8">Aggregation state type for aggregate number 8</typeparam>,
        /// <typeparam name="TState9">Aggregation state type for aggregate number 9</typeparam>,
        /// <typeparam name="TState10">Aggregation state type for aggregate number 10</typeparam>,
        /// <typeparam name="TState11">Aggregation state type for aggregate number 11</typeparam>
        /// <typeparam name="TResult1">Result type for aggregate number 1</typeparam>,
        /// <typeparam name="TResult2">Result type for aggregate number 2</typeparam>,
        /// <typeparam name="TResult3">Result type for aggregate number 3</typeparam>,
        /// <typeparam name="TResult4">Result type for aggregate number 4</typeparam>,
        /// <typeparam name="TResult5">Result type for aggregate number 5</typeparam>,
        /// <typeparam name="TResult6">Result type for aggregate number 6</typeparam>,
        /// <typeparam name="TResult7">Result type for aggregate number 7</typeparam>,
        /// <typeparam name="TResult8">Result type for aggregate number 8</typeparam>,
        /// <typeparam name="TResult9">Result type for aggregate number 9</typeparam>,
        /// <typeparam name="TResult10">Result type for aggregate number 10</typeparam>,
        /// <typeparam name="TResult11">Result type for aggregate number 11</typeparam>
        /// <typeparam name="TResult">Result type of the merged aggregation</typeparam>
        /// <param name="aggregate1">Aggregation specification number 1</param>,
        /// <param name="aggregate2">Aggregation specification number 2</param>,
        /// <param name="aggregate3">Aggregation specification number 3</param>,
        /// <param name="aggregate4">Aggregation specification number 4</param>,
        /// <param name="aggregate5">Aggregation specification number 5</param>,
        /// <param name="aggregate6">Aggregation specification number 6</param>,
        /// <param name="aggregate7">Aggregation specification number 7</param>,
        /// <param name="aggregate8">Aggregation specification number 8</param>,
        /// <param name="aggregate9">Aggregation specification number 9</param>,
        /// <param name="aggregate10">Aggregation specification number 10</param>,
        /// <param name="aggregate11">Aggregation specification number 11</param>
        /// <param name="merger">Function to take the result of all aggregations and merge them into a single result</param>
        /// <returns></returns>
        public static IAggregate<TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, TResult> Combine<TInput, TState1, TResult1, TState2, TResult2, TState3, TResult3, TState4, TResult4, TState5, TResult5, TState6, TResult6, TState7, TResult7, TState8, TResult8, TState9, TResult9, TState10, TResult10, TState11, TResult11, TResult>(
            IAggregate<TInput, TState1, TResult1> aggregate1,
            IAggregate<TInput, TState2, TResult2> aggregate2,
            IAggregate<TInput, TState3, TResult3> aggregate3,
            IAggregate<TInput, TState4, TResult4> aggregate4,
            IAggregate<TInput, TState5, TResult5> aggregate5,
            IAggregate<TInput, TState6, TResult6> aggregate6,
            IAggregate<TInput, TState7, TResult7> aggregate7,
            IAggregate<TInput, TState8, TResult8> aggregate8,
            IAggregate<TInput, TState9, TResult9> aggregate9,
            IAggregate<TInput, TState10, TResult10> aggregate10,
            IAggregate<TInput, TState11, TResult11> aggregate11,
            Expression<Func<TResult1, TResult2, TResult3, TResult4, TResult5, TResult6, TResult7, TResult8, TResult9, TResult10, TResult11, TResult>> merger)
        {
            Contract.Requires(aggregate1 != null);
            Contract.Requires(aggregate2 != null);
            Contract.Requires(aggregate3 != null);
            Contract.Requires(aggregate4 != null);
            Contract.Requires(aggregate5 != null);
            Contract.Requires(aggregate6 != null);
            Contract.Requires(aggregate7 != null);
            Contract.Requires(aggregate8 != null);
            Contract.Requires(aggregate9 != null);
            Contract.Requires(aggregate10 != null);
            Contract.Requires(aggregate11 != null);
            Contract.Requires(merger != null);

            var duplicate = new bool[11];
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, TState1>> target1 = state => state.Item1;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, TState2>> target2 = state => state.Item2;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, TState3>> target3 = state => state.Item3;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, TState4>> target4 = state => state.Item4;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, TState5>> target5 = state => state.Item5;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, TState6>> target6 = state => state.Item6;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, TState7>> target7 = state => state.Item7;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, TState8>> target8 = state => state.Item8;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, TState9>> target9 = state => state.Item9;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, TState10>> target10 = state => state.Item10;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, TState11>> target11 = state => state.Item11;
            var target = new System.Collections.Generic.List<LambdaExpression> { target1, target2, target3, target4, target5, target6, target7, target8, target9, target10, target11 };
            if (aggregate2.HasSameStateAs(aggregate1))
            {
                duplicate[1] = true; target[1] = target[0];
            }
            if (aggregate3.HasSameStateAs(aggregate1))
            {
                duplicate[2] = true; target[2] = target[0];
            }
            else if (aggregate3.HasSameStateAs(aggregate2))
            {
                duplicate[2] = true; target[2] = target[1];
            }
            if (aggregate4.HasSameStateAs(aggregate1))
            {
                duplicate[3] = true; target[3] = target[0];
            }
            else if (aggregate4.HasSameStateAs(aggregate2))
            {
                duplicate[3] = true; target[3] = target[1];
            }
            else if (aggregate4.HasSameStateAs(aggregate3))
            {
                duplicate[3] = true; target[3] = target[2];
            }
            if (aggregate5.HasSameStateAs(aggregate1))
            {
                duplicate[4] = true; target[4] = target[0];
            }
            else if (aggregate5.HasSameStateAs(aggregate2))
            {
                duplicate[4] = true; target[4] = target[1];
            }
            else if (aggregate5.HasSameStateAs(aggregate3))
            {
                duplicate[4] = true; target[4] = target[2];
            }
            else if (aggregate5.HasSameStateAs(aggregate4))
            {
                duplicate[4] = true; target[4] = target[3];
            }
            if (aggregate6.HasSameStateAs(aggregate1))
            {
                duplicate[5] = true; target[5] = target[0];
            }
            else if (aggregate6.HasSameStateAs(aggregate2))
            {
                duplicate[5] = true; target[5] = target[1];
            }
            else if (aggregate6.HasSameStateAs(aggregate3))
            {
                duplicate[5] = true; target[5] = target[2];
            }
            else if (aggregate6.HasSameStateAs(aggregate4))
            {
                duplicate[5] = true; target[5] = target[3];
            }
            else if (aggregate6.HasSameStateAs(aggregate5))
            {
                duplicate[5] = true; target[5] = target[4];
            }
            if (aggregate7.HasSameStateAs(aggregate1))
            {
                duplicate[6] = true; target[6] = target[0];
            }
            else if (aggregate7.HasSameStateAs(aggregate2))
            {
                duplicate[6] = true; target[6] = target[1];
            }
            else if (aggregate7.HasSameStateAs(aggregate3))
            {
                duplicate[6] = true; target[6] = target[2];
            }
            else if (aggregate7.HasSameStateAs(aggregate4))
            {
                duplicate[6] = true; target[6] = target[3];
            }
            else if (aggregate7.HasSameStateAs(aggregate5))
            {
                duplicate[6] = true; target[6] = target[4];
            }
            else if (aggregate7.HasSameStateAs(aggregate6))
            {
                duplicate[6] = true; target[6] = target[5];
            }
            if (aggregate8.HasSameStateAs(aggregate1))
            {
                duplicate[7] = true; target[7] = target[0];
            }
            else if (aggregate8.HasSameStateAs(aggregate2))
            {
                duplicate[7] = true; target[7] = target[1];
            }
            else if (aggregate8.HasSameStateAs(aggregate3))
            {
                duplicate[7] = true; target[7] = target[2];
            }
            else if (aggregate8.HasSameStateAs(aggregate4))
            {
                duplicate[7] = true; target[7] = target[3];
            }
            else if (aggregate8.HasSameStateAs(aggregate5))
            {
                duplicate[7] = true; target[7] = target[4];
            }
            else if (aggregate8.HasSameStateAs(aggregate6))
            {
                duplicate[7] = true; target[7] = target[5];
            }
            else if (aggregate8.HasSameStateAs(aggregate7))
            {
                duplicate[7] = true; target[7] = target[6];
            }
            if (aggregate9.HasSameStateAs(aggregate1))
            {
                duplicate[8] = true; target[8] = target[0];
            }
            else if (aggregate9.HasSameStateAs(aggregate2))
            {
                duplicate[8] = true; target[8] = target[1];
            }
            else if (aggregate9.HasSameStateAs(aggregate3))
            {
                duplicate[8] = true; target[8] = target[2];
            }
            else if (aggregate9.HasSameStateAs(aggregate4))
            {
                duplicate[8] = true; target[8] = target[3];
            }
            else if (aggregate9.HasSameStateAs(aggregate5))
            {
                duplicate[8] = true; target[8] = target[4];
            }
            else if (aggregate9.HasSameStateAs(aggregate6))
            {
                duplicate[8] = true; target[8] = target[5];
            }
            else if (aggregate9.HasSameStateAs(aggregate7))
            {
                duplicate[8] = true; target[8] = target[6];
            }
            else if (aggregate9.HasSameStateAs(aggregate8))
            {
                duplicate[8] = true; target[8] = target[7];
            }
            if (aggregate10.HasSameStateAs(aggregate1))
            {
                duplicate[9] = true; target[9] = target[0];
            }
            else if (aggregate10.HasSameStateAs(aggregate2))
            {
                duplicate[9] = true; target[9] = target[1];
            }
            else if (aggregate10.HasSameStateAs(aggregate3))
            {
                duplicate[9] = true; target[9] = target[2];
            }
            else if (aggregate10.HasSameStateAs(aggregate4))
            {
                duplicate[9] = true; target[9] = target[3];
            }
            else if (aggregate10.HasSameStateAs(aggregate5))
            {
                duplicate[9] = true; target[9] = target[4];
            }
            else if (aggregate10.HasSameStateAs(aggregate6))
            {
                duplicate[9] = true; target[9] = target[5];
            }
            else if (aggregate10.HasSameStateAs(aggregate7))
            {
                duplicate[9] = true; target[9] = target[6];
            }
            else if (aggregate10.HasSameStateAs(aggregate8))
            {
                duplicate[9] = true; target[9] = target[7];
            }
            else if (aggregate10.HasSameStateAs(aggregate9))
            {
                duplicate[9] = true; target[9] = target[8];
            }
            if (aggregate11.HasSameStateAs(aggregate1))
            {
                duplicate[10] = true; target[10] = target[0];
            }
            else if (aggregate11.HasSameStateAs(aggregate2))
            {
                duplicate[10] = true; target[10] = target[1];
            }
            else if (aggregate11.HasSameStateAs(aggregate3))
            {
                duplicate[10] = true; target[10] = target[2];
            }
            else if (aggregate11.HasSameStateAs(aggregate4))
            {
                duplicate[10] = true; target[10] = target[3];
            }
            else if (aggregate11.HasSameStateAs(aggregate5))
            {
                duplicate[10] = true; target[10] = target[4];
            }
            else if (aggregate11.HasSameStateAs(aggregate6))
            {
                duplicate[10] = true; target[10] = target[5];
            }
            else if (aggregate11.HasSameStateAs(aggregate7))
            {
                duplicate[10] = true; target[10] = target[6];
            }
            else if (aggregate11.HasSameStateAs(aggregate8))
            {
                duplicate[10] = true; target[10] = target[7];
            }
            else if (aggregate11.HasSameStateAs(aggregate9))
            {
                duplicate[10] = true; target[10] = target[8];
            }
            else if (aggregate11.HasSameStateAs(aggregate10))
            {
                duplicate[10] = true; target[10] = target[9];
            }

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>>> newInitialState =
                () => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.InitialState() : () => default),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.InitialState() : () => default),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.InitialState() : () => default),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.InitialState() : () => default),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.InitialState() : () => default),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.InitialState() : () => default),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.InitialState() : () => default),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.InitialState() : () => default),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.InitialState() : () => default),
                    Item10 = CallInliner.Call(!duplicate[9] ? aggregate10.InitialState() : () => default),
                    Item11 = CallInliner.Call(!duplicate[10] ? aggregate11.InitialState() : () => default)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, long, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>>> newAccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Accumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Accumulate() : (s, a, b) => s, oldState.Item2, timestamp, input),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Accumulate() : (s, a, b) => s, oldState.Item3, timestamp, input),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Accumulate() : (s, a, b) => s, oldState.Item4, timestamp, input),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Accumulate() : (s, a, b) => s, oldState.Item5, timestamp, input),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Accumulate() : (s, a, b) => s, oldState.Item6, timestamp, input),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.Accumulate() : (s, a, b) => s, oldState.Item7, timestamp, input),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.Accumulate() : (s, a, b) => s, oldState.Item8, timestamp, input),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.Accumulate() : (s, a, b) => s, oldState.Item9, timestamp, input),
                    Item10 = CallInliner.Call(!duplicate[9] ? aggregate10.Accumulate() : (s, a, b) => s, oldState.Item10, timestamp, input),
                    Item11 = CallInliner.Call(!duplicate[10] ? aggregate11.Accumulate() : (s, a, b) => s, oldState.Item11, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, long, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>>> newDeaccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Deaccumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Deaccumulate() : (s, a, b) => s, oldState.Item2, timestamp, input),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Deaccumulate() : (s, a, b) => s, oldState.Item3, timestamp, input),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Deaccumulate() : (s, a, b) => s, oldState.Item4, timestamp, input),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Deaccumulate() : (s, a, b) => s, oldState.Item5, timestamp, input),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Deaccumulate() : (s, a, b) => s, oldState.Item6, timestamp, input),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.Deaccumulate() : (s, a, b) => s, oldState.Item7, timestamp, input),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.Deaccumulate() : (s, a, b) => s, oldState.Item8, timestamp, input),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.Deaccumulate() : (s, a, b) => s, oldState.Item9, timestamp, input),
                    Item10 = CallInliner.Call(!duplicate[9] ? aggregate10.Deaccumulate() : (s, a, b) => s, oldState.Item10, timestamp, input),
                    Item11 = CallInliner.Call(!duplicate[10] ? aggregate11.Deaccumulate() : (s, a, b) => s, oldState.Item11, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>>> newDifference =
                (leftState, rightState) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Difference() : (l, r) => l, leftState.Item1, rightState.Item1),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Difference() : (l, r) => l, leftState.Item2, rightState.Item2),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Difference() : (l, r) => l, leftState.Item3, rightState.Item3),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Difference() : (l, r) => l, leftState.Item4, rightState.Item4),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Difference() : (l, r) => l, leftState.Item5, rightState.Item5),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Difference() : (l, r) => l, leftState.Item6, rightState.Item6),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.Difference() : (l, r) => l, leftState.Item7, rightState.Item7),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.Difference() : (l, r) => l, leftState.Item8, rightState.Item8),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.Difference() : (l, r) => l, leftState.Item9, rightState.Item9),
                    Item10 = CallInliner.Call(!duplicate[9] ? aggregate10.Difference() : (l, r) => l, leftState.Item10, rightState.Item10),
                    Item11 = CallInliner.Call(!duplicate[10] ? aggregate11.Difference() : (l, r) => l, leftState.Item11, rightState.Item11)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, TResult>> newComputeResult =
                state =>
                CallInliner.Call(
                    merger,
                    CallInliner.Call(aggregate1.ComputeResult(), CallInliner.Call(target[0] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, TState1>>, state)),
                    CallInliner.Call(aggregate2.ComputeResult(), CallInliner.Call(target[1] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, TState2>>, state)),
                    CallInliner.Call(aggregate3.ComputeResult(), CallInliner.Call(target[2] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, TState3>>, state)),
                    CallInliner.Call(aggregate4.ComputeResult(), CallInliner.Call(target[3] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, TState4>>, state)),
                    CallInliner.Call(aggregate5.ComputeResult(), CallInliner.Call(target[4] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, TState5>>, state)),
                    CallInliner.Call(aggregate6.ComputeResult(), CallInliner.Call(target[5] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, TState6>>, state)),
                    CallInliner.Call(aggregate7.ComputeResult(), CallInliner.Call(target[6] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, TState7>>, state)),
                    CallInliner.Call(aggregate8.ComputeResult(), CallInliner.Call(target[7] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, TState8>>, state)),
                    CallInliner.Call(aggregate9.ComputeResult(), CallInliner.Call(target[8] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, TState9>>, state)),
                    CallInliner.Call(aggregate10.ComputeResult(), CallInliner.Call(target[9] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, TState10>>, state)),
                    CallInliner.Call(aggregate11.ComputeResult(), CallInliner.Call(target[10] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, TState11>>, state)));

            return GeneratedAggregate.Create(
                initialState: newInitialState.InlineCalls(),
                accumulate: newAccumulate.InlineCalls(),
                deaccumulate: newDeaccumulate.InlineCalls(),
                difference: newDifference.InlineCalls(),
                computeResult: newComputeResult.InlineCalls());
        }

        /// <summary>
        /// Performs multiple aggregations simultaneously
        /// </summary>
        /// <typeparam name="TInput">The input stream to aggregate</typeparam>
        /// <typeparam name="TState1">Aggregation state type for aggregate number 1</typeparam>,
        /// <typeparam name="TState2">Aggregation state type for aggregate number 2</typeparam>,
        /// <typeparam name="TState3">Aggregation state type for aggregate number 3</typeparam>,
        /// <typeparam name="TState4">Aggregation state type for aggregate number 4</typeparam>,
        /// <typeparam name="TState5">Aggregation state type for aggregate number 5</typeparam>,
        /// <typeparam name="TState6">Aggregation state type for aggregate number 6</typeparam>,
        /// <typeparam name="TState7">Aggregation state type for aggregate number 7</typeparam>,
        /// <typeparam name="TState8">Aggregation state type for aggregate number 8</typeparam>,
        /// <typeparam name="TState9">Aggregation state type for aggregate number 9</typeparam>,
        /// <typeparam name="TState10">Aggregation state type for aggregate number 10</typeparam>,
        /// <typeparam name="TState11">Aggregation state type for aggregate number 11</typeparam>,
        /// <typeparam name="TState12">Aggregation state type for aggregate number 12</typeparam>
        /// <typeparam name="TResult1">Result type for aggregate number 1</typeparam>,
        /// <typeparam name="TResult2">Result type for aggregate number 2</typeparam>,
        /// <typeparam name="TResult3">Result type for aggregate number 3</typeparam>,
        /// <typeparam name="TResult4">Result type for aggregate number 4</typeparam>,
        /// <typeparam name="TResult5">Result type for aggregate number 5</typeparam>,
        /// <typeparam name="TResult6">Result type for aggregate number 6</typeparam>,
        /// <typeparam name="TResult7">Result type for aggregate number 7</typeparam>,
        /// <typeparam name="TResult8">Result type for aggregate number 8</typeparam>,
        /// <typeparam name="TResult9">Result type for aggregate number 9</typeparam>,
        /// <typeparam name="TResult10">Result type for aggregate number 10</typeparam>,
        /// <typeparam name="TResult11">Result type for aggregate number 11</typeparam>,
        /// <typeparam name="TResult12">Result type for aggregate number 12</typeparam>
        /// <typeparam name="TResult">Result type of the merged aggregation</typeparam>
        /// <param name="aggregate1">Aggregation specification number 1</param>,
        /// <param name="aggregate2">Aggregation specification number 2</param>,
        /// <param name="aggregate3">Aggregation specification number 3</param>,
        /// <param name="aggregate4">Aggregation specification number 4</param>,
        /// <param name="aggregate5">Aggregation specification number 5</param>,
        /// <param name="aggregate6">Aggregation specification number 6</param>,
        /// <param name="aggregate7">Aggregation specification number 7</param>,
        /// <param name="aggregate8">Aggregation specification number 8</param>,
        /// <param name="aggregate9">Aggregation specification number 9</param>,
        /// <param name="aggregate10">Aggregation specification number 10</param>,
        /// <param name="aggregate11">Aggregation specification number 11</param>,
        /// <param name="aggregate12">Aggregation specification number 12</param>
        /// <param name="merger">Function to take the result of all aggregations and merge them into a single result</param>
        /// <returns></returns>
        public static IAggregate<TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TResult> Combine<TInput, TState1, TResult1, TState2, TResult2, TState3, TResult3, TState4, TResult4, TState5, TResult5, TState6, TResult6, TState7, TResult7, TState8, TResult8, TState9, TResult9, TState10, TResult10, TState11, TResult11, TState12, TResult12, TResult>(
            IAggregate<TInput, TState1, TResult1> aggregate1,
            IAggregate<TInput, TState2, TResult2> aggregate2,
            IAggregate<TInput, TState3, TResult3> aggregate3,
            IAggregate<TInput, TState4, TResult4> aggregate4,
            IAggregate<TInput, TState5, TResult5> aggregate5,
            IAggregate<TInput, TState6, TResult6> aggregate6,
            IAggregate<TInput, TState7, TResult7> aggregate7,
            IAggregate<TInput, TState8, TResult8> aggregate8,
            IAggregate<TInput, TState9, TResult9> aggregate9,
            IAggregate<TInput, TState10, TResult10> aggregate10,
            IAggregate<TInput, TState11, TResult11> aggregate11,
            IAggregate<TInput, TState12, TResult12> aggregate12,
            Expression<Func<TResult1, TResult2, TResult3, TResult4, TResult5, TResult6, TResult7, TResult8, TResult9, TResult10, TResult11, TResult12, TResult>> merger)
        {
            Contract.Requires(aggregate1 != null);
            Contract.Requires(aggregate2 != null);
            Contract.Requires(aggregate3 != null);
            Contract.Requires(aggregate4 != null);
            Contract.Requires(aggregate5 != null);
            Contract.Requires(aggregate6 != null);
            Contract.Requires(aggregate7 != null);
            Contract.Requires(aggregate8 != null);
            Contract.Requires(aggregate9 != null);
            Contract.Requires(aggregate10 != null);
            Contract.Requires(aggregate11 != null);
            Contract.Requires(aggregate12 != null);
            Contract.Requires(merger != null);

            var duplicate = new bool[12];
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TState1>> target1 = state => state.Item1;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TState2>> target2 = state => state.Item2;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TState3>> target3 = state => state.Item3;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TState4>> target4 = state => state.Item4;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TState5>> target5 = state => state.Item5;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TState6>> target6 = state => state.Item6;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TState7>> target7 = state => state.Item7;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TState8>> target8 = state => state.Item8;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TState9>> target9 = state => state.Item9;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TState10>> target10 = state => state.Item10;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TState11>> target11 = state => state.Item11;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TState12>> target12 = state => state.Item12;
            var target = new System.Collections.Generic.List<LambdaExpression> { target1, target2, target3, target4, target5, target6, target7, target8, target9, target10, target11, target12 };
            if (aggregate2.HasSameStateAs(aggregate1))
            {
                duplicate[1] = true; target[1] = target[0];
            }
            if (aggregate3.HasSameStateAs(aggregate1))
            {
                duplicate[2] = true; target[2] = target[0];
            }
            else if (aggregate3.HasSameStateAs(aggregate2))
            {
                duplicate[2] = true; target[2] = target[1];
            }
            if (aggregate4.HasSameStateAs(aggregate1))
            {
                duplicate[3] = true; target[3] = target[0];
            }
            else if (aggregate4.HasSameStateAs(aggregate2))
            {
                duplicate[3] = true; target[3] = target[1];
            }
            else if (aggregate4.HasSameStateAs(aggregate3))
            {
                duplicate[3] = true; target[3] = target[2];
            }
            if (aggregate5.HasSameStateAs(aggregate1))
            {
                duplicate[4] = true; target[4] = target[0];
            }
            else if (aggregate5.HasSameStateAs(aggregate2))
            {
                duplicate[4] = true; target[4] = target[1];
            }
            else if (aggregate5.HasSameStateAs(aggregate3))
            {
                duplicate[4] = true; target[4] = target[2];
            }
            else if (aggregate5.HasSameStateAs(aggregate4))
            {
                duplicate[4] = true; target[4] = target[3];
            }
            if (aggregate6.HasSameStateAs(aggregate1))
            {
                duplicate[5] = true; target[5] = target[0];
            }
            else if (aggregate6.HasSameStateAs(aggregate2))
            {
                duplicate[5] = true; target[5] = target[1];
            }
            else if (aggregate6.HasSameStateAs(aggregate3))
            {
                duplicate[5] = true; target[5] = target[2];
            }
            else if (aggregate6.HasSameStateAs(aggregate4))
            {
                duplicate[5] = true; target[5] = target[3];
            }
            else if (aggregate6.HasSameStateAs(aggregate5))
            {
                duplicate[5] = true; target[5] = target[4];
            }
            if (aggregate7.HasSameStateAs(aggregate1))
            {
                duplicate[6] = true; target[6] = target[0];
            }
            else if (aggregate7.HasSameStateAs(aggregate2))
            {
                duplicate[6] = true; target[6] = target[1];
            }
            else if (aggregate7.HasSameStateAs(aggregate3))
            {
                duplicate[6] = true; target[6] = target[2];
            }
            else if (aggregate7.HasSameStateAs(aggregate4))
            {
                duplicate[6] = true; target[6] = target[3];
            }
            else if (aggregate7.HasSameStateAs(aggregate5))
            {
                duplicate[6] = true; target[6] = target[4];
            }
            else if (aggregate7.HasSameStateAs(aggregate6))
            {
                duplicate[6] = true; target[6] = target[5];
            }
            if (aggregate8.HasSameStateAs(aggregate1))
            {
                duplicate[7] = true; target[7] = target[0];
            }
            else if (aggregate8.HasSameStateAs(aggregate2))
            {
                duplicate[7] = true; target[7] = target[1];
            }
            else if (aggregate8.HasSameStateAs(aggregate3))
            {
                duplicate[7] = true; target[7] = target[2];
            }
            else if (aggregate8.HasSameStateAs(aggregate4))
            {
                duplicate[7] = true; target[7] = target[3];
            }
            else if (aggregate8.HasSameStateAs(aggregate5))
            {
                duplicate[7] = true; target[7] = target[4];
            }
            else if (aggregate8.HasSameStateAs(aggregate6))
            {
                duplicate[7] = true; target[7] = target[5];
            }
            else if (aggregate8.HasSameStateAs(aggregate7))
            {
                duplicate[7] = true; target[7] = target[6];
            }
            if (aggregate9.HasSameStateAs(aggregate1))
            {
                duplicate[8] = true; target[8] = target[0];
            }
            else if (aggregate9.HasSameStateAs(aggregate2))
            {
                duplicate[8] = true; target[8] = target[1];
            }
            else if (aggregate9.HasSameStateAs(aggregate3))
            {
                duplicate[8] = true; target[8] = target[2];
            }
            else if (aggregate9.HasSameStateAs(aggregate4))
            {
                duplicate[8] = true; target[8] = target[3];
            }
            else if (aggregate9.HasSameStateAs(aggregate5))
            {
                duplicate[8] = true; target[8] = target[4];
            }
            else if (aggregate9.HasSameStateAs(aggregate6))
            {
                duplicate[8] = true; target[8] = target[5];
            }
            else if (aggregate9.HasSameStateAs(aggregate7))
            {
                duplicate[8] = true; target[8] = target[6];
            }
            else if (aggregate9.HasSameStateAs(aggregate8))
            {
                duplicate[8] = true; target[8] = target[7];
            }
            if (aggregate10.HasSameStateAs(aggregate1))
            {
                duplicate[9] = true; target[9] = target[0];
            }
            else if (aggregate10.HasSameStateAs(aggregate2))
            {
                duplicate[9] = true; target[9] = target[1];
            }
            else if (aggregate10.HasSameStateAs(aggregate3))
            {
                duplicate[9] = true; target[9] = target[2];
            }
            else if (aggregate10.HasSameStateAs(aggregate4))
            {
                duplicate[9] = true; target[9] = target[3];
            }
            else if (aggregate10.HasSameStateAs(aggregate5))
            {
                duplicate[9] = true; target[9] = target[4];
            }
            else if (aggregate10.HasSameStateAs(aggregate6))
            {
                duplicate[9] = true; target[9] = target[5];
            }
            else if (aggregate10.HasSameStateAs(aggregate7))
            {
                duplicate[9] = true; target[9] = target[6];
            }
            else if (aggregate10.HasSameStateAs(aggregate8))
            {
                duplicate[9] = true; target[9] = target[7];
            }
            else if (aggregate10.HasSameStateAs(aggregate9))
            {
                duplicate[9] = true; target[9] = target[8];
            }
            if (aggregate11.HasSameStateAs(aggregate1))
            {
                duplicate[10] = true; target[10] = target[0];
            }
            else if (aggregate11.HasSameStateAs(aggregate2))
            {
                duplicate[10] = true; target[10] = target[1];
            }
            else if (aggregate11.HasSameStateAs(aggregate3))
            {
                duplicate[10] = true; target[10] = target[2];
            }
            else if (aggregate11.HasSameStateAs(aggregate4))
            {
                duplicate[10] = true; target[10] = target[3];
            }
            else if (aggregate11.HasSameStateAs(aggregate5))
            {
                duplicate[10] = true; target[10] = target[4];
            }
            else if (aggregate11.HasSameStateAs(aggregate6))
            {
                duplicate[10] = true; target[10] = target[5];
            }
            else if (aggregate11.HasSameStateAs(aggregate7))
            {
                duplicate[10] = true; target[10] = target[6];
            }
            else if (aggregate11.HasSameStateAs(aggregate8))
            {
                duplicate[10] = true; target[10] = target[7];
            }
            else if (aggregate11.HasSameStateAs(aggregate9))
            {
                duplicate[10] = true; target[10] = target[8];
            }
            else if (aggregate11.HasSameStateAs(aggregate10))
            {
                duplicate[10] = true; target[10] = target[9];
            }
            if (aggregate12.HasSameStateAs(aggregate1))
            {
                duplicate[11] = true; target[11] = target[0];
            }
            else if (aggregate12.HasSameStateAs(aggregate2))
            {
                duplicate[11] = true; target[11] = target[1];
            }
            else if (aggregate12.HasSameStateAs(aggregate3))
            {
                duplicate[11] = true; target[11] = target[2];
            }
            else if (aggregate12.HasSameStateAs(aggregate4))
            {
                duplicate[11] = true; target[11] = target[3];
            }
            else if (aggregate12.HasSameStateAs(aggregate5))
            {
                duplicate[11] = true; target[11] = target[4];
            }
            else if (aggregate12.HasSameStateAs(aggregate6))
            {
                duplicate[11] = true; target[11] = target[5];
            }
            else if (aggregate12.HasSameStateAs(aggregate7))
            {
                duplicate[11] = true; target[11] = target[6];
            }
            else if (aggregate12.HasSameStateAs(aggregate8))
            {
                duplicate[11] = true; target[11] = target[7];
            }
            else if (aggregate12.HasSameStateAs(aggregate9))
            {
                duplicate[11] = true; target[11] = target[8];
            }
            else if (aggregate12.HasSameStateAs(aggregate10))
            {
                duplicate[11] = true; target[11] = target[9];
            }
            else if (aggregate12.HasSameStateAs(aggregate11))
            {
                duplicate[11] = true; target[11] = target[10];
            }

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>>> newInitialState =
                () => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.InitialState() : () => default),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.InitialState() : () => default),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.InitialState() : () => default),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.InitialState() : () => default),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.InitialState() : () => default),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.InitialState() : () => default),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.InitialState() : () => default),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.InitialState() : () => default),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.InitialState() : () => default),
                    Item10 = CallInliner.Call(!duplicate[9] ? aggregate10.InitialState() : () => default),
                    Item11 = CallInliner.Call(!duplicate[10] ? aggregate11.InitialState() : () => default),
                    Item12 = CallInliner.Call(!duplicate[11] ? aggregate12.InitialState() : () => default)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, long, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>>> newAccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Accumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Accumulate() : (s, a, b) => s, oldState.Item2, timestamp, input),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Accumulate() : (s, a, b) => s, oldState.Item3, timestamp, input),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Accumulate() : (s, a, b) => s, oldState.Item4, timestamp, input),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Accumulate() : (s, a, b) => s, oldState.Item5, timestamp, input),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Accumulate() : (s, a, b) => s, oldState.Item6, timestamp, input),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.Accumulate() : (s, a, b) => s, oldState.Item7, timestamp, input),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.Accumulate() : (s, a, b) => s, oldState.Item8, timestamp, input),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.Accumulate() : (s, a, b) => s, oldState.Item9, timestamp, input),
                    Item10 = CallInliner.Call(!duplicate[9] ? aggregate10.Accumulate() : (s, a, b) => s, oldState.Item10, timestamp, input),
                    Item11 = CallInliner.Call(!duplicate[10] ? aggregate11.Accumulate() : (s, a, b) => s, oldState.Item11, timestamp, input),
                    Item12 = CallInliner.Call(!duplicate[11] ? aggregate12.Accumulate() : (s, a, b) => s, oldState.Item12, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, long, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>>> newDeaccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Deaccumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Deaccumulate() : (s, a, b) => s, oldState.Item2, timestamp, input),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Deaccumulate() : (s, a, b) => s, oldState.Item3, timestamp, input),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Deaccumulate() : (s, a, b) => s, oldState.Item4, timestamp, input),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Deaccumulate() : (s, a, b) => s, oldState.Item5, timestamp, input),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Deaccumulate() : (s, a, b) => s, oldState.Item6, timestamp, input),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.Deaccumulate() : (s, a, b) => s, oldState.Item7, timestamp, input),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.Deaccumulate() : (s, a, b) => s, oldState.Item8, timestamp, input),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.Deaccumulate() : (s, a, b) => s, oldState.Item9, timestamp, input),
                    Item10 = CallInliner.Call(!duplicate[9] ? aggregate10.Deaccumulate() : (s, a, b) => s, oldState.Item10, timestamp, input),
                    Item11 = CallInliner.Call(!duplicate[10] ? aggregate11.Deaccumulate() : (s, a, b) => s, oldState.Item11, timestamp, input),
                    Item12 = CallInliner.Call(!duplicate[11] ? aggregate12.Deaccumulate() : (s, a, b) => s, oldState.Item12, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>>> newDifference =
                (leftState, rightState) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Difference() : (l, r) => l, leftState.Item1, rightState.Item1),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Difference() : (l, r) => l, leftState.Item2, rightState.Item2),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Difference() : (l, r) => l, leftState.Item3, rightState.Item3),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Difference() : (l, r) => l, leftState.Item4, rightState.Item4),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Difference() : (l, r) => l, leftState.Item5, rightState.Item5),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Difference() : (l, r) => l, leftState.Item6, rightState.Item6),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.Difference() : (l, r) => l, leftState.Item7, rightState.Item7),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.Difference() : (l, r) => l, leftState.Item8, rightState.Item8),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.Difference() : (l, r) => l, leftState.Item9, rightState.Item9),
                    Item10 = CallInliner.Call(!duplicate[9] ? aggregate10.Difference() : (l, r) => l, leftState.Item10, rightState.Item10),
                    Item11 = CallInliner.Call(!duplicate[10] ? aggregate11.Difference() : (l, r) => l, leftState.Item11, rightState.Item11),
                    Item12 = CallInliner.Call(!duplicate[11] ? aggregate12.Difference() : (l, r) => l, leftState.Item12, rightState.Item12)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TResult>> newComputeResult =
                state =>
                CallInliner.Call(
                    merger,
                    CallInliner.Call(aggregate1.ComputeResult(), CallInliner.Call(target[0] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TState1>>, state)),
                    CallInliner.Call(aggregate2.ComputeResult(), CallInliner.Call(target[1] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TState2>>, state)),
                    CallInliner.Call(aggregate3.ComputeResult(), CallInliner.Call(target[2] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TState3>>, state)),
                    CallInliner.Call(aggregate4.ComputeResult(), CallInliner.Call(target[3] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TState4>>, state)),
                    CallInliner.Call(aggregate5.ComputeResult(), CallInliner.Call(target[4] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TState5>>, state)),
                    CallInliner.Call(aggregate6.ComputeResult(), CallInliner.Call(target[5] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TState6>>, state)),
                    CallInliner.Call(aggregate7.ComputeResult(), CallInliner.Call(target[6] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TState7>>, state)),
                    CallInliner.Call(aggregate8.ComputeResult(), CallInliner.Call(target[7] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TState8>>, state)),
                    CallInliner.Call(aggregate9.ComputeResult(), CallInliner.Call(target[8] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TState9>>, state)),
                    CallInliner.Call(aggregate10.ComputeResult(), CallInliner.Call(target[9] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TState10>>, state)),
                    CallInliner.Call(aggregate11.ComputeResult(), CallInliner.Call(target[10] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TState11>>, state)),
                    CallInliner.Call(aggregate12.ComputeResult(), CallInliner.Call(target[11] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TState12>>, state)));

            return GeneratedAggregate.Create(
                initialState: newInitialState.InlineCalls(),
                accumulate: newAccumulate.InlineCalls(),
                deaccumulate: newDeaccumulate.InlineCalls(),
                difference: newDifference.InlineCalls(),
                computeResult: newComputeResult.InlineCalls());
        }

        /// <summary>
        /// Performs multiple aggregations simultaneously
        /// </summary>
        /// <typeparam name="TInput">The input stream to aggregate</typeparam>
        /// <typeparam name="TState1">Aggregation state type for aggregate number 1</typeparam>,
        /// <typeparam name="TState2">Aggregation state type for aggregate number 2</typeparam>,
        /// <typeparam name="TState3">Aggregation state type for aggregate number 3</typeparam>,
        /// <typeparam name="TState4">Aggregation state type for aggregate number 4</typeparam>,
        /// <typeparam name="TState5">Aggregation state type for aggregate number 5</typeparam>,
        /// <typeparam name="TState6">Aggregation state type for aggregate number 6</typeparam>,
        /// <typeparam name="TState7">Aggregation state type for aggregate number 7</typeparam>,
        /// <typeparam name="TState8">Aggregation state type for aggregate number 8</typeparam>,
        /// <typeparam name="TState9">Aggregation state type for aggregate number 9</typeparam>,
        /// <typeparam name="TState10">Aggregation state type for aggregate number 10</typeparam>,
        /// <typeparam name="TState11">Aggregation state type for aggregate number 11</typeparam>,
        /// <typeparam name="TState12">Aggregation state type for aggregate number 12</typeparam>,
        /// <typeparam name="TState13">Aggregation state type for aggregate number 13</typeparam>
        /// <typeparam name="TResult1">Result type for aggregate number 1</typeparam>,
        /// <typeparam name="TResult2">Result type for aggregate number 2</typeparam>,
        /// <typeparam name="TResult3">Result type for aggregate number 3</typeparam>,
        /// <typeparam name="TResult4">Result type for aggregate number 4</typeparam>,
        /// <typeparam name="TResult5">Result type for aggregate number 5</typeparam>,
        /// <typeparam name="TResult6">Result type for aggregate number 6</typeparam>,
        /// <typeparam name="TResult7">Result type for aggregate number 7</typeparam>,
        /// <typeparam name="TResult8">Result type for aggregate number 8</typeparam>,
        /// <typeparam name="TResult9">Result type for aggregate number 9</typeparam>,
        /// <typeparam name="TResult10">Result type for aggregate number 10</typeparam>,
        /// <typeparam name="TResult11">Result type for aggregate number 11</typeparam>,
        /// <typeparam name="TResult12">Result type for aggregate number 12</typeparam>,
        /// <typeparam name="TResult13">Result type for aggregate number 13</typeparam>
        /// <typeparam name="TResult">Result type of the merged aggregation</typeparam>
        /// <param name="aggregate1">Aggregation specification number 1</param>,
        /// <param name="aggregate2">Aggregation specification number 2</param>,
        /// <param name="aggregate3">Aggregation specification number 3</param>,
        /// <param name="aggregate4">Aggregation specification number 4</param>,
        /// <param name="aggregate5">Aggregation specification number 5</param>,
        /// <param name="aggregate6">Aggregation specification number 6</param>,
        /// <param name="aggregate7">Aggregation specification number 7</param>,
        /// <param name="aggregate8">Aggregation specification number 8</param>,
        /// <param name="aggregate9">Aggregation specification number 9</param>,
        /// <param name="aggregate10">Aggregation specification number 10</param>,
        /// <param name="aggregate11">Aggregation specification number 11</param>,
        /// <param name="aggregate12">Aggregation specification number 12</param>,
        /// <param name="aggregate13">Aggregation specification number 13</param>
        /// <param name="merger">Function to take the result of all aggregations and merge them into a single result</param>
        /// <returns></returns>
        public static IAggregate<TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TResult> Combine<TInput, TState1, TResult1, TState2, TResult2, TState3, TResult3, TState4, TResult4, TState5, TResult5, TState6, TResult6, TState7, TResult7, TState8, TResult8, TState9, TResult9, TState10, TResult10, TState11, TResult11, TState12, TResult12, TState13, TResult13, TResult>(
            IAggregate<TInput, TState1, TResult1> aggregate1,
            IAggregate<TInput, TState2, TResult2> aggregate2,
            IAggregate<TInput, TState3, TResult3> aggregate3,
            IAggregate<TInput, TState4, TResult4> aggregate4,
            IAggregate<TInput, TState5, TResult5> aggregate5,
            IAggregate<TInput, TState6, TResult6> aggregate6,
            IAggregate<TInput, TState7, TResult7> aggregate7,
            IAggregate<TInput, TState8, TResult8> aggregate8,
            IAggregate<TInput, TState9, TResult9> aggregate9,
            IAggregate<TInput, TState10, TResult10> aggregate10,
            IAggregate<TInput, TState11, TResult11> aggregate11,
            IAggregate<TInput, TState12, TResult12> aggregate12,
            IAggregate<TInput, TState13, TResult13> aggregate13,
            Expression<Func<TResult1, TResult2, TResult3, TResult4, TResult5, TResult6, TResult7, TResult8, TResult9, TResult10, TResult11, TResult12, TResult13, TResult>> merger)
        {
            Contract.Requires(aggregate1 != null);
            Contract.Requires(aggregate2 != null);
            Contract.Requires(aggregate3 != null);
            Contract.Requires(aggregate4 != null);
            Contract.Requires(aggregate5 != null);
            Contract.Requires(aggregate6 != null);
            Contract.Requires(aggregate7 != null);
            Contract.Requires(aggregate8 != null);
            Contract.Requires(aggregate9 != null);
            Contract.Requires(aggregate10 != null);
            Contract.Requires(aggregate11 != null);
            Contract.Requires(aggregate12 != null);
            Contract.Requires(aggregate13 != null);
            Contract.Requires(merger != null);

            var duplicate = new bool[13];
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TState1>> target1 = state => state.Item1;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TState2>> target2 = state => state.Item2;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TState3>> target3 = state => state.Item3;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TState4>> target4 = state => state.Item4;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TState5>> target5 = state => state.Item5;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TState6>> target6 = state => state.Item6;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TState7>> target7 = state => state.Item7;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TState8>> target8 = state => state.Item8;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TState9>> target9 = state => state.Item9;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TState10>> target10 = state => state.Item10;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TState11>> target11 = state => state.Item11;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TState12>> target12 = state => state.Item12;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TState13>> target13 = state => state.Item13;
            var target = new System.Collections.Generic.List<LambdaExpression> { target1, target2, target3, target4, target5, target6, target7, target8, target9, target10, target11, target12, target13 };
            if (aggregate2.HasSameStateAs(aggregate1))
            {
                duplicate[1] = true; target[1] = target[0];
            }
            if (aggregate3.HasSameStateAs(aggregate1))
            {
                duplicate[2] = true; target[2] = target[0];
            }
            else if (aggregate3.HasSameStateAs(aggregate2))
            {
                duplicate[2] = true; target[2] = target[1];
            }
            if (aggregate4.HasSameStateAs(aggregate1))
            {
                duplicate[3] = true; target[3] = target[0];
            }
            else if (aggregate4.HasSameStateAs(aggregate2))
            {
                duplicate[3] = true; target[3] = target[1];
            }
            else if (aggregate4.HasSameStateAs(aggregate3))
            {
                duplicate[3] = true; target[3] = target[2];
            }
            if (aggregate5.HasSameStateAs(aggregate1))
            {
                duplicate[4] = true; target[4] = target[0];
            }
            else if (aggregate5.HasSameStateAs(aggregate2))
            {
                duplicate[4] = true; target[4] = target[1];
            }
            else if (aggregate5.HasSameStateAs(aggregate3))
            {
                duplicate[4] = true; target[4] = target[2];
            }
            else if (aggregate5.HasSameStateAs(aggregate4))
            {
                duplicate[4] = true; target[4] = target[3];
            }
            if (aggregate6.HasSameStateAs(aggregate1))
            {
                duplicate[5] = true; target[5] = target[0];
            }
            else if (aggregate6.HasSameStateAs(aggregate2))
            {
                duplicate[5] = true; target[5] = target[1];
            }
            else if (aggregate6.HasSameStateAs(aggregate3))
            {
                duplicate[5] = true; target[5] = target[2];
            }
            else if (aggregate6.HasSameStateAs(aggregate4))
            {
                duplicate[5] = true; target[5] = target[3];
            }
            else if (aggregate6.HasSameStateAs(aggregate5))
            {
                duplicate[5] = true; target[5] = target[4];
            }
            if (aggregate7.HasSameStateAs(aggregate1))
            {
                duplicate[6] = true; target[6] = target[0];
            }
            else if (aggregate7.HasSameStateAs(aggregate2))
            {
                duplicate[6] = true; target[6] = target[1];
            }
            else if (aggregate7.HasSameStateAs(aggregate3))
            {
                duplicate[6] = true; target[6] = target[2];
            }
            else if (aggregate7.HasSameStateAs(aggregate4))
            {
                duplicate[6] = true; target[6] = target[3];
            }
            else if (aggregate7.HasSameStateAs(aggregate5))
            {
                duplicate[6] = true; target[6] = target[4];
            }
            else if (aggregate7.HasSameStateAs(aggregate6))
            {
                duplicate[6] = true; target[6] = target[5];
            }
            if (aggregate8.HasSameStateAs(aggregate1))
            {
                duplicate[7] = true; target[7] = target[0];
            }
            else if (aggregate8.HasSameStateAs(aggregate2))
            {
                duplicate[7] = true; target[7] = target[1];
            }
            else if (aggregate8.HasSameStateAs(aggregate3))
            {
                duplicate[7] = true; target[7] = target[2];
            }
            else if (aggregate8.HasSameStateAs(aggregate4))
            {
                duplicate[7] = true; target[7] = target[3];
            }
            else if (aggregate8.HasSameStateAs(aggregate5))
            {
                duplicate[7] = true; target[7] = target[4];
            }
            else if (aggregate8.HasSameStateAs(aggregate6))
            {
                duplicate[7] = true; target[7] = target[5];
            }
            else if (aggregate8.HasSameStateAs(aggregate7))
            {
                duplicate[7] = true; target[7] = target[6];
            }
            if (aggregate9.HasSameStateAs(aggregate1))
            {
                duplicate[8] = true; target[8] = target[0];
            }
            else if (aggregate9.HasSameStateAs(aggregate2))
            {
                duplicate[8] = true; target[8] = target[1];
            }
            else if (aggregate9.HasSameStateAs(aggregate3))
            {
                duplicate[8] = true; target[8] = target[2];
            }
            else if (aggregate9.HasSameStateAs(aggregate4))
            {
                duplicate[8] = true; target[8] = target[3];
            }
            else if (aggregate9.HasSameStateAs(aggregate5))
            {
                duplicate[8] = true; target[8] = target[4];
            }
            else if (aggregate9.HasSameStateAs(aggregate6))
            {
                duplicate[8] = true; target[8] = target[5];
            }
            else if (aggregate9.HasSameStateAs(aggregate7))
            {
                duplicate[8] = true; target[8] = target[6];
            }
            else if (aggregate9.HasSameStateAs(aggregate8))
            {
                duplicate[8] = true; target[8] = target[7];
            }
            if (aggregate10.HasSameStateAs(aggregate1))
            {
                duplicate[9] = true; target[9] = target[0];
            }
            else if (aggregate10.HasSameStateAs(aggregate2))
            {
                duplicate[9] = true; target[9] = target[1];
            }
            else if (aggregate10.HasSameStateAs(aggregate3))
            {
                duplicate[9] = true; target[9] = target[2];
            }
            else if (aggregate10.HasSameStateAs(aggregate4))
            {
                duplicate[9] = true; target[9] = target[3];
            }
            else if (aggregate10.HasSameStateAs(aggregate5))
            {
                duplicate[9] = true; target[9] = target[4];
            }
            else if (aggregate10.HasSameStateAs(aggregate6))
            {
                duplicate[9] = true; target[9] = target[5];
            }
            else if (aggregate10.HasSameStateAs(aggregate7))
            {
                duplicate[9] = true; target[9] = target[6];
            }
            else if (aggregate10.HasSameStateAs(aggregate8))
            {
                duplicate[9] = true; target[9] = target[7];
            }
            else if (aggregate10.HasSameStateAs(aggregate9))
            {
                duplicate[9] = true; target[9] = target[8];
            }
            if (aggregate11.HasSameStateAs(aggregate1))
            {
                duplicate[10] = true; target[10] = target[0];
            }
            else if (aggregate11.HasSameStateAs(aggregate2))
            {
                duplicate[10] = true; target[10] = target[1];
            }
            else if (aggregate11.HasSameStateAs(aggregate3))
            {
                duplicate[10] = true; target[10] = target[2];
            }
            else if (aggregate11.HasSameStateAs(aggregate4))
            {
                duplicate[10] = true; target[10] = target[3];
            }
            else if (aggregate11.HasSameStateAs(aggregate5))
            {
                duplicate[10] = true; target[10] = target[4];
            }
            else if (aggregate11.HasSameStateAs(aggregate6))
            {
                duplicate[10] = true; target[10] = target[5];
            }
            else if (aggregate11.HasSameStateAs(aggregate7))
            {
                duplicate[10] = true; target[10] = target[6];
            }
            else if (aggregate11.HasSameStateAs(aggregate8))
            {
                duplicate[10] = true; target[10] = target[7];
            }
            else if (aggregate11.HasSameStateAs(aggregate9))
            {
                duplicate[10] = true; target[10] = target[8];
            }
            else if (aggregate11.HasSameStateAs(aggregate10))
            {
                duplicate[10] = true; target[10] = target[9];
            }
            if (aggregate12.HasSameStateAs(aggregate1))
            {
                duplicate[11] = true; target[11] = target[0];
            }
            else if (aggregate12.HasSameStateAs(aggregate2))
            {
                duplicate[11] = true; target[11] = target[1];
            }
            else if (aggregate12.HasSameStateAs(aggregate3))
            {
                duplicate[11] = true; target[11] = target[2];
            }
            else if (aggregate12.HasSameStateAs(aggregate4))
            {
                duplicate[11] = true; target[11] = target[3];
            }
            else if (aggregate12.HasSameStateAs(aggregate5))
            {
                duplicate[11] = true; target[11] = target[4];
            }
            else if (aggregate12.HasSameStateAs(aggregate6))
            {
                duplicate[11] = true; target[11] = target[5];
            }
            else if (aggregate12.HasSameStateAs(aggregate7))
            {
                duplicate[11] = true; target[11] = target[6];
            }
            else if (aggregate12.HasSameStateAs(aggregate8))
            {
                duplicate[11] = true; target[11] = target[7];
            }
            else if (aggregate12.HasSameStateAs(aggregate9))
            {
                duplicate[11] = true; target[11] = target[8];
            }
            else if (aggregate12.HasSameStateAs(aggregate10))
            {
                duplicate[11] = true; target[11] = target[9];
            }
            else if (aggregate12.HasSameStateAs(aggregate11))
            {
                duplicate[11] = true; target[11] = target[10];
            }
            if (aggregate13.HasSameStateAs(aggregate1))
            {
                duplicate[12] = true; target[12] = target[0];
            }
            else if (aggregate13.HasSameStateAs(aggregate2))
            {
                duplicate[12] = true; target[12] = target[1];
            }
            else if (aggregate13.HasSameStateAs(aggregate3))
            {
                duplicate[12] = true; target[12] = target[2];
            }
            else if (aggregate13.HasSameStateAs(aggregate4))
            {
                duplicate[12] = true; target[12] = target[3];
            }
            else if (aggregate13.HasSameStateAs(aggregate5))
            {
                duplicate[12] = true; target[12] = target[4];
            }
            else if (aggregate13.HasSameStateAs(aggregate6))
            {
                duplicate[12] = true; target[12] = target[5];
            }
            else if (aggregate13.HasSameStateAs(aggregate7))
            {
                duplicate[12] = true; target[12] = target[6];
            }
            else if (aggregate13.HasSameStateAs(aggregate8))
            {
                duplicate[12] = true; target[12] = target[7];
            }
            else if (aggregate13.HasSameStateAs(aggregate9))
            {
                duplicate[12] = true; target[12] = target[8];
            }
            else if (aggregate13.HasSameStateAs(aggregate10))
            {
                duplicate[12] = true; target[12] = target[9];
            }
            else if (aggregate13.HasSameStateAs(aggregate11))
            {
                duplicate[12] = true; target[12] = target[10];
            }
            else if (aggregate13.HasSameStateAs(aggregate12))
            {
                duplicate[12] = true; target[12] = target[11];
            }

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>>> newInitialState =
                () => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.InitialState() : () => default),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.InitialState() : () => default),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.InitialState() : () => default),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.InitialState() : () => default),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.InitialState() : () => default),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.InitialState() : () => default),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.InitialState() : () => default),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.InitialState() : () => default),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.InitialState() : () => default),
                    Item10 = CallInliner.Call(!duplicate[9] ? aggregate10.InitialState() : () => default),
                    Item11 = CallInliner.Call(!duplicate[10] ? aggregate11.InitialState() : () => default),
                    Item12 = CallInliner.Call(!duplicate[11] ? aggregate12.InitialState() : () => default),
                    Item13 = CallInliner.Call(!duplicate[12] ? aggregate13.InitialState() : () => default)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, long, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>>> newAccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Accumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Accumulate() : (s, a, b) => s, oldState.Item2, timestamp, input),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Accumulate() : (s, a, b) => s, oldState.Item3, timestamp, input),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Accumulate() : (s, a, b) => s, oldState.Item4, timestamp, input),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Accumulate() : (s, a, b) => s, oldState.Item5, timestamp, input),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Accumulate() : (s, a, b) => s, oldState.Item6, timestamp, input),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.Accumulate() : (s, a, b) => s, oldState.Item7, timestamp, input),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.Accumulate() : (s, a, b) => s, oldState.Item8, timestamp, input),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.Accumulate() : (s, a, b) => s, oldState.Item9, timestamp, input),
                    Item10 = CallInliner.Call(!duplicate[9] ? aggregate10.Accumulate() : (s, a, b) => s, oldState.Item10, timestamp, input),
                    Item11 = CallInliner.Call(!duplicate[10] ? aggregate11.Accumulate() : (s, a, b) => s, oldState.Item11, timestamp, input),
                    Item12 = CallInliner.Call(!duplicate[11] ? aggregate12.Accumulate() : (s, a, b) => s, oldState.Item12, timestamp, input),
                    Item13 = CallInliner.Call(!duplicate[12] ? aggregate13.Accumulate() : (s, a, b) => s, oldState.Item13, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, long, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>>> newDeaccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Deaccumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Deaccumulate() : (s, a, b) => s, oldState.Item2, timestamp, input),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Deaccumulate() : (s, a, b) => s, oldState.Item3, timestamp, input),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Deaccumulate() : (s, a, b) => s, oldState.Item4, timestamp, input),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Deaccumulate() : (s, a, b) => s, oldState.Item5, timestamp, input),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Deaccumulate() : (s, a, b) => s, oldState.Item6, timestamp, input),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.Deaccumulate() : (s, a, b) => s, oldState.Item7, timestamp, input),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.Deaccumulate() : (s, a, b) => s, oldState.Item8, timestamp, input),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.Deaccumulate() : (s, a, b) => s, oldState.Item9, timestamp, input),
                    Item10 = CallInliner.Call(!duplicate[9] ? aggregate10.Deaccumulate() : (s, a, b) => s, oldState.Item10, timestamp, input),
                    Item11 = CallInliner.Call(!duplicate[10] ? aggregate11.Deaccumulate() : (s, a, b) => s, oldState.Item11, timestamp, input),
                    Item12 = CallInliner.Call(!duplicate[11] ? aggregate12.Deaccumulate() : (s, a, b) => s, oldState.Item12, timestamp, input),
                    Item13 = CallInliner.Call(!duplicate[12] ? aggregate13.Deaccumulate() : (s, a, b) => s, oldState.Item13, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>>> newDifference =
                (leftState, rightState) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Difference() : (l, r) => l, leftState.Item1, rightState.Item1),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Difference() : (l, r) => l, leftState.Item2, rightState.Item2),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Difference() : (l, r) => l, leftState.Item3, rightState.Item3),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Difference() : (l, r) => l, leftState.Item4, rightState.Item4),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Difference() : (l, r) => l, leftState.Item5, rightState.Item5),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Difference() : (l, r) => l, leftState.Item6, rightState.Item6),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.Difference() : (l, r) => l, leftState.Item7, rightState.Item7),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.Difference() : (l, r) => l, leftState.Item8, rightState.Item8),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.Difference() : (l, r) => l, leftState.Item9, rightState.Item9),
                    Item10 = CallInliner.Call(!duplicate[9] ? aggregate10.Difference() : (l, r) => l, leftState.Item10, rightState.Item10),
                    Item11 = CallInliner.Call(!duplicate[10] ? aggregate11.Difference() : (l, r) => l, leftState.Item11, rightState.Item11),
                    Item12 = CallInliner.Call(!duplicate[11] ? aggregate12.Difference() : (l, r) => l, leftState.Item12, rightState.Item12),
                    Item13 = CallInliner.Call(!duplicate[12] ? aggregate13.Difference() : (l, r) => l, leftState.Item13, rightState.Item13)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TResult>> newComputeResult =
                state =>
                CallInliner.Call(
                    merger,
                    CallInliner.Call(aggregate1.ComputeResult(), CallInliner.Call(target[0] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TState1>>, state)),
                    CallInliner.Call(aggregate2.ComputeResult(), CallInliner.Call(target[1] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TState2>>, state)),
                    CallInliner.Call(aggregate3.ComputeResult(), CallInliner.Call(target[2] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TState3>>, state)),
                    CallInliner.Call(aggregate4.ComputeResult(), CallInliner.Call(target[3] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TState4>>, state)),
                    CallInliner.Call(aggregate5.ComputeResult(), CallInliner.Call(target[4] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TState5>>, state)),
                    CallInliner.Call(aggregate6.ComputeResult(), CallInliner.Call(target[5] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TState6>>, state)),
                    CallInliner.Call(aggregate7.ComputeResult(), CallInliner.Call(target[6] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TState7>>, state)),
                    CallInliner.Call(aggregate8.ComputeResult(), CallInliner.Call(target[7] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TState8>>, state)),
                    CallInliner.Call(aggregate9.ComputeResult(), CallInliner.Call(target[8] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TState9>>, state)),
                    CallInliner.Call(aggregate10.ComputeResult(), CallInliner.Call(target[9] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TState10>>, state)),
                    CallInliner.Call(aggregate11.ComputeResult(), CallInliner.Call(target[10] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TState11>>, state)),
                    CallInliner.Call(aggregate12.ComputeResult(), CallInliner.Call(target[11] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TState12>>, state)),
                    CallInliner.Call(aggregate13.ComputeResult(), CallInliner.Call(target[12] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TState13>>, state)));

            return GeneratedAggregate.Create(
                initialState: newInitialState.InlineCalls(),
                accumulate: newAccumulate.InlineCalls(),
                deaccumulate: newDeaccumulate.InlineCalls(),
                difference: newDifference.InlineCalls(),
                computeResult: newComputeResult.InlineCalls());
        }

        /// <summary>
        /// Performs multiple aggregations simultaneously
        /// </summary>
        /// <typeparam name="TInput">The input stream to aggregate</typeparam>
        /// <typeparam name="TState1">Aggregation state type for aggregate number 1</typeparam>,
        /// <typeparam name="TState2">Aggregation state type for aggregate number 2</typeparam>,
        /// <typeparam name="TState3">Aggregation state type for aggregate number 3</typeparam>,
        /// <typeparam name="TState4">Aggregation state type for aggregate number 4</typeparam>,
        /// <typeparam name="TState5">Aggregation state type for aggregate number 5</typeparam>,
        /// <typeparam name="TState6">Aggregation state type for aggregate number 6</typeparam>,
        /// <typeparam name="TState7">Aggregation state type for aggregate number 7</typeparam>,
        /// <typeparam name="TState8">Aggregation state type for aggregate number 8</typeparam>,
        /// <typeparam name="TState9">Aggregation state type for aggregate number 9</typeparam>,
        /// <typeparam name="TState10">Aggregation state type for aggregate number 10</typeparam>,
        /// <typeparam name="TState11">Aggregation state type for aggregate number 11</typeparam>,
        /// <typeparam name="TState12">Aggregation state type for aggregate number 12</typeparam>,
        /// <typeparam name="TState13">Aggregation state type for aggregate number 13</typeparam>,
        /// <typeparam name="TState14">Aggregation state type for aggregate number 14</typeparam>
        /// <typeparam name="TResult1">Result type for aggregate number 1</typeparam>,
        /// <typeparam name="TResult2">Result type for aggregate number 2</typeparam>,
        /// <typeparam name="TResult3">Result type for aggregate number 3</typeparam>,
        /// <typeparam name="TResult4">Result type for aggregate number 4</typeparam>,
        /// <typeparam name="TResult5">Result type for aggregate number 5</typeparam>,
        /// <typeparam name="TResult6">Result type for aggregate number 6</typeparam>,
        /// <typeparam name="TResult7">Result type for aggregate number 7</typeparam>,
        /// <typeparam name="TResult8">Result type for aggregate number 8</typeparam>,
        /// <typeparam name="TResult9">Result type for aggregate number 9</typeparam>,
        /// <typeparam name="TResult10">Result type for aggregate number 10</typeparam>,
        /// <typeparam name="TResult11">Result type for aggregate number 11</typeparam>,
        /// <typeparam name="TResult12">Result type for aggregate number 12</typeparam>,
        /// <typeparam name="TResult13">Result type for aggregate number 13</typeparam>,
        /// <typeparam name="TResult14">Result type for aggregate number 14</typeparam>
        /// <typeparam name="TResult">Result type of the merged aggregation</typeparam>
        /// <param name="aggregate1">Aggregation specification number 1</param>,
        /// <param name="aggregate2">Aggregation specification number 2</param>,
        /// <param name="aggregate3">Aggregation specification number 3</param>,
        /// <param name="aggregate4">Aggregation specification number 4</param>,
        /// <param name="aggregate5">Aggregation specification number 5</param>,
        /// <param name="aggregate6">Aggregation specification number 6</param>,
        /// <param name="aggregate7">Aggregation specification number 7</param>,
        /// <param name="aggregate8">Aggregation specification number 8</param>,
        /// <param name="aggregate9">Aggregation specification number 9</param>,
        /// <param name="aggregate10">Aggregation specification number 10</param>,
        /// <param name="aggregate11">Aggregation specification number 11</param>,
        /// <param name="aggregate12">Aggregation specification number 12</param>,
        /// <param name="aggregate13">Aggregation specification number 13</param>,
        /// <param name="aggregate14">Aggregation specification number 14</param>
        /// <param name="merger">Function to take the result of all aggregations and merge them into a single result</param>
        /// <returns></returns>
        public static IAggregate<TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TResult> Combine<TInput, TState1, TResult1, TState2, TResult2, TState3, TResult3, TState4, TResult4, TState5, TResult5, TState6, TResult6, TState7, TResult7, TState8, TResult8, TState9, TResult9, TState10, TResult10, TState11, TResult11, TState12, TResult12, TState13, TResult13, TState14, TResult14, TResult>(
            IAggregate<TInput, TState1, TResult1> aggregate1,
            IAggregate<TInput, TState2, TResult2> aggregate2,
            IAggregate<TInput, TState3, TResult3> aggregate3,
            IAggregate<TInput, TState4, TResult4> aggregate4,
            IAggregate<TInput, TState5, TResult5> aggregate5,
            IAggregate<TInput, TState6, TResult6> aggregate6,
            IAggregate<TInput, TState7, TResult7> aggregate7,
            IAggregate<TInput, TState8, TResult8> aggregate8,
            IAggregate<TInput, TState9, TResult9> aggregate9,
            IAggregate<TInput, TState10, TResult10> aggregate10,
            IAggregate<TInput, TState11, TResult11> aggregate11,
            IAggregate<TInput, TState12, TResult12> aggregate12,
            IAggregate<TInput, TState13, TResult13> aggregate13,
            IAggregate<TInput, TState14, TResult14> aggregate14,
            Expression<Func<TResult1, TResult2, TResult3, TResult4, TResult5, TResult6, TResult7, TResult8, TResult9, TResult10, TResult11, TResult12, TResult13, TResult14, TResult>> merger)
        {
            Contract.Requires(aggregate1 != null);
            Contract.Requires(aggregate2 != null);
            Contract.Requires(aggregate3 != null);
            Contract.Requires(aggregate4 != null);
            Contract.Requires(aggregate5 != null);
            Contract.Requires(aggregate6 != null);
            Contract.Requires(aggregate7 != null);
            Contract.Requires(aggregate8 != null);
            Contract.Requires(aggregate9 != null);
            Contract.Requires(aggregate10 != null);
            Contract.Requires(aggregate11 != null);
            Contract.Requires(aggregate12 != null);
            Contract.Requires(aggregate13 != null);
            Contract.Requires(aggregate14 != null);
            Contract.Requires(merger != null);

            var duplicate = new bool[14];
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState1>> target1 = state => state.Item1;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState2>> target2 = state => state.Item2;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState3>> target3 = state => state.Item3;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState4>> target4 = state => state.Item4;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState5>> target5 = state => state.Item5;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState6>> target6 = state => state.Item6;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState7>> target7 = state => state.Item7;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState8>> target8 = state => state.Item8;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState9>> target9 = state => state.Item9;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState10>> target10 = state => state.Item10;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState11>> target11 = state => state.Item11;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState12>> target12 = state => state.Item12;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState13>> target13 = state => state.Item13;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState14>> target14 = state => state.Item14;
            var target = new System.Collections.Generic.List<LambdaExpression> { target1, target2, target3, target4, target5, target6, target7, target8, target9, target10, target11, target12, target13, target14 };
            if (aggregate2.HasSameStateAs(aggregate1))
            {
                duplicate[1] = true; target[1] = target[0];
            }
            if (aggregate3.HasSameStateAs(aggregate1))
            {
                duplicate[2] = true; target[2] = target[0];
            }
            else if (aggregate3.HasSameStateAs(aggregate2))
            {
                duplicate[2] = true; target[2] = target[1];
            }
            if (aggregate4.HasSameStateAs(aggregate1))
            {
                duplicate[3] = true; target[3] = target[0];
            }
            else if (aggregate4.HasSameStateAs(aggregate2))
            {
                duplicate[3] = true; target[3] = target[1];
            }
            else if (aggregate4.HasSameStateAs(aggregate3))
            {
                duplicate[3] = true; target[3] = target[2];
            }
            if (aggregate5.HasSameStateAs(aggregate1))
            {
                duplicate[4] = true; target[4] = target[0];
            }
            else if (aggregate5.HasSameStateAs(aggregate2))
            {
                duplicate[4] = true; target[4] = target[1];
            }
            else if (aggregate5.HasSameStateAs(aggregate3))
            {
                duplicate[4] = true; target[4] = target[2];
            }
            else if (aggregate5.HasSameStateAs(aggregate4))
            {
                duplicate[4] = true; target[4] = target[3];
            }
            if (aggregate6.HasSameStateAs(aggregate1))
            {
                duplicate[5] = true; target[5] = target[0];
            }
            else if (aggregate6.HasSameStateAs(aggregate2))
            {
                duplicate[5] = true; target[5] = target[1];
            }
            else if (aggregate6.HasSameStateAs(aggregate3))
            {
                duplicate[5] = true; target[5] = target[2];
            }
            else if (aggregate6.HasSameStateAs(aggregate4))
            {
                duplicate[5] = true; target[5] = target[3];
            }
            else if (aggregate6.HasSameStateAs(aggregate5))
            {
                duplicate[5] = true; target[5] = target[4];
            }
            if (aggregate7.HasSameStateAs(aggregate1))
            {
                duplicate[6] = true; target[6] = target[0];
            }
            else if (aggregate7.HasSameStateAs(aggregate2))
            {
                duplicate[6] = true; target[6] = target[1];
            }
            else if (aggregate7.HasSameStateAs(aggregate3))
            {
                duplicate[6] = true; target[6] = target[2];
            }
            else if (aggregate7.HasSameStateAs(aggregate4))
            {
                duplicate[6] = true; target[6] = target[3];
            }
            else if (aggregate7.HasSameStateAs(aggregate5))
            {
                duplicate[6] = true; target[6] = target[4];
            }
            else if (aggregate7.HasSameStateAs(aggregate6))
            {
                duplicate[6] = true; target[6] = target[5];
            }
            if (aggregate8.HasSameStateAs(aggregate1))
            {
                duplicate[7] = true; target[7] = target[0];
            }
            else if (aggregate8.HasSameStateAs(aggregate2))
            {
                duplicate[7] = true; target[7] = target[1];
            }
            else if (aggregate8.HasSameStateAs(aggregate3))
            {
                duplicate[7] = true; target[7] = target[2];
            }
            else if (aggregate8.HasSameStateAs(aggregate4))
            {
                duplicate[7] = true; target[7] = target[3];
            }
            else if (aggregate8.HasSameStateAs(aggregate5))
            {
                duplicate[7] = true; target[7] = target[4];
            }
            else if (aggregate8.HasSameStateAs(aggregate6))
            {
                duplicate[7] = true; target[7] = target[5];
            }
            else if (aggregate8.HasSameStateAs(aggregate7))
            {
                duplicate[7] = true; target[7] = target[6];
            }
            if (aggregate9.HasSameStateAs(aggregate1))
            {
                duplicate[8] = true; target[8] = target[0];
            }
            else if (aggregate9.HasSameStateAs(aggregate2))
            {
                duplicate[8] = true; target[8] = target[1];
            }
            else if (aggregate9.HasSameStateAs(aggregate3))
            {
                duplicate[8] = true; target[8] = target[2];
            }
            else if (aggregate9.HasSameStateAs(aggregate4))
            {
                duplicate[8] = true; target[8] = target[3];
            }
            else if (aggregate9.HasSameStateAs(aggregate5))
            {
                duplicate[8] = true; target[8] = target[4];
            }
            else if (aggregate9.HasSameStateAs(aggregate6))
            {
                duplicate[8] = true; target[8] = target[5];
            }
            else if (aggregate9.HasSameStateAs(aggregate7))
            {
                duplicate[8] = true; target[8] = target[6];
            }
            else if (aggregate9.HasSameStateAs(aggregate8))
            {
                duplicate[8] = true; target[8] = target[7];
            }
            if (aggregate10.HasSameStateAs(aggregate1))
            {
                duplicate[9] = true; target[9] = target[0];
            }
            else if (aggregate10.HasSameStateAs(aggregate2))
            {
                duplicate[9] = true; target[9] = target[1];
            }
            else if (aggregate10.HasSameStateAs(aggregate3))
            {
                duplicate[9] = true; target[9] = target[2];
            }
            else if (aggregate10.HasSameStateAs(aggregate4))
            {
                duplicate[9] = true; target[9] = target[3];
            }
            else if (aggregate10.HasSameStateAs(aggregate5))
            {
                duplicate[9] = true; target[9] = target[4];
            }
            else if (aggregate10.HasSameStateAs(aggregate6))
            {
                duplicate[9] = true; target[9] = target[5];
            }
            else if (aggregate10.HasSameStateAs(aggregate7))
            {
                duplicate[9] = true; target[9] = target[6];
            }
            else if (aggregate10.HasSameStateAs(aggregate8))
            {
                duplicate[9] = true; target[9] = target[7];
            }
            else if (aggregate10.HasSameStateAs(aggregate9))
            {
                duplicate[9] = true; target[9] = target[8];
            }
            if (aggregate11.HasSameStateAs(aggregate1))
            {
                duplicate[10] = true; target[10] = target[0];
            }
            else if (aggregate11.HasSameStateAs(aggregate2))
            {
                duplicate[10] = true; target[10] = target[1];
            }
            else if (aggregate11.HasSameStateAs(aggregate3))
            {
                duplicate[10] = true; target[10] = target[2];
            }
            else if (aggregate11.HasSameStateAs(aggregate4))
            {
                duplicate[10] = true; target[10] = target[3];
            }
            else if (aggregate11.HasSameStateAs(aggregate5))
            {
                duplicate[10] = true; target[10] = target[4];
            }
            else if (aggregate11.HasSameStateAs(aggregate6))
            {
                duplicate[10] = true; target[10] = target[5];
            }
            else if (aggregate11.HasSameStateAs(aggregate7))
            {
                duplicate[10] = true; target[10] = target[6];
            }
            else if (aggregate11.HasSameStateAs(aggregate8))
            {
                duplicate[10] = true; target[10] = target[7];
            }
            else if (aggregate11.HasSameStateAs(aggregate9))
            {
                duplicate[10] = true; target[10] = target[8];
            }
            else if (aggregate11.HasSameStateAs(aggregate10))
            {
                duplicate[10] = true; target[10] = target[9];
            }
            if (aggregate12.HasSameStateAs(aggregate1))
            {
                duplicate[11] = true; target[11] = target[0];
            }
            else if (aggregate12.HasSameStateAs(aggregate2))
            {
                duplicate[11] = true; target[11] = target[1];
            }
            else if (aggregate12.HasSameStateAs(aggregate3))
            {
                duplicate[11] = true; target[11] = target[2];
            }
            else if (aggregate12.HasSameStateAs(aggregate4))
            {
                duplicate[11] = true; target[11] = target[3];
            }
            else if (aggregate12.HasSameStateAs(aggregate5))
            {
                duplicate[11] = true; target[11] = target[4];
            }
            else if (aggregate12.HasSameStateAs(aggregate6))
            {
                duplicate[11] = true; target[11] = target[5];
            }
            else if (aggregate12.HasSameStateAs(aggregate7))
            {
                duplicate[11] = true; target[11] = target[6];
            }
            else if (aggregate12.HasSameStateAs(aggregate8))
            {
                duplicate[11] = true; target[11] = target[7];
            }
            else if (aggregate12.HasSameStateAs(aggregate9))
            {
                duplicate[11] = true; target[11] = target[8];
            }
            else if (aggregate12.HasSameStateAs(aggregate10))
            {
                duplicate[11] = true; target[11] = target[9];
            }
            else if (aggregate12.HasSameStateAs(aggregate11))
            {
                duplicate[11] = true; target[11] = target[10];
            }
            if (aggregate13.HasSameStateAs(aggregate1))
            {
                duplicate[12] = true; target[12] = target[0];
            }
            else if (aggregate13.HasSameStateAs(aggregate2))
            {
                duplicate[12] = true; target[12] = target[1];
            }
            else if (aggregate13.HasSameStateAs(aggregate3))
            {
                duplicate[12] = true; target[12] = target[2];
            }
            else if (aggregate13.HasSameStateAs(aggregate4))
            {
                duplicate[12] = true; target[12] = target[3];
            }
            else if (aggregate13.HasSameStateAs(aggregate5))
            {
                duplicate[12] = true; target[12] = target[4];
            }
            else if (aggregate13.HasSameStateAs(aggregate6))
            {
                duplicate[12] = true; target[12] = target[5];
            }
            else if (aggregate13.HasSameStateAs(aggregate7))
            {
                duplicate[12] = true; target[12] = target[6];
            }
            else if (aggregate13.HasSameStateAs(aggregate8))
            {
                duplicate[12] = true; target[12] = target[7];
            }
            else if (aggregate13.HasSameStateAs(aggregate9))
            {
                duplicate[12] = true; target[12] = target[8];
            }
            else if (aggregate13.HasSameStateAs(aggregate10))
            {
                duplicate[12] = true; target[12] = target[9];
            }
            else if (aggregate13.HasSameStateAs(aggregate11))
            {
                duplicate[12] = true; target[12] = target[10];
            }
            else if (aggregate13.HasSameStateAs(aggregate12))
            {
                duplicate[12] = true; target[12] = target[11];
            }
            if (aggregate14.HasSameStateAs(aggregate1))
            {
                duplicate[13] = true; target[13] = target[0];
            }
            else if (aggregate14.HasSameStateAs(aggregate2))
            {
                duplicate[13] = true; target[13] = target[1];
            }
            else if (aggregate14.HasSameStateAs(aggregate3))
            {
                duplicate[13] = true; target[13] = target[2];
            }
            else if (aggregate14.HasSameStateAs(aggregate4))
            {
                duplicate[13] = true; target[13] = target[3];
            }
            else if (aggregate14.HasSameStateAs(aggregate5))
            {
                duplicate[13] = true; target[13] = target[4];
            }
            else if (aggregate14.HasSameStateAs(aggregate6))
            {
                duplicate[13] = true; target[13] = target[5];
            }
            else if (aggregate14.HasSameStateAs(aggregate7))
            {
                duplicate[13] = true; target[13] = target[6];
            }
            else if (aggregate14.HasSameStateAs(aggregate8))
            {
                duplicate[13] = true; target[13] = target[7];
            }
            else if (aggregate14.HasSameStateAs(aggregate9))
            {
                duplicate[13] = true; target[13] = target[8];
            }
            else if (aggregate14.HasSameStateAs(aggregate10))
            {
                duplicate[13] = true; target[13] = target[9];
            }
            else if (aggregate14.HasSameStateAs(aggregate11))
            {
                duplicate[13] = true; target[13] = target[10];
            }
            else if (aggregate14.HasSameStateAs(aggregate12))
            {
                duplicate[13] = true; target[13] = target[11];
            }
            else if (aggregate14.HasSameStateAs(aggregate13))
            {
                duplicate[13] = true; target[13] = target[12];
            }

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>>> newInitialState =
                () => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.InitialState() : () => default),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.InitialState() : () => default),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.InitialState() : () => default),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.InitialState() : () => default),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.InitialState() : () => default),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.InitialState() : () => default),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.InitialState() : () => default),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.InitialState() : () => default),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.InitialState() : () => default),
                    Item10 = CallInliner.Call(!duplicate[9] ? aggregate10.InitialState() : () => default),
                    Item11 = CallInliner.Call(!duplicate[10] ? aggregate11.InitialState() : () => default),
                    Item12 = CallInliner.Call(!duplicate[11] ? aggregate12.InitialState() : () => default),
                    Item13 = CallInliner.Call(!duplicate[12] ? aggregate13.InitialState() : () => default),
                    Item14 = CallInliner.Call(!duplicate[13] ? aggregate14.InitialState() : () => default)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, long, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>>> newAccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Accumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Accumulate() : (s, a, b) => s, oldState.Item2, timestamp, input),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Accumulate() : (s, a, b) => s, oldState.Item3, timestamp, input),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Accumulate() : (s, a, b) => s, oldState.Item4, timestamp, input),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Accumulate() : (s, a, b) => s, oldState.Item5, timestamp, input),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Accumulate() : (s, a, b) => s, oldState.Item6, timestamp, input),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.Accumulate() : (s, a, b) => s, oldState.Item7, timestamp, input),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.Accumulate() : (s, a, b) => s, oldState.Item8, timestamp, input),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.Accumulate() : (s, a, b) => s, oldState.Item9, timestamp, input),
                    Item10 = CallInliner.Call(!duplicate[9] ? aggregate10.Accumulate() : (s, a, b) => s, oldState.Item10, timestamp, input),
                    Item11 = CallInliner.Call(!duplicate[10] ? aggregate11.Accumulate() : (s, a, b) => s, oldState.Item11, timestamp, input),
                    Item12 = CallInliner.Call(!duplicate[11] ? aggregate12.Accumulate() : (s, a, b) => s, oldState.Item12, timestamp, input),
                    Item13 = CallInliner.Call(!duplicate[12] ? aggregate13.Accumulate() : (s, a, b) => s, oldState.Item13, timestamp, input),
                    Item14 = CallInliner.Call(!duplicate[13] ? aggregate14.Accumulate() : (s, a, b) => s, oldState.Item14, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, long, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>>> newDeaccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Deaccumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Deaccumulate() : (s, a, b) => s, oldState.Item2, timestamp, input),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Deaccumulate() : (s, a, b) => s, oldState.Item3, timestamp, input),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Deaccumulate() : (s, a, b) => s, oldState.Item4, timestamp, input),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Deaccumulate() : (s, a, b) => s, oldState.Item5, timestamp, input),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Deaccumulate() : (s, a, b) => s, oldState.Item6, timestamp, input),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.Deaccumulate() : (s, a, b) => s, oldState.Item7, timestamp, input),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.Deaccumulate() : (s, a, b) => s, oldState.Item8, timestamp, input),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.Deaccumulate() : (s, a, b) => s, oldState.Item9, timestamp, input),
                    Item10 = CallInliner.Call(!duplicate[9] ? aggregate10.Deaccumulate() : (s, a, b) => s, oldState.Item10, timestamp, input),
                    Item11 = CallInliner.Call(!duplicate[10] ? aggregate11.Deaccumulate() : (s, a, b) => s, oldState.Item11, timestamp, input),
                    Item12 = CallInliner.Call(!duplicate[11] ? aggregate12.Deaccumulate() : (s, a, b) => s, oldState.Item12, timestamp, input),
                    Item13 = CallInliner.Call(!duplicate[12] ? aggregate13.Deaccumulate() : (s, a, b) => s, oldState.Item13, timestamp, input),
                    Item14 = CallInliner.Call(!duplicate[13] ? aggregate14.Deaccumulate() : (s, a, b) => s, oldState.Item14, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>>> newDifference =
                (leftState, rightState) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Difference() : (l, r) => l, leftState.Item1, rightState.Item1),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Difference() : (l, r) => l, leftState.Item2, rightState.Item2),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Difference() : (l, r) => l, leftState.Item3, rightState.Item3),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Difference() : (l, r) => l, leftState.Item4, rightState.Item4),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Difference() : (l, r) => l, leftState.Item5, rightState.Item5),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Difference() : (l, r) => l, leftState.Item6, rightState.Item6),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.Difference() : (l, r) => l, leftState.Item7, rightState.Item7),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.Difference() : (l, r) => l, leftState.Item8, rightState.Item8),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.Difference() : (l, r) => l, leftState.Item9, rightState.Item9),
                    Item10 = CallInliner.Call(!duplicate[9] ? aggregate10.Difference() : (l, r) => l, leftState.Item10, rightState.Item10),
                    Item11 = CallInliner.Call(!duplicate[10] ? aggregate11.Difference() : (l, r) => l, leftState.Item11, rightState.Item11),
                    Item12 = CallInliner.Call(!duplicate[11] ? aggregate12.Difference() : (l, r) => l, leftState.Item12, rightState.Item12),
                    Item13 = CallInliner.Call(!duplicate[12] ? aggregate13.Difference() : (l, r) => l, leftState.Item13, rightState.Item13),
                    Item14 = CallInliner.Call(!duplicate[13] ? aggregate14.Difference() : (l, r) => l, leftState.Item14, rightState.Item14)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TResult>> newComputeResult =
                state =>
                CallInliner.Call(
                    merger,
                    CallInliner.Call(aggregate1.ComputeResult(), CallInliner.Call(target[0] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState1>>, state)),
                    CallInliner.Call(aggregate2.ComputeResult(), CallInliner.Call(target[1] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState2>>, state)),
                    CallInliner.Call(aggregate3.ComputeResult(), CallInliner.Call(target[2] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState3>>, state)),
                    CallInliner.Call(aggregate4.ComputeResult(), CallInliner.Call(target[3] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState4>>, state)),
                    CallInliner.Call(aggregate5.ComputeResult(), CallInliner.Call(target[4] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState5>>, state)),
                    CallInliner.Call(aggregate6.ComputeResult(), CallInliner.Call(target[5] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState6>>, state)),
                    CallInliner.Call(aggregate7.ComputeResult(), CallInliner.Call(target[6] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState7>>, state)),
                    CallInliner.Call(aggregate8.ComputeResult(), CallInliner.Call(target[7] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState8>>, state)),
                    CallInliner.Call(aggregate9.ComputeResult(), CallInliner.Call(target[8] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState9>>, state)),
                    CallInliner.Call(aggregate10.ComputeResult(), CallInliner.Call(target[9] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState10>>, state)),
                    CallInliner.Call(aggregate11.ComputeResult(), CallInliner.Call(target[10] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState11>>, state)),
                    CallInliner.Call(aggregate12.ComputeResult(), CallInliner.Call(target[11] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState12>>, state)),
                    CallInliner.Call(aggregate13.ComputeResult(), CallInliner.Call(target[12] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState13>>, state)),
                    CallInliner.Call(aggregate14.ComputeResult(), CallInliner.Call(target[13] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TState14>>, state)));

            return GeneratedAggregate.Create(
                initialState: newInitialState.InlineCalls(),
                accumulate: newAccumulate.InlineCalls(),
                deaccumulate: newDeaccumulate.InlineCalls(),
                difference: newDifference.InlineCalls(),
                computeResult: newComputeResult.InlineCalls());
        }

        /// <summary>
        /// Performs multiple aggregations simultaneously
        /// </summary>
        /// <typeparam name="TInput">The input stream to aggregate</typeparam>
        /// <typeparam name="TState1">Aggregation state type for aggregate number 1</typeparam>,
        /// <typeparam name="TState2">Aggregation state type for aggregate number 2</typeparam>,
        /// <typeparam name="TState3">Aggregation state type for aggregate number 3</typeparam>,
        /// <typeparam name="TState4">Aggregation state type for aggregate number 4</typeparam>,
        /// <typeparam name="TState5">Aggregation state type for aggregate number 5</typeparam>,
        /// <typeparam name="TState6">Aggregation state type for aggregate number 6</typeparam>,
        /// <typeparam name="TState7">Aggregation state type for aggregate number 7</typeparam>,
        /// <typeparam name="TState8">Aggregation state type for aggregate number 8</typeparam>,
        /// <typeparam name="TState9">Aggregation state type for aggregate number 9</typeparam>,
        /// <typeparam name="TState10">Aggregation state type for aggregate number 10</typeparam>,
        /// <typeparam name="TState11">Aggregation state type for aggregate number 11</typeparam>,
        /// <typeparam name="TState12">Aggregation state type for aggregate number 12</typeparam>,
        /// <typeparam name="TState13">Aggregation state type for aggregate number 13</typeparam>,
        /// <typeparam name="TState14">Aggregation state type for aggregate number 14</typeparam>,
        /// <typeparam name="TState15">Aggregation state type for aggregate number 15</typeparam>
        /// <typeparam name="TResult1">Result type for aggregate number 1</typeparam>,
        /// <typeparam name="TResult2">Result type for aggregate number 2</typeparam>,
        /// <typeparam name="TResult3">Result type for aggregate number 3</typeparam>,
        /// <typeparam name="TResult4">Result type for aggregate number 4</typeparam>,
        /// <typeparam name="TResult5">Result type for aggregate number 5</typeparam>,
        /// <typeparam name="TResult6">Result type for aggregate number 6</typeparam>,
        /// <typeparam name="TResult7">Result type for aggregate number 7</typeparam>,
        /// <typeparam name="TResult8">Result type for aggregate number 8</typeparam>,
        /// <typeparam name="TResult9">Result type for aggregate number 9</typeparam>,
        /// <typeparam name="TResult10">Result type for aggregate number 10</typeparam>,
        /// <typeparam name="TResult11">Result type for aggregate number 11</typeparam>,
        /// <typeparam name="TResult12">Result type for aggregate number 12</typeparam>,
        /// <typeparam name="TResult13">Result type for aggregate number 13</typeparam>,
        /// <typeparam name="TResult14">Result type for aggregate number 14</typeparam>,
        /// <typeparam name="TResult15">Result type for aggregate number 15</typeparam>
        /// <typeparam name="TResult">Result type of the merged aggregation</typeparam>
        /// <param name="aggregate1">Aggregation specification number 1</param>,
        /// <param name="aggregate2">Aggregation specification number 2</param>,
        /// <param name="aggregate3">Aggregation specification number 3</param>,
        /// <param name="aggregate4">Aggregation specification number 4</param>,
        /// <param name="aggregate5">Aggregation specification number 5</param>,
        /// <param name="aggregate6">Aggregation specification number 6</param>,
        /// <param name="aggregate7">Aggregation specification number 7</param>,
        /// <param name="aggregate8">Aggregation specification number 8</param>,
        /// <param name="aggregate9">Aggregation specification number 9</param>,
        /// <param name="aggregate10">Aggregation specification number 10</param>,
        /// <param name="aggregate11">Aggregation specification number 11</param>,
        /// <param name="aggregate12">Aggregation specification number 12</param>,
        /// <param name="aggregate13">Aggregation specification number 13</param>,
        /// <param name="aggregate14">Aggregation specification number 14</param>,
        /// <param name="aggregate15">Aggregation specification number 15</param>
        /// <param name="merger">Function to take the result of all aggregations and merge them into a single result</param>
        /// <returns></returns>
        public static IAggregate<TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TResult> Combine<TInput, TState1, TResult1, TState2, TResult2, TState3, TResult3, TState4, TResult4, TState5, TResult5, TState6, TResult6, TState7, TResult7, TState8, TResult8, TState9, TResult9, TState10, TResult10, TState11, TResult11, TState12, TResult12, TState13, TResult13, TState14, TResult14, TState15, TResult15, TResult>(
            IAggregate<TInput, TState1, TResult1> aggregate1,
            IAggregate<TInput, TState2, TResult2> aggregate2,
            IAggregate<TInput, TState3, TResult3> aggregate3,
            IAggregate<TInput, TState4, TResult4> aggregate4,
            IAggregate<TInput, TState5, TResult5> aggregate5,
            IAggregate<TInput, TState6, TResult6> aggregate6,
            IAggregate<TInput, TState7, TResult7> aggregate7,
            IAggregate<TInput, TState8, TResult8> aggregate8,
            IAggregate<TInput, TState9, TResult9> aggregate9,
            IAggregate<TInput, TState10, TResult10> aggregate10,
            IAggregate<TInput, TState11, TResult11> aggregate11,
            IAggregate<TInput, TState12, TResult12> aggregate12,
            IAggregate<TInput, TState13, TResult13> aggregate13,
            IAggregate<TInput, TState14, TResult14> aggregate14,
            IAggregate<TInput, TState15, TResult15> aggregate15,
            Expression<Func<TResult1, TResult2, TResult3, TResult4, TResult5, TResult6, TResult7, TResult8, TResult9, TResult10, TResult11, TResult12, TResult13, TResult14, TResult15, TResult>> merger)
        {
            Contract.Requires(aggregate1 != null);
            Contract.Requires(aggregate2 != null);
            Contract.Requires(aggregate3 != null);
            Contract.Requires(aggregate4 != null);
            Contract.Requires(aggregate5 != null);
            Contract.Requires(aggregate6 != null);
            Contract.Requires(aggregate7 != null);
            Contract.Requires(aggregate8 != null);
            Contract.Requires(aggregate9 != null);
            Contract.Requires(aggregate10 != null);
            Contract.Requires(aggregate11 != null);
            Contract.Requires(aggregate12 != null);
            Contract.Requires(aggregate13 != null);
            Contract.Requires(aggregate14 != null);
            Contract.Requires(aggregate15 != null);
            Contract.Requires(merger != null);

            var duplicate = new bool[15];
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState1>> target1 = state => state.Item1;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState2>> target2 = state => state.Item2;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState3>> target3 = state => state.Item3;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState4>> target4 = state => state.Item4;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState5>> target5 = state => state.Item5;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState6>> target6 = state => state.Item6;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState7>> target7 = state => state.Item7;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState8>> target8 = state => state.Item8;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState9>> target9 = state => state.Item9;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState10>> target10 = state => state.Item10;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState11>> target11 = state => state.Item11;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState12>> target12 = state => state.Item12;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState13>> target13 = state => state.Item13;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState14>> target14 = state => state.Item14;
            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState15>> target15 = state => state.Item15;
            var target = new System.Collections.Generic.List<LambdaExpression> { target1, target2, target3, target4, target5, target6, target7, target8, target9, target10, target11, target12, target13, target14, target15 };
            if (aggregate2.HasSameStateAs(aggregate1))
            {
                duplicate[1] = true; target[1] = target[0];
            }
            if (aggregate3.HasSameStateAs(aggregate1))
            {
                duplicate[2] = true; target[2] = target[0];
            }
            else if (aggregate3.HasSameStateAs(aggregate2))
            {
                duplicate[2] = true; target[2] = target[1];
            }
            if (aggregate4.HasSameStateAs(aggregate1))
            {
                duplicate[3] = true; target[3] = target[0];
            }
            else if (aggregate4.HasSameStateAs(aggregate2))
            {
                duplicate[3] = true; target[3] = target[1];
            }
            else if (aggregate4.HasSameStateAs(aggregate3))
            {
                duplicate[3] = true; target[3] = target[2];
            }
            if (aggregate5.HasSameStateAs(aggregate1))
            {
                duplicate[4] = true; target[4] = target[0];
            }
            else if (aggregate5.HasSameStateAs(aggregate2))
            {
                duplicate[4] = true; target[4] = target[1];
            }
            else if (aggregate5.HasSameStateAs(aggregate3))
            {
                duplicate[4] = true; target[4] = target[2];
            }
            else if (aggregate5.HasSameStateAs(aggregate4))
            {
                duplicate[4] = true; target[4] = target[3];
            }
            if (aggregate6.HasSameStateAs(aggregate1))
            {
                duplicate[5] = true; target[5] = target[0];
            }
            else if (aggregate6.HasSameStateAs(aggregate2))
            {
                duplicate[5] = true; target[5] = target[1];
            }
            else if (aggregate6.HasSameStateAs(aggregate3))
            {
                duplicate[5] = true; target[5] = target[2];
            }
            else if (aggregate6.HasSameStateAs(aggregate4))
            {
                duplicate[5] = true; target[5] = target[3];
            }
            else if (aggregate6.HasSameStateAs(aggregate5))
            {
                duplicate[5] = true; target[5] = target[4];
            }
            if (aggregate7.HasSameStateAs(aggregate1))
            {
                duplicate[6] = true; target[6] = target[0];
            }
            else if (aggregate7.HasSameStateAs(aggregate2))
            {
                duplicate[6] = true; target[6] = target[1];
            }
            else if (aggregate7.HasSameStateAs(aggregate3))
            {
                duplicate[6] = true; target[6] = target[2];
            }
            else if (aggregate7.HasSameStateAs(aggregate4))
            {
                duplicate[6] = true; target[6] = target[3];
            }
            else if (aggregate7.HasSameStateAs(aggregate5))
            {
                duplicate[6] = true; target[6] = target[4];
            }
            else if (aggregate7.HasSameStateAs(aggregate6))
            {
                duplicate[6] = true; target[6] = target[5];
            }
            if (aggregate8.HasSameStateAs(aggregate1))
            {
                duplicate[7] = true; target[7] = target[0];
            }
            else if (aggregate8.HasSameStateAs(aggregate2))
            {
                duplicate[7] = true; target[7] = target[1];
            }
            else if (aggregate8.HasSameStateAs(aggregate3))
            {
                duplicate[7] = true; target[7] = target[2];
            }
            else if (aggregate8.HasSameStateAs(aggregate4))
            {
                duplicate[7] = true; target[7] = target[3];
            }
            else if (aggregate8.HasSameStateAs(aggregate5))
            {
                duplicate[7] = true; target[7] = target[4];
            }
            else if (aggregate8.HasSameStateAs(aggregate6))
            {
                duplicate[7] = true; target[7] = target[5];
            }
            else if (aggregate8.HasSameStateAs(aggregate7))
            {
                duplicate[7] = true; target[7] = target[6];
            }
            if (aggregate9.HasSameStateAs(aggregate1))
            {
                duplicate[8] = true; target[8] = target[0];
            }
            else if (aggregate9.HasSameStateAs(aggregate2))
            {
                duplicate[8] = true; target[8] = target[1];
            }
            else if (aggregate9.HasSameStateAs(aggregate3))
            {
                duplicate[8] = true; target[8] = target[2];
            }
            else if (aggregate9.HasSameStateAs(aggregate4))
            {
                duplicate[8] = true; target[8] = target[3];
            }
            else if (aggregate9.HasSameStateAs(aggregate5))
            {
                duplicate[8] = true; target[8] = target[4];
            }
            else if (aggregate9.HasSameStateAs(aggregate6))
            {
                duplicate[8] = true; target[8] = target[5];
            }
            else if (aggregate9.HasSameStateAs(aggregate7))
            {
                duplicate[8] = true; target[8] = target[6];
            }
            else if (aggregate9.HasSameStateAs(aggregate8))
            {
                duplicate[8] = true; target[8] = target[7];
            }
            if (aggregate10.HasSameStateAs(aggregate1))
            {
                duplicate[9] = true; target[9] = target[0];
            }
            else if (aggregate10.HasSameStateAs(aggregate2))
            {
                duplicate[9] = true; target[9] = target[1];
            }
            else if (aggregate10.HasSameStateAs(aggregate3))
            {
                duplicate[9] = true; target[9] = target[2];
            }
            else if (aggregate10.HasSameStateAs(aggregate4))
            {
                duplicate[9] = true; target[9] = target[3];
            }
            else if (aggregate10.HasSameStateAs(aggregate5))
            {
                duplicate[9] = true; target[9] = target[4];
            }
            else if (aggregate10.HasSameStateAs(aggregate6))
            {
                duplicate[9] = true; target[9] = target[5];
            }
            else if (aggregate10.HasSameStateAs(aggregate7))
            {
                duplicate[9] = true; target[9] = target[6];
            }
            else if (aggregate10.HasSameStateAs(aggregate8))
            {
                duplicate[9] = true; target[9] = target[7];
            }
            else if (aggregate10.HasSameStateAs(aggregate9))
            {
                duplicate[9] = true; target[9] = target[8];
            }
            if (aggregate11.HasSameStateAs(aggregate1))
            {
                duplicate[10] = true; target[10] = target[0];
            }
            else if (aggregate11.HasSameStateAs(aggregate2))
            {
                duplicate[10] = true; target[10] = target[1];
            }
            else if (aggregate11.HasSameStateAs(aggregate3))
            {
                duplicate[10] = true; target[10] = target[2];
            }
            else if (aggregate11.HasSameStateAs(aggregate4))
            {
                duplicate[10] = true; target[10] = target[3];
            }
            else if (aggregate11.HasSameStateAs(aggregate5))
            {
                duplicate[10] = true; target[10] = target[4];
            }
            else if (aggregate11.HasSameStateAs(aggregate6))
            {
                duplicate[10] = true; target[10] = target[5];
            }
            else if (aggregate11.HasSameStateAs(aggregate7))
            {
                duplicate[10] = true; target[10] = target[6];
            }
            else if (aggregate11.HasSameStateAs(aggregate8))
            {
                duplicate[10] = true; target[10] = target[7];
            }
            else if (aggregate11.HasSameStateAs(aggregate9))
            {
                duplicate[10] = true; target[10] = target[8];
            }
            else if (aggregate11.HasSameStateAs(aggregate10))
            {
                duplicate[10] = true; target[10] = target[9];
            }
            if (aggregate12.HasSameStateAs(aggregate1))
            {
                duplicate[11] = true; target[11] = target[0];
            }
            else if (aggregate12.HasSameStateAs(aggregate2))
            {
                duplicate[11] = true; target[11] = target[1];
            }
            else if (aggregate12.HasSameStateAs(aggregate3))
            {
                duplicate[11] = true; target[11] = target[2];
            }
            else if (aggregate12.HasSameStateAs(aggregate4))
            {
                duplicate[11] = true; target[11] = target[3];
            }
            else if (aggregate12.HasSameStateAs(aggregate5))
            {
                duplicate[11] = true; target[11] = target[4];
            }
            else if (aggregate12.HasSameStateAs(aggregate6))
            {
                duplicate[11] = true; target[11] = target[5];
            }
            else if (aggregate12.HasSameStateAs(aggregate7))
            {
                duplicate[11] = true; target[11] = target[6];
            }
            else if (aggregate12.HasSameStateAs(aggregate8))
            {
                duplicate[11] = true; target[11] = target[7];
            }
            else if (aggregate12.HasSameStateAs(aggregate9))
            {
                duplicate[11] = true; target[11] = target[8];
            }
            else if (aggregate12.HasSameStateAs(aggregate10))
            {
                duplicate[11] = true; target[11] = target[9];
            }
            else if (aggregate12.HasSameStateAs(aggregate11))
            {
                duplicate[11] = true; target[11] = target[10];
            }
            if (aggregate13.HasSameStateAs(aggregate1))
            {
                duplicate[12] = true; target[12] = target[0];
            }
            else if (aggregate13.HasSameStateAs(aggregate2))
            {
                duplicate[12] = true; target[12] = target[1];
            }
            else if (aggregate13.HasSameStateAs(aggregate3))
            {
                duplicate[12] = true; target[12] = target[2];
            }
            else if (aggregate13.HasSameStateAs(aggregate4))
            {
                duplicate[12] = true; target[12] = target[3];
            }
            else if (aggregate13.HasSameStateAs(aggregate5))
            {
                duplicate[12] = true; target[12] = target[4];
            }
            else if (aggregate13.HasSameStateAs(aggregate6))
            {
                duplicate[12] = true; target[12] = target[5];
            }
            else if (aggregate13.HasSameStateAs(aggregate7))
            {
                duplicate[12] = true; target[12] = target[6];
            }
            else if (aggregate13.HasSameStateAs(aggregate8))
            {
                duplicate[12] = true; target[12] = target[7];
            }
            else if (aggregate13.HasSameStateAs(aggregate9))
            {
                duplicate[12] = true; target[12] = target[8];
            }
            else if (aggregate13.HasSameStateAs(aggregate10))
            {
                duplicate[12] = true; target[12] = target[9];
            }
            else if (aggregate13.HasSameStateAs(aggregate11))
            {
                duplicate[12] = true; target[12] = target[10];
            }
            else if (aggregate13.HasSameStateAs(aggregate12))
            {
                duplicate[12] = true; target[12] = target[11];
            }
            if (aggregate14.HasSameStateAs(aggregate1))
            {
                duplicate[13] = true; target[13] = target[0];
            }
            else if (aggregate14.HasSameStateAs(aggregate2))
            {
                duplicate[13] = true; target[13] = target[1];
            }
            else if (aggregate14.HasSameStateAs(aggregate3))
            {
                duplicate[13] = true; target[13] = target[2];
            }
            else if (aggregate14.HasSameStateAs(aggregate4))
            {
                duplicate[13] = true; target[13] = target[3];
            }
            else if (aggregate14.HasSameStateAs(aggregate5))
            {
                duplicate[13] = true; target[13] = target[4];
            }
            else if (aggregate14.HasSameStateAs(aggregate6))
            {
                duplicate[13] = true; target[13] = target[5];
            }
            else if (aggregate14.HasSameStateAs(aggregate7))
            {
                duplicate[13] = true; target[13] = target[6];
            }
            else if (aggregate14.HasSameStateAs(aggregate8))
            {
                duplicate[13] = true; target[13] = target[7];
            }
            else if (aggregate14.HasSameStateAs(aggregate9))
            {
                duplicate[13] = true; target[13] = target[8];
            }
            else if (aggregate14.HasSameStateAs(aggregate10))
            {
                duplicate[13] = true; target[13] = target[9];
            }
            else if (aggregate14.HasSameStateAs(aggregate11))
            {
                duplicate[13] = true; target[13] = target[10];
            }
            else if (aggregate14.HasSameStateAs(aggregate12))
            {
                duplicate[13] = true; target[13] = target[11];
            }
            else if (aggregate14.HasSameStateAs(aggregate13))
            {
                duplicate[13] = true; target[13] = target[12];
            }
            if (aggregate15.HasSameStateAs(aggregate1))
            {
                duplicate[14] = true; target[14] = target[0];
            }
            else if (aggregate15.HasSameStateAs(aggregate2))
            {
                duplicate[14] = true; target[14] = target[1];
            }
            else if (aggregate15.HasSameStateAs(aggregate3))
            {
                duplicate[14] = true; target[14] = target[2];
            }
            else if (aggregate15.HasSameStateAs(aggregate4))
            {
                duplicate[14] = true; target[14] = target[3];
            }
            else if (aggregate15.HasSameStateAs(aggregate5))
            {
                duplicate[14] = true; target[14] = target[4];
            }
            else if (aggregate15.HasSameStateAs(aggregate6))
            {
                duplicate[14] = true; target[14] = target[5];
            }
            else if (aggregate15.HasSameStateAs(aggregate7))
            {
                duplicate[14] = true; target[14] = target[6];
            }
            else if (aggregate15.HasSameStateAs(aggregate8))
            {
                duplicate[14] = true; target[14] = target[7];
            }
            else if (aggregate15.HasSameStateAs(aggregate9))
            {
                duplicate[14] = true; target[14] = target[8];
            }
            else if (aggregate15.HasSameStateAs(aggregate10))
            {
                duplicate[14] = true; target[14] = target[9];
            }
            else if (aggregate15.HasSameStateAs(aggregate11))
            {
                duplicate[14] = true; target[14] = target[10];
            }
            else if (aggregate15.HasSameStateAs(aggregate12))
            {
                duplicate[14] = true; target[14] = target[11];
            }
            else if (aggregate15.HasSameStateAs(aggregate13))
            {
                duplicate[14] = true; target[14] = target[12];
            }
            else if (aggregate15.HasSameStateAs(aggregate14))
            {
                duplicate[14] = true; target[14] = target[13];
            }

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>>> newInitialState =
                () => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.InitialState() : () => default),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.InitialState() : () => default),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.InitialState() : () => default),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.InitialState() : () => default),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.InitialState() : () => default),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.InitialState() : () => default),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.InitialState() : () => default),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.InitialState() : () => default),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.InitialState() : () => default),
                    Item10 = CallInliner.Call(!duplicate[9] ? aggregate10.InitialState() : () => default),
                    Item11 = CallInliner.Call(!duplicate[10] ? aggregate11.InitialState() : () => default),
                    Item12 = CallInliner.Call(!duplicate[11] ? aggregate12.InitialState() : () => default),
                    Item13 = CallInliner.Call(!duplicate[12] ? aggregate13.InitialState() : () => default),
                    Item14 = CallInliner.Call(!duplicate[13] ? aggregate14.InitialState() : () => default),
                    Item15 = CallInliner.Call(!duplicate[14] ? aggregate15.InitialState() : () => default)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, long, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>>> newAccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Accumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Accumulate() : (s, a, b) => s, oldState.Item2, timestamp, input),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Accumulate() : (s, a, b) => s, oldState.Item3, timestamp, input),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Accumulate() : (s, a, b) => s, oldState.Item4, timestamp, input),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Accumulate() : (s, a, b) => s, oldState.Item5, timestamp, input),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Accumulate() : (s, a, b) => s, oldState.Item6, timestamp, input),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.Accumulate() : (s, a, b) => s, oldState.Item7, timestamp, input),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.Accumulate() : (s, a, b) => s, oldState.Item8, timestamp, input),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.Accumulate() : (s, a, b) => s, oldState.Item9, timestamp, input),
                    Item10 = CallInliner.Call(!duplicate[9] ? aggregate10.Accumulate() : (s, a, b) => s, oldState.Item10, timestamp, input),
                    Item11 = CallInliner.Call(!duplicate[10] ? aggregate11.Accumulate() : (s, a, b) => s, oldState.Item11, timestamp, input),
                    Item12 = CallInliner.Call(!duplicate[11] ? aggregate12.Accumulate() : (s, a, b) => s, oldState.Item12, timestamp, input),
                    Item13 = CallInliner.Call(!duplicate[12] ? aggregate13.Accumulate() : (s, a, b) => s, oldState.Item13, timestamp, input),
                    Item14 = CallInliner.Call(!duplicate[13] ? aggregate14.Accumulate() : (s, a, b) => s, oldState.Item14, timestamp, input),
                    Item15 = CallInliner.Call(!duplicate[14] ? aggregate15.Accumulate() : (s, a, b) => s, oldState.Item15, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, long, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>>> newDeaccumulate =
                (oldState, timestamp, input) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Deaccumulate() : (s, a, b) => s, oldState.Item1, timestamp, input),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Deaccumulate() : (s, a, b) => s, oldState.Item2, timestamp, input),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Deaccumulate() : (s, a, b) => s, oldState.Item3, timestamp, input),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Deaccumulate() : (s, a, b) => s, oldState.Item4, timestamp, input),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Deaccumulate() : (s, a, b) => s, oldState.Item5, timestamp, input),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Deaccumulate() : (s, a, b) => s, oldState.Item6, timestamp, input),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.Deaccumulate() : (s, a, b) => s, oldState.Item7, timestamp, input),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.Deaccumulate() : (s, a, b) => s, oldState.Item8, timestamp, input),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.Deaccumulate() : (s, a, b) => s, oldState.Item9, timestamp, input),
                    Item10 = CallInliner.Call(!duplicate[9] ? aggregate10.Deaccumulate() : (s, a, b) => s, oldState.Item10, timestamp, input),
                    Item11 = CallInliner.Call(!duplicate[10] ? aggregate11.Deaccumulate() : (s, a, b) => s, oldState.Item11, timestamp, input),
                    Item12 = CallInliner.Call(!duplicate[11] ? aggregate12.Deaccumulate() : (s, a, b) => s, oldState.Item12, timestamp, input),
                    Item13 = CallInliner.Call(!duplicate[12] ? aggregate13.Deaccumulate() : (s, a, b) => s, oldState.Item13, timestamp, input),
                    Item14 = CallInliner.Call(!duplicate[13] ? aggregate14.Deaccumulate() : (s, a, b) => s, oldState.Item14, timestamp, input),
                    Item15 = CallInliner.Call(!duplicate[14] ? aggregate15.Deaccumulate() : (s, a, b) => s, oldState.Item15, timestamp, input)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>>> newDifference =
                (leftState, rightState) => new StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>
                {
                    Item1 = CallInliner.Call(!duplicate[0] ? aggregate1.Difference() : (l, r) => l, leftState.Item1, rightState.Item1),
                    Item2 = CallInliner.Call(!duplicate[1] ? aggregate2.Difference() : (l, r) => l, leftState.Item2, rightState.Item2),
                    Item3 = CallInliner.Call(!duplicate[2] ? aggregate3.Difference() : (l, r) => l, leftState.Item3, rightState.Item3),
                    Item4 = CallInliner.Call(!duplicate[3] ? aggregate4.Difference() : (l, r) => l, leftState.Item4, rightState.Item4),
                    Item5 = CallInliner.Call(!duplicate[4] ? aggregate5.Difference() : (l, r) => l, leftState.Item5, rightState.Item5),
                    Item6 = CallInliner.Call(!duplicate[5] ? aggregate6.Difference() : (l, r) => l, leftState.Item6, rightState.Item6),
                    Item7 = CallInliner.Call(!duplicate[6] ? aggregate7.Difference() : (l, r) => l, leftState.Item7, rightState.Item7),
                    Item8 = CallInliner.Call(!duplicate[7] ? aggregate8.Difference() : (l, r) => l, leftState.Item8, rightState.Item8),
                    Item9 = CallInliner.Call(!duplicate[8] ? aggregate9.Difference() : (l, r) => l, leftState.Item9, rightState.Item9),
                    Item10 = CallInliner.Call(!duplicate[9] ? aggregate10.Difference() : (l, r) => l, leftState.Item10, rightState.Item10),
                    Item11 = CallInliner.Call(!duplicate[10] ? aggregate11.Difference() : (l, r) => l, leftState.Item11, rightState.Item11),
                    Item12 = CallInliner.Call(!duplicate[11] ? aggregate12.Difference() : (l, r) => l, leftState.Item12, rightState.Item12),
                    Item13 = CallInliner.Call(!duplicate[12] ? aggregate13.Difference() : (l, r) => l, leftState.Item13, rightState.Item13),
                    Item14 = CallInliner.Call(!duplicate[13] ? aggregate14.Difference() : (l, r) => l, leftState.Item14, rightState.Item14),
                    Item15 = CallInliner.Call(!duplicate[14] ? aggregate15.Difference() : (l, r) => l, leftState.Item15, rightState.Item15)
                };

            Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TResult>> newComputeResult =
                state =>
                CallInliner.Call(
                    merger,
                    CallInliner.Call(aggregate1.ComputeResult(), CallInliner.Call(target[0] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState1>>, state)),
                    CallInliner.Call(aggregate2.ComputeResult(), CallInliner.Call(target[1] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState2>>, state)),
                    CallInliner.Call(aggregate3.ComputeResult(), CallInliner.Call(target[2] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState3>>, state)),
                    CallInliner.Call(aggregate4.ComputeResult(), CallInliner.Call(target[3] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState4>>, state)),
                    CallInliner.Call(aggregate5.ComputeResult(), CallInliner.Call(target[4] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState5>>, state)),
                    CallInliner.Call(aggregate6.ComputeResult(), CallInliner.Call(target[5] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState6>>, state)),
                    CallInliner.Call(aggregate7.ComputeResult(), CallInliner.Call(target[6] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState7>>, state)),
                    CallInliner.Call(aggregate8.ComputeResult(), CallInliner.Call(target[7] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState8>>, state)),
                    CallInliner.Call(aggregate9.ComputeResult(), CallInliner.Call(target[8] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState9>>, state)),
                    CallInliner.Call(aggregate10.ComputeResult(), CallInliner.Call(target[9] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState10>>, state)),
                    CallInliner.Call(aggregate11.ComputeResult(), CallInliner.Call(target[10] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState11>>, state)),
                    CallInliner.Call(aggregate12.ComputeResult(), CallInliner.Call(target[11] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState12>>, state)),
                    CallInliner.Call(aggregate13.ComputeResult(), CallInliner.Call(target[12] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState13>>, state)),
                    CallInliner.Call(aggregate14.ComputeResult(), CallInliner.Call(target[13] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState14>>, state)),
                    CallInliner.Call(aggregate15.ComputeResult(), CallInliner.Call(target[14] as Expression<Func<StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TState15>>, state)));

            return GeneratedAggregate.Create(
                initialState: newInitialState.InlineCalls(),
                accumulate: newAccumulate.InlineCalls(),
                deaccumulate: newDeaccumulate.InlineCalls(),
                difference: newDifference.InlineCalls(),
                computeResult: newComputeResult.InlineCalls());
        }
    }
}