// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.ComponentModel;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing.Aggregates
{
    /// <summary>
    /// Currently for internal use only
    /// </summary>
    /// <typeparam name="T"></typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct NullOutputWrapper<T>
    {
        /// <summary>
        /// Currently for internal use only
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public ulong Count;

        /// <summary>
        /// Currently for internal use only
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T State;
    }

    /// <summary>
    /// Currently for internal use only
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public static partial class AggregateFunctions
    {
        /// <summary>
        /// Transforms the input to the aggregate with the specified transform expression.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static IAggregate<TInput, TState, TResult> Wrap<TInput, TAggregateInput, TState, TResult>(
            this IAggregate<TAggregateInput, TState, TResult> aggregate,
            Expression<Func<TInput, TAggregateInput>> transform)
        {
            Contract.Requires(aggregate != null);
            Contract.Requires(transform != null);
            return aggregate.TransformInput(transform);
        }

        /// <summary>
        /// Transforms the input to the aggregate with the specified transform expression. Also makes the input
        /// to the aggregate nullable with the behavior of dropping null values.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static IAggregate<TInput, TState, TResult> Wrap<TInput, TAggregateInput, TState, TResult>(
            this IAggregate<TAggregateInput, TState, TResult> aggregate,
            Expression<Func<TInput, TAggregateInput?>> transform) where TAggregateInput : struct
        {
            Contract.Requires(aggregate != null);
            Contract.Requires(transform != null);
            return aggregate.MakeInputNullableAndSkipNulls().TransformInput(transform);
        }

        internal static IAggregate<TInput, TState, TResult> TransformInput<TInput, TAggregateInput, TState, TResult>(
            this IAggregate<TAggregateInput, TState, TResult> aggregate,
            Expression<Func<TInput, TAggregateInput>> transform)
        {
            Contract.Requires(aggregate != null);
            Contract.Requires(transform != null);

            return GeneratedAggregate.Create(
                initialState: aggregate.InitialState(),
                accumulate: aggregate.Accumulate().TransformInput3(transform),
                deaccumulate: aggregate.Deaccumulate().TransformInput3(transform),
                difference: aggregate.Difference(),
                computeResult: aggregate.ComputeResult());
        }

        private static Expression<Func<T1, T2, TInput, TOutput>> TransformInput3<T1, T2, T3, TInput, TOutput>(
            this Expression<Func<T1, T2, T3, TOutput>> func,
            Expression<Func<TInput, T3>> transform)
        {
            Contract.Requires(func != null);
            Contract.Requires(transform != null);
            var result = func.ReplaceParametersInBody(func.Parameters[0], func.Parameters[1], transform.Body);
            var transformParam = transform.Parameters[0];
            return Expression.Lambda<Func<T1, T2, TInput, TOutput>>(result, new[] { func.Parameters[0], func.Parameters[1], transformParam });
        }

        internal static IAggregate<TInput, TState, TResult> TransformOutput<TInput, TState, TAggregateResult, TResult>(
            this IAggregate<TInput, TState, TAggregateResult> aggregate,
            Expression<Func<TAggregateResult, TResult>> transform)
        {
            Contract.Requires(aggregate != null);
            Contract.Requires(transform != null);

            return GeneratedAggregate.Create(
                initialState: aggregate.InitialState(),
                accumulate: aggregate.Accumulate(),
                deaccumulate: aggregate.Deaccumulate(),
                difference: aggregate.Difference(),
                computeResult: aggregate.ComputeResult().TransformOutput(transform));
        }

        private static Expression<Func<T1, TOutput>> TransformOutput<T1, TFuncOutput, TOutput>(
            this Expression<Func<T1, TFuncOutput>> func,
            Expression<Func<TFuncOutput, TOutput>> transform)
        {
            Contract.Requires(func != null);
            Contract.Requires(transform != null);
            var result = transform.ReplaceParametersInBody(func.Body);
            return Expression.Lambda<Func<T1, TOutput>>(result, func.Parameters);
        }

        /// <summary>
        /// Applies a filter to the input of an aggregate. Any values for which the filter predicate returns false
        /// will be dropped.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static IAggregate<TInput, TState, TResult> ApplyFilter<TInput, TState, TResult>(
            this IAggregate<TInput, TState, TResult> aggregate,
            Expression<Func<TInput, bool>> filter)
        {
            Contract.Requires(aggregate != null);
            if (filter == null || filter.Body.ExpressionEquals(Expression.Constant(true))) return aggregate;

            Expression<Func<TState, long, TInput, TState>> newAccumulate = (oldState, timestamp, input) =>
                CallInliner.Call(filter, input) ? CallInliner.Call(aggregate.Accumulate(), oldState, timestamp, input) : oldState;

            Expression<Func<TState, long, TInput, TState>> newDeaccumulate = (oldState, timestamp, input) =>
                CallInliner.Call(filter, input) ? CallInliner.Call(aggregate.Deaccumulate(), oldState, timestamp, input) : oldState;

            return GeneratedAggregate.Create(
                initialState: aggregate.InitialState(),
                accumulate: newAccumulate.InlineCalls(),
                deaccumulate: newDeaccumulate.InlineCalls(),
                difference: aggregate.Difference(),
                computeResult: aggregate.ComputeResult());
        }

        /// <summary>
        /// Makes the input to the aggregate nullable with the behavior of dropping null values.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static IAggregate<TInput?, TState, TResult> MakeInputNullableAndSkipNulls<TInput, TState, TResult>(
            this IAggregate<TInput, TState, TResult> aggregate) where TInput : struct
        {
            Contract.Requires(aggregate != null);

            Expression<Func<TState, long, TInput?, TState>> newAccumulate = (oldState, timestamp, input) =>
                input.HasValue ? CallInliner.Call(aggregate.Accumulate(), oldState, timestamp, input.Value) : oldState;

            Expression<Func<TState, long, TInput?, TState>> newDeaccumulate = (oldState, timestamp, input) =>
                input.HasValue ? CallInliner.Call(aggregate.Deaccumulate(), oldState, timestamp, input.Value) : oldState;

            return GeneratedAggregate.Create(
                initialState: aggregate.InitialState(),
                accumulate: newAccumulate.InlineCalls(),
                deaccumulate: newDeaccumulate.InlineCalls(),
                difference: aggregate.Difference(),
                computeResult: aggregate.ComputeResult());
        }

        /// <summary>
        /// Drops null values on the input if the input type is either reference-type of value-type nullable.
        /// The function does not change the aggregate if the input type is a non-nullable value-type.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static IAggregate<TInput, TState, TResult> SkipNulls<TInput, TState, TResult>(
            this IAggregate<TInput, TState, TResult> aggregate)
        {
            Contract.Requires(aggregate != null);

            var inputType = typeof(TInput).GetTypeInfo();
            return inputType.IsClass
                ? GeneratedAggregate.Create(
                    initialState: aggregate.InitialState(),
                    accumulate: AddSkipNullClassLogic(aggregate.Accumulate()),
                    deaccumulate: AddSkipNullClassLogic(aggregate.Deaccumulate()),
                    difference: aggregate.Difference(),
                    computeResult: aggregate.ComputeResult())
                : inputType.IsGenericType && inputType.GetGenericTypeDefinition() == typeof(Nullable<>)
                ? GeneratedAggregate.Create(
                    initialState: aggregate.InitialState(),
                    accumulate: AddSkipNullValueLogic(aggregate.Accumulate()),
                    deaccumulate: AddSkipNullValueLogic(aggregate.Deaccumulate()),
                    difference: aggregate.Difference(),
                    computeResult: aggregate.ComputeResult())
                : aggregate;
        }

        private static Expression<Func<TState, long, TInput, TState>> AddSkipNullClassLogic<TState, TInput>(Expression<Func<TState, long, TInput, TState>> expression)
        {
            var state = expression.Parameters[0];
            var timestamp = expression.Parameters[1];
            var input = expression.Parameters[2];
            var newBody = Expression.Condition(
                Expression.ReferenceNotEqual(input, Expression.Constant(null, typeof(TInput))),
                expression.Body,
                state);
            return Expression.Lambda<Func<TState, long, TInput, TState>>(newBody, state, timestamp, input);
        }

        private static Expression<Func<TState, long, TInput, TState>> AddSkipNullValueLogic<TState, TInput>(Expression<Func<TState, long, TInput, TState>> expression)
        {
            var state = expression.Parameters[0];
            var timestamp = expression.Parameters[1];
            var input = expression.Parameters[2];
            var newBody = Expression.Condition(
                Expression.Property(input, "HasValue"),
                expression.Body,
                state);
            return Expression.Lambda<Func<TState, long, TInput, TState>>(newBody, state, timestamp, input);
        }

        /// <summary>
        /// Makes the output of the aggregate nullable with the behavior being to automatically return "null"
        /// instead of calling the aggregate's "ComputeResult" expression when the snapshot is empty. An
        /// empty snapshot is possible when some of the input values are dropped (either through dropping nulls
        /// or with a filter).
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static IAggregate<TInput, NullOutputWrapper<TState>, TResult?> MakeOutputNullableAndOutputNullWhenEmpty<TInput, TState, TResult>(
            this IAggregate<TInput, TState, TResult> aggregate) where TResult : struct
        {
            Contract.Requires(aggregate != null);

            Expression<Func<NullOutputWrapper<TState>>> newInitialState =
                () => new NullOutputWrapper<TState>
                {
                    Count = 0,
                    State = CallInliner.Call(aggregate.InitialState())
                };

            Expression<Func<NullOutputWrapper<TState>, long, TInput, NullOutputWrapper<TState>>> newAccumulate =
                (oldState, timestamp, input) => new NullOutputWrapper<TState>
                {
                    Count = oldState.Count + 1,
                    State = CallInliner.Call(aggregate.Accumulate(), oldState.State, timestamp, input)
                };

            Expression<Func<NullOutputWrapper<TState>, long, TInput, NullOutputWrapper<TState>>> newDeaccumulate =
                (oldState, timestamp, input) => new NullOutputWrapper<TState>
                {
                    Count = oldState.Count - 1,
                    State = CallInliner.Call(aggregate.Deaccumulate(), oldState.State, timestamp, input)
                };

            Expression<Func<NullOutputWrapper<TState>, NullOutputWrapper<TState>, NullOutputWrapper<TState>>> newDifference =
                (leftState, rightState) => new NullOutputWrapper<TState>
                {
                    Count = leftState.Count - rightState.Count,
                    State = CallInliner.Call(aggregate.Difference(), leftState.State, rightState.State)
                };

            Expression<Func<NullOutputWrapper<TState>, TResult?>> newComputeResult =
                state => state.Count == 0 ? (TResult?)null : CallInliner.Call(aggregate.ComputeResult(), state.State);

            return GeneratedAggregate.Create(
                initialState: newInitialState.InlineCalls(),
                accumulate: newAccumulate.InlineCalls(),
                deaccumulate: newDeaccumulate.InlineCalls(),
                difference: newDifference.InlineCalls(),
                computeResult: newComputeResult.InlineCalls());
        }

        /// <summary>
        /// Modifies the aggregate to return "null" instead of calling the aggregate's "ComputeResult" expression
        /// when the snapshot is empty. An empty snapshot is possible when some of the input values are dropped
        /// (either through dropping nulls or with a filter).
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static IAggregate<TInput, NullOutputWrapper<TState>, TResult> OutputDefaultWhenEmpty<TInput, TState, TResult>(
            this IAggregate<TInput, TState, TResult> aggregate)
        {
            Contract.Requires(aggregate != null);

            Expression<Func<NullOutputWrapper<TState>>> newInitialState =
                () => new NullOutputWrapper<TState>
                {
                    Count = 0,
                    State = CallInliner.Call(aggregate.InitialState())
                };

            Expression<Func<NullOutputWrapper<TState>, long, TInput, NullOutputWrapper<TState>>> newAccumulate =
                (oldState, timestamp, input) => new NullOutputWrapper<TState>
                {
                    Count = oldState.Count + 1,
                    State = CallInliner.Call(aggregate.Accumulate(), oldState.State, timestamp, input)
                };

            Expression<Func<NullOutputWrapper<TState>, long, TInput, NullOutputWrapper<TState>>> newDeaccumulate =
                (oldState, timestamp, input) => new NullOutputWrapper<TState>
                {
                    Count = oldState.Count - 1,
                    State = CallInliner.Call(aggregate.Deaccumulate(), oldState.State, timestamp, input)
                };

            Expression<Func<NullOutputWrapper<TState>, NullOutputWrapper<TState>, NullOutputWrapper<TState>>> newDifference =
                (leftState, rightState) => new NullOutputWrapper<TState>
                {
                    Count = leftState.Count - rightState.Count,
                    State = CallInliner.Call(aggregate.Difference(), leftState.State, rightState.State)
                };

            Expression<Func<NullOutputWrapper<TState>, TResult>> newComputeResult =
                state => state.Count == 0 ? default : CallInliner.Call(aggregate.ComputeResult(), state.State);

            return GeneratedAggregate.Create(
                initialState: newInitialState.InlineCalls(),
                accumulate: newAccumulate.InlineCalls(),
                deaccumulate: newDeaccumulate.InlineCalls(),
                difference: newDifference.InlineCalls(),
                computeResult: newComputeResult.InlineCalls());
        }
    }
}
