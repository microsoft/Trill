// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.StreamProcessing.Internal;

namespace Microsoft.StreamProcessing.Aggregates
{
    internal sealed class TumblingMaxAggregate<T> : IAggregate<T, MinMaxState<T>, T>
    {
        private static readonly long InvalidSyncTime = StreamEvent.MinSyncTime - 1;
        private readonly Expression<Func<MinMaxState<T>, long, T, MinMaxState<T>>> accumulate;

        public TumblingMaxAggregate() : this(ComparerExpression<T>.Default) { }

        public TumblingMaxAggregate(IComparerExpression<T> comparer)
        {
            Contract.Requires(comparer != null);

            var stateExpression = Expression.Parameter(typeof(MinMaxState<T>), "state");
            var timestampExpression = Expression.Parameter(typeof(long), "timestamp");
            var inputExpression = Expression.Parameter(typeof(T), "input");

            Expression<Func<MinMaxState<T>>> constructor = () => new MinMaxState<T>();
            Expression<Func<MinMaxState<T>, long>> currentTimestamp = (state) => state.currentTimestamp;
            Expression<Func<MinMaxState<T>, T>> currentValue = (state) => state.currentValue;
            var currentTimestampExpression = currentTimestamp.ReplaceParametersInBody(stateExpression);
            var comparerExpression = comparer.GetCompareExpr().ReplaceParametersInBody(
                inputExpression, currentValue.ReplaceParametersInBody(stateExpression));

            var typeInfo = typeof(MinMaxState<T>).GetTypeInfo();
            this.accumulate = Expression.Lambda<Func<MinMaxState<T>, long, T, MinMaxState<T>>>(
                Expression.Condition(
                    Expression.OrElse(
                        Expression.Equal(currentTimestampExpression, Expression.Constant(InvalidSyncTime)),
                        Expression.GreaterThan(comparerExpression, Expression.Constant(0))),
                    Expression.MemberInit(
                        (NewExpression)constructor.Body,
                        Expression.Bind(typeInfo.GetField("currentTimestamp"), timestampExpression),
                        Expression.Bind(typeInfo.GetField("currentValue"), inputExpression)),
                    stateExpression),
                stateExpression,
                timestampExpression,
                inputExpression);
        }

        public Expression<Func<MinMaxState<T>>> InitialState()
            => () => new MinMaxState<T> { currentTimestamp = InvalidSyncTime };

        public Expression<Func<MinMaxState<T>, long, T, MinMaxState<T>>> Accumulate() => this.accumulate;

        public Expression<Func<MinMaxState<T>, long, T, MinMaxState<T>>> Deaccumulate()
            => (state, timestamp, input) => state; // never invoked, hence not implemented

        public Expression<Func<MinMaxState<T>, MinMaxState<T>, MinMaxState<T>>> Difference()
            => (leftSet, rightSet) => leftSet; // never invoked, hence not implemented

        public Expression<Func<MinMaxState<T>, T>> ComputeResult() => state => state.currentValue;
    }
}
