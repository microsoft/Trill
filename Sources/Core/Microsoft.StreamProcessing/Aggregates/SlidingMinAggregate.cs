// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;
using Microsoft.StreamProcessing.Internal;

namespace Microsoft.StreamProcessing.Aggregates
{
    internal sealed class SlidingMinAggregate<T> : IAggregate<T, MinMaxState<T>, T>
    {
        private static readonly long InvalidSyncTime = StreamEvent.MinSyncTime - 1;
        private readonly Comparison<T> comparer;

        public SlidingMinAggregate(QueryContainer container) : this(ComparerExpression<T>.Default, container) { }

        public SlidingMinAggregate(IComparerExpression<T> comparer, QueryContainer container)
        {
            Contract.Requires(comparer != null);
            this.comparer = comparer.GetCompareExpr().Compile();

            var generator = comparer.CreateSortedDictionaryGenerator<T, long>(container);
            Expression<Func<Func<SortedDictionary<T, long>>, MinMaxState<T>>> template
                = (g) => new MinMaxState<T> { savedValues = new SortedMultiSet<T>(g), currentValue = default, currentTimestamp = InvalidSyncTime };
            var replaced = template.ReplaceParametersInBody(generator);
            this.initialState = Expression.Lambda<Func<MinMaxState<T>>>(replaced);
        }

        private readonly Expression<Func<MinMaxState<T>>> initialState;
        public Expression<Func<MinMaxState<T>>> InitialState() => initialState;

        public Expression<Func<MinMaxState<T>, long, T, MinMaxState<T>>> Accumulate()
            => (state, timestamp, input) => Accumulate(state, timestamp, input);

        private MinMaxState<T> Accumulate(MinMaxState<T> state, long timestamp, T input)
        {
            if (timestamp == state.currentTimestamp)
            {
                if (this.comparer(input, state.currentValue) < 0)
                    state.currentValue = input;
            }
            else
            {
                if (state.currentTimestamp != InvalidSyncTime)
                    state.savedValues.Add(state.currentValue);
                state.currentTimestamp = timestamp;
                state.currentValue = input;
            }
            return state;
        }

        public Expression<Func<MinMaxState<T>, long, T, MinMaxState<T>>> Deaccumulate()
            => (state, timestamp, input) => state; // never invoked, hence not implemented

        public Expression<Func<MinMaxState<T>, MinMaxState<T>, MinMaxState<T>>> Difference()
            => (leftSet, rightSet) => Difference(leftSet, rightSet);

        private static MinMaxState<T> Difference(MinMaxState<T> leftSet, MinMaxState<T> rightSet)
        {
            if (leftSet.currentTimestamp != InvalidSyncTime)
            {
                leftSet.currentTimestamp = InvalidSyncTime;
                leftSet.savedValues.Add(leftSet.currentValue);
            }
            if (rightSet.currentTimestamp != InvalidSyncTime)
            {
                rightSet.currentTimestamp = InvalidSyncTime;
                rightSet.savedValues.Add(rightSet.currentValue);
            }

            leftSet.savedValues.RemoveAll(rightSet.savedValues);
            return leftSet;
        }

        public Expression<Func<MinMaxState<T>, T>> ComputeResult() => state => ComputeResult(state);

        private T ComputeResult(MinMaxState<T> state)
        {
            if (state.savedValues.IsEmpty)
                return (state.currentTimestamp == InvalidSyncTime) ? default : state.currentValue;
            else
            {
                if (state.currentTimestamp == InvalidSyncTime)
                    return state.savedValues.First();
                else
                {
                    var first = state.savedValues.First();
                    return this.comparer(first, state.currentValue) < 0 ? first : state.currentValue;
                }
            }
        }
    }
}
