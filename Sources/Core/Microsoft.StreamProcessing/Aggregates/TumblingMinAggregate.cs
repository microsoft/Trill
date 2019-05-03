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
    internal class TumblingMinAggregate<T> : IAggregate<T, MinMaxState<T>, T>
    {
        private static readonly long InvalidSyncTime = StreamEvent.MinSyncTime - 1;
        private readonly Comparison<T> comparer;

        public TumblingMinAggregate() : this(ComparerExpression<T>.Default) { }

        public TumblingMinAggregate(IComparerExpression<T> comparer)
        {
            Contract.Requires(comparer != null);
            this.comparer = comparer.GetCompareExpr().Compile();
        }

        public Expression<Func<MinMaxState<T>>> InitialState()
            => () => new MinMaxState<T> { currentValue = default, currentTimestamp = InvalidSyncTime };

        public Expression<Func<MinMaxState<T>, long, T, MinMaxState<T>>> Accumulate()
            => (state, timestamp, input) => Accumulate(state, timestamp, input);

        private MinMaxState<T> Accumulate(MinMaxState<T> state, long timestamp, T input)
        {
            if (state.currentTimestamp == InvalidSyncTime)
            {
                state.currentTimestamp = timestamp;
                state.currentValue = input;
            }
            else if (this.comparer(input, state.currentValue) < 0)
                state.currentValue = input;
            return state;
        }

        public Expression<Func<MinMaxState<T>, long, T, MinMaxState<T>>> Deaccumulate()
            => (state, timestamp, input) => state; // never invoked, hence not implemented

        public Expression<Func<MinMaxState<T>, MinMaxState<T>, MinMaxState<T>>> Difference()
            => (leftSet, rightSet) => leftSet; // never invoked, hence not implemented

        public Expression<Func<MinMaxState<T>, T>> ComputeResult()
            => state => (state.currentTimestamp == InvalidSyncTime) ? default : state.currentValue;
    }
}
