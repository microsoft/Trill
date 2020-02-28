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
    /* The following invariant holds for the state:
     * 1. The 'values' list is always in decreasing order of payload - that happens as we append only smaller values than last.
     * 2. The 'values.timestamp' is always in increasing order - as the new values are always appended.
     * 3. The 'currentTimestamp' is higher than all timestamps of the 'state.values' list
     * 4. The 'currentValue' payload need not be the lowest (relative to 'state.values')
     * -
     * Further for usage from HoppingPipe:
     * 5. The 'ecq.state.values' list will always be empty, optimizing difference operation
     * 6. The difference calculation happens by timestamp, and uses one bulk remove (RemoveRange/Clear) for values.
    */
    internal abstract class SlidingMinMaxAggregate<T> : IAggregate<T, MinMaxState<T>, T>
    {
        private static readonly long InvalidSyncTime = StreamEvent.MinSyncTime - 1;

        // The comparer is used as if we are working for Max operation. For Min, we reverse the expression
        private readonly Comparison<T> comparer;

        public SlidingMinMaxAggregate(IComparerExpression<T> comparer)
        {
            Contract.Requires(comparer != null);
            this.comparer = comparer.GetCompareExpr().Compile();
        }

        public Expression<Func<MinMaxState<T>>> InitialState()
            => () => new MinMaxState<T> { values = new ElasticCircularBuffer<ValueAndTimestamp<T>>(), currentValue = default, currentTimestamp = InvalidSyncTime };

        public Expression<Func<MinMaxState<T>, long, T, MinMaxState<T>>> Accumulate()
            => (state, timestamp, input) => Accumulate(state, timestamp, input);

        private MinMaxState<T> Accumulate(MinMaxState<T> state, long timestamp, T input)
        {
            if (timestamp == state.currentTimestamp)
            {
                if (this.comparer(input, state.currentValue) > 0)
                    state.currentValue = input;
            }
            else
            {
                // Save only if new input is smaller,
                // If the current input is larger (or equal), we never require older value as they expire before current
                if (state.currentTimestamp != InvalidSyncTime &&
                    this.comparer(input, state.currentValue) < 0)
                {
                    PushToCollection(state);
                }
                state.currentTimestamp = timestamp;
                state.currentValue = input;
            }
            return state;
        }

        private void PushToCollection(MinMaxState<T> state)
        {
            Contract.Assert(state.currentTimestamp != InvalidSyncTime);
            Contract.Assert(state.values.Count == 0 || state.values.PeekLast().timestamp <= state.currentTimestamp); // Ensure new timestamp is higher

            while (state.values.Count != 0 &&
                   this.comparer(state.values.PeekLast().value, state.currentValue) <= 0)
            {
                state.values.PopLast();
            }

            var newValue = new ValueAndTimestamp<T> { timestamp = state.currentTimestamp, value = state.currentValue };
            state.values.Enqueue(ref newValue);
        }

        public Expression<Func<MinMaxState<T>, long, T, MinMaxState<T>>> Deaccumulate()
            => (state, timestamp, input) => Deaccumulate(state, timestamp, input);

        private static MinMaxState<T> Deaccumulate(MinMaxState<T> state, long timestamp, T input)
        {
            throw new NotImplementedException($"{nameof(SlidingMinMaxAggregate<T>)} does not implement Deaccumulate()");
        }

        public Expression<Func<MinMaxState<T>, MinMaxState<T>, MinMaxState<T>>> Difference()
            => (leftSet, rightSet) => Difference(leftSet, rightSet);

        private static MinMaxState<T> Difference(MinMaxState<T> leftSet, MinMaxState<T> rightSet)
        {
            long maxRightTimestamp;

            if (rightSet.currentTimestamp != InvalidSyncTime)
            {
                maxRightTimestamp = rightSet.currentTimestamp;
            }
            else
            {
                if (rightSet.values.Count == 0)
                    return leftSet;

                // The right set will never contain values for HoppingPipe, adding below for completeness
                maxRightTimestamp = rightSet.values.PeekLast().timestamp;
            }

            if (leftSet.currentTimestamp != InvalidSyncTime &&
                leftSet.currentTimestamp <= maxRightTimestamp)
            {
                leftSet.currentTimestamp = InvalidSyncTime; // Discard all values if rightSet's maxTime covers it.
                leftSet.values.Clear();
                return leftSet;
            }
            else
            {
                while (leftSet.values.Count != 0 &&
                       leftSet.values.PeekFirst().timestamp <= maxRightTimestamp)
                {
                    leftSet.values.Dequeue();
                }
            }

            return leftSet;
        }

        public Expression<Func<MinMaxState<T>, T>> ComputeResult() => state => ComputeResult(state);

        private T ComputeResult(MinMaxState<T> state)
        {
            if (state.values.Count == 0)
                return (state.currentTimestamp == InvalidSyncTime) ? default : state.currentValue;
            else
            {
                var first = state.values.PeekFirst().value;

                if (state.currentTimestamp == InvalidSyncTime)
                    return first;
                else
                {
                    return this.comparer(first, state.currentValue) > 0 ? first : state.currentValue;
                }
            }
        }
    }

    internal sealed class SlidingMaxAggregate<T> : SlidingMinMaxAggregate<T>
    {
        public SlidingMaxAggregate()
            : this(ComparerExpression<T>.Default) { }

        public SlidingMaxAggregate(IComparerExpression<T> comparer)
            : base(comparer) { }
    }

    internal sealed class SlidingMinAggregate<T> : SlidingMinMaxAggregate<T>
    {
        public SlidingMinAggregate()
            : this(ComparerExpression<T>.Default) { }

        public SlidingMinAggregate(IComparerExpression<T> comparer)
            : base(ComparerExpression<T>.Reverse(comparer)) { }
    }
}
