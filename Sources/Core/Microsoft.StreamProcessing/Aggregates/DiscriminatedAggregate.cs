// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Aggregates
{
    internal struct DiscriminatedUnion<TLeft, TRight>
    {
        public TLeft Left;
        public TRight Right;
        public bool isLeft;
    }

    internal class DiscriminatedAggregate<TLeft, TRight, TState, TResult> : IAggregate<DiscriminatedUnion<TLeft, TRight>, TState, TResult>
    {
        private readonly IBinaryAggregate<TLeft, TRight, TState, TResult> binary;

        public DiscriminatedAggregate(IBinaryAggregate<TLeft, TRight, TState, TResult> binary) => this.binary = binary;

        private static Expression<Func<TState, long, DiscriminatedUnion<TLeft, TRight>, TState>> MergeMethods(Expression<Func<TState, long, TLeft, TState>> accumulateLeft, Expression<Func<TState, long, TRight, TState>> accumulateRight)
        {
            Expression<Func<TState, long, DiscriminatedUnion<TLeft, TRight>, TState>> placeholder = (s, l, d) => default;

            Expression<Func<DiscriminatedUnion<TLeft, TRight>, TLeft>> leftSelector = d => d.Left;
            var leftProperty = leftSelector.ReplaceParametersInBody(placeholder.Parameters[2]);

            Expression<Func<DiscriminatedUnion<TLeft, TRight>, TRight>> rightSelector = d => d.Right;
            var rightProperty = rightSelector.ReplaceParametersInBody(placeholder.Parameters[2]);

            Expression<Func<DiscriminatedUnion<TLeft, TRight>, bool>> discriminatorSelector = d => d.isLeft;
            var discriminator = discriminatorSelector.ReplaceParametersInBody(placeholder.Parameters[2]);

            return Expression.Lambda<Func<TState, long, DiscriminatedUnion<TLeft, TRight>, TState>>(
                Expression.Condition(
                    discriminator,
                    accumulateLeft.ReplaceParametersInBody(placeholder.Parameters[0], placeholder.Parameters[1], leftProperty),
                    accumulateRight.ReplaceParametersInBody(placeholder.Parameters[0], placeholder.Parameters[1], rightProperty)),
                placeholder.Parameters);
        }

        public Expression<Func<TState, long, DiscriminatedUnion<TLeft, TRight>, TState>> Accumulate()
            => MergeMethods(this.binary.AccumulateLeft(), this.binary.AccumulateRight());

        public Expression<Func<TState, TResult>> ComputeResult() => this.binary.ComputeResult();

        public Expression<Func<TState, long, DiscriminatedUnion<TLeft, TRight>, TState>> Deaccumulate()
            => MergeMethods(this.binary.DeaccumulateLeft(), this.binary.DeaccumulateRight());

        public Expression<Func<TState, TState, TState>> Difference() => this.binary.Difference();

        public Expression<Func<TState>> InitialState() => this.binary.InitialState();
    }
}
