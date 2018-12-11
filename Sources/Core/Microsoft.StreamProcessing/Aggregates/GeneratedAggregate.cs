// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Aggregates
{
    internal class GeneratedAggregate<TInput, TState, TResult> : IAggregate<TInput, TState, TResult>
    {
        private readonly Expression<Func<TState>> initialState;

        private readonly Expression<Func<TState, long, TInput, TState>> accumulate;

        private readonly Expression<Func<TState, long, TInput, TState>> deaccumulate;

        private readonly Expression<Func<TState, TState, TState>> difference;

        private readonly Expression<Func<TState, TResult>> computeResult;

        public GeneratedAggregate(
            Expression<Func<TState>> initialState,
            Expression<Func<TState, long, TInput, TState>> accumulate,
            Expression<Func<TState, long, TInput, TState>> deaccumulate,
            Expression<Func<TState, TState, TState>> difference,
            Expression<Func<TState, TResult>> computeResult)
        {
            Contract.Requires(initialState != null);
            Contract.Requires(accumulate != null);
            Contract.Requires(deaccumulate != null);
            Contract.Requires(difference != null);
            Contract.Requires(computeResult != null);
            this.initialState = initialState;
            this.accumulate = accumulate;
            this.deaccumulate = deaccumulate;
            this.difference = difference;
            this.computeResult = computeResult;
        }

        public Expression<Func<TState>> InitialState() => this.initialState;

        public Expression<Func<TState, long, TInput, TState>> Accumulate() => this.accumulate;

        public Expression<Func<TState, long, TInput, TState>> Deaccumulate() => this.deaccumulate;

        public Expression<Func<TState, TState, TState>> Difference() => this.difference;

        public Expression<Func<TState, TResult>> ComputeResult() => this.computeResult;

        public void Print()
        {
            Console.WriteLine("Initial State: " + this.initialState);
            Console.WriteLine("Accumulate: " + this.accumulate);
            Console.WriteLine("Deaccumulate: " + this.deaccumulate);
            Console.WriteLine("Difference: " + this.difference);
            Console.WriteLine("Compute Result: " + this.computeResult);
        }
    }

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
            Expression leftProperty = leftSelector.ReplaceParametersInBody(placeholder.Parameters[2]);

            Expression<Func<DiscriminatedUnion<TLeft, TRight>, TRight>> rightSelector = d => d.Right;
            Expression rightProperty = rightSelector.ReplaceParametersInBody(placeholder.Parameters[2]);

            Expression<Func<DiscriminatedUnion<TLeft, TRight>, bool>> discriminatorSelector = d => d.isLeft;
            Expression discriminator = discriminatorSelector.ReplaceParametersInBody(placeholder.Parameters[2]);

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
