// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;
using System.Text;

namespace Microsoft.StreamProcessing.Aggregates
{
    internal static class GeneratedAggregate
    {
        public static GeneratedAggregate<TInput, TState, TResult> Create<TInput, TState, TResult>(
            Expression<Func<TState>> initialState,
            Expression<Func<TState, long, TInput, TState>> accumulate,
            Expression<Func<TState, long, TInput, TState>> deaccumulate,
            Expression<Func<TState, TState, TState>> difference,
            Expression<Func<TState, TResult>> computeResult)
            => new GeneratedAggregate<TInput, TState, TResult>(initialState, accumulate, deaccumulate, difference, computeResult);
    }

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

        public override string ToString()
            => new StringBuilder()
                .AppendLine("Initial State: " + this.initialState)
                .AppendLine("Accumulate: " + this.accumulate)
                .AppendLine("Deaccumulate: " + this.deaccumulate)
                .AppendLine("Difference: " + this.difference)
                .AppendLine("Compute Result: " + this.computeResult)
                .ToString();
    }
}
