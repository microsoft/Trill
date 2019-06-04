// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Aggregates
{
    internal sealed class CountAggregate<TInput> : ISummableAggregate<TInput, ulong, ulong>
    {
        private static readonly Expression<Func<ulong>> init = () => 0;
        public Expression<Func<ulong>> InitialState() => init;

        private static readonly Expression<Func<ulong, long, TInput, ulong>> acc = (oldCount, timestamp, input) => oldCount + 1;
        public Expression<Func<ulong, long, TInput, ulong>> Accumulate() => acc;

        private static readonly Expression<Func<ulong, long, TInput, ulong>> dec = (oldCount, timestamp, input) => oldCount - 1;
        public Expression<Func<ulong, long, TInput, ulong>> Deaccumulate() => dec;

        private static readonly Expression<Func<ulong, ulong, ulong>> diff = (leftCount, rightCount) => leftCount - rightCount;
        public Expression<Func<ulong, ulong, ulong>> Difference() => diff;

        private static readonly Expression<Func<ulong, ulong, ulong>> sum = (leftCount, rightCount) => leftCount - rightCount;
        public Expression<Func<ulong, ulong, ulong>> Sum() => sum;

        private static readonly Expression<Func<ulong, ulong>> res = count => count;
        public Expression<Func<ulong, ulong>> ComputeResult() => res;
    }
}
