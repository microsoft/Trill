// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Aggregates
{
    internal static class NaiveAggregate
    {
        public static NaiveAggregate<TInput, TOutput> Generate<TInput, TOutput>(Expression<Func<List<TInput>, TOutput>> resultConstructor)
            => new NaiveAggregate<TInput, TOutput>(resultConstructor);
    }

    internal sealed class NaiveAggregate<TInput, TOutput> : ListAggregateBase<TInput, TOutput>
    {
        private readonly Expression<Func<List<TInput>, TOutput>> createResult;

        public NaiveAggregate(Expression<Func<List<TInput>, TOutput>> resultConstructor)
            => this.createResult = resultConstructor;

        public override Expression<Func<List<TInput>, TOutput>> ComputeResult() => createResult;
    }
}
