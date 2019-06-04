// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Aggregates
{
    internal sealed class SingleOrDefaultAggregate<T> : SortedMultisetAggregateBase<T, T>
    {
        public SingleOrDefaultAggregate(QueryContainer container) : base(ComparerExpression<T>.Default, container) { }

        private static readonly Expression<Func<SortedMultiSet<T>, T>> res = set => set.SingleOrDefault();
        public override Expression<Func<SortedMultiSet<T>, T>> ComputeResult() => res;
    }
}
