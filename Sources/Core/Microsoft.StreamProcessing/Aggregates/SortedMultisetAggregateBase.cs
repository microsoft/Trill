// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Microsoft.StreamProcessing.Internal;

namespace Microsoft.StreamProcessing.Aggregates
{
    internal abstract class SortedMultisetAggregateBase<T, R> : ISummableAggregate<T, SortedMultiSet<T>, R>
    {
        protected SortedMultisetAggregateBase(IComparerExpression<T> comparer, QueryContainer container)
        {
            var generator = comparer.CreateSortedDictionaryGenerator<T, long>(container);
            Expression<Func<Func<SortedDictionary<T, long>>, SortedMultiSet<T>>> template
                = (g) => new SortedMultiSet<T>(g);
            var replaced = template.ReplaceParametersInBody(generator);
            initialState = Expression.Lambda<Func<SortedMultiSet<T>>>(replaced);
        }

        private readonly Expression<Func<SortedMultiSet<T>>> initialState;
        public Expression<Func<SortedMultiSet<T>>> InitialState() => initialState;

        private static readonly Expression<Func<SortedMultiSet<T>, long, T, SortedMultiSet<T>>> acc
            = (set, timestamp, input) => set.Add(input);
        public Expression<Func<SortedMultiSet<T>, long, T, SortedMultiSet<T>>> Accumulate() => acc;

        private static readonly Expression<Func<SortedMultiSet<T>, long, T, SortedMultiSet<T>>> dec
            = (set, timestamp, input) => set.Remove(input);
        public Expression<Func<SortedMultiSet<T>, long, T, SortedMultiSet<T>>> Deaccumulate() => dec;

        private static readonly Expression<Func<SortedMultiSet<T>, SortedMultiSet<T>, SortedMultiSet<T>>> diff
            = (leftSet, rightSet) => leftSet.RemoveAll(rightSet);
        public Expression<Func<SortedMultiSet<T>, SortedMultiSet<T>, SortedMultiSet<T>>> Difference() => diff;

        private static readonly Expression<Func<SortedMultiSet<T>, SortedMultiSet<T>, SortedMultiSet<T>>> sum
            = (leftSet, rightSet) => leftSet.AddAll(rightSet);
        public Expression<Func<SortedMultiSet<T>, SortedMultiSet<T>, SortedMultiSet<T>>> Sum() => sum;

        public abstract Expression<Func<SortedMultiSet<T>, R>> ComputeResult();
    }
}
