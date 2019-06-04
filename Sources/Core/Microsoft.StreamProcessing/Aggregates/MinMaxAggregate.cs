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
    internal abstract class MinMaxAggregateBase<T> : ISummableAggregate<T, MinMaxState<T>, T>
    {
        protected MinMaxAggregateBase(QueryContainer container) : this(ComparerExpression<T>.Default, container) { }

        protected MinMaxAggregateBase(IComparerExpression<T> comparer, QueryContainer container)
        {
            Contract.Requires(comparer != null);

            var generator = comparer.CreateSortedDictionaryGenerator<T, long>(container);
            Expression<Func<Func<SortedDictionary<T, long>>, MinMaxState<T>>> template
                = (g) => new MinMaxState<T> { savedValues = new SortedMultiSet<T>(g) };
            var replaced = template.ReplaceParametersInBody(generator);
            this.initialState = Expression.Lambda<Func<MinMaxState<T>>>(replaced);
        }

        private readonly Expression<Func<MinMaxState<T>>> initialState;
        public Expression<Func<MinMaxState<T>>> InitialState() => initialState;

        private static readonly Expression<Func<MinMaxState<T>, long, T, MinMaxState<T>>> acc
            = (set, timestamp, input) => new MinMaxState<T> { savedValues = set.savedValues.Add(input) };
        public Expression<Func<MinMaxState<T>, long, T, MinMaxState<T>>> Accumulate() => acc;

        private static readonly Expression<Func<MinMaxState<T>, long, T, MinMaxState<T>>> dec
            = (set, timestamp, input) => new MinMaxState<T> { savedValues = set.savedValues.Remove(input) };
        public Expression<Func<MinMaxState<T>, long, T, MinMaxState<T>>> Deaccumulate() => dec;

        private static readonly Expression<Func<MinMaxState<T>, MinMaxState<T>, MinMaxState<T>>> diff
            = (leftSet, rightSet) => new MinMaxState<T> { savedValues = leftSet.savedValues.RemoveAll(rightSet.savedValues) };
        public Expression<Func<MinMaxState<T>, MinMaxState<T>, MinMaxState<T>>> Difference() => diff;

        private static readonly Expression<Func<MinMaxState<T>, MinMaxState<T>, MinMaxState<T>>> sum
            = (leftSet, rightSet) => new MinMaxState<T> { savedValues = leftSet.savedValues.AddAll(rightSet.savedValues) };
        public Expression<Func<MinMaxState<T>, MinMaxState<T>, MinMaxState<T>>> Sum() => sum;

        public abstract Expression<Func<MinMaxState<T>, T>> ComputeResult();
    }

    internal sealed class MaxAggregate<T> : MinMaxAggregateBase<T>
    {
        public MaxAggregate(QueryContainer container) : base(container) { }

        public MaxAggregate(IComparerExpression<T> comparer, QueryContainer container)
            : base(comparer, container) { }

        private static readonly Expression<Func<MinMaxState<T>, T>> result
            = set => set.savedValues.IsEmpty ? default : set.savedValues.Last();
        public override Expression<Func<MinMaxState<T>, T>> ComputeResult() => result;
    }

    internal sealed class MinAggregate<T> : MinMaxAggregateBase<T>
    {
        public MinAggregate(QueryContainer container) : base(container) { }

        public MinAggregate(IComparerExpression<T> comparer, QueryContainer container)
            : base(comparer, container) { }

        private static readonly Expression<Func<MinMaxState<T>, T>> result
            = set => set.savedValues.IsEmpty ? default : set.savedValues.First();
        public override Expression<Func<MinMaxState<T>, T>> ComputeResult() => result;
    }
}
