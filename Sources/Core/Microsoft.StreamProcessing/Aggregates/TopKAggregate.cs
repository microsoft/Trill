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
    internal sealed class TopKAggregate<T> : IAggregate<T, ITopKState<T>, List<RankedEvent<T>>>
    {
        private readonly Comparison<T> compiledRankComparer;
        private readonly int k;

        public TopKAggregate(int k, IComparerExpression<T> rankComparer, QueryContainer container, long hoppingWindowSize)
            : this(k, rankComparer, ComparerExpression<T>.Default, container, hoppingWindowSize) { }

        public TopKAggregate(int k, IComparerExpression<T> rankComparer, IComparerExpression<T> overallComparer,
            QueryContainer container, long hoppingWindowSize)
        {
            Contract.Requires(rankComparer != null);
            Contract.Requires(overallComparer != null);
            Contract.Requires(k > 0);
            this.compiledRankComparer = Reverse(rankComparer).GetCompareExpr().Compile();
            this.k = k;

            Expression<Func<Func<SortedDictionary<T, long>>, ITopKState<T>>> template;
            if (hoppingWindowSize > 0 && hoppingWindowSize < 1000000 && !Config.DisableNewOptimizations)
                template = (g) => new HoppingTopKState<T>(k, compiledRankComparer, (int)hoppingWindowSize, g);
            else
                template = (g) => new SimpleTopKState<T>(g);

            var combinedComparer = ThenOrderBy(Reverse(rankComparer), overallComparer);
            var generator = combinedComparer.CreateSortedDictionaryGenerator<T, long>(container);
            var replaced = template.ReplaceParametersInBody(generator);
            this.initialState = Expression.Lambda<Func<ITopKState<T>>>(replaced);
        }

        private static IComparerExpression<T> Reverse(IComparerExpression<T> comparer)
        {
            Contract.Requires(comparer != null);
            var expression = comparer.GetCompareExpr();
            Expression<Comparison<T>> template = (left, right) => CallInliner.Call(expression, right, left);
            var reversedExpression = template.InlineCalls();
            return new ComparerExpression<T>(reversedExpression);
        }

        private static IComparerExpression<T> ThenOrderBy(IComparerExpression<T> comparer1, IComparerExpression<T> comparer2)
        {
            Contract.Requires(comparer1 != null);
            Contract.Requires(comparer2 != null);
            var primary = comparer1.GetCompareExpr();
            var secondary = comparer2.GetCompareExpr();
            Expression<Comparison<T>> template =
                (left, right) =>
                CallInliner.Call(primary, left, right) == 0
                    ? CallInliner.Call(secondary, left, right)
                    : CallInliner.Call(primary, left, right);
            var newExpression = template.InlineCalls();
            return new ComparerExpression<T>(newExpression);
        }

        public Expression<Func<ITopKState<T>, List<RankedEvent<T>>>> ComputeResult() => set => GetTopK(set);

        private List<RankedEvent<T>> GetTopK(ITopKState<T> state)
        {
            var set = state.GetSortedValues();
            int count = (int)Math.Min(this.k, set.TotalCount);
            var result = new List<RankedEvent<T>>(count);
            int nextRank = 1;
            int outputRank = 1;
            bool first = true;
            T rankValue = default;
            foreach (var value in set.GetEnumerable())
            {
                if (first || this.compiledRankComparer(rankValue, value) != 0)
                {
                    if (result.Count >= count) break;

                    outputRank = nextRank;
                    rankValue = value;
                    first = false;
                }

                // Ranking has gaps on it, this is expected
                // Rank value follows the same as Sql Rank and not Dense_Rank
                result.Add(new RankedEvent<T>(outputRank, value));
                nextRank++;
            }

            return result;
        }

        private readonly Expression<Func<ITopKState<T>>> initialState;
        public Expression<Func<ITopKState<T>>> InitialState() => initialState;

        private static readonly Expression<Func<ITopKState<T>, long, T, ITopKState<T>>> acc
            = (state, timestamp, input) => state.Add(input, timestamp);
        public Expression<Func<ITopKState<T>, long, T, ITopKState<T>>> Accumulate() => acc;

        private static readonly Expression<Func<ITopKState<T>, long, T, ITopKState<T>>> dec
            = (state, timestamp, input) => state.Remove(input, timestamp);
        public Expression<Func<ITopKState<T>, long, T, ITopKState<T>>> Deaccumulate() => dec;

        private static readonly Expression<Func<ITopKState<T>, ITopKState<T>, ITopKState<T>>> diff
            = (leftState, rightState) => leftState.RemoveAll(rightState);
        public Expression<Func<ITopKState<T>, ITopKState<T>, ITopKState<T>>> Difference() => diff;
    }
}
