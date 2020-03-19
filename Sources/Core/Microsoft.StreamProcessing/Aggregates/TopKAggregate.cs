// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Aggregates
{
    internal sealed class TopKAggregate<T> : SortedMultisetAggregateBase<T, List<RankedEvent<T>>>
    {
        private readonly Comparison<T> compiledRankComparer;
        private readonly int k;

        public TopKAggregate(int k, QueryContainer container) : this(k, ComparerExpression<T>.Default, container) { }

        public TopKAggregate(int k, IComparerExpression<T> rankComparer, QueryContainer container)
            : this(k, rankComparer, ComparerExpression<T>.Default, container) { }

        public TopKAggregate(int k, IComparerExpression<T> rankComparer, IComparerExpression<T> overallComparer, QueryContainer container)
            : base(ThenOrderBy(Reverse(rankComparer), overallComparer), container)
        {
            Contract.Requires(rankComparer != null);
            Contract.Requires(overallComparer != null);
            Contract.Requires(k > 0);
            this.compiledRankComparer = Reverse(rankComparer).GetCompareExpr().Compile();
            this.k = k;
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

        public override Expression<Func<SortedMultiSet<T>, List<RankedEvent<T>>>> ComputeResult() => set => GetTopK(set);

        private List<RankedEvent<T>> GetTopK(SortedMultiSet<T> set)
        {
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
    }
}
