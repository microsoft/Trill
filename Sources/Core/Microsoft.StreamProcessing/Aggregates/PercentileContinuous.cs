// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Aggregates
{
    internal sealed class PercentileContinuousDoubleAggregate : SortedMultisetAggregateBase<double, double>
    {
        private readonly double percentile;

        public PercentileContinuousDoubleAggregate(double percentile, QueryContainer container)
            : this(percentile, ComparerExpression<double>.Default, container) { }

        public PercentileContinuousDoubleAggregate(double percentile, IComparerExpression<double> rankComparer, QueryContainer container)
            : base(rankComparer, container)
        {
            Contract.Requires(rankComparer != null);
            Contract.Requires(percentile >= 0.0 && percentile <= 1.0);
            this.percentile = percentile;
        }

        public override Expression<Func<SortedMultiSet<double>, double>> ComputeResult() => set => CalculatePercentile(set);

        public double CalculatePercentile(SortedMultiSet<double> set)
        {
            double result = default;
            if (set.TotalCount > 0)
            {
                double currentFloorRankPlusOne = 0;

                double floorRank = this.percentile * (set.TotalCount - 1);
                double dividend = floorRank % 1;
                floorRank = Math.Floor(floorRank);

                var enumerator = set.GetEnumerable().GetEnumerator();
                for (int index = 0; index <= floorRank; index++) enumerator.MoveNext();
                var currentFloorRank = enumerator.Current;

                if (dividend != 0)
                {
                    enumerator.MoveNext();
                    currentFloorRankPlusOne = enumerator.Current;
                }

                result = currentFloorRank + dividend * (currentFloorRankPlusOne - currentFloorRank);
            }

            return result;
        }
    }
}
