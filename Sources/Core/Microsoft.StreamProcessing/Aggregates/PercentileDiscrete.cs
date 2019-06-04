// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Aggregates
{
    internal sealed class PercentileDiscreteDoubleAggregate : SortedMultisetAggregateBase<double, double>
    {
        private readonly double percentile;

        public PercentileDiscreteDoubleAggregate(double percentile, QueryContainer container)
            : this(percentile, ComparerExpression<double>.Default, container) { }

        public PercentileDiscreteDoubleAggregate(double percentile, IComparerExpression<double> rankComparer, QueryContainer container)
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
                int rank = (int)Math.Ceiling(this.percentile * set.TotalCount) - 1;

                var enumerator = set.GetEnumerable().GetEnumerator();
                for (int index = 0; index <= rank; index++) enumerator.MoveNext();
                result = enumerator.Current;
            }

            return result;
        }
    }
 }
