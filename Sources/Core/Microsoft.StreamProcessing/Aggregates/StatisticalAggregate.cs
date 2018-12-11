// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Aggregates
{
    internal abstract class StatisticalAggregate : ListAggregateBase<double, double?>
    {
        protected static double? ComputeStdev(List<double> valueList, bool useAsPopulation)
        {
            var variance = ComputeVariance(valueList, useAsPopulation);
            return variance.HasValue ? Math.Sqrt(variance.Value) : (double?)null;
        }

        protected static double? ComputeVariance(List<double> list, bool useAsPopulation)
        {
            if (list == null || list.Count == 0) return null;
            if (list.Count == 1) return useAsPopulation ? 0.0 : (double?)null;

            // compute mean
            // instead of dividing the sum of elements we divide each element to avoid a potential overflow
            double mean = list.Sum(element => element / list.Count);

            // for the population variance the divisor is n, for the sample variance the divisor is n - 1
            var divisor = useAsPopulation ? list.Count : list.Count - 1;

            // compute variance
            // instead of dividing the sum of differences we divide each difference to try to avoid a potential overflow
            double variance = list.Select(element => element - mean).Sum(difference => (difference * difference) / divisor);

            // difference or variance can still overflow
            return double.IsInfinity(variance) ? null : (double?)variance;
        }
    }

    /// <summary>
    /// An aggregate that computes the sample standard deviation
    /// </summary>
    internal sealed class StandardDeviationDouble : StatisticalAggregate
    {
        private static readonly Expression<Func<List<double>, double?>> res = state => ComputeStdev(state, false);
        public override Expression<Func<List<double>, double?>> ComputeResult() => res;
    }

    /// <summary>
    /// An aggregate that computes the population standard deviation
    /// </summary>
    internal sealed class PopulationStandardDeviationDouble : StatisticalAggregate
    {
        private static readonly Expression<Func<List<double>, double?>> res = state => ComputeStdev(state, true);
        public override Expression<Func<List<double>, double?>> ComputeResult() => res;
    }

    /// <summary>
    /// An aggregate that computes the sample variance
    /// </summary>
    internal sealed class VarianceDouble : StatisticalAggregate
    {
        private static readonly Expression<Func<List<double>, double?>> res = state => ComputeVariance(state, false);
        public override Expression<Func<List<double>, double?>> ComputeResult() => res;
    }

    /// <summary>
    /// An aggregate that computes the population variance
    /// </summary>
    internal sealed class PopulationVarianceDouble : StatisticalAggregate
    {
        private static readonly Expression<Func<List<double>, double?>> res = state => ComputeVariance(state, true);
        public override Expression<Func<List<double>, double?>> ComputeResult() => res;
    }
}
