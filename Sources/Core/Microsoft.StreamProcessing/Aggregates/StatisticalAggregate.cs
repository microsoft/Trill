// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Numerics.Tensors;
using System.Runtime.InteropServices;

namespace Microsoft.StreamProcessing.Aggregates
{
    internal abstract class StatisticalAggregate : ListAggregateBase<double, double?>
    {
        /// <summary>At or below this size, a simple scalar loop avoids renting a scratch buffer for the second pass.</summary>
        internal const int VarianceScratchThreshold = 64;

        protected static double? ComputeStdev(List<double> valueList, bool useAsPopulation)
        {
            var variance = ComputeVariance(valueList, useAsPopulation);
            return variance.HasValue ? Math.Sqrt(variance.Value) : (double?)null;
        }

        protected static double? ComputeVariance(List<double> list, bool useAsPopulation)
        {
            if (list == null || list.Count == 0) return null;
            if (list.Count == 1) return useAsPopulation ? 0.0 : (double?)null;

            int n = list.Count;
            var divisor = useAsPopulation ? n : n - 1;
            ReadOnlySpan<double> span = CollectionsMarshal.AsSpan(list);

            // TensorPrimitives.Sum is SIMD-accelerated on supported hardware.
            // Mean is computed as Sum/n rather than Sum(x/n) per element; this is faster and
            // typically as accurate; for pathological magnitudes, per-element scaling avoids
            // intermediate overflow in the sum (trade-off: rare edge case vs. hot-path cost).
            double mean = TensorPrimitives.Sum(span) / n;

            double variance;
            if (n <= VarianceScratchThreshold)
            {
                variance = 0.0;
                for (int i = 0; i < n; i++)
                {
                    double d = span[i] - mean;
                    variance += (d * d) / divisor;
                }
            }
            else
            {
                double[] rented = ArrayPool<double>.Shared.Rent(n);
                try
                {
                    Span<double> scratch = rented.AsSpan(0, n);
                    TensorPrimitives.Subtract(span, mean, scratch);
                    TensorPrimitives.Multiply(scratch, scratch, scratch);
                    variance = TensorPrimitives.Sum(scratch) / divisor;
                }
                finally
                {
                    ArrayPool<double>.Shared.Return(rented);
                }
            }

            return double.IsInfinity(variance) ? null : variance;
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
