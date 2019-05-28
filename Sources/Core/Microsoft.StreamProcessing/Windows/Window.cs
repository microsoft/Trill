// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Microsoft.StreamProcessing.Aggregates;
using Microsoft.StreamProcessing.Internal;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Class representing a window within an aggregation operation.
    /// </summary>
    public partial class Window<TKey, TSource>
    {
        /// <summary>
        /// The filter associated with the given window. Defaults to null.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Used to avoid creating redundant readonly property.")]
        protected readonly Expression<Func<TSource, bool>> Filter;
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Used to avoid creating redundant readonly property.")]
        internal readonly StreamProperties<TKey, TSource> Properties;

        internal Window(StreamProperties<TKey, TSource> properties)
        {
            this.Properties = properties;
            this.Filter = null;
        }

        /// <summary>
        /// Creates a new window representation with the given filter and properties.
        /// </summary>
        protected Window(Expression<Func<TSource, bool>> filter, StreamProperties<TKey, TSource> properties)
        {
            Invariant.IsNotNull(filter, nameof(filter));
            this.Filter = filter;
            this.Properties = properties;
        }

        /// <summary>
        /// Filter input rows with the specified filter.
        /// </summary>
        public FilteredWindow<TKey, TSource> Where(Expression<Func<TSource, bool>> predicate)
        {
            Invariant.IsNotNull(predicate, nameof(predicate));
            if (this.Filter == null) return new FilteredWindow<TKey, TSource>(predicate, this.Properties);
            Expression<Func<TSource, bool>> andedExpressionTemplate =
                input => CallInliner.Call(this.Filter, input) && CallInliner.Call(predicate, input);
            Expression<Func<TSource, bool>> andedExpression = andedExpressionTemplate.InlineCalls();
            return new FilteredWindow<TKey, TSource>(andedExpression, this.Properties);
        }

        /// <summary>
        /// Computes a time-sensitive count aggregate using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, ulong, ulong> Count()
        {
            var aggregate = new CountAggregate<TSource>();
            return aggregate.ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive single-or-default aggregate using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, SortedMultiSet<TSource>, TSource> SingleOrDefault()
        {
            var aggregate = new SingleOrDefaultAggregate<TSource>(this.Properties.QueryContainer);
            return aggregate.ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive count aggregate of the non-null values using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, ulong, ulong> CountNotNull<TValue>(Expression<Func<TSource, TValue>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new CountAggregate<TValue>();
            return aggregate.SkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive minimum aggregate using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, MinMaxState<TValue>, TValue> Min<TValue>(Expression<Func<TSource, TValue>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));

            var aggregate = this.Properties.IsTumbling
                ? new TumblingMinAggregate<TValue>()
                : this.Properties.IsConstantDuration
                    ? new SlidingMinAggregate<TValue>(this.Properties.QueryContainer)
                    : (IAggregate<TValue, MinMaxState<TValue>, TValue>)new MinAggregate<TValue>(this.Properties.QueryContainer);

            return aggregate.SkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive minimum aggregate using snapshot semantics with the provided ordering comparer.
        /// </summary>
        public IAggregate<TSource, MinMaxState<TValue>, TValue> Min<TValue>(
            Expression<Func<TSource, TValue>> selector, IComparerExpression<TValue> comparer)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            Invariant.IsNotNull(comparer, nameof(comparer));

            var aggregate = this.Properties.IsTumbling
                ? new TumblingMinAggregate<TValue>(comparer)
                : this.Properties.IsConstantDuration
                    ? new SlidingMinAggregate<TValue>(comparer, this.Properties.QueryContainer)
                    : (IAggregate<TValue, MinMaxState<TValue>, TValue>)new MinAggregate<TValue>(comparer, this.Properties.QueryContainer);

            return aggregate.SkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive minimum aggregate using snapshot semantics with the provided ordering comparer.
        /// </summary>
        public IAggregate<TSource, MinMaxState<TValue>, TValue> Min<TValue>(Expression<Func<TSource, TValue>> selector, Expression<Comparison<TValue>> comparer)
            => Min(selector, new ComparerExpression<TValue>(comparer));

        /// <summary>
        /// Computes a time-sensitive maximum aggregate using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, MinMaxState<TValue>, TValue> Max<TValue>(Expression<Func<TSource, TValue>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));

            var aggregate = this.Properties.IsTumbling
                ? new TumblingMaxAggregate<TValue>()
                : this.Properties.IsConstantDuration
                    ? new SlidingMaxAggregate<TValue>(this.Properties.QueryContainer)
                    : (IAggregate<TValue, MinMaxState<TValue>, TValue>)new MaxAggregate<TValue>(this.Properties.QueryContainer);

            return aggregate.SkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive maximum aggregate using snapshot semantics with the provided ordering comparer.
        /// </summary>
        public IAggregate<TSource, MinMaxState<TValue>, TValue> Max<TValue>(Expression<Func<TSource, TValue>> selector, IComparerExpression<TValue> comparer)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            Invariant.IsNotNull(comparer, nameof(comparer));

            var aggregate = this.Properties.IsTumbling
                ? new TumblingMaxAggregate<TValue>(comparer)
                : this.Properties.IsConstantDuration
                    ? new SlidingMaxAggregate<TValue>(comparer, this.Properties.QueryContainer)
                    : (IAggregate<TValue, MinMaxState<TValue>, TValue>)new MaxAggregate<TValue>(comparer, this.Properties.QueryContainer);

            return aggregate.SkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive maximum aggregate using snapshot semantics with the provided ordering comparer.
        /// </summary>
        public IAggregate<TSource, MinMaxState<TValue>, TValue> Max<TValue>(Expression<Func<TSource, TValue>> selector, Expression<Comparison<TValue>> comparer)
            => Max(selector, new ComparerExpression<TValue>(comparer));

        /// <summary>
        /// Computes a time-sensitive top-k aggregate using snapshot semantics based on a key selector.
        /// </summary>
        public IAggregate<TSource, SortedMultiSet<TSource>, List<RankedEvent<TSource>>> TopK<TOrderValue>(Expression<Func<TSource, TOrderValue>> orderer, int k)
        {
            Invariant.IsNotNull(orderer, nameof(orderer));
            Invariant.IsPositive(k, nameof(k));
            var orderComparer = ComparerExpression<TOrderValue>.Default.TransformInput(orderer);
            var aggregate = new TopKAggregate<TSource>(k, orderComparer, this.Properties.QueryContainer);
            return aggregate.SkipNulls().ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive top-k aggregate using snapshot semantics based on a key selector with the provided ordering comparer.
        /// </summary>
        public IAggregate<TSource, SortedMultiSet<TSource>, List<RankedEvent<TSource>>> TopK<TOrderValue>(Expression<Func<TSource, TOrderValue>> orderer, IComparerExpression<TOrderValue> comparer, int k)
        {
            Invariant.IsNotNull(orderer, nameof(orderer));
            Invariant.IsNotNull(comparer, nameof(comparer));
            Invariant.IsPositive(k, nameof(k));
            var orderComparer = comparer.TransformInput(orderer);
            var aggregate = new TopKAggregate<TSource>(k, orderComparer, this.Properties.QueryContainer);
            return aggregate.SkipNulls().ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a percentile continuous aggregate.
        /// </summary>
        public IAggregate<TSource, SortedMultiSet<double>, double> PercentileContinuous(
            double percentile,
            Expression<Func<TSource, double>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            Invariant.IsTrue(percentile >= 0.0 && percentile <= 1.0, "percentile must be within [0.0 .. 1.0].");
            var aggregate = new PercentileContinuousDoubleAggregate(percentile, this.Properties.QueryContainer);
            return aggregate.SkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a percentile continuous aggregate.
        /// </summary>
        public IAggregate<TSource, SortedMultiSet<double>, double> PercentileContinuous(
            Expression<Comparison<double>> comparer,
            double percentile,
            Expression<Func<TSource, double>> selector)
        {
            Invariant.IsNotNull(comparer, nameof(comparer));
            Invariant.IsNotNull(selector, nameof(selector));
            Invariant.IsTrue(percentile >= 0.0 && percentile <= 1.0, "percentile must be within [0.0 .. 1.0].");
            var aggregate = new PercentileContinuousDoubleAggregate(percentile, new ComparerExpression<double>(comparer), this.Properties.QueryContainer);
            return aggregate.SkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a percentile discrete aggregate.
        /// </summary>
        public IAggregate<TSource, SortedMultiSet<double>, double> PercentileDiscrete(
            double percentile,
            Expression<Func<TSource, double>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            Invariant.IsTrue(percentile >= 0.0 && percentile <= 1.0, "percentile must be within [0.0 .. 1.0].");
            var aggregate = new PercentileDiscreteDoubleAggregate(percentile, this.Properties.QueryContainer);
            return aggregate.SkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a percentile discrete aggregate.
        /// </summary>
        public IAggregate<TSource, SortedMultiSet<double>, double> PercentileDiscrete(
            Expression<Comparison<double>> comparer,
            double percentile,
            Expression<Func<TSource, double>> selector)
        {
            Invariant.IsNotNull(comparer, nameof(comparer));
            Invariant.IsNotNull(selector, nameof(selector));
            Invariant.IsTrue(percentile >= 0.0 && percentile <= 1.0, "percentile must be within [0.0 .. 1.0].");
            var aggregate = new PercentileDiscreteDoubleAggregate(percentile, new ComparerExpression<double>(comparer), this.Properties.QueryContainer);
            return aggregate.SkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes the sample standard deviation of the elements in the window.
        /// </summary>
        public IAggregate<TSource, List<double>, double?> StandardDeviation(
            Expression<Func<TSource, double?>> selector)
            => new StandardDeviationDouble().MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);

        /// <summary>
        /// Computes the sample standard deviation of the elements in the window.
        /// </summary>
        public IAggregate<TSource, List<double>, double?> StandardDeviation(
            Expression<Func<TSource, long?>> selector)
            => new StandardDeviationDouble().MakeInputNullableAndSkipNulls()
                .TransformInput<long?, double?, List<double>, double?>(e => e)
                .TransformInput(selector).ApplyFilter(this.Filter);

        /// <summary>
        /// Computes the population standard deviation of the elements in the window.
        /// </summary>
        public IAggregate<TSource, List<double>, double?> PopulationStandardDeviation(
            Expression<Func<TSource, double?>> selector)
            => new PopulationStandardDeviationDouble().MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);

        /// <summary>
        /// Computes the population standard deviation of the elements in the window.
        /// </summary>
        public IAggregate<TSource, List<double>, double?> PopulationStandardDeviation(
            Expression<Func<TSource, long?>> selector)
            => new PopulationStandardDeviationDouble().MakeInputNullableAndSkipNulls()
                .TransformInput<long?, double?, List<double>, double?>(e => e)
                .TransformInput(selector).ApplyFilter(this.Filter);

        /// <summary>
        /// Computes the sample variance of the elements in the window.
        /// </summary>
        public IAggregate<TSource, List<double>, double?> Variance(
            Expression<Func<TSource, double?>> selector)
            => new VarianceDouble().MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);

        /// <summary>
        /// Computes the sample variance of the elements in the window.
        /// </summary>
        public IAggregate<TSource, List<double>, double?> Variance(
            Expression<Func<TSource, long?>> selector)
            => new VarianceDouble().MakeInputNullableAndSkipNulls()
                .TransformInput<long?, double?, List<double>, double?>(e => e)
                .TransformInput(selector).ApplyFilter(this.Filter);

        /// <summary>
        /// Computes the population variance of the elements in the window.
        /// </summary>
        public IAggregate<TSource, List<double>, double?> PopulationVariance(
            Expression<Func<TSource, double?>> selector)
            => new PopulationVarianceDouble().MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);

        /// <summary>
        /// Computes the population variance of the elements in the window.
        /// </summary>
        public IAggregate<TSource, List<double>, double?> PopulationVariance(
            Expression<Func<TSource, long?>> selector)
            => new PopulationVarianceDouble().MakeInputNullableAndSkipNulls()
                .TransformInput<long?, double?, List<double>, double?>(e => e)
                .TransformInput(selector).ApplyFilter(this.Filter);
    }
}