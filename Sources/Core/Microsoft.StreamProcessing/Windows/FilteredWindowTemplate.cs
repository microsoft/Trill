// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************

using System;
using System.Linq.Expressions;
using System.Numerics;
using Microsoft.StreamProcessing.Aggregates;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Derived class from Window where additional semantics are applied when a filter is present
    /// </summary>
    /// <typeparam name="TKey">Grouping key type for input data</typeparam>
    /// <typeparam name="TSource">Event payload type for input data</typeparam>
    public sealed class FilteredWindow<TKey, TSource> : Window<TKey, TSource>
    {
        internal FilteredWindow(Expression<Func<TSource, bool>> filter, StreamProperties<TKey, TSource> properties)
            : base(filter, properties)
        { }

        /// <summary>
        /// Computes a time-sensitive average aggregate over sbytes using snapshot semantics. Note that the accumulator
        /// internally is a long datatype.
        /// </summary>
        public new IAggregate<TSource, AverageState<long>, double?> Average(Expression<Func<TSource, sbyte>> selector)
        {
            Invariant.IsNotNull(selector, "selector");
            var aggregate = new AverageFilterableSByteAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over shorts using snapshot semantics. Note that the accumulator
        /// internally is a long datatype.
        /// </summary>
        public new IAggregate<TSource, AverageState<long>, double?> Average(Expression<Func<TSource, short>> selector)
        {
            Invariant.IsNotNull(selector, "selector");
            var aggregate = new AverageFilterableShortAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over ints using snapshot semantics. Note that the accumulator
        /// internally is a long datatype.
        /// </summary>
        public new IAggregate<TSource, AverageState<long>, double?> Average(Expression<Func<TSource, int>> selector)
        {
            Invariant.IsNotNull(selector, "selector");
            var aggregate = new AverageFilterableIntAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over longs using snapshot semantics. Note that the accumulator
        /// internally is a long datatype.
        /// </summary>
        public new IAggregate<TSource, AverageState<long>, double?> Average(Expression<Func<TSource, long>> selector)
        {
            Invariant.IsNotNull(selector, "selector");
            var aggregate = new AverageFilterableLongAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over bytes using snapshot semantics. Note that the accumulator
        /// internally is a ulong datatype.
        /// </summary>
        public new IAggregate<TSource, AverageState<ulong>, double?> Average(Expression<Func<TSource, byte>> selector)
        {
            Invariant.IsNotNull(selector, "selector");
            var aggregate = new AverageFilterableByteAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over ushorts using snapshot semantics. Note that the accumulator
        /// internally is a ulong datatype.
        /// </summary>
        public new IAggregate<TSource, AverageState<ulong>, double?> Average(Expression<Func<TSource, ushort>> selector)
        {
            Invariant.IsNotNull(selector, "selector");
            var aggregate = new AverageFilterableUShortAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over uints using snapshot semantics. Note that the accumulator
        /// internally is a ulong datatype.
        /// </summary>
        public new IAggregate<TSource, AverageState<ulong>, double?> Average(Expression<Func<TSource, uint>> selector)
        {
            Invariant.IsNotNull(selector, "selector");
            var aggregate = new AverageFilterableUIntAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over ulongs using snapshot semantics. Note that the accumulator
        /// internally is a ulong datatype.
        /// </summary>
        public new IAggregate<TSource, AverageState<ulong>, double?> Average(Expression<Func<TSource, ulong>> selector)
        {
            Invariant.IsNotNull(selector, "selector");
            var aggregate = new AverageFilterableULongAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over floats using snapshot semantics. Note that the accumulator
        /// internally is a float datatype.
        /// </summary>
        public new IAggregate<TSource, AverageState<float>, float?> Average(Expression<Func<TSource, float>> selector)
        {
            Invariant.IsNotNull(selector, "selector");
            var aggregate = new AverageFilterableFloatAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over doubles using snapshot semantics. Note that the accumulator
        /// internally is a double datatype.
        /// </summary>
        public new IAggregate<TSource, AverageState<double>, double?> Average(Expression<Func<TSource, double>> selector)
        {
            Invariant.IsNotNull(selector, "selector");
            var aggregate = new AverageFilterableDoubleAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over decimals using snapshot semantics. Note that the accumulator
        /// internally is a decimal datatype.
        /// </summary>
        public new IAggregate<TSource, AverageState<decimal>, decimal?> Average(Expression<Func<TSource, decimal>> selector)
        {
            Invariant.IsNotNull(selector, "selector");
            var aggregate = new AverageFilterableDecimalAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over BigIntegers using snapshot semantics. Note that the accumulator
        /// internally is a BigInteger datatype.
        /// </summary>
        public new IAggregate<TSource, AverageState<BigInteger>, double?> Average(Expression<Func<TSource, BigInteger>> selector)
        {
            Invariant.IsNotNull(selector, "selector");
            var aggregate = new AverageFilterableBigIntegerAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over Complexs using snapshot semantics. Note that the accumulator
        /// internally is a Complex datatype.
        /// </summary>
        public new IAggregate<TSource, AverageState<Complex>, Complex?> Average(Expression<Func<TSource, Complex>> selector)
        {
            Invariant.IsNotNull(selector, "selector");
            var aggregate = new AverageFilterableComplexAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average squares aggregate over sbytes using snapshot semantics. Note that the accumulator
        /// internally is a long datatype.
        /// </summary>
        public new IAggregate<TSource, AverageState<long>, double?> AverageSquares(Expression<Func<TSource, sbyte>> selector)
        {
            Invariant.IsNotNull(selector, "selector");
            var aggregate = new AverageSquareFilterableSByteAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average squares aggregate over shorts using snapshot semantics. Note that the accumulator
        /// internally is a long datatype.
        /// </summary>
        public new IAggregate<TSource, AverageState<long>, double?> AverageSquares(Expression<Func<TSource, short>> selector)
        {
            Invariant.IsNotNull(selector, "selector");
            var aggregate = new AverageSquareFilterableShortAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average squares aggregate over ints using snapshot semantics. Note that the accumulator
        /// internally is a long datatype.
        /// </summary>
        public new IAggregate<TSource, AverageState<long>, double?> AverageSquares(Expression<Func<TSource, int>> selector)
        {
            Invariant.IsNotNull(selector, "selector");
            var aggregate = new AverageSquareFilterableIntAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average squares aggregate over longs using snapshot semantics. Note that the accumulator
        /// internally is a long datatype.
        /// </summary>
        public new IAggregate<TSource, AverageState<long>, double?> AverageSquares(Expression<Func<TSource, long>> selector)
        {
            Invariant.IsNotNull(selector, "selector");
            var aggregate = new AverageSquareFilterableLongAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average squares aggregate over bytes using snapshot semantics. Note that the accumulator
        /// internally is a ulong datatype.
        /// </summary>
        public new IAggregate<TSource, AverageState<ulong>, double?> AverageSquares(Expression<Func<TSource, byte>> selector)
        {
            Invariant.IsNotNull(selector, "selector");
            var aggregate = new AverageSquareFilterableByteAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average squares aggregate over ushorts using snapshot semantics. Note that the accumulator
        /// internally is a ulong datatype.
        /// </summary>
        public new IAggregate<TSource, AverageState<ulong>, double?> AverageSquares(Expression<Func<TSource, ushort>> selector)
        {
            Invariant.IsNotNull(selector, "selector");
            var aggregate = new AverageSquareFilterableUShortAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average squares aggregate over uints using snapshot semantics. Note that the accumulator
        /// internally is a ulong datatype.
        /// </summary>
        public new IAggregate<TSource, AverageState<ulong>, double?> AverageSquares(Expression<Func<TSource, uint>> selector)
        {
            Invariant.IsNotNull(selector, "selector");
            var aggregate = new AverageSquareFilterableUIntAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average squares aggregate over ulongs using snapshot semantics. Note that the accumulator
        /// internally is a ulong datatype.
        /// </summary>
        public new IAggregate<TSource, AverageState<ulong>, double?> AverageSquares(Expression<Func<TSource, ulong>> selector)
        {
            Invariant.IsNotNull(selector, "selector");
            var aggregate = new AverageSquareFilterableULongAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average squares aggregate over floats using snapshot semantics. Note that the accumulator
        /// internally is a float datatype.
        /// </summary>
        public new IAggregate<TSource, AverageState<float>, float?> AverageSquares(Expression<Func<TSource, float>> selector)
        {
            Invariant.IsNotNull(selector, "selector");
            var aggregate = new AverageSquareFilterableFloatAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average squares aggregate over doubles using snapshot semantics. Note that the accumulator
        /// internally is a double datatype.
        /// </summary>
        public new IAggregate<TSource, AverageState<double>, double?> AverageSquares(Expression<Func<TSource, double>> selector)
        {
            Invariant.IsNotNull(selector, "selector");
            var aggregate = new AverageSquareFilterableDoubleAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average squares aggregate over decimals using snapshot semantics. Note that the accumulator
        /// internally is a decimal datatype.
        /// </summary>
        public new IAggregate<TSource, AverageState<decimal>, decimal?> AverageSquares(Expression<Func<TSource, decimal>> selector)
        {
            Invariant.IsNotNull(selector, "selector");
            var aggregate = new AverageSquareFilterableDecimalAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average squares aggregate over BigIntegers using snapshot semantics. Note that the accumulator
        /// internally is a BigInteger datatype.
        /// </summary>
        public new IAggregate<TSource, AverageState<BigInteger>, double?> AverageSquares(Expression<Func<TSource, BigInteger>> selector)
        {
            Invariant.IsNotNull(selector, "selector");
            var aggregate = new AverageSquareFilterableBigIntegerAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average squares aggregate over Complexs using snapshot semantics. Note that the accumulator
        /// internally is a Complex datatype.
        /// </summary>
        public new IAggregate<TSource, AverageState<Complex>, Complex?> AverageSquares(Expression<Func<TSource, Complex>> selector)
        {
            Invariant.IsNotNull(selector, "selector");
            var aggregate = new AverageSquareFilterableComplexAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }
    }
}
