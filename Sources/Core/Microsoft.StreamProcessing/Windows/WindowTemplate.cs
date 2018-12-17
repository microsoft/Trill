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
    public partial class Window<TKey, TSource>
    {

        /// <summary>
        /// Computes a time-sensitive sum aggregate over sbytes using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, sbyte, sbyte> Sum(Expression<Func<TSource, sbyte>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSByteAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over bytes using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, byte, byte> Sum(Expression<Func<TSource, byte>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumByteAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over shorts using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, short, short> Sum(Expression<Func<TSource, short>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumShortAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over ushorts using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, ushort, ushort> Sum(Expression<Func<TSource, ushort>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumUShortAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over ints using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, int, int> Sum(Expression<Func<TSource, int>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumIntAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over uints using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, uint, uint> Sum(Expression<Func<TSource, uint>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumUIntAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over longs using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, long, long> Sum(Expression<Func<TSource, long>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumLongAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over ulongs using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, ulong, ulong> Sum(Expression<Func<TSource, ulong>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumULongAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over floats using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, float, float> Sum(Expression<Func<TSource, float>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumFloatAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over doubles using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, double, double> Sum(Expression<Func<TSource, double>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumDoubleAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over decimals using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, decimal, decimal> Sum(Expression<Func<TSource, decimal>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumDecimalAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over BigIntegers using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, BigInteger, BigInteger> Sum(Expression<Func<TSource, BigInteger>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumBigIntegerAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over Complexs using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, Complex, Complex> Sum(Expression<Func<TSource, Complex>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumComplexAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable sbytes using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, sbyte, sbyte> Sum(Expression<Func<TSource, sbyte?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSByteAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable bytes using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, byte, byte> Sum(Expression<Func<TSource, byte?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumByteAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable shorts using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, short, short> Sum(Expression<Func<TSource, short?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumShortAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable ushorts using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, ushort, ushort> Sum(Expression<Func<TSource, ushort?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumUShortAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable ints using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, int, int> Sum(Expression<Func<TSource, int?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumIntAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable uints using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, uint, uint> Sum(Expression<Func<TSource, uint?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumUIntAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable longs using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, long, long> Sum(Expression<Func<TSource, long?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumLongAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable ulongs using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, ulong, ulong> Sum(Expression<Func<TSource, ulong?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumULongAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable floats using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, float, float> Sum(Expression<Func<TSource, float?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumFloatAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable doubles using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, double, double> Sum(Expression<Func<TSource, double?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumDoubleAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable decimals using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, decimal, decimal> Sum(Expression<Func<TSource, decimal?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumDecimalAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable BigIntegers using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, BigInteger, BigInteger> Sum(Expression<Func<TSource, BigInteger?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumBigIntegerAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable Complexs using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, Complex, Complex> Sum(Expression<Func<TSource, Complex?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumComplexAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum-squares aggregate over sbytes using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, sbyte, sbyte> SumSquares(Expression<Func<TSource, sbyte>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSquareSByteAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum-squares aggregate over bytes using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, byte, byte> SumSquares(Expression<Func<TSource, byte>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSquareByteAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum-squares aggregate over shorts using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, short, short> SumSquares(Expression<Func<TSource, short>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSquareShortAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum-squares aggregate over ushorts using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, ushort, ushort> SumSquares(Expression<Func<TSource, ushort>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSquareUShortAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum-squares aggregate over ints using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, int, int> SumSquares(Expression<Func<TSource, int>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSquareIntAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum-squares aggregate over uints using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, uint, uint> SumSquares(Expression<Func<TSource, uint>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSquareUIntAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum-squares aggregate over longs using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, long, long> SumSquares(Expression<Func<TSource, long>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSquareLongAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum-squares aggregate over ulongs using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, ulong, ulong> SumSquares(Expression<Func<TSource, ulong>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSquareULongAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum-squares aggregate over floats using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, float, float> SumSquares(Expression<Func<TSource, float>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSquareFloatAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum-squares aggregate over doubles using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, double, double> SumSquares(Expression<Func<TSource, double>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSquareDoubleAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum-squares aggregate over decimals using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, decimal, decimal> SumSquares(Expression<Func<TSource, decimal>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSquareDecimalAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum-squares aggregate over BigIntegers using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, BigInteger, BigInteger> SumSquares(Expression<Func<TSource, BigInteger>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSquareBigIntegerAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum-squares aggregate over Complexs using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, Complex, Complex> SumSquares(Expression<Func<TSource, Complex>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSquareComplexAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum-squares aggregate over nullable sbytes using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, sbyte, sbyte> SumSquares(Expression<Func<TSource, sbyte?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSquareSByteAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum-squares aggregate over nullable bytes using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, byte, byte> SumSquares(Expression<Func<TSource, byte?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSquareByteAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum-squares aggregate over nullable shorts using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, short, short> SumSquares(Expression<Func<TSource, short?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSquareShortAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum-squares aggregate over nullable ushorts using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, ushort, ushort> SumSquares(Expression<Func<TSource, ushort?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSquareUShortAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum-squares aggregate over nullable ints using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, int, int> SumSquares(Expression<Func<TSource, int?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSquareIntAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum-squares aggregate over nullable uints using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, uint, uint> SumSquares(Expression<Func<TSource, uint?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSquareUIntAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum-squares aggregate over nullable longs using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, long, long> SumSquares(Expression<Func<TSource, long?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSquareLongAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum-squares aggregate over nullable ulongs using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, ulong, ulong> SumSquares(Expression<Func<TSource, ulong?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSquareULongAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum-squares aggregate over nullable floats using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, float, float> SumSquares(Expression<Func<TSource, float?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSquareFloatAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum-squares aggregate over nullable doubles using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, double, double> SumSquares(Expression<Func<TSource, double?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSquareDoubleAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum-squares aggregate over nullable decimals using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, decimal, decimal> SumSquares(Expression<Func<TSource, decimal?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSquareDecimalAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum-squares aggregate over nullable BigIntegers using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, BigInteger, BigInteger> SumSquares(Expression<Func<TSource, BigInteger?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSquareBigIntegerAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum-squares aggregate over nullable Complexs using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, Complex, Complex> SumSquares(Expression<Func<TSource, Complex?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new SumSquareComplexAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over sbytes using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, sbyte, sbyte> Product(Expression<Func<TSource, sbyte>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new ProductSByteAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over bytes using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, byte, byte> Product(Expression<Func<TSource, byte>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new ProductByteAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over shorts using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, short, short> Product(Expression<Func<TSource, short>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new ProductShortAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over ushorts using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, ushort, ushort> Product(Expression<Func<TSource, ushort>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new ProductUShortAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over ints using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, int, int> Product(Expression<Func<TSource, int>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new ProductIntAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over uints using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, uint, uint> Product(Expression<Func<TSource, uint>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new ProductUIntAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over longs using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, long, long> Product(Expression<Func<TSource, long>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new ProductLongAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over ulongs using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, ulong, ulong> Product(Expression<Func<TSource, ulong>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new ProductULongAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over floats using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, float, float> Product(Expression<Func<TSource, float>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new ProductFloatAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over doubles using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, double, double> Product(Expression<Func<TSource, double>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new ProductDoubleAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over decimals using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, decimal, decimal> Product(Expression<Func<TSource, decimal>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new ProductDecimalAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over BigIntegers using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, BigInteger, BigInteger> Product(Expression<Func<TSource, BigInteger>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new ProductBigIntegerAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over Complexs using snapshot semantics.
        /// </summary>
        public IAggregate<TSource, Complex, Complex> Product(Expression<Func<TSource, Complex>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new ProductComplexAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable sbytes using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, sbyte, sbyte> Product(Expression<Func<TSource, sbyte?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new ProductSByteAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable bytes using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, byte, byte> Product(Expression<Func<TSource, byte?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new ProductByteAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable shorts using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, short, short> Product(Expression<Func<TSource, short?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new ProductShortAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable ushorts using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, ushort, ushort> Product(Expression<Func<TSource, ushort?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new ProductUShortAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable ints using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, int, int> Product(Expression<Func<TSource, int?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new ProductIntAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable uints using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, uint, uint> Product(Expression<Func<TSource, uint?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new ProductUIntAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable longs using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, long, long> Product(Expression<Func<TSource, long?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new ProductLongAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable ulongs using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, ulong, ulong> Product(Expression<Func<TSource, ulong?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new ProductULongAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable floats using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, float, float> Product(Expression<Func<TSource, float?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new ProductFloatAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable doubles using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, double, double> Product(Expression<Func<TSource, double?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new ProductDoubleAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable decimals using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, decimal, decimal> Product(Expression<Func<TSource, decimal?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new ProductDecimalAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable BigIntegers using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, BigInteger, BigInteger> Product(Expression<Func<TSource, BigInteger?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new ProductBigIntegerAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable Complexs using snapshot semantics. Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public IAggregate<TSource, Complex, Complex> Product(Expression<Func<TSource, Complex?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new ProductComplexAggregate();
            return aggregate.MakeInputNullableAndSkipNulls().Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over sbytes using snapshot semantics. Note that the accumulator
        /// internally is a long datatype.
        /// </summary>
        public IAggregate<TSource, AverageState<long>, double> Average(Expression<Func<TSource, sbyte>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageSByteAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over shorts using snapshot semantics. Note that the accumulator
        /// internally is a long datatype.
        /// </summary>
        public IAggregate<TSource, AverageState<long>, double> Average(Expression<Func<TSource, short>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageShortAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over ints using snapshot semantics. Note that the accumulator
        /// internally is a long datatype.
        /// </summary>
        public IAggregate<TSource, AverageState<long>, double> Average(Expression<Func<TSource, int>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageIntAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over longs using snapshot semantics. Note that the accumulator
        /// internally is a long datatype.
        /// </summary>
        public IAggregate<TSource, AverageState<long>, double> Average(Expression<Func<TSource, long>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageLongAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over bytes using snapshot semantics. Note that the accumulator
        /// internally is a ulong datatype.
        /// </summary>
        public IAggregate<TSource, AverageState<ulong>, double> Average(Expression<Func<TSource, byte>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageByteAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over ushorts using snapshot semantics. Note that the accumulator
        /// internally is a ulong datatype.
        /// </summary>
        public IAggregate<TSource, AverageState<ulong>, double> Average(Expression<Func<TSource, ushort>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageUShortAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over uints using snapshot semantics. Note that the accumulator
        /// internally is a ulong datatype.
        /// </summary>
        public IAggregate<TSource, AverageState<ulong>, double> Average(Expression<Func<TSource, uint>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageUIntAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over ulongs using snapshot semantics. Note that the accumulator
        /// internally is a ulong datatype.
        /// </summary>
        public IAggregate<TSource, AverageState<ulong>, double> Average(Expression<Func<TSource, ulong>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageULongAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over floats using snapshot semantics. Note that the accumulator
        /// internally is a float datatype.
        /// </summary>
        public IAggregate<TSource, AverageState<float>, float> Average(Expression<Func<TSource, float>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageFloatAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over doubles using snapshot semantics. Note that the accumulator
        /// internally is a double datatype.
        /// </summary>
        public IAggregate<TSource, AverageState<double>, double> Average(Expression<Func<TSource, double>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageDoubleAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over decimals using snapshot semantics. Note that the accumulator
        /// internally is a decimal datatype.
        /// </summary>
        public IAggregate<TSource, AverageState<decimal>, decimal> Average(Expression<Func<TSource, decimal>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageDecimalAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over BigIntegers using snapshot semantics. Note that the accumulator
        /// internally is a BigInteger datatype.
        /// </summary>
        public IAggregate<TSource, AverageState<BigInteger>, double> Average(Expression<Func<TSource, BigInteger>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageBigIntegerAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over Complexs using snapshot semantics. Note that the accumulator
        /// internally is a Complex datatype.
        /// </summary>
        public IAggregate<TSource, AverageState<Complex>, Complex> Average(Expression<Func<TSource, Complex>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageComplexAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable sbytes using snapshot semantics. Note that the accumulator
        /// internally is a long datatype and that nulls have no affect on the average.
        /// </summary>
        public IAggregate<TSource, AverageState<long>, double?> Average(Expression<Func<TSource, sbyte?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageNullableSByteAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable shorts using snapshot semantics. Note that the accumulator
        /// internally is a long datatype and that nulls have no affect on the average.
        /// </summary>
        public IAggregate<TSource, AverageState<long>, double?> Average(Expression<Func<TSource, short?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageNullableShortAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable ints using snapshot semantics. Note that the accumulator
        /// internally is a long datatype and that nulls have no affect on the average.
        /// </summary>
        public IAggregate<TSource, AverageState<long>, double?> Average(Expression<Func<TSource, int?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageNullableIntAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable longs using snapshot semantics. Note that the accumulator
        /// internally is a long datatype and that nulls have no affect on the average.
        /// </summary>
        public IAggregate<TSource, AverageState<long>, double?> Average(Expression<Func<TSource, long?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageNullableLongAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable bytes using snapshot semantics. Note that the accumulator
        /// internally is a ulong datatype and that nulls have no affect on the average.
        /// </summary>
        public IAggregate<TSource, AverageState<ulong>, double?> Average(Expression<Func<TSource, byte?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageNullableByteAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable ushorts using snapshot semantics. Note that the accumulator
        /// internally is a ulong datatype and that nulls have no affect on the average.
        /// </summary>
        public IAggregate<TSource, AverageState<ulong>, double?> Average(Expression<Func<TSource, ushort?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageNullableUShortAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable uints using snapshot semantics. Note that the accumulator
        /// internally is a ulong datatype and that nulls have no affect on the average.
        /// </summary>
        public IAggregate<TSource, AverageState<ulong>, double?> Average(Expression<Func<TSource, uint?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageNullableUIntAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable ulongs using snapshot semantics. Note that the accumulator
        /// internally is a ulong datatype and that nulls have no affect on the average.
        /// </summary>
        public IAggregate<TSource, AverageState<ulong>, double?> Average(Expression<Func<TSource, ulong?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageNullableULongAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable floats using snapshot semantics. Note that the accumulator
        /// internally is a float datatype and that nulls have no affect on the average.
        /// </summary>
        public IAggregate<TSource, AverageState<float>, float?> Average(Expression<Func<TSource, float?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageNullableFloatAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable doubles using snapshot semantics. Note that the accumulator
        /// internally is a double datatype and that nulls have no affect on the average.
        /// </summary>
        public IAggregate<TSource, AverageState<double>, double?> Average(Expression<Func<TSource, double?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageNullableDoubleAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable decimals using snapshot semantics. Note that the accumulator
        /// internally is a decimal datatype and that nulls have no affect on the average.
        /// </summary>
        public IAggregate<TSource, AverageState<decimal>, decimal?> Average(Expression<Func<TSource, decimal?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageNullableDecimalAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable BigIntegers using snapshot semantics. Note that the accumulator
        /// internally is a BigInteger datatype and that nulls have no affect on the average.
        /// </summary>
        public IAggregate<TSource, AverageState<BigInteger>, double?> Average(Expression<Func<TSource, BigInteger?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageNullableBigIntegerAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable Complexs using snapshot semantics. Note that the accumulator
        /// internally is a Complex datatype and that nulls have no affect on the average.
        /// </summary>
        public IAggregate<TSource, AverageState<Complex>, Complex?> Average(Expression<Func<TSource, Complex?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageNullableComplexAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average-squares aggregate over sbytes using snapshot semantics. Note that the accumulator
        /// internally is a long datatype.
        /// </summary>
        public IAggregate<TSource, AverageState<long>, double> AverageSquares(Expression<Func<TSource, sbyte>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageSquareSByteAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average-squares aggregate over shorts using snapshot semantics. Note that the accumulator
        /// internally is a long datatype.
        /// </summary>
        public IAggregate<TSource, AverageState<long>, double> AverageSquares(Expression<Func<TSource, short>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageSquareShortAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average-squares aggregate over ints using snapshot semantics. Note that the accumulator
        /// internally is a long datatype.
        /// </summary>
        public IAggregate<TSource, AverageState<long>, double> AverageSquares(Expression<Func<TSource, int>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageSquareIntAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average-squares aggregate over longs using snapshot semantics. Note that the accumulator
        /// internally is a long datatype.
        /// </summary>
        public IAggregate<TSource, AverageState<long>, double> AverageSquares(Expression<Func<TSource, long>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageSquareLongAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average-squares aggregate over bytes using snapshot semantics. Note that the accumulator
        /// internally is a ulong datatype.
        /// </summary>
        public IAggregate<TSource, AverageState<ulong>, double> AverageSquares(Expression<Func<TSource, byte>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageSquareByteAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average-squares aggregate over ushorts using snapshot semantics. Note that the accumulator
        /// internally is a ulong datatype.
        /// </summary>
        public IAggregate<TSource, AverageState<ulong>, double> AverageSquares(Expression<Func<TSource, ushort>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageSquareUShortAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average-squares aggregate over uints using snapshot semantics. Note that the accumulator
        /// internally is a ulong datatype.
        /// </summary>
        public IAggregate<TSource, AverageState<ulong>, double> AverageSquares(Expression<Func<TSource, uint>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageSquareUIntAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average-squares aggregate over ulongs using snapshot semantics. Note that the accumulator
        /// internally is a ulong datatype.
        /// </summary>
        public IAggregate<TSource, AverageState<ulong>, double> AverageSquares(Expression<Func<TSource, ulong>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageSquareULongAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average-squares aggregate over floats using snapshot semantics. Note that the accumulator
        /// internally is a float datatype.
        /// </summary>
        public IAggregate<TSource, AverageState<float>, float> AverageSquares(Expression<Func<TSource, float>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageSquareFloatAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average-squares aggregate over doubles using snapshot semantics. Note that the accumulator
        /// internally is a double datatype.
        /// </summary>
        public IAggregate<TSource, AverageState<double>, double> AverageSquares(Expression<Func<TSource, double>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageSquareDoubleAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average-squares aggregate over decimals using snapshot semantics. Note that the accumulator
        /// internally is a decimal datatype.
        /// </summary>
        public IAggregate<TSource, AverageState<decimal>, decimal> AverageSquares(Expression<Func<TSource, decimal>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageSquareDecimalAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average-squares aggregate over BigIntegers using snapshot semantics. Note that the accumulator
        /// internally is a BigInteger datatype.
        /// </summary>
        public IAggregate<TSource, AverageState<BigInteger>, double> AverageSquares(Expression<Func<TSource, BigInteger>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageSquareBigIntegerAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average-squares aggregate over Complexs using snapshot semantics. Note that the accumulator
        /// internally is a Complex datatype.
        /// </summary>
        public IAggregate<TSource, AverageState<Complex>, Complex> AverageSquares(Expression<Func<TSource, Complex>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageSquareComplexAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average-squares aggregate over nullable sbytes using snapshot semantics. Note that the accumulator
        /// internally is a long datatype and that nulls have no affect on the average.
        /// </summary>
        public IAggregate<TSource, AverageState<long>, double?> AverageSquares(Expression<Func<TSource, sbyte?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageSquareNullableSByteAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average-squares aggregate over nullable shorts using snapshot semantics. Note that the accumulator
        /// internally is a long datatype and that nulls have no affect on the average.
        /// </summary>
        public IAggregate<TSource, AverageState<long>, double?> AverageSquares(Expression<Func<TSource, short?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageSquareNullableShortAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average-squares aggregate over nullable ints using snapshot semantics. Note that the accumulator
        /// internally is a long datatype and that nulls have no affect on the average.
        /// </summary>
        public IAggregate<TSource, AverageState<long>, double?> AverageSquares(Expression<Func<TSource, int?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageSquareNullableIntAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average-squares aggregate over nullable longs using snapshot semantics. Note that the accumulator
        /// internally is a long datatype and that nulls have no affect on the average.
        /// </summary>
        public IAggregate<TSource, AverageState<long>, double?> AverageSquares(Expression<Func<TSource, long?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageSquareNullableLongAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average-squares aggregate over nullable bytes using snapshot semantics. Note that the accumulator
        /// internally is a ulong datatype and that nulls have no affect on the average.
        /// </summary>
        public IAggregate<TSource, AverageState<ulong>, double?> AverageSquares(Expression<Func<TSource, byte?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageSquareNullableByteAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average-squares aggregate over nullable ushorts using snapshot semantics. Note that the accumulator
        /// internally is a ulong datatype and that nulls have no affect on the average.
        /// </summary>
        public IAggregate<TSource, AverageState<ulong>, double?> AverageSquares(Expression<Func<TSource, ushort?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageSquareNullableUShortAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average-squares aggregate over nullable uints using snapshot semantics. Note that the accumulator
        /// internally is a ulong datatype and that nulls have no affect on the average.
        /// </summary>
        public IAggregate<TSource, AverageState<ulong>, double?> AverageSquares(Expression<Func<TSource, uint?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageSquareNullableUIntAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average-squares aggregate over nullable ulongs using snapshot semantics. Note that the accumulator
        /// internally is a ulong datatype and that nulls have no affect on the average.
        /// </summary>
        public IAggregate<TSource, AverageState<ulong>, double?> AverageSquares(Expression<Func<TSource, ulong?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageSquareNullableULongAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average-squares aggregate over nullable floats using snapshot semantics. Note that the accumulator
        /// internally is a float datatype and that nulls have no affect on the average.
        /// </summary>
        public IAggregate<TSource, AverageState<float>, float?> AverageSquares(Expression<Func<TSource, float?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageSquareNullableFloatAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average-squares aggregate over nullable doubles using snapshot semantics. Note that the accumulator
        /// internally is a double datatype and that nulls have no affect on the average.
        /// </summary>
        public IAggregate<TSource, AverageState<double>, double?> AverageSquares(Expression<Func<TSource, double?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageSquareNullableDoubleAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average-squares aggregate over nullable decimals using snapshot semantics. Note that the accumulator
        /// internally is a decimal datatype and that nulls have no affect on the average.
        /// </summary>
        public IAggregate<TSource, AverageState<decimal>, decimal?> AverageSquares(Expression<Func<TSource, decimal?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageSquareNullableDecimalAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average-squares aggregate over nullable BigIntegers using snapshot semantics. Note that the accumulator
        /// internally is a BigInteger datatype and that nulls have no affect on the average.
        /// </summary>
        public IAggregate<TSource, AverageState<BigInteger>, double?> AverageSquares(Expression<Func<TSource, BigInteger?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageSquareNullableBigIntegerAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }

        /// <summary>
        /// Computes a time-sensitive average-squares aggregate over nullable Complexs using snapshot semantics. Note that the accumulator
        /// internally is a Complex datatype and that nulls have no affect on the average.
        /// </summary>
        public IAggregate<TSource, AverageState<Complex>, Complex?> AverageSquares(Expression<Func<TSource, Complex?>> selector)
        {
            Invariant.IsNotNull(selector, nameof(selector));
            var aggregate = new AverageSquareNullableComplexAggregate();
            return aggregate.Wrap(selector).ApplyFilter(this.Filter);
        }
    }
}
