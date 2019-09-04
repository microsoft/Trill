// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Numerics;

namespace Microsoft.StreamProcessing.Aggregates
{

    internal sealed class ProductSByteAggregate : IAggregate<sbyte, sbyte, sbyte>
    {
        public Expression<Func<sbyte>> InitialState() => () => 1;

        public Expression<Func<sbyte, long, sbyte, sbyte>> Accumulate()
            => (oldProduct, timestamp, input) => (sbyte)(oldProduct * input);

        public Expression<Func<sbyte, long, sbyte, sbyte>> Deaccumulate()
            => (oldProduct, timestamp, input) => (sbyte)(oldProduct / input);

        public Expression<Func<sbyte, sbyte, sbyte>> Difference()
            => (leftProduct, rightProduct) => (sbyte)(leftProduct / rightProduct);

        public Expression<Func<sbyte, sbyte, sbyte>> Sum()
            => (leftProduct, rightProduct) => (sbyte)(leftProduct * rightProduct);

        public Expression<Func<sbyte, sbyte>> ComputeResult() => product => product;
    }

    internal sealed class ProductByteAggregate : IAggregate<byte, byte, byte>
    {
        public Expression<Func<byte>> InitialState() => () => 1;

        public Expression<Func<byte, long, byte, byte>> Accumulate()
            => (oldProduct, timestamp, input) => (byte)(oldProduct * input);

        public Expression<Func<byte, long, byte, byte>> Deaccumulate()
            => (oldProduct, timestamp, input) => (byte)(oldProduct / input);

        public Expression<Func<byte, byte, byte>> Difference()
            => (leftProduct, rightProduct) => (byte)(leftProduct / rightProduct);

        public Expression<Func<byte, byte, byte>> Sum()
            => (leftProduct, rightProduct) => (byte)(leftProduct * rightProduct);

        public Expression<Func<byte, byte>> ComputeResult() => product => product;
    }

    internal sealed class ProductShortAggregate : IAggregate<short, short, short>
    {
        public Expression<Func<short>> InitialState() => () => 1;

        public Expression<Func<short, long, short, short>> Accumulate()
            => (oldProduct, timestamp, input) => (short)(oldProduct * input);

        public Expression<Func<short, long, short, short>> Deaccumulate()
            => (oldProduct, timestamp, input) => (short)(oldProduct / input);

        public Expression<Func<short, short, short>> Difference()
            => (leftProduct, rightProduct) => (short)(leftProduct / rightProduct);

        public Expression<Func<short, short, short>> Sum()
            => (leftProduct, rightProduct) => (short)(leftProduct * rightProduct);

        public Expression<Func<short, short>> ComputeResult() => product => product;
    }

    internal sealed class ProductUShortAggregate : IAggregate<ushort, ushort, ushort>
    {
        public Expression<Func<ushort>> InitialState() => () => 1;

        public Expression<Func<ushort, long, ushort, ushort>> Accumulate()
            => (oldProduct, timestamp, input) => (ushort)(oldProduct * input);

        public Expression<Func<ushort, long, ushort, ushort>> Deaccumulate()
            => (oldProduct, timestamp, input) => (ushort)(oldProduct / input);

        public Expression<Func<ushort, ushort, ushort>> Difference()
            => (leftProduct, rightProduct) => (ushort)(leftProduct / rightProduct);

        public Expression<Func<ushort, ushort, ushort>> Sum()
            => (leftProduct, rightProduct) => (ushort)(leftProduct * rightProduct);

        public Expression<Func<ushort, ushort>> ComputeResult() => product => product;
    }

    internal sealed class ProductIntAggregate : IAggregate<int, int, int>
    {
        public Expression<Func<int>> InitialState() => () => 1;

        public Expression<Func<int, long, int, int>> Accumulate()
            => (oldProduct, timestamp, input) => (int)(oldProduct * input);

        public Expression<Func<int, long, int, int>> Deaccumulate()
            => (oldProduct, timestamp, input) => (int)(oldProduct / input);

        public Expression<Func<int, int, int>> Difference()
            => (leftProduct, rightProduct) => (int)(leftProduct / rightProduct);

        public Expression<Func<int, int, int>> Sum()
            => (leftProduct, rightProduct) => (int)(leftProduct * rightProduct);

        public Expression<Func<int, int>> ComputeResult() => product => product;
    }

    internal sealed class ProductUIntAggregate : IAggregate<uint, uint, uint>
    {
        public Expression<Func<uint>> InitialState() => () => 1;

        public Expression<Func<uint, long, uint, uint>> Accumulate()
            => (oldProduct, timestamp, input) => (uint)(oldProduct * input);

        public Expression<Func<uint, long, uint, uint>> Deaccumulate()
            => (oldProduct, timestamp, input) => (uint)(oldProduct / input);

        public Expression<Func<uint, uint, uint>> Difference()
            => (leftProduct, rightProduct) => (uint)(leftProduct / rightProduct);

        public Expression<Func<uint, uint, uint>> Sum()
            => (leftProduct, rightProduct) => (uint)(leftProduct * rightProduct);

        public Expression<Func<uint, uint>> ComputeResult() => product => product;
    }

    internal sealed class ProductLongAggregate : IAggregate<long, long, long>
    {
        public Expression<Func<long>> InitialState() => () => 1;

        public Expression<Func<long, long, long, long>> Accumulate()
            => (oldProduct, timestamp, input) => (long)(oldProduct * input);

        public Expression<Func<long, long, long, long>> Deaccumulate()
            => (oldProduct, timestamp, input) => (long)(oldProduct / input);

        public Expression<Func<long, long, long>> Difference()
            => (leftProduct, rightProduct) => (long)(leftProduct / rightProduct);

        public Expression<Func<long, long, long>> Sum()
            => (leftProduct, rightProduct) => (long)(leftProduct * rightProduct);

        public Expression<Func<long, long>> ComputeResult() => product => product;
    }

    internal sealed class ProductULongAggregate : IAggregate<ulong, ulong, ulong>
    {
        public Expression<Func<ulong>> InitialState() => () => 1;

        public Expression<Func<ulong, long, ulong, ulong>> Accumulate()
            => (oldProduct, timestamp, input) => (ulong)(oldProduct * input);

        public Expression<Func<ulong, long, ulong, ulong>> Deaccumulate()
            => (oldProduct, timestamp, input) => (ulong)(oldProduct / input);

        public Expression<Func<ulong, ulong, ulong>> Difference()
            => (leftProduct, rightProduct) => (ulong)(leftProduct / rightProduct);

        public Expression<Func<ulong, ulong, ulong>> Sum()
            => (leftProduct, rightProduct) => (ulong)(leftProduct * rightProduct);

        public Expression<Func<ulong, ulong>> ComputeResult() => product => product;
    }

    internal sealed class ProductFloatAggregate : IAggregate<float, float, float>
    {
        public Expression<Func<float>> InitialState() => () => 1;

        public Expression<Func<float, long, float, float>> Accumulate()
            => (oldProduct, timestamp, input) => (float)(oldProduct * input);

        public Expression<Func<float, long, float, float>> Deaccumulate()
            => (oldProduct, timestamp, input) => (float)(oldProduct / input);

        public Expression<Func<float, float, float>> Difference()
            => (leftProduct, rightProduct) => (float)(leftProduct / rightProduct);

        public Expression<Func<float, float, float>> Sum()
            => (leftProduct, rightProduct) => (float)(leftProduct * rightProduct);

        public Expression<Func<float, float>> ComputeResult() => product => product;
    }

    internal sealed class ProductDoubleAggregate : IAggregate<double, double, double>
    {
        public Expression<Func<double>> InitialState() => () => 1;

        public Expression<Func<double, long, double, double>> Accumulate()
            => (oldProduct, timestamp, input) => (double)(oldProduct * input);

        public Expression<Func<double, long, double, double>> Deaccumulate()
            => (oldProduct, timestamp, input) => (double)(oldProduct / input);

        public Expression<Func<double, double, double>> Difference()
            => (leftProduct, rightProduct) => (double)(leftProduct / rightProduct);

        public Expression<Func<double, double, double>> Sum()
            => (leftProduct, rightProduct) => (double)(leftProduct * rightProduct);

        public Expression<Func<double, double>> ComputeResult() => product => product;
    }

    internal sealed class ProductDecimalAggregate : IAggregate<decimal, decimal, decimal>
    {
        public Expression<Func<decimal>> InitialState() => () => 1;

        public Expression<Func<decimal, long, decimal, decimal>> Accumulate()
            => (oldProduct, timestamp, input) => (decimal)(oldProduct * input);

        public Expression<Func<decimal, long, decimal, decimal>> Deaccumulate()
            => (oldProduct, timestamp, input) => (decimal)(oldProduct / input);

        public Expression<Func<decimal, decimal, decimal>> Difference()
            => (leftProduct, rightProduct) => (decimal)(leftProduct / rightProduct);

        public Expression<Func<decimal, decimal, decimal>> Sum()
            => (leftProduct, rightProduct) => (decimal)(leftProduct * rightProduct);

        public Expression<Func<decimal, decimal>> ComputeResult() => product => product;
    }

    internal sealed class ProductBigIntegerAggregate : IAggregate<BigInteger, BigInteger, BigInteger>
    {
        public Expression<Func<BigInteger>> InitialState() => () => 1;

        public Expression<Func<BigInteger, long, BigInteger, BigInteger>> Accumulate()
            => (oldProduct, timestamp, input) => (BigInteger)(oldProduct * input);

        public Expression<Func<BigInteger, long, BigInteger, BigInteger>> Deaccumulate()
            => (oldProduct, timestamp, input) => (BigInteger)(oldProduct / input);

        public Expression<Func<BigInteger, BigInteger, BigInteger>> Difference()
            => (leftProduct, rightProduct) => (BigInteger)(leftProduct / rightProduct);

        public Expression<Func<BigInteger, BigInteger, BigInteger>> Sum()
            => (leftProduct, rightProduct) => (BigInteger)(leftProduct * rightProduct);

        public Expression<Func<BigInteger, BigInteger>> ComputeResult() => product => product;
    }

    internal sealed class ProductComplexAggregate : IAggregate<Complex, Complex, Complex>
    {
        public Expression<Func<Complex>> InitialState() => () => 1;

        public Expression<Func<Complex, long, Complex, Complex>> Accumulate()
            => (oldProduct, timestamp, input) => (Complex)(oldProduct * input);

        public Expression<Func<Complex, long, Complex, Complex>> Deaccumulate()
            => (oldProduct, timestamp, input) => (Complex)(oldProduct / input);

        public Expression<Func<Complex, Complex, Complex>> Difference()
            => (leftProduct, rightProduct) => (Complex)(leftProduct / rightProduct);

        public Expression<Func<Complex, Complex, Complex>> Sum()
            => (leftProduct, rightProduct) => (Complex)(leftProduct * rightProduct);

        public Expression<Func<Complex, Complex>> ComputeResult() => product => product;
    }

    /// <summary>
    /// Extension methods to allow more aggregates
    /// </summary>
    public static partial class AggregateExtensions
    {
        /// <summary>
        /// Performs a multiplication of all elements in a sequence
        /// </summary>
        /// <param name="enumerable">The sequence to multiply</param>
        /// <returns>The product of all of the elements in the sequence</returns>
        [Aggregate(typeof(ProductSByteAggregate))]
        public static sbyte Product(this IEnumerable<sbyte> enumerable)
            => enumerable.Aggregate((sbyte)1, (s, i) => (sbyte)(s * i));

        /// <summary>
        /// Performs a multiplication of all elements in a sequence
        /// </summary>
        /// <param name="enumerable">The sequence to multiply</param>
        /// <returns>The product of all of the elements in the sequence</returns>
        [Aggregate(typeof(ProductByteAggregate))]
        public static byte Product(this IEnumerable<byte> enumerable)
            => enumerable.Aggregate((byte)1, (s, i) => (byte)(s * i));

        /// <summary>
        /// Performs a multiplication of all elements in a sequence
        /// </summary>
        /// <param name="enumerable">The sequence to multiply</param>
        /// <returns>The product of all of the elements in the sequence</returns>
        [Aggregate(typeof(ProductShortAggregate))]
        public static short Product(this IEnumerable<short> enumerable)
            => enumerable.Aggregate((short)1, (s, i) => (short)(s * i));

        /// <summary>
        /// Performs a multiplication of all elements in a sequence
        /// </summary>
        /// <param name="enumerable">The sequence to multiply</param>
        /// <returns>The product of all of the elements in the sequence</returns>
        [Aggregate(typeof(ProductUShortAggregate))]
        public static ushort Product(this IEnumerable<ushort> enumerable)
            => enumerable.Aggregate((ushort)1, (s, i) => (ushort)(s * i));

        /// <summary>
        /// Performs a multiplication of all elements in a sequence
        /// </summary>
        /// <param name="enumerable">The sequence to multiply</param>
        /// <returns>The product of all of the elements in the sequence</returns>
        [Aggregate(typeof(ProductIntAggregate))]
        public static int Product(this IEnumerable<int> enumerable)
            => enumerable.Aggregate((int)1, (s, i) => (int)(s * i));

        /// <summary>
        /// Performs a multiplication of all elements in a sequence
        /// </summary>
        /// <param name="enumerable">The sequence to multiply</param>
        /// <returns>The product of all of the elements in the sequence</returns>
        [Aggregate(typeof(ProductUIntAggregate))]
        public static uint Product(this IEnumerable<uint> enumerable)
            => enumerable.Aggregate((uint)1, (s, i) => (uint)(s * i));

        /// <summary>
        /// Performs a multiplication of all elements in a sequence
        /// </summary>
        /// <param name="enumerable">The sequence to multiply</param>
        /// <returns>The product of all of the elements in the sequence</returns>
        [Aggregate(typeof(ProductLongAggregate))]
        public static long Product(this IEnumerable<long> enumerable)
            => enumerable.Aggregate((long)1, (s, i) => (long)(s * i));

        /// <summary>
        /// Performs a multiplication of all elements in a sequence
        /// </summary>
        /// <param name="enumerable">The sequence to multiply</param>
        /// <returns>The product of all of the elements in the sequence</returns>
        [Aggregate(typeof(ProductULongAggregate))]
        public static ulong Product(this IEnumerable<ulong> enumerable)
            => enumerable.Aggregate((ulong)1, (s, i) => (ulong)(s * i));

        /// <summary>
        /// Performs a multiplication of all elements in a sequence
        /// </summary>
        /// <param name="enumerable">The sequence to multiply</param>
        /// <returns>The product of all of the elements in the sequence</returns>
        [Aggregate(typeof(ProductFloatAggregate))]
        public static float Product(this IEnumerable<float> enumerable)
            => enumerable.Aggregate((float)1, (s, i) => (float)(s * i));

        /// <summary>
        /// Performs a multiplication of all elements in a sequence
        /// </summary>
        /// <param name="enumerable">The sequence to multiply</param>
        /// <returns>The product of all of the elements in the sequence</returns>
        [Aggregate(typeof(ProductDoubleAggregate))]
        public static double Product(this IEnumerable<double> enumerable)
            => enumerable.Aggregate((double)1, (s, i) => (double)(s * i));

        /// <summary>
        /// Performs a multiplication of all elements in a sequence
        /// </summary>
        /// <param name="enumerable">The sequence to multiply</param>
        /// <returns>The product of all of the elements in the sequence</returns>
        [Aggregate(typeof(ProductDecimalAggregate))]
        public static decimal Product(this IEnumerable<decimal> enumerable)
            => enumerable.Aggregate((decimal)1, (s, i) => (decimal)(s * i));

        /// <summary>
        /// Performs a multiplication of all elements in a sequence
        /// </summary>
        /// <param name="enumerable">The sequence to multiply</param>
        /// <returns>The product of all of the elements in the sequence</returns>
        [Aggregate(typeof(ProductBigIntegerAggregate))]
        public static BigInteger Product(this IEnumerable<BigInteger> enumerable)
            => enumerable.Aggregate((BigInteger)1, (s, i) => (BigInteger)(s * i));

        /// <summary>
        /// Performs a multiplication of all elements in a sequence
        /// </summary>
        /// <param name="enumerable">The sequence to multiply</param>
        /// <returns>The product of all of the elements in the sequence</returns>
        [Aggregate(typeof(ProductComplexAggregate))]
        public static Complex Product(this IEnumerable<Complex> enumerable)
            => enumerable.Aggregate((Complex)1, (s, i) => (Complex)(s * i));

    }
}
