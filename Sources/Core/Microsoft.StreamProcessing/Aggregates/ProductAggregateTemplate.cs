// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using System.Numerics;

namespace Microsoft.StreamProcessing.Aggregates
{

    internal sealed class ProductSByteAggregate : ISummableAggregate<sbyte, sbyte, sbyte>
    {
        public Expression<Func<sbyte>> InitialState() => () => 0;

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

    internal sealed class ProductByteAggregate : ISummableAggregate<byte, byte, byte>
    {
        public Expression<Func<byte>> InitialState() => () => 0;

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

    internal sealed class ProductShortAggregate : ISummableAggregate<short, short, short>
    {
        public Expression<Func<short>> InitialState() => () => 0;

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

    internal sealed class ProductUShortAggregate : ISummableAggregate<ushort, ushort, ushort>
    {
        public Expression<Func<ushort>> InitialState() => () => 0;

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

    internal sealed class ProductIntAggregate : ISummableAggregate<int, int, int>
    {
        public Expression<Func<int>> InitialState() => () => 0;

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

    internal sealed class ProductUIntAggregate : ISummableAggregate<uint, uint, uint>
    {
        public Expression<Func<uint>> InitialState() => () => 0;

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

    internal sealed class ProductLongAggregate : ISummableAggregate<long, long, long>
    {
        public Expression<Func<long>> InitialState() => () => 0;

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

    internal sealed class ProductULongAggregate : ISummableAggregate<ulong, ulong, ulong>
    {
        public Expression<Func<ulong>> InitialState() => () => 0;

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

    internal sealed class ProductFloatAggregate : ISummableAggregate<float, float, float>
    {
        public Expression<Func<float>> InitialState() => () => 0;

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

    internal sealed class ProductDoubleAggregate : ISummableAggregate<double, double, double>
    {
        public Expression<Func<double>> InitialState() => () => 0;

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

    internal sealed class ProductDecimalAggregate : ISummableAggregate<decimal, decimal, decimal>
    {
        public Expression<Func<decimal>> InitialState() => () => 0;

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

    internal sealed class ProductBigIntegerAggregate : ISummableAggregate<BigInteger, BigInteger, BigInteger>
    {
        public Expression<Func<BigInteger>> InitialState() => () => 0;

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

    internal sealed class ProductComplexAggregate : ISummableAggregate<Complex, Complex, Complex>
    {
        public Expression<Func<Complex>> InitialState() => () => 0;

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
}
