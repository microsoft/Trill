// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using System.Numerics;

namespace Microsoft.StreamProcessing.Aggregates
{

    internal sealed class SumSquareSByteAggregate : IAggregate<sbyte, sbyte, sbyte>
    {
        public Expression<Func<sbyte>> InitialState() => () => 0;

        public Expression<Func<sbyte, long, sbyte, sbyte>> Accumulate()
            => (oldSum, timestamp, input) => (sbyte)(oldSum + input * input);

        public Expression<Func<sbyte, long, sbyte, sbyte>> Deaccumulate()
            => (oldSum, timestamp, input) => (sbyte)(oldSum - input * input);

        public Expression<Func<sbyte, sbyte, sbyte>> Difference()
            => (leftSum, rightSum) => (sbyte)(leftSum - rightSum);

        public Expression<Func<sbyte, sbyte, sbyte>> Sum()
            => (leftSum, rightSum) => (sbyte)(leftSum + rightSum);

        public Expression<Func<sbyte, sbyte>> ComputeResult() => sum => sum;
    }

    internal sealed class SumSquareByteAggregate : IAggregate<byte, byte, byte>
    {
        public Expression<Func<byte>> InitialState() => () => 0;

        public Expression<Func<byte, long, byte, byte>> Accumulate()
            => (oldSum, timestamp, input) => (byte)(oldSum + input * input);

        public Expression<Func<byte, long, byte, byte>> Deaccumulate()
            => (oldSum, timestamp, input) => (byte)(oldSum - input * input);

        public Expression<Func<byte, byte, byte>> Difference()
            => (leftSum, rightSum) => (byte)(leftSum - rightSum);

        public Expression<Func<byte, byte, byte>> Sum()
            => (leftSum, rightSum) => (byte)(leftSum + rightSum);

        public Expression<Func<byte, byte>> ComputeResult() => sum => sum;
    }

    internal sealed class SumSquareShortAggregate : IAggregate<short, short, short>
    {
        public Expression<Func<short>> InitialState() => () => 0;

        public Expression<Func<short, long, short, short>> Accumulate()
            => (oldSum, timestamp, input) => (short)(oldSum + input * input);

        public Expression<Func<short, long, short, short>> Deaccumulate()
            => (oldSum, timestamp, input) => (short)(oldSum - input * input);

        public Expression<Func<short, short, short>> Difference()
            => (leftSum, rightSum) => (short)(leftSum - rightSum);

        public Expression<Func<short, short, short>> Sum()
            => (leftSum, rightSum) => (short)(leftSum + rightSum);

        public Expression<Func<short, short>> ComputeResult() => sum => sum;
    }

    internal sealed class SumSquareUShortAggregate : IAggregate<ushort, ushort, ushort>
    {
        public Expression<Func<ushort>> InitialState() => () => 0;

        public Expression<Func<ushort, long, ushort, ushort>> Accumulate()
            => (oldSum, timestamp, input) => (ushort)(oldSum + input * input);

        public Expression<Func<ushort, long, ushort, ushort>> Deaccumulate()
            => (oldSum, timestamp, input) => (ushort)(oldSum - input * input);

        public Expression<Func<ushort, ushort, ushort>> Difference()
            => (leftSum, rightSum) => (ushort)(leftSum - rightSum);

        public Expression<Func<ushort, ushort, ushort>> Sum()
            => (leftSum, rightSum) => (ushort)(leftSum + rightSum);

        public Expression<Func<ushort, ushort>> ComputeResult() => sum => sum;
    }

    internal sealed class SumSquareIntAggregate : IAggregate<int, int, int>
    {
        public Expression<Func<int>> InitialState() => () => 0;

        public Expression<Func<int, long, int, int>> Accumulate()
            => (oldSum, timestamp, input) => (int)(oldSum + input * input);

        public Expression<Func<int, long, int, int>> Deaccumulate()
            => (oldSum, timestamp, input) => (int)(oldSum - input * input);

        public Expression<Func<int, int, int>> Difference()
            => (leftSum, rightSum) => (int)(leftSum - rightSum);

        public Expression<Func<int, int, int>> Sum()
            => (leftSum, rightSum) => (int)(leftSum + rightSum);

        public Expression<Func<int, int>> ComputeResult() => sum => sum;
    }

    internal sealed class SumSquareUIntAggregate : IAggregate<uint, uint, uint>
    {
        public Expression<Func<uint>> InitialState() => () => 0;

        public Expression<Func<uint, long, uint, uint>> Accumulate()
            => (oldSum, timestamp, input) => (uint)(oldSum + input * input);

        public Expression<Func<uint, long, uint, uint>> Deaccumulate()
            => (oldSum, timestamp, input) => (uint)(oldSum - input * input);

        public Expression<Func<uint, uint, uint>> Difference()
            => (leftSum, rightSum) => (uint)(leftSum - rightSum);

        public Expression<Func<uint, uint, uint>> Sum()
            => (leftSum, rightSum) => (uint)(leftSum + rightSum);

        public Expression<Func<uint, uint>> ComputeResult() => sum => sum;
    }

    internal sealed class SumSquareLongAggregate : IAggregate<long, long, long>
    {
        public Expression<Func<long>> InitialState() => () => 0;

        public Expression<Func<long, long, long, long>> Accumulate()
            => (oldSum, timestamp, input) => (long)(oldSum + input * input);

        public Expression<Func<long, long, long, long>> Deaccumulate()
            => (oldSum, timestamp, input) => (long)(oldSum - input * input);

        public Expression<Func<long, long, long>> Difference()
            => (leftSum, rightSum) => (long)(leftSum - rightSum);

        public Expression<Func<long, long, long>> Sum()
            => (leftSum, rightSum) => (long)(leftSum + rightSum);

        public Expression<Func<long, long>> ComputeResult() => sum => sum;
    }

    internal sealed class SumSquareULongAggregate : IAggregate<ulong, ulong, ulong>
    {
        public Expression<Func<ulong>> InitialState() => () => 0;

        public Expression<Func<ulong, long, ulong, ulong>> Accumulate()
            => (oldSum, timestamp, input) => (ulong)(oldSum + input * input);

        public Expression<Func<ulong, long, ulong, ulong>> Deaccumulate()
            => (oldSum, timestamp, input) => (ulong)(oldSum - input * input);

        public Expression<Func<ulong, ulong, ulong>> Difference()
            => (leftSum, rightSum) => (ulong)(leftSum - rightSum);

        public Expression<Func<ulong, ulong, ulong>> Sum()
            => (leftSum, rightSum) => (ulong)(leftSum + rightSum);

        public Expression<Func<ulong, ulong>> ComputeResult() => sum => sum;
    }

    internal sealed class SumSquareFloatAggregate : IAggregate<float, float, float>
    {
        public Expression<Func<float>> InitialState() => () => 0;

        public Expression<Func<float, long, float, float>> Accumulate()
            => (oldSum, timestamp, input) => (float)(oldSum + input * input);

        public Expression<Func<float, long, float, float>> Deaccumulate()
            => (oldSum, timestamp, input) => (float)(oldSum - input * input);

        public Expression<Func<float, float, float>> Difference()
            => (leftSum, rightSum) => (float)(leftSum - rightSum);

        public Expression<Func<float, float, float>> Sum()
            => (leftSum, rightSum) => (float)(leftSum + rightSum);

        public Expression<Func<float, float>> ComputeResult() => sum => sum;
    }

    internal sealed class SumSquareDoubleAggregate : IAggregate<double, double, double>
    {
        public Expression<Func<double>> InitialState() => () => 0;

        public Expression<Func<double, long, double, double>> Accumulate()
            => (oldSum, timestamp, input) => (double)(oldSum + input * input);

        public Expression<Func<double, long, double, double>> Deaccumulate()
            => (oldSum, timestamp, input) => (double)(oldSum - input * input);

        public Expression<Func<double, double, double>> Difference()
            => (leftSum, rightSum) => (double)(leftSum - rightSum);

        public Expression<Func<double, double, double>> Sum()
            => (leftSum, rightSum) => (double)(leftSum + rightSum);

        public Expression<Func<double, double>> ComputeResult() => sum => sum;
    }

    internal sealed class SumSquareDecimalAggregate : IAggregate<decimal, decimal, decimal>
    {
        public Expression<Func<decimal>> InitialState() => () => 0;

        public Expression<Func<decimal, long, decimal, decimal>> Accumulate()
            => (oldSum, timestamp, input) => (decimal)(oldSum + input * input);

        public Expression<Func<decimal, long, decimal, decimal>> Deaccumulate()
            => (oldSum, timestamp, input) => (decimal)(oldSum - input * input);

        public Expression<Func<decimal, decimal, decimal>> Difference()
            => (leftSum, rightSum) => (decimal)(leftSum - rightSum);

        public Expression<Func<decimal, decimal, decimal>> Sum()
            => (leftSum, rightSum) => (decimal)(leftSum + rightSum);

        public Expression<Func<decimal, decimal>> ComputeResult() => sum => sum;
    }

    internal sealed class SumSquareBigIntegerAggregate : IAggregate<BigInteger, BigInteger, BigInteger>
    {
        public Expression<Func<BigInteger>> InitialState() => () => 0;

        public Expression<Func<BigInteger, long, BigInteger, BigInteger>> Accumulate()
            => (oldSum, timestamp, input) => (BigInteger)(oldSum + input * input);

        public Expression<Func<BigInteger, long, BigInteger, BigInteger>> Deaccumulate()
            => (oldSum, timestamp, input) => (BigInteger)(oldSum - input * input);

        public Expression<Func<BigInteger, BigInteger, BigInteger>> Difference()
            => (leftSum, rightSum) => (BigInteger)(leftSum - rightSum);

        public Expression<Func<BigInteger, BigInteger, BigInteger>> Sum()
            => (leftSum, rightSum) => (BigInteger)(leftSum + rightSum);

        public Expression<Func<BigInteger, BigInteger>> ComputeResult() => sum => sum;
    }

    internal sealed class SumSquareComplexAggregate : IAggregate<Complex, Complex, Complex>
    {
        public Expression<Func<Complex>> InitialState() => () => 0;

        public Expression<Func<Complex, long, Complex, Complex>> Accumulate()
            => (oldSum, timestamp, input) => (Complex)(oldSum + input * input);

        public Expression<Func<Complex, long, Complex, Complex>> Deaccumulate()
            => (oldSum, timestamp, input) => (Complex)(oldSum - input * input);

        public Expression<Func<Complex, Complex, Complex>> Difference()
            => (leftSum, rightSum) => (Complex)(leftSum - rightSum);

        public Expression<Func<Complex, Complex, Complex>> Sum()
            => (leftSum, rightSum) => (Complex)(leftSum + rightSum);

        public Expression<Func<Complex, Complex>> ComputeResult() => sum => sum;
    }
}
