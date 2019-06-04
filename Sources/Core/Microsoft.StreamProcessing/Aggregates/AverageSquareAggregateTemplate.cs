// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using System.Numerics;

namespace Microsoft.StreamProcessing.Aggregates
{

    internal sealed class AverageSquareSByteAggregate : ISummableAggregate<sbyte, AverageState<long>, double>
    {
        public Expression<Func<AverageState<long>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<long>, long, sbyte, AverageState<long>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count + 1, Sum = oldState.Sum + (long)(input * input) };

        public Expression<Func<AverageState<long>, long, sbyte, AverageState<long>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count - 1, Sum = oldState.Sum - (long)(input * input) };

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Difference()
            => (left, right) => new AverageState<long> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Sum()
            => (left, right) => new AverageState<long> { Count = left.Count + right.Count, Sum = left.Sum + right.Sum };

        public Expression<Func<AverageState<long>, double>> ComputeResult()
            => state => (double)state.Sum / state.Count;
    }

    internal sealed class AverageSquareNullableSByteAggregate : IAggregate<sbyte?, AverageState<long>, double?>
    {
        public Expression<Func<AverageState<long>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<long>, long, sbyte?, AverageState<long>>> Accumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<long> { Count = oldState.Count + 1, Sum = oldState.Sum + (long)(input.Value * input.Value) } : oldState;

        public Expression<Func<AverageState<long>, long, sbyte?, AverageState<long>>> Deaccumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<long> { Count = oldState.Count - 1, Sum = oldState.Sum - (long)(input.Value * input.Value) } : oldState;

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Difference()
            => (left, right) => new AverageState<long> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<long>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal sealed class AverageSquareFilterableSByteAggregate : IAggregate<sbyte, AverageState<long>, double?>
    {
        public Expression<Func<AverageState<long>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<long>, long, sbyte, AverageState<long>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count + 1, Sum = oldState.Sum + (long)(input * input) };

        public Expression<Func<AverageState<long>, long, sbyte, AverageState<long>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count - 1, Sum = oldState.Sum - (long)(input * input) };

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Difference()
            => (left, right) => new AverageState<long> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<long>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal sealed class AverageSquareShortAggregate : ISummableAggregate<short, AverageState<long>, double>
    {
        public Expression<Func<AverageState<long>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<long>, long, short, AverageState<long>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count + 1, Sum = oldState.Sum + (long)(input * input) };

        public Expression<Func<AverageState<long>, long, short, AverageState<long>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count - 1, Sum = oldState.Sum - (long)(input * input) };

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Difference()
            => (left, right) => new AverageState<long> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Sum()
            => (left, right) => new AverageState<long> { Count = left.Count + right.Count, Sum = left.Sum + right.Sum };

        public Expression<Func<AverageState<long>, double>> ComputeResult()
            => state => (double)state.Sum / state.Count;
    }

    internal sealed class AverageSquareNullableShortAggregate : IAggregate<short?, AverageState<long>, double?>
    {
        public Expression<Func<AverageState<long>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<long>, long, short?, AverageState<long>>> Accumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<long> { Count = oldState.Count + 1, Sum = oldState.Sum + (long)(input.Value * input.Value) } : oldState;

        public Expression<Func<AverageState<long>, long, short?, AverageState<long>>> Deaccumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<long> { Count = oldState.Count - 1, Sum = oldState.Sum - (long)(input.Value * input.Value) } : oldState;

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Difference()
            => (left, right) => new AverageState<long> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<long>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal sealed class AverageSquareFilterableShortAggregate : IAggregate<short, AverageState<long>, double?>
    {
        public Expression<Func<AverageState<long>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<long>, long, short, AverageState<long>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count + 1, Sum = oldState.Sum + (long)(input * input) };

        public Expression<Func<AverageState<long>, long, short, AverageState<long>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count - 1, Sum = oldState.Sum - (long)(input * input) };

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Difference()
            => (left, right) => new AverageState<long> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<long>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal sealed class AverageSquareIntAggregate : ISummableAggregate<int, AverageState<long>, double>
    {
        public Expression<Func<AverageState<long>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<long>, long, int, AverageState<long>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count + 1, Sum = oldState.Sum + (long)(input * input) };

        public Expression<Func<AverageState<long>, long, int, AverageState<long>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count - 1, Sum = oldState.Sum - (long)(input * input) };

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Difference()
            => (left, right) => new AverageState<long> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Sum()
            => (left, right) => new AverageState<long> { Count = left.Count + right.Count, Sum = left.Sum + right.Sum };

        public Expression<Func<AverageState<long>, double>> ComputeResult()
            => state => (double)state.Sum / state.Count;
    }

    internal sealed class AverageSquareNullableIntAggregate : IAggregate<int?, AverageState<long>, double?>
    {
        public Expression<Func<AverageState<long>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<long>, long, int?, AverageState<long>>> Accumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<long> { Count = oldState.Count + 1, Sum = oldState.Sum + (long)(input.Value * input.Value) } : oldState;

        public Expression<Func<AverageState<long>, long, int?, AverageState<long>>> Deaccumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<long> { Count = oldState.Count - 1, Sum = oldState.Sum - (long)(input.Value * input.Value) } : oldState;

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Difference()
            => (left, right) => new AverageState<long> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<long>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal sealed class AverageSquareFilterableIntAggregate : IAggregate<int, AverageState<long>, double?>
    {
        public Expression<Func<AverageState<long>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<long>, long, int, AverageState<long>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count + 1, Sum = oldState.Sum + (long)(input * input) };

        public Expression<Func<AverageState<long>, long, int, AverageState<long>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count - 1, Sum = oldState.Sum - (long)(input * input) };

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Difference()
            => (left, right) => new AverageState<long> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<long>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal sealed class AverageSquareLongAggregate : ISummableAggregate<long, AverageState<long>, double>
    {
        public Expression<Func<AverageState<long>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<long>, long, long, AverageState<long>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count + 1, Sum = oldState.Sum + (long)(input * input) };

        public Expression<Func<AverageState<long>, long, long, AverageState<long>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count - 1, Sum = oldState.Sum - (long)(input * input) };

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Difference()
            => (left, right) => new AverageState<long> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Sum()
            => (left, right) => new AverageState<long> { Count = left.Count + right.Count, Sum = left.Sum + right.Sum };

        public Expression<Func<AverageState<long>, double>> ComputeResult()
            => state => (double)state.Sum / state.Count;
    }

    internal sealed class AverageSquareNullableLongAggregate : IAggregate<long?, AverageState<long>, double?>
    {
        public Expression<Func<AverageState<long>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<long>, long, long?, AverageState<long>>> Accumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<long> { Count = oldState.Count + 1, Sum = oldState.Sum + (long)(input.Value * input.Value) } : oldState;

        public Expression<Func<AverageState<long>, long, long?, AverageState<long>>> Deaccumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<long> { Count = oldState.Count - 1, Sum = oldState.Sum - (long)(input.Value * input.Value) } : oldState;

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Difference()
            => (left, right) => new AverageState<long> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<long>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal sealed class AverageSquareFilterableLongAggregate : IAggregate<long, AverageState<long>, double?>
    {
        public Expression<Func<AverageState<long>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<long>, long, long, AverageState<long>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count + 1, Sum = oldState.Sum + (long)(input * input) };

        public Expression<Func<AverageState<long>, long, long, AverageState<long>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count - 1, Sum = oldState.Sum - (long)(input * input) };

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Difference()
            => (left, right) => new AverageState<long> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<long>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal sealed class AverageSquareByteAggregate : ISummableAggregate<byte, AverageState<ulong>, double>
    {
        public Expression<Func<AverageState<ulong>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<ulong>, long, byte, AverageState<ulong>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count + 1, Sum = oldState.Sum + (ulong)(input * input) };

        public Expression<Func<AverageState<ulong>, long, byte, AverageState<ulong>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count - 1, Sum = oldState.Sum - (ulong)(input * input) };

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Difference()
            => (left, right) => new AverageState<ulong> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Sum()
            => (left, right) => new AverageState<ulong> { Count = left.Count + right.Count, Sum = left.Sum + right.Sum };

        public Expression<Func<AverageState<ulong>, double>> ComputeResult()
            => state => (double)state.Sum / state.Count;
    }

    internal sealed class AverageSquareNullableByteAggregate : IAggregate<byte?, AverageState<ulong>, double?>
    {
        public Expression<Func<AverageState<ulong>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<ulong>, long, byte?, AverageState<ulong>>> Accumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<ulong> { Count = oldState.Count + 1, Sum = oldState.Sum + (ulong)(input.Value * input.Value) } : oldState;

        public Expression<Func<AverageState<ulong>, long, byte?, AverageState<ulong>>> Deaccumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<ulong> { Count = oldState.Count - 1, Sum = oldState.Sum - (ulong)(input.Value * input.Value) } : oldState;

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Difference()
            => (left, right) => new AverageState<ulong> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<ulong>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal sealed class AverageSquareFilterableByteAggregate : IAggregate<byte, AverageState<ulong>, double?>
    {
        public Expression<Func<AverageState<ulong>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<ulong>, long, byte, AverageState<ulong>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count + 1, Sum = oldState.Sum + (ulong)(input * input) };

        public Expression<Func<AverageState<ulong>, long, byte, AverageState<ulong>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count - 1, Sum = oldState.Sum - (ulong)(input * input) };

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Difference()
            => (left, right) => new AverageState<ulong> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<ulong>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal sealed class AverageSquareUShortAggregate : ISummableAggregate<ushort, AverageState<ulong>, double>
    {
        public Expression<Func<AverageState<ulong>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<ulong>, long, ushort, AverageState<ulong>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count + 1, Sum = oldState.Sum + (ulong)(input * input) };

        public Expression<Func<AverageState<ulong>, long, ushort, AverageState<ulong>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count - 1, Sum = oldState.Sum - (ulong)(input * input) };

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Difference()
            => (left, right) => new AverageState<ulong> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Sum()
            => (left, right) => new AverageState<ulong> { Count = left.Count + right.Count, Sum = left.Sum + right.Sum };

        public Expression<Func<AverageState<ulong>, double>> ComputeResult()
            => state => (double)state.Sum / state.Count;
    }

    internal sealed class AverageSquareNullableUShortAggregate : IAggregate<ushort?, AverageState<ulong>, double?>
    {
        public Expression<Func<AverageState<ulong>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<ulong>, long, ushort?, AverageState<ulong>>> Accumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<ulong> { Count = oldState.Count + 1, Sum = oldState.Sum + (ulong)(input.Value * input.Value) } : oldState;

        public Expression<Func<AverageState<ulong>, long, ushort?, AverageState<ulong>>> Deaccumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<ulong> { Count = oldState.Count - 1, Sum = oldState.Sum - (ulong)(input.Value * input.Value) } : oldState;

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Difference()
            => (left, right) => new AverageState<ulong> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<ulong>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal sealed class AverageSquareFilterableUShortAggregate : IAggregate<ushort, AverageState<ulong>, double?>
    {
        public Expression<Func<AverageState<ulong>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<ulong>, long, ushort, AverageState<ulong>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count + 1, Sum = oldState.Sum + (ulong)(input * input) };

        public Expression<Func<AverageState<ulong>, long, ushort, AverageState<ulong>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count - 1, Sum = oldState.Sum - (ulong)(input * input) };

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Difference()
            => (left, right) => new AverageState<ulong> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<ulong>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal sealed class AverageSquareUIntAggregate : ISummableAggregate<uint, AverageState<ulong>, double>
    {
        public Expression<Func<AverageState<ulong>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<ulong>, long, uint, AverageState<ulong>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count + 1, Sum = oldState.Sum + (ulong)(input * input) };

        public Expression<Func<AverageState<ulong>, long, uint, AverageState<ulong>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count - 1, Sum = oldState.Sum - (ulong)(input * input) };

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Difference()
            => (left, right) => new AverageState<ulong> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Sum()
            => (left, right) => new AverageState<ulong> { Count = left.Count + right.Count, Sum = left.Sum + right.Sum };

        public Expression<Func<AverageState<ulong>, double>> ComputeResult()
            => state => (double)state.Sum / state.Count;
    }

    internal sealed class AverageSquareNullableUIntAggregate : IAggregate<uint?, AverageState<ulong>, double?>
    {
        public Expression<Func<AverageState<ulong>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<ulong>, long, uint?, AverageState<ulong>>> Accumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<ulong> { Count = oldState.Count + 1, Sum = oldState.Sum + (ulong)(input.Value * input.Value) } : oldState;

        public Expression<Func<AverageState<ulong>, long, uint?, AverageState<ulong>>> Deaccumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<ulong> { Count = oldState.Count - 1, Sum = oldState.Sum - (ulong)(input.Value * input.Value) } : oldState;

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Difference()
            => (left, right) => new AverageState<ulong> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<ulong>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal sealed class AverageSquareFilterableUIntAggregate : IAggregate<uint, AverageState<ulong>, double?>
    {
        public Expression<Func<AverageState<ulong>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<ulong>, long, uint, AverageState<ulong>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count + 1, Sum = oldState.Sum + (ulong)(input * input) };

        public Expression<Func<AverageState<ulong>, long, uint, AverageState<ulong>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count - 1, Sum = oldState.Sum - (ulong)(input * input) };

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Difference()
            => (left, right) => new AverageState<ulong> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<ulong>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal sealed class AverageSquareULongAggregate : ISummableAggregate<ulong, AverageState<ulong>, double>
    {
        public Expression<Func<AverageState<ulong>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<ulong>, long, ulong, AverageState<ulong>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count + 1, Sum = oldState.Sum + (ulong)(input * input) };

        public Expression<Func<AverageState<ulong>, long, ulong, AverageState<ulong>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count - 1, Sum = oldState.Sum - (ulong)(input * input) };

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Difference()
            => (left, right) => new AverageState<ulong> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Sum()
            => (left, right) => new AverageState<ulong> { Count = left.Count + right.Count, Sum = left.Sum + right.Sum };

        public Expression<Func<AverageState<ulong>, double>> ComputeResult()
            => state => (double)state.Sum / state.Count;
    }

    internal sealed class AverageSquareNullableULongAggregate : IAggregate<ulong?, AverageState<ulong>, double?>
    {
        public Expression<Func<AverageState<ulong>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<ulong>, long, ulong?, AverageState<ulong>>> Accumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<ulong> { Count = oldState.Count + 1, Sum = oldState.Sum + (ulong)(input.Value * input.Value) } : oldState;

        public Expression<Func<AverageState<ulong>, long, ulong?, AverageState<ulong>>> Deaccumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<ulong> { Count = oldState.Count - 1, Sum = oldState.Sum - (ulong)(input.Value * input.Value) } : oldState;

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Difference()
            => (left, right) => new AverageState<ulong> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<ulong>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal sealed class AverageSquareFilterableULongAggregate : IAggregate<ulong, AverageState<ulong>, double?>
    {
        public Expression<Func<AverageState<ulong>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<ulong>, long, ulong, AverageState<ulong>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count + 1, Sum = oldState.Sum + (ulong)(input * input) };

        public Expression<Func<AverageState<ulong>, long, ulong, AverageState<ulong>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count - 1, Sum = oldState.Sum - (ulong)(input * input) };

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Difference()
            => (left, right) => new AverageState<ulong> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<ulong>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal sealed class AverageSquareFloatAggregate : ISummableAggregate<float, AverageState<float>, float>
    {
        public Expression<Func<AverageState<float>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<float>, long, float, AverageState<float>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<float> { Count = oldState.Count + 1, Sum = oldState.Sum + (float)(input * input) };

        public Expression<Func<AverageState<float>, long, float, AverageState<float>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<float> { Count = oldState.Count - 1, Sum = oldState.Sum - (float)(input * input) };

        public Expression<Func<AverageState<float>, AverageState<float>, AverageState<float>>> Difference()
            => (left, right) => new AverageState<float> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<float>, AverageState<float>, AverageState<float>>> Sum()
            => (left, right) => new AverageState<float> { Count = left.Count + right.Count, Sum = left.Sum + right.Sum };

        public Expression<Func<AverageState<float>, float>> ComputeResult()
            => state => (float)state.Sum / state.Count;
    }

    internal sealed class AverageSquareNullableFloatAggregate : IAggregate<float?, AverageState<float>, float?>
    {
        public Expression<Func<AverageState<float>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<float>, long, float?, AverageState<float>>> Accumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<float> { Count = oldState.Count + 1, Sum = oldState.Sum + (float)(input.Value * input.Value) } : oldState;

        public Expression<Func<AverageState<float>, long, float?, AverageState<float>>> Deaccumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<float> { Count = oldState.Count - 1, Sum = oldState.Sum - (float)(input.Value * input.Value) } : oldState;

        public Expression<Func<AverageState<float>, AverageState<float>, AverageState<float>>> Difference()
            => (left, right) => new AverageState<float> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<float>, float?>> ComputeResult()
            => state => state.Count != 0 ? (float)state.Sum / state.Count : (float?)null;
    }

    internal sealed class AverageSquareFilterableFloatAggregate : IAggregate<float, AverageState<float>, float?>
    {
        public Expression<Func<AverageState<float>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<float>, long, float, AverageState<float>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<float> { Count = oldState.Count + 1, Sum = oldState.Sum + (float)(input * input) };

        public Expression<Func<AverageState<float>, long, float, AverageState<float>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<float> { Count = oldState.Count - 1, Sum = oldState.Sum - (float)(input * input) };

        public Expression<Func<AverageState<float>, AverageState<float>, AverageState<float>>> Difference()
            => (left, right) => new AverageState<float> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<float>, float?>> ComputeResult()
            => state => state.Count != 0 ? (float)state.Sum / state.Count : (float?)null;
    }

    internal sealed class AverageSquareDoubleAggregate : ISummableAggregate<double, AverageState<double>, double>
    {
        public Expression<Func<AverageState<double>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<double>, long, double, AverageState<double>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<double> { Count = oldState.Count + 1, Sum = oldState.Sum + (double)(input * input) };

        public Expression<Func<AverageState<double>, long, double, AverageState<double>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<double> { Count = oldState.Count - 1, Sum = oldState.Sum - (double)(input * input) };

        public Expression<Func<AverageState<double>, AverageState<double>, AverageState<double>>> Difference()
            => (left, right) => new AverageState<double> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<double>, AverageState<double>, AverageState<double>>> Sum()
            => (left, right) => new AverageState<double> { Count = left.Count + right.Count, Sum = left.Sum + right.Sum };

        public Expression<Func<AverageState<double>, double>> ComputeResult()
            => state => (double)state.Sum / state.Count;
    }

    internal sealed class AverageSquareNullableDoubleAggregate : IAggregate<double?, AverageState<double>, double?>
    {
        public Expression<Func<AverageState<double>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<double>, long, double?, AverageState<double>>> Accumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<double> { Count = oldState.Count + 1, Sum = oldState.Sum + (double)(input.Value * input.Value) } : oldState;

        public Expression<Func<AverageState<double>, long, double?, AverageState<double>>> Deaccumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<double> { Count = oldState.Count - 1, Sum = oldState.Sum - (double)(input.Value * input.Value) } : oldState;

        public Expression<Func<AverageState<double>, AverageState<double>, AverageState<double>>> Difference()
            => (left, right) => new AverageState<double> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<double>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal sealed class AverageSquareFilterableDoubleAggregate : IAggregate<double, AverageState<double>, double?>
    {
        public Expression<Func<AverageState<double>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<double>, long, double, AverageState<double>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<double> { Count = oldState.Count + 1, Sum = oldState.Sum + (double)(input * input) };

        public Expression<Func<AverageState<double>, long, double, AverageState<double>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<double> { Count = oldState.Count - 1, Sum = oldState.Sum - (double)(input * input) };

        public Expression<Func<AverageState<double>, AverageState<double>, AverageState<double>>> Difference()
            => (left, right) => new AverageState<double> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<double>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal sealed class AverageSquareDecimalAggregate : ISummableAggregate<decimal, AverageState<decimal>, decimal>
    {
        public Expression<Func<AverageState<decimal>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<decimal>, long, decimal, AverageState<decimal>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<decimal> { Count = oldState.Count + 1, Sum = oldState.Sum + (decimal)(input * input) };

        public Expression<Func<AverageState<decimal>, long, decimal, AverageState<decimal>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<decimal> { Count = oldState.Count - 1, Sum = oldState.Sum - (decimal)(input * input) };

        public Expression<Func<AverageState<decimal>, AverageState<decimal>, AverageState<decimal>>> Difference()
            => (left, right) => new AverageState<decimal> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<decimal>, AverageState<decimal>, AverageState<decimal>>> Sum()
            => (left, right) => new AverageState<decimal> { Count = left.Count + right.Count, Sum = left.Sum + right.Sum };

        public Expression<Func<AverageState<decimal>, decimal>> ComputeResult()
            => state => (decimal)state.Sum / state.Count;
    }

    internal sealed class AverageSquareNullableDecimalAggregate : IAggregate<decimal?, AverageState<decimal>, decimal?>
    {
        public Expression<Func<AverageState<decimal>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<decimal>, long, decimal?, AverageState<decimal>>> Accumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<decimal> { Count = oldState.Count + 1, Sum = oldState.Sum + (decimal)(input.Value * input.Value) } : oldState;

        public Expression<Func<AverageState<decimal>, long, decimal?, AverageState<decimal>>> Deaccumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<decimal> { Count = oldState.Count - 1, Sum = oldState.Sum - (decimal)(input.Value * input.Value) } : oldState;

        public Expression<Func<AverageState<decimal>, AverageState<decimal>, AverageState<decimal>>> Difference()
            => (left, right) => new AverageState<decimal> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<decimal>, decimal?>> ComputeResult()
            => state => state.Count != 0 ? (decimal)state.Sum / state.Count : (decimal?)null;
    }

    internal sealed class AverageSquareFilterableDecimalAggregate : IAggregate<decimal, AverageState<decimal>, decimal?>
    {
        public Expression<Func<AverageState<decimal>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<decimal>, long, decimal, AverageState<decimal>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<decimal> { Count = oldState.Count + 1, Sum = oldState.Sum + (decimal)(input * input) };

        public Expression<Func<AverageState<decimal>, long, decimal, AverageState<decimal>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<decimal> { Count = oldState.Count - 1, Sum = oldState.Sum - (decimal)(input * input) };

        public Expression<Func<AverageState<decimal>, AverageState<decimal>, AverageState<decimal>>> Difference()
            => (left, right) => new AverageState<decimal> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<decimal>, decimal?>> ComputeResult()
            => state => state.Count != 0 ? (decimal)state.Sum / state.Count : (decimal?)null;
    }

    internal sealed class AverageSquareBigIntegerAggregate : ISummableAggregate<BigInteger, AverageState<BigInteger>, double>
    {
        public Expression<Func<AverageState<BigInteger>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<BigInteger>, long, BigInteger, AverageState<BigInteger>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<BigInteger> { Count = oldState.Count + 1, Sum = oldState.Sum + (BigInteger)(input * input) };

        public Expression<Func<AverageState<BigInteger>, long, BigInteger, AverageState<BigInteger>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<BigInteger> { Count = oldState.Count - 1, Sum = oldState.Sum - (BigInteger)(input * input) };

        public Expression<Func<AverageState<BigInteger>, AverageState<BigInteger>, AverageState<BigInteger>>> Difference()
            => (left, right) => new AverageState<BigInteger> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<BigInteger>, AverageState<BigInteger>, AverageState<BigInteger>>> Sum()
            => (left, right) => new AverageState<BigInteger> { Count = left.Count + right.Count, Sum = left.Sum + right.Sum };

        public Expression<Func<AverageState<BigInteger>, double>> ComputeResult()
            => state => (double)state.Sum / state.Count;
    }

    internal sealed class AverageSquareNullableBigIntegerAggregate : IAggregate<BigInteger?, AverageState<BigInteger>, double?>
    {
        public Expression<Func<AverageState<BigInteger>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<BigInteger>, long, BigInteger?, AverageState<BigInteger>>> Accumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<BigInteger> { Count = oldState.Count + 1, Sum = oldState.Sum + (BigInteger)(input.Value * input.Value) } : oldState;

        public Expression<Func<AverageState<BigInteger>, long, BigInteger?, AverageState<BigInteger>>> Deaccumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<BigInteger> { Count = oldState.Count - 1, Sum = oldState.Sum - (BigInteger)(input.Value * input.Value) } : oldState;

        public Expression<Func<AverageState<BigInteger>, AverageState<BigInteger>, AverageState<BigInteger>>> Difference()
            => (left, right) => new AverageState<BigInteger> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<BigInteger>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal sealed class AverageSquareFilterableBigIntegerAggregate : IAggregate<BigInteger, AverageState<BigInteger>, double?>
    {
        public Expression<Func<AverageState<BigInteger>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<BigInteger>, long, BigInteger, AverageState<BigInteger>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<BigInteger> { Count = oldState.Count + 1, Sum = oldState.Sum + (BigInteger)(input * input) };

        public Expression<Func<AverageState<BigInteger>, long, BigInteger, AverageState<BigInteger>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<BigInteger> { Count = oldState.Count - 1, Sum = oldState.Sum - (BigInteger)(input * input) };

        public Expression<Func<AverageState<BigInteger>, AverageState<BigInteger>, AverageState<BigInteger>>> Difference()
            => (left, right) => new AverageState<BigInteger> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<BigInteger>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal sealed class AverageSquareComplexAggregate : ISummableAggregate<Complex, AverageState<Complex>, Complex>
    {
        public Expression<Func<AverageState<Complex>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<Complex>, long, Complex, AverageState<Complex>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<Complex> { Count = oldState.Count + 1, Sum = oldState.Sum + (Complex)(input * input) };

        public Expression<Func<AverageState<Complex>, long, Complex, AverageState<Complex>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<Complex> { Count = oldState.Count - 1, Sum = oldState.Sum - (Complex)(input * input) };

        public Expression<Func<AverageState<Complex>, AverageState<Complex>, AverageState<Complex>>> Difference()
            => (left, right) => new AverageState<Complex> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<Complex>, AverageState<Complex>, AverageState<Complex>>> Sum()
            => (left, right) => new AverageState<Complex> { Count = left.Count + right.Count, Sum = left.Sum + right.Sum };

        public Expression<Func<AverageState<Complex>, Complex>> ComputeResult()
            => state => (Complex)state.Sum / state.Count;
    }

    internal sealed class AverageSquareNullableComplexAggregate : IAggregate<Complex?, AverageState<Complex>, Complex?>
    {
        public Expression<Func<AverageState<Complex>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<Complex>, long, Complex?, AverageState<Complex>>> Accumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<Complex> { Count = oldState.Count + 1, Sum = oldState.Sum + (Complex)(input.Value * input.Value) } : oldState;

        public Expression<Func<AverageState<Complex>, long, Complex?, AverageState<Complex>>> Deaccumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<Complex> { Count = oldState.Count - 1, Sum = oldState.Sum - (Complex)(input.Value * input.Value) } : oldState;

        public Expression<Func<AverageState<Complex>, AverageState<Complex>, AverageState<Complex>>> Difference()
            => (left, right) => new AverageState<Complex> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<Complex>, Complex?>> ComputeResult()
            => state => state.Count != 0 ? (Complex)state.Sum / state.Count : (Complex?)null;
    }

    internal sealed class AverageSquareFilterableComplexAggregate : IAggregate<Complex, AverageState<Complex>, Complex?>
    {
        public Expression<Func<AverageState<Complex>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<Complex>, long, Complex, AverageState<Complex>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<Complex> { Count = oldState.Count + 1, Sum = oldState.Sum + (Complex)(input * input) };

        public Expression<Func<AverageState<Complex>, long, Complex, AverageState<Complex>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<Complex> { Count = oldState.Count - 1, Sum = oldState.Sum - (Complex)(input * input) };

        public Expression<Func<AverageState<Complex>, AverageState<Complex>, AverageState<Complex>>> Difference()
            => (left, right) => new AverageState<Complex> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<Complex>, Complex?>> ComputeResult()
            => state => state.Count != 0 ? (Complex)state.Sum / state.Count : (Complex?)null;
    }
}
