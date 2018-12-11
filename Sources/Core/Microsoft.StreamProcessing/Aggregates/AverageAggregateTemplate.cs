// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using System.Numerics;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing.Aggregates
{
    /// <summary>
    /// The state object used in average aggregates.
    /// </summary>
    /// <typeparam name="T">The type of the underlying elements being aggregated.</typeparam>
    [DataContract]
    public struct AverageState<T>
    {
        /// <summary>
        /// The sum of all data seen so far.
        /// </summary>
        [DataMember]
        public T Sum;

        /// <summary>
        /// The count of all events so far.
        /// </summary>
        [DataMember]
        public ulong Count;
    }

    internal class AverageSByteAggregate : IAggregate<sbyte, AverageState<long>, double>
    {
        public Expression<Func<AverageState<long>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<long>, long, sbyte, AverageState<long>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count + 1, Sum = oldState.Sum + input };

        public Expression<Func<AverageState<long>, long, sbyte, AverageState<long>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count - 1, Sum = oldState.Sum - input };

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Difference()
            => (left, right) => new AverageState<long> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<long>, double>> ComputeResult()
            => state => (double)state.Sum / state.Count;
    }

    internal class AverageNullableSByteAggregate : IAggregate<sbyte?, AverageState<long>, double?>
    {
        public Expression<Func<AverageState<long>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<long>, long, sbyte?, AverageState<long>>> Accumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<long> { Count = oldState.Count + 1, Sum = oldState.Sum + input.Value } : oldState;

        public Expression<Func<AverageState<long>, long, sbyte?, AverageState<long>>> Deaccumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<long> { Count = oldState.Count - 1, Sum = oldState.Sum - input.Value } : oldState;

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Difference()
            => (left, right) => new AverageState<long> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<long>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal class AverageFilterableSByteAggregate : IAggregate<sbyte, AverageState<long>, double?>
    {
        public Expression<Func<AverageState<long>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<long>, long, sbyte, AverageState<long>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count + 1, Sum = oldState.Sum + input };

        public Expression<Func<AverageState<long>, long, sbyte, AverageState<long>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count - 1, Sum = oldState.Sum - input };

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Difference()
            => (left, right) => new AverageState<long> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<long>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal class AverageShortAggregate : IAggregate<short, AverageState<long>, double>
    {
        public Expression<Func<AverageState<long>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<long>, long, short, AverageState<long>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count + 1, Sum = oldState.Sum + input };

        public Expression<Func<AverageState<long>, long, short, AverageState<long>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count - 1, Sum = oldState.Sum - input };

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Difference()
            => (left, right) => new AverageState<long> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<long>, double>> ComputeResult()
            => state => (double)state.Sum / state.Count;
    }

    internal class AverageNullableShortAggregate : IAggregate<short?, AverageState<long>, double?>
    {
        public Expression<Func<AverageState<long>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<long>, long, short?, AverageState<long>>> Accumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<long> { Count = oldState.Count + 1, Sum = oldState.Sum + input.Value } : oldState;

        public Expression<Func<AverageState<long>, long, short?, AverageState<long>>> Deaccumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<long> { Count = oldState.Count - 1, Sum = oldState.Sum - input.Value } : oldState;

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Difference()
            => (left, right) => new AverageState<long> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<long>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal class AverageFilterableShortAggregate : IAggregate<short, AverageState<long>, double?>
    {
        public Expression<Func<AverageState<long>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<long>, long, short, AverageState<long>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count + 1, Sum = oldState.Sum + input };

        public Expression<Func<AverageState<long>, long, short, AverageState<long>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count - 1, Sum = oldState.Sum - input };

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Difference()
            => (left, right) => new AverageState<long> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<long>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal class AverageIntAggregate : IAggregate<int, AverageState<long>, double>
    {
        public Expression<Func<AverageState<long>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<long>, long, int, AverageState<long>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count + 1, Sum = oldState.Sum + input };

        public Expression<Func<AverageState<long>, long, int, AverageState<long>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count - 1, Sum = oldState.Sum - input };

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Difference()
            => (left, right) => new AverageState<long> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<long>, double>> ComputeResult()
            => state => (double)state.Sum / state.Count;
    }

    internal class AverageNullableIntAggregate : IAggregate<int?, AverageState<long>, double?>
    {
        public Expression<Func<AverageState<long>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<long>, long, int?, AverageState<long>>> Accumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<long> { Count = oldState.Count + 1, Sum = oldState.Sum + input.Value } : oldState;

        public Expression<Func<AverageState<long>, long, int?, AverageState<long>>> Deaccumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<long> { Count = oldState.Count - 1, Sum = oldState.Sum - input.Value } : oldState;

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Difference()
            => (left, right) => new AverageState<long> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<long>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal class AverageFilterableIntAggregate : IAggregate<int, AverageState<long>, double?>
    {
        public Expression<Func<AverageState<long>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<long>, long, int, AverageState<long>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count + 1, Sum = oldState.Sum + input };

        public Expression<Func<AverageState<long>, long, int, AverageState<long>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count - 1, Sum = oldState.Sum - input };

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Difference()
            => (left, right) => new AverageState<long> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<long>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal class AverageLongAggregate : IAggregate<long, AverageState<long>, double>
    {
        public Expression<Func<AverageState<long>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<long>, long, long, AverageState<long>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count + 1, Sum = oldState.Sum + input };

        public Expression<Func<AverageState<long>, long, long, AverageState<long>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count - 1, Sum = oldState.Sum - input };

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Difference()
            => (left, right) => new AverageState<long> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<long>, double>> ComputeResult()
            => state => (double)state.Sum / state.Count;
    }

    internal class AverageNullableLongAggregate : IAggregate<long?, AverageState<long>, double?>
    {
        public Expression<Func<AverageState<long>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<long>, long, long?, AverageState<long>>> Accumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<long> { Count = oldState.Count + 1, Sum = oldState.Sum + input.Value } : oldState;

        public Expression<Func<AverageState<long>, long, long?, AverageState<long>>> Deaccumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<long> { Count = oldState.Count - 1, Sum = oldState.Sum - input.Value } : oldState;

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Difference()
            => (left, right) => new AverageState<long> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<long>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal class AverageFilterableLongAggregate : IAggregate<long, AverageState<long>, double?>
    {
        public Expression<Func<AverageState<long>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<long>, long, long, AverageState<long>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count + 1, Sum = oldState.Sum + input };

        public Expression<Func<AverageState<long>, long, long, AverageState<long>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<long> { Count = oldState.Count - 1, Sum = oldState.Sum - input };

        public Expression<Func<AverageState<long>, AverageState<long>, AverageState<long>>> Difference()
            => (left, right) => new AverageState<long> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<long>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal class AverageByteAggregate : IAggregate<byte, AverageState<ulong>, double>
    {
        public Expression<Func<AverageState<ulong>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<ulong>, long, byte, AverageState<ulong>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count + 1, Sum = oldState.Sum + input };

        public Expression<Func<AverageState<ulong>, long, byte, AverageState<ulong>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count - 1, Sum = oldState.Sum - input };

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Difference()
            => (left, right) => new AverageState<ulong> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<ulong>, double>> ComputeResult()
            => state => (double)state.Sum / state.Count;
    }

    internal class AverageNullableByteAggregate : IAggregate<byte?, AverageState<ulong>, double?>
    {
        public Expression<Func<AverageState<ulong>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<ulong>, long, byte?, AverageState<ulong>>> Accumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<ulong> { Count = oldState.Count + 1, Sum = oldState.Sum + input.Value } : oldState;

        public Expression<Func<AverageState<ulong>, long, byte?, AverageState<ulong>>> Deaccumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<ulong> { Count = oldState.Count - 1, Sum = oldState.Sum - input.Value } : oldState;

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Difference()
            => (left, right) => new AverageState<ulong> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<ulong>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal class AverageFilterableByteAggregate : IAggregate<byte, AverageState<ulong>, double?>
    {
        public Expression<Func<AverageState<ulong>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<ulong>, long, byte, AverageState<ulong>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count + 1, Sum = oldState.Sum + input };

        public Expression<Func<AverageState<ulong>, long, byte, AverageState<ulong>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count - 1, Sum = oldState.Sum - input };

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Difference()
            => (left, right) => new AverageState<ulong> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<ulong>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal class AverageUShortAggregate : IAggregate<ushort, AverageState<ulong>, double>
    {
        public Expression<Func<AverageState<ulong>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<ulong>, long, ushort, AverageState<ulong>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count + 1, Sum = oldState.Sum + input };

        public Expression<Func<AverageState<ulong>, long, ushort, AverageState<ulong>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count - 1, Sum = oldState.Sum - input };

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Difference()
            => (left, right) => new AverageState<ulong> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<ulong>, double>> ComputeResult()
            => state => (double)state.Sum / state.Count;
    }

    internal class AverageNullableUShortAggregate : IAggregate<ushort?, AverageState<ulong>, double?>
    {
        public Expression<Func<AverageState<ulong>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<ulong>, long, ushort?, AverageState<ulong>>> Accumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<ulong> { Count = oldState.Count + 1, Sum = oldState.Sum + input.Value } : oldState;

        public Expression<Func<AverageState<ulong>, long, ushort?, AverageState<ulong>>> Deaccumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<ulong> { Count = oldState.Count - 1, Sum = oldState.Sum - input.Value } : oldState;

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Difference()
            => (left, right) => new AverageState<ulong> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<ulong>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal class AverageFilterableUShortAggregate : IAggregate<ushort, AverageState<ulong>, double?>
    {
        public Expression<Func<AverageState<ulong>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<ulong>, long, ushort, AverageState<ulong>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count + 1, Sum = oldState.Sum + input };

        public Expression<Func<AverageState<ulong>, long, ushort, AverageState<ulong>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count - 1, Sum = oldState.Sum - input };

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Difference()
            => (left, right) => new AverageState<ulong> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<ulong>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal class AverageUIntAggregate : IAggregate<uint, AverageState<ulong>, double>
    {
        public Expression<Func<AverageState<ulong>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<ulong>, long, uint, AverageState<ulong>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count + 1, Sum = oldState.Sum + input };

        public Expression<Func<AverageState<ulong>, long, uint, AverageState<ulong>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count - 1, Sum = oldState.Sum - input };

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Difference()
            => (left, right) => new AverageState<ulong> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<ulong>, double>> ComputeResult()
            => state => (double)state.Sum / state.Count;
    }

    internal class AverageNullableUIntAggregate : IAggregate<uint?, AverageState<ulong>, double?>
    {
        public Expression<Func<AverageState<ulong>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<ulong>, long, uint?, AverageState<ulong>>> Accumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<ulong> { Count = oldState.Count + 1, Sum = oldState.Sum + input.Value } : oldState;

        public Expression<Func<AverageState<ulong>, long, uint?, AverageState<ulong>>> Deaccumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<ulong> { Count = oldState.Count - 1, Sum = oldState.Sum - input.Value } : oldState;

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Difference()
            => (left, right) => new AverageState<ulong> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<ulong>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal class AverageFilterableUIntAggregate : IAggregate<uint, AverageState<ulong>, double?>
    {
        public Expression<Func<AverageState<ulong>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<ulong>, long, uint, AverageState<ulong>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count + 1, Sum = oldState.Sum + input };

        public Expression<Func<AverageState<ulong>, long, uint, AverageState<ulong>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count - 1, Sum = oldState.Sum - input };

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Difference()
            => (left, right) => new AverageState<ulong> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<ulong>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal class AverageULongAggregate : IAggregate<ulong, AverageState<ulong>, double>
    {
        public Expression<Func<AverageState<ulong>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<ulong>, long, ulong, AverageState<ulong>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count + 1, Sum = oldState.Sum + input };

        public Expression<Func<AverageState<ulong>, long, ulong, AverageState<ulong>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count - 1, Sum = oldState.Sum - input };

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Difference()
            => (left, right) => new AverageState<ulong> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<ulong>, double>> ComputeResult()
            => state => (double)state.Sum / state.Count;
    }

    internal class AverageNullableULongAggregate : IAggregate<ulong?, AverageState<ulong>, double?>
    {
        public Expression<Func<AverageState<ulong>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<ulong>, long, ulong?, AverageState<ulong>>> Accumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<ulong> { Count = oldState.Count + 1, Sum = oldState.Sum + input.Value } : oldState;

        public Expression<Func<AverageState<ulong>, long, ulong?, AverageState<ulong>>> Deaccumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<ulong> { Count = oldState.Count - 1, Sum = oldState.Sum - input.Value } : oldState;

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Difference()
            => (left, right) => new AverageState<ulong> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<ulong>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal class AverageFilterableULongAggregate : IAggregate<ulong, AverageState<ulong>, double?>
    {
        public Expression<Func<AverageState<ulong>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<ulong>, long, ulong, AverageState<ulong>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count + 1, Sum = oldState.Sum + input };

        public Expression<Func<AverageState<ulong>, long, ulong, AverageState<ulong>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<ulong> { Count = oldState.Count - 1, Sum = oldState.Sum - input };

        public Expression<Func<AverageState<ulong>, AverageState<ulong>, AverageState<ulong>>> Difference()
            => (left, right) => new AverageState<ulong> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<ulong>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal class AverageFloatAggregate : IAggregate<float, AverageState<float>, float>
    {
        public Expression<Func<AverageState<float>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<float>, long, float, AverageState<float>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<float> { Count = oldState.Count + 1, Sum = oldState.Sum + input };

        public Expression<Func<AverageState<float>, long, float, AverageState<float>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<float> { Count = oldState.Count - 1, Sum = oldState.Sum - input };

        public Expression<Func<AverageState<float>, AverageState<float>, AverageState<float>>> Difference()
            => (left, right) => new AverageState<float> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<float>, float>> ComputeResult()
            => state => (float)state.Sum / state.Count;
    }

    internal class AverageNullableFloatAggregate : IAggregate<float?, AverageState<float>, float?>
    {
        public Expression<Func<AverageState<float>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<float>, long, float?, AverageState<float>>> Accumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<float> { Count = oldState.Count + 1, Sum = oldState.Sum + input.Value } : oldState;

        public Expression<Func<AverageState<float>, long, float?, AverageState<float>>> Deaccumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<float> { Count = oldState.Count - 1, Sum = oldState.Sum - input.Value } : oldState;

        public Expression<Func<AverageState<float>, AverageState<float>, AverageState<float>>> Difference()
            => (left, right) => new AverageState<float> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<float>, float?>> ComputeResult()
            => state => state.Count != 0 ? (float)state.Sum / state.Count : (float?)null;
    }

    internal class AverageFilterableFloatAggregate : IAggregate<float, AverageState<float>, float?>
    {
        public Expression<Func<AverageState<float>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<float>, long, float, AverageState<float>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<float> { Count = oldState.Count + 1, Sum = oldState.Sum + input };

        public Expression<Func<AverageState<float>, long, float, AverageState<float>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<float> { Count = oldState.Count - 1, Sum = oldState.Sum - input };

        public Expression<Func<AverageState<float>, AverageState<float>, AverageState<float>>> Difference()
            => (left, right) => new AverageState<float> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<float>, float?>> ComputeResult()
            => state => state.Count != 0 ? (float)state.Sum / state.Count : (float?)null;
    }

    internal class AverageDoubleAggregate : IAggregate<double, AverageState<double>, double>
    {
        public Expression<Func<AverageState<double>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<double>, long, double, AverageState<double>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<double> { Count = oldState.Count + 1, Sum = oldState.Sum + input };

        public Expression<Func<AverageState<double>, long, double, AverageState<double>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<double> { Count = oldState.Count - 1, Sum = oldState.Sum - input };

        public Expression<Func<AverageState<double>, AverageState<double>, AverageState<double>>> Difference()
            => (left, right) => new AverageState<double> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<double>, double>> ComputeResult()
            => state => (double)state.Sum / state.Count;
    }

    internal class AverageNullableDoubleAggregate : IAggregate<double?, AverageState<double>, double?>
    {
        public Expression<Func<AverageState<double>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<double>, long, double?, AverageState<double>>> Accumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<double> { Count = oldState.Count + 1, Sum = oldState.Sum + input.Value } : oldState;

        public Expression<Func<AverageState<double>, long, double?, AverageState<double>>> Deaccumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<double> { Count = oldState.Count - 1, Sum = oldState.Sum - input.Value } : oldState;

        public Expression<Func<AverageState<double>, AverageState<double>, AverageState<double>>> Difference()
            => (left, right) => new AverageState<double> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<double>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal class AverageFilterableDoubleAggregate : IAggregate<double, AverageState<double>, double?>
    {
        public Expression<Func<AverageState<double>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<double>, long, double, AverageState<double>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<double> { Count = oldState.Count + 1, Sum = oldState.Sum + input };

        public Expression<Func<AverageState<double>, long, double, AverageState<double>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<double> { Count = oldState.Count - 1, Sum = oldState.Sum - input };

        public Expression<Func<AverageState<double>, AverageState<double>, AverageState<double>>> Difference()
            => (left, right) => new AverageState<double> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<double>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal class AverageDecimalAggregate : IAggregate<decimal, AverageState<decimal>, decimal>
    {
        public Expression<Func<AverageState<decimal>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<decimal>, long, decimal, AverageState<decimal>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<decimal> { Count = oldState.Count + 1, Sum = oldState.Sum + input };

        public Expression<Func<AverageState<decimal>, long, decimal, AverageState<decimal>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<decimal> { Count = oldState.Count - 1, Sum = oldState.Sum - input };

        public Expression<Func<AverageState<decimal>, AverageState<decimal>, AverageState<decimal>>> Difference()
            => (left, right) => new AverageState<decimal> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<decimal>, decimal>> ComputeResult()
            => state => (decimal)state.Sum / state.Count;
    }

    internal class AverageNullableDecimalAggregate : IAggregate<decimal?, AverageState<decimal>, decimal?>
    {
        public Expression<Func<AverageState<decimal>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<decimal>, long, decimal?, AverageState<decimal>>> Accumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<decimal> { Count = oldState.Count + 1, Sum = oldState.Sum + input.Value } : oldState;

        public Expression<Func<AverageState<decimal>, long, decimal?, AverageState<decimal>>> Deaccumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<decimal> { Count = oldState.Count - 1, Sum = oldState.Sum - input.Value } : oldState;

        public Expression<Func<AverageState<decimal>, AverageState<decimal>, AverageState<decimal>>> Difference()
            => (left, right) => new AverageState<decimal> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<decimal>, decimal?>> ComputeResult()
            => state => state.Count != 0 ? (decimal)state.Sum / state.Count : (decimal?)null;
    }

    internal class AverageFilterableDecimalAggregate : IAggregate<decimal, AverageState<decimal>, decimal?>
    {
        public Expression<Func<AverageState<decimal>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<decimal>, long, decimal, AverageState<decimal>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<decimal> { Count = oldState.Count + 1, Sum = oldState.Sum + input };

        public Expression<Func<AverageState<decimal>, long, decimal, AverageState<decimal>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<decimal> { Count = oldState.Count - 1, Sum = oldState.Sum - input };

        public Expression<Func<AverageState<decimal>, AverageState<decimal>, AverageState<decimal>>> Difference()
            => (left, right) => new AverageState<decimal> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<decimal>, decimal?>> ComputeResult()
            => state => state.Count != 0 ? (decimal)state.Sum / state.Count : (decimal?)null;
    }

    internal class AverageBigIntegerAggregate : IAggregate<BigInteger, AverageState<BigInteger>, double>
    {
        public Expression<Func<AverageState<BigInteger>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<BigInteger>, long, BigInteger, AverageState<BigInteger>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<BigInteger> { Count = oldState.Count + 1, Sum = oldState.Sum + input };

        public Expression<Func<AverageState<BigInteger>, long, BigInteger, AverageState<BigInteger>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<BigInteger> { Count = oldState.Count - 1, Sum = oldState.Sum - input };

        public Expression<Func<AverageState<BigInteger>, AverageState<BigInteger>, AverageState<BigInteger>>> Difference()
            => (left, right) => new AverageState<BigInteger> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<BigInteger>, double>> ComputeResult()
            => state => (double)state.Sum / state.Count;
    }

    internal class AverageNullableBigIntegerAggregate : IAggregate<BigInteger?, AverageState<BigInteger>, double?>
    {
        public Expression<Func<AverageState<BigInteger>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<BigInteger>, long, BigInteger?, AverageState<BigInteger>>> Accumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<BigInteger> { Count = oldState.Count + 1, Sum = oldState.Sum + input.Value } : oldState;

        public Expression<Func<AverageState<BigInteger>, long, BigInteger?, AverageState<BigInteger>>> Deaccumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<BigInteger> { Count = oldState.Count - 1, Sum = oldState.Sum - input.Value } : oldState;

        public Expression<Func<AverageState<BigInteger>, AverageState<BigInteger>, AverageState<BigInteger>>> Difference()
            => (left, right) => new AverageState<BigInteger> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<BigInteger>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal class AverageFilterableBigIntegerAggregate : IAggregate<BigInteger, AverageState<BigInteger>, double?>
    {
        public Expression<Func<AverageState<BigInteger>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<BigInteger>, long, BigInteger, AverageState<BigInteger>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<BigInteger> { Count = oldState.Count + 1, Sum = oldState.Sum + input };

        public Expression<Func<AverageState<BigInteger>, long, BigInteger, AverageState<BigInteger>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<BigInteger> { Count = oldState.Count - 1, Sum = oldState.Sum - input };

        public Expression<Func<AverageState<BigInteger>, AverageState<BigInteger>, AverageState<BigInteger>>> Difference()
            => (left, right) => new AverageState<BigInteger> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<BigInteger>, double?>> ComputeResult()
            => state => state.Count != 0 ? (double)state.Sum / state.Count : (double?)null;
    }

    internal class AverageComplexAggregate : IAggregate<Complex, AverageState<Complex>, Complex>
    {
        public Expression<Func<AverageState<Complex>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<Complex>, long, Complex, AverageState<Complex>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<Complex> { Count = oldState.Count + 1, Sum = oldState.Sum + input };

        public Expression<Func<AverageState<Complex>, long, Complex, AverageState<Complex>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<Complex> { Count = oldState.Count - 1, Sum = oldState.Sum - input };

        public Expression<Func<AverageState<Complex>, AverageState<Complex>, AverageState<Complex>>> Difference()
            => (left, right) => new AverageState<Complex> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<Complex>, Complex>> ComputeResult()
            => state => (Complex)state.Sum / state.Count;
    }

    internal class AverageNullableComplexAggregate : IAggregate<Complex?, AverageState<Complex>, Complex?>
    {
        public Expression<Func<AverageState<Complex>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<Complex>, long, Complex?, AverageState<Complex>>> Accumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<Complex> { Count = oldState.Count + 1, Sum = oldState.Sum + input.Value } : oldState;

        public Expression<Func<AverageState<Complex>, long, Complex?, AverageState<Complex>>> Deaccumulate()
            => (oldState, timestamp, input) => input.HasValue ? new AverageState<Complex> { Count = oldState.Count - 1, Sum = oldState.Sum - input.Value } : oldState;

        public Expression<Func<AverageState<Complex>, AverageState<Complex>, AverageState<Complex>>> Difference()
            => (left, right) => new AverageState<Complex> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<Complex>, Complex?>> ComputeResult()
            => state => state.Count != 0 ? (Complex)state.Sum / state.Count : (Complex?)null;
    }

    internal class AverageFilterableComplexAggregate : IAggregate<Complex, AverageState<Complex>, Complex?>
    {
        public Expression<Func<AverageState<Complex>>> InitialState()
            => () => default;

        public Expression<Func<AverageState<Complex>, long, Complex, AverageState<Complex>>> Accumulate()
            => (oldState, timestamp, input) => new AverageState<Complex> { Count = oldState.Count + 1, Sum = oldState.Sum + input };

        public Expression<Func<AverageState<Complex>, long, Complex, AverageState<Complex>>> Deaccumulate()
            => (oldState, timestamp, input) => new AverageState<Complex> { Count = oldState.Count - 1, Sum = oldState.Sum - input };

        public Expression<Func<AverageState<Complex>, AverageState<Complex>, AverageState<Complex>>> Difference()
            => (left, right) => new AverageState<Complex> { Count = left.Count - right.Count, Sum = left.Sum - right.Sum };

        public Expression<Func<AverageState<Complex>, Complex?>> ComputeResult()
            => state => state.Count != 0 ? (Complex)state.Sum / state.Count : (Complex?)null;
    }
}
