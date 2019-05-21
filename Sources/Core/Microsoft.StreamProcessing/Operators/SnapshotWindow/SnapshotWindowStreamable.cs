// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Aggregates;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// An enumeration of the available state management algorithm types.
    /// </summary>
    public enum AggregatePipeType
    {
        /// <summary>
        /// States that the aggregate is being computed over a monotonically increasing set of data.
        /// </summary>
        StartEdge,

        /// <summary>
        /// States that the aggregate is being computed over a sliding window, so state management is done using a queue of unknown size cap.
        /// </summary>
        Sliding,

        /// <summary>
        /// States that the aggregate is being computed over a hopping window, so state management is done using a queue of known size cap.
        /// </summary>
        Hopping,

        /// <summary>
        /// States that a priority queue is being used to manage aggregate states.
        /// </summary>
        PriorityQueue,

        /// <summary>
        /// States that the aggregate is being computed over a tumbling window and that state management is at its simplest.
        /// </summary>
        Tumbling
    }

    [DataContract]
    internal sealed class StateAndActive<TState>
    {
        [DataMember]
        public TState state;

        [DataMember]
        public ulong active;
    }

    [DataContract]
    internal sealed class HeldState<TState>
    {
        [DataMember]
        public long timestamp;

        [DataMember]
        public TState state;

        [DataMember]
        public ulong active;
    }

    internal sealed class SnapshotWindowStreamable<TKey, TInput, TState, TOutput> :
        UnaryStreamable<TKey, TInput, TOutput>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        private readonly StreamProperties<TKey, TInput> sourceProps;
        private readonly AggregatePipeType apt;
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Used to avoid creating redundant readonly property.")]
        internal readonly IAggregate<TInput, TState, TOutput> Aggregate;

        public SnapshotWindowStreamable(IStreamable<TKey, TInput> source, IAggregate<TInput, TState, TOutput> aggregate)
            : base(source, source.Properties.Snapshot(aggregate))
        {
            Contract.Requires(source != null);

            this.Aggregate = aggregate;
            this.sourceProps = source.Properties;

            if (this.sourceProps.IsStartEdgeOnly)
                this.apt = AggregatePipeType.StartEdge;
            else if (this.sourceProps.IsConstantDuration)
            {
                if (this.sourceProps.IsTumbling)
                    this.apt = AggregatePipeType.Tumbling;
                else if (this.sourceProps.ConstantDurationLength.HasValue && this.sourceProps.IsConstantHop && this.sourceProps.ConstantHopLength.HasValue)
                    this.apt = AggregatePipeType.Hopping;
                else
                    this.apt = AggregatePipeType.Sliding;
            }
            else
                this.apt = AggregatePipeType.PriorityQueue;

            Initialize();
        }

        private static int GetAggregateFunctionsHashCode(IAggregate<TInput, TState, TOutput> a)
        {
            int x = a.Accumulate().ExpressionToCSharp().StableHash();
            x = x << 3 ^ a.ComputeResult().ExpressionToCSharp().StableHash();
            x = x << 3 ^ a.Deaccumulate().ExpressionToCSharp().StableHash();
            x = x << 3 ^ a.Difference().ExpressionToCSharp().StableHash();
            x = x << 3 ^ a.InitialState().ExpressionToCSharp().StableHash();
            return x;
        }

        internal override IStreamObserver<TKey, TInput> CreatePipe(IStreamObserver<TKey, TOutput> observer)
        {
            switch (this.apt)
            {
                case AggregatePipeType.StartEdge:
                    return CreateStartEdge(observer);
                case AggregatePipeType.Tumbling:
                    return CreateTumbling(observer);
                case AggregatePipeType.Hopping:
                    return CreateHopping(observer);
                case AggregatePipeType.Sliding:
                    return CreateSliding(observer);
                default:
                    return CreatePQ(observer);
            }
        }

        internal IStreamObserver<TKey, TInput> CreateStartEdge(IStreamObserver<TKey, TOutput> observer)
        {
            var part = typeof(TKey).GetPartitionType();
            if (part == null)
            {
                if (this.Properties.IsColumnar)
                {
                    var tuple = GetPipe();
                    Func<PlanNode, IQueryObject, PlanNode> planNode = ((PlanNode p, IQueryObject o) => new SnapshotWindowPlanNode<TInput, TState, TOutput>(
                        p, o, typeof(TKey), typeof(TInput), typeof(TOutput), this.apt, this.Aggregate, true, tuple.Item2));
                    var instance = Activator.CreateInstance(tuple.Item1, this, observer, planNode, this.Aggregate);
                    var returnValue = (IStreamObserver<TKey, TInput>)instance;
                    return returnValue;
                }
                else if (typeof(TKey) == typeof(Empty))
                {
                    var outputType = typeof(SnapshotWindowStartEdgePipeSimple<,,>).MakeGenericType(
                        typeof(TInput),
                        typeof(TState),
                        typeof(TOutput));
                    return (IStreamObserver<TKey, TInput>)Activator.CreateInstance(outputType, this, observer);
                }
                return new SnapshotWindowStartEdgePipe<TKey, TInput, TState, TOutput>(this, observer);
            }
            else
            {
                if (typeof(TKey).GetGenericTypeDefinition() == typeof(PartitionKey<>))
                {
                    var outputType = typeof(PartitionedSnapshotWindowStartEdgePipeSimple<,,,>).MakeGenericType(
                        typeof(TInput),
                        typeof(TState),
                        typeof(TOutput),
                        part);
                    return (IStreamObserver<TKey, TInput>)Activator.CreateInstance(outputType, this, observer);
                }
                else
                {
                    var outputType = typeof(PartitionedSnapshotWindowStartEdgePipe<,,,,>).MakeGenericType(
                        typeof(TKey),
                        typeof(TInput),
                        typeof(TState),
                        typeof(TOutput),
                        part);
                    return (IStreamObserver<TKey, TInput>)Activator.CreateInstance(outputType, this, observer);
                }
            }
        }

        internal IStreamObserver<TKey, TInput> CreateTumbling(IStreamObserver<TKey, TOutput> observer)
        {
            var part = typeof(TKey).GetPartitionType();
            if (part == null)
            {
                if (this.Properties.IsColumnar)
                {
                    var tuple = GetPipe();
                    Func<PlanNode, IQueryObject, PlanNode> planNode = ((PlanNode p, IQueryObject o) => new SnapshotWindowPlanNode<TInput, TState, TOutput>(
                        p, o, typeof(TKey), typeof(TInput), typeof(TOutput), this.apt, this.Aggregate, true, tuple.Item2));
                    var instance = Activator.CreateInstance(tuple.Item1, this, observer, planNode, this.Aggregate, this.sourceProps.ConstantDurationLength.Value);
                    var returnValue = (IStreamObserver<TKey, TInput>)instance;
                    return returnValue;
                }
                else if (typeof(TKey) == typeof(Empty))
                {
                    var outputType = typeof(SnapshotWindowTumblingPipeSimple<,,>).MakeGenericType(
                        typeof(TInput),
                        typeof(TState),
                        typeof(TOutput));
                    return (IStreamObserver<TKey, TInput>)Activator.CreateInstance(outputType, this, observer, this.sourceProps.ConstantDurationLength.Value);
                }
                return new SnapshotWindowTumblingPipe<TKey, TInput, TState, TOutput>(this, observer, this.sourceProps.ConstantDurationLength.Value);
            }
            else
            {
                if (typeof(TKey).GetGenericTypeDefinition() == typeof(PartitionKey<>))
                {
                    var outputType = typeof(PartitionedSnapshotWindowTumblingPipeSimple<,,,>).MakeGenericType(
                        typeof(TInput),
                        typeof(TState),
                        typeof(TOutput),
                        part);
                    return (IStreamObserver<TKey, TInput>)Activator.CreateInstance(outputType, this, observer, this.sourceProps.ConstantDurationLength.Value);
                }
                else
                {
                    var outputType = typeof(PartitionedSnapshotWindowTumblingPipe<,,,,>).MakeGenericType(
                        typeof(TKey),
                        typeof(TInput),
                        typeof(TState),
                        typeof(TOutput),
                        part);
                    return (IStreamObserver<TKey, TInput>)Activator.CreateInstance(outputType, this, observer, this.sourceProps.ConstantDurationLength.Value);
                }
            }
        }

        internal IStreamObserver<TKey, TInput> CreateHopping(IStreamObserver<TKey, TOutput> observer)
        {
            var part = typeof(TKey).GetPartitionType();
            if (part == null)
            {
                if (this.Properties.IsColumnar)
                {
                    var tuple = GetPipe();
                    Func<PlanNode, IQueryObject, PlanNode> planNode = ((PlanNode p, IQueryObject o) => new SnapshotWindowPlanNode<TInput, TState, TOutput>(
                        p, o, typeof(TKey), typeof(TInput), typeof(TOutput), this.apt, this.Aggregate, true, tuple.Item2));
                    var instance = Activator.CreateInstance(tuple.Item1, this, observer, planNode, this.Aggregate);
                    var returnValue = (IStreamObserver<TKey, TInput>)instance;
                    return returnValue;
                }
                else if (typeof(TKey) == typeof(Empty))
                {
                    var outputType = typeof(SnapshotWindowHoppingPipeSimple<,,>).MakeGenericType(
                        typeof(TInput),
                        typeof(TState),
                        typeof(TOutput));
                    return (IStreamObserver<TKey, TInput>)Activator.CreateInstance(outputType, this, observer);
                }
                return new SnapshotWindowHoppingPipe<TKey, TInput, TState, TOutput>(this, observer);
            }
            else
            {
                if (typeof(TKey).GetGenericTypeDefinition() == typeof(PartitionKey<>))
                {
                    var outputType = typeof(PartitionedSnapshotWindowHoppingPipeSimple<,,,>).MakeGenericType(
                        typeof(TInput),
                        typeof(TState),
                        typeof(TOutput),
                        part);
                    return (IStreamObserver<TKey, TInput>)Activator.CreateInstance(outputType, this, observer);
                }
                else
                {
                    var outputType = typeof(PartitionedSnapshotWindowHoppingPipe<,,,,>).MakeGenericType(
                        typeof(TKey),
                        typeof(TInput),
                        typeof(TState),
                        typeof(TOutput),
                        part);
                    return (IStreamObserver<TKey, TInput>)Activator.CreateInstance(outputType, this, observer);
                }
            }
        }

        internal IStreamObserver<TKey, TInput> CreateSliding(IStreamObserver<TKey, TOutput> observer)
        {
            var part = typeof(TKey).GetPartitionType();
            if (part == null)
            {
                if (this.Properties.IsColumnar)
                {
                    var tuple = GetPipe();
                    Func<PlanNode, IQueryObject, PlanNode> planNode = ((PlanNode p, IQueryObject o) => new SnapshotWindowPlanNode<TInput, TState, TOutput>(
                        p, o, typeof(TKey), typeof(TInput), typeof(TOutput), this.apt, this.Aggregate, true, tuple.Item2));
                    var instance = Activator.CreateInstance(tuple.Item1, this, observer, planNode, this.Aggregate);
                    var returnValue = (IStreamObserver<TKey, TInput>)instance;
                    return returnValue;
                }
                else if (typeof(TKey) == typeof(Empty))
                {
                    var outputType = typeof(SnapshotWindowSlidingPipeSimple<,,>).MakeGenericType(
                        typeof(TInput),
                        typeof(TState),
                        typeof(TOutput));
                    return (IStreamObserver<TKey, TInput>)Activator.CreateInstance(outputType, this, observer);
                }
                return new SnapshotWindowSlidingPipe<TKey, TInput, TState, TOutput>(this, observer);
            }
            else
            {
                if (typeof(TKey).GetGenericTypeDefinition() == typeof(PartitionKey<>))
                {
                    var outputType = typeof(PartitionedSnapshotWindowSlidingPipeSimple<,,,>).MakeGenericType(
                        typeof(TInput),
                        typeof(TState),
                        typeof(TOutput),
                        part);
                    return (IStreamObserver<TKey, TInput>)Activator.CreateInstance(outputType, this, observer);
                }
                else
                {
                    var outputType = typeof(PartitionedSnapshotWindowSlidingPipe<,,,,>).MakeGenericType(
                        typeof(TKey),
                        typeof(TInput),
                        typeof(TState),
                        typeof(TOutput),
                        part);
                    return (IStreamObserver<TKey, TInput>)Activator.CreateInstance(outputType, this, observer);
                }
            }
        }

        internal IStreamObserver<TKey, TInput> CreatePQ(IStreamObserver<TKey, TOutput> observer)
        {
            var part = typeof(TKey).GetPartitionType();
            if (part == null)
            {
                if (this.Properties.IsColumnar)
                {
                    var tuple = GetPipe();
                    Func<PlanNode, IQueryObject, PlanNode> planNode = ((PlanNode p, IQueryObject o) => new SnapshotWindowPlanNode<TInput, TState, TOutput>(
                        p, o, typeof(TKey), typeof(TInput), typeof(TOutput), this.apt, this.Aggregate, true, tuple.Item2));
                    var instance = Activator.CreateInstance(tuple.Item1, this, observer, planNode, this.Aggregate);
                    var returnValue = (IStreamObserver<TKey, TInput>)instance;
                    return returnValue;
                }
                else if (typeof(TKey) == typeof(Empty))
                {
                    var outputType = typeof(SnapshotWindowPriorityQueuePipeSimple<,,>).MakeGenericType(
                        typeof(TInput),
                        typeof(TState),
                        typeof(TOutput));
                    return (IStreamObserver<TKey, TInput>)Activator.CreateInstance(outputType, this, observer);
                }
                return new SnapshotWindowPriorityQueuePipe<TKey, TInput, TState, TOutput>(this, observer);
            }
            else
            {
                if (typeof(TKey).GetGenericTypeDefinition() == typeof(PartitionKey<>))
                {
                    var outputType = typeof(PartitionedSnapshotWindowPriorityQueuePipeSimple<,,,>).MakeGenericType(
                        typeof(TInput),
                        typeof(TState),
                        typeof(TOutput),
                        part);
                    return (IStreamObserver<TKey, TInput>)Activator.CreateInstance(outputType, this, observer);
                }
                else
                {
                    var outputType = typeof(PartitionedSnapshotWindowPriorityQueuePipe<,,,,>).MakeGenericType(
                    typeof(TKey),
                    typeof(TInput),
                    typeof(TState),
                    typeof(TOutput),
                    part);
                    return (IStreamObserver<TKey, TInput>)Activator.CreateInstance(outputType, this, observer);
                }
            }
        }

        protected override bool CanGenerateColumnar()
        {
            var typeOfTKey = typeof(TKey);
            var typeOfTInput = typeof(TInput);
            var typeOfTOutput = typeof(TOutput);

            if (!typeOfTInput.CanRepresentAsColumnar()) return false;
            if (typeOfTKey.GetPartitionType() != null) return false;
            if (!typeOfTOutput.CanRepresentAsColumnar()) return false;

            var lookupKey = CacheKey.Create(this.apt, GetAggregateFunctionsHashCode(this.Aggregate));

            var tuple = cachedPipes.GetOrAdd(lookupKey, key => AggregateTemplate.Generate(this, this.apt));
            var generatedPipeType = tuple.Item1;
            this.errorMessages = tuple.Item2;

            return generatedPipeType != null;
        }

        private Tuple<Type, string> GetPipe()
        {
            var lookupKey = CacheKey.Create(this.apt, GetAggregateFunctionsHashCode(this.Aggregate));

            var tuple = cachedPipes.GetOrAdd(lookupKey, key => AggregateTemplate.Generate(this, this.apt));
            return tuple;
        }
    }
}