// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using Microsoft.StreamProcessing.Aggregates;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// A node in the query plan representing a batched window snapshot operation.
    /// </summary>
    /// <typeparam name="TInput">Input type of the aggregate operation</typeparam>
    /// <typeparam name="TState">State type of the aggregate operation</typeparam>
    /// <typeparam name="TResult">Result type of the aggregate operation</typeparam>
    public sealed class SnapshotWindowPlanNode<TInput, TState, TResult> : UnaryPlanNode
    {
        /// <summary>
        /// States what kind of algorithm is being used for the computation of aggregate state.
        /// </summary>
        public AggregatePipeType InternalAggregateType { get; }

        /// <summary>
        /// States precisely what aggregate is being computed.
        /// </summary>
        public IAggregate<TInput, TState, TResult> Aggregate { get; }

        internal SnapshotWindowPlanNode(
            PlanNode previous,
            IQueryObject pipe,
            Type keyType,
            Type inputType,
            Type outputType,
            AggregatePipeType type,
            IAggregate<TInput, TState, TResult> aggregate,
            bool isGenerated, string errorMessages)
            : base(previous, pipe, keyType, outputType, inputType, isGenerated, errorMessages)
        {
            this.InternalAggregateType = type;
            this.Aggregate = aggregate;
        }

        /// <summary>
        /// Indicates that the current node is a snapshot window aggregate operation.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.SnapshotWindow;
    }
}
