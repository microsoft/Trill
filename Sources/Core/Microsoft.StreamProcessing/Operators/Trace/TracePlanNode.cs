// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// A node in the query plan representing a passthrough trace operation in the active, running query.
    /// </summary>
    public sealed class TracePlanNode : UnaryPlanNode
    {
        internal TracePlanNode(
            PlanNode previous,
            IQueryObject pipe,
            ITraceMetrics metrics,
            Type keyType,
            Type payloadType)
            : base(previous, pipe, keyType, payloadType, payloadType)
            => this.TraceMetrics = metrics;

        /// <summary>
        /// Retrieves the trace metrics for this node
        /// </summary>
        public ITraceMetrics TraceMetrics { get; }

        /// <summary>
        /// Indicates that the current node is a passthrough trace operation.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.Trace;
    }
}
