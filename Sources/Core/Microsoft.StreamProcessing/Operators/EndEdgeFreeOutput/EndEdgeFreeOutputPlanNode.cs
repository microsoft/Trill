// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// A node in the query plan representing function application to all input rows.
    /// </summary>
    public sealed class EndEdgeFreeOutputPlanNode : UnaryPlanNode
    {
        internal EndEdgeFreeOutputPlanNode(PlanNode previous, IQueryObject pipe, Type keyType, Type payloadType, bool isGenerated, string errorMessages)
            : base(previous, pipe, keyType, payloadType, payloadType, isGenerated, errorMessages)
        { }

        /// <summary>
        /// Indicates that the current node coalesces physical edges so that there are only intervals and no end edges.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.CoalesceEndEdges;
    }
}
