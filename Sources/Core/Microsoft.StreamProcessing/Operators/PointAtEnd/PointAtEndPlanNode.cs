// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace Microsoft.StreamProcessing
{

    /// <summary>
    /// A node in the query plan representing a lifetime transformation moving the lifetime to a unit interval ("PointAtEnd") operation in the active, running query.
    /// </summary>
    public sealed class PointAtEndPlanNode : UnaryPlanNode
    {
        internal PointAtEndPlanNode(
            PlanNode previous, IQueryObject pipe,
            Type keyType, Type payloadType,
            bool isGenerated, string errorMessages)
            : base(previous, pipe, keyType, payloadType, payloadType, isGenerated, errorMessages)
        { }

        /// <summary>
        /// Indicates that the current node is an operation that adjusts every event to be an interval of length 1 beginning at the end of its original interval.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.PointAtEnd;
    }

}