// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// A node in the query plan representing a unification of like events with adjacent lifetime intervals.
    /// </summary>
    public class StitchPlanNode : UnaryPlanNode
    {
        internal StitchPlanNode(
            PlanNode previous,
            IQueryObject pipe,
            Type keyType,
            Type payloadType,
            bool isGenerated,
            string errorMessages)
            : base(previous, pipe, keyType, payloadType, payloadType, isGenerated, errorMessages)
        { }

        /// <summary>
        /// Indicates that the current node is a stitch operation.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.Stitch;
    }
}
