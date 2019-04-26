// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Indicates that the current node is a union operation.
    /// </summary>
    public sealed class UnionPlanNode : BinaryPlanNode
    {
        /// <summary>
        /// States whether the two inputs of the union operation are considered to be disjoint.
        /// </summary>
        public bool IsDisjoint { get; }

        internal UnionPlanNode(PlanNode left, PlanNode right, IBinaryObserver pipe, Type keyType, Type payloadType, bool isDisjoint, bool isGenerated, string errorMessages)
            : base(left, right, pipe, keyType, payloadType, payloadType, payloadType, isGenerated, errorMessages)
            => this.IsDisjoint = isDisjoint;

        /// <summary>
        /// Indicates that the current node is a union operation.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.Union;
    }
}
