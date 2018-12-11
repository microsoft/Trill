// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// A node in the query plan representing a lifetime truncation ("ClipByConstant") operation in the active, running query.
    /// </summary>
    public sealed class ClipByConstantPlanNode : UnaryPlanNode
    {
        internal ClipByConstantPlanNode(
            PlanNode previous, IQueryObject pipe,
            Type keyType, Type payloadType,
            bool isGenerated, string errorMessages)
            : base(previous, pipe, keyType, payloadType, payloadType, isGenerated, errorMessages)
        { }

        /// <summary>
        /// Indicates that the current node is a clip by constant operation.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.ClipByConstant;
    }
}