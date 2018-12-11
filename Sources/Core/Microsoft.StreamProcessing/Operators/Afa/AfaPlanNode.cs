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
    public sealed class AfaPlanNode : UnaryPlanNode
    {
        internal AfaPlanNode(PlanNode previous, IQueryObject pipe, Type keyType, Type payloadType, bool isGenerated, string errorMessages)
            : base(previous, pipe, keyType, payloadType, payloadType, isGenerated, errorMessages)
        { }

        /// <summary>
        /// Indicates that the current node is an abstract automaton operation.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.Afa;
    }
}
