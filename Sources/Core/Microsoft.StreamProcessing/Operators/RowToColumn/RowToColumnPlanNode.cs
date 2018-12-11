// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// A node in the query plan representing a shift from row-oriented data to column-oriented in the active, running query.
    /// </summary>
    public sealed class RowToColumnPlanNode : UnaryPlanNode
    {
        internal RowToColumnPlanNode(PlanNode previous, IQueryObject pipe, Type keyType, Type payloadType)
            : base(previous, pipe, keyType, payloadType, payloadType, true, null)
        { }

        /// <summary>
        /// Indicates that the current node is a row-to-column operation.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.RowToColumn;
    }
}
