// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// A node in the query plan representing a session window truncation ("SessionWindow") operation in the active, running query.
    /// </summary>
    public sealed class SessionWindowPlanNode : UnaryPlanNode
    {
        /// <summary>
        /// Session Window Duration
        /// </summary>
        public long SessionDuration { get; }

        internal SessionWindowPlanNode(
            PlanNode previous,
            IQueryObject pipe,
            Type keyType,
            Type payloadType,
            long sessionDuration,
            bool isGenerated,
            string errorMessages)
            : base(previous, pipe, keyType, payloadType, payloadType, isGenerated, errorMessages)
        {
            this.SessionDuration = sessionDuration;
        }

        /// <summary>
        /// Indicates that the current node is a session window operation.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.SessionWindow;
    }
}
