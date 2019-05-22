// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// A node in the query plan representing a session window truncation ("SessionTimeoutWindow") operation in the active, running query.
    /// </summary>
    public sealed class SessionWindowPlanNode : UnaryPlanNode
    {
        /// <summary>
        /// Timeout for closing the current session
        /// </summary>
        public long SessionTimeout { get; }

        /// <summary>
        /// The max duration of a session timeout window
        /// </summary>
        public long MaximumDuration { get; }

        internal SessionWindowPlanNode(
            PlanNode previous, IQueryObject pipe,
            Type keyType, Type payloadType, long sessionTimeout, long maximumDuration,
            bool isGenerated, string errorMessages)
            : base(previous, pipe, keyType, payloadType, payloadType, isGenerated, errorMessages)
        {
            this.SessionTimeout = sessionTimeout;
            this.MaximumDuration = maximumDuration;
        }

        /// <summary>
        /// Indicates that the current node is a session window operation.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.SessionWindow;
    }
}
