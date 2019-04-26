// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// A node in the query plan representing a lifetime extension ("ExtendLifetime") operation in the active, running query.
    /// </summary>
    public sealed class ExtendLifetimePlanNode : UnaryPlanNode
    {
        internal ExtendLifetimePlanNode(
            PlanNode previous, IQueryObject pipe,
            Type keyType, Type payloadType,
            bool isGenerated, string errorMessages, bool negative)
            : base(previous, pipe, keyType, payloadType, payloadType, isGenerated, errorMessages)
            => this.Negative = negative;

        /// <summary>
        /// Indicates that the current node is a lifetime extension operation.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.ExtendLifetime;

        /// <summary>
        /// States whether the "Negative" version of the operator was used
        /// </summary>
        public bool Negative { get; private set; }
    }
}
