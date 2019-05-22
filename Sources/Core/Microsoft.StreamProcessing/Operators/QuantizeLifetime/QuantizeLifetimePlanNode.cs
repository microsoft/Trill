// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// A node in the query plan representing a lifetime transformation quantizing the start and end times along hopping boundaries in the active, running query.
    /// </summary>
    public sealed class QuantizeLifetimePlanNode : UnaryPlanNode
    {
        /// <summary>
        /// Represents the length of each event after transformation
        /// </summary>
        public long Width { get; }

        /// <summary>
        /// Represents the length whose multiples define the left (start) edge of each transformed lifetime
        /// </summary>
        public long Skip { get; }

        /// <summary>
        /// Represents the length whose multiples define the left (start) edge of each transformed lifetime
        /// </summary>
        public long Progress { get; }

        /// <summary>
        /// Represents the "zero" from which the interval begins skipping
        /// </summary>
        public long Offset { get; }

        internal QuantizeLifetimePlanNode(
            PlanNode previous, IQueryObject pipe,
            Type keyType, Type payloadType, long width, long skip, long progress, long offset,
            bool isGenerated, string errorMessages)
            : base(previous, pipe, keyType, payloadType, payloadType, isGenerated, errorMessages)
        {
            this.Width = width;
            this.Skip = skip;
            this.Progress = progress;
            this.Offset = offset;
        }

        /// <summary>
        /// Indicates that the current node is an operation that adjusts every event to be aligned along temporal boundaries.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.QuantizeLifetime;
    }

}