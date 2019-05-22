// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// A node in the query plan representing breaking up each event timeline into equal intervals.
    /// </summary>
    public sealed class BeatPlanNode : UnaryPlanNode
    {
        /// <summary>
        /// The offset value in the beat operation.
        /// </summary>
        public long Offset { get; }

        /// <summary>
        /// The period value in the beat operation.
        /// </summary>
        public long Period { get; }

        internal BeatPlanNode(PlanNode previous, IQueryObject pipe, Type keyType, Type payloadType, long offset, long period, bool isGenerated, string errorMessages)
            : base(previous, pipe, keyType, payloadType, payloadType, isGenerated, errorMessages)
        {
            this.Offset = offset;
            this.Period = period;
        }

        /// <summary>
        /// Indicates that the current node is a beat operation.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.Beat;
    }
}
