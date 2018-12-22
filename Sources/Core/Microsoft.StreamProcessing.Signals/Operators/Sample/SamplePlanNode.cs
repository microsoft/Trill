// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace Microsoft.StreamProcessing.Signal
{
    /// <summary>
    /// A node in the query plan representing (signal) sampling operation.
    /// </summary>
    public sealed class SamplePlanNode : UnaryPlanNode
    {
        internal SamplePlanNode(PlanNode previous, IQueryObject pipe, Type keyType, Type sourceType, Type resultType, long offset, long period)
            : base(previous, pipe, keyType, sourceType, resultType, false, null)
        {
            this.Offset = offset;
            this.Period = period;
        }

        /// <summary>
        /// Returns the kind of plan node, which can then be used for type casting.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.Sample;
        /// <summary>
        /// The offset value in the sample operation.
        /// </summary>
        public long Offset { get; private set; }
        /// <summary>
        /// The period value in the sample operation.
        /// </summary>
        public long Period { get; private set; }
    }
}
