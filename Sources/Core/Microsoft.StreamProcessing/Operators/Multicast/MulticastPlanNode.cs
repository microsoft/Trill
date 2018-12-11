// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Text;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Represents an operation in a stream to allow multiple subscribers to receive the same messages.
    /// </summary>
    public sealed class MulticastPlanNode : UnaryPlanNode
    {
        private readonly Guid id = Guid.NewGuid();

        internal MulticastPlanNode(PlanNode previous, IQueryObject pipe, Type keyType, Type payloadType)
            : base(previous, pipe, keyType, payloadType, payloadType, false, null)
        { }

        /// <summary>
        /// Indicates that the current node is a multicast operation.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.Multicast;

        internal override void PrintConciseSummary(StringBuilder builder, int indentLevel)
        {
            if (!builder.ToString().Contains(this.id.ToString()))
            {
                var childBuilder = new StringBuilder();
                childBuilder.AppendLine("Multicast node " + this.id.ToString() + ":");
                this.PreviousPlanNode.PrintConciseSummary(childBuilder, 1);
                builder.Insert(0, childBuilder.ToString());
            }

            builder.AppendLine(new string('\t', indentLevel) + "Multicast operator reference: " + this.id.ToString());
        }
    }
}
