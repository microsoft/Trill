// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.ComponentModel;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Represents an ungrouping operation in a Map-Reduce process.
    /// </summary>
    public sealed class UngroupPlanNode : PlanNode
    {
        /// <summary>
        /// The previous node in the plan, the input to the ungrouping operation.
        /// </summary>
        public PlanNode Previous { get; }

        /// <summary>
        /// The expression used to retrieve elements relative to a grouping key.
        /// </summary>
        public Expression ResultExpression { get; }

        /// <summary>
        /// The type of the grouping key of the input stream.
        /// </summary>
        public Type InputKeyType { get; }

        /// <summary>
        /// The payload type of the input groups.
        /// </summary>
        public Type GroupPayloadType { get; }

        internal UngroupPlanNode(
            PlanNode previous,
            IQueryObject pipe,
            Type inputKey,
            Type outputKey,
            Type groupPayloadType,
            Type payloadType,
            Expression resultSelector,
            bool isGenerated,
            string errorMessages)
            : base(pipe, outputKey, payloadType, isGenerated, errorMessages)
        {
            this.Previous = previous;
            this.ResultExpression = resultSelector;
            this.InputKeyType = inputKey;
            this.GroupPayloadType = groupPayloadType;
        }

        /// <summary>
        /// Indicates that the current node is an ungroup operation.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.Ungroup;

        internal override void PrintConciseSummary(System.Text.StringBuilder builder, int indentLevel)
        {
            PrintConciseGeneralState(builder, indentLevel);
            builder.AppendLine(new string('\t', indentLevel + 1) + "Previous: ");
            this.Previous.PrintConciseSummary(builder, indentLevel + 2);
        }

        internal override void CollectDormantIngressSites(System.Collections.Generic.Dictionary<string, IngressPlanNode> working)
        {
            this.Previous.CollectDormantIngressSites(working);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="visitor"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void Accept(IPlanNodeVisitor visitor)
        {
            this.Previous.Accept(visitor);
            visitor.Visit(this);
        }
    }
}
