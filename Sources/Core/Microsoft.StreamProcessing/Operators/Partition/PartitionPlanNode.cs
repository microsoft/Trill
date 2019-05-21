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
    /// Represents a grouping operation in a Map-Reduce process.
    /// </summary>
    public sealed class PartitionPlanNode : PlanNode
    {
        /// <summary>
        /// Indicates that the current node is a grouping operation.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.Partition;

        /// <summary>
        /// The previous node in the plan, the input to the grouping operation.
        /// </summary>
        public PlanNode Previous { get; }

        /// <summary>
        /// The expression used to group elements relative to a grouping key.
        /// </summary>
        public Expression GroupingExpression { get; }

        /// <summary>
        /// The amount of time that global punctuations are delayed to account for possible partition skew.
        /// </summary>
        public long PunctuationLag { get; }

        internal PartitionPlanNode(
            PlanNode previous,
            IQueryObject pipe, Type outputKey, Type payloadType,
            Expression keySelector, long punctuationLag)
            : base(pipe, outputKey, payloadType, false, string.Empty)
        {
            this.Previous = previous;
            this.GroupingExpression = keySelector;
            this.PunctuationLag = punctuationLag;
        }

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
