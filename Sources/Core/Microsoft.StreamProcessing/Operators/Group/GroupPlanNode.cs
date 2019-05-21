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
    public sealed class GroupPlanNode : PlanNode
    {
        /// <summary>
        /// Indicates that the current node is a grouping operation.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.Group;

        /// <summary>
        /// The previous node in the plan, the input to the grouping operation.
        /// </summary>
        public PlanNode Previous { get; }

        /// <summary>
        /// The type of the grouping key of the input stream.
        /// </summary>
        public Type InputKeyType { get; }

        /// <summary>
        /// The expression used to group elements relative to a grouping key.
        /// </summary>
        public Expression GroupingExpression { get; }

        /// <summary>
        /// States the shuffle ID of the grouping operation.  If the operator is not a shuffle, this value should be int.MinValue.
        /// </summary>
        public int ShuffleId { get; }

        /// <summary>
        /// States the number of branches participating in the grouping operation.  If the operator is not a shuffle, this value should be 1.
        /// </summary>
        public int TotalBranches { get; }

        /// <summary>
        /// States whether the current operation is a shuffle.
        /// </summary>
        public bool IsShuffle { get; }

        internal GroupPlanNode(
            PlanNode previous,
            IQueryObject pipe,
            Type inputKey, Type outputKey, Type payloadType,
            Expression keySelector,
            int shuffleId, int totalBranches,
            bool isShuffle, bool isGenerated, string errorMessages)
            : base(pipe, outputKey, payloadType, isGenerated, errorMessages)
        {
            this.Previous = previous;
            this.GroupingExpression = keySelector;
            this.InputKeyType = inputKey;
            this.ShuffleId = shuffleId;
            this.TotalBranches = totalBranches;
            this.IsShuffle = isShuffle;
        }

        internal override void PrintConciseSummary(System.Text.StringBuilder builder, int indentLevel)
        {
            PrintConciseGeneralState(builder, indentLevel);
            builder.AppendLine(new string('\t', indentLevel + 1) + "Previous: ");
            this.Previous.PrintConciseSummary(builder, indentLevel + 2);
        }

        internal override void CollectDormantIngressSites(System.Collections.Generic.Dictionary<string, IngressPlanNode> working)
            => this.Previous.CollectDormantIngressSites(working);

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
