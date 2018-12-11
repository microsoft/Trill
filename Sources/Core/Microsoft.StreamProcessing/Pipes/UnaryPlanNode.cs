// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// A node in the query plan representing an object with one input in the active, running query.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class UnaryPlanNode : PlanNode
    {
        /// <summary>
        /// Protected constructor for a new Unary plan node object.
        /// </summary>
        protected UnaryPlanNode(
            PlanNode previous,
            IQueryObject pipe,
            Type keyType,
            Type payloadType,
            Type inputPayloadType,
            bool isGenerated = false,
            string errorMessages = null)
            : base(pipe, keyType, payloadType, isGenerated, errorMessages)
        {
            this.PreviousPlanNode = previous;
            this.InputPayloadType = inputPayloadType;
        }

        /// <summary>
        /// States that the current node in the query plan is one that is constructed outside of the Microsoft.StreamProcessing namespace.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.UserDefined;

        /// <summary>
        /// The payload type of the input stream to the unary operator.
        /// </summary>
        public Type InputPayloadType { get; private set; }

        /// <summary>
        /// The query plan node that provides the input to this operator.
        /// </summary>
        public PlanNode PreviousPlanNode { get; private set; }

        internal override void PrintConciseSummary(StringBuilder builder, int indentLevel)
        {
            PrintConciseGeneralState(builder, indentLevel);
            builder.AppendLine(new string('\t', indentLevel + 1) + "Previous: ");
            this.PreviousPlanNode.PrintConciseSummary(builder, indentLevel + 2);
        }

        internal override void CollectDormantIngressSites(Dictionary<string, IngressPlanNode> working) => this.PreviousPlanNode.CollectDormantIngressSites(working);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void Accept(IPlanNodeVisitor visitor)
        {
            this.PreviousPlanNode.Accept(visitor);
            visitor.Visit(this);
        }
    }
}