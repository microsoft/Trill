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
    /// A node in the query plan representing a data source in the active, running query.
    /// </summary>
    public sealed class EgressPlanNode : PlanNode
    {
        internal EgressPlanNode(PlanNode previous, IEgressStreamObserver observer, Type keyType, Type payloadType, bool isGenerated, string errorMessages)
            : base(observer, keyType, payloadType, isGenerated, errorMessages)
            => this.PreviousPlanNode = previous;

        /// <summary>
        /// Returns the kind of plan node, which can then be used for type casting.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.Egress;

        /// <summary>
        /// The query plan node that provides the input to this operator.
        /// </summary>
        public PlanNode PreviousPlanNode { get; }

        internal override void CollectDormantIngressSites(Dictionary<string, IngressPlanNode> working)
            => this.PreviousPlanNode.CollectDormantIngressSites(working);

        internal override void PrintConciseSummary(StringBuilder builder, int indentLevel)
        {
            PrintConciseGeneralState(builder, indentLevel);
            builder.AppendLine(new string('\t', indentLevel + 1) + "Previous: ");
            this.PreviousPlanNode.PrintConciseSummary(builder, indentLevel + 2);
        }

        /// <summary>
        /// Visitor pattern acceptor method.
        /// </summary>
        /// <param name="visitor"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void Accept(IPlanNodeVisitor visitor)
        {
            this.PreviousPlanNode.Accept(visitor);
            visitor.Visit(this);
        }
    }
}
