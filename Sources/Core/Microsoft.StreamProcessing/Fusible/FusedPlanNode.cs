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
    public sealed class FusedPlanNode : PlanNode
    {
        internal FusedPlanNode(IEgressStreamObserver fused, Type keyType, Type payloadType, bool isGenerated, string errorMessages)
            : base(fused, keyType, payloadType, isGenerated, errorMessages)
        { }

        /// <summary>
        /// Returns the kind of plan node, which can then be used for type casting.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.Fused;

        internal override void CollectDormantIngressSites(Dictionary<string, IngressPlanNode> working)
        { }

        internal override void PrintConciseSummary(StringBuilder builder, int indentLevel)
        {
            PrintConciseGeneralState(builder, indentLevel);
        }

        /// <summary>
        /// Visitor pattern acceptor method.
        /// </summary>
        /// <param name="visitor"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void Accept(IPlanNodeVisitor visitor)
        {
            visitor.Visit(this);
        }
    }
}
