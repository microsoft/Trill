// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// A node in the query plan representing a StatefulSelect operation in the active, running query.
    /// </summary>
    public sealed class StatefulSelectPlanNode : UnaryPlanNode
    {
        /// <summary>
        /// The expression that is used to generate output events in this operator.
        /// </summary>
        public Expression Selector { get; }

        /// <summary>
        /// States whether the expression in the selector has access to the start edge of events.
        /// </summary>
        public bool IncludesKey { get; }

        /// <summary>
        /// States whether the expression in the selector has access to the start edge of events.
        /// </summary>
        public bool IncludesStartEdge { get; }

        internal StatefulSelectPlanNode(
            PlanNode previous,
            IQueryObject pipe,
            Type keyType,
            Type inputType,
            Type payloadType,
            LambdaExpression selector,
            bool isGenerated,
            string errorMessages)
            : base(previous, pipe, keyType, payloadType, inputType, isGenerated, errorMessages)
        {
            this.Selector = selector;
        }

        /// <summary>
        /// Indicates that the current node is a select many operation.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.StatefulSelect;
    }
}
