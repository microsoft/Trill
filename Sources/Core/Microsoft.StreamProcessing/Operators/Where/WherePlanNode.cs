// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// A node in the query plan representing a filtration ("where") operation in the active, running query.
    /// </summary>
    public sealed class WherePlanNode : UnaryPlanNode
    {
        /// <summary>
        /// The predicate that is used to filter events in this operator.
        /// </summary>
        public Expression Predicate { get; }

        internal WherePlanNode(
            PlanNode previous, IQueryObject pipe,
            Type keyType, Type payloadType,
            Expression predicate,
            bool isGenerated, string errorMessages)
            : base(previous, pipe, keyType, payloadType, payloadType, isGenerated, errorMessages)
            => this.Predicate = predicate;

        /// <summary>
        /// Indicates that the current node is a where (filter) operation.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.Where;
    }
}
