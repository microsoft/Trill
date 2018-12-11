// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// A node in the query plan representing an alteration to the event lifetimes to a variable duration in the active, running query.
    /// </summary>
    public sealed class AlterLifetimePlanNode : UnaryPlanNode
    {
        /// <summary>
        /// States the duration for which the event lifetimes will be set.
        /// </summary>
        public Expression DurationFunction { get; }

        /// <summary>
        /// States whether the duration after this operation will be constant.
        /// </summary>
        public bool IsConstantDuration { get; }

        /// <summary>
        /// The expression that is used to select event start time in this operator.
        /// </summary>
        public Expression StartTimeSelector { get; }

        internal AlterLifetimePlanNode(
            PlanNode previous, IQueryObject pipe,
            Type keyType, Type payloadType,
            Expression startTimeSelector, Expression durationFunction,
            bool isConstantDuration)
            : base(previous, pipe, keyType, payloadType, payloadType, false, null)
        {
            this.DurationFunction = durationFunction;
            this.StartTimeSelector = startTimeSelector;
            this.IsConstantDuration = isConstantDuration;
        }

        /// <summary>
        /// Indicates that the current node is a lifetime alteration operation.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.AlterLifetime;
    }
}
