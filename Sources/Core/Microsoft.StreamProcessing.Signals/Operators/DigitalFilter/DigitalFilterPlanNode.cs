// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using Microsoft.StreamProcessing.Signal.UDO;

namespace Microsoft.StreamProcessing.Signal
{
    /// <summary>
    /// A node in the query plan representing a sliding dot product operation.
    /// </summary>
    public sealed class DigitalFilterPlanNode<TPayload> : UnaryPlanNode
    {
        internal DigitalFilterPlanNode(
            PlanNode previous, IQueryObject pipe,
            Type keyType, Type payloadType,
            IDigitalFilter<TPayload> filter,
            bool isGenerated, string errorMessages)
            : base(previous, pipe, keyType, payloadType, payloadType, isGenerated, errorMessages)
            => this.DigitalFilter = filter;

        /// <summary>
        /// Returns the kind of plan node, which can then be used for type casting.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.DigitalFilter;
        /// <summary>
        /// Returns the filter associated with the active query operator
        /// </summary>
        public IDigitalFilter<TPayload> DigitalFilter { get; private set; }
    }
}
