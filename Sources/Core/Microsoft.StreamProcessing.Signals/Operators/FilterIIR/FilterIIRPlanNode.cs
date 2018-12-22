// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace Microsoft.StreamProcessing.Signal
{
    /// <summary>
    /// A node in the query plan representing leaky integration.
    /// </summary>
    public sealed class FilterIIRPlanNode : UnaryPlanNode
    {
        internal FilterIIRPlanNode(
            PlanNode previous, IQueryObject pipe,
            Type keyType, Type payloadType,
            long period, double factor, bool isGenerated, string errorMessages)
            : base(previous, pipe, keyType, payloadType, payloadType, isGenerated, errorMessages)
        {
            this.Period = period;
            this.Factor = factor;
        }

        /// <summary>
        /// Returns the kind of plan node, which can then be used for type casting.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.FilterIIR;
        /// <summary>
        /// The period of the uniformly-sampled signal.
        /// </summary>
        public long Period { get; private set; }
        /// <summary>
        /// The leakage factor of the leaky integrator.
        /// </summary>
        public double Factor { get; private set; }
    }
}
