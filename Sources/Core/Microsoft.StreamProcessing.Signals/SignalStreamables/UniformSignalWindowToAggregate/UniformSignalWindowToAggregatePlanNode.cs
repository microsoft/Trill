// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace Microsoft.StreamProcessing.Signal.UDO
{
    /// <summary>
    /// A node in the query plan representing a sliding dot product operation.
    /// </summary>
    public sealed class UniformSignalWindowToAggregatePlanNode<TSource, TResult> : UnaryPlanNode
    {
        internal UniformSignalWindowToAggregatePlanNode(
            PlanNode previous, IQueryObject pipe,
            Type keyType, Type sourceType, Type resultType,
            Func<ISignalWindowObservable<TSource>, ISignalObservable<TResult>> operatorPipeline,
            bool isGenerated, string errorMessages)
            : base(previous, pipe, keyType, sourceType, resultType, isGenerated, errorMessages)
            => this.OperatorPipeline = operatorPipeline;

        /// <summary>
        /// Returns the kind of plan node, which can then be used for type casting.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.WindowedPipeline;
        /// <summary>
        /// Returns the operator pipeline function represented by this windowed operator
        /// </summary>
        public Func<ISignalWindowObservable<TSource>, ISignalObservable<TResult>> OperatorPipeline { get; private set; }
    }
}