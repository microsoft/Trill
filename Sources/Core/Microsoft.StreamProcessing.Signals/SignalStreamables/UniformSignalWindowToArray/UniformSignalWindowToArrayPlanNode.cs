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
    public sealed class UniformSignalWindowToArrayPlanNode<TSource, TResult> : UnaryPlanNode
    {
        internal UniformSignalWindowToArrayPlanNode(
            PlanNode previous, IQueryObject pipe,
            Type keyType, Type sourceType, Type resultType,
            Func<ISignalWindowObservable<TSource>, ISignalWindowObservable<TResult>> operatorPipeline,
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
        public Func<ISignalWindowObservable<TSource>, ISignalWindowObservable<TResult>> OperatorPipeline { get; private set; }
    }
}