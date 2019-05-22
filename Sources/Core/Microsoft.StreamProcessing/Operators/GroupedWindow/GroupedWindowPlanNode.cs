// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using Microsoft.StreamProcessing.Aggregates;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// A node in the query plan representing a grouped window snapshot operation.
    /// </summary>
    /// <typeparam name="TInput">Input type of the aggregate operation</typeparam>
    /// <typeparam name="TState">State type of the aggregate operation</typeparam>
    /// <typeparam name="TResult">Result type of the aggregate operation</typeparam>
    public sealed class GroupedWindowPlanNode<TInput, TState, TResult> : UnaryPlanNode
    {
        /// <summary>
        /// The aggregate object used in the window operation.
        /// </summary>
        public IAggregate<TInput, TState, TResult> Aggregate { get; }

        /// <summary>
        /// The function used to select key values in the window operation.
        /// </summary>
        public Expression KeySelector { get; }

        /// <summary>
        /// The function used to compute result values in the window operation.
        /// </summary>
        public Expression FinalResultSelector { get; }

        internal GroupedWindowPlanNode(
            PlanNode previous,
            IQueryObject pipe,
            Type keyType,
            Type inputType,
            Type outputType,
            IAggregate<TInput, TState, TResult> aggregate,
            Expression keySelector,
            Expression finalResultSelector,
            bool isGenerated,
            string errorMessages)
            : base(previous, pipe, keyType, outputType, inputType, isGenerated, errorMessages)
        {
            this.Aggregate = aggregate;
            this.KeySelector = keySelector;
            this.FinalResultSelector = finalResultSelector;
        }

        /// <summary>
        /// Indicates that the current node is a grouped window aggregation operation.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.GroupedWindow;
    }
}
