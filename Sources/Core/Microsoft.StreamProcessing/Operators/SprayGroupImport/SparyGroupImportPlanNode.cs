// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Represents an operation in a Map-Reduce job that sprays data to different nodes.
    /// </summary>
    public sealed class SprayPlanNode : UnaryPlanNode
    {
        /// <summary>
        /// States the maximum number of delegated branches against which to spray data.
        /// </summary>
        public int TotalBranches { get; }

        /// <summary>
        /// States whether the spray operation sends all data to all nodes.
        /// </summary>
        public bool Multicast { get; }

        /// <summary>
        /// Returns the expression for the comparer used to determine spray sort order.
        /// </summary>
        public Expression SpraySortOrderComparer { get; }

        internal SprayPlanNode(PlanNode previous, IQueryObject pipe,
            Type keyType, Type payloadType,
            int totalBranches, bool multicast,
            Expression spraySortOrderComparer,
            bool isGenerated)
            : base(previous, pipe, keyType, payloadType, payloadType, isGenerated, null)
        {
            this.TotalBranches = totalBranches;
            this.Multicast = multicast;
            this.SpraySortOrderComparer = spraySortOrderComparer;
        }

        /// <summary>
        /// Indicates that the current node is a spray operation.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.Spray;
    }
}
