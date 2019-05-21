// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// A node in the query plan representing an object with two inputs in the active, running query.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class BinaryPlanNode : PlanNode
    {
        /// <summary>
        /// The payload type of the left input stream to the binary operator.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public Type LeftPayloadType { get; }

        /// <summary>
        /// The payload type of the right input stream to the binary operator.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public Type RightPayloadType { get; }

        private readonly IBinaryObserver pipe;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="pipe"></param>
        /// <param name="keyType"></param>
        /// <param name="leftType"></param>
        /// <param name="rightType"></param>
        /// <param name="payloadType"></param>
        /// <param name="isGenerated"></param>
        /// <param name="errorMessages"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected BinaryPlanNode(
            PlanNode left, PlanNode right, IBinaryObserver pipe, Type keyType, Type leftType, Type rightType, Type payloadType, bool isGenerated, string errorMessages)
            : base(pipe, keyType, payloadType, isGenerated, errorMessages)
        {
            this.LeftPlanNode = left;
            this.RightPlanNode = right;
            this.pipe = pipe;
            this.LeftPayloadType = leftType;
            this.RightPayloadType = rightType;
        }

        /// <summary>
        /// States whether the left input has any accumulated state in the query.
        /// If true, then sending data via the right input might produce output.
        /// </summary>
        public bool LeftInputHasState => this.pipe.LeftInputHasState;

        /// <summary>
        /// Provides a count of events currently buffered from the left input awaiting computation.
        /// </summary>
        public int CurrentlyBufferedLeftInputCount => this.pipe.CurrentlyBufferedLeftInputCount;

        /// <summary>
        /// States whether the right input has any accumulated state in the query.
        /// If true, then sending data via the left input might produce output.
        /// </summary>
        public bool RightInputHasState => this.pipe.RightInputHasState;

        /// <summary>
        /// Provides a count of events currently buffered from the right input awaiting computation.
        /// </summary>
        public int CurrentlyBufferedRightInputCount => this.pipe.CurrentlyBufferedRightInputCount;

        /// <summary>
        /// The query plan node that provides the left input to this operator.
        /// </summary>
        public PlanNode LeftPlanNode { get; private set; }

        /// <summary>
        /// The query plan node that provides the right input to this operator.
        /// </summary>
        public PlanNode RightPlanNode { get; private set; }

        internal override void PrintConciseSummary(System.Text.StringBuilder builder, int indentLevel)
        {
            PrintConciseGeneralState(builder, indentLevel);
            builder.AppendLine(new string('\t', indentLevel + 1) + "Left input: ");
            this.LeftPlanNode.PrintConciseSummary(builder, indentLevel + 2);
            builder.AppendLine(new string('\t', indentLevel + 1) + "Right input: ");
            this.RightPlanNode.PrintConciseSummary(builder, indentLevel + 2);
        }

        internal override void CollectDormantIngressSites(Dictionary<string, IngressPlanNode> working)
        {
            if (!this.LeftInputHasState) this.LeftPlanNode.CollectDormantIngressSites(working);
            if (!this.RightInputHasState) this.RightPlanNode.CollectDormantIngressSites(working);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="visitor"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void Accept(IPlanNodeVisitor visitor)
        {
            this.LeftPlanNode.Accept(visitor);
            this.RightPlanNode.Accept(visitor);
            visitor.Visit(this);
        }
    }

    /// <summary>
    /// A node in the query plan representing a join operation in the active, running query.
    /// </summary>
    public sealed class JoinPlanNode : BinaryPlanNode
    {
        internal JoinPlanNode(
            PlanNode left, PlanNode right, IBinaryObserver pipe,
            Type leftPayloadType, Type rightPayloadType, Type payloadType, Type keyType,
            JoinKind joinKind, bool isGenerated, string errorMessages)
            : base(left, right, pipe, keyType, leftPayloadType, rightPayloadType, payloadType, isGenerated, errorMessages)
            => this.JoinKind = joinKind;

        internal void AddJoinExpression(string name, Expression expression) => this.JoinExpressions.Add(name, expression);

        /// <summary>
        /// Indicates that the current node is a join operation.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.Join;

        /// <summary>
        /// Returns the kind of join represented by the current operator.
        /// </summary>
        public JoinKind JoinKind { get; private set; }

        /// <summary>
        /// Returns the set of expressions employed by the current node to compute the join.
        /// </summary>
        public Dictionary<string, Expression> JoinExpressions { get; } = new Dictionary<string, Expression>();
    }

    /// <summary>
    /// Describes the kind of join used in a particular join query plan node.
    /// </summary>
    public enum JoinKind
    {
        /// <summary>
        /// A clip operation, joining two streams relative to their respective intervals.
        /// </summary>
        Clip,

        /// <summary>
        /// An equijoin between two streams.
        /// </summary>
        EquiJoin,

        /// <summary>
        /// An equijoin where both streams are guaranteed to only contain constant-duration intervals.
        /// </summary>
        FixedIntervalEquiJoin,

        /// <summary>
        /// An equijoin where both streams are guaranteed to only contain start edges.
        /// </summary>
        StartEdgeEquijoin,

        /// <summary>
        /// An equijoin where both streams are guaranteed to only contain start edges and use the same key comparer.
        /// </summary>
        IncreasingOrderEquiJoin,

        /// <summary>
        /// A left antisemijoin operation (i.e., a "where not exists" operation).
        /// </summary>
        LeftAntiSemiJoin,
    }
}
