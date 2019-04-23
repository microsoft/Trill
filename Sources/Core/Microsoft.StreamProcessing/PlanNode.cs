// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// A node in the query plan representing the objects in the active, running query.
    /// </summary>
    public abstract class PlanNode
    {
        private readonly IQueryObject pipe;

        /// <summary>
        /// Base constructor for a node in the query plan.
        /// </summary>
        /// <param name="pipe">A pointer to the current operating query object.</param>
        /// <param name="keyType">The type of the key for the current query object.</param>
        /// <param name="payloadType">The type of the payload for the current query object.</param>
        /// <param name="isGenerated">States whether the current operator was generated via code-gen.</param>
        /// <param name="compileErrors">States what issues the code-gen encountered during compilation, if any.</param>
        protected PlanNode(IQueryObject pipe, Type keyType, Type payloadType, bool isGenerated, string compileErrors)
        {
            this.pipe = pipe;
            this.KeyType = keyType;
            this.PayloadType = payloadType;
            this.IsGenerated = isGenerated;
            this.CodeGenReport = compileErrors;
        }

        /// <summary>
        /// Retrieve the count of tuples currently resident in memory as part of buffering events into pages.
        /// </summary>
        /// <returns>The count of tuples currently resident in memory as part of buffering events into pages.</returns>
        public int GetCurrentlyBufferedOutputCount() => this.pipe.CurrentlyBufferedOutputCount;

        /// <summary>
        /// Retrieve the count of tuples currently resident in memory as part of input calculations.
        /// </summary>
        /// <returns>The count of tuples currently resident in memory as part of input calculations.</returns>
        public int GetCurrentlyBufferedInputCount() => this.pipe.CurrentlyBufferedInputCount;

        /// <summary>
        /// Tells whether the current node in the query plan was generated using code generation internally.
        /// </summary>
        public bool IsGenerated { get; }

        /// <summary>
        /// Tells what issues, if any, were encountered when trying to create a generated artifact.
        /// </summary>
        public string CodeGenReport { get; }

        /// <summary>
        /// Describes the kind of the current node.
        /// </summary>
        public abstract PlanNodeKind Kind { get; }

        /// <summary>
        /// Reports the grouping key type of the running query node.
        /// </summary>
        public Type KeyType { get; }

        /// <summary>
        /// Reports the payload type of the running query node.
        /// </summary>
        public Type PayloadType { get; }

        /// <summary>
        /// Provides a string representation of the current node.
        /// </summary>
        /// <returns>A string representation of the current node.</returns>
        public override string ToString()
        {
            var builder = new StringBuilder();
            PrintConciseSummary(builder, 0);
            return builder.ToString();
        }

        internal abstract void PrintConciseSummary(StringBuilder builder, int indentLevel);

        /// <summary>
        /// Provides a string representation of the current node.
        /// </summary>
        /// <param name="builder">The builder to which plan information should be written.</param>
        /// <param name="indentLevel">The indentation level to which the printing of this node should be printed.</param>
        protected void PrintConciseGeneralState(StringBuilder builder, int indentLevel)
        {
            builder.AppendLine(new string('\t', indentLevel) + this.Kind + " - " + this.pipe.GetType().Name +
                               (this.IsGenerated ? " (Generated)" : string.Empty));
            if (!string.IsNullOrEmpty(this.CodeGenReport))
            {
                builder.AppendLine(new string('\t', indentLevel < int.MaxValue ? indentLevel + 1 : indentLevel) +
                                   "Compile errors: " + this.CodeGenReport);
            }
        }

        internal abstract void CollectDormantIngressSites(Dictionary<string, IngressPlanNode> working);

        /// <summary>
        /// Visitor pattern acceptor method.
        /// </summary>
        /// <param name="visitor"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public abstract void Accept(IPlanNodeVisitor visitor);
    }

    /// <summary>
    /// A node in the query plan representing a data source in the active, running query.
    /// </summary>
    public sealed class IngressPlanNode : PlanNode
    {
        private readonly IIngressStreamObserver observer;

        internal IngressPlanNode(IIngressStreamObserver observer, Type keyType, Type payloadType, bool isGenerated, string compileErrors)
            : base(observer, keyType, payloadType, isGenerated, compileErrors)
            => this.observer = observer;

        /// <summary>
        /// Returns the kind of plan node, which can then be used for type casting.
        /// </summary>
        public override PlanNodeKind Kind => PlanNodeKind.Ingress;

        /// <summary>
        /// Retrieve the count of tuples currently resident in memory as part of applying policies.
        /// </summary>
        /// <returns>The count of tuples currently resident in memory as part of applying policies.</returns>
        public int GetCurrentlyBufferedAppliedPolicyCount() => this.observer.CurrentlyBufferedStartEdgeCount;

        /// <summary>
        /// Retrieve the count of tuples currently resident in memory as part of temporal reordering.
        /// </summary>
        /// <returns>The count of tuples currently resident in memory as part of temporal reordering.</returns>
        public int GetCurrentlyBufferedReorderCount() => this.observer.CurrentlyBufferedReorderCount;

        internal override void PrintConciseSummary(StringBuilder builder, int indentLevel)
        {
            PrintConciseGeneralState(builder, indentLevel);
            builder.AppendLine(new string('\t', indentLevel + 1) + "Name: " + this.observer.IngressSiteIdentifier);
        }

        internal override void CollectDormantIngressSites(Dictionary<string, IngressPlanNode> working)
            => working[this.observer.IngressSiteIdentifier] = this;

        /// <summary>
        /// Visitor pattern acceptor method.
        /// </summary>
        /// <param name="visitor"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void Accept(IPlanNodeVisitor visitor) => visitor.Visit(this);
    }

    /// <summary>
    /// The kind of plan node.
    /// </summary>
    public enum PlanNodeKind
    {
        /// <summary>
        /// Represents a simple operation at the point of data input in the running query.
        /// </summary>
        Ingress,

        /// <summary>
        /// Represents a point of data output in the running query.
        /// </summary>
        Egress,

        /// <summary>
        /// Represents a point of data input and output comprising the full running query.
        /// </summary>
        Fused,

        /// <summary>
        /// Represents an operator in a running query that filters the data to only those items that meet the given criterion.
        /// </summary>
        Where,

        /// <summary>
        /// Represents an operator in a running query that applies a function over payloads (and optionally start-time or key value) to each row of the data.
        /// </summary>
        Select,

        /// <summary>
        /// Represents an operator in a running query that applies a function to each row of the data, returning any number of rows of output per row of input.
        /// </summary>
        SelectMany,

        /// <summary>
        /// Represents an operator in a running query that alters event lifetimes.
        /// </summary>
        AlterLifetime,

        /// <summary>
        /// Represents an operator in a running query that translates row-based data into column-based data.
        /// </summary>
        RowToColumn,

        /// <summary>
        /// Represents an operator in a running query that translates column-based data into row-based data.
        /// </summary>
        ColumnToRow,

        /// <summary>
        /// Represents an operator that multicasts data to multiple other operators.
        /// </summary>
        Multicast,

        /// <summary>
        /// Represents an operator that takes a union of multiple inputs.
        /// </summary>
        Union,

        /// <summary>
        /// Represents a spray operator in a Map-Reduce operation.
        /// </summary>
        Spray,

        /// <summary>
        /// Represents an operator that generates a snapshot window.
        /// </summary>
        SnapshotWindow,

        /// <summary>
        /// Represents an operator that generates a grouping snapshot window.
        /// </summary>
        GroupedWindow,

        /// <summary>
        /// Represents an operator that condenses stream events into just start edges and intervals.
        /// </summary>
        CoalesceEndEdges,

        /// <summary>
        /// Represents a chop or beat operation, aligning edge times with beat boundaries.
        /// </summary>
        Beat,

        /// <summary>
        /// Represents a temporal partitioning operation within a stream.
        /// </summary>
        Partition,

        /// <summary>
        /// Represents a grouping operation within a stream.
        /// </summary>
        Group,

        /// <summary>
        /// Represents an ungrouping operation within a stream.
        /// </summary>
        Ungroup,

        /// <summary>
        /// Represents a join operation within a stream.
        /// </summary>
        Join,

        /// <summary>
        /// Stitch events by maximally merging adjacent events with the same payload
        /// </summary>
        Stitch,

        /// <summary>
        /// Convert events into point events at the original end time
        /// </summary>
        PointAtEnd,

        /// <summary>
        /// Plan node for augmented finite automaton (AFA)
        /// </summary>
        Afa,

        /// <summary>
        /// Extend the lifetime of the events in the stream
        /// </summary>
        ExtendLifetime,

        /// <summary>
        /// Truncate the lifetime of the events to a given constant
        /// </summary>
        ClipByConstant,

        /// <summary>
        /// Alter the lifetime of events so that they conform to sessions relative to a given timeout
        /// </summary>
        SessionWindow,

        /// <summary>
        /// Adjust the start and end times along a set of boundary points
        /// </summary>
        QuantizeLifetime,

        /// <summary>
        /// Represents an operator that is defined externally from the library
        /// </summary>
        UserDefined,
    }

    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public interface IPlanNodeVisitor
    {
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="node"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        void Visit(PlanNode node);
    }
}
