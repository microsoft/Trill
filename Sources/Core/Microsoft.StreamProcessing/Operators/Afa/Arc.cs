// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Abstract type for all AFA arcs.
    /// </summary>
    /// <typeparam name="TInput">The type of the input data.</typeparam>
    /// <typeparam name="TRegister">The type of the state register.</typeparam>
    internal abstract class Arc<TInput, TRegister>
    {
        public abstract ArcType ArcType { get; }
    }

    /// <summary>
    /// Arc that handles exactly one element (event) at a given timestamp
    /// </summary>
    /// <typeparam name="TInput">The type of the input data.</typeparam>
    /// <typeparam name="TRegister">The type of the state register.</typeparam>
    internal sealed class SingleElementArc<TInput, TRegister> : Arc<TInput, TRegister>
    {
        /// <summary>
        /// Indicates that the current arc is triggered by a single event.
        /// </summary>
        public override ArcType ArcType => ArcType.SingleElement;

        public Expression<Func<long, TInput, TRegister, bool>> Fence;
        public Expression<Func<long, TInput, TRegister, TRegister>> Transfer = null;

        public SingleElementArc() { }

        public SingleElementArc(Expression<Func<long, TInput, TRegister, bool>> fence, Expression<Func<long, TInput, TRegister, TRegister>> transfer = null)
        {
            this.Fence = fence;
            this.Transfer = transfer;
        }

        public override string ToString()
        {
            string result = "SingleElementArc: ";
            if (this.Fence != null)
                result += "Fence=" + this.Fence.ExpressionToCSharp() + " ";
            if (this.Transfer != null)
                result += "Transfer=" + this.Transfer.ExpressionToCSharp() + " ";

            return result;
        }
    }

    /// <summary>
    /// Arc that handles multiple elements (events) at a given timestamp
    /// </summary>
    /// <typeparam name="TInput">The type of the input data.</typeparam>
    /// <typeparam name="TRegister">The type of the state register.</typeparam>
    /// <typeparam name="TAccumulator"></typeparam>
    internal sealed class MultiElementArc<TInput, TRegister, TAccumulator> : Arc<TInput, TRegister>
    {
        /// <summary>
        /// Indicates that the current arc is triggered by multiple elements at a given timestamp.
        /// </summary>
        public override ArcType ArcType => ArcType.MultiElement;

        public Expression<Func<long, TRegister, TAccumulator>> Initialize;
        public Expression<Func<long, TInput, TRegister, TAccumulator, TAccumulator>> Accumulate;
        public Expression<Func<long, TInput, TAccumulator, bool>> SkipToEnd;
        public Expression<Func<long, TAccumulator, TRegister, bool>> Fence;
        public Expression<Func<long, TAccumulator, TRegister, TRegister>> Transfer;
        public Expression<Action<TAccumulator>> Dispose;

        public MultiElementArc() { }

        public override string ToString()
        {
            string result = "MultiElementArc: ";
            if (this.Initialize != null)
                result += "Init=" + this.Initialize.ExpressionToCSharp() + " ";
            if (this.Accumulate != null)
                result += "Acc=" + this.Accumulate.ExpressionToCSharp() + " ";
            if (this.SkipToEnd != null)
                result += "Skip=" + this.SkipToEnd.ExpressionToCSharp() + " ";
            if (this.Fence != null)
                result += "Fence=" + this.Fence.ExpressionToCSharp() + " ";
            if (this.Transfer != null)
                result += "Transfer=" + this.Transfer.ExpressionToCSharp() + " ";

            return result;
        }
    }

    /// <summary>
    /// Arc that handles a list of one or more elements (events) at a given timestamp
    /// </summary>
    /// <typeparam name="TInput">The type of the input data.</typeparam>
    /// <typeparam name="TRegister">The type of the state register.</typeparam>
    internal sealed class ListElementArc<TInput, TRegister> : Arc<TInput, TRegister>
    {
        /// <summary>
        /// Indicates that the current arc is a transformation triggered by a collection of elements.
        /// </summary>
        public override ArcType ArcType => ArcType.ListElement;

        public Expression<Func<long, List<TInput>, TRegister, bool>> Fence;
        public Expression<Func<long, List<TInput>, TRegister, TRegister>> Transfer = null;

        public ListElementArc() { }

        public ListElementArc(Expression<Func<long, List<TInput>, TRegister, bool>> fence, Expression<Func<long, List<TInput>, TRegister, TRegister>> transfer = null)
        {
            this.Fence = fence;
            this.Transfer = transfer;
        }
    }

    /// <summary>
    /// Epsilon arc - an arc that is a free transformation in an AFA.
    /// </summary>
    /// <typeparam name="TInput">The type of the input data.</typeparam>
    /// <typeparam name="TRegister">The type of the state register.</typeparam>
    internal sealed class EpsilonArc<TInput, TRegister> : Arc<TInput, TRegister>
    {
        /// <summary>
        /// Indicates that the current arc is an epsilon transformation.
        /// </summary>
        public override ArcType ArcType => ArcType.Epsilon;

        public override string ToString() => "EpsilonArc";
    }
}