// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Linq.Expressions;
using System.Reflection;

namespace Microsoft.StreamProcessing.Provider
{
    /// <summary>
    /// An augmented expression visitor that requires implementors to provide transformations for the method API for IQStreamable
    /// </summary>
    public abstract class QStreamableVisitor : ExpressionVisitor
    {
        private static readonly MethodInfo AlterDurationMethod = typeof(QStreamableStatic).GetMethod("AlterDuration");
        private static readonly MethodInfo GroupByMethod = typeof(QStreamableStatic).GetMethod("GroupBy");
        private static readonly MethodInfo QuantizeLifetimeMethod = typeof(QStreamableStatic).GetMethod("QuantizeLifetime");
        private static readonly MethodInfo SelectMethod = typeof(QStreamableStatic).GetMethod("Select");
        private static readonly MethodInfo WhereMethod = typeof(QStreamableStatic).GetMethod("Where");

        /// <summary>
        /// Visits the children of the System.Linq.Expressions.MethodCallExpression.
        /// </summary>
        /// <param name="node">The expression to visit.</param>
        /// <returns>The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.</returns>
        protected sealed override Expression VisitMethodCall(MethodCallExpression node)
        {
            var method = node.Method;
            if (!method.IsGenericMethod) return VisitNonStreamingMethodCall(node);
            method = method.GetGenericMethodDefinition();

            if (method == AlterDurationMethod) return VisitAlterDurationCall(node);
            if (method == GroupByMethod) return VisitGroupByCall(node);
            if (method == QuantizeLifetimeMethod) return VisitQuantizeLifetimeCall(node);
            if (method == SelectMethod) return VisitSelectCall(node);
            if (method == WhereMethod) return VisitWhereCall(node);

            return VisitNonStreamingMethodCall(node);
        }

        /// <summary>
        /// An overrideable method for handling any method that is not part of the IQStreamable API.
        /// </summary>
        /// <param name="node">The expression to visit.</param>
        /// <returns>The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.</returns>
        protected virtual Expression VisitNonStreamingMethodCall(MethodCallExpression node) => base.VisitMethodCall(node);

        /// <summary>
        /// Visits an IQStreamable Alter Duration call.
        /// </summary>
        /// <param name="node">The expression to visit.</param>
        /// <returns>The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.</returns>
        protected virtual Expression VisitAlterDurationCall(MethodCallExpression node) => node;

        /// <summary>
        /// Visits an IQStreamable Quantize Lifetime call.
        /// </summary>
        /// <param name="node">The expression to visit.</param>
        /// <returns>The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.</returns>
        protected virtual Expression VisitQuantizeLifetimeCall(MethodCallExpression node) => node;

        /// <summary>
        /// Visits an IQStreamable Select call.
        /// </summary>
        /// <param name="node">The expression to visit.</param>
        /// <returns>The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.</returns>
        protected virtual Expression VisitSelectCall(MethodCallExpression node) => node;

        /// <summary>
        /// Visits an IQStreamable Where call.
        /// </summary>
        /// <param name="node">The expression to visit.</param>
        /// <returns>The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.</returns>
        protected virtual Expression VisitWhereCall(MethodCallExpression node) => node;

        /// <summary>
        ///
        /// </summary>
        /// <param name="node">The expression to visit.</param>
        /// <returns>The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.</returns>
        protected virtual Expression VisitGroupByCall(MethodCallExpression node) => node;
    }
}
