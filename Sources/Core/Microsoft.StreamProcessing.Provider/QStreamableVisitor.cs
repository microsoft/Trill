// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Linq.Expressions;
using System.Reflection;

namespace Microsoft.StreamProcessing.Provider
{
    /// <summary>
    ///
    /// </summary>
    public abstract class QStreamableVisitor : ExpressionVisitor
    {
        private static readonly MethodInfo AlterDurationMethod = typeof(QStreamableStatic).GetMethod("AlterDuration");
        private static readonly MethodInfo GroupByMethod = typeof(QStreamableStatic).GetMethod("GroupBy");
        private static readonly MethodInfo QuantizeLifetimeMethod = typeof(QStreamableStatic).GetMethod("QuantizeLifetime");
        private static readonly MethodInfo SelectMethod = typeof(QStreamableStatic).GetMethod("Select");
        private static readonly MethodInfo WhereMethod = typeof(QStreamableStatic).GetMethod("Where");

        /// <summary>
        ///
        /// </summary>
        /// <param name="node"></param>
        /// <returns></returns>
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
        ///
        /// </summary>
        /// <param name="node"></param>
        /// <returns></returns>
        protected virtual Expression VisitNonStreamingMethodCall(MethodCallExpression node) => node;

        /// <summary>
        ///
        /// </summary>
        /// <param name="node"></param>
        /// <returns></returns>
        protected virtual Expression VisitAlterDurationCall(MethodCallExpression node) => node;

        /// <summary>
        ///
        /// </summary>
        /// <param name="node"></param>
        /// <returns></returns>
        protected virtual Expression VisitQuantizeLifetimeCall(MethodCallExpression node) => node;

        /// <summary>
        ///
        /// </summary>
        /// <param name="node"></param>
        /// <returns></returns>
        protected virtual Expression VisitSelectCall(MethodCallExpression node) => node;

        /// <summary>
        ///
        /// </summary>
        /// <param name="node"></param>
        /// <returns></returns>
        protected virtual Expression VisitWhereCall(MethodCallExpression node) => node;

        /// <summary>
        ///
        /// </summary>
        /// <param name="node"></param>
        /// <returns></returns>
        protected virtual Expression VisitGroupByCall(MethodCallExpression node) => node;
    }
}
