// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using System.Reflection;

namespace Microsoft.StreamProcessing.Provider
{
    /// <summary>
    /// An augmented expression visitor that requires implementors to provide transformations for the method API for IQStreamable
    /// </summary>
    public abstract class QStreamableVisitor : ExpressionVisitor
    {
        private static readonly MethodInfo ChopMethod = GetMethodInfo(s => s.Chop(0, 0));
        private static readonly MethodInfo ClipDurationMethod = GetMethodInfo(s => s.ClipDuration(0));
        private static readonly MethodInfo ExtendDurationMethod = GetMethodInfo(s => s.ExtendDuration(0));
        private static readonly MethodInfo QuantizeLifetimeMethod = GetMethodInfo(s => s.QuantizeLifetime(0, 0, 0, 0));
        private static readonly MethodInfo SetDurationMethod = GetMethodInfo(s => s.SetDuration(0));
        private static readonly MethodInfo StitchMethod = GetMethodInfo(s => s.Stitch());

        private static readonly MethodInfo GroupByMethod = GetMethodInfo(s => s.GroupBy(o => o, o => o));
        private static readonly MethodInfo SelectMethod = GetMethodInfo(s => s.Select(o => o));
        private static readonly MethodInfo SelectManyMethod = GetMethodInfo(s => s.SelectMany(o => Array.Empty<object>()));
        private static readonly MethodInfo WhereMethod = GetMethodInfo(s => s.Where(o => true));

        private static readonly MethodInfo ClipDurationBinaryMethod = GetMethodInfo((a, b) => a.ClipDuration(b, aa => true, bb => true));
        private static readonly MethodInfo JoinMethod = GetMethodInfo((a, b) => a.Join(b, aa => true, bb => true));
        private static readonly MethodInfo UnionMethod = GetMethodInfo((a, b) => a.Union(b));
        private static readonly MethodInfo WhereNotExistsMethod = GetMethodInfo((a, b) => a.WhereNotExists(b, aa => true, bb => true));

        private static MethodInfo GetMethodInfo<T>(Expression<Func<IQStreamable<object>, IQStreamable<T>>> expression)
            => ((MethodCallExpression)expression.Body).Method.GetGenericMethodDefinition();

        private static MethodInfo GetMethodInfo(Expression<Func<IQStreamable<object>, IQStreamable<object>, IQStreamable<object>>> expression)
            => ((MethodCallExpression)expression.Body).Method.GetGenericMethodDefinition();

        private static MethodInfo GetMethodInfo(Expression<Func<IQStreamable<object>, IQStreamable<object>, IQStreamable<ValueTuple<object, object>>>> expression)
            => ((MethodCallExpression)expression.Body).Method.GetGenericMethodDefinition();

        /// <summary>
        /// Visits the children of the System.Linq.Expressions.MethodCallExpression.
        /// </summary>
        /// <param name="node">The expression to visit.</param>
        /// <returns>The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.</returns>
        protected sealed override Expression VisitMethodCall(MethodCallExpression node)
        {
            var originalMethod = node.Method;
            if (!originalMethod.IsGenericMethod) return VisitNonStreamingMethodCall(node);
            var method = originalMethod.GetGenericMethodDefinition();
            var genericTypes = originalMethod.GetGenericArguments();

            if (method == ChopMethod) return VisitChopCall(node.Arguments[0], genericTypes[0], (long)((ConstantExpression)node.Arguments[1]).Value, (long)((ConstantExpression)node.Arguments[2]).Value);
            if (method == ClipDurationMethod) return VisitClipDurationByConstantCall(node.Arguments[0], genericTypes[0], (long)((ConstantExpression)node.Arguments[1]).Value);
            if (method == ExtendDurationMethod) return VisitExtendDurationCall(node.Arguments[0], genericTypes[0], (long)((ConstantExpression)node.Arguments[1]).Value);
            if (method == QuantizeLifetimeMethod) return VisitQuantizeLifetimeCall(node.Arguments[0], genericTypes[0], (long)((ConstantExpression)node.Arguments[1]).Value, (long)((ConstantExpression)node.Arguments[2]).Value, (long)((ConstantExpression)node.Arguments[3]).Value, (long)((ConstantExpression)node.Arguments[3]).Value);
            if (method == SetDurationMethod) return VisitSetDurationCall(node.Arguments[0], genericTypes[0], (long)((ConstantExpression)node.Arguments[1]).Value);
            if (method == StitchMethod) return VisitStitchCall(node.Arguments[0], genericTypes[0]);

            if (method == GroupByMethod) return VisitGroupByCall(node.Arguments[0], genericTypes[0], genericTypes[1], genericTypes[2], (LambdaExpression)node.Arguments[1], (LambdaExpression)node.Arguments[2]);
            if (method == SelectMethod) return VisitSelectCall(node.Arguments[0], genericTypes[0], genericTypes[1], (LambdaExpression)node.Arguments[1], false);
            if (method == SelectManyMethod) return VisitSelectManyCall(node.Arguments[0], genericTypes[0], genericTypes[1], (LambdaExpression)node.Arguments[1], false);
            if (method == WhereMethod) return VisitWhereCall(node.Arguments[0], genericTypes[0], (LambdaExpression)node.Arguments[1]);

            if (method == ClipDurationBinaryMethod) return VisitClipDurationByEventCall(node.Arguments[0], node.Arguments[1], genericTypes[0], genericTypes[1], genericTypes[2], (LambdaExpression)node.Arguments[2], (LambdaExpression)node.Arguments[3]);
            if (method == JoinMethod) return VisitJoinCall(node.Arguments[0], node.Arguments[1], genericTypes[0], genericTypes[1], genericTypes[2], (LambdaExpression)node.Arguments[2], (LambdaExpression)node.Arguments[3]);
            if (method == UnionMethod) return VisitUnionCall(node.Arguments[0], node.Arguments[1], genericTypes[0]);
            if (method == WhereNotExistsMethod) return VisitWhereNotExistsCall(node.Arguments[0], node.Arguments[1], genericTypes[0], genericTypes[1], genericTypes[2], (LambdaExpression)node.Arguments[2], (LambdaExpression)node.Arguments[3]);

            return VisitNonStreamingMethodCall(node);
        }

        /// <summary>
        /// An overrideable method for handling any method that is not part of the IQStreamable API.
        /// </summary>
        /// <param name="node">The expression to visit.</param>
        /// <returns>The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.</returns>
        protected virtual Expression VisitNonStreamingMethodCall(MethodCallExpression node) => base.VisitMethodCall(node);

        /// <summary>
        /// Visits an IQStreamable Chop call.
        /// </summary>
        /// <param name="argument">The expression representing the streamable this method is called on.</param>
        /// <param name="elementType">The type of the underlying payload element in the stream.</param>
        /// <param name="period">Period to partition event lifetimes into.</param>
        /// <param name="offset">Offset from the start of time to apply the partitioning (default is 0).</param>
        /// <returns>The modified expression.</returns>
        protected abstract Expression VisitChopCall(Expression argument, Type elementType, long period, long offset);

        /// <summary>
        /// Visits an IQStreamable Clip Duration call.
        /// </summary>
        /// <param name="argument">The expression representing the streamable this method is called on.</param>
        /// <param name="elementType">The type of the underlying payload element in the stream.</param>
        /// <param name="maximumDuration">The maximum duration of each event</param>
        /// <returns>The modified expression.</returns>
        protected abstract Expression VisitClipDurationByConstantCall(Expression argument, Type elementType, long maximumDuration);

        /// <summary>
        /// Visits an IQStreamable Extend Duration call.
        /// </summary>
        /// <param name="argument">The expression representing the streamable this method is called on.</param>
        /// <param name="elementType">The type of the underlying payload element in the stream.</param>
        /// <param name="duration">The amount to extend (or contract) the duration for each event</param>
        /// <returns>The modified expression.</returns>
        protected abstract Expression VisitExtendDurationCall(Expression argument, Type elementType, long duration);

        /// <summary>
        /// Visits an IQStreamable Quantize Lifetime call.
        /// </summary>
        /// <param name="argument">The expression representing the streamable this method is called on.</param>
        /// <param name="elementType">The type of the underlying payload element in the stream.</param>
        /// <param name="windowSize">Window size</param>
        /// <param name="period">Period (or hop size)</param>
        /// <param name="progress">Interval at which to create partial results</param>
        /// <param name="offset">Offset from the start of time</param>
        /// <returns>The modified expression.</returns>
        protected abstract Expression VisitQuantizeLifetimeCall(Expression argument, Type elementType, long windowSize, long period, long progress, long offset);

        /// <summary>
        /// Visits an IQStreamable Set Duration call.
        /// </summary>
        /// <param name="argument">The expression representing the streamable this method is called on.</param>
        /// <param name="elementType">The type of the underlying payload element in the stream.</param>
        /// <param name="duration">The new duration for each event</param>
        /// <returns>The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.</returns>
        protected abstract Expression VisitSetDurationCall(Expression argument, Type elementType, long duration);

        /// <summary>
        /// Visits an IQStreamable Stitch call.
        /// </summary>
        /// <param name="argument">The expression representing the streamable this method is called on.</param>
        /// <param name="elementType">The type of the underlying payload element in the stream.</param>
        /// <returns>The modified expression.</returns>
        protected abstract Expression VisitStitchCall(Expression argument, Type elementType);

        /// <summary>
        /// Visits an IQStreamable Select call.
        /// </summary>
        /// <param name="argument">The expression representing the streamable this method is called on.</param>
        /// <param name="inputElementType">The type of the underlying payload element in the input stream.</param>
        /// <param name="outputElementType">The type of the underlying payload element in the output stream.</param>
        /// <param name="selectExpression">The expression applied to each element in the stream.</param>
        /// <param name="includeStartEdge">States whether the select expression includes the start edge.</param>
        /// <returns>The modified expression.</returns>
        protected abstract Expression VisitSelectCall(Expression argument, Type inputElementType, Type outputElementType, LambdaExpression selectExpression, bool includeStartEdge);

        /// <summary>
        /// Visits an IQStreamable SelectMany call.
        /// </summary>
        /// <param name="argument">The expression representing the streamable this method is called on.</param>
        /// <param name="inputElementType">The type of the underlying payload element in the input stream.</param>
        /// <param name="outputElementType">The type of the underlying payload element in the output stream.</param>
        /// <param name="selectExpression">The expression applied to each element in the stream.</param>
        /// <param name="includeStartEdge">States whether the select expression includes the start edge.</param>
        /// <returns>The modified expression.</returns>
        protected abstract Expression VisitSelectManyCall(Expression argument, Type inputElementType, Type outputElementType, LambdaExpression selectExpression, bool includeStartEdge);

        /// <summary>
        /// Visits an IQStreamable Where call.
        /// </summary>
        /// <param name="argument">The expression representing the streamable this method is called on.</param>
        /// <param name="elementType">The type of the underlying payload element in the stream.</param>
        /// <param name="whereExpression">The filter expression applied to each element in the stream.</param>
        /// <returns>The modified expression.</returns>
        protected abstract Expression VisitWhereCall(Expression argument, Type elementType, LambdaExpression whereExpression);

        /// <summary>
        /// Visits an IQStreamable GroupBy call.
        /// </summary>
        /// <param name="argument">The expression representing the streamable this method is called on.</param>
        /// <param name="inputElementType">The type of the underlying payload element in the input stream.</param>
        /// <param name="keyType">The type of the grouping key established by the key selector.</param>
        /// <param name="outputElementType">The type of the underlying payload element in the output stream.</param>
        /// <param name="keySelector">A function to extract the key for each element.</param>
        /// <param name="elementSelector">A function to determine the element type of the window.</param>
        /// <returns>The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.</returns>
        protected abstract Expression VisitGroupByCall(Expression argument, Type inputElementType, Type keyType, Type outputElementType, LambdaExpression keySelector, LambdaExpression elementSelector);

        /// <summary>
        /// Visits an IQStreamable binary ClipEventDuration call.
        /// </summary>
        /// <param name="left">The expression representing the left input this method is called on.</param>
        /// <param name="right">The expression representing the right input this method is called on.</param>
        /// <param name="leftType">The type of the underlying payload element in the left input stream.</param>
        /// <param name="rightType">The type of the underlying payload element in the right input stream.</param>
        /// <param name="keyType">The type of the join key established by the left and right key selectors.</param>
        /// <param name="leftKeySelector">A function to extract the join key from the left input.</param>
        /// <param name="rightKeySelector">A function to extract the join key from the right input.</param>
        /// <returns>The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.</returns>
        protected abstract Expression VisitClipDurationByEventCall(Expression left, Expression right, Type leftType, Type rightType, Type keyType, LambdaExpression leftKeySelector, LambdaExpression rightKeySelector);

        /// <summary>
        /// Visits an IQStreamable binary Join call.
        /// </summary>
        /// <param name="left">The expression representing the left input this method is called on.</param>
        /// <param name="right">The expression representing the right input this method is called on.</param>
        /// <param name="leftType">The type of the underlying payload element in the left input stream.</param>
        /// <param name="rightType">The type of the underlying payload element in the right input stream.</param>
        /// <param name="keyType">The type of the join key established by the left and right key selectors.</param>
        /// <param name="leftKeySelector">A function to extract the join key from the left input.</param>
        /// <param name="rightKeySelector">A function to extract the join key from the right input.</param>
        /// <returns>The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.</returns>
        protected abstract Expression VisitJoinCall(Expression left, Expression right, Type leftType, Type rightType, Type keyType, LambdaExpression leftKeySelector, LambdaExpression rightKeySelector);

        /// <summary>
        /// Visits an IQStreamable binary Union call.
        /// </summary>
        /// <param name="left">The expression representing the left input this method is called on.</param>
        /// <param name="right">The expression representing the right input this method is called on.</param>
        /// <param name="elementType">The type of the underlying payload element in the stream.</param>
        /// <returns>The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.</returns>
        protected abstract Expression VisitUnionCall(Expression left, Expression right, Type elementType);

        /// <summary>
        /// Visits an IQStreamable binary WhereNotExists call.
        /// </summary>
        /// <param name="left">The expression representing the left input this method is called on.</param>
        /// <param name="right">The expression representing the right input this method is called on.</param>
        /// <param name="leftType">The type of the underlying payload element in the left input stream.</param>
        /// <param name="rightType">The type of the underlying payload element in the right input stream.</param>
        /// <param name="keyType">The type of the join key established by the left and right key selectors.</param>
        /// <param name="leftKeySelector">A function to extract the join key from the left input.</param>
        /// <param name="rightKeySelector">A function to extract the join key from the right input.</param>
        /// <returns>The modified expression, if it or any subexpression was modified; otherwise, returns the original expression.</returns>
        protected abstract Expression VisitWhereNotExistsCall(Expression left, Expression right, Type leftType, Type rightType, Type keyType, LambdaExpression leftKeySelector, LambdaExpression rightKeySelector);
    }
}
