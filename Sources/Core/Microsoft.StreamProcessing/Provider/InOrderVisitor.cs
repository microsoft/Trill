// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.StreamProcessing.Aggregates;

namespace Microsoft.StreamProcessing.Provider
{
    internal sealed class InOrderVisitor : QStreamableVisitor
    {
        public static InOrderVisitor Instance = new InOrderVisitor();

        private InOrderVisitor() { }

        protected override Expression VisitChopCall(Expression argument, Type elementType, long period, long offset)
            => VisitUnaryStreamProcessingMethod(nameof(GenerateChopCall), argument, elementType, period, offset);

        private static Expression<Func<IStreamable<Empty, TPayload>, IStreamable<Empty, TPayload>>> GenerateChopCall<TPayload>(
            long period, long offset)
            => (stream) => new BeatStreamable<Empty, TPayload>(stream, period, offset);

        protected override Expression VisitClipDurationByConstantCall(Expression argument, Type elementType, long maximumDuration)
            => VisitUnaryStreamProcessingMethod(nameof(GenerateClipDurationByConstantCall), argument, elementType, maximumDuration);

        private static Expression<Func<IStreamable<Empty, TPayload>, IStreamable<Empty, TPayload>>> GenerateClipDurationByConstantCall<TPayload>(
            long maximumDuration)
            => (stream) => new ExtendLifetimeStreamable<Empty, TPayload>(stream, maximumDuration);

        protected override Expression VisitClipDurationByEventCall(Expression left, Expression right, Type leftType, Type rightType, Type keyType, LambdaExpression leftKeySelector, LambdaExpression rightKeySelector)
            => VisitBinaryStreamProcessingMethod(nameof(GenerateClipDurationByEventCall), left, right, leftType, rightType, keyType, leftKeySelector, rightKeySelector);

        private static Expression<Func<IStreamable<Empty, TLeft>, IStreamable<Empty, TRight>, Expression<Func<TLeft, TKey>>, Expression<Func<TRight, TKey>>, IStreamable<Empty, TLeft>>> GenerateClipDurationByEventCall<TLeft, TRight, TKey>()
            => (left, right, leftSelector, rightSelector) => left.ClipEventDuration(right, leftSelector, rightSelector);

        protected override Expression VisitExtendDurationCall(Expression argument, Type elementType, long duration)
            => VisitUnaryStreamProcessingMethod(nameof(GenerateExtendDurationCall), argument, elementType, duration);

        private static Expression<Func<IStreamable<Empty, TPayload>, IStreamable<Empty, TPayload>>> GenerateExtendDurationCall<TPayload>(
            long duration)
            => (stream) => new ExtendLifetimeStreamable<Empty, TPayload>(stream, duration);

        protected override Expression VisitGroupByCall(Expression argument, Type inputType, Type keyType, Type outputType, LambdaExpression keySelector, LambdaExpression elementSelector)
            => (GetType()
                .GetMethod(nameof(GenerateGroupByCall), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)
                .MakeGenericMethod(inputType, keyType, outputType).Invoke(null, Array.Empty<object>()) as LambdaExpression)
                .ReplaceParametersInBody(Visit(argument), Expression.Constant(keySelector), Expression.Constant(elementSelector));

        private static Expression<Func<IStreamable<Empty, TPayload>, Expression<Func<TPayload, TKey>>, Expression<Func<TPayload, TElement>>, IStreamable<Empty, IGrouping<TKey, TElement>>>> GenerateGroupByCall<TPayload, TKey, TElement>()
            => (stream, keySelector, elementSelector) => stream.GroupAggregate(
                keySelector,
                new NaiveAggregate<TElement, IEnumerable<TElement>>(o => o).Wrap(elementSelector),
                (g, l) => (IGrouping<TKey, TElement>)new NaiveGrouping<TKey, TElement>(g.Key, l));

        private static readonly MethodInfo GroupApplyMethodInfo = ((MethodCallExpression)GenerateGroupByCall<object, object, object>().Body).Method.GetGenericMethodDefinition();

        private static bool IsNaiveGrouping(Expression expression)
        {
            if (!(expression is MethodCallExpression methodExpression)) return false;
            if (methodExpression.Method.GetGenericMethodDefinition() != GroupApplyMethodInfo) return false;
            if (methodExpression.Arguments.Count != 4) return false;
            var constructor = methodExpression.Arguments[3];

            // Unnest a quote to get the argument
            if (constructor.NodeType != ExpressionType.Quote) return false;
            constructor = ((UnaryExpression)constructor).Operand;

            var groupExpression = ((LambdaExpression)constructor).Body;
            if (groupExpression.NodeType != ExpressionType.Convert) return false;

            var unaryExpression = (UnaryExpression)groupExpression;
            return unaryExpression.Type.IsGenericType
                && unaryExpression.Type.GetGenericTypeDefinition() == typeof(IGrouping<,>)
                && unaryExpression.Operand.Type.IsGenericType
                && unaryExpression.Operand.Type.GetGenericTypeDefinition() == typeof(NaiveGrouping<,>);
        }

        protected override Expression VisitJoinCall(Expression left, Expression right, Type leftType, Type rightType, Type keyType, LambdaExpression leftKeySelector, LambdaExpression rightKeySelector)
            => VisitBinaryStreamProcessingMethod(nameof(GenerateJoinCall), left, right, leftType, rightType, keyType, leftKeySelector, rightKeySelector);

        private static Expression<Func<IStreamable<Empty, TLeft>, IStreamable<Empty, TRight>, Expression<Func<TLeft, TKey>>, Expression<Func<TRight, TKey>>, IStreamable<Empty, ValueTuple<TLeft, TRight>>>> GenerateJoinCall<TLeft, TRight, TKey>()
            => (left, right, leftSelector, rightSelector) => left.Join(right, leftSelector, rightSelector, (l, r) => ValueTuple.Create(l, r));

        protected override Expression VisitQuantizeLifetimeCall(Expression argument, Type elementType, long width, long skip, long progress, long offset)
            => VisitUnaryStreamProcessingMethod(nameof(GenerateQuantizeLifetimeCall), argument, elementType, width, skip, progress, offset);

        private static Expression<Func<IStreamable<Empty, TPayload>, IStreamable<Empty, TPayload>>> GenerateQuantizeLifetimeCall<TPayload>(
            long width, long skip, long progress, long offset)
            => (stream) => new QuantizeLifetimeStreamable<Empty, TPayload>(stream, width, skip, progress, offset);

        protected override Expression VisitSelectCall(Expression argument, Type inputElementType, Type outputElementType, LambdaExpression selectExpression, bool includeStartEdge)
        {
            var visitedArgument = Visit(argument);

            // Combine Select with previous element if it was a naive grouping and is being aggregated
            if (!includeStartEdge
                && inputElementType.IsGenericType
                && inputElementType.GetGenericTypeDefinition() == typeof(IGrouping<,>)
                && IsNaiveGrouping(visitedArgument))
            {
                var groupAggregateExpression = (MethodCallExpression)visitedArgument;
                var genericMethodArguments = groupAggregateExpression.Method.GetGenericArguments();
                genericMethodArguments[genericMethodArguments.Length - 1] = outputElementType;
                var newGroupAggregateMethod = groupAggregateExpression.Method
                    .GetGenericMethodDefinition().MakeGenericMethod(genericMethodArguments);

                return Expression.Call(
                    groupAggregateExpression.Object,
                    newGroupAggregateMethod,
                    groupAggregateExpression.Arguments[0],
                    groupAggregateExpression.Arguments[1],
                    groupAggregateExpression.Arguments[2],
                    Expression.Constant(GroupByFirstPassVisitor.CreateConstructorFromSelect(selectExpression)));
            }

            return VisitSelectStreamProcessingMethod(
                           includeStartEdge ? nameof(GenerateSelectWithStartEdgeCall) : nameof(GenerateSelectCall),
                           visitedArgument,
                           inputElementType,
                           outputElementType,
                           selectExpression);
        }

        private static Expression<Func<IStreamable<Empty, TInput>, Expression<Func<TInput, TOutput>>, IStreamable<Empty, TOutput>>> GenerateSelectCall<TInput, TOutput>()
            => (stream, selector) => stream.Select(selector);

        private static Expression<Func<IStreamable<Empty, TInput>, Expression<Func<long, TInput, TOutput>>, IStreamable<Empty, TOutput>>> GenerateSelectWithStartEdgeCall<TInput, TOutput>()
            => (stream, selector) => stream.Select(selector);

        protected override Expression VisitSelectManyCall(Expression argument, Type inputElementType, Type outputElementType, LambdaExpression selectExpression, bool includeStartEdge)
            => VisitSelectStreamProcessingMethod(
                includeStartEdge ? nameof(GenerateSelectManyWithStartEdgeCall) : nameof(GenerateSelectManyCall),
                Visit(argument),
                inputElementType,
                outputElementType,
                selectExpression);

        private static Expression<Func<IStreamable<Empty, TInput>, Expression<Func<TInput, IEnumerable<TOutput>>>, IStreamable<Empty, TOutput>>> GenerateSelectManyCall<TInput, TOutput>()
            => (stream, selector) => stream.SelectMany(selector);

        private static Expression<Func<IStreamable<Empty, TInput>, Expression<Func<long, TInput, IEnumerable<TOutput>>>, IStreamable<Empty, TOutput>>> GenerateSelectManyWithStartEdgeCall<TInput, TOutput>()
            => (stream, selector) => stream.SelectMany(selector);

        protected override Expression VisitSetDurationCall(Expression argument, Type elementType, long duration)
            => VisitUnaryStreamProcessingMethod(nameof(GenerateSetDurationCall), argument, elementType, duration);

        private static Expression<Func<IStreamable<Empty, TPayload>, IStreamable<Empty, TPayload>>> GenerateSetDurationCall<TPayload>(
            long duration)
            => (stream) => stream.AlterEventDuration(duration);

        protected override Expression VisitStitchCall(Expression argument, Type elementType)
            => VisitUnaryStreamProcessingMethod(nameof(GenerateStitchCall), argument, elementType);

        private static Expression<Func<IStreamable<Empty, TPayload>, IStreamable<Empty, TPayload>>> GenerateStitchCall<TPayload>()
            => (stream) => new StitchStreamable<Empty, TPayload>(stream);

        protected override Expression VisitUnionCall(Expression left, Expression right, Type elementType)
            => (GetType()
                .GetMethod(nameof(GenerateUnionCall), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)
                .MakeGenericMethod(elementType).Invoke(null, Array.Empty<object>()) as LambdaExpression)
                .ReplaceParametersInBody(Visit(left), Visit(right));

        private static Expression<Func<IStreamable<Empty, TPayload>, IStreamable<Empty, TPayload>, IStreamable<Empty, TPayload>>> GenerateUnionCall<TPayload>()
            => (left, right) => left.Union(right);

        protected override Expression VisitWhereCall(Expression argument, Type elementType, LambdaExpression whereExpression)
            => VisitUnaryStreamProcessingMethod(nameof(GenerateWhereCall), argument, elementType, whereExpression);

        private static Expression<Func<IStreamable<Empty, TPayload>, IStreamable<Empty, TPayload>>> GenerateWhereCall<TPayload>(
            Expression<Func<TPayload, bool>> predicate)
            => (s) => new WhereStreamable<Empty, TPayload>(s, predicate);

        protected override Expression VisitWhereNotExistsCall(Expression left, Expression right, Type leftType, Type rightType, Type keyType, LambdaExpression leftKeySelector, LambdaExpression rightKeySelector)
            => VisitBinaryStreamProcessingMethod(nameof(GenerateWhereNotExistsCall), left, right, leftType, rightType, keyType, leftKeySelector, rightKeySelector);

        private static Expression<Func<IStreamable<Empty, TLeft>, IStreamable<Empty, TRight>, Expression<Func<TLeft, TKey>>, Expression<Func<TRight, TKey>>, IStreamable<Empty, TLeft>>> GenerateWhereNotExistsCall<TLeft, TRight, TKey>()
            => (left, right, leftSelector, rightSelector) => left.WhereNotExists(right, leftSelector, rightSelector);

        private Expression VisitUnaryStreamProcessingMethod(string methodName, Expression argument, Type elementType, params object[] parameters)
            => (GetType()
                .GetMethod(methodName, System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)
                .MakeGenericMethod(elementType).Invoke(null, parameters) as LambdaExpression)
                .ReplaceParametersInBody(Visit(argument));

        private Expression VisitSelectStreamProcessingMethod(
            string methodName, Expression visitedArgument, Type inputElementType, Type outputElementType, Expression selectExpression)
            => (GetType()
                .GetMethod(methodName, System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)
                .MakeGenericMethod(inputElementType, outputElementType).Invoke(null, Array.Empty<object>()) as LambdaExpression)
                .ReplaceParametersInBody(visitedArgument, Expression.Constant(selectExpression));

        private Expression VisitBinaryStreamProcessingMethod(string methodName, Expression leftInput, Expression rightInput, Type leftInputType, Type rightInputType, Type keyInputType, Expression leftSelector, Expression rightSelector)
            => (GetType()
                .GetMethod(methodName, System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)
                .MakeGenericMethod(leftInputType, rightInputType, keyInputType).Invoke(null, Array.Empty<object>()) as LambdaExpression)
                .ReplaceParametersInBody(Visit(leftInput), Visit(rightInput), Expression.Constant(leftSelector), Expression.Constant(rightSelector));
    }
}
