// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
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

        private static Expression<Func<IStreamable<Empty, TLeft>, IStreamable<Empty, TRight>, IStreamable<Empty, TLeft>>> GenerateClipDurationByEventCall<TLeft, TRight, TKey>(
            Expression<Func<TLeft, TKey>> leftSelector,
            Expression<Func<TRight, TKey>> rightSelector)
            => (left, right) => left.ClipEventDuration(right, leftSelector, rightSelector);

        protected override Expression VisitExtendDurationCall(Expression argument, Type elementType, long duration)
            => VisitUnaryStreamProcessingMethod(nameof(GenerateExtendDurationCall), argument, elementType, duration);

        private static Expression<Func<IStreamable<Empty, TPayload>, IStreamable<Empty, TPayload>>> GenerateExtendDurationCall<TPayload>(
            long duration)
            => (stream) => new ExtendLifetimeStreamable<Empty, TPayload>(stream, duration);

        protected override Expression VisitGroupByCall(Expression argument, Type inputType, Type keyType, Type outputType, LambdaExpression keySelector, LambdaExpression elementSelector)
            => (GetType()
                .GetMethod(nameof(GenerateGroupByCall))
                .MakeGenericMethod(inputType, keyType, outputType).Invoke(null, new object[] { keySelector, elementSelector }) as LambdaExpression)
                .ReplaceParametersInBody(Visit(argument));

        private static Expression<Func<IStreamable<Empty, TPayload>, IStreamable<Empty, NaiveGrouping<TKey, TElement>>>> GenerateGroupByCall<TPayload, TKey, TElement>(
            Expression<Func<TPayload, TKey>> keySelector,
            Expression<Func<TPayload, TElement>> elementSelector)
            => (stream) => stream.GroupAggregate(
                keySelector,
                new NaiveAggregate<TElement, List<TElement>>(o => o).Wrap(elementSelector),
                (g, l) => new NaiveGrouping<TKey, TElement>(g.Key, l));

        protected override Expression VisitJoinCall(Expression left, Expression right, Type leftType, Type rightType, Type keyType, LambdaExpression leftKeySelector, LambdaExpression rightKeySelector)
            => VisitBinaryStreamProcessingMethod(nameof(GenerateJoinCall), left, right, leftType, rightType, keyType, leftKeySelector, rightKeySelector);

        private static Expression<Func<IStreamable<Empty, TLeft>, IStreamable<Empty, TRight>, IStreamable<Empty, ValueTuple<TLeft, TRight>>>> GenerateJoinCall<TLeft, TRight, TKey>(
            Expression<Func<TLeft, TKey>> leftSelector,
            Expression<Func<TRight, TKey>> rightSelector)
            => (left, right) => left.Join(right, leftSelector, rightSelector, (l, r) => ValueTuple.Create(l, r));

        protected override Expression VisitQuantizeLifetimeCall(Expression argument, Type elementType, long width, long skip, long progress, long offset)
            => VisitUnaryStreamProcessingMethod(nameof(GenerateQuantizeLifetimeCall), argument, elementType, width, skip, progress, offset);

        private static Expression<Func<IStreamable<Empty, TPayload>, IStreamable<Empty, TPayload>>> GenerateQuantizeLifetimeCall<TPayload>(
            long width, long skip, long progress, long offset)
            => (stream) => new QuantizeLifetimeStreamable<Empty, TPayload>(stream, width, skip, progress, offset);

        protected override Expression VisitSelectCall(Expression argument, Type inputElementType, Type outputElementType, LambdaExpression selectExpression, bool includeStartEdge)
            => VisitSelectStreamProcessingMethod(
                includeStartEdge ? nameof(GenerateSelectWithStartEdgeCall) : nameof(GenerateSelectCall),
                argument,
                inputElementType,
                outputElementType,
                selectExpression);

        private static Expression<Func<IStreamable<Empty, TInput>, IStreamable<Empty, TOutput>>> GenerateSelectCall<TInput, TOutput>(
            Expression<Func<TInput, TOutput>> selector)
            => (stream) => stream.Select(selector);

        private static Expression<Func<IStreamable<Empty, TInput>, IStreamable<Empty, TOutput>>> GenerateSelectWithStartEdgeCall<TInput, TOutput>(
            Expression<Func<long, TInput, TOutput>> selector)
            => (stream) => stream.Select(selector);

        protected override Expression VisitSelectManyCall(Expression argument, Type inputElementType, Type outputElementType, LambdaExpression selectExpression, bool includeStartEdge)
            => VisitSelectStreamProcessingMethod(
                includeStartEdge ? nameof(GenerateSelectManyWithStartEdgeCall) : nameof(GenerateSelectManyCall),
                argument,
                inputElementType,
                outputElementType,
                selectExpression);

        private static Expression<Func<IStreamable<Empty, TInput>, IStreamable<Empty, TOutput>>> GenerateSelectManyCall<TInput, TOutput>(
            Expression<Func<TInput, IEnumerable<TOutput>>> selector)
            => (stream) => stream.SelectMany(selector);

        private static Expression<Func<IStreamable<Empty, TInput>, IStreamable<Empty, TOutput>>> GenerateSelectManyWithStartEdgeCall<TInput, TOutput>(
            Expression<Func<long, TInput, IEnumerable<TOutput>>> selector)
            => (stream) => stream.SelectMany(selector);

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
                .GetMethod(nameof(GenerateUnionCall))
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

        private static Expression<Func<IStreamable<Empty, TLeft>, IStreamable<Empty, TRight>, IStreamable<Empty, TLeft>>> GenerateWhereNotExistsCall<TLeft, TRight, TKey>(
            Expression<Func<TLeft, TKey>> leftSelector,
            Expression<Func<TRight, TKey>> rightSelector)
            => (left, right) => left.WhereNotExists(right, leftSelector, rightSelector);

        private Expression VisitUnaryStreamProcessingMethod(string methodName, Expression argument, Type elementType, params object[] parameters)
            => (GetType()
                .GetMethod(methodName)
                .MakeGenericMethod(elementType).Invoke(null, parameters) as LambdaExpression)
                .ReplaceParametersInBody(Visit(argument));

        private Expression VisitSelectStreamProcessingMethod(
            string methodName, Expression argument, Type inputElementType, Type outputElementType, params object[] parameters)
            => (GetType()
                .GetMethod(methodName)
                .MakeGenericMethod(inputElementType, outputElementType).Invoke(null, parameters) as LambdaExpression)
                .ReplaceParametersInBody(Visit(argument));

        private Expression VisitBinaryStreamProcessingMethod(string methodName, Expression leftInput, Expression rightInput, Type leftInputType, Type rightInputType, Type keyInputType, params object[] parameters)
            => (GetType()
                .GetMethod(methodName)
                .MakeGenericMethod(leftInputType, rightInputType, keyInputType).Invoke(null, parameters) as LambdaExpression)
                .ReplaceParametersInBody(Visit(leftInput), Visit(rightInput));
    }
}
