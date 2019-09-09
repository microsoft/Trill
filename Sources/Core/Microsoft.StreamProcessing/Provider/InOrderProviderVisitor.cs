// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Provider
{
    internal class InOrderProviderVisitor : QStreamableVisitor
    {
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

        protected override Expression VisitClipDurationByEventCall(Expression left, Expression right, Type leftType, Type rightType, Type keyType, LambdaExpression leftKeySelector, LambdaExpression rightKeySelector) => throw new NotImplementedException();

        protected override Expression VisitExtendDurationCall(Expression argument, Type elementType, long duration)
            => VisitUnaryStreamProcessingMethod(nameof(GenerateExtendDurationCall), argument, elementType, duration);

        private static Expression<Func<IStreamable<Empty, TPayload>, IStreamable<Empty, TPayload>>> GenerateExtendDurationCall<TPayload>(
            long duration)
            => (stream) => new ExtendLifetimeStreamable<Empty, TPayload>(stream, duration);

        protected override Expression VisitGroupByCall(Expression argument, Type inputType, Type keyType, Type outputType, LambdaExpression keySelector, LambdaExpression elementSelector) => throw new NotImplementedException();

        protected override Expression VisitJoinCall(Expression left, Expression right, Type leftType, Type rightType, Type keyType, LambdaExpression leftKeySelector, LambdaExpression rightKeySelector) => throw new NotImplementedException();

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

        protected override Expression VisitUnionCall(Expression left, Expression right, Type elementType) => throw new NotImplementedException();

        protected override Expression VisitWhereCall(Expression argument, Type elementType, LambdaExpression whereExpression)
            => VisitUnaryStreamProcessingMethod(nameof(GenerateWhereCall), argument, elementType, whereExpression);

        private static Expression<Func<IStreamable<Empty, TPayload>, IStreamable<Empty, TPayload>>> GenerateWhereCall<TPayload>(
            Expression<Func<TPayload, bool>> predicate)
            => (s) => new WhereStreamable<Empty, TPayload>(s, predicate);

        protected override Expression VisitWhereNotExistsCall(Expression left, Expression right, Type leftType, Type rightType, Type keyType, LambdaExpression leftKeySelector, LambdaExpression rightKeySelector) => throw new NotImplementedException();

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
    }
}
