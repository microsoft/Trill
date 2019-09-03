// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Provider
{
    internal class InOrderProviderVisitor : QStreamableVisitor
    {
        protected override Expression VisitChopCall(Expression argument, long period, long offset) => throw new NotImplementedException();

        protected override Expression VisitClipDurationCall(Expression argument, long maximumDuration) => throw new NotImplementedException();

        protected override Expression VisitClipEventDurationCall(Expression left, Expression right, LambdaExpression leftKeySelector, LambdaExpression rightKeySelector) => throw new NotImplementedException();

        protected override Expression VisitExtendDurationCall(Expression argument, long duration) => throw new NotImplementedException();

        protected override Expression VisitGroupByCall(Expression argument, LambdaExpression keySelector, LambdaExpression elementSelector) => throw new NotImplementedException();

        protected override Expression VisitJoinCall(Expression left, Expression right, LambdaExpression leftKeySelector, LambdaExpression rightKeySelector, LambdaExpression resultSelector) => throw new NotImplementedException();

        protected override Expression VisitQuantizeLifetimeCall(Expression argument, long windowSize, long period, long offset) => throw new NotImplementedException();

        protected override Expression VisitSelectCall(Expression argument, LambdaExpression selectExpression, bool includeStartEdge) => throw new NotImplementedException();

        protected override Expression VisitSelectManyCall(Expression argument, LambdaExpression selectExpression, bool includeStartEdge) => throw new NotImplementedException();

        protected override Expression VisitSetDurationCall(Expression argument, long duration) => throw new NotImplementedException();

        protected override Expression VisitStitchCall(Expression argument) => throw new NotImplementedException();

        protected override Expression VisitUnionCall(Expression left, Expression right) => throw new NotImplementedException();

        protected override Expression VisitWhereCall(Expression argument, LambdaExpression whereExpression)
            => (GetType()
                .GetMethod("GenerateWhereCall")
                .MakeGenericMethod(argument.Type.GetGenericArguments()).Invoke(null, Array.Empty<object>()) as LambdaExpression)
                .ReplaceParametersInBody(Visit(argument), Expression.Constant(whereExpression));

        private static Expression<Func<IStreamable<TKey, TPayload>, Expression<Func<TPayload, bool>>, IStreamable<TKey, TPayload>>> GenerateWhereCall<TKey, TPayload>()
            => (s, p) => new WhereStreamable<TKey, TPayload>(s, p);

        protected override Expression VisitWhereNotExistsCall(Expression left, Expression right, LambdaExpression leftKeySelector, LambdaExpression rightKeySelector) => throw new NotImplementedException();
    }
}
