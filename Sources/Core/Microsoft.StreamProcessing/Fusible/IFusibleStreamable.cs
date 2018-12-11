// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    internal interface IFusibleStreamable<TKey, TPayload> : IStreamable<TKey, TPayload>
    {
        bool CanFuseSelect(LambdaExpression expression, bool hasStart, bool hasKey);
        IFusibleStreamable<TKey, TResult> FuseSelect<TResult>(Expression<Func<TPayload, TResult>> expression);
        IFusibleStreamable<TKey, TResult> FuseSelect<TResult>(Expression<Func<long, TPayload, TResult>> expression);
        IFusibleStreamable<TKey, TResult> FuseSelectWithKey<TResult>(Expression<Func<TKey, TPayload, TResult>> expression);
        IFusibleStreamable<TKey, TResult> FuseSelectWithKey<TResult>(Expression<Func<long, TKey, TPayload, TResult>> expression);

        bool CanFuseSelectMany(LambdaExpression expression, bool hasStart, bool hasKey);
        IFusibleStreamable<TKey, TResult> FuseSelectMany<TResult>(Expression<Func<TPayload, IEnumerable<TResult>>> expression);
        IFusibleStreamable<TKey, TResult> FuseSelectMany<TResult>(Expression<Func<long, TPayload, IEnumerable<TResult>>> expression);
        IFusibleStreamable<TKey, TResult> FuseSelectManyWithKey<TResult>(Expression<Func<TKey, TPayload, IEnumerable<TResult>>> expression);
        IFusibleStreamable<TKey, TResult> FuseSelectManyWithKey<TResult>(Expression<Func<long, TKey, TPayload, IEnumerable<TResult>>> expression);

        IFusibleStreamable<TKey, TPayload> FuseWhere(Expression<Func<TPayload, bool>> expression);

        IFusibleStreamable<TKey, TPayload> FuseSetDurationConstant(long value);

        bool CanFuseEgressObservable { get; }
        IObservable<TResult> FuseEgressObservable<TResult>(Expression<Func<long, long, TPayload, TKey, TResult>> expression, QueryContainer container, string identifier);
    }
}
