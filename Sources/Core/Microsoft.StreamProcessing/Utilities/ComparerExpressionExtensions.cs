// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Internal
{
    internal static class ComparerExpressionExtensions
    {
        public static IComparerExpression<TNewInput> TransformInput<TOldInput, TNewInput>(this IComparerExpression<TOldInput> comparer, Expression<Func<TNewInput, TOldInput>> transform)
        {
            Contract.Requires(comparer != null);
            Contract.Requires(transform != null);
            var expression = comparer.GetCompareExpr();
            Expression<Comparison<TNewInput>> template =
                (left, right) => CallInliner.Call(expression, CallInliner.Call(transform, left), CallInliner.Call(transform, right));
            var transformedExpression = template.InlineCalls();
            return new ComparerExpression<TNewInput>(transformedExpression);
        }
    }
}
