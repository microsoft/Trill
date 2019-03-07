// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using System.Reflection;

namespace Microsoft.StreamProcessing
{
    internal static class EqualityComparerExtensions
    {
        public static IEqualityComparerExpression<T> GetCompoundEqualityComparerExpression<T, T1, T2, T3>(Expression<Func<T, T1>> lambda1, IEqualityComparerExpression<T1> iece1, Expression<Func<T, T2>> lambda2, IEqualityComparerExpression<T2> iece2, Expression<Func<T, T3>> lambda3, IEqualityComparerExpression<T3> iece3)
        {
            var iece1EqualsExpr = iece1.GetEqualsExpr();
            var iece2EqualsExpr = iece2.GetEqualsExpr();
            var iece3EqualsExpr = iece3.GetEqualsExpr();

            Expression<Func<T, T, bool>> equalsTemplate =
                (left, right) =>
                    CallInliner.Call(iece1EqualsExpr, CallInliner.Call(lambda1, left), CallInliner.Call(lambda1, right))
                    && CallInliner.Call(iece2EqualsExpr, CallInliner.Call(lambda2, left), CallInliner.Call(lambda2, right))
                    && CallInliner.Call(iece3EqualsExpr, CallInliner.Call(lambda3, left), CallInliner.Call(lambda3, right))
                    ;

            var iece1HashExpr = iece1.GetGetHashCodeExpr();
            var iece2HashExpr = iece2.GetGetHashCodeExpr();
            var iece3HashExpr = iece3.GetGetHashCodeExpr();

            Expression<Func<T, int>> hashTemplate =
                value =>
                    CallInliner.Call(iece1HashExpr, CallInliner.Call(lambda1, value)) ^
                    CallInliner.Call(iece2HashExpr, CallInliner.Call(lambda2, value)) ^
                    CallInliner.Call(iece3HashExpr, CallInliner.Call(lambda3, value))
                    ;

            return new EqualityComparerExpression<T>(equalsTemplate.InlineCalls(), hashTemplate.InlineCalls());
        }

        public static IEqualityComparerExpression<T> GetCompoundEqualityComparerExpression<T, T1, T2>(Expression<Func<T, T1>> lambda1, IEqualityComparerExpression<T1> iece1, Expression<Func<T, T2>> lambda2, IEqualityComparerExpression<T2> iece2)
        {
            var iece1EqualsExpr = iece1.GetEqualsExpr();
            var iece2EqualsExpr = iece2.GetEqualsExpr();
            Expression<Func<T, T, bool>> equalsTemplate =
                (left, right) =>
                    CallInliner.Call(iece1EqualsExpr, CallInliner.Call(lambda1, left), CallInliner.Call(lambda1, right))
                    && CallInliner.Call(iece2EqualsExpr, CallInliner.Call(lambda2, left), CallInliner.Call(lambda2, right));

            var iece1HashExpr = iece1.GetGetHashCodeExpr();
            var iece2HashExpr = iece2.GetGetHashCodeExpr();
            Expression<Func<T, int>> hashTemplate =
                value =>
                    CallInliner.Call(iece1HashExpr, CallInliner.Call(lambda1, value)) ^
                    CallInliner.Call(iece2HashExpr, CallInliner.Call(lambda2, value));

            return new EqualityComparerExpression<T>(equalsTemplate.InlineCalls(), hashTemplate.InlineCalls());
        }

        public static bool ExpressionEquals(this Expression source, Expression other)
            => EqualityComparer.IsEqual(source, other);

        public static bool ExpressionEquals<T>(this IComparerExpression<T> source, IComparerExpression<T> other)
            => EqualityComparer.IsEqual(source.GetCompareExpr(), other.GetCompareExpr());

        public static bool ExpressionEquals<T>(this IEqualityComparerExpression<T> source, IEqualityComparerExpression<T> other)
            => EqualityComparer.IsEqual(source.GetEqualsExpr(), other.GetEqualsExpr())
            && EqualityComparer.IsEqual(source.GetGetHashCodeExpr(), other.GetGetHashCodeExpr());

        /// <summary>
        /// Performs a special kind of equality test on IEqualityComparer&lt;T&gt; in which case, both the Equals function and GetHashCode function are checked
        /// If the objects are not of the same type, then false is returned.
        /// Otherwise, an expression tree visitor walks the trees associated with the two arguments
        /// and returns true only if they are isomorphic modulo alpha conversions.
        /// </summary>
        public static bool EqualityExpressionEquals(this object source, object other)
        {
            var t = source.GetType();
            foreach (var iface in t.GetTypeInfo().GetInterfaces())
            {
                if (!iface.GetTypeInfo().IsGenericType) continue;
                if (!iface.GetGenericTypeDefinition().Equals(typeof(IEqualityComparerExpression<>))) continue;
                if (iface.GetTypeInfo().IsAssignableFrom(other.GetType()))
                    return TryIsEqualIECE(source, other);
            }
            return false;
        }

        // Force the binding to happen at runtime
        private static dynamic TryIsEqualIECE(dynamic obj1, dynamic obj2) => IsEqualIECE(obj1, obj2);

        private static bool IsEqualIECE<T>(IEqualityComparerExpression<T> o1, IEqualityComparerExpression<T> o2)
            => EqualityComparer.IsEqual(o1.GetEqualsExpr(), o2.GetEqualsExpr())
            && EqualityComparer.IsEqual(o1.GetGetHashCodeExpr(), o2.GetGetHashCodeExpr());

        // catch-all for other types
        private static bool IsEqualIECE(object o1, object o2) => false;
    }
}