// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    internal sealed class CompoundGroupKeyEqualityComparer<TOuterKey, TInnerKey> : IEqualityComparerExpression<CompoundGroupKey<TOuterKey, TInnerKey>>
    {
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Comparer is a function and thus immutable")]
        public readonly IEqualityComparerExpression<TOuterKey> outerComparer;
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Comparer is a function and thus immutable")]
        public readonly IEqualityComparerExpression<TInnerKey> innerComparer;

        public CompoundGroupKeyEqualityComparer(IEqualityComparerExpression<TOuterKey> outerComparer, IEqualityComparerExpression<TInnerKey> innerComparer)
        {
            this.outerComparer = outerComparer ?? EqualityComparerExpression<TOuterKey>.Default;
            this.innerComparer = innerComparer ?? EqualityComparerExpression<TInnerKey>.Default;
        }

        public Expression<Func<CompoundGroupKey<TOuterKey, TInnerKey>, CompoundGroupKey<TOuterKey, TInnerKey>, bool>> GetEqualsExpr()
        {
            var equalityOnT1 = this.outerComparer.GetEqualsExpr();
            var equalityOnT2 = this.innerComparer.GetEqualsExpr();

            // (e1,e2) => equalityOnT1(e1.outerGroup, e2.outerGroup) && equalityOnT2(e1.innerGroup, e2.innerGroup)
            var e1 = Expression.Parameter(typeof(CompoundGroupKey<TOuterKey, TInnerKey>), "e1");
            var e2 = Expression.Parameter(typeof(CompoundGroupKey<TOuterKey, TInnerKey>), "e2");
            return Expression.Lambda<Func<CompoundGroupKey<TOuterKey, TInnerKey>, CompoundGroupKey<TOuterKey, TInnerKey>, bool>>(
                Expression.AndAlso(
                    Expression.Invoke(equalityOnT1, Expression.Field(e1, "outerGroup"), Expression.Field(e2, "outerGroup")),
                    Expression.Invoke(equalityOnT2, Expression.Field(e1, "innerGroup"), Expression.Field(e2, "innerGroup"))),
                    new ParameterExpression[] { e1, e2 });
        }

        public Expression<Func<CompoundGroupKey<TOuterKey, TInnerKey>, int>> GetGetHashCodeExpr()
        {
            var hashOnT1 = this.outerComparer.GetGetHashCodeExpr();
            var hashOnT2 = this.innerComparer.GetGetHashCodeExpr();

            // (e1) => hashOnT1(e1.outerGroup) && hashOnT2(e1.innerGroup)
            var e1 = Expression.Parameter(typeof(CompoundGroupKey<TOuterKey, TInnerKey>), "e1");
            return Expression.Lambda<Func<CompoundGroupKey<TOuterKey, TInnerKey>, int>>(
                Expression.ExclusiveOr(
                    Expression.Invoke(hashOnT1, Expression.Field(e1, "outerGroup")),
                    Expression.Invoke(hashOnT2, Expression.Field(e1, "innerGroup"))),
                    new ParameterExpression[] { e1 });
        }
    }

    internal sealed class CompoundGroupKeyComparer<TOuterKey, TInnerKey> : IComparerExpression<CompoundGroupKey<TOuterKey, TInnerKey>>
    {
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Comparer is a function and thus immutable")]
        public readonly IComparerExpression<TOuterKey> outerComparer;
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Comparer is a function and thus immutable")]
        public readonly IComparerExpression<TInnerKey> innerComparer;

        public CompoundGroupKeyComparer(IComparerExpression<TOuterKey> outerComparer, IComparerExpression<TInnerKey> innerComparer)
        {
            this.outerComparer = outerComparer ?? ComparerExpression<TOuterKey>.Default;
            this.innerComparer = innerComparer ?? ComparerExpression<TInnerKey>.Default;
        }

        public Expression<Comparison<CompoundGroupKey<TOuterKey, TInnerKey>>> GetCompareExpr()
        {
            var comparerOnT1 = this.outerComparer.GetCompareExpr();
            var comparerOnT2 = this.innerComparer.GetCompareExpr();

            var e1 = Expression.Parameter(typeof(CompoundGroupKey<TOuterKey, TInnerKey>), "e1");
            var e2 = Expression.Parameter(typeof(CompoundGroupKey<TOuterKey, TInnerKey>), "e2");
            if ((comparerOnT1 == null) || typeof(TOuterKey) == typeof(Empty))
            {
                // (e1,e2) => comparerOnT2(e1.innerGroup, e2.innerGroup)
                return Expression.Lambda<Comparison<CompoundGroupKey<TOuterKey, TInnerKey>>>(
                        Expression.Invoke(comparerOnT2, Expression.Field(e1, "innerGroup"), Expression.Field(e2, "innerGroup")),
                        new ParameterExpression[] { e1, e2 });
            }
            else
            {
                // (e1,e2) => comparerOnT1(e1.outerGroup, e2.outerGroup) != 0 ? (comparerOnT1(e1.outerGroup, e2.outerGroup)) : comparerOnT2(e1.innerGroup, e2.innerGroup)
                return Expression.Lambda<Comparison<CompoundGroupKey<TOuterKey, TInnerKey>>>(
                    Expression.Condition(
                    Expression.NotEqual(
                    Expression.Invoke(comparerOnT1, Expression.Field(e1, "outerGroup"), Expression.Field(e2, "outerGroup")),
                    Expression.Constant(0, typeof(int))),
                    Expression.Invoke(comparerOnT1, Expression.Field(e1, "outerGroup"), Expression.Field(e2, "outerGroup")),
                    Expression.Invoke(comparerOnT2, Expression.Field(e1, "innerGroup"), Expression.Field(e2, "innerGroup"))),
                    new ParameterExpression[] { e1, e2 });
            }
        }
    }
}