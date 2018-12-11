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

        public Expression<Func<CompoundGroupKey<TOuterKey, TInnerKey>, CompoundGroupKey<TOuterKey, TInnerKey>, bool>> GetEqualsExpr() => Utility.CreateCompoundEqualityPredicate(this.outerComparer.GetEqualsExpr(), this.innerComparer.GetEqualsExpr());

        public Expression<Func<CompoundGroupKey<TOuterKey, TInnerKey>, int>> GetGetHashCodeExpr() => Utility.CreateCompoundHashPredicate(this.outerComparer.GetGetHashCodeExpr(), this.innerComparer.GetGetHashCodeExpr());
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

        public Expression<Comparison<CompoundGroupKey<TOuterKey, TInnerKey>>> GetCompareExpr() => Utility.CreateCompoundComparerPredicate(this.outerComparer.GetCompareExpr(), this.innerComparer.GetCompareExpr());
    }
}