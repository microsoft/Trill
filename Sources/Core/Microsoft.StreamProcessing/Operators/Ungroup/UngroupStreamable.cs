#define PARALLELMERGE
// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    internal sealed class UngroupStreamable<TOuterKey, TInnerKey, TInnerResult, TResult> : Streamable<TOuterKey, TResult>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification="Used to avoid creating redundant readonly property.")]
        public readonly Expression<Func<TInnerKey, TInnerResult, TResult>> ResultSelector;
        private IStreamable<CompoundGroupKey<TOuterKey, TInnerKey>, TInnerResult> Source;

        public UngroupStreamable(
            IEqualityComparerExpression<TOuterKey> comparer,
            IStreamable<CompoundGroupKey<TOuterKey, TInnerKey>, TInnerResult> source,
            Expression<Func<TInnerKey, TInnerResult, TResult>> resultSelector)
            : base(source.Properties.Ungroup(resultSelector))
        {
            Contract.Requires(source != null);
            Contract.Requires(resultSelector != null);

            this.Source = source;
            this.ResultSelector = resultSelector;
        }

        public override IDisposable Subscribe(IStreamObserver<TOuterKey, TResult> observer)
        {
            if (this.Properties.IsColumnar && CanGenerateColumnar())
                return this.Source.Subscribe(GetPipe(observer));
            else
                return this.Source.Subscribe(CreatePipe(observer));
        }

        internal IStreamObserver<CompoundGroupKey<TOuterKey, TInnerKey>, TInnerResult> CreatePipe(
            IStreamObserver<TOuterKey, TResult> observer)
        {
            if (typeof(TOuterKey).GetPartitionType() == null) return new UngroupNestedPipe<TOuterKey, TInnerKey, TInnerResult, TResult>(this, observer);
            return new PartitionedUngroupNestedPipe<TOuterKey, TInnerKey, TInnerResult, TResult>(this, observer);
        }

        private bool CanGenerateColumnar()
        {
            var typeOfTOuterKey = typeof(TOuterKey);
            var typeOfTInnerKey = typeof(TInnerKey);
            var typeOfTResult = typeof(TResult);

            if (!typeOfTResult.CanRepresentAsColumnar()) return false;
            if (typeOfTOuterKey.GetPartitionType() != null) return false;
            if (typeOfTInnerKey.GetPartitionType() != null) return false;

            // For now, restrict the inner key to be anything other than an anonymous type since those can't be ungrouped without using reflection.
            if (typeOfTInnerKey.IsAnonymousType()) return false;

            var lookupKey = CacheKey.Create(this.ResultSelector.ExpressionToCSharp());

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => UngroupTemplate.Generate<TOuterKey, TInnerKey, TInnerResult, TResult>(this.ResultSelector));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private IStreamObserver<CompoundGroupKey<TOuterKey, TInnerKey>, TInnerResult> GetPipe(IStreamObserver<TOuterKey, TResult> observer)
        {
            var lookupKey = CacheKey.Create(this.ResultSelector.ExpressionToCSharp());

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => UngroupTemplate.Generate<TOuterKey, TInnerKey, TInnerResult, TResult>(this.ResultSelector));
            Func<PlanNode, IQueryObject, PlanNode> planNode = ((PlanNode p, IQueryObject o) => new UngroupPlanNode(
                    p,
                    o,
                    typeof(CompoundGroupKey<TOuterKey, TInnerKey>),
                    typeof(TOuterKey),
                    typeof(TInnerResult),
                    typeof(TResult),
                    this.ResultSelector,
                    true,
                    generatedPipeType.Item2));

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, planNode);
            var returnValue = (IStreamObserver<CompoundGroupKey<TOuterKey, TInnerKey>, TInnerResult>)instance;
            return returnValue;
        }

    }

    internal sealed class UngroupStreamable<TInnerKey, TInnerResult, TResult> : Streamable<Empty, TResult>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification="Used to avoid creating redundant readonly property.")]
        public readonly Expression<Func<TInnerKey, TInnerResult, TResult>> ResultSelector;
        private IStreamable<TInnerKey, TInnerResult> Source;

        public UngroupStreamable(
            IStreamable<TInnerKey, TInnerResult> source,
            Expression<Func<TInnerKey, TInnerResult, TResult>> resultSelector)
            : base(source.Properties.Ungroup(resultSelector))
        {
            Contract.Requires(source != null);
            Contract.Requires(resultSelector != null);

            this.Source = source;
            this.ResultSelector = resultSelector;
        }

        public override IDisposable Subscribe(IStreamObserver<Empty, TResult> observer)
        {
            if (this.Properties.IsColumnar && CanGenerateColumnar())
                return this.Source.Subscribe(GetPipe(observer));
            else
                return this.Source.Subscribe(CreatePipe(observer));
        }

        internal IStreamObserver<TInnerKey, TInnerResult> CreatePipe(
            IStreamObserver<Empty, TResult> observer)
        {
            return new UngroupPipe<TInnerKey, TInnerResult, TResult>(this, observer);
        }

        private bool CanGenerateColumnar()
        {
            var typeOfTInnerKey = typeof(TInnerKey);
            var typeOfTResult = typeof(TResult);

            if (!typeOfTResult.CanRepresentAsColumnar()) return false;
            if (typeOfTInnerKey.GetPartitionType() != null) return false;

            // For now, restrict the inner key to be anything other than an anonymous type since those can't be ungrouped without using reflection.
            if (typeOfTInnerKey.IsAnonymousType()) return false;

            var lookupKey = CacheKey.Create(this.ResultSelector.ExpressionToCSharp());

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => UngroupTemplate.Generate<TInnerKey, TInnerResult, TResult>(this.ResultSelector));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private IStreamObserver<TInnerKey, TInnerResult> GetPipe(IStreamObserver<Empty, TResult> observer)
        {
            var lookupKey = CacheKey.Create(this.ResultSelector.ExpressionToCSharp());

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => UngroupTemplate.Generate<TInnerKey, TInnerResult, TResult>(this.ResultSelector));
            Func<PlanNode, IQueryObject, PlanNode> planNode = ((PlanNode p, IQueryObject o) => new UngroupPlanNode(
                    p,
                    o,
                    typeof(TInnerKey),
                    typeof(Empty),
                    typeof(TInnerResult),
                    typeof(TResult),
                    this.ResultSelector,
                    true,
                    generatedPipeType.Item2));

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, planNode);
            var returnValue = (IStreamObserver<TInnerKey, TInnerResult>)instance;
            return returnValue;
        }

    }

}