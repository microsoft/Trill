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
    internal sealed class GroupNestedStreamable<TOuterKey, TSource, TInnerKey> : Streamable<CompoundGroupKey<TOuterKey, TInnerKey>, TSource>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new();

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification="Used to avoid creating redundant readonly property.")]
        public readonly Expression<Func<TSource, TInnerKey>> KeySelector;
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification="Used to avoid creating redundant readonly property.")]
        public readonly IStreamable<TOuterKey, TSource> Source;

        public GroupNestedStreamable(
            IStreamable<TOuterKey, TSource> source, Expression<Func<TSource, TInnerKey>> keySelector)
            : base(source.Properties.GroupNested(keySelector))
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(keySelector);

            this.Source = source;
            this.KeySelector = keySelector;

            if (this.Source.Properties.IsColumnar && !this.CanGenerateColumnar())
            {
                this.properties = this.properties.ToRowBased();
                this.Source = this.Source.ColumnToRow();
            }
        }

        public override IDisposable Subscribe(IStreamObserver<CompoundGroupKey<TOuterKey, TInnerKey>, TSource> observer)
        {
            var pipe = this.Properties.IsColumnar
                ? this.GetPipe(observer)
                : this.CreatePipe(observer);
            return this.Source.Subscribe(pipe);
        }

        private IStreamObserver<TOuterKey, TSource> CreatePipe(IStreamObserver<CompoundGroupKey<TOuterKey, TInnerKey>, TSource> observer)
        {
            if (typeof(TOuterKey).GetPartitionType() == null) return new GroupNestedPipe<TOuterKey, TSource, TInnerKey>(this, observer);
            return new PartitionedGroupNestedPipe<TOuterKey, TSource, TInnerKey>(this, observer);
        }

        private bool CanGenerateColumnar()
        {
            var typeOfTOuterKey = typeof(TOuterKey);
            var typeOfTSource = typeof(TSource);
            var typeOfTInnerKey = typeof(TInnerKey);

            if (!typeOfTSource.CanRepresentAsColumnar()) return false;
            if (typeOfTOuterKey.GetPartitionType() != null) return false;
            if (typeOfTInnerKey.GetPartitionType() != null) return false;

            var lookupKey = CacheKey.Create(this.KeySelector.ToString());

            var comparer = (this.Properties.KeyEqualityComparer as CompoundGroupKeyEqualityComparer<TOuterKey, TInnerKey>).innerComparer.GetGetHashCodeExpr();
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => GroupTemplate.Generate<TOuterKey, TSource, TInnerKey>(comparer, this.KeySelector, true));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private IStreamObserver<TOuterKey, TSource> GetPipe(IStreamObserver<CompoundGroupKey<TOuterKey, TInnerKey>, TSource> observer)
        {
            var lookupKey = CacheKey.Create(this.KeySelector.ToString());

             var comparer = (this.Properties.KeyEqualityComparer as CompoundGroupKeyEqualityComparer<TOuterKey, TInnerKey>).innerComparer.GetGetHashCodeExpr();
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => GroupTemplate.Generate<TOuterKey, TSource, TInnerKey>(comparer, this.KeySelector, true));
            Func<PlanNode, IQueryObject, PlanNode> planNode = ((PlanNode p, IQueryObject o) => new GroupPlanNode(
                    p,
                    o,
                    typeof(TOuterKey),
                    typeof(CompoundGroupKey<TOuterKey, TInnerKey>),
                    typeof(TSource),
                    this.KeySelector,
                    int.MinValue,
                    1,
                    false,
                    true,
                    generatedPipeType.Item2));

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, comparer, this.KeySelector, planNode);
            var returnValue = (IStreamObserver<TOuterKey, TSource>)instance;
            return returnValue;
        }
    }

    internal sealed class GroupStreamable<TOuterKey, TSource, TInnerKey> : Streamable<TInnerKey, TSource>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new();

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification="Used to avoid creating redundant readonly property.")]
        public readonly Expression<Func<TSource, TInnerKey>> KeySelector;
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification="Used to avoid creating redundant readonly property.")]
        public readonly IStreamable<TOuterKey, TSource> Source;

        public GroupStreamable(
            IStreamable<TOuterKey, TSource> source, Expression<Func<TSource, TInnerKey>> keySelector)
            : base(source.Properties.Group(keySelector))
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(keySelector);

            this.Source = source;
            this.KeySelector = keySelector;

            if (this.Source.Properties.IsColumnar && !this.CanGenerateColumnar())
            {
                this.properties = this.properties.ToRowBased();
                this.Source = this.Source.ColumnToRow();
            }
        }

        public override IDisposable Subscribe(IStreamObserver<TInnerKey, TSource> observer)
        {
            var pipe = this.Properties.IsColumnar
                ? this.GetPipe(observer)
                : this.CreatePipe(observer);
            return this.Source.Subscribe(pipe);
        }

        private IStreamObserver<TOuterKey, TSource> CreatePipe(IStreamObserver<TInnerKey, TSource> observer)
        {
            return new GroupPipe<TOuterKey, TSource, TInnerKey>(this, observer);
        }

        private bool CanGenerateColumnar()
        {
            var typeOfTOuterKey = typeof(TOuterKey);
            var typeOfTSource = typeof(TSource);
            var typeOfTInnerKey = typeof(TInnerKey);

            if (!typeOfTSource.CanRepresentAsColumnar()) return false;
            if (typeOfTOuterKey.GetPartitionType() != null) return false;
            if (typeOfTInnerKey.GetPartitionType() != null) return false;

            // For now, restrict the inner key to be anything other than an anonymous type since those can't be ungrouped without using reflection.
            if (typeOfTInnerKey.IsAnonymousType()) return false;

            var lookupKey = CacheKey.Create(this.KeySelector.ToString());

            var comparer = this.Properties.KeyEqualityComparer.GetGetHashCodeExpr();
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => GroupTemplate.Generate<TOuterKey, TSource, TInnerKey>(comparer, this.KeySelector, false));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private IStreamObserver<TOuterKey, TSource> GetPipe(IStreamObserver<TInnerKey, TSource> observer)
        {
            var lookupKey = CacheKey.Create(this.KeySelector.ToString());

             var comparer = this.Properties.KeyEqualityComparer.GetGetHashCodeExpr();
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => GroupTemplate.Generate<TOuterKey, TSource, TInnerKey>(comparer, this.KeySelector, false));
            Func<PlanNode, IQueryObject, PlanNode> planNode = ((PlanNode p, IQueryObject o) => new GroupPlanNode(
                    p,
                    o,
                    typeof(TOuterKey),
                    typeof(TInnerKey),
                    typeof(TSource),
                    this.KeySelector,
                    int.MinValue,
                    1,
                    false,
                    true,
                    generatedPipeType.Item2));

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, comparer, this.KeySelector, planNode);
            var returnValue = (IStreamObserver<TOuterKey, TSource>)instance;
            return returnValue;
        }
    }

}