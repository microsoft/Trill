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
    internal sealed class SelectManyStreamable<TKey, TSource, TResult> : UnaryStreamable<TKey, TSource, TResult>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        public readonly LambdaExpression Selector;
        internal readonly bool HasStartEdge;
        internal readonly bool HasKey;

        public SelectManyStreamable(IStreamable<TKey, TSource> source, LambdaExpression selector, bool hasStartEdge = false, bool hasKey = false)
            : base(source, source.Properties.SelectMany<TResult>(selector))
        {
            Contract.Requires(source != null);
            Contract.Requires(selector != null);

            this.Selector = selector;
            this.HasStartEdge = hasStartEdge;
            this.HasKey = hasKey;

            Initialize();
        }

        internal override IStreamObserver<TKey, TSource> CreatePipe(IStreamObserver<TKey, TResult> observer)
        {
            if (this.Source.Properties.IsColumnar)
                return GetPipe(observer);

            if (!this.HasStartEdge && !this.HasKey)
                return new SelectManyPipe<TKey, TSource, TResult>(this, observer);
            else if (this.HasStartEdge && !this.HasKey)
                return new SelectManyPipeWithStartEdge<TKey, TSource, TResult>(this, observer);
            else if (!this.HasStartEdge && this.HasKey)
                return new SelectManyKeyPipe<TKey, TSource, TResult>(this, observer);
            else
                return new SelectManyKeyPipeWithStartEdge<TKey, TSource, TResult>(this, observer);
        }

        protected override bool CanGenerateColumnar()
        {
            var typeOfTKey = typeof(TKey);
            var typeOfTSource = typeof(TSource);
            var typeOfTResult = typeof(TResult);

            if (!typeOfTSource.CanRepresentAsColumnar() || !typeOfTResult.CanRepresentAsColumnar() || typeOfTKey.GetPartitionType() != null)
            {
                return false;
            }

            var lookupKey = CacheKey.Create(this.Selector.ExpressionToCSharp(), this.HasStartEdge, this.HasKey);

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => SelectManyTemplate.Generate(this));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private UnaryPipe<TKey, TSource, TResult> GetPipe(IStreamObserver<TKey, TResult> observer)
        {
            var lookupKey = CacheKey.Create(this.Selector.ExpressionToCSharp(), this.HasStartEdge, this.HasKey);

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => SelectManyTemplate.Generate(this));
            Func<PlanNode, IQueryObject, PlanNode> planNode = ((PlanNode p, IQueryObject o) => new SelectManyPlanNode(p, o, typeof(TKey), typeof(TSource), typeof(TResult), this.Selector, this.HasKey, this.HasStartEdge, true, generatedPipeType.Item2));

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, planNode);
            var returnValue = (UnaryPipe<TKey, TSource, TResult>)instance;
            return returnValue;
        }
    }
}