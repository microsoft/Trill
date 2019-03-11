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
    internal sealed class SelectStreamable<TKey, TPayload, TResult> : UnaryStreamable<TKey, TPayload, TResult>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        public Type KeyType { get; } = typeof(TKey);
        public Type PayloadType { get; } = typeof(TPayload);
        public Type ResultType { get; } = typeof(TResult);
        public LambdaExpression Selector { get; }
        public bool HasStartEdge { get; }
        public bool HasKey { get; }

        public SelectStreamable(IStreamable<TKey, TPayload> source, LambdaExpression selector, bool hasStartEdge = false, bool hasKey = false)
            : base(source, source.Properties.Select<TResult>(selector, hasStartEdge, hasKey))
        {
            Contract.Requires(source != null);
            Contract.Requires(selector != null);

            this.Selector = selector;
            this.HasStartEdge = hasStartEdge;
            this.HasKey = hasKey;

            Initialize();
        }

        internal override IStreamObserver<TKey, TPayload> CreatePipe(IStreamObserver<TKey, TResult> observer)
        {
            if (this.Source.Properties.IsColumnar) return GetPipe(observer);

            if (!this.HasStartEdge && !this.HasKey)
                return new SelectPipe<TKey, TPayload, TResult>(this, observer);
            else if (this.HasStartEdge && !this.HasKey)
                return new SelectPipeWithStartEdge<TKey, TPayload, TResult>(this, observer);
            else if (!this.HasStartEdge && this.HasKey)
                return new SelectKeyPipe<TKey, TPayload, TResult>(this, observer);
            else
                return new SelectKeyPipeWithStartEdge<TKey, TPayload, TResult>(this, observer);
        }

        protected override bool CanGenerateColumnar()
        {
            var typeOfTKey = typeof(TKey);
            var typeOfTSource = typeof(TPayload);
            var typeOfTResult = typeof(TResult);

            if (!typeOfTSource.CanRepresentAsColumnar()) return false;
            if (!typeOfTResult.CanRepresentAsColumnar()) return false;
            if (typeOfTKey.GetPartitionType() != null) return false;

            var lookupKey = CacheKey.Create(this.Selector.ExpressionToCSharp(), this.HasStartEdge, this.HasKey);

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => SelectTemplate.Generate(this));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private UnaryPipe<TKey, TPayload, TResult> GetPipe(IStreamObserver<TKey, TResult> observer)
        {
            var lookupKey = CacheKey.Create(this.Selector.ExpressionToCSharp(), this.HasStartEdge, this.HasKey);

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => SelectTemplate.Generate(this));
            Func<PlanNode, IQueryObject, PlanNode> planNode = (PlanNode p, IQueryObject o) => new SelectPlanNode(p, o, typeof(TKey), typeof(TPayload), typeof(TResult), this.Selector, this.HasKey, this.HasStartEdge, true, generatedPipeType.Item2);

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, planNode);
            var returnValue = (UnaryPipe<TKey, TPayload, TResult>)instance;
            return returnValue;
        }

        public override string ToString() => "Select(" + this.Selector.ExpressionToCSharp() + ")";
    }
}
