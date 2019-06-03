// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using System.Reflection;

namespace Microsoft.StreamProcessing
{
    internal sealed class FusedObservable<TKey, TInput, TPayload, TResult, TOutput> : IObservable<TOutput>
    {
        private readonly IObservable<TInput> observable;
        private readonly ParameterExpression inputParameter;
        private readonly Expression<Func<TInput, long>> startEdgeExtractor;
        private readonly Expression<Func<TInput, long>> endEdgeExtractor;
        private readonly Expression<Func<TInput, TKey>> keyExtractor;
        private readonly Expression<Func<TInput, TPayload>> payloadExtractor;
        private readonly FuseModule fuseModule;
        private readonly Expression<Func<long, long, TResult, TKey, TOutput>> resultConstructor;
        private readonly QueryContainer container;
        private readonly string ingressIdentifier;
        private readonly string egressIdentifier;

        public FusedObservable(
            IObservable<TInput> observable,
            Expression<Func<TInput, long>> startEdgeExtractor,
            Expression<Func<TInput, long>> endEdgeExtractor,
            Expression<Func<TInput, TKey>> keyExtractor,
            Expression<Func<TInput, TPayload>> payloadExtractor,
            FuseModule fuseModule,
            Expression<Func<long, long, TResult, TKey, TOutput>> resultConstructor,
            QueryContainer container,
            string ingressIdentifier,
            string egressIdentifier)
        {
            this.observable = observable;
            this.inputParameter = startEdgeExtractor.Parameters[0];
            this.startEdgeExtractor = startEdgeExtractor;
            this.endEdgeExtractor = Expression.Lambda<Func<TInput, long>>(
                endEdgeExtractor.ReplaceParametersInBody(this.inputParameter), this.inputParameter);
            this.keyExtractor = Expression.Lambda<Func<TInput, TKey>>(
                keyExtractor.ReplaceParametersInBody(this.inputParameter), this.inputParameter);
            this.payloadExtractor = Expression.Lambda<Func<TInput, TPayload>>(
                payloadExtractor.ReplaceParametersInBody(this.inputParameter), this.inputParameter);
            this.fuseModule = fuseModule;
            this.resultConstructor = resultConstructor;

            this.container = container;
            this.ingressIdentifier = ingressIdentifier;
            this.egressIdentifier = egressIdentifier;
            if (this.container != null) this.container.RegisterEgressSite(egressIdentifier);
        }

        public IDisposable Subscribe(IObserver<TOutput> observer)
        {
            Expression<Action<TOutput>> onNext = (o) => observer.OnNext(o);

            var onNextResultBody = onNext.ReplaceParametersInBody(this.resultConstructor.Body);
            var onNextResult = Expression.Lambda<Action<long, long, TResult, TKey>>(
                onNextResultBody, this.resultConstructor.Parameters);

            var onNextFused = this.fuseModule.Coalesce<TPayload, TResult, TKey>(onNextResult);

            Expression onNextPunctuation;
            var baseType = typeof(TOutput);
            if (baseType.GetTypeInfo().IsGenericType) baseType = baseType.GetGenericTypeDefinition();

            onNextPunctuation = baseType == typeof(StreamEvent<>) || baseType == typeof(PartitionedStreamEvent<,>)
                ? Expression.IfThenElse(
                    Expression.NotEqual(onNextFused.Parameters[1], Expression.Constant(long.MinValue)),
                    onNextFused.Body,
                    onNextResultBody)
                : Expression.IfThen(
                    Expression.NotEqual(onNextFused.Parameters[1], Expression.Constant(long.MinValue)),
                    onNextFused.Body);

            var onNextReplacedParameters = ParameterSubstituter.Replace(
                onNextFused.Parameters,
                onNextPunctuation,
                this.startEdgeExtractor.Body,
                this.endEdgeExtractor.Body,
                this.payloadExtractor.Body,
                this.keyExtractor.Body);

            var onNextFinal = Expression.Lambda<Action<TInput>>(onNextReplacedParameters, this.inputParameter);

            var pipe = new FusedObservablePipe<TInput>(observer.OnCompleted, observer.OnError, onNextFinal.Compile(), this.ingressIdentifier);
            if (this.container != null) this.container.RegisterEgressPipe(this.egressIdentifier, pipe);
            return this.observable.Subscribe(pipe);
        }
    }
}
