// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    internal sealed class StatefulSelectStreamable<TKey, TSource, TState, TResult> : UnaryStreamable<TKey, TSource, TResult>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        private readonly Expression<Func<long, TState, TSource, TResult>> statefulSelect;
        private readonly Expression<Func<TState>> initialState;
        private readonly long stateTimeout;

        public StatefulSelectStreamable(
            IStreamable<TKey, TSource> source,
            Expression<Func<long, TState, TSource, TResult>> statefulSelect,
            Expression<Func<TState>> initialState,
            long stateTimeout)
            : base(source, source.Properties.SelectMany<TResult>(statefulSelect))
        {
            this.statefulSelect = statefulSelect;
            this.initialState = initialState;
            this.stateTimeout = stateTimeout;
            this.LookupKey = CacheKey.Create(this.statefulSelect.ExpressionToCSharp(), this.initialState.ExpressionToCSharp(), this.stateTimeout);
            Initialize();
        }

        private CacheKey LookupKey { get; }

        internal override IStreamObserver<TKey, TSource> CreatePipe(IStreamObserver<TKey, TResult> observer)
            => /* this.Source.Properties.IsColumnar
            ? GetPipe(observer)
            : */ new StatefulSelectPipe<TKey, TSource, TState, TResult>(this, observer, this.statefulSelect, this.initialState, this.stateTimeout);

        protected override bool CanGenerateColumnar() => false;
        /*
        {
            var typeOfTKey = typeof(TKey);
            var typeOfTSource = typeof(TSource);
            var typeOfTResult = typeof(TResult);

            if (!typeOfTSource.CanRepresentAsColumnar() || !typeOfTResult.CanRepresentAsColumnar() || typeOfTKey.GetPartitionType() != null)
            {
                return false;
            }

            return GetOrAddColumnarPipeType(out this.errorMessages) != null;
        }

        private UnaryPipe<TKey, TSource, TResult> GetPipe(IStreamObserver<TKey, TResult> observer)
        {
            var generatedPipeType = GetOrAddColumnarPipeType(out var errorMessages);
            Func<PlanNode, IQueryObject, PlanNode> planNodeFunc =
                (planNode, pipe) => new StatefulSelectPlanNode(planNode, pipe, typeof(TKey), typeof(TSource), typeof(TResult), this.statefulSelect, true, errorMessages);

            return (UnaryPipe<TKey, TSource, TResult>)Activator.CreateInstance(generatedPipeType, this, observer, planNodeFunc); // TODO
        }

        private Type GetOrAddColumnarPipeType(out string errorMessages)
        {
            Type type;
            (type, errorMessages) = cachedPipes.GetOrAdd(this.LookupKey, key => StatefulSelectTemplate.Generate(this));
            return type;
        }
        */
    }
}