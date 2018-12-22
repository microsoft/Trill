// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;

namespace Microsoft.StreamProcessing.Signal.UDO
{
    internal sealed class UniformSignalWindowArrayToAggregateStreamable<TKey, TSource, TResult> : UnaryStreamable<TKey, TSource[], TResult>
    {
        internal readonly int WindowSize;
        internal readonly Func<ISignalWindowObservable<TSource>, ISignalObservable<TResult>> OperatorPipeline;

        public UniformSignalWindowArrayToAggregateStreamable(
            WindowedUniformSignal<TKey, TSource> source,
            Func<ISignalWindowObservable<TSource>, ISignalObservable<TResult>> operatorPipeline)
            : base(source.Stream, source.Properties.WindowedPipelineArrayToAggregate(operatorPipeline))
        {
            Contract.Requires(source != null);

            WindowSize = source.WindowSize;
            OperatorPipeline = operatorPipeline;

            Initialize();
        }

        internal override IStreamObserver<TKey, TSource[]> CreatePipe(IStreamObserver<TKey, TResult> observer)
            => new UniformSignalWindowArrayToAggregatePipe<TKey, TSource, TResult>(this, observer);

        // TODO: CODEGEN
        protected override bool CanGenerateColumnar() => false;
    }
}