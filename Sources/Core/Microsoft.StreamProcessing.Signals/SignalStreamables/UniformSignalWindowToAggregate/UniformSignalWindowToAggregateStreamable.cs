// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;

namespace Microsoft.StreamProcessing.Signal.UDO
{
    internal sealed class UniformSignalWindowToAggregateStreamable<TKey, TSource, TResult> : UnaryStreamable<TKey, TSource, TResult>
    {
        internal readonly long periodTicks;
        internal readonly long offsetTicks;
        internal readonly int windowSize;
        internal readonly int hopSize;
        internal readonly bool setMissingDataToNull;
        internal readonly Func<ISignalWindowObservable<TSource>, ISignalObservable<TResult>> operatorPipeline;

        public UniformSignalWindowToAggregateStreamable(
            UniformSignal<TKey, TSource> source,
            int sampleWindowSize, int sampleHopSize, bool setMissingDataToNull,
            Func<ISignalWindowObservable<TSource>, ISignalObservable<TResult>> operatorPipeline)
            : base(source.Stream, source.Properties.WindowedPipelineToAggregate(operatorPipeline, sampleHopSize))
        {
            Contract.Requires(source != null);

            this.periodTicks = source.Properties.Period;
            this.offsetTicks = source.Properties.Offset;
            this.windowSize = sampleWindowSize;
            this.hopSize = sampleHopSize;
            this.setMissingDataToNull = setMissingDataToNull;
            this.operatorPipeline = operatorPipeline;

            Initialize();
        }

        internal override IStreamObserver<TKey, TSource> CreatePipe(IStreamObserver<TKey, TResult> observer)
            => new UniformSignalWindowToAggregatePipe<TKey, TSource, TResult>(this, observer);

        // TODO: CODEGEN
        protected override bool CanGenerateColumnar() => false;
    }
}