// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using Microsoft.StreamProcessing.Aggregates;

namespace Microsoft.StreamProcessing.Signal.UDO
{
    internal sealed class UniformSignalWindowArrayUnwindowStreamable<TKey, TSource, TState, TResult> : UnaryStreamable<TKey, TSource[], TResult>
    {
        internal readonly long sourcePeriodTicks;
        internal readonly int hopSize;
        internal readonly int windowSize;
        internal readonly int outputWindowSize;
        internal readonly Func<ISignalWindowObservable<TSource>, ISignalWindowObservable<TResult>> operatorPipeline;
        internal readonly IAggregate<TResult, TState, TResult> aggregate;
        internal readonly UnwindowPipeType pipeType;

        public UniformSignalWindowArrayUnwindowStreamable(
            WindowedUniformSignal<TKey, TSource> source,
            int outputWindowSize,
            Func<ISignalWindowObservable<TSource>, ISignalWindowObservable<TResult>> operatorPipeline,
            IAggregate<TResult, TState, TResult> aggregate)
            : base(source.Stream, source.Properties.WindowedPipelineArrayUnwindow(operatorPipeline))
        {
            Contract.Requires(source != null);

            this.sourcePeriodTicks = source.SourcePeriod;
            this.hopSize = source.HopSize;
            this.windowSize = source.WindowSize;
            this.outputWindowSize = outputWindowSize;
            this.operatorPipeline = operatorPipeline;
            this.aggregate = aggregate;

            bool isUngrouped = typeof(TKey) == typeof(Empty);
            bool isOutputOverlapping = this.outputWindowSize > this.hopSize;

            if (isUngrouped)
            {
                this.pipeType = isOutputOverlapping
                    ? UnwindowPipeType.UngroupedOverlapping
                    : UnwindowPipeType.UngroupedNonOverlapping;
            }
            else pipeType = UnwindowPipeType.Grouped;

            Initialize();
        }

        internal override IStreamObserver<TKey, TSource[]> CreatePipe(IStreamObserver<TKey, TResult> observer)
        {
            switch (this.pipeType)
            {
                case UnwindowPipeType.UngroupedNonOverlapping:
                    return new UniformSignalWindowArrayUnwindowPipeNonOverlapping<TKey, TSource, TState, TResult>(this, observer);
                case UnwindowPipeType.UngroupedOverlapping:
                    return new UniformSignalWindowArrayUnwindowPipeOverlapping<TKey, TSource, TState, TResult>(this, observer);
                case UnwindowPipeType.Grouped:
                    return new UniformSignalWindowArrayUnwindowPipeOverlapping<TKey, TSource, TState, TResult>(this, observer);
                default:
                    return null;
            }
        }

        // TODO: CODEGEN
        protected override bool CanGenerateColumnar() => false;
    }
}