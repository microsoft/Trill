// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Aggregates;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing.Signal.UDO
{
    internal enum UnwindowPipeType { UngroupedNonOverlapping, UngroupedOverlapping, Grouped }

    internal sealed class UniformSignalWindowUnwindowStreamable<TKey, TSource, TState, TResult> : UnaryStreamable<TKey, TSource, TResult>
    {
        internal readonly long PeriodTicks;
        internal readonly long OffsetTicks;
        internal readonly int WindowSize;
        internal readonly int HopSize;
        internal readonly int OutputWindowSize;
        internal readonly bool SetMissingDataToNull;
        internal readonly Func<ISignalWindowObservable<TSource>, ISignalWindowObservable<TResult>> OperatorPipeline;
        internal readonly IAggregate<TResult, TState, TResult> Aggregate;
        internal readonly UnwindowPipeType pipeType;

        public UniformSignalWindowUnwindowStreamable(
            UniformSignal<TKey, TSource> source,
            int windowSize, int hopSize, bool setMissingDataToNull, int outputWindowSize,
            Func<ISignalWindowObservable<TSource>, ISignalWindowObservable<TResult>> operatorPipeline,
            IAggregate<TResult, TState, TResult> aggregate)
            : base(source.Stream, source.Properties.WindowedPipelineUnwindow(operatorPipeline))
        {
            Contract.Requires(source != null);

            PeriodTicks = source.Properties.Period;
            OffsetTicks = source.Properties.Offset;
            WindowSize = windowSize;
            HopSize = hopSize;
            OutputWindowSize = outputWindowSize;
            SetMissingDataToNull = setMissingDataToNull;
            OperatorPipeline = operatorPipeline;
            Aggregate = aggregate;

            bool isUngrouped = typeof(TKey) == typeof(Empty);
            bool isOutputOverlapping = OutputWindowSize > HopSize;

            if (isUngrouped)
            {
                pipeType = isOutputOverlapping
                    ? UnwindowPipeType.UngroupedOverlapping
                    : UnwindowPipeType.UngroupedNonOverlapping;
            }
            else pipeType = UnwindowPipeType.Grouped;

            Initialize();
        }

        internal override IStreamObserver<TKey, TSource> CreatePipe(IStreamObserver<TKey, TResult> observer)
        {
            switch (this.pipeType)
            {
                case UnwindowPipeType.UngroupedNonOverlapping:
                    return new UngroupedNonOverlappingPipe<TKey, TSource, TState, TResult>(this, observer);
                case UnwindowPipeType.UngroupedOverlapping:
                    return new OverlappingPipe<TKey, TSource, TState, TResult>(this, observer);
                case UnwindowPipeType.Grouped:
                    return new OverlappingPipe<TKey, TSource, TState, TResult>(this, observer);
                default:
                    return null;
            }
        }

        // TODO: CODEGEN
        protected override bool CanGenerateColumnar() => false;
    }
}