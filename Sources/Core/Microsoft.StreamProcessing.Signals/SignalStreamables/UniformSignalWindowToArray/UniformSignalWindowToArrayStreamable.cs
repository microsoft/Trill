// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;

namespace Microsoft.StreamProcessing.Signal.UDO
{
    internal sealed class UniformSignalWindowToArrayStreamable<TKey, TSource, TResult> : UnaryStreamable<TKey, TSource, TResult[]>
    {
        internal readonly long PeriodTicks;
        internal readonly long OffsetTicks;
        internal readonly int WindowSize;
        internal readonly int HopSize;
        internal readonly bool SetMissingDataToNull;
        internal readonly Func<ISignalWindowObservable<TSource>, ISignalWindowObservable<TResult>> OperatorPipeline;

        public UniformSignalWindowToArrayStreamable(
            UniformSignal<TKey, TSource> source,
            int windowSize, int hopSize, bool setMissingDataToNull, int outputWindowSize,
            Func<ISignalWindowObservable<TSource>, ISignalWindowObservable<TResult>> operatorPipeline)
            : base(source.Stream, source.Properties.WindowedPipelineToArray(
                operatorPipeline, source.Period, outputWindowSize, hopSize, setMissingDataToNull))
        {
            Contract.Requires(source != null);

            PeriodTicks = source.Properties.Period;
            OffsetTicks = source.Properties.Offset;
            WindowSize = windowSize;
            HopSize = hopSize;
            SetMissingDataToNull = setMissingDataToNull;
            OperatorPipeline = operatorPipeline;

            Initialize();
        }

        internal override IStreamObserver<TKey, TSource> CreatePipe(IStreamObserver<TKey, TResult[]> observer)
            => new UniformSignalWindowToArrayPipe<TKey, TSource, TResult>(this, observer);

        // TODO: CODEGEN
        protected override bool CanGenerateColumnar() => false;
    }
}