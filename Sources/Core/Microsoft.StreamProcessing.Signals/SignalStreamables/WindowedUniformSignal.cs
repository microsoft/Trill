// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace Microsoft.StreamProcessing.Signal.UDO
{
    /// <summary>
    /// Class that holds the set of signal properties for a uniformly-sampled windowed signal
    /// </summary>
    /// <typeparam name="TKey">Type of mapping key for the signal</typeparam>
    /// <typeparam name="TPayload">Type of payload for the signal</typeparam>
    internal class WindowedUniformSignalProperties<TKey, TPayload> : UniformSignalProperties<TKey, TPayload[]>
    {
        internal long SourcePeriod { get; }

        internal int SampleWindowSize { get; }

        internal int SampleHopSize { get; }

        internal bool SetMissingDataToNull { get; }

        internal WindowedUniformSignalProperties(
            UniformSignalProperties<TKey, TPayload[]> properties,
            long sourcePeriod, int sampleWindowSize, int sampleHopSize, bool setMissingDataToNull)
            : base(properties, properties.Period, properties.Offset, properties.InterpolationPolicy)
        {
            SourcePeriod = sourcePeriod;
            SampleWindowSize = sampleWindowSize;
            SampleHopSize = sampleHopSize;
            SetMissingDataToNull = setMissingDataToNull;
        }

        internal UniformSignalProperties<TKey, TResult> WindowedPipelineArrayToAggregate<TResult>(
            Func<ISignalWindowObservable<TPayload>, ISignalObservable<TResult>> operatorPipeline)
        {
            return new UniformSignalProperties<TKey, TResult>(
                CloneToNewPayloadType<TResult>(),
                Period,
                Offset,
                null);
        }

        public WindowedUniformSignalProperties<TKey, TResult> WindowedPipelineArrayToArray<TResult>(
            Func<ISignalWindowObservable<TPayload>, ISignalWindowObservable<TResult>> operatorPipeline,
            int outputWindowSize)
        {
            var properties =
                new UniformSignalProperties<TKey, TResult[]>(
                    CloneToNewPayloadType<TResult[]>(),
                    Period,
                    Offset,
                    null);

            return new WindowedUniformSignalProperties<TKey, TResult>(
                properties,
                SourcePeriod,
                outputWindowSize,
                SampleHopSize,
                SetMissingDataToNull);
        }

        internal UniformSignalProperties<TKey, TResult> WindowedPipelineArrayUnwindow<TResult>(
            Func<ISignalWindowObservable<TPayload>, ISignalWindowObservable<TResult>> operatorPipeline)
        {
            return new UniformSignalProperties<TKey, TResult>(
                CloneToNewPayloadType<TResult>(),
                SourcePeriod,
                Offset,
                null);
        }
    }

    /// <summary>
    /// Wrapper class for holding a uniformly-sampled windowed signal streamable
    /// </summary>
    /// <typeparam name="TKey">Grouping key type</typeparam>
    /// <typeparam name="TPayload">Data/event payload type</typeparam>
    public sealed class WindowedUniformSignal<TKey, TPayload> : UniformSignal<TKey, TPayload[]>
    {
        internal WindowedUniformSignal(IStreamable<TKey, TPayload[]> stream, WindowedUniformSignalProperties<TKey, TPayload> properties)
            : base(stream, properties) { }

        internal new WindowedUniformSignalProperties<TKey, TPayload> Properties => (WindowedUniformSignalProperties<TKey, TPayload>)stream.Properties;

        /// <summary>
        /// Retrieves the operator's source period
        /// </summary>
        public long SourcePeriod => Properties.SourcePeriod;

        /// <summary>
        /// Retrieves the operator's sample window size
        /// </summary>
        public int WindowSize => Properties.SampleWindowSize;

        /// <summary>
        /// Retrieves the operator's sample hop size
        /// </summary>
        public int HopSize => Properties.SampleHopSize;

        /// <summary>
        /// States whether this operator has been instructed to set all missing data to null
        /// </summary>
        public bool SetMissingDataToNull => Properties.SetMissingDataToNull;
    }
}
