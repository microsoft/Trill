// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Numerics;

namespace Microsoft.StreamProcessing.Signal
{
    internal static class SignalPropertiesExtensions
    {
        public static UniformSignalProperties<TKey, TResult> Sample<TKey, TSource, TResult>(
            this StreamProperties<TKey, TSource> source,
            long period, long offset)
            => new UniformSignalProperties<TKey, TResult>(
                source.CloneToNewPayloadType<TResult>(),
                period,
                offset,
                null);

        public static UniformSignalProperties<TKey, TResult> Interpolate<TKey, TSource, TResult>(
            this StreamProperties<TKey, TSource> source,
            long period, long offset, InterpolationPolicy<TResult> policy)
            => new UniformSignalProperties<TKey, TResult>(
                source.CloneToNewPayloadType<TResult>(),
                period,
                offset,
                policy);

        public static UniformSignalProperties<TKey, TPayload> DotProduct<TKey, TPayload>(
            this UniformSignalProperties<TKey, TPayload> source, int sampleHopSize)
            => new UniformSignalProperties<TKey, TPayload>(
                source,
                source.Period * sampleHopSize,
                source.Offset,
                source.InterpolationPolicy);

        public static UniformSignalProperties<TKey, Complex[]> DFT<TKey, TPayload>(
            this UniformSignalProperties<TKey, TPayload> source, int sampleHopSize)
            => new UniformSignalProperties<TKey, Complex[]>(
                source.CloneToNewPayloadType<Complex[]>(),
                source.Period * sampleHopSize,
                source.Offset,
                null);

        public static UniformSignalProperties<TKey, Complex> InverseDFT<TKey>(
            this UniformSignalProperties<TKey, Complex[]> source, int sampleWindowSize)
        {
            if (source.Period % sampleWindowSize != 0) throw new ArgumentException("Inverse DFT over sliding windows is not supported.");
            var period = source.Period / sampleWindowSize;

            return new UniformSignalProperties<TKey, Complex>(
                source.CloneToNewPayloadType<Complex>(),
                period,
                source.Offset,
                null);
        }

        public static UniformSignalProperties<TKey, TOutput> UniformSignalOperator<TKey, TInput, TOutput>(
            this UniformSignalProperties<TKey, TInput> source,
            int sampleHopSize, InterpolationPolicy<TOutput> policy)
            => new UniformSignalProperties<TKey, TOutput>(
                source.CloneToNewPayloadType<TOutput>(),
                source.Period * sampleHopSize,
                source.Offset,
                policy);
    }

    /// <summary>
    /// Class that holds the set of signal properties for a uniformly-sampled signal
    /// </summary>
    /// <typeparam name="TKey">Type of mapping key for the signal</typeparam>
    /// <typeparam name="TPayload">Type of payload for the signal</typeparam>
    internal class UniformSignalProperties<TKey, TPayload> : StreamProperties<TKey, TPayload>
    {
        /// <summary>
        /// Indicates the interpolation policy of the signal.
        /// </summary>
        internal InterpolationPolicy<TPayload> InterpolationPolicy { get; }

        /// <summary>
        /// Indicates whether the interpolation policy changed but the signal has not been re-interpolated.
        /// </summary>
        internal bool IsInterpolationPolicyDirty => InterpolationPolicy != null;

        public UniformSignalProperties(
            StreamProperties<TKey, TPayload> properties,
            long period,
            long offset,
            InterpolationPolicy<TPayload> policy)
            : base(properties)
        {
            // A discrete-time signal consists of a sequence of event points
            IsConstantDuration = true;
            ConstantDurationLength = 1;

            IsIntervalFree = false;
            IsSyncTimeSimultaneityFree = true;

            // Discrete-time signal is uniformly sampled
            IsConstantHop = true;
            ConstantHopLength = period;
            ConstantHopOffset = offset;

            this.InterpolationPolicy = policy;
        }

        internal new UniformSignalProperties<TKey, TPayload> Clone() => new UniformSignalProperties<TKey, TPayload>(this, Period, Offset, InterpolationPolicy);

        internal long Period => ConstantHopLength.Value;

        internal long Offset => ConstantHopOffset.Value;

        internal UniformSignalProperties<TKey, TPayload> SetStreamProperties(StreamProperties<TKey, TPayload> properties)
            => new UniformSignalProperties<TKey, TPayload>(properties, Period, Offset, InterpolationPolicy);

        public UniformSignalProperties<TKey, TPayload> FilterIIR() => Clone();

        public UniformSignalProperties<TKey, TPayload> DigitalFilter() => Clone();

        public UniformSignalProperties<TKey, TResult> WindowedPipelineToAggregate<TResult>(
            Func<UDO.ISignalWindowObservable<TPayload>, UDO.ISignalObservable<TResult>> operatorPipeline,
            int sampleHopSize)
            => new UniformSignalProperties<TKey, TResult>(
                CloneToNewPayloadType<TResult>(),
                Period * sampleHopSize,
                Offset,
                null);

        public UDO.WindowedUniformSignalProperties<TKey, TResult> WindowedPipelineToArray<TResult>(
            Func<UDO.ISignalWindowObservable<TPayload>, UDO.ISignalWindowObservable<TResult>> operatorPipeline,
            long sourcePeriod, int sampleWindowSize, int sampleHopSize, bool setMissingDataToNull)
        {
            var properties =
                new UniformSignalProperties<TKey, TResult[]>(
                    CloneToNewPayloadType<TResult[]>(),
                    Period * sampleHopSize,
                    Offset,
                    null);

            return new UDO.WindowedUniformSignalProperties<TKey, TResult>(
                properties,
                sourcePeriod,
                sampleWindowSize,
                sampleHopSize,
                setMissingDataToNull);
        }

        public UniformSignalProperties<TKey, TResult> WindowedPipelineUnwindow<TResult>(
            Func<UDO.ISignalWindowObservable<TPayload>, UDO.ISignalWindowObservable<TResult>> operatorPipeline)
            => new UniformSignalProperties<TKey, TResult>(
                CloneToNewPayloadType<TResult>(),
                Period,
                Offset,
                null);
    }
}
