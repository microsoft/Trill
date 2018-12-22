// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;
using System.Numerics;
using Microsoft.StreamProcessing.Internal;

namespace Microsoft.StreamProcessing.Signal
{
    /// <summary>
    /// Static class for providing extension methods on signal streamables
    /// </summary>
    public static class SignalStreamableExtensions
    {
        #region Import/export ops

        /// <summary>
        /// Translate an ordinary streamable into a continuous function streamable
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TPayload">Payload type</typeparam>
        /// <typeparam name="TResult">Result type for the continuous function</typeparam>
        /// <param name="stream">Input stream</param>
        /// <param name="function">The function used to derive the continuous values</param>
        /// <returns>A continuous function streamable</returns>
        public static ContinuousSignal<TKey, TPayload, TResult> ToSignal<TKey, TPayload, TResult>(
            this IStreamable<TKey, TPayload> stream,
            Expression<Func<long, TPayload, TResult>> function)
        {
            if (!stream.Properties.IsEventOverlappingFree)
                throw new InvalidOperationException("Only aggregate streams can be converted into signals.");

            return new ContinuousSignal<TKey, TPayload, TResult>(stream, function);
        }

        /// <summary>
        /// Translate a uniform signal into a continuous function streamable
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TPayload">Payload type</typeparam>
        /// <param name="source">Input stream</param>
        /// <returns>A continuous function streamable</returns>
        public static ContinuousSignal<TKey, TPayload, TPayload> ToSignal<TKey, TPayload>(
            this UniformSignal<TKey, TPayload> source)
            => new ContinuousSignal<TKey, TPayload, TPayload>(source.Stream, (t, e) => e);

        /// <summary>
        /// Translate an ordinary streamable into a uniform window streamable
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TPayload">Payload type</typeparam>
        /// <param name="stream">Input stream</param>
        /// <param name="offset">The offset used for data sampling</param>
        /// <param name="period">The period used for data sampling</param>
        /// <param name="policy">An interpolation policy for filling in missing values</param>
        /// <returns>A uniform window streamable</returns>
        public static UniformSignal<TKey, TPayload> ToUniformSignal<TKey, TPayload>(
            this IStreamable<TKey, TPayload> stream, long period, long offset,
            InterpolationPolicy<TPayload> policy = null)
        {
            if (!stream.Properties.IsEventOverlappingFree)
                throw new InvalidOperationException("Only aggregate streams can be converted into signals.");

            var s = new ContinuousSignal<TKey, TPayload, TPayload>(stream, (t, e) => e);
            return s.ToUniformSignal(period, offset, policy);
        }

        /// <summary>
        /// Create a uniformly sampled signal streamable from a continuous function streamable
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TInput">The type of the function input for the continuous function</typeparam>
        /// <typeparam name="TOutput">The type of the function output for the continuous function</typeparam>
        /// <param name="signal">The input, a continuous function streamable</param>
        /// <param name="offset">The offset used for data sampling</param>
        /// <param name="period">The period used for data sampling</param>
        /// <param name="policy">An interpolation policy for filling in missing values</param>
        /// <returns>A uniform window streamable</returns>
        public static UniformSignal<TKey, TOutput> ToUniformSignal<TKey, TInput, TOutput>(
            this ContinuousSignal<TKey, TInput, TOutput> signal, long period, long offset,
            InterpolationPolicy<TOutput> policy = null)
        {
            Contract.Requires(period > 0);

            return signal.SampleAndInterpolate(period, offset, policy);
        }

        /// <summary>
        /// Retrieve the stream portion of a continuous function streamable
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TSource">Payload type</typeparam>
        /// <typeparam name="TResult">Result type for the continuous function</typeparam>
        /// <param name="signal">Input continuous function streamable from which to retrieve the underlying stream</param>
        /// <returns>The underlying stream from a continuous function streamable</returns>
        public static IStreamable<TKey, TSource> ToStream<TKey, TSource, TResult>(
            this ContinuousSignal<TKey, TSource, TResult> signal)
            => signal.Stream;

        /// <summary>
        /// Retrieve the stream portion of a uniform window streamable
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TPayload">Data/event payload type</typeparam>
        /// <param name="source">The uniform window streamable from which to retrieve the stream</param>
        /// <returns>The underlying stream from a uniform window streamable</returns>
        public static IStreamable<TKey, TPayload> ToStream<TKey, TPayload>(
            this UniformSignal<TKey, TPayload> source)
            => source.Stream;

        /// <summary>
        /// Translate an ordinary streamable into a uniform window streamable, with the additional assertion that the underlying stream is already a signal
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TPayload">Payload type</typeparam>
        /// <param name="stream">Input stream</param>
        /// <param name="offset">The offset used for data sampling</param>
        /// <param name="period">The period used for data sampling</param>
        /// <param name="policy">An interpolation policy for filling in missing values</param>
        /// <returns>A uniform window streamable</returns>
        public static UniformSignal<TKey, TPayload> CastToUniformSignal<TKey, TPayload>(
            this IStreamable<TKey, TPayload> stream, long period, long offset,
            InterpolationPolicy<TPayload> policy = null)
        {
            var properties = new UniformSignalProperties<TKey, TPayload>(stream.Properties, period, offset, policy);

            return new UniformSignal<TKey, TPayload>(stream, properties);
        }

        #endregion

        #region Sampling operators

        private static UniformSignal<TKey, TResult> SampleAndInterpolate<TKey, TSource, TResult>(
            this ContinuousSignal<TKey, TSource, TResult> source, long period, long offset,
            InterpolationPolicy<TResult> policy = null)
        {
            Contract.Requires(period > 0);

            // TODO: Consider merging SampleStreamable and InterpolateStreamable
            var stream =
                (policy != null ?
                    (IStreamable<TKey, TResult>)new InterpolateStreamable<TKey, TSource, TResult>(source, period, offset, policy) :
                    new SampleStreamable<TKey, TSource, TResult>(source, period, offset));
            var properties = (UniformSignalProperties<TKey, TResult>)stream.Properties;

            return new UniformSignal<TKey, TResult>(stream, properties);
        }

        /// <summary>
        /// Applies the interpolation policy inside a uniform signal to produce a solid, uninterrupted stream of data
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TPayload">Data/event type of the payload</typeparam>
        /// <param name="source">The uniform signal whose values should be interpolated</param>
        /// <returns>A new uniform signal whose values have been interpolated</returns>
        public static UniformSignal<TKey, TPayload> Interpolate<TKey, TPayload>(
            this UniformSignal<TKey, TPayload> source)
            => source.ToSignal().SampleAndInterpolate(source.Period, source.Offset, source.InterpolationPolicy);

        /// <summary>
        /// Performs a resampling operation over a continuous function stream
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TSource">Data/event type for the domain of the continuous function</typeparam>
        /// <typeparam name="TResult">Data/event type for the range of the continuous function</typeparam>
        /// <param name="source">Data source, a continuous function stream</param>
        /// <param name="period">The time period to be taking samples</param>
        /// <param name="offset">The time offset for taking samples</param>
        /// <param name="policy">The interpolation policy to apply when filling in missing values</param>
        /// <returns>A uniform signal that is the result of sampling from the continuous function stream</returns>
        public static UniformSignal<TKey, TResult> Resample<TKey, TSource, TResult>(
            this ContinuousSignal<TKey, TSource, TResult> source, long period, long offset,
            InterpolationPolicy<TResult> policy = null)
            => source.SampleAndInterpolate(period, offset, policy);

        /// <summary>
        /// Performs a resampling operation over a uniform signal stream
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TPayload">Data/event type for the signal</typeparam>
        /// <param name="source">Data source, a continuous function stream</param>
        /// <param name="period">The time period to be taking samples</param>
        /// <param name="offset">The time offset for taking samples</param>
        /// <returns>A uniform signal that is the result of resampling from the input uniform signal stream</returns>
        public static UniformSignal<TKey, TPayload> Resample<TKey, TPayload>(
            this UniformSignal<TKey, TPayload> source, long period, long offset)
            => period == source.Period && offset == source.Offset
                ? source
                : source.ToSignal().SampleAndInterpolate(period, offset, source.InterpolationPolicy);

        /// <summary>
        /// Performs a resampling operation over a uniform signal stream
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TPayload">Data/event type for the signal</typeparam>
        /// <param name="source">Data source, a continuous function stream</param>
        /// <param name="period">The time period to be taking samples</param>
        /// <param name="offset">The time offset for taking samples</param>
        /// <param name="policy">The interpolation policy to apply when filling in missing values</param>
        /// <returns>A uniform signal that is the result of resampling from the input uniform signal stream</returns>
        public static UniformSignal<TKey, TPayload> Resample<TKey, TPayload>(
            this UniformSignal<TKey, TPayload> source, long period, long offset,
            InterpolationPolicy<TPayload> policy)
        {
            if (period == source.Period && offset == source.Offset &&
                !source.Properties.IsInterpolationPolicyDirty &&
                (policy == null && source.InterpolationPolicy == null ||
                 policy != null && policy.Equals(source.InterpolationPolicy))) { return source; }

            return source.ToSignal().SampleAndInterpolate(period, offset, policy);
        }

        /// <summary>
        /// Perform an upsampling of the data in a uniform window signal
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TPayload">Data/event type for the signal</typeparam>
        /// <param name="source">Uniform signal from which to upsample data</param>
        /// <param name="factor">The factor to which the data should be upsampled</param>
        /// <returns>A uniform window signal whose data has been upsampled</returns>
        public static UniformSignal<TKey, TPayload> Upsample<TKey, TPayload>(
            this UniformSignal<TKey, TPayload> source, int factor)
        {
            Contract.Requires(factor >= 1);

            if (factor == 1) { return source; }

            long period = (long)Math.Ceiling((double)source.Period / factor);

            return source.ToSignal().SampleAndInterpolate(period, source.Offset, source.InterpolationPolicy);
        }

        /// <summary>
        /// Perform a downsampling of the data in a uniform window signal
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TPayload">Data/event type for the signal</typeparam>
        /// <param name="source">Uniform signal from which to downsample data</param>
        /// <param name="factor">The factor to which the data should be downsample</param>
        /// <returns>A uniform window signal whose data has been downsample</returns>
        public static UniformSignal<TKey, TPayload> Downsample<TKey, TPayload>(
            this UniformSignal<TKey, TPayload> source, int factor)
        {
            Contract.Requires(factor >= 1);

            return factor == 1
                ? source
                : source.ToSignal().SampleAndInterpolate(source.Period * factor, source.Offset, source.InterpolationPolicy);
        }

        #endregion

        #region Misc signal operators

        /// <summary>
        /// Performs a correlation over a uniform window signal
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <param name="source">Data source</param>
        /// <param name="coefficients">Coefficients to use in the correlation operation</param>
        /// <returns>The result of the correlation operation</returns>
        public static UniformSignal<TKey, double> Correlation<TKey>(
            this UniformSignal<TKey, double> source, double[] coefficients)
        {
            Contract.Requires(coefficients != null);

            var digitalFilter = new UDO.LinearFeedforwardDoubleFilter(coefficients, false);
            var signal = new DigitalFilterStreamable<TKey, double>(source, digitalFilter);
            var properties = (UniformSignalProperties<TKey, double>)signal.Properties;

            return new UniformSignal<TKey, double>(signal, properties);
        }

        /// <summary>
        /// Performs a correlation over a uniform window signal
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <param name="source">Data source</param>
        /// <param name="coefficients">Coefficients to use in the correlation operation</param>
        /// <returns>The result of the correlation operation</returns>
        public static UniformSignal<TKey, Complex> Correlation<TKey>(
            this UniformSignal<TKey, Complex> source, double[] coefficients)
        {
            Contract.Requires(coefficients != null);

            var digitalFilter = new UDO.LinearComplexFilter(coefficients, Array.Empty<double>(), false);
            var signal = new DigitalFilterStreamable<TKey, Complex>(source, digitalFilter);
            var properties = (UniformSignalProperties<TKey, Complex>)signal.Properties;

            return new UniformSignal<TKey, Complex>(signal, properties);
        }

        /// <summary>
        /// Performs a convolution over a uniform window signal
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <param name="source">Data source</param>
        /// <param name="coefficients">Coefficients to use in the convolution operation</param>
        /// <returns>The result of the convolution operation</returns>
        public static UniformSignal<TKey, double> Convolution<TKey>(
            this UniformSignal<TKey, double> source, double[] coefficients)
            => source.Correlation(coefficients.Reverse());

        /// <summary>
        /// Performs a convolution over a uniform window signal
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <param name="source">Data source</param>
        /// <param name="coefficients">Coefficients to use in the convolution operation</param>
        /// <returns>The result of the convolution operation</returns>
        public static UniformSignal<TKey, Complex> Convolution<TKey>(
            this UniformSignal<TKey, Complex> source, double[] coefficients)
            => source.Correlation(coefficients.Reverse());

        /// <summary>
        /// Performs a finite impulse response filter over a uniform window signal
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <param name="source">Data source</param>
        /// <param name="coefficients">Coefficients to use in the filter operation</param>
        /// <returns>The result of the FIR filter operation</returns>
        public static UniformSignal<TKey, double> FilterFIR<TKey>(
            this UniformSignal<TKey, double> source, double[] coefficients)
        {
            Contract.Requires(coefficients != null);

            var digitalFilter = new UDO.LinearFeedforwardDoubleFilter(coefficients, true);
            var signal = new DigitalFilterStreamable<TKey, double>(source, digitalFilter);
            var properties = (UniformSignalProperties<TKey, double>)signal.Properties;

            return new UniformSignal<TKey, double>(signal, properties);
        }

        /// <summary>
        /// Performs a finite impulse response filter over a uniform window signal
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <param name="source">Data source</param>
        /// <param name="coefficients">Coefficients to use in the filter operation</param>
        /// <returns>The result of the FIR filter operation</returns>
        public static UniformSignal<TKey, Complex> FilterFIR<TKey>(
            this UniformSignal<TKey, Complex> source, double[] coefficients)
        {
            Contract.Requires(coefficients != null);

            var digitalFilter = new UDO.LinearComplexFilter(coefficients, Array.Empty<double>(), true);
            var signal = new DigitalFilterStreamable<TKey, Complex>(source, digitalFilter);
            var properties = (UniformSignalProperties<TKey, Complex>)signal.Properties;

            return new UniformSignal<TKey, Complex>(signal, properties);
        }

        /// <summary>
        /// Performs an infinite impulse response filter over a uniform window signal
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TPayload">Data/event payload type</typeparam>
        /// <param name="source">Data source</param>
        /// <param name="factor">Factor to use in the filter operation</param>
        /// <returns>The result of the IIR filter operation</returns>
        public static UniformSignal<TKey, TPayload> FilterIIR<TKey, TPayload>(
            this UniformSignal<TKey, TPayload> source, double factor)
        {
            if (factor == 0.0) { return source; }

            var signal = new FilterIIRStreamable<TKey, TPayload>(source, factor);
            var properties = (UniformSignalProperties<TKey, TPayload>)signal.Properties;

            return new UniformSignal<TKey, TPayload>(signal, properties);
        }

        /// <summary>
        /// Performs a difference operation over a uniform window signal
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <param name="source">Data source</param>
        /// <returns>The result of the difference operation</returns>
        public static UniformSignal<TKey, double> Difference<TKey>(
            this UniformSignal<TKey, double> source) => source.Correlation(new double[] { -1, 1 });

        /// <summary>
        /// Performs a difference operation over a uniform window signal
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <param name="source">Data source</param>
        /// <returns>The result of the difference operation</returns>
        public static UniformSignal<TKey, Complex> Difference<TKey>(
            this UniformSignal<TKey, Complex> source) => source.Correlation(new double[] { -1, 1 });

        /// <summary>
        /// Timeshifts a continuous function signal
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TSource">Domain type of the continuous function from the input</typeparam>
        /// <typeparam name="TResult">Range type of the continuous function from the input</typeparam>
        /// <param name="source">Source data for the shift operation</param>
        /// <param name="shift">Number of ticks to shift the data</param>
        /// <returns>The same continuous function signal but timeshifted</returns>
        public static ContinuousSignal<TKey, TSource, TResult> Shift<TKey, TSource, TResult>(
            this ContinuousSignal<TKey, TSource, TResult> source, long shift)
            => shift == 0
                ? source
                : new ContinuousSignal<TKey, TSource, TResult>(source.Stream.ShiftEventLifetime(vs => shift), source.Function);

        /// <summary>
        /// Timeshifts a uniform window signal
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TPayload">Data/event payload type of the signal</typeparam>
        /// <param name="source">Source data for the shift operation</param>
        /// <param name="shift">Number of ticks to shift the data</param>
        /// <returns>The same uniform window signal but timeshifted</returns>
        public static UniformSignal<TKey, TPayload> Shift<TKey, TPayload>(
            this UniformSignal<TKey, TPayload> source, long shift)
        {
            if (shift == 0) { return source; }

            var stream = source.Stream.ShiftEventLifetime(vs => shift)
                               .SetProperty().IsEventOverlappingFree(true);

            var offset = (source.Offset + shift) % source.Period;
            var properties = new UniformSignalProperties<TKey, TPayload>(stream.Properties, source.Period, offset, source.InterpolationPolicy);

            return new UniformSignal<TKey, TPayload>(stream, properties);
        }

        /// <summary>
        /// Timeshifts a uniform window signal by a fixed number of samples
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TPayload">Data/event payload type of the signal</typeparam>
        /// <param name="source">Source data for the shift operation</param>
        /// <param name="numOfSamples">Number of samples to shift the data</param>
        /// <returns>The same uniform window signal but timeshifted</returns>
        public static UniformSignal<TKey, TPayload> Shift<TKey, TPayload>(
            this UniformSignal<TKey, TPayload> source, int numOfSamples)
        {
            if (numOfSamples == 0) { return source; }

            var stream = source.Stream.ShiftEventLifetime(vs => numOfSamples * source.Period)
                               .SetProperty().IsEventOverlappingFree(true);

            var properties = new UniformSignalProperties<TKey, TPayload>(stream.Properties, source.Period, source.Offset, source.InterpolationPolicy);

            return new UniformSignal<TKey, TPayload>(stream, properties);
        }

        #endregion

        #region Signal aggregates

        /// <summary>
        /// Perform a windowed summation over a uniform signal
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <param name="source">Input data source signal</param>
        /// <param name="sampleWindowSize">Streaming window size for aggregation</param>
        /// <param name="sampleHopSize">Streaming window hop length for aggregation</param>
        /// <returns>The result of the aggregation</returns>
        public static UniformSignal<TKey, double> Sum<TKey>(
            this UniformSignal<TKey, double> source,
            int sampleWindowSize, int sampleHopSize)
        {
            Contract.Requires(sampleWindowSize > 0);
            Contract.Requires(sampleHopSize > 0);

            long windowSize = sampleWindowSize * source.Period;
            long hopSize = sampleHopSize * source.Period;

            var stream =
                source.Stream
                    .HoppingWindowLifetime(windowSize, hopSize, source.Offset)
                    .Aggregate(w => w.Sum(e => e))
                    .AlterEventDuration(1)
                    .SetProperty().IsEventOverlappingFree(true);
            var properties = new UniformSignalProperties<TKey, double>(
                stream.Properties, hopSize, source.Offset, source.InterpolationPolicy);

            return new UniformSignal<TKey, double>(stream, properties);
        }

#if false
        /// <summary>
        /// Perform a windowed summation over a uniform signal
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <param name="source">Input data source signal</param>
        /// <param name="sampleWindowSize">Streaming window size for aggregation</param>
        /// <param name="sampleHopSize">Streaming window hop length for aggregation</param>
        /// <returns>The result of the aggregation</returns>
        public static UniformSignal<TKey, Complex> Sum<TKey>(
            this UniformSignal<TKey, Complex> source,
            int sampleWindowSize, int sampleHopSize)
        {
            Contract.Requires(sampleWindowSize > 0);
            Contract.Requires(sampleHopSize > 0);

            long windowSize = sampleWindowSize * source.Period;
            long hopSize = sampleHopSize * source.Period;

            var stream =
                source.Stream
                    .HoppingWindowLifetime(windowSize, hopSize, source.Offset)
                    .Aggregate(w => w.Sum(e => e))
                    .AlterEventDuration(1)
                    .SetProperty().IsEventOverlappingFree(true);
            var properties = new UniformSignalProperties<TKey, Complex>(
                stream.Properties, hopSize, source.Offset, source.InterpolationPolicy);

            return new UniformSignal<TKey, Complex>(stream, properties);
        }
#endif

        /// <summary>
        /// Perform a windowed average over a uniform signal
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <param name="source">Input data source signal</param>
        /// <param name="sampleWindowSize">Streaming window size for aggregation</param>
        /// <param name="sampleHopSize">Streaming window hop length for aggregation</param>
        /// <returns>The result of the aggregation</returns>
        public static UniformSignal<TKey, double> Average<TKey>(
            this UniformSignal<TKey, double> source,
            int sampleWindowSize, int sampleHopSize)
        {
            Contract.Requires(sampleWindowSize > 0);
            Contract.Requires(sampleHopSize > 0);

            long windowSize = sampleWindowSize * source.Period;
            long hopSize = sampleHopSize * source.Period;

            var stream =
                source.Stream
                    .HoppingWindowLifetime(windowSize, hopSize, source.Offset)
                    .Aggregate(w => w.Average(e => e))
                    .AlterEventDuration(1)
                    .SetProperty().IsEventOverlappingFree(true);
            var properties = new UniformSignalProperties<TKey, double>(
                stream.Properties, hopSize, source.Offset, source.InterpolationPolicy);

            return new UniformSignal<TKey, double>(stream, properties);
        }

#if false
        /// <summary>
        /// Perform a windowed average over a uniform signal
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <param name="source">Input data source signal</param>
        /// <param name="sampleWindowSize">Streaming window size for aggregation</param>
        /// <param name="sampleHopSize">Streaming window hop length for aggregation</param>
        /// <returns>The result of the aggregation</returns>
        public static UniformSignal<TKey, Complex> Average<TKey>(
            this UniformSignal<TKey, Complex> source,
            int sampleWindowSize, int sampleHopSize)
        {
            Contract.Requires(sampleWindowSize > 0);
            Contract.Requires(sampleHopSize > 0);

            long windowSize = sampleWindowSize * source.Period;
            long hopSize = sampleHopSize * source.Period;

            var stream =
                source.Stream
                    .HoppingWindowLifetime(windowSize, hopSize, source.Offset)
                    .Aggregate(w => w.Average(e => e))
                    .AlterEventDuration(1)
                    .SetProperty().IsEventOverlappingFree(true);
            var properties = new UniformSignalProperties<TKey, Complex>(
                stream.Properties, hopSize, source.Offset, source.InterpolationPolicy);

            return new UniformSignal<TKey, Complex>(stream, properties);
        }
#endif

        /// <summary>
        /// Perform a windowed energy aggregation (sum of squares) over a uniform signal
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <param name="source">Input data source signal</param>
        /// <param name="sampleWindowSize">Streaming window size for aggregation</param>
        /// <param name="sampleHopSize">Streaming window hop length for aggregation</param>
        /// <returns>The result of the aggregation</returns>
        public static UniformSignal<TKey, double> Energy<TKey>(
            this UniformSignal<TKey, double> source,
            int sampleWindowSize, int sampleHopSize)
        {
            Contract.Requires(sampleWindowSize > 0);
            Contract.Requires(sampleHopSize > 0);

            long windowSize = sampleWindowSize * source.Period;
            long hopSize = sampleHopSize * source.Period;

            var stream =
                source.Stream
                    .HoppingWindowLifetime(windowSize, hopSize, source.Offset)
                    .Aggregate(w => w.SumSquares(e => e))
                    .AlterEventDuration(1)
                    .SetProperty().IsEventOverlappingFree(true);
            var properties = new UniformSignalProperties<TKey, double>(
                stream.Properties, hopSize, source.Offset, source.InterpolationPolicy);

            return new UniformSignal<TKey, double>(stream, properties);
        }

#if false
        /// <summary>
        /// Perform a windowed energy aggregation (sum of squares) over a uniform signal
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <param name="source">Input data source signal</param>
        /// <param name="sampleWindowSize">Streaming window size for aggregation</param>
        /// <param name="sampleHopSize">Streaming window hop length for aggregation</param>
        /// <returns>The result of the aggregation</returns>
        public static UniformSignal<TKey, Complex> Energy<TKey>(
            this UniformSignal<TKey, Complex> source,
            int sampleWindowSize, int sampleHopSize)
        {
            Contract.Requires(sampleWindowSize > 0);
            Contract.Requires(sampleHopSize > 0);

            long windowSize = sampleWindowSize * source.Period;
            long hopSize = sampleHopSize * source.Period;

            var stream =
                source.Stream
                    .HoppingWindowLifetime(windowSize, hopSize, source.Offset)
                    .Aggregate(w => w.SumSquares(e => e))
                    .AlterEventDuration(1)
                    .SetProperty().IsEventOverlappingFree(true);
            var properties = new UniformSignalProperties<TKey, Complex>(
                stream.Properties, hopSize, source.Offset, source.InterpolationPolicy);

            return new UniformSignal<TKey, Complex>(stream, properties);
        }
#endif

        /// <summary>
        /// Perform a windowed power aggregation (average of squares) over a uniform signal
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <param name="source">Input data source signal</param>
        /// <param name="sampleWindowSize">Streaming window size for aggregation</param>
        /// <param name="sampleHopSize">Streaming window hop length for aggregation</param>
        /// <returns>The result of the aggregation</returns>
        public static UniformSignal<TKey, double> Power<TKey>(
            this UniformSignal<TKey, double> source,
            int sampleWindowSize, int sampleHopSize)
        {
            Contract.Requires(sampleWindowSize > 0);
            Contract.Requires(sampleHopSize > 0);

            long windowSize = sampleWindowSize * source.Period;
            long hopSize = sampleHopSize * source.Period;

            var stream =
                source.Stream
                    .HoppingWindowLifetime(windowSize, hopSize, source.Offset)
                    .Aggregate(w => w.AverageSquares(e => e))
                    .AlterEventDuration(1)
                    .SetProperty().IsEventOverlappingFree(true);
            var properties = new UniformSignalProperties<TKey, double>(
                stream.Properties, hopSize, source.Offset, source.InterpolationPolicy);

            return new UniformSignal<TKey, double>(stream, properties);
        }

#if false
        /// <summary>
        /// Perform a windowed power aggregation (average of squares) over a uniform signal
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <param name="source">Input data source signal</param>
        /// <param name="sampleWindowSize">Streaming window size for aggregation</param>
        /// <param name="sampleHopSize">Streaming window hop length for aggregation</param>
        /// <returns>The result of the aggregation</returns>
        public static UniformSignal<TKey, Complex> Power<TKey>(
            this UniformSignal<TKey, Complex> source,
            int sampleWindowSize, int sampleHopSize)
        {
            Contract.Requires(sampleWindowSize > 0);
            Contract.Requires(sampleHopSize > 0);

            long windowSize = sampleWindowSize * source.Period;
            long hopSize = sampleHopSize * source.Period;

            var stream =
                source.Stream
                    .HoppingWindowLifetime(windowSize, hopSize, source.Offset)
                    .Aggregate(w => w.AverageSquares(e => e))
                    .AlterEventDuration(1)
                    .SetProperty().IsEventOverlappingFree(true);
            var properties = new UniformSignalProperties<TKey, Complex>(
                stream.Properties, hopSize, source.Offset, source.InterpolationPolicy);

            return new UniformSignal<TKey, Complex>(stream, properties);
        }
#endif

#endregion

#region Binary operations

#region Plus operation

        /// <summary>
        /// Add two signals together
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TLeftSource">Domain for the continuous function on the left input</typeparam>
        /// <typeparam name="TRightSource">Domain for the continuous function on the right input</typeparam>
        /// <typeparam name="TResult">Data/event type of the result</typeparam>
        /// <param name="left">Left operand of the addition</param>
        /// <param name="right">Right operand of the addition</param>
        /// <returns>A continuous function signal whose values come from the addition of the sources</returns>
        public static ContinuousSignal<TKey, StructTuple<TLeftSource, TRightSource>, TResult> Plus<TKey, TLeftSource, TRightSource, TResult>(
            this ContinuousSignal<TKey, TLeftSource, TResult> left,
            ContinuousSignal<TKey, TRightSource, TResult> right)
        {
            Invariant.IsTrue(typeof(TResult).SupportsOperator("+"), "Type " + typeof(TResult) + " does not support the addition operator with second argument " + typeof(TResult) + ".");
            var stream = left.Stream.Join(right.Stream, (l, r) => new StructTuple<TLeftSource, TRightSource> { Item1 = l, Item2 = r });

            var timeParameter = Expression.Parameter(typeof(long));
            var structParameter = Expression.Parameter(typeof(StructTuple<TLeftSource, TRightSource>));
            var leftBody = left.Function.ReplaceParametersInBody(timeParameter, Expression.Property(structParameter, "Item1"));
            var rightBody = right.Function.ReplaceParametersInBody(timeParameter, Expression.Property(structParameter, "Item2"));

            return new ContinuousSignal<TKey, StructTuple<TLeftSource, TRightSource>, TResult>(
                stream,
                Expression.Lambda<Func<long, StructTuple<TLeftSource, TRightSource>, TResult>>(
                    Expression.Add(leftBody, rightBody), timeParameter, structParameter));
        }

        /// <summary>
        /// Add two signals together
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TLeftSource">Domain for the continuous function on the left input</typeparam>
        /// <typeparam name="TResult">Data/event type of the result</typeparam>
        /// <param name="left">Left operand of the addition</param>
        /// <param name="right">Right operand of the addition</param>
        /// <returns>A uniform signal whose values come from the addition of the sources</returns>
        public static UniformSignal<TKey, TResult> Plus<TKey, TLeftSource, TResult>(
            this ContinuousSignal<TKey, TLeftSource, TResult> left,
            UniformSignal<TKey, TResult> right)
        {
            Invariant.IsTrue(typeof(TResult).SupportsOperator("+"), "Type " + typeof(TResult) + " does not support the addition operator with second argument " + typeof(TResult) + ".");
            var leftParam = Expression.Parameter(typeof(TResult));
            var rightParam = Expression.Parameter(typeof(TResult));
            var add = Expression.Add(leftParam, rightParam);
            return left.Join(right, Expression.Lambda<Func<TResult, TResult, TResult>>(add, leftParam, rightParam), null);
        }

        /// <summary>
        /// Add two signals together
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TRightSource">Domain for the continuous function on the right input</typeparam>
        /// <typeparam name="TResult">Data/event type of the result</typeparam>
        /// <param name="left">Left operand of the addition</param>
        /// <param name="right">Right operand of the addition</param>
        /// <returns>A uniform signal whose values come from the addition of the sources</returns>
        public static UniformSignal<TKey, TResult> Plus<TKey, TRightSource, TResult>(
            this UniformSignal<TKey, TResult> left,
            ContinuousSignal<TKey, TRightSource, TResult> right)
        {
            Invariant.IsTrue(typeof(TResult).SupportsOperator("+"), "Type " + typeof(TResult) + " does not support the addition operator with second argument " + typeof(TResult) + ".");
            var leftParam = Expression.Parameter(typeof(TResult));
            var rightParam = Expression.Parameter(typeof(TResult));
            var add = Expression.Add(leftParam, rightParam);
            return left.Join(right, Expression.Lambda<Func<TResult, TResult, TResult>>(add, leftParam, rightParam), null);
        }

#endregion

#region Minus operation

        /// <summary>
        /// Subtract one signal from another
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TLeftSource">Domain for the continuous function on the left input</typeparam>
        /// <typeparam name="TRightSource">Domain for the continuous function on the right input</typeparam>
        /// <typeparam name="TResult">Data/event type of the result</typeparam>
        /// <param name="left">Left operand of the subtraction</param>
        /// <param name="right">Right operand of the subtraction</param>
        /// <returns>A continuous function signal whose values come from the subtraction of the sources</returns>
        public static ContinuousSignal<TKey, StructTuple<TLeftSource, TRightSource>, TResult> Minus<TKey, TLeftSource, TRightSource, TResult>(
            this ContinuousSignal<TKey, TLeftSource, TResult> left,
            ContinuousSignal<TKey, TRightSource, TResult> right)
        {
            Invariant.IsTrue(typeof(TResult).SupportsOperator("-"), "Type " + typeof(TResult) + " does not support the subtraction operator with second argument " + typeof(TResult) + ".");
            var stream = left.Stream.Join(right.Stream, (l, r) => new StructTuple<TLeftSource, TRightSource> { Item1 = l, Item2 = r });

            var timeParameter = Expression.Parameter(typeof(long));
            var structParameter = Expression.Parameter(typeof(StructTuple<TLeftSource, TRightSource>));
            var leftBody = left.Function.ReplaceParametersInBody(timeParameter, Expression.Property(structParameter, "Item1"));
            var rightBody = right.Function.ReplaceParametersInBody(timeParameter, Expression.Property(structParameter, "Item2"));

            return new ContinuousSignal<TKey, StructTuple<TLeftSource, TRightSource>, TResult>(
                stream,
                Expression.Lambda<Func<long, StructTuple<TLeftSource, TRightSource>, TResult>>(
                    Expression.Subtract(leftBody, rightBody), timeParameter, structParameter));
        }

        /// <summary>
        /// Subtract one signal from another
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TLeftSource">Domain for the continuous function on the left input</typeparam>
        /// <typeparam name="TResult">Data/event type of the result</typeparam>
        /// <param name="left">Left operand of the subtraction</param>
        /// <param name="right">Right operand of the subtraction</param>
        /// <returns>A uniform signal whose values come from the subtraction of the sources</returns>
        public static UniformSignal<TKey, TResult> Minus<TKey, TLeftSource, TResult>(
            this ContinuousSignal<TKey, TLeftSource, TResult> left,
            UniformSignal<TKey, TResult> right)
        {
            Invariant.IsTrue(typeof(TResult).SupportsOperator("-"), "Type " + typeof(TResult) + " does not support the subtraction operator with second argument " + typeof(TResult) + ".");
            var leftParam = Expression.Parameter(typeof(TResult));
            var rightParam = Expression.Parameter(typeof(TResult));
            var subtract = Expression.Subtract(leftParam, rightParam);
            return left.Join(right, Expression.Lambda<Func<TResult, TResult, TResult>>(subtract, leftParam, rightParam), null);
        }

        /// <summary>
        /// Subtract one signal from another
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TRightSource">Domain for the continuous function on the right input</typeparam>
        /// <typeparam name="TResult">Data/event type of the result</typeparam>
        /// <param name="left">Left operand of the subtraction</param>
        /// <param name="right">Right operand of the subtraction</param>
        /// <returns>A uniform signal whose values come from the subtraction of the sources</returns>
        public static UniformSignal<TKey, TResult> Minus<TKey, TRightSource, TResult>(
            this UniformSignal<TKey, TResult> left,
            ContinuousSignal<TKey, TRightSource, TResult> right)
        {
            Invariant.IsTrue(typeof(TResult).SupportsOperator("-"), "Type " + typeof(TResult) + " does not support the subtraction operator with second argument " + typeof(TResult) + ".");
            var leftParam = Expression.Parameter(typeof(TResult));
            var rightParam = Expression.Parameter(typeof(TResult));
            var subtract = Expression.Subtract(leftParam, rightParam);
            return left.Join(right, Expression.Lambda<Func<TResult, TResult, TResult>>(subtract, leftParam, rightParam), null);
        }

#endregion

#region Times operation

        /// <summary>
        /// Multiply two signals together
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TLeftSource">Domain for the continuous function on the left input</typeparam>
        /// <typeparam name="TRightSource">Domain for the continuous function on the right input</typeparam>
        /// <typeparam name="TResult">Data/event type of the result</typeparam>
        /// <param name="left">Left operand of the multiplication</param>
        /// <param name="right">Right operand of the multiplication</param>
        /// <returns>A continuous function signal whose values come from the multiplication of the sources</returns>
        public static ContinuousSignal<TKey, StructTuple<TLeftSource, TRightSource>, TResult> Times<TKey, TLeftSource, TRightSource, TResult>(
            this ContinuousSignal<TKey, TLeftSource, TResult> left,
            ContinuousSignal<TKey, TRightSource, TResult> right)
        {
            Invariant.IsTrue(typeof(TResult).SupportsOperator("*"), "Type " + typeof(TResult) + " does not support the multiplication operator with second argument " + typeof(TResult) + ".");
            var stream = left.Stream.Join(right.Stream, (l, r) => new StructTuple<TLeftSource, TRightSource> { Item1 = l, Item2 = r });

            var timeParameter = Expression.Parameter(typeof(long));
            var structParameter = Expression.Parameter(typeof(StructTuple<TLeftSource, TRightSource>));
            var leftBody = left.Function.ReplaceParametersInBody(timeParameter, Expression.Property(structParameter, "Item1"));
            var rightBody = right.Function.ReplaceParametersInBody(timeParameter, Expression.Property(structParameter, "Item2"));

            return new ContinuousSignal<TKey, StructTuple<TLeftSource, TRightSource>, TResult>(
                stream,
                Expression.Lambda<Func<long, StructTuple<TLeftSource, TRightSource>, TResult>>(
                    Expression.Multiply(leftBody, rightBody), timeParameter, structParameter));
        }

        /// <summary>
        /// Multiply two signals together
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TLeftSource">Domain for the continuous function on the left input</typeparam>
        /// <typeparam name="TResult">Data/event type of the result</typeparam>
        /// <param name="left">Left operand of the multiplication</param>
        /// <param name="right">Right operand of the multiplication</param>
        /// <returns>A uniform signal whose values come from the multiplication of the sources</returns>
        public static UniformSignal<TKey, TResult> Times<TKey, TLeftSource, TResult>(
            this ContinuousSignal<TKey, TLeftSource, TResult> left,
            UniformSignal<TKey, TResult> right)
        {
            Invariant.IsTrue(typeof(TResult).SupportsOperator("*"), "Type " + typeof(TResult) + " does not support the multiplication operator with second argument " + typeof(TResult) + ".");
            var leftParam = Expression.Parameter(typeof(TResult));
            var rightParam = Expression.Parameter(typeof(TResult));
            var multiply = Expression.Multiply(leftParam, rightParam);
            return left.Join(right, Expression.Lambda<Func<TResult, TResult, TResult>>(multiply, leftParam, rightParam), null);
        }

        /// <summary>
        /// Multiply two signals together
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TRightSource">Domain for the continuous function on the right input</typeparam>
        /// <typeparam name="TResult">Data/event type of the result</typeparam>
        /// <param name="left">Left operand of the multiplication</param>
        /// <param name="right">Right operand of the multiplication</param>
        /// <returns>A uniform signal whose values come from the multiplication of the sources</returns>
        public static UniformSignal<TKey, TResult> Times<TKey, TRightSource, TResult>(
            this UniformSignal<TKey, TResult> left,
            ContinuousSignal<TKey, TRightSource, TResult> right)
        {
            Invariant.IsTrue(typeof(TResult).SupportsOperator("*"), "Type " + typeof(TResult) + " does not support the multiplication operator with second argument " + typeof(TResult) + ".");
            var leftParam = Expression.Parameter(typeof(TResult));
            var rightParam = Expression.Parameter(typeof(TResult));
            var multiply = Expression.Multiply(leftParam, rightParam);
            return left.Join(right, Expression.Lambda<Func<TResult, TResult, TResult>>(multiply, leftParam, rightParam), null);
        }

#endregion

#endregion

#region Lifted IStreamable operators

#region Join operators

        /// <summary>
        /// Performs a cross-product between one non-uniform signal and one uniform signal
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TLeftSource">Domain type for the function in the left input</typeparam>
        /// <typeparam name="TLeftResult">Range type for the function in the left input</typeparam>
        /// <typeparam name="TRightResult">Data/event type for the left input signal</typeparam>
        /// <typeparam name="TResult">Type of the result of the data</typeparam>
        /// <param name="left">Left input to the join</param>
        /// <param name="right">Right input to the join</param>
        /// <param name="resultSelector">Selector function for determining the result value of the join</param>
        /// <param name="policy">Interpolation policy for filling in missing values</param>
        /// <returns>A uniform signal of the result of the join operation</returns>
        public static UniformSignal<TKey, TResult> Join<TKey, TLeftSource, TLeftResult, TRightResult, TResult>(
            this ContinuousSignal<TKey, TLeftSource, TLeftResult> left,
            UniformSignal<TKey, TRightResult> right,
            Expression<Func<TLeftResult, TRightResult, TResult>> resultSelector,
            InterpolationPolicy<TResult> policy)
        {
            var uniformLeft = left.SampleAndInterpolate(right.Period, right.Offset);
            return uniformLeft.Join(right, resultSelector, policy);
        }

        /// <summary>
        /// Performs a cross-product between one uniform signal and one non-uniform signal
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TLeftResult">Data/event type for the left input signal</typeparam>
        /// <typeparam name="TRightSource">Domain type for the function in the right input</typeparam>
        /// <typeparam name="TRightResult">Range type for the function in the right input</typeparam>
        /// <typeparam name="TResult">Type of the result of the data</typeparam>
        /// <param name="left">Left input to the join</param>
        /// <param name="right">Right input to the join</param>
        /// <param name="resultSelector">Selector function for determining the result value of the join</param>
        /// <param name="policy">Interpolation policy for filling in missing values</param>
        /// <returns>A uniform signal of the result of the join operation</returns>
        public static UniformSignal<TKey, TResult> Join<TKey, TLeftResult, TRightSource, TRightResult, TResult>(
            this UniformSignal<TKey, TLeftResult> left,
            ContinuousSignal<TKey, TRightSource, TRightResult> right,
            Expression<Func<TLeftResult, TRightResult, TResult>> resultSelector,
            InterpolationPolicy<TResult> policy) => left.Join(right.SampleAndInterpolate(left.Period, left.Offset), resultSelector, policy);

        /// <summary>
        /// Performs a cross-product between two uniform signals
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TLeft">Data/event type for the left input signal</typeparam>
        /// <typeparam name="TRight">Data/event type for the right input signal</typeparam>
        /// <typeparam name="TResult">Type of the result of the data</typeparam>
        /// <param name="left">Left input to the join</param>
        /// <param name="right">Right input to the join</param>
        /// <param name="resultSelector">Selector function for determining the result value of the join</param>
        /// <param name="policy">Interpolation policy for filling in missing values</param>
        /// <returns>A uniform signal of the result of the join operation</returns>
        public static UniformSignal<TKey, TResult> Join<TKey, TLeft, TRight, TResult>(
            this UniformSignal<TKey, TLeft> left,
            UniformSignal<TKey, TRight> right,
            Expression<Func<TLeft, TRight, TResult>> resultSelector,
            InterpolationPolicy<TResult> policy)
        {
            IStreamable<TKey, TLeft> leftStream;
            IStreamable<TKey, TRight> rightStream;
            long period;
            long offset;

            if (left.Period == right.Period && left.Offset == right.Offset)
            {
                leftStream = left.Stream;
                rightStream = right.Stream;
                period = left.Period;
                offset = left.Offset;
            }
            else if (left.Period < right.Period || (left.Period == right.Period && left.Offset < right.Offset))
            {
                leftStream = left.Stream;
                rightStream = right.Resample(left.Period, left.Offset).Stream;
                period = left.Period;
                offset = left.Offset;
            }
            else
            {
                leftStream = left.Resample(right.Period, right.Offset).Stream;
                rightStream = right.Stream;
                period = right.Period;
                offset = right.Offset;
            }

            var stream = leftStream.Join(rightStream, resultSelector);
            var properties = new UniformSignalProperties<TKey, TResult>(stream.Properties, period, offset, policy);

            return new UniformSignal<TKey, TResult>(stream, properties);
        }

#endregion

#endregion
    }
}
