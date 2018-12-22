// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Numerics;
using Microsoft.StreamProcessing.Aggregates;

namespace Microsoft.StreamProcessing.Signal.UDO
{
    /// <summary>
    /// Static class for providing extension methods on signal and windowed signal classes
    /// </summary>
    public static class SignalWindowStreamableExtensions
    {
        /// <summary>
        /// Transform a uniform signal to a windowed uniform signal
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TInput">Data/event payload type for the original uniform signal</typeparam>
        /// <typeparam name="TOutput">Data/event payload type for the resulting windowed uniform signal</typeparam>
        /// <param name="source">The input signal stream</param>
        /// <param name="sampleWindowSize">The sampling window size for the windowed signal</param>
        /// <param name="sampleHopSize">The sampling hop size for the windowed signal</param>
        /// <param name="setMissingDataToNull">States whether to initialize all missing data to null</param>
        /// <param name="windowFunc">A windowed transformation function</param>
        /// <returns>A windowed signal</returns>
        public static WindowedUniformSignal<TKey, TOutput> Window<TKey, TInput, TOutput>(
            this UniformSignal<TKey, TInput> source,
            int sampleWindowSize, int sampleHopSize, bool setMissingDataToNull,
            Func<ISignalWindowObservable<TInput>, ISignalWindowObservable<TOutput>> windowFunc)
        {
            var outputWindowSize = windowFunc(new BaseSignalWindowObservable<TInput>(sampleWindowSize)).WindowSize;
            var signal = new UniformSignalWindowToArrayStreamable<TKey, TInput, TOutput>(
                source, sampleWindowSize, sampleHopSize, setMissingDataToNull, outputWindowSize, windowFunc);
            var properties = (WindowedUniformSignalProperties<TKey, TOutput>)signal.Properties;

            return new WindowedUniformSignal<TKey, TOutput>(signal, properties);
        }

        /// <summary>
        /// Transform a uniform signal to another uniform signal via windowing
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TInput">Data/event payload type for the original uniform signal</typeparam>
        /// <typeparam name="TOutput">Data/event payload type for the resulting uniform signal</typeparam>
        /// <param name="source">The input signal stream</param>
        /// <param name="sampleWindowSize">The sampling window size for the windowed signal</param>
        /// <param name="sampleHopSize">The sampling hop size for the windowed signal</param>
        /// <param name="setMissingDataToNull">States whether to initialize all missing data to null</param>
        /// <param name="windowFunc">A windowed transformation function</param>
        /// <returns>A new, transformed uniform signal</returns>
        public static UniformSignal<TKey, TOutput> Window<TKey, TInput, TOutput>(
            this UniformSignal<TKey, TInput> source,
            int sampleWindowSize, int sampleHopSize, bool setMissingDataToNull,
            Func<ISignalWindowObservable<TInput>, ISignalObservable<TOutput>> windowFunc)
        {
            var signal = new UniformSignalWindowToAggregateStreamable<TKey, TInput, TOutput>(
                source, sampleWindowSize, sampleHopSize, setMissingDataToNull, windowFunc);
            var properties = (UniformSignalProperties<TKey, TOutput>)signal.Properties;

            return new UniformSignal<TKey, TOutput>(signal, properties);
        }

        /// <summary>
        /// Transform a uniform signal to another uniform signal via windowing
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TInput">Data/event payload type for the original uniform signal</typeparam>
        /// <typeparam name="TOutput">Data/event payload type for the resulting uniform signal</typeparam>
        /// <typeparam name="TState">Type of state involved in the aggregation</typeparam>
        /// <param name="source">The input signal stream</param>
        /// <param name="sampleWindowSize">The sampling window size for the windowed signal</param>
        /// <param name="sampleHopSize">The sampling hop size for the windowed signal</param>
        /// <param name="setMissingDataToNull">States whether to initialize all missing data to null</param>
        /// <param name="windowFunc">A windowed transformation function</param>
        /// <param name="aggregateFunc">An aggregation function</param>
        /// <returns>A new, transformed uniform signal</returns>
        public static UniformSignal<TKey, TOutput> Window<TKey, TInput, TOutput, TState>(
            this UniformSignal<TKey, TInput> source,
            int sampleWindowSize, int sampleHopSize, bool setMissingDataToNull,
            Func<ISignalWindowObservable<TInput>, ISignalWindowObservable<TOutput>> windowFunc,
            Func<Window<TKey, TOutput>, IAggregate<TOutput, TState, TOutput>> aggregateFunc)
        {
            var outputWindowSize = windowFunc(new BaseSignalWindowObservable<TInput>(sampleWindowSize)).WindowSize;
            var aggregate = aggregateFunc(new Window<TKey, TOutput>(source.Properties.CloneToNewPayloadType<TOutput>()));

            var signal = new UniformSignalWindowUnwindowStreamable<TKey, TInput, TState, TOutput>(
                source, sampleWindowSize, sampleHopSize, setMissingDataToNull,
                outputWindowSize, windowFunc, aggregate);
            var properties = (UniformSignalProperties<TKey, TOutput>)signal.Properties;

            return new UniformSignal<TKey, TOutput>(signal, properties);
        }

        /// <summary>
        /// Select method applying an array-to-array window transformation
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TInput">Data/event payload type for the original signal</typeparam>
        /// <typeparam name="TOutput">Data/event payload type for the resulting signal</typeparam>
        /// <param name="source">The input signal stream</param>
        /// <param name="windowFunc">A windowed transformation function</param>
        /// <returns>A new windowed uniform signal whose window arrays have been transformed</returns>
        public static WindowedUniformSignal<TKey, TOutput> Select<TKey, TInput, TOutput>(
            this WindowedUniformSignal<TKey, TInput> source,
            Func<ISignalWindowObservable<TInput>, ISignalWindowObservable<TOutput>> windowFunc)
        {
            var outputWindowSize = windowFunc(new BaseSignalWindowObservable<TInput>(source.WindowSize)).WindowSize;
            var signal = new UniformSignalWindowArrayToArrayStreamable<TKey, TInput, TOutput>(source, outputWindowSize, windowFunc);
            var properties = (WindowedUniformSignalProperties<TKey, TOutput>)signal.Properties;

            return new WindowedUniformSignal<TKey, TOutput>(signal, properties);
        }

        /// <summary>
        /// Select method applying an array-to-array window transformation
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TInput">Data/event payload type for the original signal</typeparam>
        /// <typeparam name="TOutput">Data/event payload type for the resulting signal</typeparam>
        /// <param name="source">The input signal stream</param>
        /// <param name="windowFunc">A windowed transformation function</param>
        /// <returns>A new uniform signal whose window arrays have been transformed</returns>
        public static UniformSignal<TKey, TOutput> Select<TKey, TInput, TOutput>(
            this WindowedUniformSignal<TKey, TInput> source,
            Func<ISignalWindowObservable<TInput>, ISignalObservable<TOutput>> windowFunc)
        {
            var signal = new UniformSignalWindowArrayToAggregateStreamable<TKey, TInput, TOutput>(source, windowFunc);
            var properties = (UniformSignalProperties<TKey, TOutput>)signal.Properties;

            return new UniformSignal<TKey, TOutput>(signal, properties);
        }

        /// <summary>
        /// Select method applying an array-to-array window transformation
        /// </summary>
        /// <typeparam name="TKey">Grouping key type</typeparam>
        /// <typeparam name="TInput">Data/event payload type for the original signal</typeparam>
        /// <typeparam name="TOutput">Data/event payload type for the resulting signal</typeparam>
        /// <typeparam name="TState">Type of state involved in the aggregation</typeparam>
        /// <param name="source">The input signal stream</param>
        /// <param name="windowFunc">A windowed transformation function</param>
        /// <param name="aggregateFunc">An aggregation function</param>
        /// <returns>A new uniform signal whose window arrays have been transformed</returns>
        public static UniformSignal<TKey, TOutput> Select<TKey, TInput, TOutput, TState>(
            this WindowedUniformSignal<TKey, TInput> source,
            Func<ISignalWindowObservable<TInput>, ISignalWindowObservable<TOutput>> windowFunc,
            Func<Window<TKey, TOutput>, IAggregate<TOutput, TState, TOutput>> aggregateFunc)
        {
            var outputWindowSize = windowFunc(new BaseSignalWindowObservable<TInput>(source.WindowSize)).WindowSize;
            var aggregate = aggregateFunc(new Window<TKey, TOutput>(source.Properties.CloneToNewPayloadType<TOutput>()));

            var signal = new UniformSignalWindowArrayUnwindowStreamable<TKey, TInput, TState, TOutput>(
                source, outputWindowSize, windowFunc, aggregate);
            var properties = (UniformSignalProperties<TKey, TOutput>)signal.Properties;

            return new UniformSignal<TKey, TOutput>(signal, properties);
        }
    }

    /// <summary>
    /// Static class for providing extension methods on windowed signal observables
    /// </summary>
    public static class SignalWindowExtensions
    {
        /// <summary>
        /// Apply a window function to a window observable
        /// </summary>
        /// <param name="source">The input signal to which to apply the window function</param>
        /// <param name="window">The array representing a window</param>
        /// <returns>The transformed window observable</returns>
        public static ISignalWindowObservable<double> WindowFunction(this ISignalWindowObservable<double> source, double[] window)
        {
            var signalOperator = new ElementWiseProductDoubleOperator(window, true);
            return new SignalWindowOperatorPipe<double, double>(source, signalOperator);
        }

        /// <summary>
        /// Filters out all but the top K elements in a window
        /// </summary>
        /// <param name="source">The signal to be filtered</param>
        /// <param name="topK">The number of elements to retain</param>
        /// <returns>The filtered observable</returns>
        public static ISignalWindowObservable<double> FilterTopK(this ISignalWindowObservable<double> source, int topK)
        {
            var signalOperator = new SelectWindowOperator<double, double>((input, output) => input.TopK(topK, output), source.WindowSize, source.WindowSize);
            return new SignalWindowOperatorPipe<double, double>(source, signalOperator);
        }

        /// <summary>
        /// Filters out all but the top K elements in a window
        /// </summary>
        /// <param name="source">The signal to be filtered</param>
        /// <param name="topK">The number of elements to retain</param>
        /// <returns>The filtered observable</returns>
        public static ISignalWindowObservable<Complex> FilterTopK(this ISignalWindowObservable<Complex> source, int topK)
        {
            var signalOperator = new SelectWindowOperator<Complex, Complex>((input, output) => input.TopK(topK, output), source.WindowSize, source.WindowSize);
            return new SignalWindowOperatorPipe<Complex, Complex>(source, signalOperator);
        }

        /*public static ISignalWindowObservable<Complex> FFT(this ISignalWindowObservable<double> source)
        {
            var signalOperator = new FFTComplexOperator(source.WindowSize);
            return new SignalWindowOperatorPipe<double, Complex>(source, signalOperator);
        }*/

            /*
        /// <summary>
        ///
        /// </summary>
        /// <param name="source"></param>
        /// <returns></returns>
        public static ISignalWindowObservable<Complex> InverseFFT(this ISignalWindowObservable<Complex> source)
        {
            var signalOperator = new InverseFFTComplexOperator(source.WindowSize);
            return new SignalWindowOperatorPipe<Complex, Complex>(source, signalOperator);
        }*/

        /// <summary>
        /// Apply a discrete fourier transform to a signal observable
        /// </summary>
        /// <param name="source">The input signal to transform</param>
        /// <returns>A new signal stream, the result of the DFT</returns>
        public static ISignalWindowObservable<Complex> DFT(this ISignalWindowObservable<double> source)
        {
            var signalOperator = new DFTDoubleOperator(source.WindowSize);
            return new SignalWindowOperatorPipe<double, Complex>(source, signalOperator);
        }

        /// <summary>
        /// Apply a discrete fourier transform to a signal observable
        /// </summary>
        /// <param name="source">The input signal to transform</param>
        /// <returns>A new signal stream, the result of the DFT</returns>
        public static ISignalWindowObservable<Complex> DFT(this ISignalWindowObservable<Complex> source)
        {
            var signalOperator = new DFTComplexOperator(source.WindowSize);
            return new SignalWindowOperatorPipe<Complex, Complex>(source, signalOperator);
        }

        /// <summary>
        /// Apply an inverse discrete fourier transform to a signal observable
        /// </summary>
        /// <param name="source">The input signal to transform</param>
        /// <returns>A new signal stream, the result of the inverse DFT</returns>
        public static ISignalWindowObservable<Complex> InverseDFT(this ISignalWindowObservable<Complex> source)
        {
            var signalOperator = new InverseDFTComplexOperator(source.WindowSize);
            return new SignalWindowOperatorPipe<Complex, Complex>(source, signalOperator);
        }

        /// <summary>
        /// Apply an auto-correlation transformation to a signal
        /// </summary>
        /// <param name="source">The input signal to be transformed</param>
        /// <returns>The signal result of the auto-correlation</returns>
        public static ISignalWindowObservable<double> AutoCorrelation(this ISignalWindowObservable<double> source)
        {
            var signalOperator = new AutoCorrelationDoubleOperator(source.WindowSize);
            return new SignalWindowOperatorPipe<double, double>(source, signalOperator);
        }

        /// <summary>
        /// Apply a cross-correlation transformation to a signal
        /// </summary>
        /// <param name="source">The input signal to be transformed</param>
        /// <param name="weights">The weights to be applied during cross-correlation</param>
        /// <returns>The signal result of the cross-correlation</returns>
        public static ISignalWindowObservable<double> CrossCorrelation(this ISignalWindowObservable<double> source, double[] weights)
        {
            var signalOperator = new CorrelationDoubleOperator(source.WindowSize, weights);
            return new SignalWindowOperatorPipe<double, double>(source, signalOperator);
        }

        /// <summary>
        /// Convolve a signaled window according to the given weights
        /// </summary>
        /// <param name="source">The signal to run through convolution</param>
        /// <param name="weights">The weight array given to the convolution operation</param>
        /// <returns>The convolved signal</returns>
        public static ISignalWindowObservable<double> Convolution(this ISignalWindowObservable<double> source, double[] weights)
        {
            var signalOperator = new CorrelationDoubleOperator(source.WindowSize, weights.Reverse());
            return new SignalWindowOperatorPipe<double, double>(source, signalOperator);
        }
    }
}
