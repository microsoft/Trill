// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
namespace Microsoft.StreamProcessing.Signal.UDO
{
    /// <summary>
    /// Observer analog for signal streams
    /// </summary>
    /// <typeparam name="T">Type of the underlying data/event payload</typeparam>
    public interface ISignalObserver<T>
    {
        /// <summary>
        /// Callback method for signal observables to call
        /// </summary>
        /// <param name="time">Time at which a result is available</param>
        /// <param name="result">The result that is now available</param>
        void OnNext(long time, ref T result);
    }

    /// <summary>
    /// Observable analog for signal streams
    /// </summary>
    /// <typeparam name="T">Type of the underlying data/event payload</typeparam>
    public interface ISignalObservable<T>
    {
        /// <summary>
        /// Registration method for signal observers to receive updates
        /// </summary>
        /// <param name="observer">The observer whose callback methods should be called on event progress</param>
        void Subscribe(ISignalObserver<T> observer);
    }

    /// <summary>
    /// Observer analog for windowed signal streams
    /// </summary>
    /// <typeparam name="T">Type of the underlying data/event payload</typeparam>
    public interface ISignalWindowObserver<T>
    {
        /// <summary>
        /// Callback method for signal observables to call on initialization
        /// </summary>
        /// <param name="time">Time at which a result is available</param>
        /// <param name="window">The window that is now available</param>
        void OnInit(long time, BaseWindow<T> window);

        /// <summary>
        /// Callback method for signal observables to call on hopping
        /// </summary>
        /// <param name="time">Time at which a result is available</param>
        /// <param name="window">The window that is now available</param>
        void OnHop(long time, BaseWindow<T> window);

        /// <summary>
        /// Provides a clone of the current execution chain link
        /// </summary>
        /// <param name="source">The observable to pass along the execution chain copy</param>
        /// <returns>A new instance of the observer</returns>
        object Clone(ISignalWindowObservable<T> source);
    }

    /// <summary>
    /// Observable analog for windowed signal streams
    /// </summary>
    /// <typeparam name="T">Type of the underlying data/event payload</typeparam>
    public interface ISignalWindowObservable<T>
    {
        /// <summary>
        /// Returns the window size for the windowed signal
        /// </summary>
        int WindowSize { get; }

        /// <summary>
        /// Registration method for windowed signal observers to receive updates
        /// </summary>
        /// <param name="observer">The observer whose callback methods should be called on event progress</param>
        void Subscribe(ISignalWindowObserver<T> observer);
    }

    internal sealed class BaseSignalWindowObservable<T> : ISignalWindowObservable<T>
    {
        public int WindowSize { get; }

        public ISignalWindowObserver<T> observer;

        public BaseSignalWindowObservable(int windowSize) => this.WindowSize = windowSize;

        public void Subscribe(ISignalWindowObserver<T> observer) => this.observer = observer;
    }
}