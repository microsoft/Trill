// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.ComponentModel;
using System.IO;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public interface IQueryObject
    {
        /// <summary>
        /// Property to report how many events are currently buffered in the output batch.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        int CurrentlyBufferedOutputCount { get; }

        /// <summary>
        /// Property to report how many events are currently buffered from inputs and are awaiting computation.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        int CurrentlyBufferedInputCount { get; }

        /// <summary>
        /// Method to call to trigger the production of a query plan from the set of running operations.
        /// </summary>
        /// <param name="previous">The previous node in the query plan.</param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        void ProduceQueryPlan(PlanNode previous);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        void OnFlush();

        /// <summary>
        /// Method to call when checkpointing the state of this observer.
        /// </summary>
        /// <param name="stream">The stream to which to write the checkpoint state.</param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        void Checkpoint(Stream stream);

        /// <summary>
        /// Method to call when restoring the state of this observer.
        /// </summary>
        /// <param name="stream">The stream from which to retrieve the restoration state.</param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        void Restore(Stream stream);

        /// <summary>
        /// In case of an exception, use this method to reset any internal state that needs to be reset.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        void Reset();
    }

    internal interface IIngressStreamObserver : IQueryObject, IDisposable
    {
        int CurrentlyBufferedReorderCount { get; }

        int CurrentlyBufferedStartEdgeCount { get; }

        string IngressSiteIdentifier { get; }

        IDisposable DelayedDisposable { get; }

        void Enable();
    }

    internal interface IEgressStreamObserver : IQueryObject
    {
        void AttachProcess(string identifier, Process p);
    }

    /// <summary>
    /// Represents an observer with two inputs, each of which may have state that is accumulating while waiting for the other input.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public interface IBinaryObserver : IQueryObject
    {
        /// <summary>
        /// Returns whether the left input has any associated built-up state.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        bool LeftInputHasState { get; }

        /// <summary>
        /// Returns whether the right input has any associated built-up state.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        bool RightInputHasState { get; }

        /// <summary>
        /// Property to report how many keys have events currently buffered from the left input and are awaiting computation.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        int CurrentlyBufferedLeftKeyCount { get; }

        /// <summary>
        /// Property to report how many keys have events currently buffered from the right input and are awaiting computation.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        int CurrentlyBufferedRightKeyCount { get; }

        /// <summary>
        /// Property to report how many events are currently buffered from the left input and are awaiting computation.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        int CurrentlyBufferedLeftInputCount { get; }

        /// <summary>
        /// Property to report how many events are currently buffered from the right input and are awaiting computation.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        int CurrentlyBufferedRightInputCount { get; }
    }

    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public interface IStreamObserver<TKey, TPayload> : IObserver<StreamMessage<TKey, TPayload>>, IQueryObject
    {
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        Guid ClassId { get; }
    }

    internal interface IBinaryObserver<TKey, TLeft, TRight, TPayload> : IBinaryObserver
    {
        IStreamObserver<TKey, TLeft> Left { get; }
        IStreamObserver<TKey, TRight> Right { get; }
    }

    internal interface IBothStreamObserverAndStreamObservable<TKey, TPayload> : IStreamObserver<TKey, TPayload>
    {
        void AddObserver(IStreamObserver<TKey, TPayload> observer);
    }

    internal interface IStreamObserverAndGroupedStreamObservable<TOuterKey, TSource, TInnerKey> : IStreamObserver<TOuterKey, TSource>
    {
        void AddObserver(IStreamObserver<TInnerKey, TSource> observer);
    }

    internal interface IStreamObserverAndSameKeyGroupedStreamObservable<TOuterKey, TSource, TInnerKey> : IStreamObserver<TOuterKey, TSource>
    {
        void AddObserver(IStreamObserver<TInnerKey, TSource> observer);
    }

    internal interface IStreamObserverAndNestedGroupedStreamObservable<TOuterKey, TSource, TInnerKey> : IStreamObserver<TOuterKey, TSource>
    {
        void AddObserver(IStreamObserver<CompoundGroupKey<TOuterKey, TInnerKey>, TSource> observer);
    }
}
