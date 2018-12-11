// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Primary interface for streamable operations - users should not be creating new classes that implement this interface without direct communication with the development team
    /// </summary>
    /// <typeparam name="TKey">Grouping key type for the streaming data</typeparam>
    /// <typeparam name="TPayload">Event payload type for the streaming data</typeparam>
    public interface IStreamable<TKey, TPayload>
    {
        /// <summary>
        /// Immediately starts sending events from the stream to the observer.
        /// </summary>
        /// <param name="observer">The observer to which events are sent.</param>
        /// <returns>An object that can be used to cancel the subscription or otherwise
        /// notify the stream that the caller is no longer interested in receiving any
        /// more events.</returns>
        IDisposable Subscribe(IStreamObserver<TKey, TPayload> observer);

        /// <summary>
        /// Returns the current properties of the stream, such as whether the stream
        /// is a constant-duration stream.
        /// </summary>
        StreamProperties<TKey, TPayload> Properties { get; }

        /// <summary>
        /// Returns any errors that were encountered during code generation.
        /// </summary>
        string ErrorMessages { get; }
    }
}
