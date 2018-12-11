// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Streamable extension methods.
    /// </summary>
    public static partial class Streamable
    {
        /// <summary>
        /// Exports a streamable as an observable of change list events. Produces events that represent either insertions or deletions.
        /// </summary>
        /// <typeparam name="TPayload">The type of object being streamed</typeparam>
        /// <param name="stream">An IStreamable object that is intended to be an output to the query.</param>
        /// <returns>An IObservable object of events for output data from the query.</returns>
        public static IObservable<TPayload> ToAtemporalObservable<TPayload>(
            this IStreamable<Empty, TPayload> stream)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToAtemporalObservable(null, Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Registers an IStreamable object as an output of a query, with output as a list of change events.
        /// </summary>
        /// <typeparam name="TPayload">The type of object being streamed</typeparam>
        /// <param name="container">The query container to which an egress point is being added.</param>
        /// <param name="identifier">A string that can uniquely identify the point of egress in the query.</param>
        /// <param name="stream">An IStreamable object that is intended to be an output to the query.</param>
        /// <returns>An IObservable object of events for output data from the query.</returns>
        public static IObservable<TPayload> RegisterAtemporalOutput<TPayload>(
            this QueryContainer container,
            IStreamable<Empty, TPayload> stream,
            string identifier = null)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToAtemporalObservable(container, identifier ?? Guid.NewGuid().ToString());
        }

        internal static IObservable<TPayload> ToAtemporalObservable<TPayload>(
            this IStreamable<Empty, TPayload> stream,
            QueryContainer container,
            string identifier)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return new MonotonicObservable<TPayload>(stream, container, identifier);
        }
    }
}