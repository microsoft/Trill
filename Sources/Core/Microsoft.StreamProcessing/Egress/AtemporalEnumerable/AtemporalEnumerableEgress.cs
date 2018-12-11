// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace Microsoft.StreamProcessing
{
    public static partial class Streamable
    {
        /// <summary>
        /// Exports a streamable as an enumerable that can be queried at any time for its current state.
        /// </summary>
        /// <typeparam name="TPayload">The type of object being streamed and output</typeparam>
        /// <param name="stream">The stream to convert into an enumerable</param>
        /// <returns></returns>
        public static EvolvingStateEnumerable<TPayload> ToEnumerable<TPayload>(
            this IStreamable<Empty, TPayload> stream)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToEnumerable(null, Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Registers an IStreamable object as an output of a query, with output as a progressively changing enumerable.
        /// </summary>
        /// <typeparam name="TPayload">The type of the payload of the data source.</typeparam>
        /// <param name="container">A query container to which this egress point can be attached.</param>
        /// <param name="stream">An IStreamable object that is intended to be an output to the query.</param>
        /// <param name="identifier">A string that can uniquely identify the point of egress in the query.</param>
        /// <returns>An IObservable object of change list events for output data from the query.</returns>
        public static EvolvingStateEnumerable<TPayload> RegisterOutputAsEnumerable<TPayload>(this QueryContainer container, IStreamable<Empty, TPayload> stream, string identifier = null)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return stream.ToEnumerable(container, identifier ?? Guid.NewGuid().ToString());
        }

        internal static EvolvingStateEnumerable<TPayload> ToEnumerable<TPayload>(
            this IStreamable<Empty, TPayload> stream,
            QueryContainer container,
            string identifier)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return new EvolvingStateEnumerable<TPayload>(stream, container, identifier);
        }

    }
}
