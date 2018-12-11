// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;

namespace Microsoft.StreamProcessing
{
    internal static class StreamMessageEgress
    {
        /// <summary>
        /// Exports a streamable as a grouped observable of batches. Produces data within and between batches which is sync time ordered.
        /// </summary>
        internal static IObservable<StreamMessage<TUnit, TPayload>> ToStreamMessageObservable<TUnit, TPayload>(
            this IStreamable<TUnit, TPayload> stream)
        {
            Contract.Requires(stream != null);

            return RegisterOutputAsStreamMessages(null, stream, null);
        }

        internal static IObservable<StreamMessage<TUnit, TPayload>> RegisterOutputAsStreamMessages<TUnit, TPayload>(
            this QueryContainer container,
            IStreamable<TUnit, TPayload> stream,
            string identifier = null)
        {
            Contract.Requires(stream != null);

            return new StreamMessageEgressObservable<TUnit, TPayload>(stream, container, identifier ?? Guid.NewGuid().ToString());
        }
    }
}
