// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;

namespace Microsoft.StreamProcessing
{
    public static partial class Streamable
    {
        /// <summary>
        /// Registers a data source to be used as input with the query container.
        /// </summary>
        /// <typeparam name="TPayload">The type of the payload of the data source.</typeparam>
        /// <param name="data">A sequence of data elements created by the client.</param>
        /// <param name="timelinePolicy">Describes how to simulate a progression of time with the data.</param>
        /// <param name="flushPolicy">When to flush batched output events.</param>
        /// <param name="periodicPunctuationPolicy">Whether to add periodic punctuations to the resulting stream.</param>
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        public static IIngressStreamable<Empty, TPayload> ToAtemporalStreamable<TPayload>(
            this IObservable<TPayload> data,
            TimelinePolicy timelinePolicy = null,
            FlushPolicy flushPolicy = FlushPolicy.FlushOnPunctuation,
            PeriodicPunctuationPolicy periodicPunctuationPolicy = null,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.EndOfStream)
        {
            return data.ToCheckpointableStreamable(
                flushPolicy,
                periodicPunctuationPolicy,
                onCompletedPolicy,
                timelinePolicy,
                null,
                Guid.NewGuid().ToString());
        }

        /// <summary>
        /// Registers a data source to be used as input with the query container.
        /// </summary>
        /// <typeparam name="TPayload">The type of the payload of the data source.</typeparam>
        /// <param name="data">A sequence of data elements created by the client.</param>
        /// <param name="timelinePolicy">Describes how to simulate a progression of time with the data.</param>
        /// <param name="flushPolicy">When to flush batched output events.</param>
        /// <param name="periodicPunctuationPolicy">Whether to add periodic punctuations to the resulting stream.</param>
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <param name="container">A query containter to which this ingress point can be attached.</param>
        /// <param name="identifier">If provided, a unique name to identify to point of ingress in the query.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        public static IIngressStreamable<Empty, TPayload> RegisterAtemporalInput<TPayload>(
            this QueryContainer container,
            IObservable<TPayload> data,
            TimelinePolicy timelinePolicy = null,
            FlushPolicy flushPolicy = FlushPolicy.FlushOnPunctuation,
            PeriodicPunctuationPolicy periodicPunctuationPolicy = null,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.EndOfStream,
            string identifier = null)
        {
            return data.ToCheckpointableStreamable(
                flushPolicy,
                periodicPunctuationPolicy,
                onCompletedPolicy,
                timelinePolicy,
                container,
                identifier ?? Guid.NewGuid().ToString());
        }

        internal static IIngressStreamable<Empty, TPayload> ToCheckpointableStreamable<TPayload>(
            this IObservable<TPayload> streamEvents,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy periodicPunctuationPolicy,
            OnCompletedPolicy completedPolicy,
            TimelinePolicy timelinePolicy,
            QueryContainer container,
            string identifier)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            if (periodicPunctuationPolicy == null)
                periodicPunctuationPolicy = PeriodicPunctuationPolicy.None();

            return new MonotonicIngressStreamable<TPayload>(
                streamEvents,
                flushPolicy,
                periodicPunctuationPolicy,
                completedPolicy,
                timelinePolicy ?? TimelinePolicy.WallClock(),
                container,
                identifier);
        }
    }
}