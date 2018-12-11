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
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        public static IIngressStreamable<Empty, TPayload> ToAtemporalArrayStreamable<TPayload>(
            this IObservable<ArraySegment<TPayload>> data,
            TimelinePolicy timelinePolicy = null,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.None)
        {
            return data.ToCheckpointableStreamable(
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
        /// <param name="onCompletedPolicy">How to handle the completion of a stream.</param>
        /// <param name="container">A query containter to which this ingress point can be attached.</param>
        /// <param name="identifier">If provided, a unique name to identify to point of ingress in the query.</param>
        /// <returns>An IStreamable that can be used in queries.</returns>
        public static IIngressStreamable<Empty, TPayload> RegisterAtemporalArrayInput<TPayload>(
            this QueryContainer container,
            IObservable<ArraySegment<TPayload>> data,
            TimelinePolicy timelinePolicy = null,
            OnCompletedPolicy onCompletedPolicy = OnCompletedPolicy.None,
            string identifier = null)
        {
            return data.ToCheckpointableStreamable(
                onCompletedPolicy,
                timelinePolicy,
                container,
                identifier ?? Guid.NewGuid().ToString());
        }

        internal static IIngressStreamable<Empty, TPayload> ToCheckpointableStreamable<TPayload>(
            this IObservable<ArraySegment<TPayload>> streamEvents,
            OnCompletedPolicy completedPolicy,
            TimelinePolicy timelinePolicy,
            QueryContainer container,
            string identifier)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            // For the moment, there is no way to support array-based ingress with a timeline policy and a punctuation policy
            if (timelinePolicy.timelineEnum == TimelineEnum.WallClock && timelinePolicy.punctuationInterval == default(TimeSpan))
            {
                throw new NotSupportedException("There is currently no support for array-based ingress based on clock time with punctuations.  For the moment, either switch to a sequence-based policy or remove the punctuation period parameter.");
            }
            return new MonotonicArrayIngressStreamable<TPayload>(
                streamEvents,
                completedPolicy,
                timelinePolicy ?? TimelinePolicy.WallClock(),
                container,
                identifier);
        }
    }
}