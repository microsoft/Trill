// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// An interface corresponding to a streamable at an ingress point to a query.
    /// </summary>
    /// <typeparam name="TKey">The type of the key in the stream.</typeparam>
    /// <typeparam name="TPayload">The type of the payload objects of the input source.</typeparam>
    public interface IIngressStreamable<TKey, TPayload> : IStreamable<TKey, TPayload>
    {
        /// <summary>
        /// An identifier that uniquely identifies the ingress site relative to a running query.
        /// </summary>
        string IngressSiteIdentifier { get; }
    }

    /// <summary>
    /// Interface for a passive ingress site that can be externally triggered to read available data.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TPayload"></typeparam>
    public interface IPassiveIngressStreamable<TKey, TPayload> : IIngressStreamable<TKey, TPayload>
    {
        /// <summary>
        /// Trigger the stream to read (pull) data and push to the downstream pipeline
        /// </summary>
        /// <returns>False if reached end of stream (completed), true otherwise</returns>
        bool Trigger();
    }

    /// <summary>
    /// An interface corresponding to a streamable at an ingress point to a query where input may be out of order.
    /// </summary>
    /// <typeparam name="TPayload">The type of the payload objects of the input source.</typeparam>
    public interface IObservableIngressStreamable<TPayload> : IIngressStreamable<Empty, TPayload>
    {
        /// <summary>
        /// Returns a diagnostic observable of stream events that had to be dropped or adjusted due to out-of-order arrival.
        /// </summary>
        /// <returns>An observable that one can subscribe to for the stream events that arrive out of order.</returns>
        IObservable<OutOfOrderStreamEvent<TPayload>> GetDroppedAdjustedEventsDiagnostic();
    }

    /// <summary>
    /// An interface corresponding to a streamable at an ingress point to a query where input may be out of order.
    /// </summary>
    /// <typeparam name="TPartitionKey">The type of the partition key of the stream.</typeparam>
    /// <typeparam name="TPayload">The type of the payload objects of the input source.</typeparam>
    public interface IPartitionedIngressStreamable<TPartitionKey, TPayload> : IIngressStreamable<PartitionKey<TPartitionKey>, TPayload>
    {
        /// <summary>
        /// Returns a diagnostic observable of stream events that had to be dropped or adjusted due to out-of-order arrival.
        /// </summary>
        /// <returns>An observable that one can subscribe to for the stream events that arrive out of order.</returns>
        IObservable<OutOfOrderPartitionedStreamEvent<TPartitionKey, TPayload>> GetDroppedAdjustedEventsDiagnostic();
    }
}
