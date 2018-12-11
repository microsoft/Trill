// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Specifies when to flush batched output events.
    /// </summary>
    [DataContract]
    public enum FlushPolicy
    {
        /// <summary>
        /// Do not flush. Output events will be batched and egressed normally.
        /// </summary>
        /// <returns>An instance of the flush policy</returns>
        None,

        /// <summary>
        /// When a punctuation is ingressed or generated, a flush will also be propagated through the query.
        /// This is the default policy.
        /// </summary>
        FlushOnPunctuation,

        /// <summary>
        /// When a batch is filled on ingress, a flush will be propagated through the query.
        /// </summary>
        FlushOnBatchBoundary,
    }

    /// <summary>
    /// Specifies when to flush batched output events.
    /// </summary>
    [DataContract]
    public enum PartitionedFlushPolicy
    {
        /// <summary>
        /// Do not flush. Output events will be batched and egressed normally.
        /// </summary>
        /// <returns>An instance of the flush policy</returns>
        None,

        /// <summary>
        /// When a low watermark is ingressed, a flush will also be propagated through the query.
        /// This is the default policy.
        /// </summary>
        FlushOnLowWatermark,

        /// <summary>
        /// When a batch is filled on ingress, a flush will be propagated through the query.
        /// </summary>
        FlushOnBatchBoundary,
    }
}