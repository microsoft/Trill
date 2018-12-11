// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Event kinds for row-wise StreamEvents
    /// </summary>
    public enum StreamEventKind
    {
        /// <summary>
        /// Punctuation event kind
        /// </summary>
        Punctuation,

        /// <summary>
        /// Start edge event kind
        /// </summary>
        Start,

        /// <summary>
        /// End edge event kind
        /// </summary>
        End,

        /// <summary>
        /// Interval event kind
        /// </summary>
        Interval,

        /// <summary>
        /// Low watermark event kind
        /// </summary>
        LowWatermark,
    }
}