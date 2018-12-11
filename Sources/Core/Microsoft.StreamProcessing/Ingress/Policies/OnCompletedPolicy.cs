// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Specifies how to handle query completion (on receiving an OnCompleted call).
    /// </summary>
    public enum OnCompletedPolicy
    {
        /// <summary>
        /// Halt the query immediately - do not flush partial output or move time forward.
        /// </summary>
        None,

        /// <summary>
        /// Flush partial output (in batches within the system) before completion. Do not move time forward.
        /// </summary>
        Flush,

        /// <summary>
        /// This is the end of stream. Move time to infinity and flush all output before completion.
        /// </summary>
        EndOfStream
    }
}