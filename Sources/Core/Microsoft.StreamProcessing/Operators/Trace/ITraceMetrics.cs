// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Traces metrics seen by the trace operator
    /// </summary>
    public interface ITraceMetrics
    {
        /// <summary>
        /// Identifier for the trace operator
        /// </summary>
        string TraceId { get; }

        /// <summary>
        /// Total event count seen by the trace operator, excluding end edges.
        /// </summary>
        long EventCount { get; }

        /// <summary>
        /// Maximum sync time seen by the trace operator
        /// </summary>
        long MaxSyncTime { get; }

        /// <summary>
        /// Max punctuation time seen by the trace operator
        /// </summary>
        long MaxPunctuationTime { get; }
    }
}
