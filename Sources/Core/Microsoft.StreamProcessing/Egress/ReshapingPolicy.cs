// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Policy that specifies whether and how events are reshaped at egress
    /// </summary>
    public enum ReshapingPolicy
    {
        /// <summary>
        /// No action taken to reshape output (this is the default)
        /// </summary>
        None,

        /// <summary>
        /// Coalesce end edges with their starts into interval events (WARNING: may cause delay of output until the end of stream)
        /// </summary>
        CoalesceEndEdges
    }
}
