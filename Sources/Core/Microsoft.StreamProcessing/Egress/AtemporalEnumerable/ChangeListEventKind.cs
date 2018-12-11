// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Event kinds for insertions or deletions from a change list ingress or egress
    /// </summary>
    [DataContract]
    internal enum ChangeListEventKind
    {
        /// <summary>
        /// Insert event kind
        /// </summary>
        Insert,

        /// <summary>
        /// Delete event kind
        /// </summary>
        Delete,
    }
}
