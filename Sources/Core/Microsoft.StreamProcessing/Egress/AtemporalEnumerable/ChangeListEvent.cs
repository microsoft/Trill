// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Represents a single insertion or deletion of a value from a logical data set
    /// </summary>
    /// <typeparam name="TPayload">The type of the data being inserted or deleted</typeparam>
    [DataContract]
    internal struct ChangeListEvent<TPayload>
    {
        /// <summary>
        /// Flag stating whether the event is a value insertion or deletion
        /// </summary>
        [DataMember]
        public ChangeListEventKind EventKind;

        /// <summary>
        /// Payload of the event
        /// </summary>
        [DataMember]
        public TPayload Payload;
    }
}