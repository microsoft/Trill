// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing.Internal
{
    /// <summary>
    /// The value representation with its timestamp.
    /// </summary>
    /// <typeparam name="T">The type of the underlying elements being aggregated.</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct ValueAndTimestamp<T>
    {
        /// <summary>
        /// The timestamp of the payload.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public long timestamp;

        /// <summary>
        /// Payload of the event
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T value;
    }

    /// <summary>
    /// The state object used in minimum and maximum aggregates.
    /// </summary>
    /// <typeparam name="T">The type of the underlying elements being aggregated.</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct MinMaxState<T>
    {
        /// <summary>
        /// A sorted multiset of all values currently in state.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public SortedMultiSet<T> savedValues;

        /// <summary>
        /// List of values and its timestamp
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public ElasticCircularBuffer<ValueAndTimestamp<T>> values;

        /// <summary>
        /// The current value if the aggregate were to be computed immediately.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T currentValue;

        /// <summary>
        /// The timestamp of the last operation on this state object.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public long currentTimestamp;
    }
}
