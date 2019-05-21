// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.ComponentModel;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing.Internal
{
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
