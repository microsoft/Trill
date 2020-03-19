// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Represents key value for a nested group apply branch.
    /// </summary>
    /// <typeparam name="TOuterKey">Key type for outer branch. Where there is no containing
    /// branch, this is Empty.</typeparam>
    /// <typeparam name="TInnerKey">Key type for nested branch.</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct CompoundGroupKey<TOuterKey, TInnerKey>
    {
        /// <summary>
        /// The value of the inner grouping key.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public TInnerKey innerGroup;

        /// <summary>
        /// The value of the outer grouping key.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public TOuterKey outerGroup;

        /// <summary>
        /// A hash code incorporating both key elements.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int hashCode;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public TInnerKey InnerGroup => this.innerGroup;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public TOuterKey OuterGroup => this.outerGroup;

        /// <summary>
        /// Provides a string representation of the compound grouping key.
        /// </summary>
        /// <returns>A string representation of the compound grouping key.</returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override string ToString() => new { this.OuterGroup, this.InnerGroup }.ToString();

        /// <summary>
        /// Provides a hashcode of the compound grouping key.
        /// </summary>
        /// <returns>A hashcode of the compound grouping key.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override int GetHashCode() => this.hashCode;
    }

    /// <summary>
    /// The type that is used to hold the key introduced by a grouping operator.
    /// </summary>
    /// <typeparam name="T">The type of the key.</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct GroupSelectorInput<T>
    {
        /// <summary>
        /// The key value for the grouping
        /// </summary>
        [DataMember]
        public T Key;

        /// <summary>
        /// Create a new group selector
        /// </summary>
        /// <param name="key">The value of the key associated with the group</param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public GroupSelectorInput(T key) => this.Key = key;
    }

    /// <summary>
    /// The type that is used to hold the key introduced by a partitioned input.
    /// </summary>
    /// <typeparam name="T">The type of the key.</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct PartitionKey<T>
    {
        /// <summary>
        /// The value associated with the given partition.
        /// </summary>
        [DataMember]
        public T Key;

        /// <summary>
        /// Used to construct a new partition key.
        /// </summary>
        /// <param name="key"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public PartitionKey(T key) => this.Key = key;

        /// <summary>
        /// Provides a hash code for the given partition key.
        /// </summary>
        /// <returns>A hash code for the given partition key.</returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override int GetHashCode() => this.Key.GetHashCode();
    }

    /// <summary>
    /// An event, coupled with a relative ranking within a group
    /// </summary>
    /// <typeparam name="T">The type of the event</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct RankedEvent<T>
    {
        [DataMember]
        private int rank;
        [DataMember]
        private T payload;

        /// <summary>
        /// The rank of the event within the current grouping
        /// </summary>
        public int Rank => this.rank;

        /// <summary>
        /// The actual event associated with the ranking
        /// </summary>
        public T Payload => this.payload;

        internal RankedEvent(int rank, T payload)
        {
            this.rank = rank;
            this.payload = payload;
        }

        /// <summary>
        /// Provides a string representation of the ranked event.
        /// </summary>
        /// <returns>A string representation of the ranked event.</returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override string ToString() => $"[Rank={this.rank}, Payload={this.payload}]";
    }
}