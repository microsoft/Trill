// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing.Internal.Collections
{
    /// <summary>
    /// Represents a mathematical "bag", where individual items can be stored more than once but order-independent.
    /// </summary>
    /// <typeparam name="T">The type of the underlying item collection</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public sealed class MultiSet<T>
    {
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Used to avoid creating redundant readonly property.")]
        [DataMember]
        private Dictionary<T, long> Elements = new Dictionary<T, long>();
        [DataMember]
        private long count;

        /// <summary>
        /// Creates a new instance of a multiset.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public MultiSet() { }

        /// <summary>
        /// Declares whether the multiset is empty
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool IsEmpty
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => this.count == 0;
        }

        /// <summary>
        /// Adds an item to the multiset
        /// </summary>
        /// <param name="key">The item to add to the multiset</param>
        /// <returns>A pointer to the current multiset to allow for functional composition</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public MultiSet<T> Add(T key)
        {
            this.Elements.TryGetValue(key, out long keyCount);
            keyCount++;
            this.Elements[key] = keyCount;
            this.count++;
            return this;
        }

        /// <summary>
        /// Removes an item from the multiset
        /// </summary>
        /// <param name="key">The item to remove from the multiset</param>
        /// <returns>A pointer to the current multiset to allow for functional composition</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public MultiSet<T> Remove(T key)
        {
            this.Elements.TryGetValue(key, out long keyCount);
            keyCount--;
            if (keyCount > 0)
                this.Elements[key] = keyCount;
            else if (keyCount == 0)
                this.Elements.Remove(key);
            else
                throw new ArgumentException("Attempting to remove element not in set.");

            this.count--;
            return this;
        }

        /// <summary>
        /// Returns the number of elements (not distinct elements) in the multiset
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public long Count => this.count;

        /// <summary>
        /// Returns an enumerable over the multiset objects
        /// </summary>
        /// <returns>An enumerable that enumerates over the values in the multiset</returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public IEnumerable<T> GetEnumerable()
        {
            foreach (var keyAndCount in this.Elements)
            {
                long keyCount = keyAndCount.Value;
                for (long i = 0; i < keyCount; i++)
                    yield return keyAndCount.Key;
            }
        }
    }
}
