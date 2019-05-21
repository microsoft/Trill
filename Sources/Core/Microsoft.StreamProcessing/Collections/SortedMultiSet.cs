// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Represents an ordered mathematical bag, where elements can be added more than once and returned in sort order.
    /// </summary>
    /// <typeparam name="T">The type of the underlying elements.</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public sealed class SortedMultiSet<T>
    {
        private static readonly Func<SortedDictionary<T, long>, IEnumerable<KeyValuePair<T, long>>> reverseEnumerable = CreateReverseMethod();

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Used to avoid creating redundant readonly property.")]
        [DataMember]
        private SortedDictionary<T, long> Elements;
        [DataMember]
        private long count;

        private static Func<SortedDictionary<T, long>, IEnumerable<KeyValuePair<T, long>>> CreateReverseMethod()
        {
            var type = typeof(SortedDictionary<T, long>);
            var parameter = Expression.Parameter(type);

            var field = type.GetTypeInfo().GetField("_set", BindingFlags.NonPublic | BindingFlags.Instance);
            var set = Expression.Field(parameter, field);
            var member = set.Type.GetTypeInfo().GetMethod("Reverse");
            var reverse = Expression.Call(set, member);

            return Expression.Lambda<Func<SortedDictionary<T, long>, IEnumerable<KeyValuePair<T, long>>>>(reverse, parameter).Compile();
        }

        /// <summary>
        /// Creates a new instance of a Sorted Multiset.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public SortedMultiSet() : this(() => new SortedDictionary<T, long>()) { }

        /// <summary>
        /// Creates a new instance of a Sorted Multiset where the underlying dictionary is generated.
        /// </summary>
        /// <param name="generator">The generator function for creating the underlying storage of the object.</param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public SortedMultiSet(Func<SortedDictionary<T, long>> generator) => this.Elements = generator();

        /// <summary>
        /// States whether the given instance of a Sorted Multiset is empty.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool IsEmpty
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => this.count == 0;
        }

        /// <summary>
        /// Returns the number of elements contained in the Sorted Multiset, including multiplicity.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public long TotalCount
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => this.count;
        }

        /// <summary>
        /// Returns the number of unique elements contained in the Sorted Multiset.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public long UniqueCount
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => this.Elements.Count;
        }

        /// <summary>
        /// Returns the element count of the given lookup key.
        /// </summary>
        /// <param name="key">The element to search for.</param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public long this[T key]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                this.Elements.TryGetValue(key, out long keyCount);
                return keyCount;
            }
        }

        /// <summary>
        /// Determines whether the Sorted Multiset contains at least one instance of the given element.
        /// </summary>
        /// <param name="key">The element to search for.</param>
        /// <returns>Whether the element was found at least once in the Sorted Multiset.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool Contains(T key) => this.Elements.ContainsKey(key);

        /// <summary>
        /// Add the given element once to the current Sorted Multiset.
        /// </summary>
        /// <param name="key">The element that should be added to the current object.</param>
        /// <returns>A reference to the current Sorted Multiset to allow for functional composition.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public SortedMultiSet<T> Add(T key)
        {
            this.Elements.TryGetValue(key, out long keyCount);
            keyCount++;
            this.Elements[key] = keyCount;
            this.count++;
            return this;
        }

        /// <summary>
        /// Add the given element to the current Sorted Multiset the given number of times.
        /// </summary>
        /// <param name="key">The element to add to the Sorted Multiset.</param>
        /// <param name="increment">The number of times to add the element.</param>
        /// <returns>A reference to the current Sorted Multiset to allow for functional composition.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public SortedMultiSet<T> Add(T key, long increment)
        {
            Contract.Requires(increment >= 1);
            this.Elements.TryGetValue(key, out long keyCount);
            keyCount += increment;
            this.Elements[key] = keyCount;
            this.count += increment;
            return this;
        }

        /// <summary>
        /// Add all of the elements in the given set to the current Sorted Multiset, inluding multiplicity.
        /// </summary>
        /// <param name="set">The Sorted Multiset containing the elements that should be added to the current object.</param>
        /// <returns>A reference to the current Sorted Multiset to allow for functional composition.</returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public SortedMultiSet<T> AddAll(SortedMultiSet<T> set)
        {
            Contract.Requires(set != null);
            foreach (var keyAndCount in set.Elements)
                Add(keyAndCount.Key, keyAndCount.Value);

            return this;
        }

        /// <summary>
        /// Remove the given element once from the current Sorted Multiset.
        /// </summary>
        /// <param name="key">The element that should be removed from the current object.</param>
        /// <returns>A reference to the current Sorted Multiset to allow for functional composition.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public SortedMultiSet<T> Remove(T key)
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
        /// Remove the given element from the current Sorted Multiset the given number of times.
        /// </summary>
        /// <param name="key">The element to remove from the Sorted Multiset.</param>
        /// <param name="decrement">The number of times to remove the element.</param>
        /// <returns>A reference to the current Sorted Multiset to allow for functional composition.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public SortedMultiSet<T> Remove(T key, long decrement)
        {
            Contract.Requires(decrement >= 1);
            this.Elements.TryGetValue(key, out long keyCount);
            keyCount -= decrement;
            if (keyCount > 0)
                this.Elements[key] = keyCount;
            else if (keyCount == 0)
                this.Elements.Remove(key);
            else
                throw new ArgumentException("Attempting to remove element not in set.");

            this.count -= decrement;
            return this;
        }

        /// <summary>
        /// Remove all of the elements in the given set from the current Sorted Multiset, inluding multiplicity.
        /// </summary>
        /// <param name="set">The Sorted Multiset containing the elements that should be removed from the current object.</param>
        /// <returns>A reference to the current Sorted Multiset to allow for functional composition.</returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public SortedMultiSet<T> RemoveAll(SortedMultiSet<T> set)
        {
            Contract.Requires(set != null);
            foreach (var keyAndCount in set.Elements)
                Remove(keyAndCount.Key, keyAndCount.Value);

            return this;
        }

        /// <summary>
        /// Clear the contents of the Sorted Multiset.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Clear()
        {
            this.Elements.Clear();
            this.count = 0;
        }

        /// <summary>
        /// Gets the first element in the Sorted Multiset.
        /// </summary>
        /// <returns>The first element in the Sorted Multiset.</returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T First() => this.Elements.First().Key;

        /// <summary>
        /// Gets the last element in the Sorted Multiset.
        /// </summary>
        /// <returns>The last element in the Sorted Multiset.</returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T Last() => reverseEnumerable(this.Elements).First().Key;

        internal T SingleOrDefault()
        {
            if (this.Elements.Count != 1) return default;
            var element = this.Elements.Single();
            return element.Value != 1 ? (default) : element.Key;
        }

        /// <summary>
        /// Get all elements, including multiplicity, of the Sorted Multiset in sort order.
        /// </summary>
        /// <returns>An object that enumerates all of the elements in the Sorted Multiset, including multiplicity.</returns>
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
