// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Microsoft.StreamProcessing.Internal.Collections
{
    /// <summary>
    /// A dictionary that supports concurrency with similar interface to .NET's ConcurrentDictionary.
    /// However, this dictionary changes the implementation and GetOrAdd functions to
    /// guarantee atomicity per-key for factory lambdas.
    /// </summary>
    /// <typeparam name="TValue">Type of values in the dictionary</typeparam>
    internal sealed class SafeConcurrentDictionary<TValue> : IEnumerable<KeyValuePair<CacheKey, TValue>>
    {
        private readonly ConcurrentDictionary<CacheKey, TValue> dictionary = new ConcurrentDictionary<CacheKey, TValue>();

        private readonly ConcurrentDictionary<CacheKey, object> keyLocks = new ConcurrentDictionary<CacheKey, object>();

        /// <summary>
        /// Adds a key/value pair to the dictionary if it does not exist.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TValue GetOrAdd(CacheKey key, Func<CacheKey, TValue> valueFactory)
        {
            if (this.dictionary.TryGetValue(key, out var value))
            {
                return value;
            }
            lock (GetLock(key))
            {
                return this.dictionary.GetOrAdd(key, valueFactory);
            }
        }

        /// <summary>
        /// Returns an enumerator of the elements in the dictionary.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<KeyValuePair<CacheKey, TValue>> GetEnumerator() => this.dictionary.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <summary>
        /// Retrieves lock associated with a key (creating it if it does not exist).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private object GetLock(CacheKey key) => this.keyLocks.GetOrAdd(key, v => new object());
    }
}
