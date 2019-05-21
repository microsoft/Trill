// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing.Internal.Collections
{
    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// Fast dictionary implementation, sparse entries, no next pointers, bitvector pre-filtering, lean API
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public class FastDictionary<TKey, TValue>
    {
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public const int IteratorStart = -1;

        private const int DefaultSize = 10;

        private readonly Func<TKey, TKey, bool> comparerEquals;
        private readonly Func<TKey, int> comparerGetHashCode;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public Entry<TKey, TValue>[] entries;
        [DataMember]
        private int resizeThreshold;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int Count;
        [DataMember]
        private int Size;
        [DataMember]
        private byte[] bitvector;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public FastDictionary()
            : this(DefaultSize, EqualityComparerExpression<TKey>.DefaultEqualsFunction, EqualityComparerExpression<TKey>.DefaultGetHashCodeFunction)
        { }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="capacity"></param>
        /// <param name="equals"></param>
        /// <param name="getHashCode"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public FastDictionary(int capacity, Func<TKey, TKey, bool> equals, Func<TKey, int> getHashCode)
        {
            this.Size = HashHelpers.GetPrime(capacity);
            this.entries = new Entry<TKey, TValue>[this.Size];
            this.bitvector = new byte[1 + (this.Size >> 3)];
            this.comparerEquals = equals;
            this.comparerGetHashCode = getHashCode;
            this.resizeThreshold = this.Size >> 1;
            this.Count = 0;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool Lookup(TKey key, out int index)
        {
            int num = this.comparerGetHashCode(key) & 0x7fffffff;
            index = num % this.Size;

            do
            {
                if ((this.bitvector[index >> 3] & (0x1 << (index & 0x7))) == 0) return false;
                if (this.comparerEquals(key, this.entries[index].key)) return true;

                index++;
                if (index == this.Size) index = 0;
            }
            while (true);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="hashCode"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool Lookup(TKey key, int hashCode, out int index)
        {
            int num = hashCode & 0x7fffffff;
            index = num % this.Size;

            do
            {
                if ((this.bitvector[index >> 3] & (0x1 << (index & 0x7))) == 0) return false;
                if (this.comparerEquals(key, this.entries[index].key)) return true;

                index++;
                if (index == this.Size) index = 0;
            }
            while (true);
        }

        /// <summary>
        /// Insert takes an index as ref parameter because Insert may resize the dictionary, which would cause index to change.
        /// </summary>
        /// <param name="index"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Insert(ref int index, TKey key, TValue value)
        {
            this.entries[index].key = key;
            this.entries[index].value = value;
            this.bitvector[index >> 3] |= (byte)((0x1 << (index & 0x7)));
            this.Count++;
            if (this.Count > this.resizeThreshold)
            {
                Resize();
                /* resizing may make index obsolete, hence we compute the index */
                Lookup(key, out index);
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        /* User has to pass in the initial index value of IteratorStart
         * Iterate returns false when done, else returns true after setting the index to the
         * next iterated element
         */
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool Iterate(ref int index)
        {
            index++;
            while ((this.bitvector[index >> 3] & (0x1 << (index & 0x7))) == 0)
            {
                index++;
                if (index >= this.Size) return false;
            }
            return true;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Clear()
        {
            this.Count = 0;
            Array.Clear(this.bitvector, 0, 1 + (this.Size >> 3));
            if (Config.ClearColumnsOnReturn)
                Array.Clear(this.entries, 0, this.entries.Length);
        }

        private void Resize()
        {
            int newSize = HashHelpers.ExpandPrime(this.Size * 2);
            var newEntries = new Entry<TKey, TValue>[newSize];
            byte[] newBitvector = new byte[1 + (newSize >> 3)];

            int index = 0;
            int num, newindex;
            while (index < this.Size)
            {
                if ((this.bitvector[index >> 3] & (0x1 << (index & 0x7))) != 0)
                {
                    num = this.comparerGetHashCode(this.entries[index].key) & 0x7fffffff;
                    newindex = num % newSize;
                    while ((newBitvector[newindex >> 3] & (0x1 << (newindex & 0x7))) != 0)
                    {
                        newindex++;
                        if (newindex == newSize)
                            newindex = 0;
                    }
                    newEntries[newindex] = this.entries[index];
                    newBitvector[newindex >> 3] |= (byte)((0x1 << (newindex & 0x7)));
                }
                index++;
            }

            this.Size = newSize;
            this.entries = newEntries;
            this.bitvector = newBitvector;
            this.resizeThreshold = this.Size >> 1;
        }
    }
}