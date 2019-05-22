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
    /// Fast dictionary implementation, sparse entries, no next pointers, bitvector pre-filtering, lean API.
    /// Supports remove operation
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public class FastDictionary2<TKey, TValue>
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
        public Entry2<TKey, TValue>[] entries;
        [DataMember]
        private int count;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int Size;
        [DataMember]
        private int[] buckets;
        [DataMember]
        private int freeCount;
        [DataMember]
        private int freeList;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int Count => this.count - this.freeCount;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public FastDictionary2()
            : this(DefaultSize, EqualityComparerExpression<TKey>.DefaultEqualsFunction, EqualityComparerExpression<TKey>.DefaultGetHashCodeFunction)
        { }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="capacity"></param>
        /// <param name="equals"></param>
        /// <param name="getHashCode"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public FastDictionary2(int capacity, Func<TKey, TKey, bool> equals, Func<TKey, int> getHashCode)
        {
            this.Size = HashHelpers.GetPrime(capacity);
            this.comparerEquals = equals;
            this.comparerGetHashCode = getHashCode;

            Initialize();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Initialize()
        {
            this.entries = new Entry2<TKey, TValue>[this.Size];
            this.buckets = new int[this.Size];
            for (int i = 0; i < this.Size; i++) this.buckets[i] = -1;
            this.freeList = -1;
            this.count = 0;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Clear()
        {
            if (Config.ClearColumnsOnReturn)
            {
                Array.Clear(this.entries, 0, this.entries.Length);
                Array.Clear(this.buckets, 0, this.buckets.Length);
            }
            else
            {
                Array.Clear(this.entries, 0, this.count);
                Array.Clear(this.buckets, 0, this.count);
            }

            this.count = 0;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int Insert(TKey key, TValue value)
        {
            int freeList;
            int num = this.comparerGetHashCode(key) & 0x7fffffff;
            int index = num % this.Size;
            if (this.freeCount > 0)
            {
                freeList = this.freeList;
                this.freeList = this.entries[freeList].next;
                this.freeCount--;
            }
            else
            {
                if (this.count == this.Size)
                {
                    Resize();
                    index = num % this.Size;
                }
                freeList = this.count;
                this.count++;
            }

            this.entries[freeList].hashCode = num;
            this.entries[freeList].next = this.buckets[index];
            this.entries[freeList].key = key;
            this.entries[freeList].value = value;
            this.buckets[index] = freeList;
            if (this.Count > (this.Size >> 1)) Resize();
            return freeList;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="hashCode"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int Insert(TKey key, TValue value, int hashCode)
        {
            int freeList;
            int num = hashCode & 0x7fffffff;
            int index = num % this.Size;
            if (this.freeCount > 0)
            {
                freeList = this.freeList;
                this.freeList = this.entries[freeList].next;
                this.freeCount--;
            }
            else
            {
                if (this.count == this.Size)
                {
                    Resize();
                    index = num % this.Size;
                }
                freeList = this.count;
                this.count++;
            }

            this.entries[freeList].hashCode = num;
            this.entries[freeList].next = this.buckets[index];
            this.entries[freeList].key = key;
            this.entries[freeList].value = value;
            this.buckets[index] = freeList;
            if (this.Count > (this.Size >> 1)) Resize();
            return freeList;
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
            for (index = this.buckets[num % this.Size]; index >= 0; index = this.entries[index].next)
            {
                if ((this.entries[index].hashCode == num) && this.comparerEquals(this.entries[index].key, key))
                {
                    return true;
                }
            }
            return false;
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
            for (index = this.buckets[num % this.Size]; index >= 0; index = this.entries[index].next)
            {
                if ((this.entries[index].hashCode == num) && this.comparerEquals(this.entries[index].key, key))
                {
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool Remove(TKey key)
        {
            int num = this.comparerGetHashCode(key) & 0x7fffffff;
            int index = num % this.Size;
            int num3 = -1;
            for (int i = this.buckets[index]; i >= 0; i = this.entries[i].next)
            {
                if ((this.entries[i].hashCode == num) && this.comparerEquals(this.entries[i].key, key))
                {
                    if (num3 < 0)
                    {
                        this.buckets[index] = this.entries[i].next;
                    }
                    else
                    {
                        this.entries[num3].next = this.entries[i].next;
                    }

                    this.entries[i].hashCode = -1;
                    this.entries[i].next = this.freeList;
                    if (Config.ClearColumnsOnReturn) this.entries[i].value = default;
                    this.freeList = i;
                    this.freeCount++;
                    return true;
                }
                num3 = i;
            }
            return false;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="hashCode"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool Remove(TKey key, int hashCode)
        {
            int num = hashCode & 0x7fffffff;
            int index = num % this.Size;
            int num3 = -1;
            for (int i = this.buckets[index]; i >= 0; i = this.entries[i].next)
            {
                if ((this.entries[i].hashCode == num) && this.comparerEquals(this.entries[i].key, key))
                {
                    if (num3 < 0)
                    {
                        this.buckets[index] = this.entries[i].next;
                    }
                    else
                    {
                        this.entries[num3].next = this.entries[i].next;
                    }

                    this.entries[i].hashCode = -1;
                    this.entries[i].next = this.freeList;
                    if (Config.ClearColumnsOnReturn) this.entries[i].value = default;
                    this.freeList = i;
                    this.freeCount++;
                    return true;
                }
                num3 = i;
            }
            return false;
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
            if (index >= this.count)
                return false;
            while (this.entries[index].hashCode < 0)
            {
                index++;
                if (index >= this.count)
                    return false;
            }
            return true;
        }

        private void Resize()
        {
            int newSize = HashHelpers.ExpandPrime(this.Size * 2);

            int[] numArray = new int[newSize];
            for (int i = 0; i < numArray.Length; i++)
            {
                numArray[i] = -1;
            }
            var destinationArray = new Entry2<TKey, TValue>[newSize];
            Array.Copy(this.entries, 0, destinationArray, 0, this.count);
            for (int j = 0; j < this.count; j++)
            {
                int hash = destinationArray[j].hashCode;
                if (hash >= 0)
                {
                    int index = hash % newSize;
                    destinationArray[j].next = numArray[index];
                    numArray[index] = j;
                }
            }

            this.buckets = numArray;
            this.entries = destinationArray;
            this.Size = newSize;
        }
    }
}
