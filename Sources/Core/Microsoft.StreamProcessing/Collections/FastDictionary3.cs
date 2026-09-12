// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Buffers;
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
    public class FastDictionary3<TKey, TValue> : IDisposable
    {
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public const int IteratorStart = -1;

        private const int DefaultSize = 10;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification="Lambdas are immutable")]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public readonly Func<TKey, TKey, bool> comparerEquals;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public Entry3<TKey, TValue>[] entries;
        [DataMember]
        private int resizeThreshold;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int Count;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int Size;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public byte[] bitvector;
        [DataMember]
        private byte[] dirtyBitvector;

        [IgnoreDataMember]
        private bool bitvectorFromPool;

        [IgnoreDataMember]
        private bool dirtyBitvectorFromPool;

        [IgnoreDataMember]
        private bool disposed;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public FastDictionary3()
            : this(DefaultSize, EqualityComparerExpression<TKey>.DefaultEqualsFunction, EqualityComparerExpression<TKey>.DefaultGetHashCodeFunction)
        { }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="capacity"></param>
        /// <param name="equals"></param>
        /// <param name="getHashCode"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public FastDictionary3(int capacity, Func<TKey, TKey, bool> equals, Func<TKey, int> getHashCode)
        {
            this.Size = HashHelpers.GetPrime(capacity);
            this.entries = new Entry3<TKey, TValue>[this.Size];
            int bvLen = HashHelpers.BitvectorByteLength(this.Size);
            this.bitvector = new byte[bvLen];
            this.dirtyBitvector = new byte[bvLen];
            this.comparerEquals = equals;
            this.resizeThreshold = this.Size >> 1;
            this.Count = 0;
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
                if ((this.bitvector[index >> 3] & (0x1 << (index & 0x7))) == 0)
                {
                    return false;
                }

                if ((hashCode == this.entries[index].hash) && (this.comparerEquals(key, this.entries[index].key)))
                {
                    return true;
                }

                index++;
                if (index == this.Size)
                    index = 0;
            }
            while (true);
        }

        /// <summary>
        /// Insert takes an index as ref parameter because Insert may resize the dictionary, which would cause index to change.
        /// </summary>
        /// <param name="index"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="hashCode"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Insert(ref int index, TKey key, TValue value, int hashCode)
        {
            this.entries[index].key = key;
            this.entries[index].value = value;
            this.entries[index].hash = hashCode;
            this.bitvector[index >> 3] |= (byte)((0x1 << (index & 0x7)));
            this.dirtyBitvector[index >> 3] |= (byte)((0x1 << (index & 0x7))); // insert causes dirtiness

            this.Count++;
            if (this.Count > this.resizeThreshold)
            {
                this.Resize();
                /* resizing may make index obsolete, hence we compute the index */
                this.Lookup(key, hashCode, out index);
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
                if (index >= this.Size)
                    return false;
            }
            return true;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool IterateDirty(ref int index)
        {
            index++;
            while ((this.dirtyBitvector[index >> 3] & (0x1 << (index & 0x7))) == 0)
            {
                index++;
                if (index >= this.Size)
                    return false;
            }
            return true;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Clean() => this.dirtyBitvector.AsSpan(0, HashHelpers.BitvectorByteLength(this.Size)).Clear();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool IsClean(ref int index) => ((this.dirtyBitvector[index >> 3] & (0x1 << (index & 0x7))) == 0);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="index"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void SetDirty(ref int index) => this.dirtyBitvector[index >> 3] |= (byte)((0x1 << (index & 0x7)));

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Clear()
        {
            this.Count = 0;
            if (Config.ClearColumnsOnReturn)
                this.entries.AsSpan().Clear();
            int bvLen = HashHelpers.BitvectorByteLength(this.Size);
            this.bitvector.AsSpan(0, bvLen).Clear();
            this.dirtyBitvector.AsSpan(0, bvLen).Clear();
        }

        private void Resize()
        {
            int newSize = HashHelpers.ExpandPrime(this.Size);
            var newEntries = new Entry3<TKey, TValue>[newSize];
            int newBvLen = HashHelpers.BitvectorByteLength(newSize);
            byte[] newBitvector = ArrayPool<byte>.Shared.Rent(newBvLen);
            byte[] newDirtyBitvector = ArrayPool<byte>.Shared.Rent(newBvLen);
            newBitvector.AsSpan(0, newBvLen).Clear();
            newDirtyBitvector.AsSpan(0, newBvLen).Clear();

            int index = 0;
            int num, newindex;
            while (index < this.Size)
            {
                if ((this.bitvector[index >> 3] & (0x1 << (index & 0x7))) != 0)
                {
                    num = this.entries[index].hash & 0x7fffffff;
                    newindex = num % newSize;
                    while ((newBitvector[newindex >> 3] & (0x1 << (newindex & 0x7))) != 0)
                    {
                        newindex++;
                        if (newindex == newSize)
                            newindex = 0;
                    }
                    newEntries[newindex] = this.entries[index];
                    newBitvector[newindex >> 3] |= (byte)((0x1 << (newindex & 0x7)));

                    if ((this.dirtyBitvector[index >> 3] & (0x1 << (index & 0x7))) != 0)
                    {
                        newDirtyBitvector[newindex >> 3] |= (byte)((0x1 << (newindex & 0x7)));
                    }
                }
                index++;
            }

            if (this.bitvectorFromPool)
            {
                ArrayPool<byte>.Shared.Return(this.bitvector);
            }

            if (this.dirtyBitvectorFromPool)
            {
                ArrayPool<byte>.Shared.Return(this.dirtyBitvector);
            }

            this.Size = newSize;
            this.entries = newEntries;
            this.bitvector = newBitvector;
            this.dirtyBitvector = newDirtyBitvector;
            this.bitvectorFromPool = true;
            this.dirtyBitvectorFromPool = true;
            this.resizeThreshold = this.Size >> 1;
        }

        /// <summary>
        /// Returns pooled bitvector storage to <see cref="ArrayPool{T}.Shared"/> when applicable.
        /// After disposal, do not use this instance.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Dispose()
        {
            if (this.disposed)
            {
                return;
            }

            this.disposed = true;

            if (this.bitvectorFromPool)
            {
                ArrayPool<byte>.Shared.Return(this.bitvector);
                this.bitvectorFromPool = false;
            }

            if (this.dirtyBitvectorFromPool)
            {
                ArrayPool<byte>.Shared.Return(this.dirtyBitvector);
                this.dirtyBitvectorFromPool = false;
            }

            this.bitvector = [];
            this.dirtyBitvector = [];
        }

    }
}