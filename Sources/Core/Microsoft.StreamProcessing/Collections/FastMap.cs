// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.ComponentModel;
using System.Diagnostics.Contracts;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing.Internal.Collections
{
    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public sealed class FastMap<T>
    {
        private const int DefaultCapacity = 17;
        private const long OnlyNextBits = 0x00000000FFFFFFFFL;
        private const long OnlyHashBits = ~OnlyNextBits;

        // Constant to AND with Int32 values to make positive.
        private const int NotHighestBit = 0x7FFFFFFF;

        // Index value denoting last element of a linked-list.
        private const int EndOfList = 0;

        // Index value denoting last element of an invisible (non-findable) linked-list.
        private const int EndOfInvisibleList = ~EndOfList;

        // Index of the head of the free linked-list.
        [DataMember]
        private int freeHead = EndOfInvisibleList;

        // Index of the head of the invisible linked-list.
        [DataMember]
        private int invisibleHead = EndOfInvisibleList;

        // Index of the head of the linked-list corresponding to each bucket
        // where bucket equals hash % capacity.
        [DataMember]
        private int[] bucketHeads;

        // Hash value and next pointer for each item in 'values' stored with
        // hash value in upper 32-bits and next index in lower 32-bits.
        // Next index will be bit-wise inverted if it is in an "invisible" list,
        // including the free list - this makes those element have a negative
        // next field.
        // Note I did not use a structure because of performance reasons and
        // apparently you cannot use 'fixed' on an array of structures.
        [DataMember]
        private long[] hashAndNext;

        // Actual values inserted into map. Index 0 is not used.
        [DataMember]
        private T[] values;

        // Number of values currently in the map.
        [DataMember]
        private int count;

        // Number of values that are (or were) in-use. Only the values in
        // 'values' and 'nextHashes' indexes [1, initialized] have meaningful
        // 'next' values.
        [DataMember]
        private int initialized;

        // Index of capacity out of PrimeFuncs.Primes[] array.
        [DataMember]
        private int primeIndex;

        // Number of values the FastMap can hold.
        [DataMember]
        private int capacity;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public FastMap() : this(DefaultCapacity) { }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="minCapacity"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public FastMap(int minCapacity)
        {
            Contract.Requires(minCapacity > 0);

            this.primeIndex = PrimeFuncs.FindIndexOfPrimeGreaterOrEqualTo(minCapacity);
            this.capacity = PrimeFuncs.Primes[this.primeIndex];
            this.bucketHeads = new int[this.capacity];
            this.hashAndNext = new long[this.capacity + 1];
            this.values = new T[this.capacity + 1];
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int Count
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => this.count;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool IsEmpty
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => this.count == 0;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool IsInvisibleEmpty
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => this.invisibleHead == EndOfInvisibleList;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T[] Values
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => this.values;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="hash"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int Insert(int hash)
        {
            // Allocate free value to store new value.
            int index = AllocateValue();

            // Insert 'index' into bucket linked-list.
            int bucketPos = (hash & NotHighestBit) % this.capacity;
            int bucketHead = this.bucketHeads[bucketPos];
            this.hashAndNext[index] = ((long)hash << 32) | (uint)bucketHead;
            this.bucketHeads[bucketPos] = index;
            return index;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="hash"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int Insert(int hash, T value)
        {
            int index = Insert(hash);
            this.values[index] = value;
            return index;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="hash"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int InsertInvisible(int hash)
        {
            // Allocate free value to store new value.
            int index = AllocateValue();

            // Insert 'index' into invisible linked-list.
            this.hashAndNext[index] = ((long)hash << 32) | (uint)this.invisibleHead;
            this.invisibleHead = ~index;
            return index;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="index"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void MakeInvisible(int index)
        {
            Contract.Assume(index > 0 && index <= this.initialized);

            // Remove from current list.
            long hashNext = RemoveFromList(index);

            // Insert into invisible list (with inverted index to denote invisible list).
            this.hashAndNext[index] = (hashNext & OnlyHashBits) | (uint)this.invisibleHead;
            this.invisibleHead = ~index;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="index"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Remove(int index)
        {
            Contract.Assume(index > 0 && index <= this.initialized);

            if (Config.ClearColumnsOnReturn)
            {
                this.values[index] = default;
            }

            // Remove from current list.
            RemoveFromList(index);

            // Insert into free list (with inverted index to denote invisible list).
            this.hashAndNext[index] = (uint)this.freeHead;
            this.freeHead = ~index;
            this.count--;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int GetHash(int index)
        {
            Contract.Assume(index > 0 && index <= this.initialized);
            return (int)(this.hashAndNext[index] >> 32);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe void Clear()
        {
            fixed (int* bucketHeadArray = this.bucketHeads)
            {
                // Clear all bucket linked-lists.
                int* bucketsHeadPtr = bucketHeadArray;
                for (int i = this.capacity; i > 0; i--)
                {
                    *bucketsHeadPtr++ = EndOfList;
                }

                // Setting 'initialized' to zero puts all values back in free pool, so also
                // reset free linked-list.
                this.freeHead = EndOfInvisibleList;
                this.invisibleHead = EndOfInvisibleList;
                this.count = 0;
                this.initialized = 0;
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="hash"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public FindTraverser Find(int hash) => new FindTraverser(this, hash);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="hash"></param>
        /// <param name="ft"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool Find(int hash, ref FindTraverser ft)
        {
            int currIndex = (hash & NotHighestBit) % this.capacity;
            int nextIndex = this.bucketHeads[currIndex];
            if (nextIndex == EndOfList) return false;

            ft.map = this;
            ft.hash = hash;
            ft.prevIndex = 0;
            ft.prevIndexIsHead = false;
            ft.currIndex = (hash & NotHighestBit) % this.capacity;
            ft.currIndexIsHead = true;
            ft.nextIndex = this.bucketHeads[currIndex];

            // ft = new FindTraverser(this, hash, currIndex, nextIndex);
            return true;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public VisibleTraverser Traverse() => new VisibleTraverser(this);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public InvisibleTraverser TraverseInvisible() => new InvisibleTraverser(this);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int AllocateValue()
        {
            if (this.freeHead != EndOfInvisibleList)
            {
                // Return the entry at the head of the free linked-list and
                // update the free linked-list to point to the *next* entry.
                this.count++;
                int allocatedIndex = ~this.freeHead;
                this.freeHead = (int)this.hashAndNext[allocatedIndex];
                return allocatedIndex;
            }

            if (this.initialized >= this.capacity)
            {
                // No free entries available, so resize.
                Grow();
            }

            this.count++;
            return ++this.initialized;
        }

        private unsafe void Grow()
        {
            // Throw exception if already at max capacity.
            if (this.primeIndex == PrimeFuncs.Primes.Length - 1)
            {
                throw new InvalidOperationException("FastMap has reached maximum size");
            }

            // Save pointers to old arrays.
            long[] oldHashAndNext = this.hashAndNext;
            T[] oldValues = this.values;

            // Allocate new arrays of double the size.
            this.primeIndex++;
            this.capacity = PrimeFuncs.Primes[this.primeIndex];
            this.bucketHeads = new int[this.capacity];
            this.hashAndNext = new long[this.capacity + 1];
            this.values = new T[this.capacity + 1];

            // Copy over old values and insert into new buckets.
            // Only [1, initialized] are populated values.
            Array.Copy(oldValues, 1, this.values, 1, this.initialized);

            // Re-insert keys into hash table.
            fixed (long* oldHashAndNextArray = oldHashAndNext)
            fixed (long* hashAndNextArray = this.hashAndNext)
            fixed (int* bucketHeadArray = this.bucketHeads)
            {
                long* oldHashNextPtr = oldHashAndNextArray + 1;
                long* hashNextPtr = hashAndNextArray + 1;
                for (int index = 1; index <= this.initialized; index++)
                {
                    // Insert value into new hash table.
                    long oldHashNext = *oldHashNextPtr;
                    int oldNext = (int)oldHashNext;
                    if (oldNext >= 0)
                    {
                        // Value is in "visible" list so needs to be rehashed.
                        int oldHash = (int)(oldHashNext >> 32);
                        int bucketPos = (oldHash & NotHighestBit) % this.capacity;
                        int bucketHead = *(bucketHeadArray + bucketPos);
                        *hashNextPtr = (oldHashNext & OnlyHashBits) | (uint)bucketHead;
                        *(bucketHeadArray + bucketPos) = index;
                    }
                    else
                    {
                        // Value is in "invisible" list and does not need to be rehashed.
                        *hashNextPtr = oldHashNext;
                    }

                    oldHashNextPtr++;
                    hashNextPtr++;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe long RemoveFromList(int index)
        {
            fixed (long* hashNextArray = this.hashAndNext)
            {
                // Save values for item to remove.
                long removedHashNext = *(hashNextArray + index);
                int removedNext = (int)removedHashNext;

                // Remove from linked-list.
                int prevIndex;
                if (removedNext >= 0)
                {
                    // Element is in the "visible" list.

                    // Traverse bucket linked-list.
                    int removedHash = (int)(removedHashNext >> 32);
                    int bucketPos = (removedHash & NotHighestBit) % this.capacity;
                    prevIndex = this.bucketHeads[bucketPos];
                    if (prevIndex == index)
                    {
                        // Handle case of index being first in linked-list.
                        this.bucketHeads[bucketPos] = removedNext;
                        return removedHashNext;
                    }
                }
                else
                {
                    // Element is in the "invisible" linked list.

                    // Traverse invisible linked-list (searching for inverted index).
                    index = ~index;
                    prevIndex = this.invisibleHead;
                    if (prevIndex == index)
                    {
                        // Handle case of index being first in linked-list.
                        this.invisibleHead = removedNext;
                        return removedHashNext;
                    }
                }

                // Handle case of index NOT being first in linked-list.
                while (true)
                {
                    long currHashNext = *(hashNextArray + prevIndex);
                    int currNext = (int)currHashNext;
                    if (currNext == index)
                    {
                        *(hashNextArray + prevIndex) = (currHashNext & OnlyHashBits) | (uint)removedNext;
                        return removedHashNext;
                    }

                    prevIndex = currNext;
                }
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public struct FindTraverser
        {
            internal FastMap<T> map;

            internal int hash;

            internal int prevIndex;

            internal int currIndex;

            internal int nextIndex;

            internal bool prevIndexIsHead;

            internal bool currIndexIsHead;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="map"></param>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public FindTraverser(FastMap<T> map)
            {
                this.map = map;
                this.hash = 0;
                this.prevIndex = 0;
                this.prevIndexIsHead = false;
                this.currIndex = 0;
                this.currIndexIsHead = false;
                this.nextIndex = 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal FindTraverser(FastMap<T> map, int hash)
            {
                this.map = map;
                this.hash = hash;
                this.prevIndex = 0;
                this.prevIndexIsHead = false;
                this.currIndex = (hash & NotHighestBit) % map.capacity;
                this.currIndexIsHead = true;
                this.nextIndex = map.bucketHeads[this.currIndex];
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="hash"></param>
            /// <returns></returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            [EditorBrowsable(EditorBrowsableState.Never)]
            public unsafe bool Find(int hash)
            {
                this.currIndex = (hash & NotHighestBit) % this.map.capacity;
                this.nextIndex = this.map.bucketHeads[this.currIndex];

                this.hash = hash;
                this.prevIndex = 0;
                this.prevIndexIsHead = false;
                this.currIndexIsHead = true;

                return (this.nextIndex != EndOfList);
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="index"></param>
            /// <returns></returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            [EditorBrowsable(EditorBrowsableState.Never)]
            public unsafe bool Next(out int index)
            {
                fixed (long* hashAndNextArray = this.map.hashAndNext)
                {
                    // While not at end of list.
                    while (this.nextIndex != EndOfList)
                    {
                        // Traverse to next in linked-list.
                        this.prevIndex = this.currIndex;
                        this.prevIndexIsHead = this.currIndexIsHead;
                        this.currIndex = this.nextIndex;
                        this.currIndexIsHead = false;
                        long currHashNext = *(hashAndNextArray + this.currIndex);
                        this.nextIndex = (int)currHashNext;

                        // If hash for currIndex matches, then return true.
                        int currHash = (int)(currHashNext >> 32);
                        if (currHash == this.hash)
                        {
                            index = this.currIndex;
                            return true;
                        }
                    }

                    // No items with matching hash was found, so return false.
                    index = 0;
                    return false;
                }
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            [EditorBrowsable(EditorBrowsableState.Never)]
            public void Remove()
            {
                // Remove currIndex, so move prevIndex up to currIndex.
                int removedIndex = this.currIndex;
                this.currIndex = this.prevIndex;
                this.currIndexIsHead = this.prevIndexIsHead;

                // Have currIndex (which was prevIndex)'s next pointer point to nextIndex.
                if (this.currIndexIsHead)
                {
                    this.map.bucketHeads[this.currIndex] = this.nextIndex;
                }
                else
                {
                    long currHashNext = this.map.hashAndNext[this.currIndex];
                    this.map.hashAndNext[this.currIndex] = (currHashNext & OnlyHashBits) | (uint)this.nextIndex;
                }

                // Put removedIndex in the free list.
                this.map.hashAndNext[removedIndex] = this.map.freeHead;
                this.map.freeHead = ~removedIndex;
                this.map.count--;
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <returns></returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            [EditorBrowsable(EditorBrowsableState.Never)]
            public int InsertAt()
            {
                int newIndex = this.map.AllocateValue();

                // Link new index to next index
                this.map.hashAndNext[newIndex] = ((long)this.hash << 32) | (uint)this.nextIndex;

                // Link curr index to new index
                if (this.currIndexIsHead)
                {
                    this.map.bucketHeads[this.currIndex] = newIndex;
                }
                else
                {
                    long currHashNext = this.map.hashAndNext[this.currIndex];
                    this.map.hashAndNext[this.currIndex] = (currHashNext & OnlyHashBits) | (uint)newIndex;
                }

                this.prevIndex = this.currIndex;
                this.prevIndexIsHead = this.currIndexIsHead;
                this.currIndex = newIndex;
                this.currIndexIsHead = false;

                return newIndex;
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public struct VisibleTraverser
        {
            private readonly FastMap<T> map;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public int currIndex;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="map"></param>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            [EditorBrowsable(EditorBrowsableState.Never)]
            public VisibleTraverser(FastMap<T> map)
            {
                this.map = map;
                this.currIndex = 0;
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="index"></param>
            /// <param name="hash"></param>
            /// <returns></returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            [EditorBrowsable(EditorBrowsableState.Never)]
            public unsafe bool Next(out int index, out int hash)
            {
                fixed (long* hashAndNextArray = this.map.hashAndNext)
                {
                    long* hashNextPtr = hashAndNextArray + this.currIndex;
                    int initialized = this.map.initialized;
                    while (this.currIndex < initialized)
                    {
                        this.currIndex++;
                        hashNextPtr++;

                        long currHashNext = *hashNextPtr;
                        int currNext = (int)currHashNext;
                        if (currNext >= 0)
                        {
                            int currHash = (int)(currHashNext >> 32);
                            index = this.currIndex;
                            hash = currHash;
                            return true;
                        }
                    }

                    index = 0;
                    hash = 0;
                    return false;
                }
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            [EditorBrowsable(EditorBrowsableState.Never)]
            public void Remove() => this.map.Remove(this.currIndex);

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            [EditorBrowsable(EditorBrowsableState.Never)]
            public void MakeInvisible() => this.map.MakeInvisible(this.currIndex);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public struct InvisibleTraverser
        {
            private readonly FastMap<T> map;

            private int prevIndex;

            private int currIndex;

            private int nextIndex;

            private bool prevIndexIsHead;

            private bool currIndexIsHead;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal InvisibleTraverser(FastMap<T> map)
            {
                this.map = map;
                this.prevIndex = 0;
                this.prevIndexIsHead = false;
                this.currIndex = 0;
                this.currIndexIsHead = true;
                this.nextIndex = ~map.invisibleHead;
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="index"></param>
            /// <param name="hash"></param>
            /// <returns></returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            [EditorBrowsable(EditorBrowsableState.Never)]
            public unsafe bool Next(out int index, out int hash)
            {
                fixed (long* hashAndNextArray = this.map.hashAndNext)
                {
                    // Return false if at end of list.
                    if (this.nextIndex == EndOfList)
                    {
                        index = 0;
                        hash = 0;
                        return false;
                    }

                    // Traverse to next in linked-list.
                    this.prevIndex = this.currIndex;
                    this.prevIndexIsHead = this.currIndexIsHead;
                    this.currIndex = this.nextIndex;
                    this.currIndexIsHead = false;
                    long currHashNext = *(hashAndNextArray + this.currIndex);
                    this.nextIndex = ~(int)currHashNext;

                    // Return true with current index and hash.
                    int currHash = (int)(currHashNext >> 32);
                    index = this.currIndex;
                    hash = currHash;
                    return true;
                }
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            [EditorBrowsable(EditorBrowsableState.Never)]
            public void Remove()
            {
                // Remove currIndex, so move prevIndex up to currIndex.
                int removedIndex = this.currIndex;
                this.currIndex = this.prevIndex;
                this.currIndexIsHead = this.prevIndexIsHead;

                // Have currIndex (which was prevIndex)'s next pointer point to nextIndex.
                if (this.currIndexIsHead)
                {
                    this.map.invisibleHead = ~this.nextIndex;
                }
                else
                {
                    long currHashNext = this.map.hashAndNext[this.currIndex];
                    this.map.hashAndNext[this.currIndex] = (currHashNext & OnlyHashBits) | (uint)(~this.nextIndex);
                }

                // Put removedIndex in the free list.
                this.map.hashAndNext[removedIndex] = this.map.freeHead;
                this.map.freeHead = ~removedIndex;
                this.map.count--;
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            [EditorBrowsable(EditorBrowsableState.Never)]
            public void MakeVisible()
            {
                // Remove currIndex, so move prevIndex up to currIndex.
                int visibleIndex = this.currIndex;
                this.currIndex = this.prevIndex;
                this.currIndexIsHead = this.prevIndexIsHead;

                // Have currIndex (which was prevIndex)'s next pointer point to nextIndex.
                if (this.currIndexIsHead)
                {
                    this.map.invisibleHead = ~this.nextIndex;
                }
                else
                {
                    long currHashNext = this.map.hashAndNext[this.currIndex];
                    this.map.hashAndNext[this.currIndex] = (currHashNext & OnlyHashBits) | (uint)(~this.nextIndex);
                }

                // Put visibleIndex in the visible list.
                long visibleHashNext = this.map.hashAndNext[visibleIndex];
                int visibleHash = (int)(visibleHashNext >> 32);
                int bucketPos = (visibleHash & NotHighestBit) % this.map.capacity;
                int bucketHead = this.map.bucketHeads[bucketPos];
                this.map.hashAndNext[visibleIndex] = (visibleHashNext & OnlyHashBits) | (uint)bucketHead;
                this.map.bucketHeads[bucketPos] = visibleIndex;
            }
        }
    }
}
