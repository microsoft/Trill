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
    public sealed class FastLinkedList<T>
    {
        private const int DefaultCapacity = 16;

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

        // Hash value and next pointer for each item in 'values' stored with
        // hash value in upper 32-bits and next index in lower 32-bits.
        // Next index will be bit-wise inverted if it is in an "invisible" list,
        // including the free list - this makes those element have a negative
        // next field.
        // Note I did not use a structure because of performance reasons and
        // apparantly you cannot use 'fixed' on an array of structures.
        [DataMember]
        private int[] next;

        [DataMember]
        private int listHead;

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

        // Number of values the FastMap can hold.
        [DataMember]
        private int capacity;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public FastLinkedList() : this(DefaultCapacity)
        {
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="minCapacity"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public FastLinkedList(int minCapacity)
        {
            Contract.Requires(minCapacity > 0);

            this.capacity = minCapacity;
            this.listHead = 0;
            this.next = new int[this.capacity + 1];
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
        public int Capacity
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => this.capacity;
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int Insert()
        {
            // Allocate free value to store new value.
            int index = AllocateValue();

            // Insert 'index' into bucket linked-list.
            this.next[index] = this.listHead;
            this.listHead = index;
            return index;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int Insert(T value)
        {
            int index = Insert();
            this.values[index] = value;
            return index;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int Insert(ref T value)
        {
            int index = Insert();
            this.values[index] = value;
            return index;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int InsertInvisible()
        {
            // Allocate free value to store new value.
            int index = AllocateValue();

            // Insert 'index' into invisible linked-list.
            this.next[index] = this.invisibleHead;
            this.invisibleHead = ~index;
            return index;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int InsertInvisible(T value)
        {
            int index = InsertInvisible();
            this.values[index] = value;
            return index;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int InsertInvisible(ref T value)
        {
            int index = InsertInvisible();
            this.values[index] = value;
            return index;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="index"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void MakeVisible(int index)
        {
            Contract.Assume(index > 0 && index <= this.initialized);

            // Remove from current list.
            RemoveFromList(index);

            // Insert into visible list.
            this.next[index] = this.listHead;
            this.listHead = index;
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
            RemoveFromList(index);

            // Insert into invisible list (with inverted index to denote invisible list).
            this.next[index] = this.invisibleHead;
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

            // Remove from current list.
            RemoveFromList(index);

            // Insert into free list (with inverted index to denote invisible list).
            this.next[index] = this.freeHead;
            this.freeHead = ~index;
            this.count--;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe void Clear()
        {
            this.listHead = EndOfList;

            // Setting 'initialized' to zero puts all values back in free pool, so also
            // reset free linked-list.
            this.freeHead = EndOfInvisibleList;
            this.invisibleHead = EndOfInvisibleList;
            this.count = 0;
            this.initialized = 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int AllocateValue()
        {
            if (this.freeHead != EndOfInvisibleList)
            {
                // Return the entry at the head of the free linked-list and
                // update the free linked-list to point to the *next* entry.
                this.count++;
                int allocatedIndex = ~this.freeHead;
                this.freeHead = this.next[allocatedIndex];
                return allocatedIndex;
            }

            if (this.initialized < this.capacity)
            {
                // 'initialized' specified the last value index that is currently
                // in-use (either allocated or free). So, increment and return
                // the next entry.
                this.count++;
                return ++this.initialized;
            }

            // No free entries available, so resize.
            Grow();

            // Now, we're guaranteed for initialized to be available.
            this.count++;
            return ++this.initialized;
        }

        private unsafe void Grow()
        {
            // Save pointers to old arrays.
            int[] oldHashAndNext = this.next;
            T[] oldValues = this.values;

            // Allocate new arrays of double the size.
            this.capacity *= 2;
            this.next = new int[this.capacity + 1];
            this.values = new T[this.capacity + 1];

            // Copy over old values and insert into new buckets.
            // Only [1, initialized] are populated values.
            Array.Copy(oldValues, 1, this.values, 1, this.initialized);
            Array.Copy(oldHashAndNext, 1, this.next, 1, this.initialized);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe void RemoveFromList(int index)
        {
            fixed (int* hashNextArray = this.next)
            {
                // Save values for item to remove.
                int removedNext = *(hashNextArray + index);

                // Remove from linked-list.
                int prevIndex;
                if (removedNext >= 0)
                {
                    // Element is in the "visible" list.

                    // Traverse bucket linked-list.
                    prevIndex = this.listHead;
                    if (prevIndex == index)
                    {
                        // Handle case of index being first in linked-list.
                        this.listHead = removedNext;
                        return;
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
                        return;
                    }
                }

                // Handle case of index NOT being first in linked-list.
                while (true)
                {
                    long currHashNext = *(hashNextArray + prevIndex);
                    int currNext = (int)currHashNext;
                    if (currNext == index)
                    {
                        *(hashNextArray + prevIndex) = removedNext;
                        return;
                    }

                    prevIndex = currNext;
                }
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public struct ListTraverser
        {
            internal FastLinkedList<T> list;

            internal int prevIndex;

            internal int currIndex;

            internal int nextIndex;

            internal bool prevIndexIsHead;

            internal bool currIndexIsHead;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="list"></param>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public ListTraverser(FastLinkedList<T> list)
            {
                this.list = list;
                this.currIndex = 0;
                this.nextIndex = list == null ? 0 : list.listHead;
                this.prevIndex = 0;
                this.prevIndexIsHead = false;
                this.currIndexIsHead = true;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal ListTraverser(FastLinkedList<T> list, int hash)
            {
                this.list = list;
                this.prevIndex = 0;
                this.prevIndexIsHead = false;
                this.currIndex = 0;
                this.currIndexIsHead = true;
                this.nextIndex = list.listHead;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal ListTraverser(FastLinkedList<T> list, int hash, int currIndex, int nextIndex)
            {
                this.list = list;
                this.prevIndex = 0;
                this.prevIndexIsHead = false;
                this.currIndex = currIndex;
                this.currIndexIsHead = true;
                this.nextIndex = nextIndex;
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <returns></returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            [EditorBrowsable(EditorBrowsableState.Never)]
            public unsafe bool Reset()
            {
                this.currIndex = 0;
                this.nextIndex = this.list.listHead;
                if (this.nextIndex == EndOfList) return false;
                this.prevIndex = 0;
                this.prevIndexIsHead = false;
                this.currIndexIsHead = true;
                return true;
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <returns></returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            [EditorBrowsable(EditorBrowsableState.Never)]
            public unsafe bool Find()
            {
                this.currIndex = 0;
                this.nextIndex = this.list.listHead;
                if (this.nextIndex == EndOfList) return false;
                this.prevIndex = 0;
                this.prevIndexIsHead = false;
                this.currIndexIsHead = true;
                return true;
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
                fixed (int* hashAndNextArray = this.list.next)
                {
                    // While not at end of list.
                    while (this.nextIndex != EndOfList)
                    {
                        // Traverse to next in linked-list.
                        this.prevIndex = this.currIndex;
                        this.prevIndexIsHead = this.currIndexIsHead;
                        this.currIndex = this.nextIndex;
                        this.currIndexIsHead = false;
                        this.nextIndex = *(hashAndNextArray + this.currIndex);

                        index = this.currIndex;
                        return true;
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
                    this.list.listHead = this.nextIndex;
                }
                else
                {
                    this.list.next[this.currIndex] = this.nextIndex;
                }

                // Put removedIndex in the free list.
                this.list.next[removedIndex] = this.list.freeHead;
                this.list.freeHead = ~removedIndex;
                this.list.count--;
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            [EditorBrowsable(EditorBrowsableState.Never)]
            public void MakeInvisible()
            {
                // Remove currIndex, so move prevIndex up to currIndex.
                int invisibleIndex = this.currIndex;
                this.currIndex = this.prevIndex;
                this.currIndexIsHead = this.prevIndexIsHead;

                // Have currIndex (which was prevIndex)'s next pointer point to nextIndex.
                if (this.currIndexIsHead)
                {
                    this.list.listHead = this.nextIndex;
                }
                else
                {
                    this.list.next[this.currIndex] = this.nextIndex;
                }

                // Put invisibleIndex in the invisible list.
                this.list.next[invisibleIndex] = this.list.invisibleHead;
                this.list.invisibleHead = ~invisibleIndex;
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public struct VisibleTraverser
        {
            private readonly FastLinkedList<T> list;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public int currIndex;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="list"></param>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            [EditorBrowsable(EditorBrowsableState.Never)]
            public VisibleTraverser(FastLinkedList<T> list)
            {
                this.list = list;
                this.currIndex = 0;
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
                fixed (int* hashAndNextArray = this.list.next)
                {
                    int* hashNextPtr = hashAndNextArray + this.currIndex;
                    int initialized = this.list.initialized;
                    while (this.currIndex < initialized)
                    {
                        this.currIndex++;
                        hashNextPtr++;

                        long currHashNext = *hashNextPtr;
                        int currNext = (int)currHashNext;
                        if (currNext >= 0)
                        {
                            index = this.currIndex;
                            return true;
                        }
                    }

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
                this.list.Remove(this.currIndex);
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            [EditorBrowsable(EditorBrowsableState.Never)]
            public void MakeInvisible()
            {
                this.list.MakeInvisible(this.currIndex);
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public struct InvisibleTraverser
        {
            private readonly FastLinkedList<T> list;

            private int prevIndex;

            private int currIndex;

            private int nextIndex;

            private bool prevIndexIsHead;

            private bool currIndexIsHead;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal InvisibleTraverser(FastLinkedList<T> list)
            {
                this.list = list;
                this.prevIndex = 0;
                this.prevIndexIsHead = false;
                this.currIndex = 0;
                this.currIndexIsHead = true;
                this.nextIndex = ~list.invisibleHead;
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
                fixed (int* hashAndNextArray = this.list.next)
                {
                    // Return false if at end of list.
                    if (this.nextIndex == EndOfList)
                    {
                        index = 0;
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
                    index = this.currIndex;
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
                    this.list.invisibleHead = ~this.nextIndex;
                }
                else
                {
                    this.list.next[this.currIndex] = ~this.nextIndex;
                }

                // Put removedIndex in the free list.
                this.list.next[removedIndex] = this.list.freeHead;
                this.list.freeHead = ~removedIndex;
                this.list.count--;
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
                    this.list.invisibleHead = ~this.nextIndex;
                }
                else
                {
                    this.list.next[this.currIndex] = ~this.nextIndex;
                }

                // Put visibleIndex in the visible list.
                this.list.next[visibleIndex] = this.list.listHead;
                this.list.listHead = visibleIndex;
            }
        }
    }
}
