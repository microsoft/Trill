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
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public sealed class RemovableEndPointHeap
    {
        private const int DefaultCapacity = 16;
        private const int MaxCapacity = int.MaxValue;

        private const int EndOfList = -1;

        [DataContract]
        private struct Element
        {
            [DataMember]
            public long Time;

            [DataMember]
            public int Value;

            [DataMember]
            public int Index;
        }

        // Elements in the heap.
        [DataMember]
        private Element[] heap;

        // Locations of items in the heap to allow for deletion.
        [DataMember]
        private int[] locations;

        // Number of values current in the heap.
        [DataMember]
        private int count;

        // Index of lowest uninitialized entry in locations.
        [DataMember]
        private int initialized;

        // Head of free entry linked list.
        [DataMember]
        private int freeHead = EndOfList;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public RemovableEndPointHeap() : this(DefaultCapacity) { }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="capacity"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public RemovableEndPointHeap(int capacity)
        {
            Contract.Requires(capacity > 0 && capacity <= MaxCapacity);
            this.heap = new Element[capacity];
            this.locations = new int[capacity];
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
        public int Capacity
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => this.heap.Length;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="time"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int Insert(long time, int value)
        {
            // Get index for this insert, growing if necesarry.
            int index = Allocate();

            // Find location for new element in heap, default to end.
            int heapPos = this.count;
            this.count++;

            // Heapify-up for end of heap.
            var element = new Element { Time = time, Value = value, Index = index };
            HeapifyUp(ref element, heapPos);

            return index;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="time"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool TryPeekNext(out long time, out int value)
        {
            // If heap is empty, return false.
            if (this.count == 0)
            {
                time = 0;
                value = 0;
                return false;
            }

            // Return top element.
            var top = this.heap[0];
            time = top.Time;
            value = top.Value;
            return true;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="time"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool TryGetNext(out long time, out int value)
        {
            // If heap is empty, return false.
            if (this.count == 0)
            {
                time = 0;
                value = 0;
                return false;
            }

            // Return top element and remove top.
            var top = this.heap[0];
            time = top.Time;
            value = top.Value;
            RemoveTop();
            return true;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="maxTime"></param>
        /// <param name="time"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool TryGetNextInclusive(long maxTime, out long time, out int value)
        {
            // If heap is empty or top element is beyond maxTime, return false.
            if (this.count == 0)
            {
                time = 0;
                value = 0;
                return false;
            }
            var top = this.heap[0];
            if (top.Time > maxTime)
            {
                time = 0;
                value = 0;
                return false;
            }

            // Return top element and remove top.
            time = top.Time;
            value = top.Value;
            RemoveTop();
            return true;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="maxTime"></param>
        /// <param name="time"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool TryGetNextExclusive(long maxTime, out long time, out int value)
        {
            // If heap is empty or top element is beyond maxTime, return false.
            if (this.count == 0)
            {
                time = 0;
                value = 0;
                return false;
            }
            var top = this.heap[0];
            if (top.Time >= maxTime)
            {
                time = 0;
                value = 0;
                return false;
            }

            // Return top element and remove top.
            time = top.Time;
            value = top.Value;
            RemoveTop();
            return true;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void RemoveTop()
        {
            Contract.Requires(this.Count > 0);
            int index = this.heap[0].Index;
            Free(index);
            this.count--;
            if (this.count > 0)
            {
                HeapifyDown(ref this.heap[this.count], 0);
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="index"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Remove(int index)
        {
            Contract.Assume(index >= 0 && index < this.initialized);
            int heapPos = this.locations[index];
            Free(index);
            this.count--;
            if (heapPos < this.count)
            {
                Heapify(ref this.heap[this.count], heapPos);
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Clear()
        {
            this.count = 0;
            this.initialized = 0;
            this.freeHead = EndOfList;
        }

        private void Heapify(ref Element element, int heapPos)
        {
            if (this.Count == 1)
            {
                this.heap[0] = element;
                this.locations[element.Index] = 0;
                return;
            }
            if (heapPos == 0)
            {
                HeapifyDown(ref element, 0);
            }
            else
            {
                if (!HeapifyUp(ref element, heapPos))
                {
                    HeapifyDown(ref element, heapPos);
                }
            }
        }

        private unsafe bool HeapifyUp(ref Element element, int heapPos)
        {
            fixed (Element* heapArray = this.heap)
            fixed (int* locationArray = this.locations)
            {
                // Loop while position 'pos' still has a parent.
                bool moved = false;
                long elementTime = element.Time;
                while (heapPos > 0)
                {
                    // Determine if position 'heapPos' would be consistent with its parent.
                    int parentPos = (heapPos - 1) >> 1;
                    Element parent = *(heapArray + parentPos);
                    if (parent.Time <= elementTime)
                    {
                        // Parent is <= time, so heap would be consistent and we are done.
                        break;
                    }

                    // Heap is not consistent, so move insertion point to location of
                    // parent and move parent to current 'insertPos'.
                    *(heapArray + heapPos) = parent;
                    *(locationArray + parent.Index) = heapPos;
                    heapPos = parentPos;
                    moved = true;
                }

                // Insert element into heap.
                *(heapArray + heapPos) = element;
                *(locationArray + element.Index) = heapPos;
                return moved;
            }
        }

        private unsafe void HeapifyDown(ref Element element, int heapPos)
        {
            fixed (Element* heapArray = this.heap)
            fixed (int* locationArray = this.locations)
            {
                // Loop while position 'heapPos' still has at least one child.
                long elementTime = element.Time;
                int lastPosWithChild = (this.count >> 1) - 1;
                while (heapPos <= lastPosWithChild)
                {
                    // Determine if position 'insertPos' is consistent with its children (or child).
                    int leftChildPos = (heapPos << 1) + 1;
                    var leftChild = *(heapArray + leftChildPos);
                    int rightChildPos = leftChildPos + 1;
                    int smallestChildPos;
                    Element smallestChild;
                    if (rightChildPos < this.count)
                    {
                        var rightChild = *(heapArray + rightChildPos);
                        if (rightChild.Time < leftChild.Time)
                        {
                            smallestChildPos = rightChildPos;
                            smallestChild = rightChild;
                        }
                        else
                        {
                            smallestChildPos = leftChildPos;
                            smallestChild = leftChild;
                        }
                    }
                    else
                    {
                        smallestChildPos = leftChildPos;
                        smallestChild = leftChild;
                    }

                    if (elementTime <= smallestChild.Time)
                    {
                        // Parent is <= time of its smallest child, so heap would be consistent and we are done.
                        break;
                    }

                    // Heap is not consistent, so move insertion point to location of smallest child
                    // and move smallest child to current 'insertPos'
                    *(heapArray + heapPos) = smallestChild;
                    *(locationArray + smallestChild.Index) = heapPos;
                    heapPos = smallestChildPos;
                }

                // Insert element into heap.
                *(heapArray + heapPos) = element;
                *(locationArray + element.Index) = heapPos;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int Allocate()
        {
            if (this.freeHead != EndOfList)
            {
                // Free linked-list is not empty, so take top entry.
                int next = this.freeHead;
                this.freeHead = this.locations[next];
                return next;
            }

            if (this.initialized == this.heap.Length)
            {
                // Out of capacity, so grow.
                Grow();
            }

            // Take next element from initialized.
            return this.initialized++;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Free(int index)
        {
            // Add "index" to free linked-list.
            this.locations[index] = this.freeHead;
            this.freeHead = index;
        }

        private void Grow()
        {
            // Throw exception if already at max capacity.
            int capacity = this.heap.Length;
            if (capacity == MaxCapacity)
            {
                throw new InvalidOperationException("Heap has reached maximum size");
            }

            // Calculate new capacity to be twice as large as before.
            capacity <<= 1;
            if (capacity < 0 || capacity > MaxCapacity)
            {
                // Handle case of capacity going larger than MaxCapacity,
                // or overflowing (which would make it appear negative).
                capacity = MaxCapacity;
            }

            // Create new arrays and copy values over.
            Element[] oldHeap = this.heap;
            this.heap = new Element[capacity];
            Array.Copy(oldHeap, this.heap, this.count);

            int[] oldLocations = this.locations;
            this.locations = new int[capacity];
            Array.Copy(oldLocations, this.locations, this.initialized);
        }
    }
}
