// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing.Internal.Collections
{
    [DataContract]
    internal sealed class EndPointHeap : IEndPointOrderer
    {
        private const int DefaultCapacity = 16;
        private const int MaxCapacity = int.MaxValue;

        // End-points of the values.
        [DataMember]
        private long[] times;

        // Actual values inserted into the heap.
        [DataMember]
        private int[] values;

        // Number of values current in the heap.
        [DataMember]
        private int count;

        // Number of values the heap can hold.
        [DataMember]
        private int capacity;

        public EndPointHeap() : this(DefaultCapacity) { }

        public EndPointHeap(int capacity)
        {
            Contract.Requires(capacity > 0 && capacity <= MaxCapacity);

            this.times = new long[capacity];
            this.values = new int[capacity];
            this.capacity = capacity;
        }

        public int Count
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => this.count;
        }

        public bool IsEmpty
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => this.count == 0;
        }

        public int Capacity
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => this.capacity;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void Insert(long time, int value)
        {
            // If out of space in the stack, then grow.
            if (this.count == this.capacity) Grow();

            fixed (long* timeArray = this.times)
            fixed (int* valueArray = this.values)
            {
                // Find location for new element in heap, default to end.
                int insertPos = this.count;
                this.count++;

                // Loop while position 'pos' still has a parent.
                while (insertPos > 0)
                {
                    // Determine if position 'insertPos' would be consistent with its parent.
                    int parentPos = (insertPos - 1) >> 1;
                    long parentTime = *(timeArray + parentPos);
                    if (parentTime <= time)
                    {
                        // Parent is <= time, so heap would be consistent and we are done.
                        break;
                    }

                    // Heap is not consistent, so move insertion point to location of
                    // parent and move parent to current 'insertPos'.
                    *(timeArray + insertPos) = parentTime;
                    *(valueArray + insertPos) = *(valueArray + parentPos);
                    insertPos = parentPos;
                }

                // Insert element into heap.
                *(timeArray + insertPos) = time;
                *(valueArray + insertPos) = value;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
            time = this.times[0];
            value = this.values[0];
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
            time = this.times[0];
            value = this.values[0];
            RemoveTop();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetNextInclusive(long maxTime, out long time, out int value)
        {
            // If heap is empty or top element is beyond maxTime, return false.
            if (this.count == 0 || this.times[0] > maxTime)
            {
                time = 0;
                value = 0;
                return false;
            }

            // Return top element and remove top.
            time = this.times[0];
            value = this.values[0];
            RemoveTop();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetNextExclusive(long maxTime, out long time, out int value)
        {
            // If heap is empty or top element is beyond maxTime, return false.
            if (this.count == 0 || this.times[0] >= maxTime)
            {
                time = 0;
                value = 0;
                return false;
            }

            // Return top element and remove top.
            time = this.times[0];
            value = this.values[0];
            RemoveTop();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void RemoveTop()
        {
            Contract.Assume(this.count > 0);

            // Handle case of only a few elements in heap.
            if (this.count == 1)
            {
                this.count = 0;
                return;
            }

            fixed (long* timeArray = this.times)
            {
                fixed (int* valueArray = this.values)
                {
                    // Find new location for last element in heap, default to root.
                    this.count--;
                    long time = *(timeArray + this.count);
                    int insertPos = 0;

                    // Loop while position 'insertPos' still has at least one child.
                    int lastPosWithChild = (this.count >> 1) - 1;
                    while (insertPos <= lastPosWithChild)
                    {
                        // Determine if position 'insertPos' is consistent with its children (or child).
                        int leftChildPos = (insertPos << 1) + 1;
                        long leftChildTime = *(timeArray + leftChildPos);
                        int rightChildPos = leftChildPos + 1;
                        int smallestChildPos;
                        long smallestChildTime;
                        if (rightChildPos < this.count)
                        {
                            long rightChildTime = *(timeArray + rightChildPos);
                            if (rightChildTime < leftChildTime)
                            {
                                smallestChildPos = rightChildPos;
                                smallestChildTime = rightChildTime;
                            }
                            else
                            {
                                smallestChildPos = leftChildPos;
                                smallestChildTime = leftChildTime;
                            }
                        }
                        else
                        {
                            smallestChildPos = leftChildPos;
                            smallestChildTime = leftChildTime;
                        }

                        if (time <= smallestChildTime)
                        {
                            // Parent is <= time of its smallest child, so heap would be consistent and we are done.
                            break;
                        }

                        // Heap is not consistent, so move insertion point to location of smallest child
                        // and move smallest child to current 'insertPos'
                        *(timeArray + insertPos) = smallestChildTime;
                        *(valueArray + insertPos) = *(valueArray + smallestChildPos);
                        insertPos = smallestChildPos;
                    }

                    // Insert element into heap.
                    *(timeArray + insertPos) = time;
                    *(valueArray + insertPos) = *(valueArray + this.count);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clear() => this.count = 0;

        private void Grow()
        {
            // Throw exception if already at max capacity.
            if (this.capacity == MaxCapacity)
            {
                throw new InvalidOperationException("EndPointHeap has reached maximum size");
            }

            // Calculate new capacity to be twice as large as before.
            this.capacity <<= 1;
            if (this.capacity < 0 || this.capacity > MaxCapacity)
            {
                // Handle case of capacity going larger than MaxCapacity,
                // or overflowing (which would make it appear negative).
                this.capacity = MaxCapacity;
            }

            // Create new arrays and copy values over.
            long[] oldTimes = this.times;
            this.times = new long[this.capacity];
            Array.Copy(oldTimes, this.times, this.count);

            int[] oldValues = this.values;
            this.values = new int[this.capacity];
            Array.Copy(oldValues, this.values, this.count);
        }
    }
}
