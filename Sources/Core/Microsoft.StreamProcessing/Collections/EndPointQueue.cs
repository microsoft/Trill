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
    internal sealed class EndPointQueue : IEndPointOrderer
    {
        private const int DefaultCapacity = 16;

        // End-points of the corresponding values.
        [DataMember]
        private long[] times;

        // Values of the corresponding end-points.
        [DataMember]
        private int[] values;

        // Position within the array to make next insertion.
        [DataMember]
        private int writeIndex;

        // Position within the array to make next get of peek.
        [DataMember]
        private int readIndex;

        public int Count
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                int count = this.writeIndex - this.readIndex;
                if (count < 0)
                {
                    count += this.values.Length;
                }
                return count;
            }
        }

        public bool IsEmpty
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => this.writeIndex == this.readIndex;
        }

        public int Capacity
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => this.values.Length;
        }

        public EndPointQueue() : this(DefaultCapacity) { }

        public EndPointQueue(int capacity)
        {
            Contract.Requires(capacity > 0);

            this.times = new long[capacity];
            this.values = new int[capacity];
        }

        public void Insert(long time, int value)
        {
            // Determine what write index will be after write.
            int insertIndex = this.writeIndex;
            int newWriteIndex = insertIndex + 1;
            if (newWriteIndex == this.values.Length)
            {
                newWriteIndex = 0;
            }

            // Grow the array if needed.
            if (newWriteIndex == this.readIndex)
            {
                Grow();

                // After growing, it is guaranteed that writeIndex + 1 does not need to wrap.
                insertIndex = this.writeIndex;
                newWriteIndex = insertIndex + 1;
            }

#if DEBUG
            // Perform check time is non-decreasing (if in debug mode).
            if (this.Count > 0)
            {
                int lastIndex = insertIndex - 1;
                if (lastIndex == -1)
                {
                    lastIndex = this.values.Length - 1;
                }
                if (this.times[lastIndex] > time)
                {
                    throw new ArgumentException("Time is decreasing from last insert");
                }
            }
#endif

            // Insert element at insert index.
            this.times[insertIndex] = time;
            this.values[insertIndex] = value;

            // Update write index.
            this.writeIndex = newWriteIndex;
        }

        public bool TryPeekNext(out long time, out int value)
        {
            // If queue is empty, return false.
            int peekIndex = this.readIndex;
            if (peekIndex == this.writeIndex)
            {
                time = 0;
                value = 0;
                return false;
            }

            // Return element at top of queue.
            time = this.times[peekIndex];
            value = this.values[peekIndex];
            return true;
        }

        public bool TryGetNext(out long time, out int value)
        {
            // If queue is empty, return false.
            int getIndex = this.readIndex;
            if (getIndex == this.writeIndex)
            {
                time = 0;
                value = 0;
                return false;
            }

            // Return element at top of queue.
            time = this.times[getIndex];
            value = this.values[getIndex];

            // Update readIndex.
            int nextReadIndex = getIndex + 1;
            if (nextReadIndex == this.values.Length)
            {
                nextReadIndex = 0;
            }

            this.readIndex = nextReadIndex;
            return true;
        }

        public bool TryGetNextInclusive(long maxTime, out long time, out int value)
        {
            // If queue is empty, return false.
            int getIndex = this.readIndex;
            if (getIndex == this.writeIndex)
            {
                time = 0;
                value = 0;
                return false;
            }

            // Check time of element at top of queue.
            long topTime = this.times[getIndex];
            if (topTime > maxTime)
            {
                time = 0;
                value = 0;
                return false;
            }

            // Return element at top of queue.
            time = topTime;
            value = this.values[getIndex];

            // Update readIndex.
            int nextReadIndex = getIndex + 1;
            if (nextReadIndex == this.values.Length)
            {
                nextReadIndex = 0;
            }

            this.readIndex = nextReadIndex;
            return true;
        }

        public bool TryGetNextExclusive(long maxTime, out long time, out int value)
        {
            // If queue is empty, return false.
            int getIndex = this.readIndex;
            if (getIndex == this.writeIndex)
            {
                time = 0;
                value = 0;
                return false;
            }

            // Check time of element at top of queue.
            long topTime = this.times[getIndex];
            if (topTime >= maxTime)
            {
                time = 0;
                value = 0;
                return false;
            }

            // Return element at top of queue.
            time = topTime;
            value = this.values[getIndex];

            // Update readIndex.
            int nextReadIndex = getIndex + 1;
            if (nextReadIndex == this.values.Length)
            {
                nextReadIndex = 0;
            }

            this.readIndex = nextReadIndex;
            return true;
        }

        public void RemoveTop()
        {
            Contract.Assume(this.Count > 0);

            // Update readIndex.
            int nextReadIndex = this.readIndex + 1;
            if (nextReadIndex == this.values.Length)
            {
                nextReadIndex = 0;
            }

            this.readIndex = nextReadIndex;
        }

        public void Clear()
        {
            this.readIndex = 0;
            this.writeIndex = 0;
        }

        private void Grow()
        {
            // Calculate new capacity to be double previous, capped at int.MaxValue if overflow.
            var oldTimes = this.times;
            var oldValues = this.values;
            int oldCapacity = oldValues.Length;
            int newCapacity = oldCapacity * 2;
            if (newCapacity < 0)
            {
                newCapacity = int.MaxValue;
            }

            // Create new arrays.
            this.times = new long[newCapacity];
            this.values = new int[newCapacity];

            // Copy values over.
            int count = this.writeIndex - this.readIndex;
            if (count < 0)
            {
                count += oldCapacity;
            }

            // - Part 1: Copy from [readIndex, (less of count or until end of array)].
            int copy1Length = Math.Min(oldCapacity - this.readIndex, count);
            Array.Copy(oldTimes, this.readIndex, this.times, 0, copy1Length);
            Array.Copy(oldValues, this.readIndex, this.values, 0, copy1Length);

            // - Part 2: Copy from [0, (remaining elements)].
            int copy2Length = count - copy1Length;
            if (copy2Length > 0)
            {
                Array.Copy(oldTimes, 0, this.times, copy1Length, copy2Length);
                Array.Copy(oldValues, 0, this.values, copy1Length, copy2Length);
            }

            // Update indexes.
            this.readIndex = 0;
            this.writeIndex = count;
        }
    }
}
