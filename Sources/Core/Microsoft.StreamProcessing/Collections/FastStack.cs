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
    public sealed class FastStack<T>
    {
        private const int DefaultCapacity = 16;
        private const int MaxCapacity = int.MaxValue;

        // Actual values inserted into the stack.
        [DataMember]
        private T[] values;

        // Number of values current in the stack.
        [DataMember]
        private int count;

        // Number of values the stack can hold.
        [DataMember]
        private int capacity;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public FastStack() : this(DefaultCapacity) { }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="capacity"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public FastStack(int capacity)
        {
            Contract.Requires(capacity > 0 && capacity <= MaxCapacity);

            this.values = new T[capacity];
            this.capacity = capacity;
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
        public T[] Values
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => this.values;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int Push()
        {
            // If out of space in the stack, then grow.
            if (this.count == this.capacity) Grow();
            return this.count++;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Clear() => this.count = 0;

        private void Grow()
        {
            // Throw exception if already at max capacity.
            if (this.capacity == MaxCapacity)
                throw new InvalidOperationException("Stack has reached maximum size");

            // Calculate new capacity to be twice as large as before.
            this.capacity <<= 1;
            if (this.capacity < 0 || this.capacity > MaxCapacity)
            {
                // Handle case of capacity going larger than MaxCapacity,
                // or overflowing (which would make it appear negative).
                this.capacity = MaxCapacity;
            }

            // Create new value array and copy values over.
            T[] oldValues = this.values;
            this.values = new T[this.capacity];
            Array.Copy(oldValues, this.values, this.count);
        }
    }
}
