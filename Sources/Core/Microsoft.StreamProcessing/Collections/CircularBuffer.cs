// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public sealed class CircularBuffer<T>
    {
        [DataMember]
        private int capacityMask = 0xfff;
        [DataMember]
        internal T[] Items;
        [DataMember]
        internal int head = 0;
        [DataMember]
        internal int tail = 0;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public CircularBuffer() => this.Items = new T[this.capacityMask + 1];

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="capacity"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public CircularBuffer(int capacity)
        {
            // Adjust capacity to nearest power of 2, less 1, with a minimum of 8
            var temp = 8;
            while (temp <= capacity) temp <<= 1;

            this.Items = new T[temp];
            this.capacityMask = temp - 1;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T PeekFirst() => this.Items[this.head];

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T PeekLast() => this.Items[(this.tail - 1) & this.capacityMask];

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="value"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Enqueue(ref T value)
        {
            int next = (this.tail + 1) & this.capacityMask;
            if (next == this.head) throw new InvalidOperationException("The list is full!");
            this.Items[this.tail] = value;
            this.tail = next;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T Dequeue()
        {
            if (this.head == this.tail) throw new InvalidOperationException("The list is empty!");
            int oldhead = this.head;
            this.head = (this.head + 1) & this.capacityMask;
            var ret = this.Items[oldhead];
            this.Items[oldhead] = default;
            return ret;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool IsFull() => ((this.tail + 1) & this.capacityMask) == this.head;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool IsEmpty() => this.head == this.tail;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public IEnumerable<T> Iterate()
        {
            int i = this.head;
            while (i != this.tail)
            {
                yield return this.Items[i];
                i = (i + 1) & this.capacityMask;
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int Count => (this.tail >= this.head) ? (this.tail - this.head) : (this.tail - this.head + this.capacityMask + 1);
    }

    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public sealed class ElasticCircularBuffer<T> : IEnumerable<T>
    {
        private readonly LinkedList<CircularBuffer<T>> buffers = new LinkedList<CircularBuffer<T>>();
        private LinkedListNode<CircularBuffer<T>> head;
        private LinkedListNode<CircularBuffer<T>> tail;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public ElasticCircularBuffer()
        {
            var node = new LinkedListNode<CircularBuffer<T>>(new CircularBuffer<T>());
            this.buffers.AddFirst(node);
            this.tail = this.head = node;
            this.Count = 0;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="value"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Enqueue(ref T value)
        {
            if (this.tail.Value.IsFull())
            {
                var next = this.tail.Next;
                if (next == null) next = this.buffers.First;
                if (!next.Value.IsEmpty())
                {
                    next = new LinkedListNode<CircularBuffer<T>>(new CircularBuffer<T>());
                    this.buffers.AddAfter(this.tail, next);
                }

                this.tail = next;
            }

            this.tail.Value.Enqueue(ref value);
            this.Count++;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="value"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Add(T value) => Enqueue(ref value);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T Dequeue()
        {
            if (this.head.Value.IsEmpty())
            {
                if (this.head == this.tail)
                    throw new InvalidOperationException("The list is empty!");

                this.head = this.head.Next;
                if (this.head == null) this.head = this.buffers.First;
            }

            this.Count--;
            return this.head.Value.Dequeue();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T PeekFirst()
        {
            if (this.head.Value.head == this.head.Value.tail)
            {
                if (this.head == this.tail)
                    throw new InvalidOperationException("The list is empty!");

                this.head = this.head.Next;
                if (this.head == null) this.head = this.buffers.First;
            }
            return this.head.Value.Items[this.head.Value.head];
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T PeekLast()
        {
            if (this.tail.Value.IsEmpty())
                throw new InvalidOperationException("The list is empty!");
            return this.tail.Value.PeekLast();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool IsEmpty() => this.head.Value.IsEmpty() && (this.head == this.tail);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int Count { get; private set; }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public IEnumerator<T> GetEnumerator()
        {
            foreach (var buffer in this.buffers)
            {
                foreach (var item in buffer.Iterate()) yield return item;
            }
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
