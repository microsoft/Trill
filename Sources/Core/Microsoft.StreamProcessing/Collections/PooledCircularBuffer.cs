// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing.Internal.Collections
{
    [DataContract]
    internal sealed class PooledCircularBuffer<T>
    {
        [DataMember]
        public int DefaultCapacity;
        [DataMember]
        public ColumnBatch<T> Items;
        [DataMember]
        public int head = 0;
        [DataMember]
        public int tail = 0;

        public PooledCircularBuffer(int capacity, ColumnPool<T> pool)
        {
            this.DefaultCapacity = capacity;
            pool.Get(out this.Items);
            this.Items.UsedLength = capacity + 1;
        }

        public T PeekFirst() => this.Items.col[this.head];

        public T PeekLast() => this.Items.col[(this.tail - 1) & this.DefaultCapacity];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Enqueue(ref T value)
        {
            int next = (this.tail + 1) & this.DefaultCapacity;
            if (next == this.head) throw new InvalidOperationException("The list is full!");
            this.Items.col[this.tail] = value;
            this.tail = next;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Dequeue()
        {
            if (this.head == this.tail) throw new InvalidOperationException("The list is empty!");
            int oldhead = this.head;
            this.head = (this.head + 1) & this.DefaultCapacity;
            var ret = this.Items.col[oldhead];
            this.Items.col[oldhead] = default;
            return ret;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsFull() => ((this.tail + 1) & this.DefaultCapacity) == this.head;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsEmpty() => this.head == this.tail;

        public IEnumerable<T> Iterate()
        {
            int i = this.head;
            while (i != this.tail)
            {
                yield return this.Items.col[i];
                i = (i + 1) & this.DefaultCapacity;
            }
        }

        public void Return()
        {
            if (this.Items != null)
            {
                this.Items.UsedLength = 0;
                this.Items.Return();
                this.Items = null;
            }
        }
    }

    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public sealed class PooledElasticCircularBuffer<T> : IEnumerable<T>, IDisposable
    {
        private const int Capacity = 0xff;
        private readonly LinkedList<PooledCircularBuffer<T>> buffers = new LinkedList<PooledCircularBuffer<T>>();
        private LinkedListNode<PooledCircularBuffer<T>> head;
        private LinkedListNode<PooledCircularBuffer<T>> tail;
        private readonly ColumnPool<T> pool;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public PooledElasticCircularBuffer()
        {
            this.pool = MemoryManager.GetColumnPool<T>(Capacity + 1);
            var node = new LinkedListNode<PooledCircularBuffer<T>>(new PooledCircularBuffer<T>(Capacity, this.pool));
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
                    next = new LinkedListNode<PooledCircularBuffer<T>>(new PooledCircularBuffer<T>(Capacity, this.pool));
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
            if (this.head.Value.IsEmpty()) throw new InvalidOperationException("The list is empty!");
            this.Count--;
            var toReturn = this.head.Value.Dequeue();
            if (this.head.Value.IsEmpty() && this.head != this.tail)
            {
                var oldHead = this.head;
                this.head = this.head.Next;
                oldHead.Value.Return();
                this.buffers.Remove(oldHead);
            }
            return toReturn;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool TryDequeue(out T value)
        {
            if (this.head.Value.IsEmpty())
            {
                value = default;
                return false;
            }

            this.Count--;
            value = this.head.Value.Dequeue();
            if (this.head.Value.IsEmpty() && this.head != this.tail)
            {
                var oldHead = this.head;
                this.head = this.head.Next;
                oldHead.Value.Return();
                this.buffers.Remove(oldHead);
                if (this.head == null) this.head = this.buffers.First;
            }
            return true;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T PeekFirst()
        {
            if (this.head.Value.IsEmpty()) throw new InvalidOperationException("The list is empty!");
            return this.head.Value.PeekFirst();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool TryPeekFirst(out T value)
        {
            if (this.head.Value.IsEmpty())
            {
                value = default;
                return false;
            }
            value = this.head.Value.PeekFirst();
            return true;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T PeekLast()
        {
            if (this.tail.Value.IsEmpty()) throw new InvalidOperationException("The list is empty!");
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

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        public void Dispose()
        {
            foreach (var b in this.buffers) b.Return();
        }
    }
}
