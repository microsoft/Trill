// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// A priority queue structure for sorting mostly in-order data.
    /// </summary>
    /// <typeparam name="T">The element type of the priority queue.</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public class PriorityQueue<T> : IEnumerable<T>
    {
        [DataMember]
        private List<T> data = new List<T>();

        private readonly IComparer<T> comp;

        /// <summary>
        /// Create an instance of a priority queue with the default comparer for the underlying item type.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public PriorityQueue() : this(Comparer<T>.Default) { }

        /// <summary>
        /// Create an instance of a priority queue with the specified comparer for the underlying item type.
        /// </summary>
        /// <param name="comp">The comparer to be used on elements added to the queue.</param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public PriorityQueue(IComparer<T> comp) => this.comp = comp;

        /// <summary>
        /// Add a new item to the priority queue.
        /// </summary>
        /// <param name="item">The item to add to the priority queue.</param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Enqueue(T item)
        {
            this.data.Add(item);
            int ci = this.data.Count - 1; // child index; start at end
            while (ci > 0)
            {
                int pi = (ci - 1) / 2; // parent index
                if (this.comp.Compare(this.data[ci], this.data[pi]) >= 0) break; // child item is larger than (or equal) parent so we're done
                T tmp = this.data[ci];
                this.data[ci] = this.data[pi];
                this.data[pi] = tmp;
                ci = pi;
            }
        }

        /// <summary>
        /// Determines whether the priority queue is empty.
        /// </summary>
        /// <returns>True if the priority queue is empty.</returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool IsEmpty() => this.data.Count == 0;

        /// <summary>
        /// Dequeue an element from the priority queue.
        /// </summary>
        /// <returns>The element removed from the queue.</returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T Dequeue()
        {
            // assumes pq is not empty; up to calling code
            int li = this.data.Count - 1; // last index (before removal)
            var frontItem = this.data[0];   // fetch the front
            this.data[0] = this.data[li];
            this.data.RemoveAt(li);

            --li; // last index (after removal)
            int pi = 0; // parent index. start at front of pq
            while (true)
            {
                int ci = pi * 2 + 1; // left child index of parent
                if (ci > li) break;  // no children so done
                int rc = ci + 1;     // right child
                if (rc <= li && this.comp.Compare(this.data[rc], this.data[ci]) < 0) // if there is a rc (ci + 1), and it is smaller than left child, use the rc instead
                    ci = rc;
                if (this.comp.Compare(this.data[pi], this.data[ci]) <= 0) break; // parent is smaller than (or equal to) smallest child so done
                var tmp = this.data[pi];
                this.data[pi] = this.data[ci];
                this.data[ci] = tmp; // swap parent and child
                pi = ci;
            }

            // shrink list if needed
            if ((this.data.Count << 3) < this.data.Capacity) this.data.Capacity >>= 1;

            return frontItem;
        }

        /// <summary>
        /// Looks at the next item that would be dequeued.
        /// </summary>
        /// <returns>The item next to be dequeued.</returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T Peek() => this.data[0];

        /// <summary>
        /// Returns the number of elements in the priority queue.
        /// </summary>
        /// <returns>The number of elements in the priority queue.</returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int Count() => this.data.Count;

        /// <summary>
        /// Provides a textual representation of the priority queue.
        /// </summary>
        /// <returns>A textual representation of the priority queue.</returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override string ToString()
        {
            string s = string.Empty;
            for (int i = 0; i < this.data.Count; ++i)
                s += this.data[i].ToString() + " ";
            s += "count = " + this.data.Count;
            return s;
        }

        /// <summary>
        /// Returns an enumerator that loops over the items in the priority queue.
        /// </summary>
        /// <returns>Enumerator that loops over the items in the priority queue.</returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public IEnumerator<T> GetEnumerator() => this.data.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => this.data.GetEnumerator();
    }

    /// <summary>
    /// A priority queue structure for sorting mostly in-order data, where each item in the queue is a stream event.
    /// </summary>
    /// <typeparam name="T">The element type of the stream events of the priority queue.</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public sealed class StreamEventPriorityQueue<T> : PriorityQueue<StreamEvent<T>>
    {
        /// <summary>
        /// Create an instance of a priority queue with the default comparer for stream events.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public StreamEventPriorityQueue() : base(new StreamEventSyncTimeComparer<T>()) { }
    }

    /// <summary>
    /// A priority queue structure for sorting mostly in-order data, where each item in the queue is a partitioned stream event.
    /// </summary>
    /// <typeparam name="K">The key type of the stream events of the priority queue.</typeparam>
    /// <typeparam name="P">The element type of the stream events of the priority queue.</typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public sealed class PartitionedStreamEventPriorityQueue<K, P> : PriorityQueue<PartitionedStreamEvent<K, P>>
    {
        /// <summary>
        /// Create an instance of a priority queue with the default comparer for partitioned stream events.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public PartitionedStreamEventPriorityQueue() : base(new PartitionedStreamEventSyncTimeComparer<K, P>()) { }
    }
}
