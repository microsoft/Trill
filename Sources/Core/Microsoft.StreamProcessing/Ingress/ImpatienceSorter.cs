// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing.Internal
{
    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public sealed class ImpatienceSorter<TPayload> : IDisposable
    {
        [DataMember]
        private int MaxFibers = 8;
        [DataMember]
        private long[] Tails;
        [DataMember]
        private List<PooledElasticCircularBuffer<StreamEvent<TPayload>>> Fibers;
        [DataMember]
        private int NumFibers = 0;
        [DataMember]
        private PooledElasticCircularBuffer<StreamEvent<TPayload>>[] MergeSource;
        [DataMember]
        private long NextAffectingSyncTime;

        private DataStructurePool<PooledElasticCircularBuffer<StreamEvent<TPayload>>> ecbPool;
        private List<PooledElasticCircularBuffer<StreamEvent<TPayload>>> toReturn =
            new List<PooledElasticCircularBuffer<StreamEvent<TPayload>>>();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public ImpatienceSorter()
        {
            this.Tails = new long[this.MaxFibers];
            this.Fibers = new List<PooledElasticCircularBuffer<StreamEvent<TPayload>>>();
            this.MergeSource = new PooledElasticCircularBuffer<StreamEvent<TPayload>>[this.MaxFibers];
            this.NextAffectingSyncTime = StreamEvent.InfinitySyncTime;
            this.ecbPool = new DataStructurePool<PooledElasticCircularBuffer<StreamEvent<TPayload>>>();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="streamEvent"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Enqueue(ref StreamEvent<TPayload> streamEvent)
        {
            int loc;

            if (streamEvent.SyncTime >= this.Tails[0])
                loc = 0;
            else
                loc = BinarySearch(streamEvent.SyncTime);

            // Add a new queue
            if (loc == this.NumFibers)
            {
                // Double space to support more queues
                if (this.NumFibers >= this.MaxFibers)
                {
                    long[] tmp = new long[this.MaxFibers * 2];
                    Array.Copy(this.Tails, tmp, this.MaxFibers);
                    this.Tails = tmp;

                    this.MaxFibers = this.MaxFibers * 2;

                    this.MergeSource = new PooledElasticCircularBuffer<StreamEvent<TPayload>>[this.MaxFibers];
                }

                // Add new queue
                this.ecbPool.Get(out PooledElasticCircularBuffer<StreamEvent<TPayload>> ecb);
                this.Fibers.Add(ecb);
                this.NumFibers++;
            }

            this.Fibers[loc].Enqueue(ref streamEvent);
            if ((loc > 0) && (this.Fibers[loc].Count == 1))
            {
                if (streamEvent.SyncTime < this.NextAffectingSyncTime) this.NextAffectingSyncTime = streamEvent.SyncTime;
            }

            this.Tails[loc] = streamEvent.SyncTime;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="recheck"></param>
        /// <param name="timestamp"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public PooledElasticCircularBuffer<StreamEvent<TPayload>> DequeueUntil(long timestamp, out bool recheck)
        {
            recheck = false;
            if (this.NumFibers == 0) return null;

            int numMerge = 0;
            if (this.Fibers[0].Count > 0)
            {
                if (this.Fibers[0].PeekFirst().SyncTime <= timestamp)
                {
                    this.MergeSource[numMerge] = this.Fibers[0];
                    numMerge++;
                }
            }

            if (timestamp >= this.NextAffectingSyncTime)
            {
                for (int i = 1; i < this.NumFibers; i++)
                {
                    if (this.Fibers[i].Count > 0)
                    {
                        if (this.Fibers[i].PeekFirst().SyncTime <= timestamp)
                        {
                            this.MergeSource[numMerge] = this.Fibers[i];
                            numMerge++;
                        }
                    }
                }
            }

            if (numMerge == 0) return null;
            if (numMerge == 1)
            {
                recheck = true;
                return this.MergeSource[0];
            }

            PooledElasticCircularBuffer<StreamEvent<TPayload>> queue;
            while (numMerge > 2)
            {
                int i = 0;
                for (i = 0; i < (numMerge >> 1); i++)
                {
                    // merge pair
                    this.ecbPool.Get(out queue);
                    this.toReturn.Add(queue);
                    this.MergeSource[i] = Merge(this.MergeSource[2 * i], this.MergeSource[2 * i + 1], queue, timestamp);
                }

                if ((numMerge & 0x1) != 0) // odd; copy over the last merge queue
                {
                    this.MergeSource[i++] = this.MergeSource[numMerge - 1];
                }

                numMerge = i;
            }

            this.ecbPool.Get(out queue);
            this.MergeSource[0] = Merge(this.MergeSource[0], this.MergeSource[1], queue, timestamp);

            for (int i = 0; i < this.toReturn.Count; i++)
            {
                this.ecbPool.Return(this.toReturn[i]);
            }

            this.toReturn.Clear();

            // Recalculate NextAffecting
            this.NextAffectingSyncTime = StreamEvent.InfinitySyncTime;
            for (int i = 1; i < this.NumFibers; i++)
            {
                if (this.Fibers[i].Count > 0)
                {
                    var time = this.Fibers[i].PeekFirst().SyncTime;
                    if (time < this.NextAffectingSyncTime)
                    {
                        this.NextAffectingSyncTime = time;
                    }
                }
            }

            return this.MergeSource[0];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static PooledElasticCircularBuffer<StreamEvent<TPayload>> Merge(
            PooledElasticCircularBuffer<StreamEvent<TPayload>> left,
            PooledElasticCircularBuffer<StreamEvent<TPayload>> right,
            PooledElasticCircularBuffer<StreamEvent<TPayload>> result,
            long timestamp)
        {
            while (true)
            {
                if ((left.Count == 0) || (left.PeekFirst().SyncTime > timestamp))
                {
                    while ((right.Count > 0) && (right.PeekFirst().SyncTime <= timestamp))
                    {
                        var tmp = right.Dequeue();
                        result.Enqueue(ref tmp);
                    }

                    return result;
                }

                if ((right.Count == 0) || (right.PeekFirst().SyncTime > timestamp))
                {
                    while ((left.Count > 0) && (left.PeekFirst().SyncTime <= timestamp))
                    {
                        var tmp = left.Dequeue();
                        result.Enqueue(ref tmp);
                    }
                    return result;
                }

                if (left.PeekFirst().SyncTime < right.PeekFirst().SyncTime)
                {
                    var tmp = left.Dequeue();
                    result.Enqueue(ref tmp);
                }
                else
                {
                    var tmp = right.Dequeue();
                    result.Enqueue(ref tmp);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int BinarySearch(long vsync)
        {
            int imin = 0;
            int imax = this.NumFibers - 1;

            // Handle empty array
            if (this.NumFibers == 0)
                return 0;

            while (imin < imax)
            {
                int imid = imin + (imax - imin) / 2;

                if (this.Tails[imid] > vsync)
                    imin = imid + 1;
                else
                    imax = imid;
            }

            if (vsync >= this.Tails[imin])
                return imin;
            else
                return imin + 1;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="streamEvents"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Return(PooledElasticCircularBuffer<StreamEvent<TPayload>> streamEvents)
        {
            this.ecbPool.Return(streamEvents);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int Count()
        {
            return this.Fibers.Select(o => o.Count).Sum();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Dispose()
        {
            foreach (var f in this.Fibers) f.Dispose();

            for (int i = 0; i < this.MergeSource.Length; i++)
            {
                if (this.MergeSource[i] != null) this.MergeSource[i].Dispose();
            }
            this.ecbPool.Dispose();
        }
    }

    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TPayload"></typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public sealed class PartitionedImpatienceSorter<TKey, TPayload> : IDisposable
    {
        [DataMember]
        private FastDictionary<TKey, ImpatienceSorter> sorters;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public PartitionedImpatienceSorter()
        {
            this.sorters = new FastDictionary<TKey, ImpatienceSorter>();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="streamEvent"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Enqueue(ref PartitionedStreamEvent<TKey, TPayload> streamEvent)
        {
            ImpatienceSorter sorter;
            if (!this.sorters.Lookup(streamEvent.PartitionKey, out int index))
            {
                sorter = new ImpatienceSorter();
                this.sorters.Insert(ref index, streamEvent.PartitionKey, sorter);
            }
            else
            {
                sorter = this.sorters.entries[index].value;
            }

            sorter.Enqueue(ref streamEvent);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="timestamp"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public FastDictionary<TKey, Tuple<bool, PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>>>> DequeueUntil(
            long timestamp)
        {
            var partitionedStreamEvents =
                new FastDictionary<TKey, Tuple<bool, PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>>>>();
            int index = FastDictionary<TKey, ImpatienceSorter>.IteratorStart;
            while (this.sorters.Iterate(ref index))
            {
                var entry = this.sorters.entries[index];
                var streamEvents = entry.value.DequeueUntil(timestamp, out bool recheck);
                partitionedStreamEvents.Lookup(entry.key, out int insertIdx);
                partitionedStreamEvents.Insert(ref insertIdx, entry.key, Tuple.Create(recheck, streamEvents));
            }

            return partitionedStreamEvents;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="recheck"></param>
        /// <param name="timestamp"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>> DequeueUntil(
            TKey partitionKey, long timestamp, out bool recheck)
        {
            recheck = false;
            if (!this.sorters.Lookup(partitionKey, out int index))
                return null;
            var sorter = this.sorters.entries[index].value;
            return sorter.DequeueUntil(timestamp, out recheck);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="streamEvents"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Return(TKey partitionKey, PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>> streamEvents)
        {
            if (!this.sorters.Lookup(partitionKey, out int index)) return;
            var sorter = this.sorters.entries[index].value;
            sorter.Return(streamEvents);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int Count()
        {
            int count = 0;
            int index = FastDictionary<TKey, ImpatienceSorter>.IteratorStart;
            while (this.sorters.Iterate(ref index))
            {
                count += this.sorters.entries[index].value.Count();
            }

            return count;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Dispose()
        {
            int index = FastDictionary<TKey, ImpatienceSorter>.IteratorStart;
            while (this.sorters.Iterate(ref index))
            {
                this.sorters.entries[index].value.Dispose();
            }
        }
    
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        [DataContract]
        [EditorBrowsable(EditorBrowsableState.Never)]
        private sealed class ImpatienceSorter : IDisposable
        {
            [DataMember]
            private int MaxFibers = 8;
            [DataMember]
            private long[] Tails;
            [DataMember]
            private List<PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>>> Fibers;
            [DataMember]
            private int NumFibers = 0;
            [DataMember]
            private PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>>[] MergeSource;
            [DataMember]
            private long NextAffectingSyncTime;
    
            private DataStructurePool<PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>>> ecbPool;
            private List<PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>>> toReturn =
                new List<PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>>>();
    
            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public ImpatienceSorter()
            {
                this.Tails = new long[this.MaxFibers];
                this.Fibers = new List<PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>>>();
                this.MergeSource = new PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>>[this.MaxFibers];
                this.NextAffectingSyncTime = StreamEvent.InfinitySyncTime;
                this.ecbPool = new DataStructurePool<PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>>>();
            }
    
            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="streamEvent"></param>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public void Enqueue(ref PartitionedStreamEvent<TKey, TPayload> streamEvent)
            {
                int loc;
    
                if (streamEvent.SyncTime >= this.Tails[0])
                    loc = 0;
                else
                    loc = BinarySearch(streamEvent.SyncTime);
    
                // Add a new queue
                if (loc == this.NumFibers)
                {
                    // Double space to support more queues
                    if (this.NumFibers >= this.MaxFibers)
                    {
                        long[] tmp = new long[this.MaxFibers * 2];
                        Array.Copy(this.Tails, tmp, this.MaxFibers);
                        this.Tails = tmp;
    
                        this.MaxFibers = this.MaxFibers * 2;
    
                        this.MergeSource = new PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>>[this.MaxFibers];
                    }
    
                    // Add new queue
                    this.ecbPool.Get(out PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>> ecb);
                    this.Fibers.Add(ecb);
                    this.NumFibers++;
                }
    
                this.Fibers[loc].Enqueue(ref streamEvent);
                if ((loc > 0) && (this.Fibers[loc].Count == 1))
                {
                    if (streamEvent.SyncTime < this.NextAffectingSyncTime) this.NextAffectingSyncTime = streamEvent.SyncTime;
                }
    
                this.Tails[loc] = streamEvent.SyncTime;
            }
    
            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="recheck"></param>
            /// <param name="timestamp"></param>
            /// <returns></returns>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>> DequeueUntil(long timestamp, out bool recheck)
            {
                recheck = false;
                if (this.NumFibers == 0) return null;
    
                int numMerge = 0;
                if (this.Fibers[0].Count > 0)
                {
                    if (this.Fibers[0].PeekFirst().SyncTime <= timestamp)
                    {
                        this.MergeSource[numMerge] = this.Fibers[0];
                        numMerge++;
                    }
                }
    
                if (timestamp >= this.NextAffectingSyncTime)
                {
                    for (int i = 1; i < this.NumFibers; i++)
                    {
                        if (this.Fibers[i].Count > 0)
                        {
                            if (this.Fibers[i].PeekFirst().SyncTime <= timestamp)
                            {
                                this.MergeSource[numMerge] = this.Fibers[i];
                                numMerge++;
                            }
                        }
                    }
                }
    
                if (numMerge == 0) return null;
                if (numMerge == 1)
                {
                    recheck = true;
                    return this.MergeSource[0];
                }
    
                PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>> queue;
                while (numMerge > 2)
                {
                    int i = 0;
                    for (i = 0; i < (numMerge >> 1); i++)
                    {
                        // merge pair
                        this.ecbPool.Get(out queue);
                        this.toReturn.Add(queue);
                        this.MergeSource[i] = Merge(this.MergeSource[2 * i], this.MergeSource[2 * i + 1], queue, timestamp);
                    }
    
                    if ((numMerge & 0x1) != 0) // odd; copy over the last merge queue
                    {
                        this.MergeSource[i++] = this.MergeSource[numMerge - 1];
                    }
    
                    numMerge = i;
                }
    
                this.ecbPool.Get(out queue);
                this.MergeSource[0] = Merge(this.MergeSource[0], this.MergeSource[1], queue, timestamp);
    
                for (int i = 0; i < this.toReturn.Count; i++)
                {
                    this.ecbPool.Return(this.toReturn[i]);
                }
    
                this.toReturn.Clear();
    
                // Recalculate NextAffecting
                this.NextAffectingSyncTime = StreamEvent.InfinitySyncTime;
                for (int i = 1; i < this.NumFibers; i++)
                {
                    if (this.Fibers[i].Count > 0)
                    {
                        var time = this.Fibers[i].PeekFirst().SyncTime;
                        if (time < this.NextAffectingSyncTime)
                        {
                            this.NextAffectingSyncTime = time;
                        }
                    }
                }
    
                return this.MergeSource[0];
            }
    
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>> Merge(
                PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>> left,
                PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>> right,
                PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>> result,
                long timestamp)
            {
                while (true)
                {
                    if ((left.Count == 0) || (left.PeekFirst().SyncTime > timestamp))
                    {
                        while ((right.Count > 0) && (right.PeekFirst().SyncTime <= timestamp))
                        {
                            var tmp = right.Dequeue();
                            result.Enqueue(ref tmp);
                        }
    
                        return result;
                    }
    
                    if ((right.Count == 0) || (right.PeekFirst().SyncTime > timestamp))
                    {
                        while ((left.Count > 0) && (left.PeekFirst().SyncTime <= timestamp))
                        {
                            var tmp = left.Dequeue();
                            result.Enqueue(ref tmp);
                        }
                        return result;
                    }
    
                    if (left.PeekFirst().SyncTime < right.PeekFirst().SyncTime)
                    {
                        var tmp = left.Dequeue();
                        result.Enqueue(ref tmp);
                    }
                    else
                    {
                        var tmp = right.Dequeue();
                        result.Enqueue(ref tmp);
                    }
                }
            }
    
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private int BinarySearch(long vsync)
            {
                int imin = 0;
                int imax = this.NumFibers - 1;
    
                // Handle empty array
                if (this.NumFibers == 0)
                    return 0;
    
                while (imin < imax)
                {
                    int imid = imin + (imax - imin) / 2;
    
                    if (this.Tails[imid] > vsync)
                        imin = imid + 1;
                    else
                        imax = imid;
                }
    
                if (vsync >= this.Tails[imin])
                    return imin;
                else
                    return imin + 1;
            }
    
            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="streamEvents"></param>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public void Return(PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>> streamEvents)
            {
                this.ecbPool.Return(streamEvents);
            }
    
            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <returns></returns>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public int Count()
            {
                return this.Fibers.Select(o => o.Count).Sum();
            }
    
            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public void Dispose()
            {
                foreach (var f in this.Fibers) f.Dispose();

                for (int i = 0; i < this.MergeSource.Length; i++)
                {
                    if (this.MergeSource[i] != null) this.MergeSource[i].Dispose();
                }

                this.ecbPool.Dispose();
            }
        }
    }
}
