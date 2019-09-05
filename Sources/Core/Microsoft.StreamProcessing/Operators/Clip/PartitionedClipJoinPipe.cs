// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class PartitionedClipJoinPipe<TKey, TLeft, TRight, TPartitionKey> : BinaryPipe<TKey, TLeft, TRight, TLeft>
    {
        private readonly MemoryPool<TKey, TLeft> pool;
        private readonly Func<TKey, TPartitionKey> getPartitionKey = GetPartitionExtractor<TPartitionKey, TKey>();

        [SchemaSerialization]
        private readonly Expression<Func<TKey, TKey, bool>> keyComparer;
        private readonly Func<TKey, TKey, bool> keyComparerEquals;
        [SchemaSerialization]
        private readonly Expression<Func<TLeft, TLeft, bool>> leftComparer;
        private readonly Func<TLeft, TLeft, bool> leftComparerEquals;

        [DataMember]
        private FastDictionary2<TPartitionKey, Queue<LEntry>> leftQueue = new FastDictionary2<TPartitionKey, Queue<LEntry>>();
        [DataMember]
        private FastDictionary2<TPartitionKey, Queue<REntry>> rightQueue = new FastDictionary2<TPartitionKey, Queue<REntry>>();
        [DataMember]
        private HashSet<TPartitionKey> processQueue = new HashSet<TPartitionKey>();
        [DataMember]
        private HashSet<TPartitionKey> seenKeys = new HashSet<TPartitionKey>();
        [DataMember]
        private HashSet<TPartitionKey> cleanKeys = new HashSet<TPartitionKey>();

        [DataMember]
        private StreamMessage<TKey, TLeft> output;
        [DataMember]
        private FastDictionary<TPartitionKey, PartitionEntry> partitionData = new FastDictionary<TPartitionKey, PartitionEntry>();
        [DataMember]
        private long lastLeftCTI = long.MinValue;
        [DataMember]
        private long lastRightCTI = long.MinValue;
        [DataMember]
        private bool emitCTI = false;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public PartitionedClipJoinPipe() { }

        public PartitionedClipJoinPipe(ClipJoinStreamable<TKey, TLeft, TRight> stream, IStreamObserver<TKey, TLeft> observer)
            : base(stream, observer)
        {
            this.keyComparer = stream.Properties.KeyEqualityComparer.GetEqualsExpr();
            this.keyComparerEquals = this.keyComparer.Compile();

            this.leftComparer = stream.LeftComparer.GetEqualsExpr();
            this.leftComparerEquals = this.leftComparer.Compile();

            this.pool = MemoryManager.GetMemoryPool<TKey, TLeft>(stream.Properties.IsColumnar);
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        protected override void ProduceBinaryQueryPlan(PlanNode left, PlanNode right)
        {
            var node = new JoinPlanNode(
                left, right, this,
                typeof(TLeft), typeof(TRight), typeof(TLeft), typeof(TKey), JoinKind.Clip, false, null);
            node.AddJoinExpression("key comparer", this.keyComparer);
            node.AddJoinExpression("left key comparer", this.leftComparer);
            this.Observer.ProduceQueryPlan(node);
        }

        private void NewPartition(TPartitionKey pKey)
        {
            this.leftQueue.Insert(pKey, new Queue<LEntry>());
            this.rightQueue.Insert(pKey, new Queue<REntry>());

            if (!this.partitionData.Lookup(pKey, out int eph)) this.partitionData.Insert(ref eph, pKey, new PartitionEntry());
        }

        protected override void DisposeState() => this.output.Free();

        protected override void ProcessBothBatches(StreamMessage<TKey, TLeft> leftBatch, StreamMessage<TKey, TRight> rightBatch, out bool leftBatchDone, out bool rightBatchDone, out bool leftBatchFree, out bool rightBatchFree)
        {
            ProcessLeftBatch(leftBatch, out leftBatchDone, out leftBatchFree);
            ProcessRightBatch(rightBatch, out rightBatchDone, out rightBatchFree);
        }

        protected override void ProcessLeftBatch(StreamMessage<TKey, TLeft> batch, out bool leftBatchDone, out bool leftBatchFree)
        {
            leftBatchDone = true;
            leftBatchFree = true;
            batch.iter = 0;

            Queue<LEntry> queue = null;
            TPartitionKey previous = default;
            bool first = true;
            for (var i = 0; i < batch.Count; i++)
            {
                if ((batch.bitvector.col[i >> 6] & (1L << (i & 0x3f))) != 0
                    && (batch.vother.col[i] >= 0)) continue;

                if (batch.vother.col[i] == PartitionedStreamEvent.LowWatermarkOtherTime)
                {
                    bool timeAdvanced = this.lastLeftCTI != batch.vsync.col[i];
                    if (timeAdvanced && this.lastLeftCTI < this.lastRightCTI) this.emitCTI = true;
                    this.lastLeftCTI = batch.vsync.col[i];
                    foreach (var p in this.seenKeys) this.processQueue.Add(p);
                    continue;
                }

                var partitionKey = this.getPartitionKey(batch.key.col[i]);
                if (first || !partitionKey.Equals(previous))
                {
                    if (this.seenKeys.Add(partitionKey)) NewPartition(partitionKey);
                    this.leftQueue.Lookup(partitionKey, out int index);
                    queue = this.leftQueue.entries[index].value;
                    this.processQueue.Add(partitionKey);
                }
                var e = new LEntry
                {
                    Key = batch.key.col[i],
                    Sync = batch.vsync.col[i],
                    Other = batch.vother.col[i],
                    Payload = batch.payload.col[i],
                    Hash = batch.hash.col[i],
                };
                queue.Enqueue(e);
                first = false;
                previous = partitionKey;
            }
            ProcessPendingEntries();
        }

        protected override void ProcessRightBatch(StreamMessage<TKey, TRight> batch, out bool rightBatchDone, out bool rightBatchFree)
        {
            rightBatchDone = true;
            rightBatchFree = true;
            batch.iter = 0;
            Queue<REntry> queue = null;
            TPartitionKey previous = default;
            bool first = true;
            for (var i = 0; i < batch.Count; i++)
            {
                if ((batch.bitvector.col[i >> 6] & (1L << (i & 0x3f))) != 0
                    && (batch.vother.col[i] >= 0)) continue;

                if (batch.vother.col[i] == PartitionedStreamEvent.LowWatermarkOtherTime)
                {
                    bool timeAdvanced = this.lastRightCTI != batch.vsync.col[i];
                    if (timeAdvanced && this.lastRightCTI < this.lastLeftCTI) this.emitCTI = true;
                    this.lastRightCTI = batch.vsync.col[i];
                    foreach (var p in this.seenKeys) this.processQueue.Add(p);
                    continue;
                }

                var partitionKey = this.getPartitionKey(batch.key.col[i]);
                if (first || !partitionKey.Equals(previous))
                {
                    if (this.seenKeys.Add(partitionKey)) NewPartition(partitionKey);
                    this.rightQueue.Lookup(partitionKey, out int index);
                    queue = this.rightQueue.entries[index].value;
                    this.processQueue.Add(partitionKey);
                }
                var e = new REntry
                {
                    Key = batch.key.col[i],
                    Sync = batch.vsync.col[i],
                    Other = batch.vother.col[i],
                    Hash = batch.hash.col[i],
                };
                queue.Enqueue(e);
                first = false;
                previous = partitionKey;
            }
            ProcessPendingEntries();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessPendingEntries()
        {
            foreach (var pKey in this.processQueue)
            {
                // Partition is no longer clean if we are processing it. If it is still clean, it will be added below.
                this.cleanKeys.Remove(pKey);

                Queue<LEntry> leftWorking = null;
                Queue<REntry> rightWorking = null;

                this.leftQueue.Lookup(pKey, out int index);
                leftWorking = this.leftQueue.entries[index].value;
                rightWorking = this.rightQueue.entries[index].value;
                this.partitionData.Lookup(pKey, out index);
                var partition = this.partitionData.entries[index].value;

                while (true)
                {
                    LEntry leftEntry;
                    REntry rightEntry;
                    var old = partition.currTime;
                    bool hasLeftBatch = leftWorking.Count != 0;
                    bool hasRightBatch = rightWorking.Count != 0;
                    if (hasLeftBatch && hasRightBatch)
                    {
                        leftEntry = leftWorking.Peek();
                        rightEntry = rightWorking.Peek();
                        partition.nextLeftTime = leftEntry.Sync;
                        partition.nextRightTime = rightEntry.Sync;

                        if (partition.nextLeftTime <= partition.nextRightTime)
                        {
                            UpdateTime(partition, partition.nextLeftTime);

                            if (leftEntry.Other != long.MinValue)
                            {
                                ProcessLeftEvent(
                                    partition,
                                    partition.nextLeftTime,
                                    leftEntry.Other,
                                    ref leftEntry.Key,
                                    leftEntry.Payload,
                                    leftEntry.Hash);

                            }
                            else if (partition.currTime > old)
                            {
                                AddToBatch(
                                    partition.currTime,
                                    long.MinValue,
                                    ref leftEntry.Key,
                                    ref leftEntry.Payload,
                                    leftEntry.Hash);
                            }

                            leftWorking.Dequeue();
                        }
                        else
                        {
                            UpdateTime(partition, partition.nextRightTime);

                            if (rightEntry.Other != long.MinValue)
                            {
                                ProcessRightEvent(
                                    partition,
                                    partition.nextRightTime,
                                    rightEntry.Other,
                                    ref rightEntry.Key,
                                    rightEntry.Hash);

                            }
                            else if (partition.currTime > old)
                            {
                                var l = default(TLeft);
                                AddToBatch(
                                    partition.currTime,
                                    long.MinValue,
                                    ref rightEntry.Key,
                                    ref l,
                                    rightEntry.Hash);
                            }

                            rightWorking.Dequeue();
                        }
                    }
                    else if (hasLeftBatch)
                    {
                        leftEntry = leftWorking.Peek();
                        partition.nextLeftTime = leftEntry.Sync;
                        partition.nextRightTime = Math.Max(partition.nextRightTime, this.lastRightCTI);
                        if (partition.nextLeftTime > partition.nextRightTime)
                        {
                            // If we have not yet reached the lesser of the two sides (in this case, right), and we don't
                            // have input from that side, reach that time now. This can happen with low watermarks.
                            if (partition.currTime < partition.nextRightTime)
                                UpdateTime(partition, partition.nextRightTime);
                            break;
                        }

                        UpdateTime(partition, partition.nextLeftTime);

                        if (leftEntry.Other != long.MinValue)
                        {
                            ProcessLeftEvent(
                                partition,
                                partition.nextLeftTime,
                                leftEntry.Other,
                                ref leftEntry.Key,
                                leftEntry.Payload,
                                leftEntry.Hash);
                        }
                        else if (partition.currTime > old)
                        {
                            AddToBatch(
                                partition.currTime,
                                long.MinValue,
                                ref leftEntry.Key,
                                ref leftEntry.Payload,
                                leftEntry.Hash);
                        }

                        leftWorking.Dequeue();
                    }
                    else if (hasRightBatch)
                    {
                        rightEntry = rightWorking.Peek();
                        partition.nextRightTime = rightEntry.Sync;
                        partition.nextLeftTime = Math.Max(partition.nextLeftTime, this.lastLeftCTI);
                        if (partition.nextLeftTime < partition.nextRightTime)
                        {
                            // If we have not yet reached the lesser of the two sides (in this case, left), and we don't
                            // have input from that side, reach that time now. This can happen with low watermarks.
                            if (partition.currTime < partition.nextLeftTime)
                                UpdateTime(partition, partition.nextLeftTime);
                            break;
                        }

                        UpdateTime(partition, partition.nextRightTime);

                        if (rightEntry.Other != long.MinValue)
                        {
                            ProcessRightEvent(
                                partition,
                                partition.nextRightTime,
                                rightEntry.Other,
                                ref rightEntry.Key,
                                rightEntry.Hash);
                        }
                        else if (partition.currTime > old)
                        {
                            var l = default(TLeft);
                            AddToBatch(
                                partition.currTime,
                                long.MinValue,
                                ref rightEntry.Key,
                                ref l,
                                rightEntry.Hash);
                        }

                        rightWorking.Dequeue();
                    }
                    else
                    {
                        if (partition.nextLeftTime < this.lastLeftCTI)
                            partition.nextLeftTime = this.lastLeftCTI;
                        if (partition.nextRightTime < this.lastRightCTI)
                            partition.nextRightTime = this.lastRightCTI;

                        UpdateTime(partition, Math.Min(this.lastLeftCTI, this.lastRightCTI));
                        if (partition.IsClean()) this.cleanKeys.Add(pKey);

                        break;
                    }
                }
            }

            if (this.emitCTI)
            {
                var earliest = Math.Min(this.lastLeftCTI, this.lastRightCTI);
                AddLowWatermarkToBatch(earliest);
                this.emitCTI = false;
                foreach (var p in this.cleanKeys)
                {
                    this.seenKeys.Remove(p);
                    this.leftQueue.Remove(p);
                    this.rightQueue.Remove(p);
                }

                this.cleanKeys.Clear();
            }

            this.processQueue.Clear();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateTime(PartitionEntry partition, long time)
        {
            if (time > partition.currTime)
            {
                partition.currTime = time;
                ReachTime(partition);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessLeftEvent(PartitionEntry partition, long start, long end, ref TKey key, TLeft payload, int hash)
        {
            var lim = partition.leftIntervalMap;
            var lem = partition.leftEdgeMap;
            if (start < end)
            {
                // Row is a start edge or interval.
                var leph = partition.leftEndPointHeap;
                bool isInterval = end < StreamEvent.InfinitySyncTime;
                if (isInterval)
                {
                    bool isFullyOutputtable = partition.nextRightTime >= end;
                    if (isFullyOutputtable)
                    {
                        // Output full interval.
                        AddToBatch(start, end, ref key, ref payload, hash);
                    }
                    else
                    {
                        // Insert into map to remember interval.
                        int mapIndex = lim.Insert(hash);

                        // Insert into heap to schedule removal at endpoint.
                        int heapIndex = leph.Insert(end, mapIndex);

                        // Set value in map, also remembering heap's index.
                        lim.Values[mapIndex].Initialize(start, ref key, ref payload, heapIndex);

                        // Output start edge.
                        AddToBatch(start, StreamEvent.InfinitySyncTime, ref key, ref payload, hash);
                    }
                }
                else
                {
                    int index = lem.Insert(hash);
                    lem.Values[index].Populate(start, ref key, ref payload);

                    // Output start edge.
                    AddToBatch(start, StreamEvent.InfinitySyncTime, ref key, ref payload, hash);
                }
            }
            else
            {
                // Row is an end edge.
                var leftEvents = lem.Find(hash);
                while (leftEvents.Next(out int index))
                {
                    if (AreSame(end, ref key, ref payload, ref lem.Values[index]))
                    {
                        // Output end edge.
                        AddToBatch(start, end, ref key, ref payload, hash);

                        // Remove from leftMap.
                        leftEvents.Remove();
                        break;
                    }
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessRightEvent(PartitionEntry partition, long start, long end, ref TKey key, int hash)
        {
            if (start >= end)
            {
                // Row is an end edge, which we don't care about because the start edge would have already
                // removed all joining left events.
                return;
            }

            // Mark any matching left intervals as no longer active.
            var lim = partition.leftIntervalMap;
            var lem = partition.leftEdgeMap;
            var leph = partition.leftEndPointHeap;
            var leftIntervals = lim.Find(hash);
            while (leftIntervals.Next(out int index))
            {
                long leftStart = lim.Values[index].Start;
                if (leftStart < start && this.keyComparerEquals(key, lim.Values[index].Key))
                {
                    // Output end edge.
                    AddToBatch(
                        start,
                        leftStart,
                        ref lim.Values[index].Key,
                        ref lim.Values[index].Payload,
                        hash);

                    // Remove from heap and map.
                    leph.Remove(lim.Values[index].HeapIndex);
                    leftIntervals.Remove();
                }
            }

            // Remove any matching left edges.
            var leftEdges = lem.Find(hash);
            while (leftEdges.Next(out int index))
            {
                long leftStart = lem.Values[index].Start;
                if (leftStart < start && this.keyComparerEquals(key, lem.Values[index].Key))
                {
                    // Output end edge.
                    AddToBatch(
                        start,
                        leftStart,
                        ref lem.Values[index].Key,
                        ref lem.Values[index].Payload,
                        hash);

                    // Remove left event.
                    leftEdges.Remove();
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReachTime(PartitionEntry partition)
        {
            // Carry-out all interval endpoints for left intervals that end prior or at new current time.
            var lim = partition.leftIntervalMap;
            var leph = partition.leftEndPointHeap;
            while (leph.TryGetNextInclusive(partition.currTime, out long endPointTime, out int index))
            {
                // Output end edge.
                AddToBatch(
                    endPointTime,
                    lim.Values[index].Start,
                    ref lim.Values[index].Key,
                    ref lim.Values[index].Payload,
                    lim.GetHash(index));

                // Remove from leftMap.
                lim.Remove(index);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AddLowWatermarkToBatch(long start)
        {
            if (start > this.lastCTI)
            {
                this.lastCTI = start;

                int index = this.output.Count++;
                this.output.vsync.col[index] = start;
                this.output.vother.col[index] = PartitionedStreamEvent.LowWatermarkOtherTime;
                this.output.key.col[index] = default;
                this.output[index] = default;
                this.output.hash.col[index] = 0;
                this.output.bitvector.col[index >> 6] |= 1L << (index & 0x3f);

                if (this.output.Count == Config.DataBatchSize) FlushContents();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AddToBatch(long start, long end, ref TKey key, ref TLeft payload, int hash)
        {
            if (start < this.lastCTI)
            {
                throw new StreamProcessingOutOfOrderException("Outputting an event out of order!");
            }

            int index = this.output.Count++;
            this.output.vsync.col[index] = start;
            this.output.vother.col[index] = end;
            this.output.key.col[index] = key;
            this.output[index] = payload;
            this.output.hash.col[index] = hash;
            if (end == long.MinValue)
            {
                this.output.bitvector.col[index >> 6] |= (1L << (index & 0x3f));
            }

            if (this.output.Count == Config.DataBatchSize) FlushContents();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool AreSame(long start, ref TKey key, ref TLeft payload, ref LeftEdge active)
            => start == active.Start && this.keyComparerEquals(key, active.Key) && this.leftComparerEquals(payload, active.Payload);

        protected override void FlushContents()
        {
            if (this.output.Count == 0) return;
            this.Observer.OnNext(this.output);
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        public override int CurrentlyBufferedOutputCount => this.output.Count;

        public override int CurrentlyBufferedLeftInputCount
        {
            get
            {
                int count = base.CurrentlyBufferedLeftInputCount;
                int key = FastDictionary2<TPartitionKey, Queue<LEntry>>.IteratorStart;
                while (this.leftQueue.Iterate(ref key)) count += this.leftQueue.entries[key].value.Count;
                int iter = FastDictionary<TPartitionKey, PartitionEntry>.IteratorStart;
                while (this.partitionData.Iterate(ref iter))
                {
                    var partition = this.partitionData.entries[iter].value;
                    count += partition.leftEdgeMap.Count;
                    count += partition.leftIntervalMap.Count;
                }
                return count;
            }
        }

        public override int CurrentlyBufferedRightInputCount
        {
            get
            {
                var count = base.CurrentlyBufferedRightInputCount;
                int key = FastDictionary2<TPartitionKey, Queue<REntry>>.IteratorStart;
                while (this.rightQueue.Iterate(ref key)) count += this.rightQueue.entries[key].value.Count;
                return count;
            }
        }

        public override int CurrentlyBufferedLeftKeyCount
        {
            get
            {
                var keys = new HashSet<TPartitionKey>();
                int key = FastDictionary2<TPartitionKey, Queue<LEntry>>.IteratorStart;
                while (this.leftQueue.Iterate(ref key)) if (this.leftQueue.entries[key].value.Count > 0) keys.Add(this.leftQueue.entries[key].key);
                int iter = FastDictionary<TPartitionKey, PartitionEntry>.IteratorStart;
                while (this.partitionData.Iterate(ref iter))
                {
                    var partition = this.partitionData.entries[iter].value;
                    if (partition.leftEdgeMap.Count > 0) keys.Add(this.partitionData.entries[iter].key);
                    if (partition.leftIntervalMap.Count > 0) keys.Add(this.partitionData.entries[iter].key);
                }
                return keys.Count;
            }
        }

        public override int CurrentlyBufferedRightKeyCount
        {
            get
            {
                var keys = new HashSet<TPartitionKey>();
                int key = FastDictionary2<TPartitionKey, Queue<REntry>>.IteratorStart;
                while (this.rightQueue.Iterate(ref key)) if (this.rightQueue.entries[key].value.Count > 0) keys.Add(this.rightQueue.entries[key].key);
                return keys.Count;
            }
        }

        [DataContract]
        private struct LeftInterval
        {
            [DataMember]
            public long Start;
            [DataMember]
            public TKey Key;
            [DataMember]
            public TLeft Payload;
            [DataMember]
            public int HeapIndex;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Initialize(long start, ref TKey key, ref TLeft payload, int heapIndex)
            {
                this.Start = start;
                this.Key = key;
                this.Payload = payload;
                this.HeapIndex = heapIndex;
            }

            public override string ToString()
                => "[Start=" + this.Start + ", Key='" + this.Key + "', Payload='" + this.Payload + "', HeapIndex=" + this.HeapIndex + "]";
        }

        [DataContract]
        private struct LeftEdge
        {
            [DataMember]
            public long Start;
            [DataMember]
            public TKey Key;
            [DataMember]
            public TLeft Payload;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Populate(long start, ref TKey key, ref TLeft payload)
            {
                this.Start = start;
                this.Key = key;
                this.Payload = payload;
            }

            public override string ToString()
                => "[Start=" + this.Start + ", Key='" + this.Key + "', Payload='" + this.Payload + "]";
        }

        public override bool LeftInputHasState
        {
            get
            {
                int index = FastDictionary<TPartitionKey, HashSet<PartitionedStreamEvent<TKey, TLeft>>>.IteratorStart;
                int count = 0;
                while (this.leftQueue.Iterate(ref index)) count += this.leftQueue.entries[index].value.Count;
                return count != 0;
            }
        }

        public override bool RightInputHasState
        {
            get
            {
                int index = FastDictionary<TPartitionKey, HashSet<PartitionedStreamEvent<TKey, TRight>>>.IteratorStart;
                int count = 0;
                while (this.rightQueue.Iterate(ref index)) count += this.rightQueue.entries[index].value.Count;
                return count != 0;
            }
        }

        [DataContract]
        private sealed class LEntry
        {
            [DataMember]
            public long Sync;
            [DataMember]
            public long Other;
            [DataMember]
            public int Hash;
            [DataMember]
            public TKey Key;
            [DataMember]
            public TLeft Payload;
        }

        [DataContract]
        private sealed class REntry
        {
            [DataMember]
            public long Sync;
            [DataMember]
            public long Other;
            [DataMember]
            public int Hash;
            [DataMember]
            public TKey Key;
        }

        private sealed class PartitionEntry
        {
            /// <summary>
            /// Stores intervals for active left events.
            /// </summary>
            [DataMember]
            public FastMap<LeftInterval> leftIntervalMap = new FastMap<LeftInterval>();

            /// <summary>
            /// Stores left start edges at <see cref="currTime"/>
            /// </summary>
            [DataMember]
            public FastMap<LeftEdge> leftEdgeMap = new FastMap<LeftEdge>();

            /// <summary>
            /// Stores left end edges at some point in the future, i.e. after <see cref="currTime"/>.
            /// These can originate from edge end events or interval events.
            /// </summary>
            [DataMember]
            public RemovableEndPointHeap leftEndPointHeap = new RemovableEndPointHeap();

            [DataMember]
            public long nextLeftTime = long.MinValue;
            [DataMember]
            public long nextRightTime = long.MinValue;
            [DataMember]
            public long currTime = long.MinValue;

            public bool IsClean() => this.leftIntervalMap.IsEmpty && this.leftEdgeMap.IsEmpty && this.leftEndPointHeap.IsEmpty;
        }
    }
}
