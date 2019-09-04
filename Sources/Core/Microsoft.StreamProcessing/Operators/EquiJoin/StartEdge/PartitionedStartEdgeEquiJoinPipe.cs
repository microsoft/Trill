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
    internal sealed class PartitionedStartEdgeEquiJoinPipe<TKey, TLeft, TRight, TResult, TPartitionKey> : BinaryPipe<TKey, TLeft, TRight, TResult>, IBinaryObserver
    {
        private const int DefaultCapacity = 64;
        private readonly MemoryPool<TKey, TResult> pool;
        private readonly string errorMessages;
        private readonly Func<TKey, TPartitionKey> getPartitionKey = GetPartitionExtractor<TPartitionKey, TKey>();

        [SchemaSerialization]
        private readonly Expression<Func<TLeft, TRight, TResult>> selectorExpr;
        private readonly Func<TLeft, TRight, TResult> selector;
        [SchemaSerialization]
        private readonly Expression<Func<TKey, TKey, bool>> keyComparerExpr;
        private readonly Func<TKey, TKey, bool> keyComparer;

        [DataMember]
        private FastDictionary2<TPartitionKey, PooledElasticCircularBuffer<LEntry>> leftQueue = new FastDictionary2<TPartitionKey, PooledElasticCircularBuffer<LEntry>>();
        [DataMember]
        private FastDictionary2<TPartitionKey, PooledElasticCircularBuffer<REntry>> rightQueue = new FastDictionary2<TPartitionKey, PooledElasticCircularBuffer<REntry>>();
        [DataMember]
        private HashSet<TPartitionKey> processQueue = new HashSet<TPartitionKey>();
        [DataMember]
        private HashSet<TPartitionKey> seenKeys = new HashSet<TPartitionKey>();
        [DataMember]
        private HashSet<TPartitionKey> cleanKeys = new HashSet<TPartitionKey>();

        [DataMember]
        private StreamMessage<TKey, TResult> output;
        [DataMember]
        private FastDictionary<TPartitionKey, PartitionEntry> partitionData = new FastDictionary<TPartitionKey, PartitionEntry>();
        [DataMember]
        private long lastLeftCTI = long.MinValue;
        [DataMember]
        private long lastRightCTI = long.MinValue;
        [DataMember]
        private bool emitCTI = false;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public PartitionedStartEdgeEquiJoinPipe() { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void UpdateNextLeftTime(PartitionEntry partition, long time)
        {
            partition.nextLeftTime = time;
            if (time == StreamEvent.InfinitySyncTime) partition.isLeftComplete = true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void UpdateNextRightTime(PartitionEntry partition, long time)
        {
            partition.nextRightTime = time;
            if (time == StreamEvent.InfinitySyncTime) partition.isRightComplete = true;
        }

        public PartitionedStartEdgeEquiJoinPipe(
            BinaryStreamable<TKey, TLeft, TRight, TResult> stream,
            Expression<Func<TLeft, TRight, TResult>> selector,
            IStreamObserver<TKey, TResult> observer)
            : base(stream, observer)
        {
            this.selectorExpr = selector;
            this.selector = this.selectorExpr.Compile();

            this.keyComparerExpr = stream.Properties.KeyEqualityComparer.GetEqualsExpr();
            this.keyComparer = this.keyComparerExpr.Compile();

            this.errorMessages = stream.ErrorMessages;

            this.pool = MemoryManager.GetMemoryPool<TKey, TResult>(stream.Properties.IsColumnar);
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        protected override void ProduceBinaryQueryPlan(PlanNode left, PlanNode right)
        {
            var node = new JoinPlanNode(
                left, right, this,
                typeof(TLeft), typeof(TRight), typeof(TResult), typeof(TKey),
                JoinKind.StartEdgeEquijoin,
                false, this.errorMessages);
            node.AddJoinExpression("selector", this.selectorExpr);
            node.AddJoinExpression("key comparer", this.keyComparerExpr);
            this.Observer.ProduceQueryPlan(node);
        }

        private void NewPartition(TPartitionKey pKey)
        {
            this.leftQueue.Insert(pKey, new PooledElasticCircularBuffer<LEntry>());
            this.rightQueue.Insert(pKey, new PooledElasticCircularBuffer<REntry>());

            if (!this.partitionData.Lookup(pKey, out int index)) this.partitionData.Insert(ref index, pKey, new PartitionEntry { key = pKey });
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override void DisposeState()
        {
            int iter;
            iter = FastDictionary<TPartitionKey, LEntry>.IteratorStart;
            while (this.leftQueue.Iterate(ref iter)) this.leftQueue.entries[iter].value.Dispose();
            iter = FastDictionary<TPartitionKey, REntry>.IteratorStart;
            while (this.rightQueue.Iterate(ref iter)) this.rightQueue.entries[iter].value.Dispose();
            this.output.Free();
        }

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

            PooledElasticCircularBuffer<LEntry> queue = null;
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
                queue.Enqueue(ref e);
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

            PooledElasticCircularBuffer<REntry> queue = null;
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
                    Payload = batch.payload.col[i],
                    Hash = batch.hash.col[i],
                };
                queue.Enqueue(ref e);
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

                PooledElasticCircularBuffer<LEntry> leftWorking = null;
                PooledElasticCircularBuffer<REntry> rightWorking = null;
                this.leftQueue.Lookup(pKey, out int index);
                leftWorking = this.leftQueue.entries[index].value;
                rightWorking = this.rightQueue.entries[index].value;

                FastMap<ActiveEvent<TLeft>> leftEdgeMapForPartition = null;
                FastMap<ActiveEvent<TRight>> rightEdgeMapForPartition = null;
                this.partitionData.Lookup(pKey, out index);
                var partition = this.partitionData.entries[index].value;
                leftEdgeMapForPartition = partition.leftEdgeMap;
                rightEdgeMapForPartition = partition.rightEdgeMap;

                while (true)
                {
                    bool hasLeftBatch = leftWorking.TryPeekFirst(out LEntry leftEntry);
                    bool hasRightBatch = rightWorking.TryPeekFirst(out REntry rightEntry);
                    FastMap<ActiveEvent<TRight>>.FindTraverser rightEdges = default;
                    FastMap<ActiveEvent<TLeft>>.FindTraverser leftEdges = default;
                    if (hasLeftBatch && hasRightBatch)
                    {
                        UpdateNextLeftTime(partition, leftEntry.Sync);
                        UpdateNextRightTime(partition, rightEntry.Sync);

                        if (partition.nextLeftTime <= partition.nextRightTime)
                        {
                            if (leftEntry.Other != long.MinValue)
                            {
                                TKey key = leftEntry.Key;

                                var hash = leftEntry.Hash;
                                if (rightEdgeMapForPartition.Find(hash, ref rightEdges))
                                {
                                    while (rightEdges.Next(out int rightIndex))
                                    {
                                        if (this.keyComparer(key, rightEdgeMapForPartition.Values[rightIndex].Key))
                                        {
                                            OutputStartEdge(partition.nextLeftTime, ref key, ref leftEntry.Payload, ref rightEdgeMapForPartition.Values[rightIndex].Payload, hash);
                                        }
                                    }
                                }
                                if (!partition.isRightComplete)
                                {
                                    int newIndex = leftEdgeMapForPartition.Insert(hash);
                                    leftEdgeMapForPartition.Values[newIndex].Populate(ref key, ref leftEntry.Payload);
                                }

                                UpdateNextLeftTime(partition, leftEntry.Sync);
                            }
                            else
                            {
                                OutputPunctuation(leftEntry.Sync, ref leftEntry.Key, leftEntry.Hash);
                            }

                            leftWorking.Dequeue();
                        }
                        else
                        {
                            if (rightEntry.Other != long.MinValue)
                            {
                                TKey key = rightEntry.Key;

                                var hash = rightEntry.Hash;
                                if (leftEdgeMapForPartition.Find(hash, ref leftEdges))
                                {
                                    while (leftEdges.Next(out int leftIndex))
                                    {
                                        if (this.keyComparer(key, leftEdgeMapForPartition.Values[leftIndex].Key))
                                        {
                                            OutputStartEdge(partition.nextRightTime, ref key, ref leftEdgeMapForPartition.Values[leftIndex].Payload, ref rightEntry.Payload, hash);
                                        }
                                    }
                                }
                                if (!partition.isLeftComplete)
                                {
                                    int newIndex = rightEdgeMapForPartition.Insert(hash);
                                    rightEdgeMapForPartition.Values[newIndex].Populate(ref key, ref rightEntry.Payload);
                                }

                                UpdateNextRightTime(partition, rightEntry.Sync);
                            }
                            else
                            {
                                OutputPunctuation(rightEntry.Sync, ref rightEntry.Key, rightEntry.Hash);
                            }

                            rightWorking.Dequeue();
                        }
                    }
                    else if (hasLeftBatch)
                    {
                        UpdateNextLeftTime(partition, leftEntry.Sync);

                        if (leftEntry.Other != long.MinValue)
                        {
                            TKey key = leftEntry.Key;

                            var hash = leftEntry.Hash;
                            if (rightEdgeMapForPartition.Find(hash, ref rightEdges))
                            {
                                while (rightEdges.Next(out int rightIndex))
                                {
                                    if (this.keyComparer(key, rightEdgeMapForPartition.Values[rightIndex].Key))
                                    {
                                        OutputStartEdge(partition.nextLeftTime, ref key, ref leftEntry.Payload, ref rightEdgeMapForPartition.Values[rightIndex].Payload, hash);
                                    }
                                }
                            }
                            if (!partition.isRightComplete)
                            {
                                int newIndex = leftEdgeMapForPartition.Insert(hash);
                                leftEdgeMapForPartition.Values[newIndex].Populate(ref key, ref leftEntry.Payload);
                            }

                            UpdateNextLeftTime(partition, leftEntry.Sync);
                        }
                        else
                        {
                            OutputPunctuation(leftEntry.Sync, ref leftEntry.Key, leftEntry.Hash);
                            return;
                        }

                        leftWorking.Dequeue();
                    }
                    else if (hasRightBatch)
                    {
                        UpdateNextRightTime(partition, rightEntry.Sync);

                        if (rightEntry.Other != long.MinValue)
                        {
                            TKey key = rightEntry.Key;

                            var hash = rightEntry.Hash;
                            if (leftEdgeMapForPartition.Find(hash, ref leftEdges))
                            {
                                while (leftEdges.Next(out int leftIndex))
                                {
                                    if (this.keyComparer(key, leftEdgeMapForPartition.Values[leftIndex].Key))
                                    {
                                        OutputStartEdge(partition.nextRightTime, ref key, ref leftEdgeMapForPartition.Values[leftIndex].Payload, ref rightEntry.Payload, hash);
                                    }
                                }
                            }
                            if (!partition.isLeftComplete)
                            {
                                int newIndex = rightEdgeMapForPartition.Insert(hash);
                                rightEdgeMapForPartition.Values[newIndex].Populate(ref key, ref rightEntry.Payload);
                            }

                            UpdateNextRightTime(partition, rightEntry.Sync);
                        }
                        else
                        {
                            OutputPunctuation(rightEntry.Sync, ref rightEntry.Key, rightEntry.Hash);
                        }

                        rightWorking.Dequeue();
                    }
                    else
                    {
                        if (partition.nextLeftTime < this.lastLeftCTI)
                            UpdateNextLeftTime(partition, this.lastLeftCTI);
                        if (partition.nextRightTime < this.lastRightCTI)
                            UpdateNextRightTime(partition, this.lastRightCTI);
                        this.cleanKeys.Add(pKey);
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

                    this.leftQueue.Lookup(p, out var index);
                    this.leftQueue.entries[index].value.Dispose();
                    this.leftQueue.Remove(p);
                    this.rightQueue.entries[index].value.Dispose();
                    this.rightQueue.Remove(p);
                }

                this.cleanKeys.Clear();
            }

            this.processQueue.Clear();
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
                this.output.bitvector.col[index >> 6] |= (1L << (index & 0x3f));

                if (this.output.Count == Config.DataBatchSize) FlushContents();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void OutputStartEdge(long start, ref TKey key, ref TLeft leftPayload, ref TRight rightPayload, int hash)
        {
            if (start < this.lastCTI)
            {
                throw new StreamProcessingOutOfOrderException("Outputting an event out of order!");
            }

            int index = this.output.Count++;
            this.output.vsync.col[index] = start;
            this.output.vother.col[index] = StreamEvent.InfinitySyncTime;
            this.output.key.col[index] = key;
            this.output[index] = this.selector(leftPayload, rightPayload);
            this.output.hash.col[index] = hash;

            if (this.output.Count == Config.DataBatchSize) FlushContents();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void OutputPunctuation(long start, ref TKey key, int hash)
        {
            if (start < this.lastCTI)
            {
                throw new StreamProcessingOutOfOrderException("Outputting an event out of order!");
            }

            int index = this.output.Count++;
            this.output.vsync.col[index] = start;
            this.output.vother.col[index] = long.MinValue;
            this.output.key.col[index] = key;
            this.output[index] = default;
            this.output.hash.col[index] = hash;
            this.output.bitvector.col[index >> 6] |= (1L << (index & 0x3f));

            if (this.output.Count == Config.DataBatchSize) FlushContents();
        }

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
                int key = FastDictionary2<TPartitionKey, PooledElasticCircularBuffer<LEntry>>.IteratorStart;
                while (this.leftQueue.Iterate(ref key)) count += this.leftQueue.entries[key].value.Count;
                int iter = FastDictionary<TPartitionKey, PartitionEntry>.IteratorStart;
                while (this.partitionData.Iterate(ref iter)) count += this.partitionData.entries[iter].value.leftEdgeMap.Count;
                return count;
            }
        }

        public override int CurrentlyBufferedRightInputCount
        {
            get
            {
                int count = base.CurrentlyBufferedRightInputCount;
                int key = FastDictionary2<TPartitionKey, PooledElasticCircularBuffer<REntry>>.IteratorStart;
                while (this.rightQueue.Iterate(ref key)) count += this.rightQueue.entries[key].value.Count;
                int iter = FastDictionary<TPartitionKey, PartitionEntry>.IteratorStart;
                while (this.partitionData.Iterate(ref iter)) count += this.partitionData.entries[iter].value.rightEdgeMap.Count;
                return count;
            }
        }

        public override int CurrentlyBufferedLeftKeyCount
        {
            get
            {
                var keys = new HashSet<TPartitionKey>();
                int key = FastDictionary2<TPartitionKey, PooledElasticCircularBuffer<LEntry>>.IteratorStart;
                while (this.leftQueue.Iterate(ref key)) if (this.leftQueue.entries[key].value.Count > 0) keys.Add(this.leftQueue.entries[key].key);
                int iter = FastDictionary<TPartitionKey, PartitionEntry>.IteratorStart;
                while (this.partitionData.Iterate(ref iter)) if (this.partitionData.entries[iter].value.leftEdgeMap.Count > 0) keys.Add(this.partitionData.entries[iter].key);
                return keys.Count;
            }
        }

        public override int CurrentlyBufferedRightKeyCount
        {
            get
            {
                var keys = new HashSet<TPartitionKey>();
                int key = FastDictionary2<TPartitionKey, PooledElasticCircularBuffer<REntry>>.IteratorStart;
                while (this.rightQueue.Iterate(ref key)) if (this.rightQueue.entries[key].value.Count > 0) keys.Add(this.rightQueue.entries[key].key);
                int iter = FastDictionary<TPartitionKey, PartitionEntry>.IteratorStart;
                while (this.partitionData.Iterate(ref iter)) if (this.partitionData.entries[iter].value.rightEdgeMap.Count > 0) keys.Add(this.partitionData.entries[iter].key);
                return keys.Count;
            }
        }

        private struct ActiveEvent<TPayload>
        {
            public TKey Key;

            public TPayload Payload;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Populate(ref TKey key, ref TPayload payload)
            {
                this.Key = key;
                this.Payload = payload;
            }

            public override string ToString() => "[Key='" + this.Key + "', Payload='" + this.Payload + "']";
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
            [DataMember]
            public TRight Payload;
        }

        [DataContract]
        private sealed class PartitionEntry
        {
            [DataMember]
            public TPartitionKey key;

            /// <summary>
            /// Currently active left start edges
            /// </summary>
            [DataMember]
            public FastMap<ActiveEvent<TLeft>> leftEdgeMap = new FastMap<ActiveEvent<TLeft>>();

            /// <summary>
            /// Currently active right start edges
            /// </summary>
            [DataMember]
            public FastMap<ActiveEvent<TRight>> rightEdgeMap = new FastMap<ActiveEvent<TRight>>();

            [DataMember]
            public long nextLeftTime = long.MinValue;

            /// <summary>
            /// True if left has reached StreamEvent.InfinitySyncTime
            /// </summary>
            [DataMember]
            public bool isLeftComplete = false;

            [DataMember]
            public long nextRightTime = long.MinValue;

            /// <summary>
            /// True if right has reached StreamEvent.InfinitySyncTime
            /// </summary>
            [DataMember]
            public bool isRightComplete = false;
        }
    }
}
