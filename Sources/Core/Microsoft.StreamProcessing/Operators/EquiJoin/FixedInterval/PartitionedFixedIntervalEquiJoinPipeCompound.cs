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
    [KnownType(typeof(EndPointHeap))]
    [KnownType(typeof(EndPointQueue))]
    internal sealed class PartitionedFixedIntervalEquiJoinPipeCompound<TGroupKey, TLeft, TRight, TResult, TPartitionKey> : BinaryPipe<CompoundGroupKey<PartitionKey<TPartitionKey>, TGroupKey>, TLeft, TRight, TResult>, IBinaryObserver
    {
        private readonly Func<TLeft, TRight, TResult> selector;
        private readonly MemoryPool<CompoundGroupKey<PartitionKey<TPartitionKey>, TGroupKey>, TResult> pool;
        private readonly Func<IEndPointOrderer> endpointGenerator;

        [SchemaSerialization]
        private readonly Expression<Func<TGroupKey, TGroupKey, bool>> keyComparer;
        private readonly Func<TGroupKey, TGroupKey, bool> keyComparerEquals;
        [SchemaSerialization]
        private readonly long leftDuration;
        [SchemaSerialization]
        private readonly long rightDuration;

        [DataMember]
        private FastDictionary2<TPartitionKey, Queue<LEntry>> leftQueue = new();
        [DataMember]
        private FastDictionary2<TPartitionKey, Queue<REntry>> rightQueue = new();
        [DataMember]
        private HashSet<TPartitionKey> processQueue = [];
        [DataMember]
        private HashSet<TPartitionKey> seenKeys = [];
        [DataMember]
        private HashSet<TPartitionKey> cleanKeys = [];

        [DataMember]
        private StreamMessage<CompoundGroupKey<PartitionKey<TPartitionKey>, TGroupKey>, TResult> output;

        [DataMember]
        private FastDictionary<TPartitionKey, PartitionEntry> partitionData = new();
        [DataMember]
        private long lastLeftCTI = long.MinValue;
        [DataMember]
        private long lastRightCTI = long.MinValue;
        [DataMember]
        private bool emitCTI = false;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public PartitionedFixedIntervalEquiJoinPipeCompound() { }

        public PartitionedFixedIntervalEquiJoinPipeCompound(
            EquiJoinStreamable<CompoundGroupKey<PartitionKey<TPartitionKey>, TGroupKey>, TLeft, TRight, TResult> stream,
            Expression<Func<TLeft, TRight, TResult>> selector,
            IStreamObserver<CompoundGroupKey<PartitionKey<TPartitionKey>, TGroupKey>, TResult> observer)
            : base(stream, observer)
        {
            this.selector = selector.Compile();
            this.leftDuration = stream.Left.Properties.ConstantDurationLength.Value;
            this.rightDuration = stream.Right.Properties.ConstantDurationLength.Value;

            this.keyComparer = EqualityComparerExpression<TGroupKey>.Default.GetEqualsExpr();
            this.keyComparerEquals = this.keyComparer.Compile();

            if (this.leftDuration == this.rightDuration)
                this.endpointGenerator = () => new EndPointQueue();
            else
                this.endpointGenerator = () => new EndPointHeap();

            this.pool = MemoryManager.GetMemoryPool<CompoundGroupKey<PartitionKey<TPartitionKey>, TGroupKey>, TResult>(stream.Properties.IsColumnar);
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        protected override void ProduceBinaryQueryPlan(PlanNode left, PlanNode right)
        {
            var node = new JoinPlanNode(
                left, right, this,
                typeof(TLeft), typeof(TRight), typeof(TLeft), typeof(CompoundGroupKey<TPartitionKey, TGroupKey>),
                JoinKind.FixedIntervalEquiJoin,
                false, null);
            node.AddJoinExpression("key comparer", this.keyComparer);
            this.Observer.ProduceQueryPlan(node);
        }

        private void NewPartition(TPartitionKey pKey)
        {
            this.leftQueue.Insert(pKey, new Queue<LEntry>());
            this.rightQueue.Insert(pKey, new Queue<REntry>());

            if (!this.partitionData.Lookup(pKey, out int index))
            {
                this.partitionData.Insert(
                    ref index, pKey, new PartitionEntry { endPointHeap = this.endpointGenerator(), key = pKey });
            }
        }

        protected override void ProcessBothBatches(StreamMessage<CompoundGroupKey<PartitionKey<TPartitionKey>, TGroupKey>, TLeft> leftBatch, StreamMessage<CompoundGroupKey<PartitionKey<TPartitionKey>, TGroupKey>, TRight> rightBatch, out bool leftBatchDone, out bool rightBatchDone, out bool leftBatchFree, out bool rightBatchFree)
        {
            this.ProcessLeftBatch(leftBatch, out leftBatchDone, out leftBatchFree);
            this.ProcessRightBatch(rightBatch, out rightBatchDone, out rightBatchFree);
        }

        protected override void ProcessLeftBatch(StreamMessage<CompoundGroupKey<PartitionKey<TPartitionKey>, TGroupKey>, TLeft> batch, out bool leftBatchDone, out bool leftBatchFree)
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

                var partitionKey = batch.key.col[i].OuterGroup.Key;
                if (first || !partitionKey.Equals(previous))
                {
                    if (this.seenKeys.Add(partitionKey)) this.NewPartition(partitionKey);
                    this.leftQueue.Lookup(partitionKey, out int index);
                    queue = this.leftQueue.entries[index].value;
                    this.processQueue.Add(partitionKey);
                }
                var e = new LEntry
                {
                    Key = batch.key.col[i].InnerGroup,
                    Sync = batch.vsync.col[i],
                    IsPunctuation = batch.vother.col[i] == StreamEvent.PunctuationOtherTime,
                    Payload = batch.payload.col[i],
                    Hash = batch.hash.col[i],
                };
                queue.Enqueue(e);
                first = false;
                previous = partitionKey;
            }

            this.ProcessPendingEntries();
        }

        protected override void ProcessRightBatch(StreamMessage<CompoundGroupKey<PartitionKey<TPartitionKey>, TGroupKey>, TRight> batch, out bool rightBatchDone, out bool rightBatchFree)
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

                var partitionKey = batch.key.col[i].OuterGroup.Key;
                if (first || !partitionKey.Equals(previous))
                {
                    if (this.seenKeys.Add(partitionKey)) this.NewPartition(partitionKey);
                    this.rightQueue.Lookup(partitionKey, out int index);
                    queue = this.rightQueue.entries[index].value;
                    this.processQueue.Add(partitionKey);
                }
                var e = new REntry
                {
                    Key = batch.key.col[i].InnerGroup,
                    Sync = batch.vsync.col[i],
                    IsPunctuation = batch.vother.col[i] == StreamEvent.PunctuationOtherTime,
                    Payload = batch.payload.col[i],
                    Hash = batch.hash.col[i],
                };
                queue.Enqueue(e);
                first = false;
                previous = partitionKey;
            }

            this.ProcessPendingEntries();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override void DisposeState()
        {
            this.partitionData.Dispose();
            this.output.Free();
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

                        if (partition.nextLeftTime < partition.nextRightTime)
                        {
                            this.UpdateTime(partition, partition.nextLeftTime);

                            if (!leftEntry.IsPunctuation)
                            {
                                this.ProcessLeftEvent(
                                    partition,
                                    partition.nextLeftTime,
                                    ref leftEntry.Key,
                                    leftEntry.Payload,
                                    leftEntry.Hash);
                            }
                            else if (partition.currTime > old)
                            {
                                var r = default(TRight);
                                this.AddToBatch(
                                    partition.currTime,
                                    StreamEvent.PunctuationOtherTime,
                                    ref leftEntry.Key,
                                    ref partition.key,
                                    ref leftEntry.Payload,
                                    ref r,
                                    leftEntry.Hash);
                            }

                            leftWorking.Dequeue();
                        }
                        else
                        {
                            this.UpdateTime(partition, partition.nextRightTime);

                            if (!rightEntry.IsPunctuation)
                            {
                                this.ProcessRightEvent(
                                    partition,
                                    partition.nextRightTime,
                                    ref rightEntry.Key,
                                    rightEntry.Payload,
                                    rightEntry.Hash);
                            }
                            else if (partition.currTime > old)
                            {
                                var l = default(TLeft);
                                this.AddToBatch(
                                    partition.currTime,
                                    StreamEvent.PunctuationOtherTime,
                                    ref rightEntry.Key,
                                    ref partition.key,
                                    ref l,
                                    ref rightEntry.Payload,
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
                                this.UpdateTime(partition, partition.nextRightTime);
                            break;
                        }

                        this.UpdateTime(partition, partition.nextLeftTime);

                        if (!leftEntry.IsPunctuation)
                        {
                            this.ProcessLeftEvent(
                                partition,
                                partition.nextLeftTime,
                                ref leftEntry.Key,
                                leftEntry.Payload,
                                leftEntry.Hash);
                        }
                        else if (partition.currTime > old)
                        {
                            var r = default(TRight);
                            this.AddToBatch(
                                partition.currTime,
                                StreamEvent.PunctuationOtherTime,
                                ref leftEntry.Key,
                                ref partition.key,
                                ref leftEntry.Payload,
                                ref r,
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
                                this.UpdateTime(partition, partition.nextLeftTime);
                            break;
                        }

                        this.UpdateTime(partition, partition.nextRightTime);

                        if (!rightEntry.IsPunctuation)
                        {
                            this.ProcessRightEvent(
                                partition,
                                partition.nextRightTime,
                                ref rightEntry.Key,
                                rightEntry.Payload,
                                rightEntry.Hash);
                        }
                        else if (partition.currTime > old)
                        {
                            var l = default(TLeft);
                            this.AddToBatch(
                                partition.currTime,
                                StreamEvent.PunctuationOtherTime,
                                ref rightEntry.Key,
                                ref partition.key,
                                ref l,
                                ref rightEntry.Payload,
                                rightEntry.Hash);
                        }

                        rightWorking.Dequeue();
                    }
                    else
                    {
                        if (partition.nextLeftTime < this.lastLeftCTI) partition.nextLeftTime = this.lastLeftCTI;
                        if (partition.nextRightTime < this.lastRightCTI) partition.nextRightTime = this.lastRightCTI;

                        this.UpdateTime(partition, Math.Min(this.lastLeftCTI, this.lastRightCTI));
                        if (partition.IsClean()) this.cleanKeys.Add(pKey);
                        break;
                    }
                }
            }

            if (this.emitCTI)
            {
                var earliest = Math.Min(this.lastLeftCTI, this.lastRightCTI);
                this.AddLowWatermarkToBatch(earliest);
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
                this.ReachTime(partition);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessLeftEvent(PartitionEntry partition, long start, ref TGroupKey key, TLeft payload, int hash)
        {
            int index = partition.leftIntervalMap.Insert(hash);
            partition.leftIntervalMap.Values[index].Populate(start, ref key, ref payload);
            this.CreateOutputForStartInterval(partition, start, ref key, ref payload, hash);
            partition.endPointHeap.Insert(start + this.leftDuration, index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessRightEvent(PartitionEntry partition, long start, ref TGroupKey key, TRight payload, int hash)
        {
            int index = partition.rightIntervalMap.Insert(hash);
            partition.rightIntervalMap.Values[index].Populate(start, ref key, ref payload);
            this.CreateOutputForStartInterval(partition, start, ref key, ref payload, hash);
            partition.endPointHeap.Insert(start + this.rightDuration, ~index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReachTime(PartitionEntry partition)
        {
            while (partition.endPointHeap.TryGetNextInclusive(partition.currTime, out long endPointTime, out int index))
            {
                if (index >= 0)
                {
                    partition.leftIntervalMap.Remove(index);
                }
                else
                {
                    partition.rightIntervalMap.Remove(~index);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CreateOutputForStartInterval(PartitionEntry partition, long currentTime, ref TGroupKey key, ref TLeft payload, int hash)
        {
            // Create end edges for all joined right intervals.
            var intervals = partition.rightIntervalMap.Find(hash);
            while (intervals.Next(out var index))
            {
                if (this.keyComparerEquals(key, partition.rightIntervalMap.Values[index].Key))
                {
                    long leftEnd = currentTime + this.leftDuration;
                    long rightEnd = partition.rightIntervalMap.Values[index].Start + this.rightDuration;
                    this.AddToBatch(
                        currentTime,
                        leftEnd < rightEnd ? leftEnd : rightEnd,
                        ref key,
                        ref partition.key,
                        ref payload,
                        ref partition.rightIntervalMap.Values[index].Payload,
                        hash);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CreateOutputForStartInterval(PartitionEntry partition, long currentTime, ref TGroupKey key, ref TRight payload, int hash)
        {
            // Create end edges for all joined left intervals.
            var intervals = partition.leftIntervalMap.Find(hash);
            while (intervals.Next(out var index))
            {
                if (this.keyComparerEquals(key, partition.leftIntervalMap.Values[index].Key))
                {
                    long rightEnd = currentTime + this.rightDuration;
                    long leftEnd = partition.leftIntervalMap.Values[index].Start + this.leftDuration;
                    this.AddToBatch(
                        currentTime,
                        rightEnd < leftEnd ? rightEnd : leftEnd,
                        ref key,
                        ref partition.key,
                        ref partition.leftIntervalMap.Values[index].Payload,
                        ref payload,
                        hash);
                }
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

                if (this.output.Count == Config.DataBatchSize) this.FlushContents();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AddToBatch(long start, long end, ref TGroupKey key, ref TPartitionKey pKey, ref TLeft leftPayload, ref TRight rightPayload, int hash)
        {
            int index = this.output.Count++;
            this.output.vsync.col[index] = start;
            this.output.vother.col[index] = end;
            this.output.key.col[index] = new CompoundGroupKey<PartitionKey<TPartitionKey>, TGroupKey> { outerGroup = new PartitionKey<TPartitionKey>(pKey), innerGroup = key };
            this.output.hash.col[index] = hash;

            if (end < 0)
                this.output.bitvector.col[index >> 6] |= 1L << (index & 0x3f);
            else
                this.output[index] = this.selector(leftPayload, rightPayload);
            if (this.output.Count == Config.DataBatchSize) this.FlushContents();
        }

        protected override void FlushContents()
        {
            if (this.output.Count == 0) return;
            this.output.Seal();
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
                    var p = this.partitionData.entries[iter].value;
                    count += p.leftIntervalMap.Count;
                }
                return count;
            }
        }
        public override int CurrentlyBufferedRightInputCount
        {
            get
            {
                int count = base.CurrentlyBufferedRightInputCount;
                int key = FastDictionary2<TPartitionKey, Queue<REntry>>.IteratorStart;
                while (this.rightQueue.Iterate(ref key)) count += this.rightQueue.entries[key].value.Count;
                int iter = FastDictionary<TPartitionKey, PartitionEntry>.IteratorStart;
                while (this.partitionData.Iterate(ref iter))
                {
                    var p = this.partitionData.entries[iter].value;
                    count += p.rightIntervalMap.Count;
                }
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
                while (this.partitionData.Iterate(ref iter))
                {
                    var p = this.partitionData.entries[iter].value;
                    if (p.leftIntervalMap.Count > 0) keys.Add(this.partitionData.entries[iter].key);
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
                int iter = FastDictionary<TPartitionKey, PartitionEntry>.IteratorStart;
                while (this.partitionData.Iterate(ref iter))
                {
                    var p = this.partitionData.entries[iter].value;
                    if (p.rightIntervalMap.Count > 0) keys.Add(this.partitionData.entries[iter].key);
                }
                return keys.Count;
            }
        }

        [DataContract]
        private struct ActiveInterval<TPayload>
        {
            [DataMember]
            public long Start;
            [DataMember]
            public TGroupKey Key;
            [DataMember]
            public TPayload Payload;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Populate(long start, ref TGroupKey key, ref TPayload payload)
            {
                this.Start = start;
                this.Key = key;
                this.Payload = payload;
            }

            public override readonly string ToString() => "[Start=" + this.Start + ", Key='" + this.Key + "', Payload='" + this.Payload + "']";
        }

        public override bool LeftInputHasState
        {
            get
            {
                int index = FastDictionary<TPartitionKey, HashSet<PartitionedStreamEvent<TPartitionKey, TLeft>>>.IteratorStart;
                while (this.leftQueue.Iterate(ref index)) if (this.leftQueue.entries[index].value.Count > 0) return true;
                return false;
            }
        }

        public override bool RightInputHasState
        {
            get
            {
                int index = FastDictionary<TPartitionKey, HashSet<PartitionedStreamEvent<TPartitionKey, TRight>>>.IteratorStart;
                while (this.rightQueue.Iterate(ref index)) if (this.rightQueue.entries[index].value.Count > 0) return true;
                return false;
            }
        }

        [DataContract]
        private sealed class LEntry
        {
            [DataMember]
            public long Sync;
            [DataMember]
            public bool IsPunctuation;
            [DataMember]
            public int Hash;
            [DataMember]
            public TGroupKey Key;
            [DataMember]
            public TLeft Payload;
        }

        [DataContract]
        private sealed class REntry
        {
            [DataMember]
            public long Sync;
            [DataMember]
            public bool IsPunctuation;
            [DataMember]
            public int Hash;
            [DataMember]
            public TGroupKey Key;
            [DataMember]
            public TRight Payload;
        }

        [DataContract]
        private sealed class PartitionEntry
        {
            [DataMember]
            public TPartitionKey key;
            [DataMember]
            public FastMap<ActiveInterval<TLeft>> leftIntervalMap = new();
            [DataMember]
            public FastMap<ActiveInterval<TRight>> rightIntervalMap = new();
            [DataMember]
            public IEndPointOrderer endPointHeap;
            [DataMember]
            public long nextLeftTime = long.MinValue;
            [DataMember]
            public long nextRightTime = long.MinValue;
            [DataMember]
            public long currTime = long.MinValue;

            public bool IsClean() => this.leftIntervalMap.IsEmpty && this.rightIntervalMap.IsEmpty;
        }
    }
}
