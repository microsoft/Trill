// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class PartitionedUnionPipe<TKey, TPayload, TPartitionKey> : BinaryPipe<TKey, TPayload, TPayload, TPayload>
    {
        private readonly MemoryPool<TKey, TPayload> pool;
        private readonly string errorMessages;
        private readonly Func<TKey, TPartitionKey> getPartitionKey = GetPartitionExtractor<TPartitionKey, TKey>();

        [DataMember]
        private FastDictionary2<TPartitionKey, PooledElasticCircularBuffer<Entry>> leftQueue = new FastDictionary2<TPartitionKey, PooledElasticCircularBuffer<Entry>>();
        [DataMember]
        private FastDictionary2<TPartitionKey, PooledElasticCircularBuffer<Entry>> rightQueue = new FastDictionary2<TPartitionKey, PooledElasticCircularBuffer<Entry>>();
        [DataMember]
        private HashSet<TPartitionKey> processQueue = new HashSet<TPartitionKey>();
        [DataMember]
        private HashSet<TPartitionKey> seenKeys = new HashSet<TPartitionKey>();
        [DataMember]
        private HashSet<TPartitionKey> cleanKeys = new HashSet<TPartitionKey>();

        [DataMember]
        private StreamMessage<TKey, TPayload> output;

        [DataMember]
        private FastDictionary<TPartitionKey, long> nextLeftTime = new FastDictionary<TPartitionKey, long>();
        [DataMember]
        private FastDictionary<TPartitionKey, long> nextRightTime = new FastDictionary<TPartitionKey, long>();
        [DataMember]
        private long lastLeftCTI = long.MinValue;
        [DataMember]
        private long lastRightCTI = long.MinValue;
        [DataMember]
        private bool emitCTI = false;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public PartitionedUnionPipe() { }

        public PartitionedUnionPipe(IStreamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer)
            : base(stream, observer)
        {
            this.errorMessages = stream.ErrorMessages;
            this.pool = MemoryManager.GetMemoryPool<TKey, TPayload>(stream.Properties.IsColumnar);
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        private void NewPartition(TPartitionKey pKey)
        {
            this.leftQueue.Insert(pKey, new PooledElasticCircularBuffer<Entry>());
            this.rightQueue.Insert(pKey, new PooledElasticCircularBuffer<Entry>());

            if (!this.nextLeftTime.Lookup(pKey, out int left)) this.nextLeftTime.Insert(ref left, pKey, StreamEvent.MinSyncTime);
            if (!this.nextRightTime.Lookup(pKey, out int right)) this.nextRightTime.Insert(ref right, pKey, StreamEvent.MinSyncTime);
        }

        protected override void ProcessBothBatches(StreamMessage<TKey, TPayload> leftBatch, StreamMessage<TKey, TPayload> rightBatch, out bool leftBatchDone, out bool rightBatchDone, out bool leftBatchFree, out bool rightBatchFree)
        {
            ProcessLeftBatch(leftBatch, out leftBatchDone, out leftBatchFree);
            ProcessRightBatch(rightBatch, out rightBatchDone, out rightBatchFree);
        }

        protected override void ProcessLeftBatch(StreamMessage<TKey, TPayload> batch, out bool leftBatchDone, out bool leftBatchFree)
        {
            leftBatchDone = true;
            leftBatchFree = true;
            batch.iter = 0;

            PooledElasticCircularBuffer<Entry> queue = null;
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
                var e = new Entry
                {
                    Key = batch.key.col[i],
                    Sync = batch.vsync.col[i],
                    Other = batch.vother.col[i],
                    Payload = batch[i],
                    Hash = batch.hash.col[i]
                };
                queue.Enqueue(ref e);
                first = false;
                previous = partitionKey;
            }

            ProcessPendingEntries();
        }

        protected override void ProcessRightBatch(StreamMessage<TKey, TPayload> batch, out bool rightBatchDone, out bool rightBatchFree)
        {
            rightBatchDone = true;
            rightBatchFree = true;
            batch.iter = 0;

            PooledElasticCircularBuffer<Entry> queue = null;
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
                var e = new Entry
                {
                    Key = batch.key.col[i],
                    Sync = batch.vsync.col[i],
                    Other = batch.vother.col[i],
                    Payload = batch[i],
                    Hash = batch.hash.col[i]
                };
                queue.Enqueue(ref e);
                first = false;
                previous = partitionKey;
            }

            ProcessPendingEntries();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override void DisposeState()
        {
            int iter;
            iter = FastDictionary<TPartitionKey, Entry>.IteratorStart;
            while (this.leftQueue.Iterate(ref iter)) this.leftQueue.entries[iter].value.Dispose();
            iter = FastDictionary<TPartitionKey, Entry>.IteratorStart;
            while (this.leftQueue.Iterate(ref iter)) this.rightQueue.entries[iter].value.Dispose();
            this.output.Free();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessPendingEntries()
        {
            foreach (var workingPartitionKey in this.processQueue)
            {
                // Partition is no longer clean if we are processing it. If it is still clean, it will be added below.
                this.cleanKeys.Remove(workingPartitionKey);

                PooledElasticCircularBuffer<Entry> leftWorking = null;
                PooledElasticCircularBuffer<Entry> rightWorking = null;

                this.leftQueue.Lookup(workingPartitionKey, out int index);
                leftWorking = this.leftQueue.entries[index].value;
                this.rightQueue.Lookup(workingPartitionKey, out index);
                rightWorking = this.rightQueue.entries[index].value;

                while (true)
                {
                    bool hasLeftEntry = leftWorking.TryPeekFirst(out Entry leftEntry);
                    bool hasRightEntry = rightWorking.TryPeekFirst(out Entry rightEntry);

                    if (hasLeftEntry && hasRightEntry)
                    {
                        this.nextLeftTime.Lookup(workingPartitionKey, out int workingIndex);
                        this.nextLeftTime.entries[workingIndex].value = leftEntry.Sync;
                        this.nextRightTime.Lookup(workingPartitionKey, out workingIndex);
                        this.nextRightTime.entries[workingIndex].value = rightEntry.Sync;

                        switch (leftEntry.Sync.CompareTo(rightEntry.Sync))
                        {
                            case -1:
                                // Output the left tuple
                                OutputCurrentTuple(leftEntry);
                                leftWorking.TryDequeue(out leftEntry);
                                break;

                            case 0:
                                // Output both tuples
                                OutputCurrentTuple(leftEntry);
                                OutputCurrentTuple(rightEntry);
                                leftWorking.TryDequeue(out leftEntry);
                                rightWorking.TryDequeue(out rightEntry);
                                break;

                            case 1:
                                // Output the right tuple
                                OutputCurrentTuple(rightEntry);
                                rightWorking.TryDequeue(out rightEntry);
                                break;

                            default: throw new InvalidOperationException();
                        }
                    }
                    else if (hasLeftEntry)
                    {
                        this.nextLeftTime.Lookup(workingPartitionKey, out int workingIndex);
                        this.nextLeftTime.entries[workingIndex].value = leftEntry.Sync;

                        var lastRightTime = !this.nextRightTime.Lookup(workingPartitionKey, out workingIndex)
                            ? long.MinValue
                            : this.nextRightTime.entries[workingIndex].value;

                        if (leftEntry.Sync <= Math.Max(lastRightTime, this.lastRightCTI))
                        {
                            // Output the left tuple
                            OutputCurrentTuple(leftEntry);
                            leftWorking.TryDequeue(out leftEntry);
                        }
                        else
                        {
                            // Need to wait for the other input
                            break;
                        }
                    }
                    else if (hasRightEntry)
                    {
                        this.nextRightTime.Lookup(workingPartitionKey, out int workingIndex);
                        this.nextRightTime.entries[workingIndex].value = rightEntry.Sync;

                        var lastLeftTime = !this.nextLeftTime.Lookup(workingPartitionKey, out workingIndex)
                            ? long.MinValue
                            : this.nextLeftTime.entries[workingIndex].value;

                        if (rightEntry.Sync <= Math.Max(lastLeftTime, this.lastLeftCTI))
                        {
                            // Output the right tuple
                            OutputCurrentTuple(rightEntry);
                            rightWorking.TryDequeue(out _);
                        }
                        else
                        {
                            // Need to wait for the other input
                            break;
                        }
                    }
                    else
                    {
                        this.cleanKeys.Add(workingPartitionKey);
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
                    this.leftQueue.Lookup(p, out int index);
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
        private void OutputCurrentTuple(Entry current)
        {
            if (current.Sync < this.lastCTI)
            {
                throw new StreamProcessingOutOfOrderException("Outputting an event out of order!");
            }

            int index = this.output.Count++;
            this.output.vsync.col[index] = current.Sync;
            this.output.vother.col[index] = current.Other;
            this.output.key.col[index] = current.Key;
            this.output[index] = current.Payload;
            this.output.hash.col[index] = current.Hash;

            if (current.Other == long.MinValue) this.output.bitvector.col[index >> 6] |= (1L << (index & 0x3f));
            if (this.output.Count == Config.DataBatchSize) FlushContents();
        }

        protected override void ProduceBinaryQueryPlan(PlanNode left, PlanNode right)
        {
            var node = new UnionPlanNode(
                left, right, this, typeof(TKey), typeof(TPayload), false, false, this.errorMessages);
            this.Observer.ProduceQueryPlan(node);
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
                var count = base.CurrentlyBufferedLeftInputCount;
                int key = FastDictionary2<TPartitionKey, PooledElasticCircularBuffer<Entry>>.IteratorStart;
                while (this.leftQueue.Iterate(ref key)) count += this.leftQueue.entries[key].value.Count;
                return count;
            }
        }

        public override int CurrentlyBufferedRightInputCount
        {
            get
            {
                var count = base.CurrentlyBufferedRightInputCount;
                int key = FastDictionary2<TPartitionKey, PooledElasticCircularBuffer<Entry>>.IteratorStart;
                while (this.rightQueue.Iterate(ref key)) count += this.rightQueue.entries[key].value.Count;
                return count;
            }
        }

        public override int CurrentlyBufferedLeftKeyCount
        {
            get
            {
                var keys = new HashSet<TPartitionKey>();
                int key = FastDictionary2<TPartitionKey, PooledElasticCircularBuffer<Entry>>.IteratorStart;
                while (this.leftQueue.Iterate(ref key)) if (this.leftQueue.entries[key].value.Count > 0) keys.Add(this.leftQueue.entries[key].key);
                return keys.Count;
            }
        }

        public override int CurrentlyBufferedRightKeyCount
        {
            get
            {
                var keys = new HashSet<TPartitionKey>();
                int key = FastDictionary2<TPartitionKey, PooledElasticCircularBuffer<Entry>>.IteratorStart;
                while (this.rightQueue.Iterate(ref key)) if (this.rightQueue.entries[key].value.Count > 0) keys.Add(this.rightQueue.entries[key].key);
                return keys.Count;
            }
        }

        public override bool LeftInputHasState
        {
            get
            {
                int index = FastDictionary<TPartitionKey, HashSet<Entry>>.IteratorStart;
                int count = 0;
                while (this.leftQueue.Iterate(ref index)) count += this.leftQueue.entries[index].value.Count;
                return count != 0;
            }
        }

        public override bool RightInputHasState
        {
            get
            {
                int index = FastDictionary<TPartitionKey, HashSet<Entry>>.IteratorStart;
                int count = 0;
                while (this.rightQueue.Iterate(ref index)) count += this.rightQueue.entries[index].value.Count;
                return count != 0;
            }
        }

        [DataContract]
        private sealed class Entry
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
            public TPayload Payload;
        }
    }
}