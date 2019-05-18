// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.Linq.Expressions;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    // Version of AlterLifetime with vother func as (a, b) => c
    [DataContract]
    internal sealed class PartitionedAlterLifetimeVariableDurationByKeyPipe<TPartitionKey, TPayload> : UnaryPipe<PartitionKey<TPartitionKey>, TPayload, TPayload>, IStreamObserver<PartitionKey<TPartitionKey>, TPayload>
    {
        private readonly MemoryPool<PartitionKey<TPartitionKey>, TPayload> pool;

        [SchemaSerialization]
        private readonly Expression<Func<TPartitionKey, long, long, long>> durationSelector;
        private readonly Func<TPartitionKey, long, long, long> durationSelectorCompiled;
        [SchemaSerialization]
        private readonly Expression<Func<TPartitionKey, long, long>> startTimeSelector;
        private readonly Func<TPartitionKey, long, long> startTimeSelectorCompiled;

        [DataMember]
        private FastDictionary<TPartitionKey, long> lastSync = new FastDictionary<TPartitionKey, long>();

        [Obsolete("Used only by serialization. Do not call directly.")]
        public PartitionedAlterLifetimeVariableDurationByKeyPipe() { }

        public PartitionedAlterLifetimeVariableDurationByKeyPipe(AlterLifetimeStreamable<PartitionKey<TPartitionKey>, TPayload> stream, IStreamObserver<PartitionKey<TPartitionKey>, TPayload> observer)
            : base(stream, observer)
        {
            Contract.Requires(stream != null);

            this.durationSelector = (Expression<Func<TPartitionKey, long, long, long>>)stream.DurationSelector;
            this.startTimeSelector = (Expression<Func<TPartitionKey, long, long>>)stream.StartTimeSelector;
            this.pool = MemoryManager.GetMemoryPool<PartitionKey<TPartitionKey>, TPayload>(stream.Properties.IsColumnar);

            if (this.startTimeSelector != null) this.startTimeSelectorCompiled = this.startTimeSelector.Compile();
            this.durationSelectorCompiled = this.durationSelector.Compile();
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new AlterLifetimePlanNode(
                previous, this,
                typeof(PartitionKey<TPartitionKey>), typeof(TPayload), this.startTimeSelector, this.durationSelector,
                false));

        public unsafe override void OnNext(StreamMessage<PartitionKey<TPartitionKey>, TPayload> batch)
        {
            batch.vsync = batch.vsync.MakeWritable(this.pool.longPool);
            batch.vother = batch.vother.MakeWritable(this.pool.longPool);
            batch.bitvector = batch.bitvector.MakeWritable(this.pool.bitvectorPool);

            var count = batch.Count;

            fixed (long* vsync = batch.vsync.col)
            fixed (long* vother = batch.vother.col)
            fixed (long* bv = batch.bitvector.col)
            for (int i = 0; i < count; i++)
            {
                int index;
                TPartitionKey partition;
                if ((bv[i >> 6] & (1L << (i & 0x3f))) != 0)
                {
                    if (vother[i] == PartitionedStreamEvent.LowWatermarkOtherTime)
                    {
                        var iter = FastDictionary<TPartitionKey, long>.IteratorStart;
                        while (this.lastSync.Iterate(ref iter))
                        {
                            if (this.lastSync.entries[iter].value < vsync[i])
                            {
                                this.lastSync.entries[iter].value = Math.Max(vsync[i], this.lastSync.entries[iter].value);
                            }
                        }
                    }
                    else if (vother[i] == PartitionedStreamEvent.PunctuationOtherTime)
                    {
                        partition = batch.key.col[i].Key;
                        if (this.startTimeSelector != null) vsync[i] = this.startTimeSelectorCompiled(partition, vsync[i]);

                        if (this.lastSync.Lookup(partition, out index))
                        {
                            this.lastSync.entries[index].value = Math.Max(vsync[i], this.lastSync.entries[index].value);
                        }
                        else
                        {
                            this.lastSync.Insert(ref index, partition, vsync[i]);
                        }
                    }
                    continue;
                }

                partition = batch.key.col[i].Key;
                if (vsync[i] < vother[i])
                {
                    // insert event
                    long old_vsync = vsync[i];
                    if (this.startTimeSelector != null)
                    {
                        vsync[i] = this.startTimeSelectorCompiled(partition, vsync[i]);
                    }
                    if (vother[i] < StreamEvent.InfinitySyncTime) // not a start-edge
                    {
                        vother[i] = vsync[i] + this.durationSelectorCompiled(partition, old_vsync, vother[i]);
                        if (vother[i] > StreamEvent.MaxSyncTime) throw new ArgumentOutOfRangeException();
                    }
                    if (vother[i] <= vsync[i]) bv[i >> 6] |= (1L << (i & 0x3f));
                }
                else
                {
                    if (vother[i] != long.MinValue) // not a CTI
                    {
                        // update the start time of the retract
                        long old_vother = vother[i];
                        if (this.startTimeSelector != null)
                        {
                            vother[i] = this.startTimeSelectorCompiled(partition, vother[i]);
                        }

                        // issue the correct end edge
                        vsync[i] = vother[i] + this.durationSelectorCompiled(partition, old_vother, vsync[i]);
                        if (vother[i] >= vsync[i]) bv[i >> 6] |= (1L << (i & 0x3f));
                    }
                    else if (this.startTimeSelector != null)
                    {
                        vsync[i] = this.startTimeSelectorCompiled(partition, vsync[i]);
                    }
                }
                if (!this.lastSync.Lookup(partition, out index))
                {
                    this.lastSync.Insert(ref index, partition, vsync[i]);
                }
                else if (vsync[i] < this.lastSync.entries[index].value)
                {
                    throw new StreamProcessingOutOfOrderException(
                        "The operator AlterLifetime produced output out of sync-time order on an input event. The current internal sync time is " + this.lastSync.entries[index].value
                        + ". The event's sync time is " + vsync[i].ToString(CultureInfo.InvariantCulture)
                        + ". The event's partition key is " + partition.ToString()
                        + ". The event's value is " + batch[i].ToString() + ".");
                }
                else
                {
                    this.lastSync.entries[index].value = vsync[i];
                }
            }

            this.Observer.OnNext(batch);
        }

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }
}
