// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class SynchronousGAPipe<TKey, TSpray> :
        Pipe<TKey, TSpray>, IBothStreamObserverAndStreamObservable<TKey, TSpray>
    {
        [SchemaSerialization]
        private readonly int totalBranches;
        [SchemaSerialization]
        private readonly bool multicast;
        [SchemaSerialization]
        private readonly Expression<Comparison<TSpray>> spraySortOrderComparer;
        private readonly Comparison<TSpray> spraySortOrderComparerFunc;

        private readonly MemoryPool<TKey, TSpray> pool;

        [DataMember]
        private int l1_spray;
        private readonly List<IStreamObserver<TKey, TSpray>> Observers;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public SynchronousGAPipe() { }

        public SynchronousGAPipe(
            SprayGroupImportStreamable<TKey, TSpray> stream,
            IStreamObserver<TKey, TSpray> observer,
            IComparerExpression<TSpray> spraySortOrderComparer,
            int totalBranches,
            bool multicast)
            : base(stream, observer)
        {
            this.spraySortOrderComparer = spraySortOrderComparer?.GetCompareExpr();
            this.spraySortOrderComparerFunc = this.spraySortOrderComparer?.Compile();
            this.totalBranches = totalBranches;
            this.multicast = multicast;

            this.l1_spray = 0;

            this.Observers = new List<IStreamObserver<TKey, TSpray>>();
            this.pool = MemoryManager.GetMemoryPool<TKey, TSpray>(stream.Properties.IsColumnar);
        }

        public void AddObserver(IStreamObserver<TKey, TSpray> observer) => this.Observers.Add(observer);

        private bool first = true;
        private TSpray lastElem = default;

        public override void OnCompleted()
        {
            for (int i = 0; i < this.totalBranches; i++) this.Observers[i].OnCompleted();
        }

        public override void OnFlush()
        {
            for (int i = 0; i < this.totalBranches; i++) this.Observers[i].OnFlush();
        }

        public void OnNext(StreamMessage<TKey, TSpray> batch)
        {
            if (this.multicast)
            {
                for (int i = 0; i < this.totalBranches; i++)
                {
                    this.pool.Get(out StreamMessage<TKey, TSpray> _outbatch);
                    _outbatch.CloneFrom(batch, false);
                    this.Observers[i].OnNext(_outbatch);
                }
                batch.Free();
                return;
            }

            // If this batch contains any punctuation, we need to broadcast them to all observers.
            StreamMessage<TKey, TSpray> broadcastMaster = null;
            for (int i = batch.Count - 1; i >= 0; i--)
            {
                if (batch.vother.col[i] < 0)
                {
                    // Create a master broadcast batch that we can clone from.
                    // TODO: maybe it's better to allocate a new batch without deleted data gaps?
                    this.pool.Get(out broadcastMaster);
                    broadcastMaster.CloneFrom(batch);
                    broadcastMaster.bitvector = broadcastMaster.bitvector.MakeWritable(this.pool.bitvectorPool);

                    // Since we only care about punctuations, delete everything
                    for (int deletingIndex = 0; deletingIndex <= broadcastMaster.Count >> 6; deletingIndex++)
                        broadcastMaster.bitvector.col[deletingIndex] = ~(0L);

                    break;
                }
            }

            int originalBatchRecipient;
            if (this.spraySortOrderComparer == null)
            {
                originalBatchRecipient = this.l1_spray;
                this.Observers[this.l1_spray].OnNext(batch);
                this.l1_spray++;
                if (this.l1_spray == this.totalBranches) this.l1_spray = 0;
            }
            else
            {
                if (this.first || (this.spraySortOrderComparerFunc(this.lastElem, batch[0]) == 0))
                {
                    this.first = false;
                    this.lastElem = batch[batch.Count - 1];
                    originalBatchRecipient = this.l1_spray;
                    this.Observers[this.l1_spray].OnNext(batch);
                }
                else
                {
                    this.lastElem = batch[batch.Count - 1];
                    this.l1_spray++;
                    if (this.l1_spray == this.totalBranches) this.l1_spray = 0;
                    originalBatchRecipient = this.l1_spray;
                    this.Observers[this.l1_spray].OnNext(batch);
                }
            }

            if (broadcastMaster != null)
            {
                // Broadcast to all except the observer that received the current batch
                int lastBroadcastIndex = this.totalBranches - 1;
                if (lastBroadcastIndex == originalBatchRecipient) lastBroadcastIndex--;
                for (int i = 0; i < this.totalBranches; i++)
                {
                    if (i == originalBatchRecipient) continue; // skip observer that received the current batch

                    if (i == lastBroadcastIndex)
                        this.Observers[i].OnNext(broadcastMaster);
                    else
                    {
                        this.pool.Get(out StreamMessage<TKey, TSpray> broadcastClone);
                        broadcastClone.CloneFrom(broadcastMaster);
                        this.Observers[i].OnNext(broadcastClone);
                    }
                }
            }
        }

        public override void ProduceQueryPlan(PlanNode previous)
        {
            var node = new SprayPlanNode(
                previous, this, typeof(TKey), typeof(TSpray), this.totalBranches, this.multicast, this.spraySortOrderComparer, false);
            this.Observers.ForEach(o => o.ProduceQueryPlan(node));
        }

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }
}
