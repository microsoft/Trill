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
    internal sealed class ShufflecastPipe<TSource, TInnerKey> :
        Pipe<TInnerKey, TSource>, IStreamObserverAndGroupedStreamObservable<TInnerKey, TSource, TInnerKey>
    {
        private readonly Func<TInnerKey, int, TSource, int[]> destinationSelectorCompiled;

        private readonly bool powerOf2;
        private readonly int totalBranchesL2;
        private readonly int totalBranchesL2Mask;

        private readonly string errorMessages;
        private readonly MemoryPool<TInnerKey, TSource> l1Pool;

        private readonly ColumnBatch<long> resetBV;

        private readonly List<IStreamObserver<TInnerKey, TSource>> Observers;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public ShufflecastPipe() { }

        public ShufflecastPipe(
            ShufflecastStreamable<TSource, TInnerKey> stream,
            IStreamObserver<TInnerKey, TSource> observer,
            int totalBranchesL2,
            Func<TInnerKey, int, TSource, int[]> destinationSelectorCompiled = null)
            : base(stream, observer)
        {
            this.totalBranchesL2 = totalBranchesL2;
            this.powerOf2 = ((totalBranchesL2 & (totalBranchesL2 - 1)) == 0);
            this.totalBranchesL2Mask = this.totalBranchesL2 - 1;
            this.destinationSelectorCompiled = destinationSelectorCompiled;

            this.errorMessages = stream.ErrorMessages;
            this.l1Pool = MemoryManager.GetMemoryPool<TInnerKey, TSource>(stream.Properties.IsColumnar);
            this.l1Pool.GetBV(out this.resetBV);
            for (int i = 0; i < this.resetBV.col.Length; i++) this.resetBV.col[i] = ~0;
            this.Observers = new List<IStreamObserver<TInnerKey, TSource>>();
        }

        public void AddObserver(IStreamObserver<TInnerKey, TSource> observer) => this.Observers.Add(observer);

        public override void OnCompleted()
        {
            this.resetBV.ReturnClear();
            for (int j = 0; j < this.totalBranchesL2; j++) this.Observers[j].OnCompleted();
        }

        public override void OnFlush()
        {
            for (int j = 0; j < this.totalBranchesL2; j++) this.Observers[j].OnFlush();
        }

        public unsafe void OnNext(StreamMessage<TInnerKey, TSource> batch)
        {
            var batches = new StreamMessage<TInnerKey, TSource>[this.totalBranchesL2];

            int[] counts = new int[this.totalBranchesL2];

            for (int i = 0; i < this.totalBranchesL2; i++)
            {
                this.l1Pool.Get(out batches[i]);

                batches[i].CloneFrom(batch);
                batches[i].bitvector.ReturnClear();
                this.resetBV.IncrementRefCount(1);
                batches[i].bitvector = this.resetBV.MakeWritable(this.l1Pool.bitvectorPool);
            }

            var count = batch.Count;
            fixed (long* src_bv = batch.bitvector.col)
            fixed (int* src_hash = batch.hash.col)
            {
                if (this.destinationSelectorCompiled == null)
                {
                    if (this.powerOf2)
                    {
                        for (int i = 0; i < count; i++)
                        {
                            if ((src_bv[i >> 6] & (1L << (i & 0x3f))) != 0)
                            {
                                if (batch.vother.col[i] < 0)
                                {
                                    AddPunctuationOrLowWatermarkToAllBatches(
                                        batches,
                                        batch.vsync.col[i], batch.vother.col[i], batch.key.col[i], batch.hash.col[i]);
                                }
                                continue;
                            }

                            var index = src_hash[i] & this.totalBranchesL2Mask;

                            batches[index].bitvector.col[i >> 6] &= ~(1L << (i & 0x3f)); // pass only along that branch
                            counts[index] = i + 1;
                        }
                    }
                    else
                    {
                        for (int i = 0; i < count; i++)
                        {
                            if ((src_bv[i >> 6] & (1L << (i & 0x3f))) != 0)
                            {
                                if (batch.vother.col[i] < 0)
                                {
                                    AddPunctuationOrLowWatermarkToAllBatches(
                                        batches,
                                        batch.vsync.col[i], batch.vother.col[i], batch.key.col[i], batch.hash.col[i]);
                                }
                                continue;
                            }

                            var index = (src_hash[i] & 0x7fffffff) % this.totalBranchesL2;

                            batches[index].bitvector.col[i >> 6] &= ~(1L << (i & 0x3f)); // pass only along that branch
                            counts[index] = i + 1;
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < count; i++)
                    {
                        if ((src_bv[i >> 6] & (1L << (i & 0x3f))) != 0)
                        {
                            if (batch.vother.col[i] < 0)
                            {
                                AddPunctuationOrLowWatermarkToAllBatches(
                                    batches,
                                    batch.vsync.col[i], batch.vother.col[i], batch.key.col[i], batch.hash.col[i]);
                            }
                            continue;
                        }

                        var dest = this.destinationSelectorCompiled(batch.key.col[i], src_hash[i], batch.payload.col[i]);
                        for (int j = 0; j < dest.Length; j++)
                        {
                            batches[j].bitvector.col[i >> 6] &= ~(1L << (i & 0x3f)); // pass only along chosen branches
                            counts[j] = i + 1;
                        }
                    }
                }
            }
            batch.Release();
            batch.Return();

            for (int i = 0; i < this.totalBranchesL2; i++)
            {
                batches[i].Count = counts[i];
                if (counts[i] > 0)
                    this.Observers[i].OnNext(batches[i]);
                else
                    batches[i].Free();
            }
        }

        private void AddPunctuationOrLowWatermarkToAllBatches(StreamMessage<TInnerKey, TSource>[] batches, long syncTime, long otherTime, TInnerKey innerKey, int hash)
        {
            for (int batchIndex = 0; batchIndex < this.totalBranchesL2; batchIndex++)
            {
                var count = batches[batchIndex].Count;
                batches[batchIndex].vsync.col[count] = syncTime;
                batches[batchIndex].vother.col[count] = otherTime;
                batches[batchIndex].key.col[count] = innerKey;
                batches[batchIndex].hash.col[count] = hash;
                batches[batchIndex].bitvector.col[count >> 6] |= (1L << (count & 0x3f));
                batches[batchIndex].Count++;

                if (count == Config.DataBatchSize - 1)
                {
                    // flush this batch
                    if (batches[batchIndex].Count > 0)
                    {
                        batches[batchIndex].Seal();
                        this.Observers[batchIndex].OnNext(batches[batchIndex]);
                        this.l1Pool.Get(out StreamMessage<TInnerKey, TSource> batch);
                        batches[batchIndex].Allocate();
                        batches[batchIndex] = batch;
                    }
                }
            }
        }

        public override void ProduceQueryPlan(PlanNode previous)
        {
            var p = Expression.Parameter(typeof(TInnerKey));
            this.Observers.ForEach(o => o.ProduceQueryPlan(new GroupPlanNode(
                previous,
                this,
                typeof(TInnerKey),
                typeof(TInnerKey),
                typeof(TSource),
                Expression.Lambda<Func<TInnerKey, TInnerKey>>(p, p),
                0, this.totalBranchesL2,
                true,
                false, this.errorMessages)));
        }

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }
}
