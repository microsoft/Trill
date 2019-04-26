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
    internal sealed class PartitionedShuffleNestedPipe<TOuterKey, TSource, TInnerKey> :
        Pipe<CompoundGroupKey<TOuterKey, TInnerKey>, TSource>, IStreamObserverAndNestedGroupedStreamObservable<TOuterKey, TSource, TInnerKey>
    {
        [SchemaSerialization]
        private readonly Expression<Func<TSource, TInnerKey>> keySelector;
        private readonly Func<TSource, TInnerKey> keySelectorFunc;
        [SchemaSerialization]
        private readonly Expression<Func<TInnerKey, int>> keyComparer;
        private readonly Func<TInnerKey, int> innerHashCode;

        private readonly bool powerOf2;
        private readonly int totalBranchesL2;
        private readonly int shuffleId;
        private readonly int totalBranchesL2Mask;

        private readonly string errorMessages;
        private readonly MemoryPool<CompoundGroupKey<TOuterKey, TInnerKey>, TSource> l1Pool;

        private readonly List<IStreamObserver<CompoundGroupKey<TOuterKey, TInnerKey>, TSource>> Observers;

        [DataMember]
        private StreamMessage<CompoundGroupKey<TOuterKey, TInnerKey>, TSource>[] batches;

        [Obsolete("Used only by serialization. Do not call directly.")]
            public PartitionedShuffleNestedPipe() { }

            public PartitionedShuffleNestedPipe(
                ShuffleNestedStreamable<TOuterKey, TSource, TInnerKey> stream,
                IStreamObserver<CompoundGroupKey<TOuterKey, TInnerKey>, TSource> observer,
                int totalBranchesL2, int shuffleId)
            : base(stream, observer)
        {
            this.keySelector = stream.KeySelector;
            this.keySelectorFunc = this.keySelector.Compile();
            this.keyComparer = ((CompoundGroupKeyEqualityComparer<TOuterKey, TInnerKey>)stream.Properties.KeyEqualityComparer).innerComparer.GetGetHashCodeExpr();
            this.innerHashCode = this.keyComparer.Compile();
            this.totalBranchesL2 = totalBranchesL2;
            this.powerOf2 = ((totalBranchesL2 & (totalBranchesL2 - 1)) == 0);
            this.totalBranchesL2Mask = totalBranchesL2 - 1;
            this.shuffleId = shuffleId;

            this.errorMessages = stream.ErrorMessages;
            this.l1Pool = MemoryManager.GetMemoryPool<CompoundGroupKey<TOuterKey, TInnerKey>, TSource>(stream.Properties.IsColumnar);

            this.Observers = new List<IStreamObserver<CompoundGroupKey<TOuterKey, TInnerKey>, TSource>>();

            this.batches = new StreamMessage<CompoundGroupKey<TOuterKey, TInnerKey>, TSource>[totalBranchesL2];
            for (int i = 0; i < totalBranchesL2; i++)
            {
                this.l1Pool.Get(out StreamMessage<CompoundGroupKey<TOuterKey, TInnerKey>, TSource> batch);
                batch.Allocate();
                this.batches[i] = batch;
            }
        }

        public void AddObserver(IStreamObserver<CompoundGroupKey<TOuterKey, TInnerKey>, TSource> observer) => this.Observers.Add(observer);

        public override void OnFlush()
        {
            FlushContents();
            for (int j = 0; j < this.totalBranchesL2; j++)
            {
                this.Observers[j].OnFlush();
            }
        }

        public override void OnCompleted()
        {
            for (int j = 0; j < this.totalBranchesL2; j++)
            {
                this.batches[j].Free();
                this.Observers[j].OnCompleted();
            }
        }

        public override void OnError(Exception exception)
        {
            for (int j = 0; j < this.totalBranchesL2; j++)
            {
                this.batches[j].Free();
                this.Observers[j].OnError(exception);
            }
        }

        public unsafe void OnNext(StreamMessage<TOuterKey, TSource> batch)
        {
            var count = batch.Count;
            var srckey = batch.key.col;
            fixed (long* src_bv = batch.bitvector.col)
            fixed (long* src_vsync = batch.vsync.col)
            fixed (long* src_vother = batch.vother.col)
            fixed (int* src_hash = batch.hash.col)
            {
                if (this.powerOf2)
                {
                    for (int i = 0; i < count; i++)
                    {
                        if ((src_bv[i >> 6] & (1L << (i & 0x3f))) != 0)
                        {
                            if (src_vother[i] < 0)
                            {
                                // Add the punctuation or low watermark to all batches in the array
                                for (int batchIndex = 0; batchIndex < this.totalBranchesL2; batchIndex++)
                                {
                                    var bb = this.batches[batchIndex];
                                    var cc = bb.Count;
                                    bb.vsync.col[cc] = src_vsync[i];
                                    bb.vother.col[cc] = src_vother[i];
                                    if (src_vother[i] == StreamEvent.PunctuationOtherTime)
                                    {
                                        bb.key.col[cc].outerGroup = srckey[i];
                                        bb.hash.col[cc] = src_hash[i];
                                    }
                                    else
                                    {
                                        bb.key.col[cc].outerGroup = default;
                                        bb.hash.col[cc] = 0;
                                    }
                                    bb.key.col[cc].innerGroup = default;
                                    bb.bitvector.col[cc >> 6] |= (1L << (cc & 0x3f));
                                    bb.Count++;

                                    if (cc == Config.DataBatchSize - 1)
                                    {
                                        // flush this batch
                                        if (this.batches[batchIndex].Count > 0)
                                        {
                                            this.batches[batchIndex].iter = this.shuffleId;
                                            this.batches[batchIndex].Seal();
                                            this.Observers[batchIndex].OnNext(this.batches[batchIndex]);
                                            this.l1Pool.Get(out this.batches[batchIndex]);
                                            this.batches[batchIndex].Allocate();
                                        }
                                    }
                                }
                            }
                            continue;
                        }

                        var key = this.keySelectorFunc(batch[i]);
                        var innerHash = this.innerHashCode(key);
                        var hash = src_hash[i] ^ innerHash;

                        var index = hash & this.totalBranchesL2Mask;
                        var b = this.batches[index];

                        var x = b.Count;

                        b.vsync.col[x] = src_vsync[i];
                        b.vother.col[x] = src_vother[i];
                        b.key.col[x].outerGroup = srckey[i];
                        b.key.col[x].innerGroup = key;
                        b.key.col[x].hashCode = hash;
                        b.hash.col[x] = hash;
                        b[x] = batch[i];
                        b.Count++;

                        if (x == Config.DataBatchSize - 1)
                        {
                            // flush this batch
                            if (this.batches[index].Count > 0)
                            {
                                this.batches[index].iter = this.shuffleId;
                                this.batches[index].Seal();
                                this.Observers[index].OnNext(this.batches[index]);
                                this.l1Pool.Get(out this.batches[index]);
                                this.batches[index].Allocate();
                            }
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < count; i++)
                    {
                        if ((src_bv[i >> 6] & (1L << (i & 0x3f))) != 0)
                        {
                            if (src_vother[i] < 0)
                            {
                                // Add the punctuation or low watermark to all batches in the array
                                for (int batchIndex = 0; batchIndex < this.totalBranchesL2; batchIndex++)
                                {
                                    var bb = this.batches[batchIndex];
                                    var cc = bb.Count;
                                    bb.vsync.col[cc] = src_vsync[i];
                                    bb.vother.col[cc] = src_vother[i];
                                    if (src_vother[i] == StreamEvent.PunctuationOtherTime)
                                    {
                                        bb.key.col[cc].outerGroup = srckey[i];
                                        bb.hash.col[cc] = src_hash[i];
                                    }
                                    else
                                    {
                                        bb.key.col[cc].outerGroup = default;
                                        bb.hash.col[cc] = 0;
                                    }
                                    bb.key.col[cc].innerGroup = default;
                                    bb.bitvector.col[cc >> 6] |= (1L << (cc & 0x3f));
                                    bb.Count++;

                                    if (cc == Config.DataBatchSize - 1)
                                    {
                                        // flush this batch
                                        if (this.batches[batchIndex].Count > 0)
                                        {
                                            this.batches[batchIndex].iter = this.shuffleId;
                                            this.batches[batchIndex].Seal();
                                            this.Observers[batchIndex].OnNext(this.batches[batchIndex]);
                                            this.l1Pool.Get(out this.batches[batchIndex]);
                                            this.batches[batchIndex].Allocate();
                                        }
                                    }
                                }
                            }
                            continue;
                        }

                        var key = this.keySelectorFunc(batch[i]);
                        var innerHash = this.innerHashCode(key);
                        var hash = src_hash[i] ^ innerHash;

                        var index = (hash & 0x7fffffff) % this.totalBranchesL2;
                        var b = this.batches[index];

                        var x = b.Count;

                        b.vsync.col[x] = src_vsync[i];
                        b.vother.col[x] = src_vother[i];
                        b.key.col[x].outerGroup = srckey[i];
                        b.key.col[x].innerGroup = key;
                        b.key.col[x].hashCode = hash;
                        b.hash.col[x] = hash;
                        b[x] = batch[i];
                        b.Count++;

                        if (x == Config.DataBatchSize - 1)
                        {
                            // flush this batch
                            if (this.batches[index].Count > 0)
                            {
                                this.batches[index].iter = this.shuffleId;
                                this.batches[index].Seal();
                                this.Observers[index].OnNext(this.batches[index]);
                                this.l1Pool.Get(out this.batches[index]);
                                this.batches[index].Allocate();
                            }
                        }
                    }
                }
            }
            batch.Release();
            batch.Return();
        }

        public override void Checkpoint(System.IO.Stream stream)
        {
            base.Checkpoint(stream);

            // Ensure that we checkpoint the other downstream operators
            for (int i = 1; i < this.Observers.Count; i++)
            {
                this.Observers[i].Checkpoint(stream);
            }
        }

        public override void Restore(System.IO.Stream stream)
        {
            base.Restore(stream);

            // Ensure that we checkpoint the other downstream operators
            for (int i = 1; i < this.Observers.Count; i++)
            {
                this.Observers[i].Restore(stream);
            }
        }

        protected override void FlushContents()
        {
            for (int i = 0; i < this.batches.Length; i++)
            {
                if (this.batches[i].Count == 0) continue;
                this.Observers[i].OnNext(this.batches[i]);
                this.l1Pool.Get(out this.batches[i]);
                this.batches[i].Allocate();
            }
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observers.ForEach(o => o.ProduceQueryPlan(new GroupPlanNode(
                previous,
                this,
                typeof(TOuterKey),
                typeof(CompoundGroupKey<TOuterKey, TInnerKey>),
                typeof(TSource),
                this.keySelector,
                this.shuffleId,
                this.totalBranchesL2,
                true,
                false,
                this.errorMessages)));

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }

    [DataContract]
    internal sealed class ShuffleNestedPipe<TOuterKey, TSource, TInnerKey> :
        Pipe<CompoundGroupKey<TOuterKey, TInnerKey>, TSource>, IStreamObserverAndNestedGroupedStreamObservable<TOuterKey, TSource, TInnerKey>
    {
        [SchemaSerialization]
        private readonly Expression<Func<TSource, TInnerKey>> keySelector;
        private readonly Func<TSource, TInnerKey> keySelectorFunc;
        [SchemaSerialization]
        private readonly Expression<Func<TInnerKey, int>> keyComparer;
        private readonly Func<TInnerKey, int> innerHashCode;

        private readonly bool powerOf2;
        private readonly int totalBranchesL2;
        private readonly int shuffleId;
        private readonly int totalBranchesL2Mask;

        private readonly string errorMessages;
        private readonly MemoryPool<CompoundGroupKey<TOuterKey, TInnerKey>, TSource> l1Pool;

        private readonly List<IStreamObserver<CompoundGroupKey<TOuterKey, TInnerKey>, TSource>> Observers;

        [DataMember]
        private StreamMessage<CompoundGroupKey<TOuterKey, TInnerKey>, TSource>[] batches;

        [Obsolete("Used only by serialization. Do not call directly.")]
            public ShuffleNestedPipe() { }

            public ShuffleNestedPipe(
                ShuffleNestedStreamable<TOuterKey, TSource, TInnerKey> stream,
                IStreamObserver<CompoundGroupKey<TOuterKey, TInnerKey>, TSource> observer,
                int totalBranchesL2, int shuffleId)
            : base(stream, observer)
        {
            this.keySelector = stream.KeySelector;
            this.keySelectorFunc = this.keySelector.Compile();
            this.keyComparer = ((CompoundGroupKeyEqualityComparer<TOuterKey, TInnerKey>)stream.Properties.KeyEqualityComparer).innerComparer.GetGetHashCodeExpr();
            this.innerHashCode = this.keyComparer.Compile();
            this.totalBranchesL2 = totalBranchesL2;
            this.powerOf2 = ((totalBranchesL2 & (totalBranchesL2 - 1)) == 0);
            this.totalBranchesL2Mask = totalBranchesL2 - 1;
            this.shuffleId = shuffleId;

            this.errorMessages = stream.ErrorMessages;
            this.l1Pool = MemoryManager.GetMemoryPool<CompoundGroupKey<TOuterKey, TInnerKey>, TSource>(stream.Properties.IsColumnar);

            this.Observers = new List<IStreamObserver<CompoundGroupKey<TOuterKey, TInnerKey>, TSource>>();

            this.batches = new StreamMessage<CompoundGroupKey<TOuterKey, TInnerKey>, TSource>[totalBranchesL2];
            for (int i = 0; i < totalBranchesL2; i++)
            {
                this.l1Pool.Get(out StreamMessage<CompoundGroupKey<TOuterKey, TInnerKey>, TSource> batch);
                batch.Allocate();
                this.batches[i] = batch;
            }
        }

        public void AddObserver(IStreamObserver<CompoundGroupKey<TOuterKey, TInnerKey>, TSource> observer) => this.Observers.Add(observer);

        public override void OnFlush()
        {
            FlushContents();
            for (int j = 0; j < this.totalBranchesL2; j++)
            {
                this.Observers[j].OnFlush();
            }
        }

        public override void OnCompleted()
        {
            for (int j = 0; j < this.totalBranchesL2; j++)
            {
                this.batches[j].Free();
                this.Observers[j].OnCompleted();
            }
        }

        public override void OnError(Exception exception)
        {
            for (int j = 0; j < this.totalBranchesL2; j++)
            {
                this.batches[j].Free();
                this.Observers[j].OnError(exception);
            }
        }

        public unsafe void OnNext(StreamMessage<TOuterKey, TSource> batch)
        {
            var count = batch.Count;
            var srckey = batch.key.col;
            fixed (long* src_bv = batch.bitvector.col)
            fixed (long* src_vsync = batch.vsync.col)
            fixed (long* src_vother = batch.vother.col)
            fixed (int* src_hash = batch.hash.col)
            {
                if (this.powerOf2)
                {
                    for (int i = 0; i < count; i++)
                    {
                        if ((src_bv[i >> 6] & (1L << (i & 0x3f))) != 0)
                        {
                            if (src_vother[i] < 0)
                            {
                                // Add the punctuation to all batches in the array
                                for (int batchIndex = 0; batchIndex < this.totalBranchesL2; batchIndex++)
                                {
                                    var bb = this.batches[batchIndex];
                                    var cc = bb.Count;
                                    bb.vsync.col[cc] = src_vsync[i];
                                    bb.vother.col[cc] = src_vother[i];
                                    bb.key.col[cc] = default;
                                    bb.hash.col[cc] = 0;
                                    bb.bitvector.col[cc >> 6] |= (1L << (cc & 0x3f));
                                    bb.Count++;

                                    if (cc == Config.DataBatchSize - 1)
                                    {
                                        // flush this batch
                                        if (this.batches[batchIndex].Count > 0)
                                        {
                                            this.batches[batchIndex].iter = this.shuffleId;
                                            this.batches[batchIndex].Seal();
                                            this.Observers[batchIndex].OnNext(this.batches[batchIndex]);
                                            this.l1Pool.Get(out this.batches[batchIndex]);
                                            this.batches[batchIndex].Allocate();
                                        }
                                    }
                                }
                            }
                            continue;
                        }

                        var key = this.keySelectorFunc(batch[i]);
                        var innerHash = this.innerHashCode(key);
                        var hash = src_hash[i] ^ innerHash;

                        var index = hash & this.totalBranchesL2Mask;
                        var b = this.batches[index];

                        var x = b.Count;

                        b.vsync.col[x] = src_vsync[i];
                        b.vother.col[x] = src_vother[i];
                        b.key.col[x].outerGroup = srckey[i];
                        b.key.col[x].innerGroup = key;
                        b.key.col[x].hashCode = hash;
                        b.hash.col[x] = hash;
                        b[x] = batch[i];
                        b.Count++;

                        if (x == Config.DataBatchSize - 1)
                        {
                            // flush this batch
                            if (this.batches[index].Count > 0)
                            {
                                this.batches[index].iter = this.shuffleId;
                                this.batches[index].Seal();
                                this.Observers[index].OnNext(this.batches[index]);
                                this.l1Pool.Get(out this.batches[index]);
                                this.batches[index].Allocate();
                            }
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < count; i++)
                    {
                        if ((src_bv[i >> 6] & (1L << (i & 0x3f))) != 0)
                        {
                            if (src_vother[i] < 0)
                            {
                                // Add the punctuation to all batches in the array
                                for (int batchIndex = 0; batchIndex < this.totalBranchesL2; batchIndex++)
                                {
                                    var bb = this.batches[batchIndex];
                                    var cc = bb.Count;
                                    bb.vsync.col[cc] = src_vsync[i];
                                    bb.vother.col[cc] = src_vother[i];
                                    bb.key.col[cc] = default;
                                    bb.hash.col[cc] = 0;
                                    bb.bitvector.col[cc >> 6] |= (1L << (cc & 0x3f));
                                    bb.Count++;

                                    if (cc == Config.DataBatchSize - 1)
                                    {
                                        // flush this batch
                                        if (this.batches[batchIndex].Count > 0)
                                        {
                                            this.batches[batchIndex].iter = this.shuffleId;
                                            this.batches[batchIndex].Seal();
                                            this.Observers[batchIndex].OnNext(this.batches[batchIndex]);
                                            this.l1Pool.Get(out this.batches[batchIndex]);
                                            this.batches[batchIndex].Allocate();
                                        }
                                    }
                                }
                            }
                            continue;
                        }

                        var key = this.keySelectorFunc(batch[i]);
                        var innerHash = this.innerHashCode(key);
                        var hash = src_hash[i] ^ innerHash;

                        var index = (hash & 0x7fffffff) % this.totalBranchesL2;
                        var b = this.batches[index];

                        var x = b.Count;

                        b.vsync.col[x] = src_vsync[i];
                        b.vother.col[x] = src_vother[i];
                        b.key.col[x].outerGroup = srckey[i];
                        b.key.col[x].innerGroup = key;
                        b.key.col[x].hashCode = hash;
                        b.hash.col[x] = hash;
                        b[x] = batch[i];
                        b.Count++;

                        if (x == Config.DataBatchSize - 1)
                        {
                            // flush this batch
                            if (this.batches[index].Count > 0)
                            {
                                this.batches[index].iter = this.shuffleId;
                                this.batches[index].Seal();
                                this.Observers[index].OnNext(this.batches[index]);
                                this.l1Pool.Get(out this.batches[index]);
                                this.batches[index].Allocate();
                            }
                        }
                    }
                }
            }
            batch.Release();
            batch.Return();
        }

        public override void Checkpoint(System.IO.Stream stream)
        {
            base.Checkpoint(stream);

            // Ensure that we checkpoint the other downstream operators
            for (int i = 1; i < this.Observers.Count; i++)
            {
                this.Observers[i].Checkpoint(stream);
            }
        }

        public override void Restore(System.IO.Stream stream)
        {
            base.Restore(stream);

            // Ensure that we checkpoint the other downstream operators
            for (int i = 1; i < this.Observers.Count; i++)
            {
                this.Observers[i].Restore(stream);
            }
        }

        protected override void FlushContents()
        {
            for (int i = 0; i < this.batches.Length; i++)
            {
                if (this.batches[i].Count == 0) continue;
                this.Observers[i].OnNext(this.batches[i]);
                this.l1Pool.Get(out this.batches[i]);
                this.batches[i].Allocate();
            }
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observers.ForEach(o => o.ProduceQueryPlan(new GroupPlanNode(
                previous,
                this,
                typeof(TOuterKey),
                typeof(CompoundGroupKey<TOuterKey, TInnerKey>),
                typeof(TSource),
                this.keySelector,
                this.shuffleId,
                this.totalBranchesL2,
                true,
                false,
                this.errorMessages)));

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }

    [DataContract]
    internal sealed class ShufflePipe<TOuterKey, TSource, TInnerKey> :
        Pipe<TInnerKey, TSource>, IStreamObserverAndGroupedStreamObservable<TOuterKey, TSource, TInnerKey>
    {
        [SchemaSerialization]
        private readonly Expression<Func<TSource, TInnerKey>> keySelector;
        private readonly Func<TSource, TInnerKey> keySelectorFunc;
        [SchemaSerialization]
        private readonly Expression<Func<TInnerKey, int>> keyComparer;
        private readonly Func<TInnerKey, int> innerHashCode;

        private readonly bool powerOf2;
        private readonly int totalBranchesL2;
        private readonly int shuffleId;
        private readonly int totalBranchesL2Mask;

        private readonly string errorMessages;
        private readonly MemoryPool<TInnerKey, TSource> l1Pool;

        private readonly List<IStreamObserver<TInnerKey, TSource>> Observers;

        [DataMember]
        private StreamMessage<TInnerKey, TSource>[] batches;

        [Obsolete("Used only by serialization. Do not call directly.")]
            public ShufflePipe() { }

            public ShufflePipe(
                ShuffleStreamable<TOuterKey, TSource, TInnerKey> stream,
                IStreamObserver<TInnerKey, TSource> observer,
                int totalBranchesL2, int shuffleId)
            : base(stream, observer)
        {
            this.keySelector = stream.KeySelector;
            this.keySelectorFunc = this.keySelector.Compile();
            this.keyComparer = stream.Properties.KeyEqualityComparer.GetGetHashCodeExpr();
            this.innerHashCode = this.keyComparer.Compile();
            this.totalBranchesL2 = totalBranchesL2;
            this.powerOf2 = ((totalBranchesL2 & (totalBranchesL2 - 1)) == 0);
            this.totalBranchesL2Mask = totalBranchesL2 - 1;
            this.shuffleId = shuffleId;

            this.errorMessages = stream.ErrorMessages;
            this.l1Pool = MemoryManager.GetMemoryPool<TInnerKey, TSource>(stream.Properties.IsColumnar);

            this.Observers = new List<IStreamObserver<TInnerKey, TSource>>();

            this.batches = new StreamMessage<TInnerKey, TSource>[totalBranchesL2];
            for (int i = 0; i < totalBranchesL2; i++)
            {
                this.l1Pool.Get(out StreamMessage<TInnerKey, TSource> batch);
                batch.Allocate();
                this.batches[i] = batch;
            }
        }

        public void AddObserver(IStreamObserver<TInnerKey, TSource> observer) => this.Observers.Add(observer);

        public override void OnFlush()
        {
            FlushContents();
            for (int j = 0; j < this.totalBranchesL2; j++)
            {
                this.Observers[j].OnFlush();
            }
        }

        public override void OnCompleted()
        {
            for (int j = 0; j < this.totalBranchesL2; j++)
            {
                this.batches[j].Free();
                this.Observers[j].OnCompleted();
            }
        }

        public override void OnError(Exception exception)
        {
            for (int j = 0; j < this.totalBranchesL2; j++)
            {
                this.batches[j].Free();
                this.Observers[j].OnError(exception);
            }
        }

        public unsafe void OnNext(StreamMessage<TOuterKey, TSource> batch)
        {
            var count = batch.Count;
            fixed (long* src_bv = batch.bitvector.col)
            fixed (long* src_vsync = batch.vsync.col)
            fixed (long* src_vother = batch.vother.col)
            fixed (int* src_hash = batch.hash.col)
            {
                if (this.powerOf2)
                {
                    for (int i = 0; i < count; i++)
                    {
                        if ((src_bv[i >> 6] & (1L << (i & 0x3f))) != 0)
                        {
                            if (src_vother[i] < 0)
                            {
                                // Add the punctuation to all batches in the array
                                for (int batchIndex = 0; batchIndex < this.totalBranchesL2; batchIndex++)
                                {
                                    var bb = this.batches[batchIndex];
                                    var cc = bb.Count;
                                    bb.vsync.col[cc] = src_vsync[i];
                                    bb.vother.col[cc] = src_vother[i];
                                    bb.key.col[cc] = default;
                                    bb.hash.col[cc] = 0;
                                    bb.bitvector.col[cc >> 6] |= (1L << (cc & 0x3f));
                                    bb.Count++;

                                    if (cc == Config.DataBatchSize - 1)
                                    {
                                        // flush this batch
                                        if (this.batches[batchIndex].Count > 0)
                                        {
                                            this.batches[batchIndex].iter = this.shuffleId;
                                            this.batches[batchIndex].Seal();
                                            this.Observers[batchIndex].OnNext(this.batches[batchIndex]);
                                            this.l1Pool.Get(out this.batches[batchIndex]);
                                            this.batches[batchIndex].Allocate();
                                        }
                                    }
                                }
                            }
                            continue;
                        }

                        var key = this.keySelectorFunc(batch[i]);
                        var innerHash = this.innerHashCode(key);
                        var hash = src_hash[i] ^ innerHash;

                        var index = hash & this.totalBranchesL2Mask;
                        var b = this.batches[index];

                        var x = b.Count;

                        b.vsync.col[x] = src_vsync[i];
                        b.vother.col[x] = src_vother[i];
                        b.key.col[x] = key;
                        b.hash.col[x] = this.innerHashCode(key);
                        b[x] = batch[i];
                        b.Count++;

                        if (x == Config.DataBatchSize - 1)
                        {
                            // flush this batch
                            if (this.batches[index].Count > 0)
                            {
                                this.batches[index].iter = this.shuffleId;
                                this.batches[index].Seal();
                                this.Observers[index].OnNext(this.batches[index]);
                                this.l1Pool.Get(out this.batches[index]);
                                this.batches[index].Allocate();
                            }
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < count; i++)
                    {
                        if ((src_bv[i >> 6] & (1L << (i & 0x3f))) != 0)
                        {
                            if (src_vother[i] < 0)
                            {
                                // Add the punctuation to all batches in the array
                                for (int batchIndex = 0; batchIndex < this.totalBranchesL2; batchIndex++)
                                {
                                    var bb = this.batches[batchIndex];
                                    var cc = bb.Count;
                                    bb.vsync.col[cc] = src_vsync[i];
                                    bb.vother.col[cc] = src_vother[i];
                                    bb.key.col[cc] = default;
                                    bb.hash.col[cc] = 0;
                                    bb.bitvector.col[cc >> 6] |= (1L << (cc & 0x3f));
                                    bb.Count++;

                                    if (cc == Config.DataBatchSize - 1)
                                    {
                                        // flush this batch
                                        if (this.batches[batchIndex].Count > 0)
                                        {
                                            this.batches[batchIndex].iter = this.shuffleId;
                                            this.batches[batchIndex].Seal();
                                            this.Observers[batchIndex].OnNext(this.batches[batchIndex]);
                                            this.l1Pool.Get(out this.batches[batchIndex]);
                                            this.batches[batchIndex].Allocate();
                                        }
                                    }
                                }
                            }
                            continue;
                        }

                        var key = this.keySelectorFunc(batch[i]);
                        var innerHash = this.innerHashCode(key);
                        var hash = src_hash[i] ^ innerHash;

                        var index = (hash & 0x7fffffff) % this.totalBranchesL2;
                        var b = this.batches[index];

                        var x = b.Count;

                        b.vsync.col[x] = src_vsync[i];
                        b.vother.col[x] = src_vother[i];
                        b.key.col[x] = key;
                        b.hash.col[x] = this.innerHashCode(key);
                        b[x] = batch[i];
                        b.Count++;

                        if (x == Config.DataBatchSize - 1)
                        {
                            // flush this batch
                            if (this.batches[index].Count > 0)
                            {
                                this.batches[index].iter = this.shuffleId;
                                this.batches[index].Seal();
                                this.Observers[index].OnNext(this.batches[index]);
                                this.l1Pool.Get(out this.batches[index]);
                                this.batches[index].Allocate();
                            }
                        }
                    }
                }
            }
            batch.Release();
            batch.Return();
        }

        public override void Checkpoint(System.IO.Stream stream)
        {
            base.Checkpoint(stream);

            // Ensure that we checkpoint the other downstream operators
            for (int i = 1; i < this.Observers.Count; i++)
            {
                this.Observers[i].Checkpoint(stream);
            }
        }

        public override void Restore(System.IO.Stream stream)
        {
            base.Restore(stream);

            // Ensure that we checkpoint the other downstream operators
            for (int i = 1; i < this.Observers.Count; i++)
            {
                this.Observers[i].Restore(stream);
            }
        }

        protected override void FlushContents()
        {
            for (int i = 0; i < this.batches.Length; i++)
            {
                if (this.batches[i].Count == 0) continue;
                this.Observers[i].OnNext(this.batches[i]);
                this.l1Pool.Get(out this.batches[i]);
                this.batches[i].Allocate();
            }
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observers.ForEach(o => o.ProduceQueryPlan(new GroupPlanNode(
                previous,
                this,
                typeof(TOuterKey),
                typeof(TInnerKey),
                typeof(TSource),
                this.keySelector,
                this.shuffleId,
                this.totalBranchesL2,
                true,
                false,
                this.errorMessages)));

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }

    [DataContract]
    internal sealed class ShuffleSameKeyPipe<TOuterKey, TSource, TInnerKey> :
        Pipe<TOuterKey, TSource>, IStreamObserverAndSameKeyGroupedStreamObservable<TOuterKey, TSource, TOuterKey>
    {

        private readonly bool powerOf2;
        private readonly int totalBranchesL2;
        private readonly int shuffleId;
        private readonly int totalBranchesL2Mask;

        private readonly string errorMessages;
        private readonly MemoryPool<TOuterKey, TSource> l1Pool;

        private readonly List<IStreamObserver<TOuterKey, TSource>> Observers;

        [DataMember]
        private StreamMessage<TOuterKey, TSource>[] batches;

        [Obsolete("Used only by serialization. Do not call directly.")]
            public ShuffleSameKeyPipe() { }

            public ShuffleSameKeyPipe(
                ShuffleSameKeyStreamable<TOuterKey, TSource, TInnerKey> stream,
                IStreamObserver<TOuterKey, TSource> observer,
                int totalBranchesL2, int shuffleId)
            : base(stream, observer)
        {
            this.totalBranchesL2 = totalBranchesL2;
            this.powerOf2 = ((totalBranchesL2 & (totalBranchesL2 - 1)) == 0);
            this.totalBranchesL2Mask = totalBranchesL2 - 1;
            this.shuffleId = shuffleId;

            this.errorMessages = stream.ErrorMessages;
            this.l1Pool = MemoryManager.GetMemoryPool<TOuterKey, TSource>(stream.Properties.IsColumnar);

            this.Observers = new List<IStreamObserver<TOuterKey, TSource>>();

            this.batches = new StreamMessage<TOuterKey, TSource>[totalBranchesL2];
            for (int i = 0; i < totalBranchesL2; i++)
            {
                this.l1Pool.Get(out StreamMessage<TOuterKey, TSource> batch);
                batch.Allocate();
                this.batches[i] = batch;
            }
        }

        public void AddObserver(IStreamObserver<TOuterKey, TSource> observer) => this.Observers.Add(observer);

        public override void OnFlush()
        {
            FlushContents();
            for (int j = 0; j < this.totalBranchesL2; j++)
            {
                this.Observers[j].OnFlush();
            }
        }

        public override void OnCompleted()
        {
            for (int j = 0; j < this.totalBranchesL2; j++)
            {
                this.batches[j].Free();
                this.Observers[j].OnCompleted();
            }
        }

        public override void OnError(Exception exception)
        {
            for (int j = 0; j < this.totalBranchesL2; j++)
            {
                this.batches[j].Free();
                this.Observers[j].OnError(exception);
            }
        }

        public unsafe void OnNext(StreamMessage<TOuterKey, TSource> batch)
        {
            var count = batch.Count;
            var srckey = batch.key.col;
            fixed (long* src_bv = batch.bitvector.col)
            fixed (long* src_vsync = batch.vsync.col)
            fixed (long* src_vother = batch.vother.col)
            fixed (int* src_hash = batch.hash.col)
            {
                if (this.powerOf2)
                {
                    for (int i = 0; i < count; i++)
                    {
                        if ((src_bv[i >> 6] & (1L << (i & 0x3f))) != 0)
                        {
                            if (src_vother[i] < 0)
                            {
                                // Add the punctuation to all batches in the array
                                for (int batchIndex = 0; batchIndex < this.totalBranchesL2; batchIndex++)
                                {
                                    var bb = this.batches[batchIndex];
                                    var cc = bb.Count;
                                    bb.vsync.col[cc] = src_vsync[i];
                                    bb.vother.col[cc] = src_vother[i];
                                    bb.key.col[cc] = default;
                                    bb.hash.col[cc] = 0;
                                    bb.bitvector.col[cc >> 6] |= (1L << (cc & 0x3f));
                                    bb.Count++;

                                    if (cc == Config.DataBatchSize - 1)
                                    {
                                        // flush this batch
                                        if (this.batches[batchIndex].Count > 0)
                                        {
                                            this.batches[batchIndex].iter = this.shuffleId;
                                            this.batches[batchIndex].Seal();
                                            this.Observers[batchIndex].OnNext(this.batches[batchIndex]);
                                            this.l1Pool.Get(out this.batches[batchIndex]);
                                            this.batches[batchIndex].Allocate();
                                        }
                                    }
                                }
                            }
                            continue;
                        }

                        var hash = src_hash[i];

                        var index = hash & this.totalBranchesL2Mask;
                        var b = this.batches[index];

                        var x = b.Count;

                        b.vsync.col[x] = src_vsync[i];
                        b.vother.col[x] = src_vother[i];
                        b.key.col[x] = srckey[i];
                        b.hash.col[x] = hash;
                        b[x] = batch[i];
                        b.Count++;

                        if (x == Config.DataBatchSize - 1)
                        {
                            // flush this batch
                            if (this.batches[index].Count > 0)
                            {
                                this.batches[index].iter = this.shuffleId;
                                this.batches[index].Seal();
                                this.Observers[index].OnNext(this.batches[index]);
                                this.l1Pool.Get(out this.batches[index]);
                                this.batches[index].Allocate();
                            }
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < count; i++)
                    {
                        if ((src_bv[i >> 6] & (1L << (i & 0x3f))) != 0)
                        {
                            if (src_vother[i] < 0)
                            {
                                // Add the punctuation to all batches in the array
                                for (int batchIndex = 0; batchIndex < this.totalBranchesL2; batchIndex++)
                                {
                                    var bb = this.batches[batchIndex];
                                    var cc = bb.Count;
                                    bb.vsync.col[cc] = src_vsync[i];
                                    bb.vother.col[cc] = src_vother[i];
                                    bb.key.col[cc] = default;
                                    bb.hash.col[cc] = 0;
                                    bb.bitvector.col[cc >> 6] |= (1L << (cc & 0x3f));
                                    bb.Count++;

                                    if (cc == Config.DataBatchSize - 1)
                                    {
                                        // flush this batch
                                        if (this.batches[batchIndex].Count > 0)
                                        {
                                            this.batches[batchIndex].iter = this.shuffleId;
                                            this.batches[batchIndex].Seal();
                                            this.Observers[batchIndex].OnNext(this.batches[batchIndex]);
                                            this.l1Pool.Get(out this.batches[batchIndex]);
                                            this.batches[batchIndex].Allocate();
                                        }
                                    }
                                }
                            }
                            continue;
                        }

                        var hash = src_hash[i];

                        var index = (hash & 0x7fffffff) % this.totalBranchesL2;
                        var b = this.batches[index];

                        var x = b.Count;

                        b.vsync.col[x] = src_vsync[i];
                        b.vother.col[x] = src_vother[i];
                        b.key.col[x] = srckey[i];
                        b.hash.col[x] = hash;
                        b[x] = batch[i];
                        b.Count++;

                        if (x == Config.DataBatchSize - 1)
                        {
                            // flush this batch
                            if (this.batches[index].Count > 0)
                            {
                                this.batches[index].iter = this.shuffleId;
                                this.batches[index].Seal();
                                this.Observers[index].OnNext(this.batches[index]);
                                this.l1Pool.Get(out this.batches[index]);
                                this.batches[index].Allocate();
                            }
                        }
                    }
                }
            }
            batch.Release();
            batch.Return();
        }

        public override void Checkpoint(System.IO.Stream stream)
        {
            base.Checkpoint(stream);

            // Ensure that we checkpoint the other downstream operators
            for (int i = 1; i < this.Observers.Count; i++)
            {
                this.Observers[i].Checkpoint(stream);
            }
        }

        public override void Restore(System.IO.Stream stream)
        {
            base.Restore(stream);

            // Ensure that we checkpoint the other downstream operators
            for (int i = 1; i < this.Observers.Count; i++)
            {
                this.Observers[i].Restore(stream);
            }
        }

        protected override void FlushContents()
        {
            for (int i = 0; i < this.batches.Length; i++)
            {
                if (this.batches[i].Count == 0) continue;
                this.Observers[i].OnNext(this.batches[i]);
                this.l1Pool.Get(out this.batches[i]);
                this.batches[i].Allocate();
            }
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observers.ForEach(o => o.ProduceQueryPlan(new GroupPlanNode(
                previous,
                this,
                typeof(TOuterKey),
                typeof(TOuterKey),
                typeof(TSource),
                null,
                this.shuffleId,
                this.totalBranchesL2,
                true,
                false,
                this.errorMessages)));

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }
}