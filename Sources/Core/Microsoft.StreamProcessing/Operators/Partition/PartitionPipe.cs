// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class PartitionPipe<TPartitionKey, TPayload> :
        Pipe<PartitionKey<TPartitionKey>, TPayload>, IStreamObserver<Empty, TPayload>
    {
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, TPartitionKey>> keySelector;
        private readonly Func<TPayload, TPartitionKey> keySelectorFunc;
        [SchemaSerialization]
        private readonly long partitionLag;

        private readonly Func<TPartitionKey, int> keyComparerGetHashCode;
        private readonly MemoryPool<PartitionKey<TPartitionKey>, TPayload> l1Pool;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public PartitionPipe() { }

        public PartitionPipe(
            PartitionStreamable<TPartitionKey, TPayload> stream,
            IStreamObserver<PartitionKey<TPartitionKey>, TPayload> observer)
            : base(stream, observer)
        {
            this.keyComparerGetHashCode = EqualityComparerExpression<TPartitionKey>.DefaultGetHashCodeFunction;
            this.keySelector = stream.KeySelector;
            this.keySelectorFunc = this.keySelector.Compile();
            this.partitionLag = stream.PartitionLag;
            this.l1Pool = MemoryManager.GetMemoryPool<PartitionKey<TPartitionKey>, TPayload>(stream.Properties.IsColumnar);
        }

        public unsafe void OnNext(StreamMessage<Empty, TPayload> batch)
        {
            this.l1Pool.Get(out StreamMessage<PartitionKey<TPartitionKey>, TPayload> outputBatch);
            outputBatch.vsync = this.partitionLag > 0
                ? batch.vsync.MakeWritable(this.l1Pool.longPool)
                : batch.vsync;
            outputBatch.vother = batch.vother.MakeWritable(this.l1Pool.longPool);
            outputBatch.payload = batch.payload;
            outputBatch.hash = batch.hash.MakeWritable(this.l1Pool.intPool);
            outputBatch.bitvector = batch.bitvector;
            this.l1Pool.GetKey(out outputBatch.key);

            outputBatch.Count = batch.Count;

            var count = batch.Count;

            var destKey = outputBatch.key.col;
            var destPayload = outputBatch;
            fixed (int* destHash = outputBatch.hash.col)
            {
                for (int i = 0; i < count; i++)
                {
                    if ((batch.bitvector.col[i >> 6] & (1L << (i & 0x3f))) != 0 &&
                        batch.vother.col[i] == StreamEvent.PunctuationOtherTime)
                    {
                        if (this.partitionLag > 0)
                        {
                            outputBatch.vsync.col[i] = batch.vsync.col[i] - this.partitionLag;
                        }
                        outputBatch.vother.col[i] = PartitionedStreamEvent.LowWatermarkOtherTime;
                        continue;
                    }

                    var key = this.keySelectorFunc(destPayload[i]);
                    destKey[i] = new PartitionKey<TPartitionKey> { Key = key };
                    destHash[i] = this.keyComparerGetHashCode(key);
                }
            }

            batch.key.Return();
            batch.Return();
            this.Observer.OnNext(outputBatch);
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new PartitionPlanNode(
                previous,
                this,
                typeof(PartitionKey<TPartitionKey>),
                typeof(TPayload), this.keySelector, this.partitionLag));

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }
}