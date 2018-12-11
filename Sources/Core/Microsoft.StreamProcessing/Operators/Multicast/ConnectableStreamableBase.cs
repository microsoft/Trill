// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.IO;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// WARNING:: this is not a 'true' connectable because it assumes that Connect happens only after all consumers
    /// have Subscribed (for internal use only, not safe for topical use)
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TPayload"></typeparam>
    internal abstract class ConnectableStreamableBase<TKey, TPayload> : Streamable<TKey, TPayload>, IStreamObserver<TKey, TPayload>, IConnectableStreamable<TKey, TPayload>
    {
        protected readonly MemoryPool<TKey, TPayload> pool;

        private Action<Exception> onError;
        private Action onFlush;
        private Action onCompleted;
        private Action<StreamMessage<TKey, TPayload>> on;
        private Action<Stream> checkpoint;
        private Action<Stream> restore;
        private Action reset;
        protected Action<PlanNode> produceQueryPlan;
        private int count;
        private StreamMessage<TKey, TPayload> batch;

        protected ConnectableStreamableBase(StreamProperties<TKey, TPayload> properties)
            : base(properties)
        {
            Contract.Requires(properties != null);

            this.pool = MemoryManager.GetMemoryPool<TKey, TPayload>(properties.IsColumnar);
            this.ClassId = Guid.NewGuid();
        }

        public abstract IDisposable Connect();

        public override IDisposable Subscribe(IStreamObserver<TKey, TPayload> consumer)
        {
            // assign unique ids to each output
            int offset = this.count++;

            void LocalOn(StreamMessage<TKey, TPayload> batch)
            {
                this.pool.Get(out this.batch);
                this.batch.CloneFrom(batch, false);
                consumer.OnNext(this.batch);
            }

            if (offset == 0)
            {
                this.onError = consumer.OnError;
                this.onFlush = consumer.OnFlush;
                this.onCompleted = consumer.OnCompleted;
                this.on = LocalOn;
                this.checkpoint = consumer.Checkpoint;
                this.restore = consumer.Restore;
                this.reset = consumer.Reset;
                this.produceQueryPlan = consumer.ProduceQueryPlan;
            }
            else
            {
                this.onError += consumer.OnError;
                this.onFlush += consumer.OnFlush;
                this.onCompleted += consumer.OnCompleted;
                this.on += LocalOn;
                this.checkpoint += consumer.Checkpoint;
                this.restore += consumer.Restore;
                this.reset += consumer.Reset;
                this.produceQueryPlan += consumer.ProduceQueryPlan;
            }

            // TODO: return actual disposable that can be used to dispose the corresponding on calls
            return Utility.EmptyDisposable;
        }

        public void OnNext(StreamMessage<TKey, TPayload> batch)
        {
            this.on(batch);
            batch.Free();
        }

        public void OnError(Exception error) => this.onError(error);

        public void Checkpoint(Stream stream) => this.checkpoint(stream);

        public void OnCompleted() => this.onCompleted();

        public void OnFlush() => this.onFlush();

        public virtual void Restore(Stream stream) => this.restore(stream);

        public void Reset() => this.reset();

        public abstract void ProduceQueryPlan(PlanNode previous);

        public int CurrentlyBufferedOutputCount => 0;
        public int CurrentlyBufferedInputCount => 0;

        public Guid ClassId { get; }
    }
}
