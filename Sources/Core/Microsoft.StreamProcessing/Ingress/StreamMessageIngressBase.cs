// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.IO;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal abstract class StreamMessageIngressBase<TKey, TPayload> : Pipe<TKey, TPayload>, IIngressStreamObserver
    {
        protected DelayedSubscription subscription;

        protected StreamMessageIngressBase() { }

        protected StreamMessageIngressBase(
            string identifier,
            IStreamable<TKey, TPayload> streamable,
            IStreamObserver<TKey, TPayload> observer)
            : base(streamable, observer)
        {
            this.IngressSiteIdentifier = identifier;
            this.subscription = new DelayedSubscription(this, observer);
        }

        public int CurrentlyBufferedReorderCount => 0;

        public int CurrentlyBufferedStartEdgeCount => 0;

        public override int CurrentlyBufferedInputCount => 0;

        public override int CurrentlyBufferedOutputCount => 0;

        public string IngressSiteIdentifier { get; }

        public IDisposable DelayedDisposable => this.subscription;

        protected override void DisposeState() => this.DelayedDisposable?.Dispose();

        protected abstract IDisposable Action(IStreamObserver<TKey, TPayload> observer);

        public override void Restore(Stream stream)
        {
            if (stream != null) base.Restore(stream);
            Enable();
        }

        public void Enable() => this.subscription.Enable();

        protected sealed class DelayedSubscription : IDisposable
        {
            private IDisposable inner;
            private readonly StreamMessageIngressBase<TKey, TPayload> current;
            private readonly IStreamObserver<TKey, TPayload> observer;
            private bool disposed;

            public DelayedSubscription(
                StreamMessageIngressBase<TKey, TPayload> current,
                IStreamObserver<TKey, TPayload> observer)
            {
                this.current = current;
                this.observer = observer;
            }

            public void Enable()
            {
                this.inner = this.current.Action(this.observer);
                if (this.disposed) DisposeInternal();
            }

            public void Dispose()
            {
                if (this.inner != null) DisposeInternal();
                this.disposed = true;
            }

            private void DisposeInternal() => this.inner.Dispose();
        }
    }
}
