// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;

namespace Microsoft.StreamProcessing
{
    internal sealed class StreamMessageIngressStreamable<TPayload> : Streamable<Empty, TPayload>
    {
        private readonly IObservable<StreamMessage<Empty, TPayload>> source;
        private readonly bool delayed;

        private readonly string identifier;
        private readonly QueryContainer container;

        public StreamMessageIngressStreamable(IObservable<StreamMessage<Empty, TPayload>> source, StreamProperties<Empty, TPayload> properties, QueryContainer container, string identifier)
            : base(properties.SetQueryContainer(container))
        {
            Contract.Requires(source != null);

            this.source = source;
            this.container = container;
            this.delayed = container != null;

            this.identifier = identifier;
            if (this.delayed) container.RegisterIngressSite(identifier);
        }

        public override IDisposable Subscribe(IStreamObserver<Empty, TPayload> observer)
        {
            Contract.EnsuresOnThrow<IngressException>(true);
            var subscription = new StreamMessageIngressSubscription<TPayload>(this.source, this.identifier, this, observer);

            if (this.delayed)
            {
                this.container.RegisterIngressPipe(this.identifier, subscription);
                return subscription.DelayedDisposable;
            }
            else
            {
                subscription.Enable();
                return subscription;
            }
        }
    }
}