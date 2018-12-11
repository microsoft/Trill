// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.IO;
using Microsoft.StreamProcessing.Serializer;

namespace Microsoft.StreamProcessing
{
    internal sealed class BinaryIngressStreamable<TKey, TPayload> : Streamable<TKey, TPayload>, IIngressStreamable<TKey, TPayload>
    {
        private readonly int numMessages;
        private readonly Stream stream;
        private readonly string identifier;
        private readonly bool delayed;
        private readonly QueryContainer container;
        private IIngressScheduler scheduler;

        public BinaryIngressStreamable(Stream binaryStream, int numMessages, IIngressScheduler scheduler, StreamProperties<TKey, TPayload> inputProperties, bool readPropertiesFromStream, QueryContainer container, string identifier)
            : base((inputProperties ?? StreamProperties<TKey, TPayload>.Default).SetQueryContainer(container))
        {
            if (readPropertiesFromStream)
            {
                var propSer = StreamableSerializer.Create<SerializedProperties>();
                var props = propSer.Deserialize(binaryStream);
                this.properties = props.ToStreamProperties<TKey, TPayload>();
            }

            this.stream = binaryStream;
            this.numMessages = numMessages;
            this.scheduler = scheduler;
            this.container = container;
            this.identifier = identifier ?? Guid.NewGuid().ToString();
            this.delayed = container != null;
            if (this.delayed) container.RegisterIngressSite(identifier);
        }

        public string IngressSiteIdentifier => this.identifier;

        public override IDisposable Subscribe(IStreamObserver<TKey, TPayload> observer)
        {
            Contract.EnsuresOnThrow<IngressException>(true);
            var subscription = new BinaryIngressReader<TKey, TPayload>(this.identifier, this, observer, this.numMessages, this.stream, this.scheduler, this.delayed);

            if (this.delayed)
            {
                this.container.RegisterIngressPipe(this.identifier, subscription);
                return subscription.DelayedDisposable;
            }
            else
            {
                return Utility.EmptyDisposable;
            }
        }
    }
}