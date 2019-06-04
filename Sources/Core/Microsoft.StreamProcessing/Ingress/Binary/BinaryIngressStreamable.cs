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
        private readonly long streamStartPosition = 0;
        private readonly bool delayed;
        private readonly QueryContainer container;
        private readonly IIngressScheduler scheduler;
        private bool subscriptionActive;

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
            if (this.stream.CanSeek)
            {
                this.streamStartPosition = binaryStream.Position;
            }
            this.numMessages = numMessages;
            this.scheduler = scheduler;
            this.container = container;
            this.IngressSiteIdentifier = identifier ?? Guid.NewGuid().ToString();
            this.delayed = container != null;
            if (this.delayed) container.RegisterIngressSite(identifier);
        }

        internal void OnSubscriptionCompleted()
        {
            if (!this.subscriptionActive)
            {
                throw new InvalidOperationException("Subscription not started");
            }

            // If the stream supports Seek, we can allow another subscription
            if (this.stream.CanSeek)
            {
                this.stream.Seek(this.streamStartPosition, SeekOrigin.Begin);
                this.subscriptionActive = false;
            }
        }

        public string IngressSiteIdentifier { get; }

        public override IDisposable Subscribe(IStreamObserver<TKey, TPayload> observer)
        {
            Contract.EnsuresOnThrow<IngressException>(true);
            if (this.subscriptionActive)
            {
                throw new InvalidOperationException(
                    $"Only one subscription per {nameof(BinaryIngressStreamable<object, object>)} instance " +
                        $"{(this.stream.CanSeek ? "allowed at a time" : "for streams that do not support Seek")}");
            }

            this.subscriptionActive = true;
            var subscription = new BinaryIngressReader<TKey, TPayload>(
                this.IngressSiteIdentifier, this, observer, this.numMessages, this.stream, this.scheduler, this.delayed, OnSubscriptionCompleted);

            if (this.delayed)
            {
                this.container.RegisterIngressPipe(this.IngressSiteIdentifier, subscription);
                return subscription.DelayedDisposable;
            }
            else
            {
                return Utility.EmptyDisposable;
            }
        }
    }
}