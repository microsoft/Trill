// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.IO;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Serializer;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class BinaryIngressStreamablePassive<TKey, TPayload> : ConnectableStreamableBase<TKey, TPayload>, IPassiveIngressStreamable<TKey, TPayload>, IIngressStreamObserver
    {
        private bool restored;
        private readonly Stream stream;
        private readonly QueryContainer container;
        private readonly StateSerializer<QueuedMessage<StreamMessage<TKey, TPayload>>> serializer;

        public BinaryIngressStreamablePassive(Stream binaryStream, StreamProperties<TKey, TPayload> inputProperties, bool readPropertiesFromStream, QueryContainer container, string identifier)
            : base((inputProperties ?? StreamProperties<TKey, TPayload>.Default).SetQueryContainer(container))
        {
            if (readPropertiesFromStream)
            {
                var propSer = StreamableSerializer.Create<SerializedProperties>();
                var props = propSer.Deserialize(binaryStream);
                this.properties = props.ToStreamProperties<TKey, TPayload>();
            }

            this.stream = binaryStream;
            this.serializer = StreamableSerializer.Create<QueuedMessage<StreamMessage<TKey, TPayload>>>(new SerializerSettings());
            this.container = container;
            this.IngressSiteIdentifier = identifier ?? Guid.NewGuid().ToString();
            container?.RegisterIngressSite(identifier);
            this.restored = container == null;
        }

        public string IngressSiteIdentifier { get; private set; }

        public int CurrentlyBufferedReorderCount => 0;

        public int CurrentlyBufferedStartEdgeCount => 0;

        public IDisposable DelayedDisposable => this;

        public override IDisposable Connect() => this;

        public void Flush() => this.stream.Flush();

        public void Dispose() => this.stream.Dispose();

        public void Enable() => this.restored = true;

        public override void Restore(Stream stream)
        {
            base.Restore(stream);
            Enable();
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.produceQueryPlan(new IngressPlanNode(this, typeof(TKey), typeof(TPayload), false, null));

        public override IDisposable Subscribe(IStreamObserver<TKey, TPayload> observer)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            this.container?.RegisterIngressPipe(this.IngressSiteIdentifier, this);
            return base.Subscribe(observer);
        }

        public bool Trigger()
        {
            if (this.produceQueryPlan == null)
                throw new IngressException("Attempted to trigger a pull without any subscribers to ingress.");
            if (!this.restored)
                throw new IngressException("Attempted to trigger a pull without having restored from query checkpoint.");

            bool done = false;

            try
            {
                var message = this.serializer.Deserialize(this.stream);
                if (message.Kind == MessageKind.Completed)
                {
                    done = true;
                }
                OnNext(message.Message);
            }
            catch (Exception e)
            {
                OnError(e);
                throw;
            }
            return !done;
        }
    }
}