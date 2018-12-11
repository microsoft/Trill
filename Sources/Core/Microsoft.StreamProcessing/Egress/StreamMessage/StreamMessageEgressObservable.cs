// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;

namespace Microsoft.StreamProcessing
{
    internal sealed class StreamMessageEgressObservable<TKey, TPayload> : IObservable<StreamMessage<TKey, TPayload>>
    {
        private readonly IStreamable<TKey, TPayload> source;
        private readonly QueryContainer container;
        private readonly string identifier;

        public StreamMessageEgressObservable(
            IStreamable<TKey, TPayload> source,
            QueryContainer container,
            string identifier)
        {
            Contract.Requires(source != null);

            this.source = source;
            this.container = container;
            this.identifier = identifier;
            if (this.container != null) this.container.RegisterEgressSite(this.identifier);
        }

        public IDisposable Subscribe(IObserver<StreamMessage<TKey, TPayload>> observer)
        {
            var pipe = new StreamMessageEgressPipe<TKey, TPayload>(observer, this.container);
            if (this.container != null) this.container.RegisterEgressPipe(this.identifier, pipe);
            return this.source.Subscribe(pipe);
        }
    }
}