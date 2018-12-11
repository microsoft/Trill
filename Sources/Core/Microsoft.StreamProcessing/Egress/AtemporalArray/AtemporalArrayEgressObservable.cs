// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;

namespace Microsoft.StreamProcessing
{
    internal sealed class MonotonicArrayObservable<TPayload> : IObservable<ArraySegment<TPayload>>
    {
        private readonly IStreamable<Empty, TPayload> source;
        private readonly Func<TPayload[]> generator;
        private readonly QueryContainer container;
        private readonly string identifier;

        public MonotonicArrayObservable(
            IStreamable<Empty, TPayload> source,
            Func<TPayload[]> generator,
            QueryContainer container,
            string identifier)
        {
            Contract.Requires(source != null);

            this.source = source;
            this.generator = generator;
            this.container = container;
            this.identifier = identifier;
            if (this.container != null) this.container.RegisterEgressSite(this.identifier);
        }

        public IDisposable Subscribe(IObserver<ArraySegment<TPayload>> observer)
        {
            var pipe = new MonotonicArrayEgressPipe<TPayload>(this.generator, observer, this.container);
            if (this.container != null) this.container.RegisterEgressPipe(this.identifier, pipe);
            return this.source.Subscribe(pipe);
        }
    }
}