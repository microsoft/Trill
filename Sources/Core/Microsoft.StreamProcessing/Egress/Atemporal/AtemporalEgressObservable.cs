// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;

namespace Microsoft.StreamProcessing
{
    internal sealed class MonotonicObservable<TPayload> : IObservable<TPayload>
    {
        private readonly IStreamable<Empty, TPayload> source;
        private readonly QueryContainer container;
        private readonly string identifier;

        public MonotonicObservable(
            IStreamable<Empty, TPayload> source,
            QueryContainer container,
            string identifier)
        {
            Contract.Requires(source != null);

            this.source = source;
            this.container = container;
            this.identifier = identifier;
            if (this.container != null) this.container.RegisterEgressSite(this.identifier);
        }

        public IDisposable Subscribe(IObserver<TPayload> observer)
        {
            var pipe = new MonotonicEgressPipe<TPayload>(
                observer, this.container);
            if (this.container != null) this.container.RegisterEgressPipe(this.identifier, pipe);
            return this.source.Subscribe(pipe);
        }
    }
    internal sealed class ReactiveObservable<TPayload> : IObservable<TPayload>
    {
        private readonly IStreamable<Empty, TPayload> source;
        private readonly QueryContainer container;
        private readonly string identifier;

        public ReactiveObservable(
            IStreamable<Empty, TPayload> source,
            QueryContainer container,
            string identifier)
        {
            Contract.Requires(source != null);

            this.source = source;
            this.container = container;
            this.identifier = identifier;
            if (this.container != null) this.container.RegisterEgressSite(this.identifier);
        }

        public IDisposable Subscribe(IObserver<TPayload> observer)
        {
            var pipe = new ReactiveEgressPipe<TPayload>(
                observer, this.container);
            if (this.container != null) this.container.RegisterEgressPipe(this.identifier, pipe);
            return this.source.Subscribe(pipe);
        }
    }
}