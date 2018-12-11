// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{

    internal sealed class StreamEventArrayObservable<TPayload> : IObservable<ArraySegment<StreamEvent<TPayload>>>
    {
        internal readonly IStreamable<Empty, TPayload> source;
        internal readonly Func<StreamEvent<TPayload>[]> generator;
        internal readonly QueryContainer container;
        internal readonly string identifier;

        public StreamEventArrayObservable(
            IStreamable<Empty, TPayload> source,
            Func<StreamEvent<TPayload>[]> generator,
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

        public IDisposable Subscribe(IObserver<ArraySegment<StreamEvent<TPayload>>> observer)
        {
            var pipe = new StreamEventArrayEgressPipe<TPayload>(
                this.generator,
                observer,
                this.container);
            if (this.container != null) this.container.RegisterEgressPipe(this.identifier, pipe);
            return this.source.Subscribe(pipe);
        }
    }

    internal sealed class StartEdgeArrayObservable<TPayload, TResult> : IObservable<ArraySegment<TResult>>
    {
        internal readonly IStreamable<Empty, TPayload> source;
        internal readonly Func<TResult[]> generator;
        internal readonly QueryContainer container;
        internal readonly string identifier;
        internal readonly Expression<Func<long, TPayload, TResult>> constructor;

        public StartEdgeArrayObservable(
            IStreamable<Empty, TPayload> source,
            Func<TResult[]> generator,
            Expression<Func<long, TPayload, TResult>> constructor,
            QueryContainer container,
            string identifier)
        {
            Contract.Requires(source != null);

            this.source = source;
            this.generator = generator;
            this.constructor = constructor;
            this.container = container;
            this.identifier = identifier;
            if (this.container != null) this.container.RegisterEgressSite(this.identifier);
        }

        public IDisposable Subscribe(IObserver<ArraySegment<TResult>> observer)
        {
            var pipe = new StartEdgeArrayEgressPipe<TPayload, TResult>(
                this.generator,
                this.constructor,
                observer,
                this.container);
            if (this.container != null) this.container.RegisterEgressPipe(this.identifier, pipe);
            return this.source.Subscribe(pipe);
        }
    }

    internal sealed class IntervalArrayObservable<TPayload, TResult> : IObservable<ArraySegment<TResult>>
    {
        internal readonly IStreamable<Empty, TPayload> source;
        internal readonly Func<TResult[]> generator;
        internal readonly QueryContainer container;
        internal readonly string identifier;
        internal readonly Expression<Func<long, long, TPayload, TResult>> constructor;

        public IntervalArrayObservable(
            IStreamable<Empty, TPayload> source,
            Func<TResult[]> generator,
            Expression<Func<long, long, TPayload, TResult>> constructor,
            QueryContainer container,
            string identifier)
        {
            Contract.Requires(source != null);

            this.source = source;
            this.generator = generator;
            this.constructor = constructor;
            this.container = container;
            this.identifier = identifier;
            if (this.container != null) this.container.RegisterEgressSite(this.identifier);
        }

        public IDisposable Subscribe(IObserver<ArraySegment<TResult>> observer)
        {
            var pipe = new IntervalArrayEgressPipe<TPayload, TResult>(
                this.generator,
                this.constructor,
                observer,
                this.container);
            if (this.container != null) this.container.RegisterEgressPipe(this.identifier, pipe);
            return this.source.Subscribe(pipe);
        }
    }

    internal sealed class PartitionedStreamEventArrayObservable<TKey, TPayload> : IObservable<ArraySegment<PartitionedStreamEvent<TKey, TPayload>>>
    {
        internal readonly IStreamable<PartitionKey<TKey>, TPayload> source;
        internal readonly Func<PartitionedStreamEvent<TKey, TPayload>[]> generator;
        internal readonly QueryContainer container;
        internal readonly string identifier;

        public PartitionedStreamEventArrayObservable(
            IStreamable<PartitionKey<TKey>, TPayload> source,
            Func<PartitionedStreamEvent<TKey, TPayload>[]> generator,
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

        public IDisposable Subscribe(IObserver<ArraySegment<PartitionedStreamEvent<TKey, TPayload>>> observer)
        {
            var pipe = new PartitionedStreamEventArrayEgressPipe<TKey, TPayload>(
                this.generator,
                observer,
                this.container);
            if (this.container != null) this.container.RegisterEgressPipe(this.identifier, pipe);
            return this.source.Subscribe(pipe);
        }
    }

    internal sealed class PartitionedStartEdgeArrayObservable<TKey, TPayload, TResult> : IObservable<ArraySegment<TResult>>
    {
        internal readonly IStreamable<PartitionKey<TKey>, TPayload> source;
        internal readonly Func<TResult[]> generator;
        internal readonly QueryContainer container;
        internal readonly string identifier;
        internal readonly Expression<Func<TKey, long, TPayload, TResult>> constructor;

        public PartitionedStartEdgeArrayObservable(
            IStreamable<PartitionKey<TKey>, TPayload> source,
            Func<TResult[]> generator,
            Expression<Func<TKey, long, TPayload, TResult>> constructor,
            QueryContainer container,
            string identifier)
        {
            Contract.Requires(source != null);

            this.source = source;
            this.generator = generator;
            this.constructor = constructor;
            this.container = container;
            this.identifier = identifier;
            if (this.container != null) this.container.RegisterEgressSite(this.identifier);
        }

        public IDisposable Subscribe(IObserver<ArraySegment<TResult>> observer)
        {
            var pipe = new PartitionedStartEdgeArrayEgressPipe<TKey, TPayload, TResult>(
                this.generator,
                this.constructor,
                observer,
                this.container);
            if (this.container != null) this.container.RegisterEgressPipe(this.identifier, pipe);
            return this.source.Subscribe(pipe);
        }
    }

    internal sealed class PartitionedIntervalArrayObservable<TKey, TPayload, TResult> : IObservable<ArraySegment<TResult>>
    {
        internal readonly IStreamable<PartitionKey<TKey>, TPayload> source;
        internal readonly Func<TResult[]> generator;
        internal readonly QueryContainer container;
        internal readonly string identifier;
        internal readonly Expression<Func<TKey, long, long, TPayload, TResult>> constructor;

        public PartitionedIntervalArrayObservable(
            IStreamable<PartitionKey<TKey>, TPayload> source,
            Func<TResult[]> generator,
            Expression<Func<TKey, long, long, TPayload, TResult>> constructor,
            QueryContainer container,
            string identifier)
        {
            Contract.Requires(source != null);

            this.source = source;
            this.generator = generator;
            this.constructor = constructor;
            this.container = container;
            this.identifier = identifier;
            if (this.container != null) this.container.RegisterEgressSite(this.identifier);
        }

        public IDisposable Subscribe(IObserver<ArraySegment<TResult>> observer)
        {
            var pipe = new PartitionedIntervalArrayEgressPipe<TKey, TPayload, TResult>(
                this.generator,
                this.constructor,
                observer,
                this.container);
            if (this.container != null) this.container.RegisterEgressPipe(this.identifier, pipe);
            return this.source.Subscribe(pipe);
        }
    }
}