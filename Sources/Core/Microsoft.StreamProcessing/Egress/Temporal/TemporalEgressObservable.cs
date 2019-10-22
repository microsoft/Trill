// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    internal sealed class StreamEventObservable<TPayload> : IObservable<StreamEvent<TPayload>>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        private string errorMessages;
        internal readonly IStreamable<Empty, TPayload> source;
        internal readonly QueryContainer container;
        internal readonly string identifier;

        public StreamEventObservable(
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

        public IDisposable Subscribe(IObserver<StreamEvent<TPayload>> observer)
        {
            EgressBoundary<Empty, TPayload, StreamEvent<TPayload>> pipe;

            if (!Config.ForceRowBasedExecution && this.source.Properties.IsColumnar && typeof(TPayload).CanRepresentAsColumnar() && CanGenerateColumnar())
                pipe = GetPipe(observer);
            else
            {
                pipe = new StreamEventEgressPipe<TPayload>(
                    observer,
                    this.container);
            }
            if (this.container != null) this.container.RegisterEgressPipe(this.identifier, pipe);
            return this.source.Subscribe(pipe);
        }

        private CacheKey CachedPipeLookupKey() => CacheKey.Create(typeof(TPayload), "StreamEvent");

        private bool CanGenerateColumnar()
        {
            if (typeof(TPayload).IsAnonymousTypeName()) return false;
            if (!typeof(TPayload).GetTypeInfo().IsVisible) return false;

            var lookupKey = CachedPipeLookupKey();
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => TemporalEgressTemplate.Generate(this));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private EgressBoundary<Empty, TPayload, StreamEvent<TPayload>> GetPipe(IObserver<StreamEvent<TPayload>> observer)
        {
            var lookupKey = CachedPipeLookupKey();
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => TemporalEgressTemplate.Generate(this));

            var instance = Activator.CreateInstance(generatedPipeType.Item1, observer, this.container);
            var returnValue = (EgressBoundary<Empty, TPayload, StreamEvent<TPayload>>)instance;
            return returnValue;
        }

        public override string ToString()
        {
            if (this.container != null)
                return "RegisterOutput({0})";
            else
                return "ToStreamEventObservable()";
        }
    }

    internal sealed class StartEdgeObservable<TPayload, TResult> : IObservable<TResult>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        private string errorMessages;
        internal readonly IStreamable<Empty, TPayload> source;
        internal readonly QueryContainer container;
        internal readonly string identifier;
        internal readonly Expression<Func<long, TPayload, TResult>> constructor;

        public StartEdgeObservable(
            IStreamable<Empty, TPayload> source,
            Expression<Func<long, TPayload, TResult>> constructor,
            QueryContainer container,
            string identifier)
        {
            Contract.Requires(source != null);

            this.source = source;
            this.constructor = constructor;
            this.container = container;
            this.identifier = identifier;
            if (this.container != null) this.container.RegisterEgressSite(this.identifier);
        }

        public IDisposable Subscribe(IObserver<TResult> observer)
        {
            EgressBoundary<Empty, TPayload, TResult> pipe;

            if (!Config.ForceRowBasedExecution && this.source.Properties.IsColumnar && typeof(TPayload).CanRepresentAsColumnar() && CanGenerateColumnar())
                pipe = GetPipe(observer);
            else
            {
                pipe = new StartEdgeEgressPipe<TPayload, TResult>(
                    this.constructor,
                    observer,
                    this.container);
            }
            if (this.container != null) this.container.RegisterEgressPipe(this.identifier, pipe);
            return this.source.Subscribe(pipe);
        }

        private CacheKey CachedPipeLookupKey() => CacheKey.Create(typeof(TPayload), this.constructor.Body.ExpressionToCSharp(), typeof(TResult), "StartEdge");

        private bool CanGenerateColumnar()
        {
            if (typeof(TPayload).IsAnonymousTypeName() || typeof(TResult).IsAnonymousTypeName()) return false;
            if (!typeof(TPayload).GetTypeInfo().IsVisible || !typeof(TResult).GetTypeInfo().IsVisible) return false;

            var lookupKey = CachedPipeLookupKey();
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => TemporalEgressTemplate.Generate(this));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private EgressBoundary<Empty, TPayload, TResult> GetPipe(IObserver<TResult> observer)
        {
            var lookupKey = CachedPipeLookupKey();
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => TemporalEgressTemplate.Generate(this));

            var instance = Activator.CreateInstance(generatedPipeType.Item1, observer, this.container);
            var returnValue = (EgressBoundary<Empty, TPayload, TResult>)instance;
            return returnValue;
        }
    }

    internal sealed class IntervalObservable<TPayload, TResult> : IObservable<TResult>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        private string errorMessages;
        internal readonly IStreamable<Empty, TPayload> source;
        internal readonly QueryContainer container;
        internal readonly string identifier;
        internal readonly Expression<Func<long, long, TPayload, TResult>> constructor;

        public IntervalObservable(
            IStreamable<Empty, TPayload> source,
            Expression<Func<long, long, TPayload, TResult>> constructor,
            QueryContainer container,
            string identifier)
        {
            Contract.Requires(source != null);

            this.source = source;
            this.constructor = constructor;
            this.container = container;
            this.identifier = identifier;
            if (this.container != null) this.container.RegisterEgressSite(this.identifier);
        }

        public IDisposable Subscribe(IObserver<TResult> observer)
        {
            EgressBoundary<Empty, TPayload, TResult> pipe;

            if (!Config.ForceRowBasedExecution && this.source.Properties.IsColumnar && typeof(TPayload).CanRepresentAsColumnar() && CanGenerateColumnar())
                pipe = GetPipe(observer);
            else
            {
                pipe = new IntervalEgressPipe<TPayload, TResult>(
                    this.constructor,
                    observer,
                    this.container);
            }
            if (this.container != null) this.container.RegisterEgressPipe(this.identifier, pipe);
            return this.source.Subscribe(pipe);
        }

        private CacheKey CachedPipeLookupKey() => CacheKey.Create(typeof(TPayload), this.constructor.Body.ExpressionToCSharp(), typeof(TResult), "Interval");

        private bool CanGenerateColumnar()
        {
            if (typeof(TPayload).IsAnonymousTypeName() || typeof(TResult).IsAnonymousTypeName()) return false;
            if (!typeof(TPayload).GetTypeInfo().IsVisible || !typeof(TResult).GetTypeInfo().IsVisible) return false;

            var lookupKey = CachedPipeLookupKey();
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => TemporalEgressTemplate.Generate(this));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private EgressBoundary<Empty, TPayload, TResult> GetPipe(IObserver<TResult> observer)
        {
            var lookupKey = CachedPipeLookupKey();
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => TemporalEgressTemplate.Generate(this));

            var instance = Activator.CreateInstance(generatedPipeType.Item1, observer, this.container);
            var returnValue = (EgressBoundary<Empty, TPayload, TResult>)instance;
            return returnValue;
        }
    }

    internal sealed class PartitionedStreamEventObservable<TKey, TPayload> : IObservable<PartitionedStreamEvent<TKey, TPayload>>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        private string errorMessages;
        internal readonly IStreamable<PartitionKey<TKey>, TPayload> source;
        internal readonly QueryContainer container;
        internal readonly string identifier;

        public PartitionedStreamEventObservable(
            IStreamable<PartitionKey<TKey>, TPayload> source,
            QueryContainer container,
            string identifier)
        {
            Contract.Requires(source != null);

            this.source = source;
            this.container = container;
            this.identifier = identifier;
            if (this.container != null) this.container.RegisterEgressSite(this.identifier);
        }

        public IDisposable Subscribe(IObserver<PartitionedStreamEvent<TKey, TPayload>> observer)
        {
            EgressBoundary<PartitionKey<TKey>, TPayload, PartitionedStreamEvent<TKey, TPayload>> pipe;

            if (!Config.ForceRowBasedExecution && this.source.Properties.IsColumnar && typeof(TPayload).CanRepresentAsColumnar() && CanGenerateColumnar())
                pipe = GetPipe(observer);
            else
            {
                pipe = new PartitionedStreamEventEgressPipe<TKey, TPayload>(
                    observer,
                    this.container);
            }
            if (this.container != null) this.container.RegisterEgressPipe(this.identifier, pipe);
            return this.source.Subscribe(pipe);
        }

        private CacheKey CachedPipeLookupKey() => CacheKey.Create(typeof(TPayload), typeof(TKey), "StreamEvent");

        private bool CanGenerateColumnar()
        {
            if (typeof(TKey).IsAnonymousTypeName() || typeof(TPayload).IsAnonymousTypeName()) return false;
            if (!typeof(TKey).GetTypeInfo().IsVisible || !typeof(TPayload).GetTypeInfo().IsVisible) return false;

            var lookupKey = CachedPipeLookupKey();
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => TemporalEgressTemplate.Generate(this));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private EgressBoundary<PartitionKey<TKey>, TPayload, PartitionedStreamEvent<TKey, TPayload>> GetPipe(IObserver<PartitionedStreamEvent<TKey, TPayload>> observer)
        {
            var lookupKey = CachedPipeLookupKey();
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => TemporalEgressTemplate.Generate(this));

            var instance = Activator.CreateInstance(generatedPipeType.Item1, observer, this.container);
            var returnValue = (EgressBoundary<PartitionKey<TKey>, TPayload, PartitionedStreamEvent<TKey, TPayload>>)instance;
            return returnValue;
        }

        public override string ToString()
        {
            if (this.container != null)
                return "RegisterOutput({0})";
            else
                return "ToStreamEventObservable()";
        }
    }

    internal sealed class PartitionedStartEdgeObservable<TKey, TPayload, TResult> : IObservable<TResult>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        private string errorMessages;
        internal readonly IStreamable<PartitionKey<TKey>, TPayload> source;
        internal readonly QueryContainer container;
        internal readonly string identifier;
        internal readonly Expression<Func<TKey, long, TPayload, TResult>> constructor;

        public PartitionedStartEdgeObservable(
            IStreamable<PartitionKey<TKey>, TPayload> source,
            Expression<Func<TKey, long, TPayload, TResult>> constructor,
            QueryContainer container,
            string identifier)
        {
            Contract.Requires(source != null);

            this.source = source;
            this.constructor = constructor;
            this.container = container;
            this.identifier = identifier;
            if (this.container != null) this.container.RegisterEgressSite(this.identifier);
        }

        public IDisposable Subscribe(IObserver<TResult> observer)
        {
            EgressBoundary<PartitionKey<TKey>, TPayload, TResult> pipe;

            if (!Config.ForceRowBasedExecution && this.source.Properties.IsColumnar && typeof(TPayload).CanRepresentAsColumnar() && CanGenerateColumnar())
                pipe = GetPipe(observer);
            else
            {
                pipe = new PartitionedStartEdgeEgressPipe<TKey, TPayload, TResult>(
                    this.constructor,
                    observer,
                    this.container);
            }
            if (this.container != null) this.container.RegisterEgressPipe(this.identifier, pipe);
            return this.source.Subscribe(pipe);
        }

        private CacheKey CachedPipeLookupKey() => CacheKey.Create(typeof(TPayload), this.constructor.Body.ExpressionToCSharp(), typeof(TResult), typeof(TKey), "StartEdge");

        private bool CanGenerateColumnar()
        {
            if (typeof(TKey).IsAnonymousTypeName() || typeof(TPayload).IsAnonymousTypeName() || typeof(TResult).IsAnonymousTypeName()) return false;
            if (!typeof(TKey).GetTypeInfo().IsVisible || !typeof(TPayload).GetTypeInfo().IsVisible || !typeof(TResult).GetTypeInfo().IsVisible) return false;

            var lookupKey = CachedPipeLookupKey();
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => TemporalEgressTemplate.Generate(this));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private EgressBoundary<PartitionKey<TKey>, TPayload, TResult> GetPipe(IObserver<TResult> observer)
        {
            var lookupKey = CachedPipeLookupKey();
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => TemporalEgressTemplate.Generate(this));

            var instance = Activator.CreateInstance(generatedPipeType.Item1, observer, this.container);
            var returnValue = (EgressBoundary<PartitionKey<TKey>, TPayload, TResult>)instance;
            return returnValue;
        }
    }

    internal sealed class PartitionedIntervalObservable<TKey, TPayload, TResult> : IObservable<TResult>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        private string errorMessages;
        internal readonly IStreamable<PartitionKey<TKey>, TPayload> source;
        internal readonly QueryContainer container;
        internal readonly string identifier;
        internal readonly Expression<Func<TKey, long, long, TPayload, TResult>> constructor;

        public PartitionedIntervalObservable(
            IStreamable<PartitionKey<TKey>, TPayload> source,
            Expression<Func<TKey, long, long, TPayload, TResult>> constructor,
            QueryContainer container,
            string identifier)
        {
            Contract.Requires(source != null);

            this.source = source;
            this.constructor = constructor;
            this.container = container;
            this.identifier = identifier;
            if (this.container != null) this.container.RegisterEgressSite(this.identifier);
        }

        public IDisposable Subscribe(IObserver<TResult> observer)
        {
            EgressBoundary<PartitionKey<TKey>, TPayload, TResult> pipe;

            if (!Config.ForceRowBasedExecution && this.source.Properties.IsColumnar && typeof(TPayload).CanRepresentAsColumnar() && CanGenerateColumnar())
                pipe = GetPipe(observer);
            else
            {
                pipe = new PartitionedIntervalEgressPipe<TKey, TPayload, TResult>(
                    this.constructor,
                    observer,
                    this.container);
            }
            if (this.container != null) this.container.RegisterEgressPipe(this.identifier, pipe);
            return this.source.Subscribe(pipe);
        }

        private CacheKey CachedPipeLookupKey() => CacheKey.Create(typeof(TPayload), this.constructor.Body.ExpressionToCSharp(), typeof(TResult), typeof(TKey), "Interval");

        private bool CanGenerateColumnar()
        {
            if (typeof(TKey).IsAnonymousTypeName() || typeof(TPayload).IsAnonymousTypeName() || typeof(TResult).IsAnonymousTypeName()) return false;
            if (!typeof(TKey).GetTypeInfo().IsVisible || !typeof(TPayload).GetTypeInfo().IsVisible || !typeof(TResult).GetTypeInfo().IsVisible) return false;

            var lookupKey = CachedPipeLookupKey();
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => TemporalEgressTemplate.Generate(this));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private EgressBoundary<PartitionKey<TKey>, TPayload, TResult> GetPipe(IObserver<TResult> observer)
        {
            var lookupKey = CachedPipeLookupKey();
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => TemporalEgressTemplate.Generate(this));

            var instance = Activator.CreateInstance(generatedPipeType.Item1, observer, this.container);
            var returnValue = (EgressBoundary<PartitionKey<TKey>, TPayload, TResult>)instance;
            return returnValue;
        }
    }
}