// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    internal sealed class StreamEventIngressStreamable<TPayload> : Streamable<Empty, TPayload>, IObservableIngressStreamable<TPayload>, IFusibleStreamable<Empty, TPayload>, IDisposable
    {
        private readonly FuseModule fuseModule;
        private readonly IObservable<StreamEvent<TPayload>> observable;
        private readonly DisorderPolicy disorderPolicy;
        private readonly FlushPolicy flushPolicy;
        private readonly PeriodicPunctuationPolicy punctuationPolicy;
        private readonly OnCompletedPolicy onCompletedPolicy;
        private readonly bool delayed;

        private readonly QueryContainer container;

        internal DiagnosticObservable<TPayload> diagnosticOutput;

        public StreamEventIngressStreamable(
            IObservable<StreamEvent<TPayload>> observable,
            DisorderPolicy disorderPolicy,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            OnCompletedPolicy onCompletedPolicy,
            QueryContainer container,
            string identifier)
            : base(StreamProperties<Empty, TPayload>.Default.SetQueryContainer(container))
        {
            ArgumentNullException.ThrowIfNull(observable);
            ArgumentNullException.ThrowIfNull(identifier);

            this.IngressSiteIdentifier = identifier;
            this.observable = observable;
            this.disorderPolicy = disorderPolicy;
            this.flushPolicy = flushPolicy;
            this.punctuationPolicy = punctuationPolicy;
            this.onCompletedPolicy = onCompletedPolicy;
            this.container = container;
            this.delayed = container != null;
            this.fuseModule = new FuseModule();
            if (this.delayed) container.RegisterIngressSite(this.IngressSiteIdentifier);

            if (Config.ForceRowBasedExecution
                || !typeof(TPayload).CanRepresentAsColumnar()
                || typeof(TPayload).IsAnonymousTypeName())
            {
                this.properties = this.properties.ToRowBased();
            }
            else this.properties = this.properties.ToDelayedColumnar(this.CanGenerateColumnar);
        }

        public void Dispose() => this.diagnosticOutput?.Dispose();

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(this.observable != null);
        }

        public IObservable<OutOfOrderStreamEvent<TPayload>> GetDroppedAdjustedEventsDiagnostic()
        {
            if (this.diagnosticOutput == null) this.diagnosticOutput = new DiagnosticObservable<TPayload>();
            return this.diagnosticOutput;
        }

        public override IDisposable Subscribe(IStreamObserver<Empty, TPayload> observer)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            IIngressStreamObserver pipe = null;
            if (this.properties.IsColumnar) pipe = this.GetPipe(observer);
            else
            {
                pipe = StreamEventSubscriptionCreator<TPayload, TPayload>.CreateSubscription(
                    this.observable,
                    this.IngressSiteIdentifier,
                    this,
                    observer,
                    this.disorderPolicy,
                    this.flushPolicy,
                    this.punctuationPolicy,
                    this.onCompletedPolicy,
                    this.diagnosticOutput,
                    this.fuseModule);
            }

            if (this.delayed)
            {
                this.container.RegisterIngressPipe(this.IngressSiteIdentifier, pipe);
                return pipe.DelayedDisposable;
            }
            else
            {
                pipe.Enable();
                return pipe;
            }
        }

        public string IngressSiteIdentifier { get; private set; } = Guid.NewGuid().ToString();

        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new();

        private bool CanGenerateColumnar()
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    this.fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    this.punctuationPolicy.ToString(),
                    this.disorderPolicy.ToString(),
                    (this.disorderPolicy.type != DisorderPolicyType.Throw && this.diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.Generate<TPayload>(
                    this.disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    this.disorderPolicy.type != DisorderPolicyType.Throw && this.diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    this.fuseModule));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private IIngressStreamObserver GetPipe(IStreamObserver<Empty, TPayload> observer)
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    this.fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    this.punctuationPolicy.ToString(),
                    this.disorderPolicy.ToString(),
                    (this.disorderPolicy.type != DisorderPolicyType.Throw && this.diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            object instance;
            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.Generate<TPayload>(
                    this.disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    this.disorderPolicy.type != DisorderPolicyType.Throw && this.diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    this.fuseModule));
            instance = Activator.CreateInstance(
                generatedPipeType.Item1,
                this.observable, this.IngressSiteIdentifier, this, observer, this.disorderPolicy, this.flushPolicy, this.punctuationPolicy, this.onCompletedPolicy, this.diagnosticOutput);
            var returnValue = (IIngressStreamObserver)instance;
            return returnValue;
        }

        public override string ToString()
        {
            if (this.container != null)
                return "RegisterInput({0}, " + this.disorderPolicy.ToString() + ", " + this.flushPolicy.ToString() + ", " + this.punctuationPolicy.ToString() + ", " + this.onCompletedPolicy.ToString() + ")";
            else
                return "ToStreamable(" + this.disorderPolicy.ToString() + ", " + this.flushPolicy.ToString() + ", " + this.punctuationPolicy.ToString() + ", " + this.onCompletedPolicy.ToString() + ")";
        }

        public bool CanFuseSelect(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<Empty, TNewResult> FuseSelect<TNewResult>(Expression<Func<TPayload, TNewResult>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelect(expression),
                this,
                this.Properties.Select<TNewResult>(expression, false, false));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelect<TNewResult>(Expression<Func<long, TPayload, TNewResult>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelect(expression),
                this,
                this.Properties.Select<TNewResult>(expression, true, false, true));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<Empty, TPayload, TNewResult>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectWithKey(expression),
                this,
                this.Properties.Select<TNewResult>(expression, false, true));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<long, Empty, TPayload, TNewResult>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectWithKey(expression),
                this,
                this.Properties.Select<TNewResult>(expression, true, true));
        }

        public bool CanFuseSelectMany(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<Empty, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectMany(expression),
                this,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<long, TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectMany(expression),
                this,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<Empty, TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectManyWithKey(expression),
                this,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<long, Empty, TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectManyWithKey(expression),
                this,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TPayload> FuseWhere(Expression<Func<TPayload, bool>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TPayload>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseWhere(expression),
                this,
                this.Properties.Where(expression));
        }

        public IFusibleStreamable<Empty, TPayload> FuseSetDurationConstant(long value)
        {
            return new StreamEventIngressStreamableFused<TPayload, TPayload>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSetDurationConstant(value),
                this,
                this.Properties.ToConstantDuration(true, value));
        }

        public IObservable<TNewResult> FuseEgressObservable<TNewResult>(Expression<Func<long, long, TPayload, Empty, TNewResult>> expression, QueryContainer container, string identifier)
        {
            return new FusedObservable<Empty, StreamEvent<TPayload>, TPayload, TPayload, TNewResult>(
                this.observable,
                (o) => o.SyncTime,
                (o) => o.OtherTime,
                (o) => Empty.Default,
                (o) => o.Payload,
                this.fuseModule,
                expression,
                container,
                this.IngressSiteIdentifier,
                identifier);
        }

        public bool CanFuseEgressObservable => Config.AllowFloatingReorderPolicy;
    }

    internal sealed class IntervalIngressStreamable<TPayload> : Streamable<Empty, TPayload>, IObservableIngressStreamable<TPayload>, IFusibleStreamable<Empty, TPayload>, IDisposable
    {
        private readonly FuseModule fuseModule;
        private readonly IObservable<TPayload> observable;
        private readonly Expression<Func<TPayload, long>> startEdgeExtractor;
        private readonly Expression<Func<TPayload, long>> endEdgeExtractor;
        private readonly DisorderPolicy disorderPolicy;
        private readonly FlushPolicy flushPolicy;
        private readonly PeriodicPunctuationPolicy punctuationPolicy;
        private readonly OnCompletedPolicy onCompletedPolicy;
        private readonly bool delayed;

        private readonly QueryContainer container;

        internal DiagnosticObservable<TPayload> diagnosticOutput;

        public IntervalIngressStreamable(
            IObservable<TPayload> observable,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            DisorderPolicy disorderPolicy,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            OnCompletedPolicy onCompletedPolicy,
            QueryContainer container,
            string identifier)
            : base(StreamProperties<Empty, TPayload>.DefaultIngress(startEdgeExtractor, endEdgeExtractor).SetQueryContainer(container))
        {
            ArgumentNullException.ThrowIfNull(observable);
            ArgumentNullException.ThrowIfNull(identifier);

            this.IngressSiteIdentifier = identifier;
            this.observable = observable;
            this.startEdgeExtractor = startEdgeExtractor;
            this.endEdgeExtractor = endEdgeExtractor;
            this.disorderPolicy = disorderPolicy;
            this.flushPolicy = flushPolicy;
            this.punctuationPolicy = punctuationPolicy;
            this.onCompletedPolicy = onCompletedPolicy;
            this.container = container;
            this.delayed = container != null;
            this.fuseModule = new FuseModule();
            if (this.delayed) container.RegisterIngressSite(this.IngressSiteIdentifier);

            if (Config.ForceRowBasedExecution
                || !typeof(TPayload).CanRepresentAsColumnar()
                || typeof(TPayload).IsAnonymousTypeName())
            {
                this.properties = this.properties.ToRowBased();
            }
            else this.properties = this.properties.ToDelayedColumnar(this.CanGenerateColumnar);
        }

        public void Dispose() => this.diagnosticOutput?.Dispose();

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(this.observable != null);
        }

        public IObservable<OutOfOrderStreamEvent<TPayload>> GetDroppedAdjustedEventsDiagnostic()
        {
            if (this.diagnosticOutput == null) this.diagnosticOutput = new DiagnosticObservable<TPayload>();
            return this.diagnosticOutput;
        }

        public override IDisposable Subscribe(IStreamObserver<Empty, TPayload> observer)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            IIngressStreamObserver pipe = null;
            if (this.properties.IsColumnar) pipe = this.GetPipe(observer);
            else
            {
                pipe = IntervalSubscriptionCreator<TPayload, TPayload>.CreateSubscription(
                    this.observable,
                    this.startEdgeExtractor,
                    this.endEdgeExtractor,
                    this.IngressSiteIdentifier,
                    this,
                    observer,
                    this.disorderPolicy,
                    this.flushPolicy,
                    this.punctuationPolicy,
                    this.onCompletedPolicy,
                    this.diagnosticOutput,
                    this.fuseModule);
            }

            if (this.delayed)
            {
                this.container.RegisterIngressPipe(this.IngressSiteIdentifier, pipe);
                return pipe.DelayedDisposable;
            }
            else
            {
                pipe.Enable();
                return pipe;
            }
        }

        public string IngressSiteIdentifier { get; private set; } = Guid.NewGuid().ToString();

        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new();

        private bool CanGenerateColumnar()
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    this.startEdgeExtractor.ExpressionToCSharp(),
                    this.endEdgeExtractor != null ? this.endEdgeExtractor.ExpressionToCSharp() : string.Empty),
                Tuple.Create(
                    this.fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    this.punctuationPolicy.ToString(),
                    this.disorderPolicy.ToString(),
                    (this.disorderPolicy.type != DisorderPolicyType.Throw && this.diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.Generate<TPayload>(
                    this.startEdgeExtractor,
                    this.endEdgeExtractor,
                    this.disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    this.disorderPolicy.type != DisorderPolicyType.Throw && this.diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    this.fuseModule));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private IIngressStreamObserver GetPipe(IStreamObserver<Empty, TPayload> observer)
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    this.startEdgeExtractor.ExpressionToCSharp(),
                    this.endEdgeExtractor != null ? this.endEdgeExtractor.ExpressionToCSharp() : string.Empty),
                Tuple.Create(
                    this.fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    this.punctuationPolicy.ToString(),
                    this.disorderPolicy.ToString(),
                    (this.disorderPolicy.type != DisorderPolicyType.Throw && this.diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            object instance;
            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.Generate<TPayload>(
                    this.startEdgeExtractor,
                    this.endEdgeExtractor,
                    this.disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    this.disorderPolicy.type != DisorderPolicyType.Throw && this.diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    this.fuseModule));
            instance = Activator.CreateInstance(
                generatedPipeType.Item1,
                this.observable, this.IngressSiteIdentifier, this, observer, this.disorderPolicy, this.flushPolicy, this.punctuationPolicy, this.onCompletedPolicy, this.diagnosticOutput);
            var returnValue = (IIngressStreamObserver)instance;
            return returnValue;
        }

        public bool CanFuseSelect(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<Empty, TNewResult> FuseSelect<TNewResult>(Expression<Func<TPayload, TNewResult>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelect(expression),
                this,
                this.Properties.Select<TNewResult>(expression, false, false));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelect<TNewResult>(Expression<Func<long, TPayload, TNewResult>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelect(expression),
                this,
                this.Properties.Select<TNewResult>(expression, true, false, true));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<Empty, TPayload, TNewResult>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectWithKey(expression),
                this,
                this.Properties.Select<TNewResult>(expression, false, true));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<long, Empty, TPayload, TNewResult>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectWithKey(expression),
                this,
                this.Properties.Select<TNewResult>(expression, true, true));
        }

        public bool CanFuseSelectMany(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<Empty, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectMany(expression),
                this,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<long, TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectMany(expression),
                this,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<Empty, TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectManyWithKey(expression),
                this,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<long, Empty, TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectManyWithKey(expression),
                this,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TPayload> FuseWhere(Expression<Func<TPayload, bool>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TPayload>(
                this.observable,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseWhere(expression),
                this,
                this.Properties.Where(expression));
        }

        public IFusibleStreamable<Empty, TPayload> FuseSetDurationConstant(long value)
        {
            return new IntervalIngressStreamableFused<TPayload, TPayload>(
                this.observable,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSetDurationConstant(value),
                this,
                this.Properties.ToConstantDuration(true, value));
        }

        public IObservable<TNewResult> FuseEgressObservable<TNewResult>(Expression<Func<long, long, TPayload, Empty, TNewResult>> expression, QueryContainer container, string identifier)
        {
            return new FusedObservable<Empty, TPayload, TPayload, TPayload, TNewResult>(
                this.observable,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                (o) => Empty.Default,
                (o) => o,
                this.fuseModule,
                expression,
                container,
                this.IngressSiteIdentifier,
                identifier);
        }

        public bool CanFuseEgressObservable => Config.AllowFloatingReorderPolicy;
    }

    internal sealed class PartitionedStreamEventIngressStreamable<TPartitionKey, TPayload> : Streamable<PartitionKey<TPartitionKey>, TPayload>, IPartitionedIngressStreamable<TPartitionKey, TPayload>, IFusibleStreamable<PartitionKey<TPartitionKey>, TPayload>, IDisposable
    {
        private readonly FuseModule fuseModule;
        private readonly IObservable<PartitionedStreamEvent<TPartitionKey, TPayload>> observable;
        private readonly DisorderPolicy disorderPolicy;
        private readonly PartitionedFlushPolicy flushPolicy;
        private readonly PeriodicPunctuationPolicy punctuationPolicy;
        private readonly PeriodicLowWatermarkPolicy lowWatermarkPolicy;
        private readonly OnCompletedPolicy onCompletedPolicy;
        private readonly bool delayed;

        private readonly QueryContainer container;

        internal PartitionedDiagnosticObservable<TPartitionKey, TPayload> diagnosticOutput;

        public PartitionedStreamEventIngressStreamable(
            IObservable<PartitionedStreamEvent<TPartitionKey, TPayload>> observable,
            DisorderPolicy disorderPolicy,
            PartitionedFlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            PeriodicLowWatermarkPolicy lowWatermarkPolicy,
            OnCompletedPolicy onCompletedPolicy,
            QueryContainer container,
            string identifier)
            : base(StreamProperties<PartitionKey<TPartitionKey>, TPayload>.Default.SetQueryContainer(container))
        {
            ArgumentNullException.ThrowIfNull(observable);
            ArgumentNullException.ThrowIfNull(identifier);

            this.IngressSiteIdentifier = identifier;
            this.observable = observable;
            this.disorderPolicy = disorderPolicy;
            this.flushPolicy = flushPolicy;
            this.punctuationPolicy = punctuationPolicy;
            this.lowWatermarkPolicy = lowWatermarkPolicy;
            this.onCompletedPolicy = onCompletedPolicy;
            this.container = container;
            this.delayed = container != null;
            this.fuseModule = new FuseModule();
            if (this.delayed) container.RegisterIngressSite(this.IngressSiteIdentifier);

            this.properties = this.properties.ToRowBased();
        }

        public void Dispose() => this.diagnosticOutput?.Dispose();

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(this.observable != null);
        }

        public IObservable<OutOfOrderPartitionedStreamEvent<TPartitionKey, TPayload>> GetDroppedAdjustedEventsDiagnostic()
        {
            if (this.diagnosticOutput == null) this.diagnosticOutput = new PartitionedDiagnosticObservable<TPartitionKey, TPayload>();
            return this.diagnosticOutput;
        }

        public override IDisposable Subscribe(IStreamObserver<PartitionKey<TPartitionKey>, TPayload> observer)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            IIngressStreamObserver pipe = null;
            if (this.properties.IsColumnar) pipe = this.GetPipe(observer);
            else
            {
                pipe = PartitionedStreamEventSubscriptionCreator<TPartitionKey, TPayload, TPayload>.CreateSubscription(
                    this.observable,
                    this.IngressSiteIdentifier,
                    this,
                    observer,
                    this.disorderPolicy,
                    this.flushPolicy,
                    this.punctuationPolicy,
                    this.lowWatermarkPolicy,
                    this.onCompletedPolicy,
                    this.diagnosticOutput,
                    this.fuseModule);
            }

            if (this.delayed)
            {
                this.container.RegisterIngressPipe(this.IngressSiteIdentifier, pipe);
                return pipe.DelayedDisposable;
            }
            else
            {
                pipe.Enable();
                return pipe;
            }
        }

        public string IngressSiteIdentifier { get; private set; } = Guid.NewGuid().ToString();

        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new();

        private bool CanGenerateColumnar()
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    this.fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    this.punctuationPolicy.ToString(),
                    this.lowWatermarkPolicy.ToString(),
                    this.disorderPolicy.ToString(),
                    (this.disorderPolicy.type != DisorderPolicyType.Throw && this.diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.Generate<TPartitionKey, TPayload>(
                    this.disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    this.disorderPolicy.type != DisorderPolicyType.Throw && this.diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    this.fuseModule));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private IIngressStreamObserver GetPipe(IStreamObserver<PartitionKey<TPartitionKey>, TPayload> observer)
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    this.fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    this.punctuationPolicy.ToString(),
                    this.lowWatermarkPolicy.ToString(),
                    this.disorderPolicy.ToString(),
                    (this.disorderPolicy.type != DisorderPolicyType.Throw && this.diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            object instance;
            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.Generate<TPartitionKey, TPayload>(
                    this.disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    this.disorderPolicy.type != DisorderPolicyType.Throw && this.diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    this.fuseModule));
            instance = Activator.CreateInstance(
                generatedPipeType.Item1,
                this.observable, this.IngressSiteIdentifier, this, observer, this.disorderPolicy, this.flushPolicy, this.punctuationPolicy, this.lowWatermarkPolicy, this.onCompletedPolicy, this.diagnosticOutput);
            var returnValue = (IIngressStreamObserver)instance;
            return returnValue;
        }

        public override string ToString()
        {
            if (this.container != null)
                return "RegisterInput({0}, " + this.disorderPolicy.ToString() + ", " + this.flushPolicy.ToString() + ", " + this.punctuationPolicy.ToString() + ", " + this.lowWatermarkPolicy.ToString() + ", " + this.onCompletedPolicy.ToString() + ")";
            else
                return "ToStreamable(" + this.disorderPolicy.ToString() + ", " + this.flushPolicy.ToString() + ", " + this.punctuationPolicy.ToString() + ", " + this.lowWatermarkPolicy.ToString() + ", " + this.onCompletedPolicy.ToString() + ")";
        }

        public bool CanFuseSelect(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelect<TNewResult>(Expression<Func<TPayload, TNewResult>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelect(expression),
                this,
                this.Properties.Select<TNewResult>(expression, false, false));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelect<TNewResult>(Expression<Func<long, TPayload, TNewResult>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelect(expression),
                this,
                this.Properties.Select<TNewResult>(expression, true, false, true));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<PartitionKey<TPartitionKey>, TPayload, TNewResult>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectWithKey(expression),
                this,
                this.Properties.Select<TNewResult>(expression, false, true));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<long, PartitionKey<TPartitionKey>, TPayload, TNewResult>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectWithKey(expression),
                this,
                this.Properties.Select<TNewResult>(expression, true, true));
        }

        public bool CanFuseSelectMany(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectMany(expression),
                this,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<long, TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectMany(expression),
                this,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<PartitionKey<TPartitionKey>, TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectManyWithKey(expression),
                this,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<long, PartitionKey<TPartitionKey>, TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectManyWithKey(expression),
                this,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TPayload> FuseWhere(Expression<Func<TPayload, bool>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TPayload>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseWhere(expression),
                this,
                this.Properties.Where(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TPayload> FuseSetDurationConstant(long value)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TPayload>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSetDurationConstant(value),
                this,
                this.Properties.ToConstantDuration(true, value));
        }

        public IObservable<TNewResult> FuseEgressObservable<TNewResult>(Expression<Func<long, long, TPayload, PartitionKey<TPartitionKey>, TNewResult>> expression, QueryContainer container, string identifier)
        {
            return new FusedObservable<PartitionKey<TPartitionKey>, PartitionedStreamEvent<TPartitionKey, TPayload>, TPayload, TPayload, TNewResult>(
                this.observable,
                (o) => o.SyncTime,
                (o) => o.OtherTime,
                (o) => new PartitionKey<TPartitionKey>(o.PartitionKey),
                (o) => o.Payload,
                this.fuseModule,
                expression,
                container,
                this.IngressSiteIdentifier,
                identifier);
        }

        public bool CanFuseEgressObservable => Config.AllowFloatingReorderPolicy;
    }

    internal sealed class PartitionedIntervalIngressStreamable<TPartitionKey, TPayload> : Streamable<PartitionKey<TPartitionKey>, TPayload>, IPartitionedIngressStreamable<TPartitionKey, TPayload>, IFusibleStreamable<PartitionKey<TPartitionKey>, TPayload>, IDisposable
    {
        private readonly FuseModule fuseModule;
        private readonly IObservable<TPayload> observable;
        private readonly Expression<Func<TPayload, TPartitionKey>> partitionExtractor;
        private readonly Expression<Func<TPayload, long>> startEdgeExtractor;
        private readonly Expression<Func<TPayload, long>> endEdgeExtractor;
        private readonly DisorderPolicy disorderPolicy;
        private readonly PartitionedFlushPolicy flushPolicy;
        private readonly PeriodicPunctuationPolicy punctuationPolicy;
        private readonly PeriodicLowWatermarkPolicy lowWatermarkPolicy;
        private readonly OnCompletedPolicy onCompletedPolicy;
        private readonly bool delayed;

        private readonly QueryContainer container;

        internal PartitionedDiagnosticObservable<TPartitionKey, TPayload> diagnosticOutput;

        public PartitionedIntervalIngressStreamable(
            IObservable<TPayload> observable,
            Expression<Func<TPayload, TPartitionKey>> partitionExtractor,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            DisorderPolicy disorderPolicy,
            PartitionedFlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            PeriodicLowWatermarkPolicy lowWatermarkPolicy,
            OnCompletedPolicy onCompletedPolicy,
            QueryContainer container,
            string identifier)
            : base(StreamProperties<PartitionKey<TPartitionKey>, TPayload>.DefaultIngress(startEdgeExtractor, endEdgeExtractor).SetQueryContainer(container))
        {
            ArgumentNullException.ThrowIfNull(observable);
            ArgumentNullException.ThrowIfNull(identifier);

            this.IngressSiteIdentifier = identifier;
            this.observable = observable;
            this.partitionExtractor = partitionExtractor;
            this.startEdgeExtractor = startEdgeExtractor;
            this.endEdgeExtractor = endEdgeExtractor;
            this.disorderPolicy = disorderPolicy;
            this.flushPolicy = flushPolicy;
            this.punctuationPolicy = punctuationPolicy;
            this.lowWatermarkPolicy = lowWatermarkPolicy;
            this.onCompletedPolicy = onCompletedPolicy;
            this.container = container;
            this.delayed = container != null;
            this.fuseModule = new FuseModule();
            if (this.delayed) container.RegisterIngressSite(this.IngressSiteIdentifier);

            this.properties = this.properties.ToRowBased();
        }

        public void Dispose() => this.diagnosticOutput?.Dispose();

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(this.observable != null);
        }

        public IObservable<OutOfOrderPartitionedStreamEvent<TPartitionKey, TPayload>> GetDroppedAdjustedEventsDiagnostic()
        {
            if (this.diagnosticOutput == null) this.diagnosticOutput = new PartitionedDiagnosticObservable<TPartitionKey, TPayload>();
            return this.diagnosticOutput;
        }

        public override IDisposable Subscribe(IStreamObserver<PartitionKey<TPartitionKey>, TPayload> observer)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            IIngressStreamObserver pipe = null;
            if (this.properties.IsColumnar) pipe = this.GetPipe(observer);
            else
            {
                pipe = PartitionedIntervalSubscriptionCreator<TPartitionKey, TPayload, TPayload>.CreateSubscription(
                    this.observable,
                    this.partitionExtractor,
                    this.startEdgeExtractor,
                    this.endEdgeExtractor,
                    this.IngressSiteIdentifier,
                    this,
                    observer,
                    this.disorderPolicy,
                    this.flushPolicy,
                    this.punctuationPolicy,
                    this.lowWatermarkPolicy,
                    this.onCompletedPolicy,
                    this.diagnosticOutput,
                    this.fuseModule);
            }

            if (this.delayed)
            {
                this.container.RegisterIngressPipe(this.IngressSiteIdentifier, pipe);
                return pipe.DelayedDisposable;
            }
            else
            {
                pipe.Enable();
                return pipe;
            }
        }

        public string IngressSiteIdentifier { get; private set; } = Guid.NewGuid().ToString();

        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new();

        private bool CanGenerateColumnar()
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    this.startEdgeExtractor.ExpressionToCSharp(),
                    this.endEdgeExtractor != null ? this.endEdgeExtractor.ExpressionToCSharp() : string.Empty,
                    this.partitionExtractor.ExpressionToCSharp()),
                Tuple.Create(
                    this.fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    this.punctuationPolicy.ToString(),
                    this.lowWatermarkPolicy.ToString(),
                    this.disorderPolicy.ToString(),
                    (this.disorderPolicy.type != DisorderPolicyType.Throw && this.diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.Generate<TPartitionKey, TPayload>(
                    this.partitionExtractor,
                    this.startEdgeExtractor,
                    this.endEdgeExtractor,
                    this.disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    this.disorderPolicy.type != DisorderPolicyType.Throw && this.diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    this.fuseModule));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private IIngressStreamObserver GetPipe(IStreamObserver<PartitionKey<TPartitionKey>, TPayload> observer)
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    this.startEdgeExtractor.ExpressionToCSharp(),
                    this.endEdgeExtractor != null ? this.endEdgeExtractor.ExpressionToCSharp() : string.Empty,
                    this.partitionExtractor.ExpressionToCSharp()),
                Tuple.Create(
                    this.fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    this.punctuationPolicy.ToString(),
                    this.lowWatermarkPolicy.ToString(),
                    this.disorderPolicy.ToString(),
                    (this.disorderPolicy.type != DisorderPolicyType.Throw && this.diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            object instance;
            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.Generate<TPartitionKey, TPayload>(
                    this.partitionExtractor,
                    this.startEdgeExtractor,
                    this.endEdgeExtractor,
                    this.disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    this.disorderPolicy.type != DisorderPolicyType.Throw && this.diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    this.fuseModule));
            instance = Activator.CreateInstance(
                generatedPipeType.Item1,
                this.observable, this.IngressSiteIdentifier, this, observer, this.disorderPolicy, this.flushPolicy, this.punctuationPolicy, this.lowWatermarkPolicy, this.onCompletedPolicy, this.diagnosticOutput);
            var returnValue = (IIngressStreamObserver)instance;
            return returnValue;
        }

        public bool CanFuseSelect(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelect<TNewResult>(Expression<Func<TPayload, TNewResult>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.partitionExtractor,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelect(expression),
                this,
                this.Properties.Select<TNewResult>(expression, false, false));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelect<TNewResult>(Expression<Func<long, TPayload, TNewResult>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.partitionExtractor,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelect(expression),
                this,
                this.Properties.Select<TNewResult>(expression, true, false, true));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<PartitionKey<TPartitionKey>, TPayload, TNewResult>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.partitionExtractor,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectWithKey(expression),
                this,
                this.Properties.Select<TNewResult>(expression, false, true));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<long, PartitionKey<TPartitionKey>, TPayload, TNewResult>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.partitionExtractor,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectWithKey(expression),
                this,
                this.Properties.Select<TNewResult>(expression, true, true));
        }

        public bool CanFuseSelectMany(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.partitionExtractor,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectMany(expression),
                this,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<long, TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.partitionExtractor,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectMany(expression),
                this,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<PartitionKey<TPartitionKey>, TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.partitionExtractor,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectManyWithKey(expression),
                this,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<long, PartitionKey<TPartitionKey>, TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.partitionExtractor,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectManyWithKey(expression),
                this,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TPayload> FuseWhere(Expression<Func<TPayload, bool>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TPayload>(
                this.observable,
                this.partitionExtractor,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseWhere(expression),
                this,
                this.Properties.Where(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TPayload> FuseSetDurationConstant(long value)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TPayload>(
                this.observable,
                this.partitionExtractor,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSetDurationConstant(value),
                this,
                this.Properties.ToConstantDuration(true, value));
        }

        public IObservable<TNewResult> FuseEgressObservable<TNewResult>(Expression<Func<long, long, TPayload, PartitionKey<TPartitionKey>, TNewResult>> expression, QueryContainer container, string identifier)
        {
            return new FusedObservable<PartitionKey<TPartitionKey>, TPayload, TPayload, TPayload, TNewResult>(
                this.observable,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                ParameterSubstituter.AddPartitionKey(this.partitionExtractor),
                (o) => o,
                this.fuseModule,
                expression,
                container,
                this.IngressSiteIdentifier,
                identifier);
        }

        public bool CanFuseEgressObservable => Config.AllowFloatingReorderPolicy;
    }

    internal sealed class StreamEventIngressStreamableFused<TPayload, TResult> : Streamable<Empty, TResult>, IFusibleStreamable<Empty, TResult>, IDisposable
    {
        private readonly StreamEventIngressStreamable<TPayload> entryPoint = null;
        private readonly FuseModule fuseModule;
        private readonly IObservable<StreamEvent<TPayload>> observable;
        private readonly DisorderPolicy disorderPolicy;
        private readonly FlushPolicy flushPolicy;
        private readonly PeriodicPunctuationPolicy punctuationPolicy;
        private readonly OnCompletedPolicy onCompletedPolicy;
        private readonly bool delayed;

        private readonly QueryContainer container;

        public StreamEventIngressStreamableFused(
            IObservable<StreamEvent<TPayload>> observable,
            DisorderPolicy disorderPolicy,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            OnCompletedPolicy onCompletedPolicy,
            QueryContainer container,
            string identifier,
            FuseModule fuseModule,
            StreamEventIngressStreamable<TPayload> entryPoint,
            StreamProperties<Empty, TResult> properties)
            : base(properties)
        {
            ArgumentNullException.ThrowIfNull(observable);
            ArgumentNullException.ThrowIfNull(identifier);

            this.IngressSiteIdentifier = identifier;
            this.observable = observable;
            this.disorderPolicy = disorderPolicy;
            this.flushPolicy = flushPolicy;
            this.punctuationPolicy = punctuationPolicy;
            this.onCompletedPolicy = onCompletedPolicy;
            this.container = container;
            this.delayed = container != null;
            this.fuseModule = fuseModule;
            this.entryPoint = entryPoint;

            if (Config.ForceRowBasedExecution
                || !typeof(TResult).CanRepresentAsColumnar()
                || typeof(TResult).IsAnonymousTypeName()
                || !typeof(TPayload).CanRepresentAsColumnar()
                || typeof(TPayload).IsAnonymousTypeName())
            {
                this.properties = properties.ToRowBased();
            }
            else this.properties = properties.ToDelayedColumnar(this.CanGenerateColumnar);
        }

        public void Dispose() => this.entryPoint?.Dispose();

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(this.observable != null);
        }

        public IObservable<OutOfOrderStreamEvent<TPayload>> GetDroppedAdjustedEventsDiagnostic()
        {
            return this.entryPoint.GetDroppedAdjustedEventsDiagnostic();
        }

        public override IDisposable Subscribe(IStreamObserver<Empty, TResult> observer)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            IIngressStreamObserver pipe = null;
            if (this.properties.IsColumnar) pipe = this.GetPipe(observer);
            else
            {
                pipe = StreamEventSubscriptionCreator<TPayload, TResult>.CreateSubscription(
                    this.observable,
                    this.IngressSiteIdentifier,
                    this,
                    observer,
                    this.disorderPolicy,
                    this.flushPolicy,
                    this.punctuationPolicy,
                    this.onCompletedPolicy,
                    this.entryPoint.diagnosticOutput,
                    this.fuseModule);
            }

            if (this.delayed)
            {
                this.container.RegisterIngressPipe(this.IngressSiteIdentifier, pipe);
                return pipe.DelayedDisposable;
            }
            else
            {
                pipe.Enable();
                return pipe;
            }
        }

        public string IngressSiteIdentifier { get; private set; } = Guid.NewGuid().ToString();

        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new();

        private bool CanGenerateColumnar()
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    this.fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    this.punctuationPolicy.ToString(),
                    this.disorderPolicy.ToString(),
                    (this.disorderPolicy.type != DisorderPolicyType.Throw && this.entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.GenerateFused<TPayload, TResult>(
                    this.disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    this.disorderPolicy.type != DisorderPolicyType.Throw && this.entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    this.fuseModule));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private IIngressStreamObserver GetPipe(IStreamObserver<Empty, TResult> observer)
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    this.fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    this.punctuationPolicy.ToString(),
                    this.disorderPolicy.ToString(),
                    (this.disorderPolicy.type != DisorderPolicyType.Throw && this.entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            object instance;
            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.GenerateFused<TPayload, TResult>(
                    this.disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    this.disorderPolicy.type != DisorderPolicyType.Throw && this.entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    this.fuseModule));
            instance = Activator.CreateInstance(
                generatedPipeType.Item1,
                this.observable, this.IngressSiteIdentifier, this, observer, this.disorderPolicy, this.flushPolicy, this.punctuationPolicy, this.onCompletedPolicy, this.entryPoint.diagnosticOutput);
            var returnValue = (IIngressStreamObserver)instance;
            return returnValue;
        }

        public override string ToString()
        {
            if (this.container != null)
                return "RegisterInput({0}, " + this.disorderPolicy.ToString() + ", " + this.flushPolicy.ToString() + ", " + this.punctuationPolicy.ToString() + ", " + this.onCompletedPolicy.ToString() + ")";
            else
                return "ToStreamable(" + this.disorderPolicy.ToString() + ", " + this.flushPolicy.ToString() + ", " + this.punctuationPolicy.ToString() + ", " + this.onCompletedPolicy.ToString() + ")";
        }

        public bool CanFuseSelect(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<Empty, TNewResult> FuseSelect<TNewResult>(Expression<Func<TResult, TNewResult>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelect(expression),
                this.entryPoint,
                this.Properties.Select<TNewResult>(expression, false, false));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelect<TNewResult>(Expression<Func<long, TResult, TNewResult>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelect(expression),
                this.entryPoint,
                this.Properties.Select<TNewResult>(expression, true, false, true));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<Empty, TResult, TNewResult>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectWithKey(expression),
                this.entryPoint,
                this.Properties.Select<TNewResult>(expression, false, true));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<long, Empty, TResult, TNewResult>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectWithKey(expression),
                this.entryPoint,
                this.Properties.Select<TNewResult>(expression, true, true));
        }

        public bool CanFuseSelectMany(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<Empty, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectMany(expression),
                this.entryPoint,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<long, TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectMany(expression),
                this.entryPoint,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<Empty, TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectManyWithKey(expression),
                this.entryPoint,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<long, Empty, TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectManyWithKey(expression),
                this.entryPoint,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TResult> FuseWhere(Expression<Func<TResult, bool>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseWhere(expression),
                this.entryPoint,
                this.Properties.Where(expression));
        }

        public IFusibleStreamable<Empty, TResult> FuseSetDurationConstant(long value)
        {
            return new StreamEventIngressStreamableFused<TPayload, TResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSetDurationConstant(value),
                this.entryPoint,
                this.Properties.ToConstantDuration(true, value));
        }

        public IObservable<TNewResult> FuseEgressObservable<TNewResult>(Expression<Func<long, long, TResult, Empty, TNewResult>> expression, QueryContainer container, string identifier)
        {
            return new FusedObservable<Empty, StreamEvent<TPayload>, TPayload, TResult, TNewResult>(
                this.observable,
                (o) => o.SyncTime,
                (o) => o.OtherTime,
                (o) => Empty.Default,
                (o) => o.Payload,
                this.fuseModule,
                expression,
                container,
                this.IngressSiteIdentifier,
                identifier);
        }

        public bool CanFuseEgressObservable => Config.AllowFloatingReorderPolicy;
    }

    internal sealed class IntervalIngressStreamableFused<TPayload, TResult> : Streamable<Empty, TResult>, IFusibleStreamable<Empty, TResult>, IDisposable
    {
        private readonly IntervalIngressStreamable<TPayload> entryPoint = null;
        private readonly FuseModule fuseModule;
        private readonly IObservable<TPayload> observable;
        private readonly Expression<Func<TPayload, long>> startEdgeExtractor;
        private readonly Expression<Func<TPayload, long>> endEdgeExtractor;
        private readonly DisorderPolicy disorderPolicy;
        private readonly FlushPolicy flushPolicy;
        private readonly PeriodicPunctuationPolicy punctuationPolicy;
        private readonly OnCompletedPolicy onCompletedPolicy;
        private readonly bool delayed;

        private readonly QueryContainer container;

        public IntervalIngressStreamableFused(
            IObservable<TPayload> observable,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            DisorderPolicy disorderPolicy,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            OnCompletedPolicy onCompletedPolicy,
            QueryContainer container,
            string identifier,
            FuseModule fuseModule,
            IntervalIngressStreamable<TPayload> entryPoint,
            StreamProperties<Empty, TResult> properties)
            : base(properties)
        {
            ArgumentNullException.ThrowIfNull(observable);
            ArgumentNullException.ThrowIfNull(identifier);

            this.IngressSiteIdentifier = identifier;
            this.observable = observable;
            this.startEdgeExtractor = startEdgeExtractor;
            this.endEdgeExtractor = endEdgeExtractor;
            this.disorderPolicy = disorderPolicy;
            this.flushPolicy = flushPolicy;
            this.punctuationPolicy = punctuationPolicy;
            this.onCompletedPolicy = onCompletedPolicy;
            this.container = container;
            this.delayed = container != null;
            this.fuseModule = fuseModule;
            this.entryPoint = entryPoint;

            if (Config.ForceRowBasedExecution
                || !typeof(TResult).CanRepresentAsColumnar()
                || typeof(TResult).IsAnonymousTypeName()
                || !typeof(TPayload).CanRepresentAsColumnar()
                || typeof(TPayload).IsAnonymousTypeName())
            {
                this.properties = properties.ToRowBased();
            }
            else this.properties = properties.ToDelayedColumnar(this.CanGenerateColumnar);
        }

        public void Dispose() => this.entryPoint?.Dispose();

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(this.observable != null);
        }

        public IObservable<OutOfOrderStreamEvent<TPayload>> GetDroppedAdjustedEventsDiagnostic()
        {
            return this.entryPoint.GetDroppedAdjustedEventsDiagnostic();
        }

        public override IDisposable Subscribe(IStreamObserver<Empty, TResult> observer)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            IIngressStreamObserver pipe = null;
            if (this.properties.IsColumnar) pipe = this.GetPipe(observer);
            else
            {
                pipe = IntervalSubscriptionCreator<TPayload, TResult>.CreateSubscription(
                    this.observable,
                    this.startEdgeExtractor,
                    this.endEdgeExtractor,
                    this.IngressSiteIdentifier,
                    this,
                    observer,
                    this.disorderPolicy,
                    this.flushPolicy,
                    this.punctuationPolicy,
                    this.onCompletedPolicy,
                    this.entryPoint.diagnosticOutput,
                    this.fuseModule);
            }

            if (this.delayed)
            {
                this.container.RegisterIngressPipe(this.IngressSiteIdentifier, pipe);
                return pipe.DelayedDisposable;
            }
            else
            {
                pipe.Enable();
                return pipe;
            }
        }

        public string IngressSiteIdentifier { get; private set; } = Guid.NewGuid().ToString();

        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new();

        private bool CanGenerateColumnar()
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    this.startEdgeExtractor.ExpressionToCSharp(),
                    this.endEdgeExtractor != null ? this.endEdgeExtractor.ExpressionToCSharp() : string.Empty),
                Tuple.Create(
                    this.fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    this.punctuationPolicy.ToString(),
                    this.disorderPolicy.ToString(),
                    (this.disorderPolicy.type != DisorderPolicyType.Throw && this.entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.GenerateFused<TPayload, TResult>(
                    this.startEdgeExtractor,
                    this.endEdgeExtractor,
                    this.disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    this.disorderPolicy.type != DisorderPolicyType.Throw && this.entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    this.fuseModule));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private IIngressStreamObserver GetPipe(IStreamObserver<Empty, TResult> observer)
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    this.startEdgeExtractor.ExpressionToCSharp(),
                    this.endEdgeExtractor != null ? this.endEdgeExtractor.ExpressionToCSharp() : string.Empty),
                Tuple.Create(
                    this.fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    this.punctuationPolicy.ToString(),
                    this.disorderPolicy.ToString(),
                    (this.disorderPolicy.type != DisorderPolicyType.Throw && this.entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            object instance;
            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.GenerateFused<TPayload, TResult>(
                    this.startEdgeExtractor,
                    this.endEdgeExtractor,
                    this.disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    this.disorderPolicy.type != DisorderPolicyType.Throw && this.entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    this.fuseModule));
            instance = Activator.CreateInstance(
                generatedPipeType.Item1,
                this.observable, this.IngressSiteIdentifier, this, observer, this.disorderPolicy, this.flushPolicy, this.punctuationPolicy, this.onCompletedPolicy, this.entryPoint.diagnosticOutput);
            var returnValue = (IIngressStreamObserver)instance;
            return returnValue;
        }

        public bool CanFuseSelect(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<Empty, TNewResult> FuseSelect<TNewResult>(Expression<Func<TResult, TNewResult>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelect(expression),
                this.entryPoint,
                this.Properties.Select<TNewResult>(expression, false, false));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelect<TNewResult>(Expression<Func<long, TResult, TNewResult>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelect(expression),
                this.entryPoint,
                this.Properties.Select<TNewResult>(expression, true, false, true));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<Empty, TResult, TNewResult>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectWithKey(expression),
                this.entryPoint,
                this.Properties.Select<TNewResult>(expression, false, true));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<long, Empty, TResult, TNewResult>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectWithKey(expression),
                this.entryPoint,
                this.Properties.Select<TNewResult>(expression, true, true));
        }

        public bool CanFuseSelectMany(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<Empty, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectMany(expression),
                this.entryPoint,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<long, TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectMany(expression),
                this.entryPoint,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<Empty, TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectManyWithKey(expression),
                this.entryPoint,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<long, Empty, TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                this.observable,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectManyWithKey(expression),
                this.entryPoint,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TResult> FuseWhere(Expression<Func<TResult, bool>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TResult>(
                this.observable,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseWhere(expression),
                this.entryPoint,
                this.Properties.Where(expression));
        }

        public IFusibleStreamable<Empty, TResult> FuseSetDurationConstant(long value)
        {
            return new IntervalIngressStreamableFused<TPayload, TResult>(
                this.observable,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSetDurationConstant(value),
                this.entryPoint,
                this.Properties.ToConstantDuration(true, value));
        }

        public IObservable<TNewResult> FuseEgressObservable<TNewResult>(Expression<Func<long, long, TResult, Empty, TNewResult>> expression, QueryContainer container, string identifier)
        {
            return new FusedObservable<Empty, TPayload, TPayload, TResult, TNewResult>(
                this.observable,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                (o) => Empty.Default,
                (o) => o,
                this.fuseModule,
                expression,
                container,
                this.IngressSiteIdentifier,
                identifier);
        }

        public bool CanFuseEgressObservable => Config.AllowFloatingReorderPolicy;
    }

    internal sealed class PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TResult> : Streamable<PartitionKey<TPartitionKey>, TResult>, IFusibleStreamable<PartitionKey<TPartitionKey>, TResult>, IDisposable
    {
        private readonly PartitionedStreamEventIngressStreamable<TPartitionKey, TPayload> entryPoint = null;
        private readonly FuseModule fuseModule;
        private readonly IObservable<PartitionedStreamEvent<TPartitionKey, TPayload>> observable;
        private readonly DisorderPolicy disorderPolicy;
        private readonly PartitionedFlushPolicy flushPolicy;
        private readonly PeriodicPunctuationPolicy punctuationPolicy;
        private readonly PeriodicLowWatermarkPolicy lowWatermarkPolicy;
        private readonly OnCompletedPolicy onCompletedPolicy;
        private readonly bool delayed;

        private readonly QueryContainer container;

        public PartitionedStreamEventIngressStreamableFused(
            IObservable<PartitionedStreamEvent<TPartitionKey, TPayload>> observable,
            DisorderPolicy disorderPolicy,
            PartitionedFlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            PeriodicLowWatermarkPolicy lowWatermarkPolicy,
            OnCompletedPolicy onCompletedPolicy,
            QueryContainer container,
            string identifier,
            FuseModule fuseModule,
            PartitionedStreamEventIngressStreamable<TPartitionKey, TPayload> entryPoint,
            StreamProperties<PartitionKey<TPartitionKey>, TResult> properties)
            : base(properties)
        {
            ArgumentNullException.ThrowIfNull(observable);
            ArgumentNullException.ThrowIfNull(identifier);

            this.IngressSiteIdentifier = identifier;
            this.observable = observable;
            this.disorderPolicy = disorderPolicy;
            this.flushPolicy = flushPolicy;
            this.punctuationPolicy = punctuationPolicy;
            this.lowWatermarkPolicy = lowWatermarkPolicy;
            this.onCompletedPolicy = onCompletedPolicy;
            this.container = container;
            this.delayed = container != null;
            this.fuseModule = fuseModule;
            this.entryPoint = entryPoint;

            this.properties = properties.ToRowBased();
        }

        public void Dispose() => this.entryPoint?.Dispose();

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(this.observable != null);
        }

        public IObservable<OutOfOrderPartitionedStreamEvent<TPartitionKey, TPayload>> GetDroppedAdjustedEventsDiagnostic()
        {
            return this.entryPoint.GetDroppedAdjustedEventsDiagnostic();
        }

        public override IDisposable Subscribe(IStreamObserver<PartitionKey<TPartitionKey>, TResult> observer)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            IIngressStreamObserver pipe = null;
            if (this.properties.IsColumnar) pipe = this.GetPipe(observer);
            else
            {
                pipe = PartitionedStreamEventSubscriptionCreator<TPartitionKey, TPayload, TResult>.CreateSubscription(
                    this.observable,
                    this.IngressSiteIdentifier,
                    this,
                    observer,
                    this.disorderPolicy,
                    this.flushPolicy,
                    this.punctuationPolicy,
                    this.lowWatermarkPolicy,
                    this.onCompletedPolicy,
                    this.entryPoint.diagnosticOutput,
                    this.fuseModule);
            }

            if (this.delayed)
            {
                this.container.RegisterIngressPipe(this.IngressSiteIdentifier, pipe);
                return pipe.DelayedDisposable;
            }
            else
            {
                pipe.Enable();
                return pipe;
            }
        }

        public string IngressSiteIdentifier { get; private set; } = Guid.NewGuid().ToString();

        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new();

        private bool CanGenerateColumnar()
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    this.fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    this.punctuationPolicy.ToString(),
                    this.lowWatermarkPolicy.ToString(),
                    this.disorderPolicy.ToString(),
                    (this.disorderPolicy.type != DisorderPolicyType.Throw && this.entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.GenerateFused<TPartitionKey, TPayload, TResult>(
                    this.disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    this.disorderPolicy.type != DisorderPolicyType.Throw && this.entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    this.fuseModule));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private IIngressStreamObserver GetPipe(IStreamObserver<PartitionKey<TPartitionKey>, TResult> observer)
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    this.fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    this.punctuationPolicy.ToString(),
                    this.lowWatermarkPolicy.ToString(),
                    this.disorderPolicy.ToString(),
                    (this.disorderPolicy.type != DisorderPolicyType.Throw && this.entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            object instance;
            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.GenerateFused<TPartitionKey, TPayload, TResult>(
                    this.disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    this.disorderPolicy.type != DisorderPolicyType.Throw && this.entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    this.fuseModule));
            instance = Activator.CreateInstance(
                generatedPipeType.Item1,
                this.observable, this.IngressSiteIdentifier, this, observer, this.disorderPolicy, this.flushPolicy, this.punctuationPolicy, this.lowWatermarkPolicy, this.onCompletedPolicy, this.entryPoint.diagnosticOutput);
            var returnValue = (IIngressStreamObserver)instance;
            return returnValue;
        }

        public override string ToString()
        {
            if (this.container != null)
                return "RegisterInput({0}, " + this.disorderPolicy.ToString() + ", " + this.flushPolicy.ToString() + ", " + this.punctuationPolicy.ToString() + ", " + this.lowWatermarkPolicy.ToString() + ", " + this.onCompletedPolicy.ToString() + ")";
            else
                return "ToStreamable(" + this.disorderPolicy.ToString() + ", " + this.flushPolicy.ToString() + ", " + this.punctuationPolicy.ToString() + ", " + this.lowWatermarkPolicy.ToString() + ", " + this.onCompletedPolicy.ToString() + ")";
        }

        public bool CanFuseSelect(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelect<TNewResult>(Expression<Func<TResult, TNewResult>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelect(expression),
                this.entryPoint,
                this.Properties.Select<TNewResult>(expression, false, false));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelect<TNewResult>(Expression<Func<long, TResult, TNewResult>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelect(expression),
                this.entryPoint,
                this.Properties.Select<TNewResult>(expression, true, false, true));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<PartitionKey<TPartitionKey>, TResult, TNewResult>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectWithKey(expression),
                this.entryPoint,
                this.Properties.Select<TNewResult>(expression, false, true));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<long, PartitionKey<TPartitionKey>, TResult, TNewResult>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectWithKey(expression),
                this.entryPoint,
                this.Properties.Select<TNewResult>(expression, true, true));
        }

        public bool CanFuseSelectMany(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectMany(expression),
                this.entryPoint,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<long, TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectMany(expression),
                this.entryPoint,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<PartitionKey<TPartitionKey>, TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectManyWithKey(expression),
                this.entryPoint,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<long, PartitionKey<TPartitionKey>, TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectManyWithKey(expression),
                this.entryPoint,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TResult> FuseWhere(Expression<Func<TResult, bool>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseWhere(expression),
                this.entryPoint,
                this.Properties.Where(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TResult> FuseSetDurationConstant(long value)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TResult>(
                this.observable,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSetDurationConstant(value),
                this.entryPoint,
                this.Properties.ToConstantDuration(true, value));
        }

        public IObservable<TNewResult> FuseEgressObservable<TNewResult>(Expression<Func<long, long, TResult, PartitionKey<TPartitionKey>, TNewResult>> expression, QueryContainer container, string identifier)
        {
            return new FusedObservable<PartitionKey<TPartitionKey>, PartitionedStreamEvent<TPartitionKey, TPayload>, TPayload, TResult, TNewResult>(
                this.observable,
                (o) => o.SyncTime,
                (o) => o.OtherTime,
                (o) => new PartitionKey<TPartitionKey>(o.PartitionKey),
                (o) => o.Payload,
                this.fuseModule,
                expression,
                container,
                this.IngressSiteIdentifier,
                identifier);
        }

        public bool CanFuseEgressObservable => Config.AllowFloatingReorderPolicy;
    }

    internal sealed class PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TResult> : Streamable<PartitionKey<TPartitionKey>, TResult>, IFusibleStreamable<PartitionKey<TPartitionKey>, TResult>, IDisposable
    {
        private readonly PartitionedIntervalIngressStreamable<TPartitionKey, TPayload> entryPoint = null;
        private readonly FuseModule fuseModule;
        private readonly IObservable<TPayload> observable;
        private readonly Expression<Func<TPayload, TPartitionKey>> partitionExtractor;
        private readonly Expression<Func<TPayload, long>> startEdgeExtractor;
        private readonly Expression<Func<TPayload, long>> endEdgeExtractor;
        private readonly DisorderPolicy disorderPolicy;
        private readonly PartitionedFlushPolicy flushPolicy;
        private readonly PeriodicPunctuationPolicy punctuationPolicy;
        private readonly PeriodicLowWatermarkPolicy lowWatermarkPolicy;
        private readonly OnCompletedPolicy onCompletedPolicy;
        private readonly bool delayed;

        private readonly QueryContainer container;

        public PartitionedIntervalIngressStreamableFused(
            IObservable<TPayload> observable,
            Expression<Func<TPayload, TPartitionKey>> partitionExtractor,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            DisorderPolicy disorderPolicy,
            PartitionedFlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            PeriodicLowWatermarkPolicy lowWatermarkPolicy,
            OnCompletedPolicy onCompletedPolicy,
            QueryContainer container,
            string identifier,
            FuseModule fuseModule,
            PartitionedIntervalIngressStreamable<TPartitionKey, TPayload> entryPoint,
            StreamProperties<PartitionKey<TPartitionKey>, TResult> properties)
            : base(properties)
        {
            ArgumentNullException.ThrowIfNull(observable);
            ArgumentNullException.ThrowIfNull(identifier);

            this.IngressSiteIdentifier = identifier;
            this.observable = observable;
            this.partitionExtractor = partitionExtractor;
            this.startEdgeExtractor = startEdgeExtractor;
            this.endEdgeExtractor = endEdgeExtractor;
            this.disorderPolicy = disorderPolicy;
            this.flushPolicy = flushPolicy;
            this.punctuationPolicy = punctuationPolicy;
            this.lowWatermarkPolicy = lowWatermarkPolicy;
            this.onCompletedPolicy = onCompletedPolicy;
            this.container = container;
            this.delayed = container != null;
            this.fuseModule = fuseModule;
            this.entryPoint = entryPoint;

            this.properties = properties.ToRowBased();
        }

        public void Dispose() => this.entryPoint?.Dispose();

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(this.observable != null);
        }

        public IObservable<OutOfOrderPartitionedStreamEvent<TPartitionKey, TPayload>> GetDroppedAdjustedEventsDiagnostic()
        {
            return this.entryPoint.GetDroppedAdjustedEventsDiagnostic();
        }

        public override IDisposable Subscribe(IStreamObserver<PartitionKey<TPartitionKey>, TResult> observer)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            IIngressStreamObserver pipe = null;
            if (this.properties.IsColumnar) pipe = this.GetPipe(observer);
            else
            {
                pipe = PartitionedIntervalSubscriptionCreator<TPartitionKey, TPayload, TResult>.CreateSubscription(
                    this.observable,
                    this.partitionExtractor,
                    this.startEdgeExtractor,
                    this.endEdgeExtractor,
                    this.IngressSiteIdentifier,
                    this,
                    observer,
                    this.disorderPolicy,
                    this.flushPolicy,
                    this.punctuationPolicy,
                    this.lowWatermarkPolicy,
                    this.onCompletedPolicy,
                    this.entryPoint.diagnosticOutput,
                    this.fuseModule);
            }

            if (this.delayed)
            {
                this.container.RegisterIngressPipe(this.IngressSiteIdentifier, pipe);
                return pipe.DelayedDisposable;
            }
            else
            {
                pipe.Enable();
                return pipe;
            }
        }

        public string IngressSiteIdentifier { get; private set; } = Guid.NewGuid().ToString();

        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new();

        private bool CanGenerateColumnar()
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    this.startEdgeExtractor.ExpressionToCSharp(),
                    this.endEdgeExtractor != null ? this.endEdgeExtractor.ExpressionToCSharp() : string.Empty,
                    this.partitionExtractor.ExpressionToCSharp()),
                Tuple.Create(
                    this.fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    this.punctuationPolicy.ToString(),
                    this.lowWatermarkPolicy.ToString(),
                    this.disorderPolicy.ToString(),
                    (this.disorderPolicy.type != DisorderPolicyType.Throw && this.entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.GenerateFused<TPartitionKey, TPayload, TResult>(
                    this.partitionExtractor,
                    this.startEdgeExtractor,
                    this.endEdgeExtractor,
                    this.disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    this.disorderPolicy.type != DisorderPolicyType.Throw && this.entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    this.fuseModule));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private IIngressStreamObserver GetPipe(IStreamObserver<PartitionKey<TPartitionKey>, TResult> observer)
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    this.startEdgeExtractor.ExpressionToCSharp(),
                    this.endEdgeExtractor != null ? this.endEdgeExtractor.ExpressionToCSharp() : string.Empty,
                    this.partitionExtractor.ExpressionToCSharp()),
                Tuple.Create(
                    this.fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    this.punctuationPolicy.ToString(),
                    this.lowWatermarkPolicy.ToString(),
                    this.disorderPolicy.ToString(),
                    (this.disorderPolicy.type != DisorderPolicyType.Throw && this.entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            object instance;
            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.GenerateFused<TPartitionKey, TPayload, TResult>(
                    this.partitionExtractor,
                    this.startEdgeExtractor,
                    this.endEdgeExtractor,
                    this.disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    this.disorderPolicy.type != DisorderPolicyType.Throw && this.entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    this.fuseModule));
            instance = Activator.CreateInstance(
                generatedPipeType.Item1,
                this.observable, this.IngressSiteIdentifier, this, observer, this.disorderPolicy, this.flushPolicy, this.punctuationPolicy, this.lowWatermarkPolicy, this.onCompletedPolicy, this.entryPoint.diagnosticOutput);
            var returnValue = (IIngressStreamObserver)instance;
            return returnValue;
        }

        public bool CanFuseSelect(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelect<TNewResult>(Expression<Func<TResult, TNewResult>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.partitionExtractor,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelect(expression),
                this.entryPoint,
                this.Properties.Select<TNewResult>(expression, false, false));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelect<TNewResult>(Expression<Func<long, TResult, TNewResult>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.partitionExtractor,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelect(expression),
                this.entryPoint,
                this.Properties.Select<TNewResult>(expression, true, false, true));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<PartitionKey<TPartitionKey>, TResult, TNewResult>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.partitionExtractor,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectWithKey(expression),
                this.entryPoint,
                this.Properties.Select<TNewResult>(expression, false, true));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<long, PartitionKey<TPartitionKey>, TResult, TNewResult>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.partitionExtractor,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectWithKey(expression),
                this.entryPoint,
                this.Properties.Select<TNewResult>(expression, true, true));
        }

        public bool CanFuseSelectMany(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.partitionExtractor,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectMany(expression),
                this.entryPoint,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<long, TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.partitionExtractor,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectMany(expression),
                this.entryPoint,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<PartitionKey<TPartitionKey>, TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.partitionExtractor,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectManyWithKey(expression),
                this.entryPoint,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<long, PartitionKey<TPartitionKey>, TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                this.observable,
                this.partitionExtractor,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSelectManyWithKey(expression),
                this.entryPoint,
                this.Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TResult> FuseWhere(Expression<Func<TResult, bool>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TResult>(
                this.observable,
                this.partitionExtractor,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseWhere(expression),
                this.entryPoint,
                this.Properties.Where(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TResult> FuseSetDurationConstant(long value)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TResult>(
                this.observable,
                this.partitionExtractor,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.disorderPolicy,
                this.flushPolicy,
                this.punctuationPolicy,
                this.lowWatermarkPolicy,
                this.onCompletedPolicy,
                this.container,
                this.IngressSiteIdentifier,
                this.fuseModule.Clone().FuseSetDurationConstant(value),
                this.entryPoint,
                this.Properties.ToConstantDuration(true, value));
        }

        public IObservable<TNewResult> FuseEgressObservable<TNewResult>(Expression<Func<long, long, TResult, PartitionKey<TPartitionKey>, TNewResult>> expression, QueryContainer container, string identifier)
        {
            return new FusedObservable<PartitionKey<TPartitionKey>, TPayload, TPayload, TResult, TNewResult>(
                this.observable,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                ParameterSubstituter.AddPartitionKey(this.partitionExtractor),
                (o) => o,
                this.fuseModule,
                expression,
                container,
                this.IngressSiteIdentifier,
                identifier);
        }

        public bool CanFuseEgressObservable => Config.AllowFloatingReorderPolicy;
    }
}