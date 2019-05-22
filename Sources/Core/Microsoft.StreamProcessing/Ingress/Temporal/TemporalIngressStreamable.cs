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
            Contract.Requires(observable != null);
            Contract.Requires(identifier != null);

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
                this.properties = properties.ToRowBased();
            }
            else this.properties = properties.ToDelayedColumnar(CanGenerateColumnar);
        }

        public void Dispose() => diagnosticOutput?.Dispose();

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(observable != null);
        }

        public IObservable<OutOfOrderStreamEvent<TPayload>> GetDroppedAdjustedEventsDiagnostic()
        {
            if (diagnosticOutput == null) diagnosticOutput = new DiagnosticObservable<TPayload>();
            return diagnosticOutput;
        }

        public override IDisposable Subscribe(IStreamObserver<Empty, TPayload> observer)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            IIngressStreamObserver pipe = null;
            if (properties.IsColumnar) pipe = GetPipe(observer);
            else
            {
                pipe = StreamEventSubscriptionCreator<TPayload, TPayload>.CreateSubscription(
                    observable,
                    this.IngressSiteIdentifier,
                    this,
                    observer,
                    disorderPolicy,
                    flushPolicy,
                    punctuationPolicy,
                    onCompletedPolicy,
                    diagnosticOutput,
                    fuseModule);
            }

            if (this.delayed)
            {
                container.RegisterIngressPipe(this.IngressSiteIdentifier, pipe);
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
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        private bool CanGenerateColumnar()
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    punctuationPolicy.ToString(),
                    disorderPolicy.ToString(),
                    (disorderPolicy.type != DisorderPolicyType.Throw && diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.Generate<TPayload>(
                    disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    disorderPolicy.type != DisorderPolicyType.Throw && diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    fuseModule));

            errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private IIngressStreamObserver GetPipe(IStreamObserver<Empty, TPayload> observer)
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    punctuationPolicy.ToString(),
                    disorderPolicy.ToString(),
                    (disorderPolicy.type != DisorderPolicyType.Throw && diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            object instance;
            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.Generate<TPayload>(
                    disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    disorderPolicy.type != DisorderPolicyType.Throw && diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    fuseModule));
            instance = Activator.CreateInstance(
                generatedPipeType.Item1,
                observable, this.IngressSiteIdentifier, this, observer, disorderPolicy, flushPolicy, punctuationPolicy, onCompletedPolicy, diagnosticOutput);
            var returnValue = (IIngressStreamObserver)instance;
            return returnValue;
        }

        public override string ToString()
        {
            if (container != null)
                return "RegisterInput({0}, " + disorderPolicy.ToString() + ", " + flushPolicy.ToString() + ", " + punctuationPolicy.ToString() + ", " + onCompletedPolicy.ToString() + ")";
            else
                return "ToStreamable(" + disorderPolicy.ToString() + ", " + flushPolicy.ToString() + ", " + punctuationPolicy.ToString() + ", " + onCompletedPolicy.ToString() + ")";
        }

        public bool CanFuseSelect(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<Empty, TNewResult> FuseSelect<TNewResult>(Expression<Func<TPayload, TNewResult>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelect(expression),
                this,
                Properties.Select<TNewResult>(expression, false, false));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelect<TNewResult>(Expression<Func<long, TPayload, TNewResult>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelect(expression),
                this,
                Properties.Select<TNewResult>(expression, true, false, true));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<Empty, TPayload, TNewResult>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectWithKey(expression),
                this,
                Properties.Select<TNewResult>(expression, false, true));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<long, Empty, TPayload, TNewResult>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectWithKey(expression),
                this,
                Properties.Select<TNewResult>(expression, true, true));
        }

        public bool CanFuseSelectMany(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<Empty, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectMany(expression),
                this,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<long, TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectMany(expression),
                this,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<Empty, TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectManyWithKey(expression),
                this,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<long, Empty, TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectManyWithKey(expression),
                this,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TPayload> FuseWhere(Expression<Func<TPayload, bool>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TPayload>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseWhere(expression),
                this,
                Properties.Where(expression));
        }

        public IFusibleStreamable<Empty, TPayload> FuseSetDurationConstant(long value)
        {
            return new StreamEventIngressStreamableFused<TPayload, TPayload>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSetDurationConstant(value),
                this,
                Properties.ToConstantDuration(true, value));
        }

        public IObservable<TNewResult> FuseEgressObservable<TNewResult>(Expression<Func<long, long, TPayload, Empty, TNewResult>> expression, QueryContainer container, string identifier)
        {
            return new FusedObservable<Empty, StreamEvent<TPayload>, TPayload, TPayload, TNewResult>(
                observable,
                (o) => o.SyncTime,
                (o) => o.OtherTime,
                (o) => Empty.Default,
                (o) => o.Payload,
                fuseModule,
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
            Contract.Requires(observable != null);
            Contract.Requires(identifier != null);

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
                this.properties = properties.ToRowBased();
            }
            else this.properties = properties.ToDelayedColumnar(CanGenerateColumnar);
        }

        public void Dispose() => diagnosticOutput?.Dispose();

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(observable != null);
        }

        public IObservable<OutOfOrderStreamEvent<TPayload>> GetDroppedAdjustedEventsDiagnostic()
        {
            if (diagnosticOutput == null) diagnosticOutput = new DiagnosticObservable<TPayload>();
            return diagnosticOutput;
        }

        public override IDisposable Subscribe(IStreamObserver<Empty, TPayload> observer)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            IIngressStreamObserver pipe = null;
            if (properties.IsColumnar) pipe = GetPipe(observer);
            else
            {
                pipe = IntervalSubscriptionCreator<TPayload, TPayload>.CreateSubscription(
                    observable,
                    startEdgeExtractor,
                    endEdgeExtractor,
                    this.IngressSiteIdentifier,
                    this,
                    observer,
                    disorderPolicy,
                    flushPolicy,
                    punctuationPolicy,
                    onCompletedPolicy,
                    diagnosticOutput,
                    fuseModule);
            }

            if (this.delayed)
            {
                container.RegisterIngressPipe(this.IngressSiteIdentifier, pipe);
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
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        private bool CanGenerateColumnar()
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    startEdgeExtractor.ExpressionToCSharp(),
                    endEdgeExtractor != null ? endEdgeExtractor.ExpressionToCSharp() : string.Empty),
                Tuple.Create(
                    fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    punctuationPolicy.ToString(),
                    disorderPolicy.ToString(),
                    (disorderPolicy.type != DisorderPolicyType.Throw && diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.Generate<TPayload>(
                    startEdgeExtractor,
                    endEdgeExtractor,
                    disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    disorderPolicy.type != DisorderPolicyType.Throw && diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    fuseModule));

            errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private IIngressStreamObserver GetPipe(IStreamObserver<Empty, TPayload> observer)
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    startEdgeExtractor.ExpressionToCSharp(),
                    endEdgeExtractor != null ? endEdgeExtractor.ExpressionToCSharp() : string.Empty),
                Tuple.Create(
                    fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    punctuationPolicy.ToString(),
                    disorderPolicy.ToString(),
                    (disorderPolicy.type != DisorderPolicyType.Throw && diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            object instance;
            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.Generate<TPayload>(
                    startEdgeExtractor,
                    endEdgeExtractor,
                    disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    disorderPolicy.type != DisorderPolicyType.Throw && diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    fuseModule));
            instance = Activator.CreateInstance(
                generatedPipeType.Item1,
                observable, this.IngressSiteIdentifier, this, observer, disorderPolicy, flushPolicy, punctuationPolicy, onCompletedPolicy, diagnosticOutput);
            var returnValue = (IIngressStreamObserver)instance;
            return returnValue;
        }

        public bool CanFuseSelect(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<Empty, TNewResult> FuseSelect<TNewResult>(Expression<Func<TPayload, TNewResult>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                observable,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelect(expression),
                this,
                Properties.Select<TNewResult>(expression, false, false));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelect<TNewResult>(Expression<Func<long, TPayload, TNewResult>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                observable,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelect(expression),
                this,
                Properties.Select<TNewResult>(expression, true, false, true));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<Empty, TPayload, TNewResult>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                observable,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectWithKey(expression),
                this,
                Properties.Select<TNewResult>(expression, false, true));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<long, Empty, TPayload, TNewResult>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                observable,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectWithKey(expression),
                this,
                Properties.Select<TNewResult>(expression, true, true));
        }

        public bool CanFuseSelectMany(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<Empty, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                observable,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectMany(expression),
                this,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<long, TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                observable,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectMany(expression),
                this,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<Empty, TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                observable,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectManyWithKey(expression),
                this,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<long, Empty, TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                observable,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectManyWithKey(expression),
                this,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TPayload> FuseWhere(Expression<Func<TPayload, bool>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TPayload>(
                observable,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseWhere(expression),
                this,
                Properties.Where(expression));
        }

        public IFusibleStreamable<Empty, TPayload> FuseSetDurationConstant(long value)
        {
            return new IntervalIngressStreamableFused<TPayload, TPayload>(
                observable,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSetDurationConstant(value),
                this,
                Properties.ToConstantDuration(true, value));
        }

        public IObservable<TNewResult> FuseEgressObservable<TNewResult>(Expression<Func<long, long, TPayload, Empty, TNewResult>> expression, QueryContainer container, string identifier)
        {
            return new FusedObservable<Empty, TPayload, TPayload, TPayload, TNewResult>(
                observable,
                startEdgeExtractor,
                endEdgeExtractor,
                (o) => Empty.Default,
                (o) => o,
                fuseModule,
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
            Contract.Requires(observable != null);
            Contract.Requires(identifier != null);

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

            this.properties = properties.ToRowBased();
        }

        public void Dispose() => diagnosticOutput?.Dispose();

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(observable != null);
        }

        public IObservable<OutOfOrderPartitionedStreamEvent<TPartitionKey, TPayload>> GetDroppedAdjustedEventsDiagnostic()
        {
            if (diagnosticOutput == null) diagnosticOutput = new PartitionedDiagnosticObservable<TPartitionKey, TPayload>();
            return diagnosticOutput;
        }

        public override IDisposable Subscribe(IStreamObserver<PartitionKey<TPartitionKey>, TPayload> observer)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            IIngressStreamObserver pipe = null;
            if (properties.IsColumnar) pipe = GetPipe(observer);
            else
            {
                pipe = PartitionedStreamEventSubscriptionCreator<TPartitionKey, TPayload, TPayload>.CreateSubscription(
                    observable,
                    this.IngressSiteIdentifier,
                    this,
                    observer,
                    disorderPolicy,
                    flushPolicy,
                    punctuationPolicy,
                    lowWatermarkPolicy,
                    onCompletedPolicy,
                    diagnosticOutput,
                    fuseModule);
            }

            if (this.delayed)
            {
                container.RegisterIngressPipe(this.IngressSiteIdentifier, pipe);
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
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        private bool CanGenerateColumnar()
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    punctuationPolicy.ToString(),
                    lowWatermarkPolicy.ToString(),
                    disorderPolicy.ToString(),
                    (disorderPolicy.type != DisorderPolicyType.Throw && diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.Generate<TPartitionKey, TPayload>(
                    disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    disorderPolicy.type != DisorderPolicyType.Throw && diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    fuseModule));

            errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private IIngressStreamObserver GetPipe(IStreamObserver<PartitionKey<TPartitionKey>, TPayload> observer)
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    punctuationPolicy.ToString(),
                    lowWatermarkPolicy.ToString(),
                    disorderPolicy.ToString(),
                    (disorderPolicy.type != DisorderPolicyType.Throw && diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            object instance;
            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.Generate<TPartitionKey, TPayload>(
                    disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    disorderPolicy.type != DisorderPolicyType.Throw && diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    fuseModule));
            instance = Activator.CreateInstance(
                generatedPipeType.Item1,
                observable, this.IngressSiteIdentifier, this, observer, disorderPolicy, flushPolicy, punctuationPolicy, lowWatermarkPolicy, onCompletedPolicy, diagnosticOutput);
            var returnValue = (IIngressStreamObserver)instance;
            return returnValue;
        }

        public override string ToString()
        {
            if (container != null)
                return "RegisterInput({0}, " + disorderPolicy.ToString() + ", " + flushPolicy.ToString() + ", " + punctuationPolicy.ToString() + ", " + lowWatermarkPolicy.ToString() + ", " + onCompletedPolicy.ToString() + ")";
            else
                return "ToStreamable(" + disorderPolicy.ToString() + ", " + flushPolicy.ToString() + ", " + punctuationPolicy.ToString() + ", " + lowWatermarkPolicy.ToString() + ", " + onCompletedPolicy.ToString() + ")";
        }

        public bool CanFuseSelect(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelect<TNewResult>(Expression<Func<TPayload, TNewResult>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelect(expression),
                this,
                Properties.Select<TNewResult>(expression, false, false));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelect<TNewResult>(Expression<Func<long, TPayload, TNewResult>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelect(expression),
                this,
                Properties.Select<TNewResult>(expression, true, false, true));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<PartitionKey<TPartitionKey>, TPayload, TNewResult>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectWithKey(expression),
                this,
                Properties.Select<TNewResult>(expression, false, true));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<long, PartitionKey<TPartitionKey>, TPayload, TNewResult>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectWithKey(expression),
                this,
                Properties.Select<TNewResult>(expression, true, true));
        }

        public bool CanFuseSelectMany(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectMany(expression),
                this,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<long, TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectMany(expression),
                this,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<PartitionKey<TPartitionKey>, TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectManyWithKey(expression),
                this,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<long, PartitionKey<TPartitionKey>, TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectManyWithKey(expression),
                this,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TPayload> FuseWhere(Expression<Func<TPayload, bool>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TPayload>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseWhere(expression),
                this,
                Properties.Where(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TPayload> FuseSetDurationConstant(long value)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TPayload>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSetDurationConstant(value),
                this,
                Properties.ToConstantDuration(true, value));
        }

        public IObservable<TNewResult> FuseEgressObservable<TNewResult>(Expression<Func<long, long, TPayload, PartitionKey<TPartitionKey>, TNewResult>> expression, QueryContainer container, string identifier)
        {
            return new FusedObservable<PartitionKey<TPartitionKey>, PartitionedStreamEvent<TPartitionKey, TPayload>, TPayload, TPayload, TNewResult>(
                observable,
                (o) => o.SyncTime,
                (o) => o.OtherTime,
                (o) => new PartitionKey<TPartitionKey>(o.PartitionKey),
                (o) => o.Payload,
                fuseModule,
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
            Contract.Requires(observable != null);
            Contract.Requires(identifier != null);

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

            this.properties = properties.ToRowBased();
        }

        public void Dispose() => diagnosticOutput?.Dispose();

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(observable != null);
        }

        public IObservable<OutOfOrderPartitionedStreamEvent<TPartitionKey, TPayload>> GetDroppedAdjustedEventsDiagnostic()
        {
            if (diagnosticOutput == null) diagnosticOutput = new PartitionedDiagnosticObservable<TPartitionKey, TPayload>();
            return diagnosticOutput;
        }

        public override IDisposable Subscribe(IStreamObserver<PartitionKey<TPartitionKey>, TPayload> observer)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            IIngressStreamObserver pipe = null;
            if (properties.IsColumnar) pipe = GetPipe(observer);
            else
            {
                pipe = PartitionedIntervalSubscriptionCreator<TPartitionKey, TPayload, TPayload>.CreateSubscription(
                    observable,
                    partitionExtractor,
                    startEdgeExtractor,
                    endEdgeExtractor,
                    this.IngressSiteIdentifier,
                    this,
                    observer,
                    disorderPolicy,
                    flushPolicy,
                    punctuationPolicy,
                    lowWatermarkPolicy,
                    onCompletedPolicy,
                    diagnosticOutput,
                    fuseModule);
            }

            if (this.delayed)
            {
                container.RegisterIngressPipe(this.IngressSiteIdentifier, pipe);
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
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        private bool CanGenerateColumnar()
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    startEdgeExtractor.ExpressionToCSharp(),
                    endEdgeExtractor != null ? endEdgeExtractor.ExpressionToCSharp() : string.Empty,
                    partitionExtractor.ExpressionToCSharp()),
                Tuple.Create(
                    fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    punctuationPolicy.ToString(),
                    lowWatermarkPolicy.ToString(),
                    disorderPolicy.ToString(),
                    (disorderPolicy.type != DisorderPolicyType.Throw && diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.Generate<TPartitionKey, TPayload>(
                    partitionExtractor,
                    startEdgeExtractor,
                    endEdgeExtractor,
                    disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    disorderPolicy.type != DisorderPolicyType.Throw && diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    fuseModule));

            errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private IIngressStreamObserver GetPipe(IStreamObserver<PartitionKey<TPartitionKey>, TPayload> observer)
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    startEdgeExtractor.ExpressionToCSharp(),
                    endEdgeExtractor != null ? endEdgeExtractor.ExpressionToCSharp() : string.Empty,
                    partitionExtractor.ExpressionToCSharp()),
                Tuple.Create(
                    fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    punctuationPolicy.ToString(),
                    lowWatermarkPolicy.ToString(),
                    disorderPolicy.ToString(),
                    (disorderPolicy.type != DisorderPolicyType.Throw && diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            object instance;
            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.Generate<TPartitionKey, TPayload>(
                    partitionExtractor,
                    startEdgeExtractor,
                    endEdgeExtractor,
                    disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    disorderPolicy.type != DisorderPolicyType.Throw && diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    fuseModule));
            instance = Activator.CreateInstance(
                generatedPipeType.Item1,
                observable, this.IngressSiteIdentifier, this, observer, disorderPolicy, flushPolicy, punctuationPolicy, lowWatermarkPolicy, onCompletedPolicy, diagnosticOutput);
            var returnValue = (IIngressStreamObserver)instance;
            return returnValue;
        }

        public bool CanFuseSelect(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelect<TNewResult>(Expression<Func<TPayload, TNewResult>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                partitionExtractor,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelect(expression),
                this,
                Properties.Select<TNewResult>(expression, false, false));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelect<TNewResult>(Expression<Func<long, TPayload, TNewResult>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                partitionExtractor,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelect(expression),
                this,
                Properties.Select<TNewResult>(expression, true, false, true));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<PartitionKey<TPartitionKey>, TPayload, TNewResult>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                partitionExtractor,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectWithKey(expression),
                this,
                Properties.Select<TNewResult>(expression, false, true));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<long, PartitionKey<TPartitionKey>, TPayload, TNewResult>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                partitionExtractor,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectWithKey(expression),
                this,
                Properties.Select<TNewResult>(expression, true, true));
        }

        public bool CanFuseSelectMany(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                partitionExtractor,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectMany(expression),
                this,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<long, TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                partitionExtractor,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectMany(expression),
                this,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<PartitionKey<TPartitionKey>, TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                partitionExtractor,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectManyWithKey(expression),
                this,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<long, PartitionKey<TPartitionKey>, TPayload, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                partitionExtractor,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectManyWithKey(expression),
                this,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TPayload> FuseWhere(Expression<Func<TPayload, bool>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TPayload>(
                observable,
                partitionExtractor,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseWhere(expression),
                this,
                Properties.Where(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TPayload> FuseSetDurationConstant(long value)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TPayload>(
                observable,
                partitionExtractor,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSetDurationConstant(value),
                this,
                Properties.ToConstantDuration(true, value));
        }

        public IObservable<TNewResult> FuseEgressObservable<TNewResult>(Expression<Func<long, long, TPayload, PartitionKey<TPartitionKey>, TNewResult>> expression, QueryContainer container, string identifier)
        {
            return new FusedObservable<PartitionKey<TPartitionKey>, TPayload, TPayload, TPayload, TNewResult>(
                observable,
                startEdgeExtractor,
                endEdgeExtractor,
                ParameterSubstituter.AddPartitionKey(partitionExtractor),
                (o) => o,
                fuseModule,
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
            Contract.Requires(observable != null);
            Contract.Requires(identifier != null);

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
            else this.properties = properties.ToDelayedColumnar(CanGenerateColumnar);
        }

        public void Dispose() => entryPoint?.Dispose();

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(observable != null);
        }

        public IObservable<OutOfOrderStreamEvent<TPayload>> GetDroppedAdjustedEventsDiagnostic()
        {
            return entryPoint.GetDroppedAdjustedEventsDiagnostic();
        }

        public override IDisposable Subscribe(IStreamObserver<Empty, TResult> observer)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            IIngressStreamObserver pipe = null;
            if (properties.IsColumnar) pipe = GetPipe(observer);
            else
            {
                pipe = StreamEventSubscriptionCreator<TPayload, TResult>.CreateSubscription(
                    observable,
                    this.IngressSiteIdentifier,
                    this,
                    observer,
                    disorderPolicy,
                    flushPolicy,
                    punctuationPolicy,
                    onCompletedPolicy,
                    entryPoint.diagnosticOutput,
                    fuseModule);
            }

            if (this.delayed)
            {
                container.RegisterIngressPipe(this.IngressSiteIdentifier, pipe);
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
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        private bool CanGenerateColumnar()
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    punctuationPolicy.ToString(),
                    disorderPolicy.ToString(),
                    (disorderPolicy.type != DisorderPolicyType.Throw && entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.GenerateFused<TPayload, TResult>(
                    disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    disorderPolicy.type != DisorderPolicyType.Throw && entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    fuseModule));

            errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private IIngressStreamObserver GetPipe(IStreamObserver<Empty, TResult> observer)
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    punctuationPolicy.ToString(),
                    disorderPolicy.ToString(),
                    (disorderPolicy.type != DisorderPolicyType.Throw && entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            object instance;
            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.GenerateFused<TPayload, TResult>(
                    disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    disorderPolicy.type != DisorderPolicyType.Throw && entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    fuseModule));
            instance = Activator.CreateInstance(
                generatedPipeType.Item1,
                observable, this.IngressSiteIdentifier, this, observer, disorderPolicy, flushPolicy, punctuationPolicy, onCompletedPolicy, entryPoint.diagnosticOutput);
            var returnValue = (IIngressStreamObserver)instance;
            return returnValue;
        }

        public override string ToString()
        {
            if (container != null)
                return "RegisterInput({0}, " + disorderPolicy.ToString() + ", " + flushPolicy.ToString() + ", " + punctuationPolicy.ToString() + ", " + onCompletedPolicy.ToString() + ")";
            else
                return "ToStreamable(" + disorderPolicy.ToString() + ", " + flushPolicy.ToString() + ", " + punctuationPolicy.ToString() + ", " + onCompletedPolicy.ToString() + ")";
        }

        public bool CanFuseSelect(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<Empty, TNewResult> FuseSelect<TNewResult>(Expression<Func<TResult, TNewResult>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelect(expression),
                entryPoint,
                Properties.Select<TNewResult>(expression, false, false));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelect<TNewResult>(Expression<Func<long, TResult, TNewResult>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelect(expression),
                entryPoint,
                Properties.Select<TNewResult>(expression, true, false, true));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<Empty, TResult, TNewResult>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectWithKey(expression),
                entryPoint,
                Properties.Select<TNewResult>(expression, false, true));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<long, Empty, TResult, TNewResult>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectWithKey(expression),
                entryPoint,
                Properties.Select<TNewResult>(expression, true, true));
        }

        public bool CanFuseSelectMany(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<Empty, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectMany(expression),
                entryPoint,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<long, TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectMany(expression),
                entryPoint,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<Empty, TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectManyWithKey(expression),
                entryPoint,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<long, Empty, TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectManyWithKey(expression),
                entryPoint,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TResult> FuseWhere(Expression<Func<TResult, bool>> expression)
        {
            return new StreamEventIngressStreamableFused<TPayload, TResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseWhere(expression),
                entryPoint,
                Properties.Where(expression));
        }

        public IFusibleStreamable<Empty, TResult> FuseSetDurationConstant(long value)
        {
            return new StreamEventIngressStreamableFused<TPayload, TResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSetDurationConstant(value),
                entryPoint,
                Properties.ToConstantDuration(true, value));
        }

        public IObservable<TNewResult> FuseEgressObservable<TNewResult>(Expression<Func<long, long, TResult, Empty, TNewResult>> expression, QueryContainer container, string identifier)
        {
            return new FusedObservable<Empty, StreamEvent<TPayload>, TPayload, TResult, TNewResult>(
                observable,
                (o) => o.SyncTime,
                (o) => o.OtherTime,
                (o) => Empty.Default,
                (o) => o.Payload,
                fuseModule,
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
            Contract.Requires(observable != null);
            Contract.Requires(identifier != null);

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
            else this.properties = properties.ToDelayedColumnar(CanGenerateColumnar);
        }

        public void Dispose() => entryPoint?.Dispose();

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(observable != null);
        }

        public IObservable<OutOfOrderStreamEvent<TPayload>> GetDroppedAdjustedEventsDiagnostic()
        {
            return entryPoint.GetDroppedAdjustedEventsDiagnostic();
        }

        public override IDisposable Subscribe(IStreamObserver<Empty, TResult> observer)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            IIngressStreamObserver pipe = null;
            if (properties.IsColumnar) pipe = GetPipe(observer);
            else
            {
                pipe = IntervalSubscriptionCreator<TPayload, TResult>.CreateSubscription(
                    observable,
                    startEdgeExtractor,
                    endEdgeExtractor,
                    this.IngressSiteIdentifier,
                    this,
                    observer,
                    disorderPolicy,
                    flushPolicy,
                    punctuationPolicy,
                    onCompletedPolicy,
                    entryPoint.diagnosticOutput,
                    fuseModule);
            }

            if (this.delayed)
            {
                container.RegisterIngressPipe(this.IngressSiteIdentifier, pipe);
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
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        private bool CanGenerateColumnar()
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    startEdgeExtractor.ExpressionToCSharp(),
                    endEdgeExtractor != null ? endEdgeExtractor.ExpressionToCSharp() : string.Empty),
                Tuple.Create(
                    fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    punctuationPolicy.ToString(),
                    disorderPolicy.ToString(),
                    (disorderPolicy.type != DisorderPolicyType.Throw && entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.GenerateFused<TPayload, TResult>(
                    startEdgeExtractor,
                    endEdgeExtractor,
                    disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    disorderPolicy.type != DisorderPolicyType.Throw && entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    fuseModule));

            errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private IIngressStreamObserver GetPipe(IStreamObserver<Empty, TResult> observer)
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    startEdgeExtractor.ExpressionToCSharp(),
                    endEdgeExtractor != null ? endEdgeExtractor.ExpressionToCSharp() : string.Empty),
                Tuple.Create(
                    fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    punctuationPolicy.ToString(),
                    disorderPolicy.ToString(),
                    (disorderPolicy.type != DisorderPolicyType.Throw && entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            object instance;
            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.GenerateFused<TPayload, TResult>(
                    startEdgeExtractor,
                    endEdgeExtractor,
                    disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    disorderPolicy.type != DisorderPolicyType.Throw && entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    fuseModule));
            instance = Activator.CreateInstance(
                generatedPipeType.Item1,
                observable, this.IngressSiteIdentifier, this, observer, disorderPolicy, flushPolicy, punctuationPolicy, onCompletedPolicy, entryPoint.diagnosticOutput);
            var returnValue = (IIngressStreamObserver)instance;
            return returnValue;
        }

        public bool CanFuseSelect(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<Empty, TNewResult> FuseSelect<TNewResult>(Expression<Func<TResult, TNewResult>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                observable,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelect(expression),
                entryPoint,
                Properties.Select<TNewResult>(expression, false, false));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelect<TNewResult>(Expression<Func<long, TResult, TNewResult>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                observable,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelect(expression),
                entryPoint,
                Properties.Select<TNewResult>(expression, true, false, true));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<Empty, TResult, TNewResult>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                observable,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectWithKey(expression),
                entryPoint,
                Properties.Select<TNewResult>(expression, false, true));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<long, Empty, TResult, TNewResult>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                observable,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectWithKey(expression),
                entryPoint,
                Properties.Select<TNewResult>(expression, true, true));
        }

        public bool CanFuseSelectMany(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<Empty, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                observable,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectMany(expression),
                entryPoint,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<long, TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                observable,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectMany(expression),
                entryPoint,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<Empty, TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                observable,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectManyWithKey(expression),
                entryPoint,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<long, Empty, TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TNewResult>(
                observable,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectManyWithKey(expression),
                entryPoint,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<Empty, TResult> FuseWhere(Expression<Func<TResult, bool>> expression)
        {
            return new IntervalIngressStreamableFused<TPayload, TResult>(
                observable,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseWhere(expression),
                entryPoint,
                Properties.Where(expression));
        }

        public IFusibleStreamable<Empty, TResult> FuseSetDurationConstant(long value)
        {
            return new IntervalIngressStreamableFused<TPayload, TResult>(
                observable,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSetDurationConstant(value),
                entryPoint,
                Properties.ToConstantDuration(true, value));
        }

        public IObservable<TNewResult> FuseEgressObservable<TNewResult>(Expression<Func<long, long, TResult, Empty, TNewResult>> expression, QueryContainer container, string identifier)
        {
            return new FusedObservable<Empty, TPayload, TPayload, TResult, TNewResult>(
                observable,
                startEdgeExtractor,
                endEdgeExtractor,
                (o) => Empty.Default,
                (o) => o,
                fuseModule,
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
            Contract.Requires(observable != null);
            Contract.Requires(identifier != null);

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

        public void Dispose() => entryPoint?.Dispose();

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(observable != null);
        }

        public IObservable<OutOfOrderPartitionedStreamEvent<TPartitionKey, TPayload>> GetDroppedAdjustedEventsDiagnostic()
        {
            return entryPoint.GetDroppedAdjustedEventsDiagnostic();
        }

        public override IDisposable Subscribe(IStreamObserver<PartitionKey<TPartitionKey>, TResult> observer)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            IIngressStreamObserver pipe = null;
            if (properties.IsColumnar) pipe = GetPipe(observer);
            else
            {
                pipe = PartitionedStreamEventSubscriptionCreator<TPartitionKey, TPayload, TResult>.CreateSubscription(
                    observable,
                    this.IngressSiteIdentifier,
                    this,
                    observer,
                    disorderPolicy,
                    flushPolicy,
                    punctuationPolicy,
                    lowWatermarkPolicy,
                    onCompletedPolicy,
                    entryPoint.diagnosticOutput,
                    fuseModule);
            }

            if (this.delayed)
            {
                container.RegisterIngressPipe(this.IngressSiteIdentifier, pipe);
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
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        private bool CanGenerateColumnar()
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    punctuationPolicy.ToString(),
                    lowWatermarkPolicy.ToString(),
                    disorderPolicy.ToString(),
                    (disorderPolicy.type != DisorderPolicyType.Throw && entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.GenerateFused<TPartitionKey, TPayload, TResult>(
                    disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    disorderPolicy.type != DisorderPolicyType.Throw && entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    fuseModule));

            errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private IIngressStreamObserver GetPipe(IStreamObserver<PartitionKey<TPartitionKey>, TResult> observer)
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    punctuationPolicy.ToString(),
                    lowWatermarkPolicy.ToString(),
                    disorderPolicy.ToString(),
                    (disorderPolicy.type != DisorderPolicyType.Throw && entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            object instance;
            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.GenerateFused<TPartitionKey, TPayload, TResult>(
                    disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    disorderPolicy.type != DisorderPolicyType.Throw && entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    fuseModule));
            instance = Activator.CreateInstance(
                generatedPipeType.Item1,
                observable, this.IngressSiteIdentifier, this, observer, disorderPolicy, flushPolicy, punctuationPolicy, lowWatermarkPolicy, onCompletedPolicy, entryPoint.diagnosticOutput);
            var returnValue = (IIngressStreamObserver)instance;
            return returnValue;
        }

        public override string ToString()
        {
            if (container != null)
                return "RegisterInput({0}, " + disorderPolicy.ToString() + ", " + flushPolicy.ToString() + ", " + punctuationPolicy.ToString() + ", " + lowWatermarkPolicy.ToString() + ", " + onCompletedPolicy.ToString() + ")";
            else
                return "ToStreamable(" + disorderPolicy.ToString() + ", " + flushPolicy.ToString() + ", " + punctuationPolicy.ToString() + ", " + lowWatermarkPolicy.ToString() + ", " + onCompletedPolicy.ToString() + ")";
        }

        public bool CanFuseSelect(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelect<TNewResult>(Expression<Func<TResult, TNewResult>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelect(expression),
                entryPoint,
                Properties.Select<TNewResult>(expression, false, false));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelect<TNewResult>(Expression<Func<long, TResult, TNewResult>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelect(expression),
                entryPoint,
                Properties.Select<TNewResult>(expression, true, false, true));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<PartitionKey<TPartitionKey>, TResult, TNewResult>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectWithKey(expression),
                entryPoint,
                Properties.Select<TNewResult>(expression, false, true));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<long, PartitionKey<TPartitionKey>, TResult, TNewResult>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectWithKey(expression),
                entryPoint,
                Properties.Select<TNewResult>(expression, true, true));
        }

        public bool CanFuseSelectMany(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectMany(expression),
                entryPoint,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<long, TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectMany(expression),
                entryPoint,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<PartitionKey<TPartitionKey>, TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectManyWithKey(expression),
                entryPoint,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<long, PartitionKey<TPartitionKey>, TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectManyWithKey(expression),
                entryPoint,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TResult> FuseWhere(Expression<Func<TResult, bool>> expression)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseWhere(expression),
                entryPoint,
                Properties.Where(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TResult> FuseSetDurationConstant(long value)
        {
            return new PartitionedStreamEventIngressStreamableFused<TPartitionKey, TPayload, TResult>(
                observable,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSetDurationConstant(value),
                entryPoint,
                Properties.ToConstantDuration(true, value));
        }

        public IObservable<TNewResult> FuseEgressObservable<TNewResult>(Expression<Func<long, long, TResult, PartitionKey<TPartitionKey>, TNewResult>> expression, QueryContainer container, string identifier)
        {
            return new FusedObservable<PartitionKey<TPartitionKey>, PartitionedStreamEvent<TPartitionKey, TPayload>, TPayload, TResult, TNewResult>(
                observable,
                (o) => o.SyncTime,
                (o) => o.OtherTime,
                (o) => new PartitionKey<TPartitionKey>(o.PartitionKey),
                (o) => o.Payload,
                fuseModule,
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
            Contract.Requires(observable != null);
            Contract.Requires(identifier != null);

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

        public void Dispose() => entryPoint?.Dispose();

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(observable != null);
        }

        public IObservable<OutOfOrderPartitionedStreamEvent<TPartitionKey, TPayload>> GetDroppedAdjustedEventsDiagnostic()
        {
            return entryPoint.GetDroppedAdjustedEventsDiagnostic();
        }

        public override IDisposable Subscribe(IStreamObserver<PartitionKey<TPartitionKey>, TResult> observer)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            IIngressStreamObserver pipe = null;
            if (properties.IsColumnar) pipe = GetPipe(observer);
            else
            {
                pipe = PartitionedIntervalSubscriptionCreator<TPartitionKey, TPayload, TResult>.CreateSubscription(
                    observable,
                    partitionExtractor,
                    startEdgeExtractor,
                    endEdgeExtractor,
                    this.IngressSiteIdentifier,
                    this,
                    observer,
                    disorderPolicy,
                    flushPolicy,
                    punctuationPolicy,
                    lowWatermarkPolicy,
                    onCompletedPolicy,
                    entryPoint.diagnosticOutput,
                    fuseModule);
            }

            if (this.delayed)
            {
                container.RegisterIngressPipe(this.IngressSiteIdentifier, pipe);
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
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        private bool CanGenerateColumnar()
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    startEdgeExtractor.ExpressionToCSharp(),
                    endEdgeExtractor != null ? endEdgeExtractor.ExpressionToCSharp() : string.Empty,
                    partitionExtractor.ExpressionToCSharp()),
                Tuple.Create(
                    fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    punctuationPolicy.ToString(),
                    lowWatermarkPolicy.ToString(),
                    disorderPolicy.ToString(),
                    (disorderPolicy.type != DisorderPolicyType.Throw && entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.GenerateFused<TPartitionKey, TPayload, TResult>(
                    partitionExtractor,
                    startEdgeExtractor,
                    endEdgeExtractor,
                    disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    disorderPolicy.type != DisorderPolicyType.Throw && entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    fuseModule));

            errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private IIngressStreamObserver GetPipe(IStreamObserver<PartitionKey<TPartitionKey>, TResult> observer)
        {
            var lookupKey = CacheKey.Create(
                Tuple.Create(
                    startEdgeExtractor.ExpressionToCSharp(),
                    endEdgeExtractor != null ? endEdgeExtractor.ExpressionToCSharp() : string.Empty,
                    partitionExtractor.ExpressionToCSharp()),
                Tuple.Create(
                    fuseModule.ToString(),
                    Config.AllowFloatingReorderPolicy,
                    punctuationPolicy.ToString(),
                    lowWatermarkPolicy.ToString(),
                    disorderPolicy.ToString(),
                    (disorderPolicy.type != DisorderPolicyType.Throw && entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty)));

            object instance;
            var generatedPipeType = cachedPipes.GetOrAdd(
                lookupKey,
                key => TemporalIngressTemplate.GenerateFused<TPartitionKey, TPayload, TResult>(
                    partitionExtractor,
                    startEdgeExtractor,
                    endEdgeExtractor,
                    disorderPolicy.reorderLatency > 0 ? "WithLatency" : string.Empty,
                    disorderPolicy.type != DisorderPolicyType.Throw && entryPoint.diagnosticOutput != null ? "WithDiagnostic" : string.Empty,
                    fuseModule));
            instance = Activator.CreateInstance(
                generatedPipeType.Item1,
                observable, this.IngressSiteIdentifier, this, observer, disorderPolicy, flushPolicy, punctuationPolicy, lowWatermarkPolicy, onCompletedPolicy, entryPoint.diagnosticOutput);
            var returnValue = (IIngressStreamObserver)instance;
            return returnValue;
        }

        public bool CanFuseSelect(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelect<TNewResult>(Expression<Func<TResult, TNewResult>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                partitionExtractor,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelect(expression),
                entryPoint,
                Properties.Select<TNewResult>(expression, false, false));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelect<TNewResult>(Expression<Func<long, TResult, TNewResult>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                partitionExtractor,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelect(expression),
                entryPoint,
                Properties.Select<TNewResult>(expression, true, false, true));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<PartitionKey<TPartitionKey>, TResult, TNewResult>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                partitionExtractor,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectWithKey(expression),
                entryPoint,
                Properties.Select<TNewResult>(expression, false, true));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectWithKey<TNewResult>(Expression<Func<long, PartitionKey<TPartitionKey>, TResult, TNewResult>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                partitionExtractor,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectWithKey(expression),
                entryPoint,
                Properties.Select<TNewResult>(expression, true, true));
        }

        public bool CanFuseSelectMany(LambdaExpression expression, bool hasStart, bool hasKey) => true;

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                partitionExtractor,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectMany(expression),
                entryPoint,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectMany<TNewResult>(Expression<Func<long, TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                partitionExtractor,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectMany(expression),
                entryPoint,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<PartitionKey<TPartitionKey>, TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                partitionExtractor,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectManyWithKey(expression),
                entryPoint,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TNewResult> FuseSelectManyWithKey<TNewResult>(Expression<Func<long, PartitionKey<TPartitionKey>, TResult, System.Collections.Generic.IEnumerable<TNewResult>>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TNewResult>(
                observable,
                partitionExtractor,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSelectManyWithKey(expression),
                entryPoint,
                Properties.SelectMany<TNewResult>(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TResult> FuseWhere(Expression<Func<TResult, bool>> expression)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TResult>(
                observable,
                partitionExtractor,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseWhere(expression),
                entryPoint,
                Properties.Where(expression));
        }

        public IFusibleStreamable<PartitionKey<TPartitionKey>, TResult> FuseSetDurationConstant(long value)
        {
            return new PartitionedIntervalIngressStreamableFused<TPartitionKey, TPayload, TResult>(
                observable,
                partitionExtractor,
                startEdgeExtractor,
                endEdgeExtractor,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                container,
                this.IngressSiteIdentifier,
                fuseModule.Clone().FuseSetDurationConstant(value),
                entryPoint,
                Properties.ToConstantDuration(true, value));
        }

        public IObservable<TNewResult> FuseEgressObservable<TNewResult>(Expression<Func<long, long, TResult, PartitionKey<TPartitionKey>, TNewResult>> expression, QueryContainer container, string identifier)
        {
            return new FusedObservable<PartitionKey<TPartitionKey>, TPayload, TPayload, TResult, TNewResult>(
                observable,
                startEdgeExtractor,
                endEdgeExtractor,
                ParameterSubstituter.AddPartitionKey(partitionExtractor),
                (o) => o,
                fuseModule,
                expression,
                container,
                this.IngressSiteIdentifier,
                identifier);
        }

        public bool CanFuseEgressObservable => Config.AllowFloatingReorderPolicy;
    }
}