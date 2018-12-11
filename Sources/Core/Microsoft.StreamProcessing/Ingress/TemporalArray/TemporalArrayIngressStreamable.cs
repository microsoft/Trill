// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    internal sealed class StreamEventArrayIngressStreamable<TPayload> : Streamable<Empty, TPayload>, IObservableIngressStreamable<TPayload>, IDisposable
    {
        private readonly IObservable<ArraySegment<StreamEvent<TPayload>>> observable;
        private readonly OnCompletedPolicy onCompletedPolicy;
        private readonly bool delayed;
        private readonly QueryContainer container;

        private DiagnosticObservable<TPayload> diagnosticOutput;

        public StreamEventArrayIngressStreamable(
            IObservable<ArraySegment<StreamEvent<TPayload>>> observable,
            OnCompletedPolicy onCompletedPolicy,
            QueryContainer container,
            string identifier)
            : base((Config.ForceRowBasedExecution || !typeof(TPayload).CanRepresentAsColumnar()
                ? StreamProperties<Empty, TPayload>.Default.ToRowBased()
                : StreamProperties<Empty, TPayload>.Default).SetQueryContainer(container))
        {
            Contract.Requires(observable != null);
            Contract.Requires(identifier != null);

            this.IngressSiteIdentifier = identifier;
            this.observable = observable;
            this.onCompletedPolicy = onCompletedPolicy;
            this.container = container;
            this.delayed = container != null;

            if (delayed) container.RegisterIngressSite(identifier);
        }

        public void Dispose()
        {
            if (diagnosticOutput != null) diagnosticOutput.Dispose();
        }

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(this.observable != null);
        }

        public IObservable<OutOfOrderStreamEvent<TPayload>> GetDroppedAdjustedEventsDiagnostic()
        {
            if (diagnosticOutput == null) diagnosticOutput = new DiagnosticObservable<TPayload>();
            return diagnosticOutput;
        }

        public override IDisposable Subscribe(IStreamObserver<Empty, TPayload> observer)
        {
            Contract.EnsuresOnThrow<IngressException>(true);
            var subscription = StreamEventArraySubscriptionCreator<TPayload>.CreateSubscription(
                this.observable,
                this.IngressSiteIdentifier,
                this,
                observer,
                onCompletedPolicy,
                diagnosticOutput);

            if (delayed)
            {
                container.RegisterIngressPipe(this.IngressSiteIdentifier, subscription);
                return subscription.DelayedDisposable;
            }
            else
            {
                subscription.Enable();
                return subscription;
            }
        }

        public string IngressSiteIdentifier { get; }
    }

    internal sealed class IntervalArrayIngressStreamable<TPayload> : Streamable<Empty, TPayload>, IObservableIngressStreamable<TPayload>, IDisposable
    {
        private readonly IObservable<ArraySegment<TPayload>> observable;
        private readonly Expression<Func<TPayload, long>> startEdgeExtractor;
        private readonly Expression<Func<TPayload, long>> endEdgeExtractor;
        private readonly OnCompletedPolicy onCompletedPolicy;
        private readonly bool delayed;
        private readonly QueryContainer container;

        private DiagnosticObservable<TPayload> diagnosticOutput;

        public IntervalArrayIngressStreamable(
            IObservable<ArraySegment<TPayload>> observable,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            OnCompletedPolicy onCompletedPolicy,
            QueryContainer container,
            string identifier)
            : base((Config.ForceRowBasedExecution || !typeof(TPayload).CanRepresentAsColumnar()
                ? StreamProperties<Empty, TPayload>.DefaultIngress(startEdgeExtractor, endEdgeExtractor).ToRowBased()
                : StreamProperties<Empty, TPayload>.DefaultIngress(startEdgeExtractor, endEdgeExtractor)).SetQueryContainer(container))
        {
            Contract.Requires(observable != null);
            Contract.Requires(identifier != null);

            this.IngressSiteIdentifier = identifier;
            this.observable = observable;
            this.startEdgeExtractor = startEdgeExtractor;
            this.endEdgeExtractor = endEdgeExtractor;
            this.onCompletedPolicy = onCompletedPolicy;
            this.container = container;
            this.delayed = container != null;

            if (delayed) container.RegisterIngressSite(identifier);
        }

        public void Dispose()
        {
            if (diagnosticOutput != null) diagnosticOutput.Dispose();
        }

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(this.observable != null);
        }

        public IObservable<OutOfOrderStreamEvent<TPayload>> GetDroppedAdjustedEventsDiagnostic()
        {
            if (diagnosticOutput == null) diagnosticOutput = new DiagnosticObservable<TPayload>();
            return diagnosticOutput;
        }

        public override IDisposable Subscribe(IStreamObserver<Empty, TPayload> observer)
        {
            Contract.EnsuresOnThrow<IngressException>(true);
            var subscription = IntervalArraySubscriptionCreator<TPayload>.CreateSubscription(
                this.observable,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.IngressSiteIdentifier,
                this,
                observer,
                onCompletedPolicy,
                diagnosticOutput);

            if (delayed)
            {
                container.RegisterIngressPipe(this.IngressSiteIdentifier, subscription);
                return subscription.DelayedDisposable;
            }
            else
            {
                subscription.Enable();
                return subscription;
            }
        }

        public string IngressSiteIdentifier { get; }
    }

    internal sealed class PartitionedStreamEventArrayIngressStreamable<TPartitionKey, TPayload> : Streamable<PartitionKey<TPartitionKey>, TPayload>, IPartitionedIngressStreamable<TPartitionKey, TPayload>, IDisposable
    {
        private readonly IObservable<ArraySegment<PartitionedStreamEvent<TPartitionKey, TPayload>>> observable;
        private readonly OnCompletedPolicy onCompletedPolicy;
        private readonly bool delayed;
        private readonly QueryContainer container;

        private PartitionedDiagnosticObservable<TPartitionKey, TPayload> diagnosticOutput;

        public PartitionedStreamEventArrayIngressStreamable(
            IObservable<ArraySegment<PartitionedStreamEvent<TPartitionKey, TPayload>>> observable,
            OnCompletedPolicy onCompletedPolicy,
            QueryContainer container,
            string identifier)
            : base((Config.ForceRowBasedExecution || !typeof(TPayload).CanRepresentAsColumnar()
                ? StreamProperties<PartitionKey<TPartitionKey>, TPayload>.Default.ToRowBased()
                : StreamProperties<PartitionKey<TPartitionKey>, TPayload>.Default).SetQueryContainer(container))
        {
            Contract.Requires(observable != null);
            Contract.Requires(identifier != null);

            this.IngressSiteIdentifier = identifier;
            this.observable = observable;
            this.onCompletedPolicy = onCompletedPolicy;
            this.container = container;
            this.delayed = container != null;

            if (delayed) container.RegisterIngressSite(identifier);
        }

        public void Dispose()
        {
            if (diagnosticOutput != null) diagnosticOutput.Dispose();
        }

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(this.observable != null);
        }

        public IObservable<OutOfOrderPartitionedStreamEvent<TPartitionKey, TPayload>> GetDroppedAdjustedEventsDiagnostic()
        {
            if (diagnosticOutput == null) diagnosticOutput = new PartitionedDiagnosticObservable<TPartitionKey, TPayload>();
            return diagnosticOutput;
        }

        public override IDisposable Subscribe(IStreamObserver<PartitionKey<TPartitionKey>, TPayload> observer)
        {
            Contract.EnsuresOnThrow<IngressException>(true);
            var subscription = PartitionedStreamEventArraySubscriptionCreator<TPartitionKey, TPayload>.CreateSubscription(
                this.observable,
                this.IngressSiteIdentifier,
                this,
                observer,
                onCompletedPolicy,
                diagnosticOutput);

            if (delayed)
            {
                container.RegisterIngressPipe(this.IngressSiteIdentifier, subscription);
                return subscription.DelayedDisposable;
            }
            else
            {
                subscription.Enable();
                return subscription;
            }
        }

        public string IngressSiteIdentifier { get; }
    }

    internal sealed class PartitionedIntervalArrayIngressStreamable<TPartitionKey, TPayload> : Streamable<PartitionKey<TPartitionKey>, TPayload>, IPartitionedIngressStreamable<TPartitionKey, TPayload>, IDisposable
    {
        private readonly IObservable<ArraySegment<TPayload>> observable;
        private readonly Expression<Func<TPayload, TPartitionKey>> partitionExtractor;
        private readonly Expression<Func<TPayload, long>> startEdgeExtractor;
        private readonly Expression<Func<TPayload, long>> endEdgeExtractor;
        private readonly OnCompletedPolicy onCompletedPolicy;
        private readonly bool delayed;
        private readonly QueryContainer container;

        private PartitionedDiagnosticObservable<TPartitionKey, TPayload> diagnosticOutput;

        public PartitionedIntervalArrayIngressStreamable(
            IObservable<ArraySegment<TPayload>> observable,
            Expression<Func<TPayload, TPartitionKey>> partitionExtractor,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            OnCompletedPolicy onCompletedPolicy,
            QueryContainer container,
            string identifier)
            : base((Config.ForceRowBasedExecution || !typeof(TPayload).CanRepresentAsColumnar()
                ? StreamProperties<PartitionKey<TPartitionKey>, TPayload>.DefaultIngress(startEdgeExtractor, endEdgeExtractor).ToRowBased()
                : StreamProperties<PartitionKey<TPartitionKey>, TPayload>.DefaultIngress(startEdgeExtractor, endEdgeExtractor)).SetQueryContainer(container))
        {
            Contract.Requires(observable != null);
            Contract.Requires(identifier != null);

            this.IngressSiteIdentifier = identifier;
            this.observable = observable;
            this.partitionExtractor = partitionExtractor;
            this.startEdgeExtractor = startEdgeExtractor;
            this.endEdgeExtractor = endEdgeExtractor;
            this.onCompletedPolicy = onCompletedPolicy;
            this.container = container;
            this.delayed = container != null;

            if (delayed) container.RegisterIngressSite(identifier);
        }

        public void Dispose()
        {
            if (diagnosticOutput != null) diagnosticOutput.Dispose();
        }

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(this.observable != null);
        }

        public IObservable<OutOfOrderPartitionedStreamEvent<TPartitionKey, TPayload>> GetDroppedAdjustedEventsDiagnostic()
        {
            if (diagnosticOutput == null) diagnosticOutput = new PartitionedDiagnosticObservable<TPartitionKey, TPayload>();
            return diagnosticOutput;
        }

        public override IDisposable Subscribe(IStreamObserver<PartitionKey<TPartitionKey>, TPayload> observer)
        {
            Contract.EnsuresOnThrow<IngressException>(true);
            var subscription = PartitionedIntervalArraySubscriptionCreator<TPartitionKey, TPayload>.CreateSubscription(
                this.observable,
                this.partitionExtractor,
                this.startEdgeExtractor,
                this.endEdgeExtractor,
                this.IngressSiteIdentifier,
                this,
                observer,
                onCompletedPolicy,
                diagnosticOutput);

            if (delayed)
            {
                container.RegisterIngressPipe(this.IngressSiteIdentifier, subscription);
                return subscription.DelayedDisposable;
            }
            else
            {
                subscription.Enable();
                return subscription;
            }
        }

        public string IngressSiteIdentifier { get; }
    }

}