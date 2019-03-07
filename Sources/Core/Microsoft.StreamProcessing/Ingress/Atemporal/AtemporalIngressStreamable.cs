// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;

namespace Microsoft.StreamProcessing
{
    internal sealed class MonotonicIngressStreamable<TPayload> : Streamable<Empty, TPayload>, IIngressStreamable<Empty, TPayload>
    {
        private readonly IObservable<TPayload> observable;
        private readonly FlushPolicy flushPolicy;
        private readonly PeriodicPunctuationPolicy punctuationPolicy;
        private readonly OnCompletedPolicy onCompletedPolicy;
        private readonly TimelinePolicy timelinePolicy;
        private readonly bool delayed;
        private readonly QueryContainer container;

        public MonotonicIngressStreamable(
            IObservable<TPayload> observable,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            OnCompletedPolicy completedPolicy,
            TimelinePolicy timelinePolicy,
            QueryContainer container,
            string identifier)
            : base((Config.ForceRowBasedExecution || !typeof(TPayload).CanRepresentAsColumnar()
                ? StreamProperties<Empty, TPayload>.Default.ToRowBased()
                : StreamProperties<Empty, TPayload>.Default).ToConstantDuration(true, StreamEvent.InfinitySyncTime).SetQueryContainer(container))
        {
            Contract.Requires(observable != null);
            Contract.Requires(identifier != null);

            this.IngressSiteIdentifier = identifier;
            this.observable = observable;
            this.flushPolicy = flushPolicy;
            this.punctuationPolicy = punctuationPolicy;
            this.onCompletedPolicy = completedPolicy;
            this.timelinePolicy = timelinePolicy;
            this.container = container;
            this.delayed = container != null;

            if (this.delayed) container.RegisterIngressSite(identifier);
        }

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(this.observable != null);
        }

        public override IDisposable Subscribe(IStreamObserver<Empty, TPayload> observer)
        {
            Contract.EnsuresOnThrow<IngressException>(true);
            var subscription = this.timelinePolicy.timelineEnum == TimelineEnum.WallClock
                ? new MonotonicSubscriptionWallClock<TPayload>(this.observable, this.IngressSiteIdentifier,
                    this,
                    observer, this.flushPolicy, this.punctuationPolicy, this.onCompletedPolicy, this.timelinePolicy)
                : (IIngressStreamObserver)new MonotonicSubscriptionSequence<TPayload>(this.observable, this.IngressSiteIdentifier,
                    this,
                    observer, this.flushPolicy, this.punctuationPolicy, this.onCompletedPolicy, this.timelinePolicy);

            if (this.delayed)
            {
                this.container.RegisterIngressPipe(this.IngressSiteIdentifier, subscription);
                return subscription.DelayedDisposable;
            }
            else
            {
                subscription.Enable();
                return subscription;
            }
        }

        public string IngressSiteIdentifier { get; private set; }
    }
}