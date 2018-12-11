// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class StreamMessageIngressSubscription<TPayload> : ObserverSubscriptionBase<StreamMessage<Empty, TPayload>, TPayload, TPayload>
    {
        public StreamMessageIngressSubscription() { }

        public StreamMessageIngressSubscription(
            IObservable<StreamMessage<Empty, TPayload>> observable,
            string identifier,
            Streamable<Empty, TPayload> stream,
            IStreamObserver<Empty, TPayload> observer)
            : base(
                observable,
                identifier,
                stream,
                observer,
                DisorderPolicy.Drop(),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.None(),
                OnCompletedPolicy.None,
                null)
        { }

        public override void OnNext(StreamMessage<Empty, TPayload> value)
        {
            Contract.Requires(value.Count > 0, "Input message should either be a control message or have Count greater than 0");
            this.Observer.OnNext(value);
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new IngressPlanNode(this, typeof(Empty), typeof(TPayload), false, null));

        protected override void OnCompleted(long punctuationTime) => throw new InvalidOperationException();
    }
}
