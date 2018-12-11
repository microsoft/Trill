// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// WARNING:: this is not a 'true' connectable because it assumes that Connect happens only after all consumers
    /// have Subscribed (for internal use only, not safe for topical use)
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TPayload"></typeparam>
    internal sealed class ConnectableStreamable<TKey, TPayload> : ConnectableStreamableBase<TKey, TPayload>, IStreamObserver<TKey, TPayload>, IConnectableStreamable<TKey, TPayload>
    {
        private readonly IStreamable<TKey, TPayload> source;

        public ConnectableStreamable(IStreamable<TKey, TPayload> source)
            : base(source.Properties)
            => this.source = source;

        public override IDisposable Connect() => this.source.Subscribe(this);

        public override void ProduceQueryPlan(PlanNode previous)
        {
            var node = new MulticastPlanNode(previous, this, typeof(TKey), typeof(TPayload));
            this.produceQueryPlan(node);
        }
    }
}
