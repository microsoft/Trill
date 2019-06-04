// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class StreamMessageEgressPipe<TKey, TPayload> : EgressBoundary<TKey, TPayload, StreamMessage<TKey, TPayload>>
    {
        [Obsolete("Used only by serialization. Do not call directly.")]
        public StreamMessageEgressPipe() { }

        public StreamMessageEgressPipe(IObserver<StreamMessage<TKey, TPayload>> observer, QueryContainer container)
            : base(observer, container) { }

        public override void OnNext(StreamMessage<TKey, TPayload> batch) => this.observer.OnNext(batch);

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }
}
