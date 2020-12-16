// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class PartitionedSessionWindowPipe<TKey, TPayload, TPartitionKey> : UnaryPipe<TKey, TPayload, TPayload>
    {
        private readonly MemoryPool<TKey, TPayload> pool;
        private readonly string errorMessages;
        private readonly Func<TKey, TPartitionKey> getPartitionKey = GetPartitionExtractor<TPartitionKey, TKey>();

        [SchemaSerialization]
        private readonly long sessionDuration;

        [DataMember]
        private StreamMessage<TKey, TPayload> output;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public PartitionedSessionWindowPipe() { }

        public PartitionedSessionWindowPipe(IStreamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer, long sessionDuration)
            : base(stream, observer)
        {
            this.sessionDuration = sessionDuration;
            this.pool = MemoryManager.GetMemoryPool<TKey, TPayload>(stream.Properties.IsColumnar);
            this.errorMessages = stream.ErrorMessages;
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(
                    new SessionWindowPlanNode(
                        previous,
                        pipe: this,
                        keyType: typeof(TKey),
                        payloadType: typeof(TPayload),
                        sessionDuration: this.sessionDuration,
                        isGenerated: false,
                        errorMessages: this.errorMessages));

        public override unsafe void OnNext(StreamMessage<TKey, TPayload> batch)
        {
            throw new NotImplementedException();
        }

        protected override void FlushContents()
        {
            if (this.output.Count == 0) return;
            this.output.Seal();
            this.Observer.OnNext(this.output);
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        public override int CurrentlyBufferedOutputCount => this.output.Count;

        public override int CurrentlyBufferedInputCount
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        protected override void UpdatePointers()
        {
            throw new NotImplementedException();
        }

        protected override void DisposeState()
        {
            this.output.Free();
            throw new NotImplementedException();
        }
    }
}