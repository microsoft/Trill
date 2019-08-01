// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************

using System.Diagnostics.Contracts;

namespace Microsoft.StreamProcessing
{
    internal sealed class TraceStreamable<TKey, TPayload> : UnaryStreamable<TKey, TPayload, TPayload>
    {
        public TraceStreamable(IStreamable<TKey, TPayload> source, string traceId)
            : base(source, source.Properties)
        {
            Contract.Requires(source != null);
            this.TraceId = traceId;
            Initialize();
        }

        public string TraceId { get; }

        internal override IStreamObserver<TKey, TPayload> CreatePipe(IStreamObserver<TKey, TPayload> observer)
            => new TracePipe<TKey, TPayload>(this, observer);

        protected override bool CanGenerateColumnar() => true;

        public override string ToString() => $"Trace({this.TraceId})";
    }
}