// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Diagnostics.Contracts;
using Microsoft.StreamProcessing.Signal.UDO;

namespace Microsoft.StreamProcessing.Signal
{
    internal sealed class DigitalFilterStreamable<TKey, TPayload> : UnaryStreamable<TKey, TPayload, TPayload>
    {
        internal readonly long period;
        internal readonly long offset;
        internal readonly IDigitalFilter<TPayload> filter;

        public DigitalFilterStreamable(UniformSignal<TKey, TPayload> source, IDigitalFilter<TPayload> filter)
            : base(source.Stream, source.Properties.DigitalFilter())
        {
            Contract.Requires(source != null);

            this.period = source.Properties.Period;
            this.offset = source.Properties.Offset;
            this.filter = filter;

            Initialize();
        }

        internal override IStreamObserver<TKey, TPayload> CreatePipe(IStreamObserver<TKey, TPayload> observer)
            => new DigitalFilterPipe<TKey, TPayload>(this, observer);

        // TODO: CODEGEN
        protected override bool CanGenerateColumnar() => false;
    }
}