// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Diagnostics.Contracts;

namespace Microsoft.StreamProcessing.Signal
{
    internal sealed class FilterIIRStreamable<TKey, TPayload> : UnaryStreamable<TKey, TPayload, TPayload>
    {
        internal readonly double factor;
        internal readonly long period;

        public FilterIIRStreamable(UniformSignal<TKey, TPayload> source, double factor)
            : base(source.Stream, source.Properties.FilterIIR())
        {
            Contract.Requires(source != null);
            Contract.Requires(factor >= 0 && factor <= 1);

            this.factor = factor;
            this.period = source.Properties.Period;

            Initialize();
        }

        internal override IStreamObserver<TKey, TPayload> CreatePipe(IStreamObserver<TKey, TPayload> observer)
            => new FilterIIRPipe<TKey, TPayload>(this, observer);

        // TODO: CODEGEN
        protected override bool CanGenerateColumnar() => false;
    }
}