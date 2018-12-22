// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Signal
{
    internal sealed class SampleStreamable<TKey, TSource, TResult> : UnaryStreamable<TKey, TSource, TResult>
    {
        public readonly long offset;
        public readonly long period;
        public readonly Expression<Func<long, TSource, TResult>> signalFunction;
        public readonly IEqualityComparerExpression<TSource> sourcePayloadEqualityComparer;

        public SampleStreamable(ContinuousSignal<TKey, TSource, TResult> source, long period, long offset)
            : base(source.Stream, source.Stream.Properties.Sample<TKey, TSource, TResult>(period, offset))
        {
            Contract.Requires(source != null);
            Contract.Requires(period > 0);

            this.offset = offset;
            this.period = period;
            this.signalFunction = source.Function;
            this.sourcePayloadEqualityComparer = source.Stream.Properties.PayloadEqualityComparer;

            Initialize();
        }

        internal override IStreamObserver<TKey, TSource> CreatePipe(IStreamObserver<TKey, TResult> observer)
            => new SamplePipe<TKey, TSource, TResult>(this, observer);

        // TODO: CODEGEN
        protected override bool CanGenerateColumnar() => false;
    }
}
