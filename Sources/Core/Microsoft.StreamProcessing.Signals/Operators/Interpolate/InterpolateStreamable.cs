// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Signal
{
    internal sealed class InterpolateStreamable<TKey, TSource, TResult> : UnaryStreamable<TKey, TSource, TResult>
    {
        public readonly long offset;
        public readonly long period;
        public readonly InterpolationPolicy<TResult> interpolationPolicy;
        public readonly Expression<Func<long, TSource, TResult>> signalFunction;
        public readonly IEqualityComparerExpression<TSource> sourcePayloadEqualityComparer;

        public InterpolateStreamable(ContinuousSignal<TKey, TSource, TResult> source, long period, long offset, InterpolationPolicy<TResult> policy)
            : base(source.Stream, source.Stream.Properties.Interpolate(period, offset, policy))
        {
            Contract.Requires(source != null);
            Contract.Requires(period > 0);
            Contract.Requires(policy != null);

            this.offset = offset;
            this.period = period;
            this.interpolationPolicy = policy;
            this.signalFunction = source.Function;
            this.sourcePayloadEqualityComparer = source.Stream.Properties.PayloadEqualityComparer;

            Initialize();
        }

        internal override IStreamObserver<TKey, TSource> CreatePipe(IStreamObserver<TKey, TResult> observer)
            => new InterpolatePipe<TKey, TSource, TResult>(this, observer);

        // TODO: CODEGEN
        protected override bool CanGenerateColumnar() => false;
    }
}
