// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;

namespace Microsoft.StreamProcessing
{
    internal sealed class MulticastStreamable<TKey, TSource, TResult> : Streamable<TKey, TResult>
    {
        private readonly IStreamable<TKey, TSource> source;
        private readonly Func<IStreamable<TKey, TSource>, IStreamable<TKey, TResult>> selector;

        public MulticastStreamable(IStreamable<TKey, TSource> source, Func<IStreamable<TKey, TSource>, IStreamable<TKey, TResult>> selector)
            : base(source.Properties.Derive(selector))
        {
            Contract.Requires(source != null);
            Contract.Requires(selector != null);

            this.source = source;
            this.selector = selector;
        }

        public override IDisposable Subscribe(IStreamObserver<TKey, TResult> observer)
        {
            var connectableInput = new ConnectableStreamable<TKey, TSource>(this.source);
            var output = this.selector(connectableInput);
            IDisposable d1 = output.Subscribe(observer);
            IDisposable d2 = connectableInput.Connect();
            return Utility.CreateDisposable(d1, d2);
        }
    }

    internal sealed class PassthroughStreamable<TKey, TSource> : Streamable<TKey, TSource>
    {
        private readonly IStreamable<TKey, TSource> source;

        public PassthroughStreamable(IStreamable<TKey, TSource> source)
            : base(source.Properties)
            => this.source = source;

        public override IDisposable Subscribe(IStreamObserver<TKey, TSource> observer) => this.source.Subscribe(observer);
    }
}
