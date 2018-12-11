// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;

namespace Microsoft.StreamProcessing
{
    internal sealed class BinaryMulticastStreamable<TKey, TSourceLeft, TSourceRight, TResult> : Streamable<TKey, TResult>
    {
        private readonly IStreamable<TKey, TSourceLeft> sourceLeft;
        private readonly IStreamable<TKey, TSourceRight> sourceRight;
        private readonly Func<IStreamable<TKey, TSourceLeft>, IStreamable<TKey, TSourceRight>, IStreamable<TKey, TResult>> selector;

        public BinaryMulticastStreamable(IStreamable<TKey, TSourceLeft> sourceLeft, IStreamable<TKey, TSourceRight> sourceRight,
            Func<IStreamable<TKey, TSourceLeft>, IStreamable<TKey, TSourceRight>, IStreamable<TKey, TResult>> selector)
            : base(sourceLeft.Properties.Derive(sourceRight.Properties, selector))
        {
            Contract.Requires(sourceLeft != null);

            this.sourceLeft = sourceLeft;
            this.sourceRight = sourceRight;
            this.selector = selector;
        }

        public override IDisposable Subscribe(IStreamObserver<TKey, TResult> observer)
        {
            var connectableInputLeft = new ConnectableStreamable<TKey, TSourceLeft>(this.sourceLeft);
            var connectableInputRight = new ConnectableStreamable<TKey, TSourceRight>(this.sourceRight);

            var output = this.selector(connectableInputLeft, connectableInputRight);
            IDisposable d1 = output.Subscribe(observer);
            IDisposable d2 = connectableInputLeft.Connect();
            IDisposable d3 = connectableInputRight.Connect();
            return Utility.CreateDisposable(d1, d2, d3);
        }
    }
}
