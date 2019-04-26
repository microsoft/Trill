// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;

namespace Microsoft.StreamProcessing
{
    internal sealed class SetPropertyStreamable<TKey, TPayload> : Streamable<TKey, TPayload>
    {
        private readonly IStreamable<TKey, TPayload> Source;

        public SetPropertyStreamable(IStreamable<TKey, TPayload> source, Func<StreamProperties<TKey, TPayload>, StreamProperties<TKey, TPayload>> propertySetter)
            : base(propertySetter(source.Properties))
        {
            Contract.Requires(source != null);

            this.Source = source;
        }

        public override sealed IDisposable Subscribe(IStreamObserver<TKey, TPayload> observer) => this.Source.Subscribe(observer);
    }
}