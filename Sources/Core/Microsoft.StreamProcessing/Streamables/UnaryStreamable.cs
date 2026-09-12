// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;

namespace Microsoft.StreamProcessing
{
    internal abstract class UnaryStreamable<TKey, TSource, TResult> : Streamable<TKey, TResult>
    {
        public IStreamable<TKey, TSource> Source;

        protected UnaryStreamable(IStreamable<TKey, TSource> source, StreamProperties<TKey, TResult> properties)
            : base(properties)
        {
            ArgumentNullException.ThrowIfNull(source);
            this.Source = source;
        }

        public override IDisposable Subscribe(IStreamObserver<TKey, TResult> observer)
        {
            var pipe = this.CreatePipe(observer);
            return this.Source.Subscribe(pipe);
        }

        internal abstract IStreamObserver<TKey, TSource> CreatePipe(IStreamObserver<TKey, TResult> observer);

        protected void Initialize()
        {
            if (this.Source.Properties.IsColumnar && !this.CanGenerateColumnar())
            {
                this.properties = this.properties.ToRowBased();
                this.Source = this.Source.ColumnToRow();
            }
        }

        protected abstract bool CanGenerateColumnar();
    }
}