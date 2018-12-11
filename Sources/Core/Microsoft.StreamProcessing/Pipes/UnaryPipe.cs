// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.ComponentModel;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class UnaryPipe<TKey, TSource, TResult> : Pipe<TKey, TResult>,
        IStreamObserver<TKey, TSource>
    {
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public abstract void OnNext(StreamMessage<TKey, TSource> batch);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected UnaryPipe() { }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected UnaryPipe(IStreamable<TKey, TResult> stream, IStreamObserver<TKey, TResult> observer)
            : base(stream, observer)
        { }
    }
}