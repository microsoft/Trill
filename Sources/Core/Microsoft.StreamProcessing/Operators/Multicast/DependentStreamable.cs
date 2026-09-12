// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Threading;

namespace Microsoft.StreamProcessing
{
    internal sealed class NWayMulticast<TKey, TSource>
    {
        private readonly Lock subscriptionLock = new();
        private ConnectableStreamable<TKey, TSource> connectableStream;
        private readonly IStreamable<TKey, TSource> source;
        private readonly int outputCount;
        private HashSet<int> toSubscribe;
        private DisposableManager crew;

        private NWayMulticast(IStreamable<TKey, TSource> source, int outputCount)
        {
            ArgumentNullException.ThrowIfNull(source);
            Contract.Requires(outputCount > 0);

            this.source = source;
            this.connectableStream = new ConnectableStreamable<TKey, TSource>(source);
            this.outputCount = outputCount;
            this.crew = new DisposableManager(outputCount);
        }

        public static IStreamable<TKey, TSource>[] GenerateStreamableArray(
            IStreamable<TKey, TSource> source, int outputCount)
            => new NWayMulticast<TKey, TSource>(source, outputCount).GenerateStreamableArray();

        private IStreamable<TKey, TSource>[] GenerateStreamableArray()
        {
            if (this.toSubscribe != null)
            {
                throw new InvalidOperationException("Cannot generate a streamable array more than once.");
            }

            this.toSubscribe = [];

            var output = new IStreamable<TKey, TSource>[this.outputCount];
            for (int i = 0; i < this.outputCount; i++)
            {
                output[i] = new DependentStreamable<TKey, TSource>(this.connectableStream, this, i);
            }
            return output;
        }

        private IDisposable Subscribe(IStreamObserver<TKey, TSource> observer, int index)
        {
            using Lock.Scope _ = this.subscriptionLock.EnterScope();
            IDisposable child;
            if (this.toSubscribe.Add(index))
            {
                child = new ChildDisposable(this.connectableStream.Subscribe(observer), this.crew, index);
            }
            else
            {
                throw new InvalidOperationException("Cannot subscribe to the same child streamable more than once.");
            }

            if (this.toSubscribe.Count == this.outputCount)
            {
                this.crew.SetListDisposable(this.connectableStream.Connect());
                this.crew = new(this.outputCount);
                this.connectableStream = new(this.source);
                this.toSubscribe.Clear();
            }
            return child;
        }

        private sealed class DependentStreamable<TKeyInner, TSourceInner> : Streamable<TKeyInner, TSourceInner>
        {
            private readonly NWayMulticast<TKeyInner, TSourceInner> leader;
            private readonly int index;

            public DependentStreamable(
                ConnectableStreamable<TKeyInner, TSourceInner> source,
                NWayMulticast<TKeyInner, TSourceInner> leader,
                int index)
                : base(source.Properties)
            {
                ArgumentNullException.ThrowIfNull(source);
                ArgumentNullException.ThrowIfNull(leader);
                Contract.Requires(index >= 0);

                this.leader = leader;
                this.index = index;
            }

            public override IDisposable Subscribe(IStreamObserver<TKeyInner, TSourceInner> observer)
                => this.leader.Subscribe(observer, this.index);
        }

        private sealed class DisposableManager
        {
            private readonly Lock disposeLock = new();
            private IDisposable last;
            private readonly HashSet<int> toDispose;

            public DisposableManager(int count)
            {
                this.toDispose = [];
                for (int i = 0; i < count; i++)
                {
                    this.toDispose.Add(i);
                }
            }

            public void SetListDisposable(IDisposable last) => this.last = last;

            public void MarkAsDisposed(int index)
            {
                using Lock.Scope _ = this.disposeLock.EnterScope();
                this.toDispose.Remove(index);
                if (this.toDispose.Count == 0)
                {
                    this.last.Dispose();
                }
            }
        }

        private sealed class ChildDisposable(IDisposable inner, DisposableManager crew, int index) : IDisposable
        {
            public void Dispose()
            {
                inner.Dispose();
                crew.MarkAsDisposed(index);
            }
        }
    }
}
