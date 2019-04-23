// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;

namespace Microsoft.StreamProcessing
{
    internal sealed class NWayMulticast<TKey, TSource>
    {
        private readonly object subscriptionLock = new object();
        private ConnectableStreamable<TKey, TSource> connectableStream;
        private readonly IStreamable<TKey, TSource> source;
        private readonly int outputCount;
        private HashSet<int> toSubscribe;
        private DisposableManager crew;

        private NWayMulticast(IStreamable<TKey, TSource> source, int outputCount)
        {
            Contract.Requires(source != null);
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

            this.toSubscribe = new HashSet<int>();

            var output = new IStreamable<TKey, TSource>[this.outputCount];
            for (int i = 0; i < this.outputCount; i++)
            {
                output[i] = new DependentStreamable<TKey, TSource>(this.connectableStream, this, i);
            }
            return output;
        }

        private IDisposable Subscribe(IStreamObserver<TKey, TSource> observer, int index)
        {
            IDisposable child;
            lock (this.subscriptionLock)
            {
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
                    this.crew = new DisposableManager(this.outputCount);
                    this.connectableStream = new ConnectableStreamable<TKey, TSource>(this.source);
                    this.toSubscribe.Clear();
                }
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
                Contract.Requires(source != null);
                Contract.Requires(leader != null);
                Contract.Requires(index >= 0);

                this.leader = leader;
                this.index = index;
            }

            public override IDisposable Subscribe(IStreamObserver<TKeyInner, TSourceInner> observer)
                => this.leader.Subscribe(observer, this.index);
        }

        private sealed class DisposableManager
        {
            private readonly object disposeLock = new object();
            private IDisposable last;
            private readonly HashSet<int> toDispose;

            public DisposableManager(int count)
            {
                this.toDispose = new HashSet<int>();
                for (int i = 0; i < count; i++)
                {
                    this.toDispose.Add(i);
                }
            }

            public void SetListDisposable(IDisposable last) => this.last = last;

            public void MarkAsDisposed(int index)
            {
                lock (this.disposeLock)
                {
                    this.toDispose.Remove(index);
                    if (this.toDispose.Count == 0)
                    {
                        this.last.Dispose();
                    }
                }
            }
        }

        private sealed class ChildDisposable : IDisposable
        {
            private readonly IDisposable inner;
            private readonly DisposableManager crew;
            private readonly int index;

            public ChildDisposable(IDisposable inner, DisposableManager crew, int index)
            {
                this.inner = inner;
                this.crew = crew;
                this.index = index;
            }

            public void Dispose()
            {
                this.inner.Dispose();
                this.crew.MarkAsDisposed(this.index);
            }
        }
    }
}
