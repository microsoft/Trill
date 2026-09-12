// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Threading;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// An enumerable object whose contents are subject to changes as additional data becomes available from a stream.
    /// </summary>
    /// <typeparam name="TPayload">The type of the streamable payload</typeparam>
    public sealed class EvolvingStateEnumerable<TPayload> : IEnumerable<TPayload>, IObserver<IEnumerable<ChangeListEvent<TPayload>>>, IDisposable
    {
        private readonly IStreamable<Empty, TPayload> source;
        private readonly QueryContainer container;
        private readonly string identifier;
        private readonly ConcurrentDictionary<TPayload, int> data = new();
        private volatile bool isComplete;
        private readonly Lock sentinel = new();
        private readonly IDisposable disposable;

        /// <summary>
        /// Constructor for this enumerable class, associating this enumerable with a data source from which the enumerable's contents are to be populated or retracted.
        /// </summary>
        /// <param name="source">The stream from which this enumerable will gather its state deltas.</param>
        /// <param name="container">The query container to which this enumerable will be registered as an egress point.</param>
        /// <param name="identifier">A string that uniquely identifies this enumerable among the egress points in the query container.</param>
        public EvolvingStateEnumerable(
            IStreamable<Empty, TPayload> source,
            QueryContainer container,
            string identifier)
        {
            ArgumentNullException.ThrowIfNull(source);

            this.source = source;
            this.container = container;
            this.identifier = identifier;
            this.container?.RegisterEgressSite(this.identifier);

            var pipe = new AtemporalEnumerableEgressPipe<TPayload>(this, this.container);
            this.container?.RegisterEgressPipe(this.identifier, pipe);
            this.disposable = this.source.Subscribe(pipe);
        }

        /// <summary>
        /// A flag stating whether the input to this enumerable has ceased producing updates and thus the enumerable is in its final state.
        /// </summary>
        public bool Completed => this.isComplete;

        IEnumerator<TPayload> IEnumerable<TPayload>.GetEnumerator()
        {
            this.sentinel.Enter();
            return new InnerEnumerator(
                this.data.SelectMany(o => Enumerable.Range(0, o.Value).Select(i => o.Key)).GetEnumerator(),
                this.sentinel.Exit);
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            this.sentinel.Enter();
            return new InnerEnumerator(
                this.data.SelectMany(o => Enumerable.Range(0, o.Value).Select(i => o.Key)).GetEnumerator(),
                this.sentinel.Exit);
        }

        void IObserver<IEnumerable<ChangeListEvent<TPayload>>>.OnCompleted() => this.isComplete = true;

        void IObserver<IEnumerable<ChangeListEvent<TPayload>>>.OnError(Exception error) => throw error;

        void IObserver<IEnumerable<ChangeListEvent<TPayload>>>.OnNext(IEnumerable<ChangeListEvent<TPayload>> batch)
        {
            using Lock.Scope _ = this.sentinel.EnterScope();
            int count;
            foreach (var value in batch)
            {
                switch (value.EventKind)
                {
                    case ChangeListEventKind.Insert:
                        if (this.data.TryGetValue(value.Payload, out count))
                            this.data.TryUpdate(value.Payload, count + 1, count);
                        else
                            this.data.AddOrUpdate(value.Payload, 1, (p, c) => c + 1);
                        break;

                    case ChangeListEventKind.Delete:
                        if (!this.data.TryGetValue(value.Payload, out count)) throw new InvalidOperationException("Should not be able to delete a value that does not exist");
                        else
                            this.data.TryUpdate(value.Payload, count - 1, count);
                        break;

                    default:
                        throw new InvalidOperationException("Switch statement should be exhaustive");
                }
            }
        }

        /// <summary>
        /// Dispose the enumerable object by disposing its underlying state.
        /// </summary>
        public void Dispose() => this.disposable.Dispose();

        private sealed class InnerEnumerator(IEnumerator<TPayload> baseEnumerator, Action exitLock)
            : IEnumerator<TPayload>
        {
            void IDisposable.Dispose()
            {
                baseEnumerator.Dispose();
                exitLock();
            }

            TPayload IEnumerator<TPayload>.Current => baseEnumerator.Current;
            object System.Collections.IEnumerator.Current => baseEnumerator.Current;
            bool System.Collections.IEnumerator.MoveNext() => baseEnumerator.MoveNext();
            void System.Collections.IEnumerator.Reset() => baseEnumerator.Reset();
        }
    }
}