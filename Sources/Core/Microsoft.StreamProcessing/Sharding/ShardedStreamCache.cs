// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Threading;

namespace Microsoft.StreamProcessing.Sharding
{
    /// <summary>
    /// Represents a node in the query graph where data is cached
    /// </summary>
    /// <typeparam name="TKey">Grouping key type for data in the query</typeparam>
    /// <typeparam name="TPayload">Event payload type for data in the query</typeparam>
    public sealed class ShardedStreamCache<TKey, TPayload> : ShardedStreamable<TKey, TPayload>, IDisposable
    {
        /// <summary>
        /// The underlying array of cached streams corresponding to the array of cached streamables in the original sharded streamable
        /// </summary>
        public StreamCache<TKey, TPayload>[] caches;

        /// <summary>
        /// Create a cached sharded streamable from an existing sharded streamable
        /// </summary>
        /// <param name="source">The sharded streamable to cache</param>
        public ShardedStreamCache(IShardedStreamable<TKey, TPayload> source)
        {
            var shardSource = (ShardedStreamable<TKey, TPayload>)source;

            this.streamables = new IStreamable<TKey, TPayload>[shardSource.Streamables.Length];
            this.caches = new StreamCache<TKey, TPayload>[shardSource.Streamables.Length];

            var observers = new ShardedCacheObserver<TKey, TPayload>[shardSource.Streamables.Length];

            for (int i = 0; i < shardSource.Streamables.Length; i++)
            {
                this.caches[i] = new StreamCache<TKey, TPayload>();
                observers[i] = new ShardedCacheObserver<TKey, TPayload>(this.caches[i], shardSource.Streamables[i].Properties);
                this.streamables[i] = this.caches[i];

                shardSource.Streamables[i].ToStreamMessageObservable().Subscribe(observers[i]);
            }

            for (int i = 0; i < shardSource.Streamables.Length; i++)
            {
                observers[i].Wait();
            }
            return;

        }

        /// <summary>
        /// Dispose of the cached shards
        /// </summary>
        public void Dispose()
        {
            for (int i = 0; i < this.caches.Length; i++)
            {
                this.caches[i].Dispose();
            }
        }
    }

    internal sealed class ShardedCacheObserver<TKey, TPayload> : IObserver<StreamMessage<TKey, TPayload>>, IDisposable
    {
        private readonly StreamCache<TKey, TPayload> cache;
        private readonly StreamProperties<TKey, TPayload> sourceProps;
        private Exception e = null;

        private readonly List<QueuedMessage<StreamMessage<TKey, TPayload>>> elements;
        private readonly AutoResetEvent done = new AutoResetEvent(false);

        public ShardedCacheObserver(StreamCache<TKey, TPayload> cache, StreamProperties<TKey, TPayload> sourceProps)
        {
            this.cache = cache;
            this.sourceProps = sourceProps;

            this.elements = new List<QueuedMessage<StreamMessage<TKey, TPayload>>>();
        }

        public void Dispose()
        {
            this.done.Set();
            this.done.Dispose();
            this.cache.Dispose();
        }

        public void OnCompleted()
        {
            this.elements.Add(new QueuedMessage<StreamMessage<TKey, TPayload>> { Kind = MessageKind.Completed });
            this.cache.Update(this.sourceProps, this.elements);
            this.done.Set();
        }

        public void OnError(Exception error)
        {
            for (int i = 0; i < this.elements.Count; i++)
            {
                var m = this.elements[i];
                if (m.Kind == MessageKind.DataBatch)
                    m.Message.Free();
            }

            this.e = error;
            this.done.Set();
        }

        public void OnNext(StreamMessage<TKey, TPayload> value)
            => this.elements.Add(new QueuedMessage<StreamMessage<TKey, TPayload>> { Kind = MessageKind.DataBatch, Message = value });

        public void Wait()
        {
            this.done.WaitOne();
            if (this.e != null) throw this.e;
        }
    }

    /// <summary>
    /// Static class for extension methods on sharded streamables
    /// </summary>
    public static class ShardedStreamableIO
    {
        /// <summary>
        /// Create a cache from a sharded streamable
        /// </summary>
        /// <typeparam name="TKey">Grouping key type for data in the query</typeparam>
        /// <typeparam name="TPayload">Event payload type for data in the query</typeparam>
        /// <param name="source">The sharded streamable to cache</param>
        /// <returns>A cached sharded streamable</returns>
        public static ShardedStreamCache<TKey, TPayload> Cache<TKey, TPayload>(this IShardedStreamable<TKey, TPayload> source)
            => new ShardedStreamCache<TKey, TPayload>(source);
    }
}