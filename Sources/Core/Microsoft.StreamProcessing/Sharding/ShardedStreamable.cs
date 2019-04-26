// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Sharding
{
    /// <summary>
    /// Concrete instantiation of the IShardedStreamable class
    /// </summary>
    /// <typeparam name="TKey">Grouping key type for data in the query</typeparam>
    /// <typeparam name="TPayload">Event payload type for data in the query</typeparam>
    public class ShardedStreamable<TKey, TPayload> : IShardedStreamable<TKey, TPayload>
    {
        /// <summary>
        /// The base array of streams
        /// </summary>
        protected IStreamable<TKey, TPayload>[] streamables;

        /// <summary>
        /// Default constructor - do not use directly
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public ShardedStreamable() { }

        /// <summary>
        /// Constructor to create a new sharded streamable instance from an existing set of streamables
        /// </summary>
        /// <param name="streamables"></param>
        public ShardedStreamable(IStreamable<TKey, TPayload>[] streamables) => this.streamables = streamables;

        /// <summary>
        /// Constructor to create a new sharded streamable instance from an existing set of sharded streamables
        /// </summary>
        /// <param name="shardedStreamables"></param>
        public ShardedStreamable(IShardedStreamable<TKey, TPayload>[] shardedStreamables)
        {
            var streamables = new List<IStreamable<TKey, TPayload>>();

            for (int i = 0; i < shardedStreamables.Length; i++)
            {
                var ss = (ShardedStreamable<TKey, TPayload>)shardedStreamables[i];

                for (int j = 0; j < ss.Streamables.Length; j++)
                    streamables.Add(ss.Streamables[j]);
            }

            this.streamables = streamables.ToArray();
        }

        /// <summary>
        /// Chained subscribe call to connect an array of observers to the underlying streamables
        /// </summary>
        /// <param name="observers">The array of observers seeking to receive data from the streamables</param>
        /// <returns>The array of disposables that are created by the corresponding subscriptions</returns>
        public IDisposable[] Subscribe(IStreamObserver<TKey, TPayload>[] observers)
        {
            var ret = new IDisposable[observers.Length];
            for (int i = 0; i < observers.Length; i++)
                ret[i] = this.streamables[i].Subscribe(observers[i]);
            return ret;
        }

        /// <summary>
        /// Execute a unary query on all shards
        /// </summary>
        /// <typeparam name="TOutput">The type of event payload in the output</typeparam>
        /// <param name="query">The query to evaluate</param>
        /// <returns>A new sharded streamable comprised of the output of the query</returns>
        public IShardedStreamable<TKey, TOutput> Query<TOutput>(Expression<Func<IStreamable<TKey, TPayload>, IStreamable<TKey, TOutput>>> query)
        {
            var result = new IStreamable<TKey, TOutput>[this.streamables.Length];
            var queryC = query.Compile();

            for (int i = 0; i < this.streamables.Length; i++)
                result[i] = queryC(this.streamables[i]);

            return new ShardedStreamable<TKey, TOutput>(result);
        }

        /// <summary>
        /// Execute a binary query on all shards
        /// </summary>
        /// <typeparam name="TPayload2">Event payload type for data from the second input</typeparam>
        /// <typeparam name="TOutput">The type of event payload in the output</typeparam>
        /// <param name="input2">The second input to the binary query</param>
        /// <param name="query">The query to evaluate</param>
        /// <returns>A new sharded streamable comprised of the output of the query</returns>
        public IShardedStreamable<TKey, TOutput> Query<TPayload2, TOutput>(
                IShardedStreamable<TKey, TPayload2> input2,
                Expression<Func<IStreamable<TKey, TPayload>, IStreamable<TKey, TPayload2>, IStreamable<TKey, TOutput>>> query)
        {
            var result = new IStreamable<TKey, TOutput>[this.streamables.Length];
            var queryC = query.Compile();

            for (int i = 0; i < this.streamables.Length; i++)
                result[i] = queryC(this.streamables[i], ((ShardedStreamable<TKey, TPayload2>)input2).streamables[i]);

            return new ShardedStreamable<TKey, TOutput>(result);
        }

        /// <summary>
        /// Reshard the sharded streamable
        /// </summary>
        /// <param name="newLocation">Assign an optional new location descriptor</param>
        /// <returns>A new sharded streamable with the new key type and values</returns>
        public IShardedStreamable<TKey, TPayload> ReShard(ILocationDescriptor newLocation = null)
        {
            int newShardArity = (newLocation != null && (int)newLocation.GetLocation() > 0) ? (int)newLocation.GetLocation() : this.Streamables.Length;

            var reshardResults = new SprayGroupImportStreamable<TKey, TPayload>[this.Streamables.Length];
            for (int i = 0; i < this.Streamables.Length; i++)
                reshardResults[i] = new SprayGroupImportStreamable<TKey, TPayload>(this.Streamables[i], newShardArity, false);

            var unionResults = new IStreamable<TKey, TPayload>[newShardArity];
            for (int i = 0; i < newShardArity; i++)
                unionResults[i] = new MultiUnionStreamable<TKey, TPayload>(reshardResults);

            return new ShardedStreamable<TKey, TPayload>(unionResults);
        }

        /// <summary>
        /// Broadcast operation on a sharded streamable
        /// </summary>
        /// <param name="newLocation">Assign an optional new location descriptor</param>
        /// <returns>A new sharded streamable post-broadcast</returns>
        public IShardedStreamable<TKey, TPayload> Broadcast(ILocationDescriptor newLocation = null)
        {
            int newShardArity = (newLocation != null && (int)newLocation.GetLocation() > 0) ? (int)newLocation.GetLocation() : this.Streamables.Length;

            var broadcastResults = new SprayGroupImportStreamable<TKey, TPayload>[this.Streamables.Length];
            for (int i = 0; i < this.Streamables.Length; i++)
                broadcastResults[i] = new SprayGroupImportStreamable<TKey, TPayload>(this.Streamables[i], newShardArity, true);

            var unionResults = new IStreamable<TKey, TPayload>[newShardArity];
            for (int i = 0; i < newShardArity; i++)
                unionResults[i] = new MultiUnionStreamable<TKey, TPayload>(broadcastResults);

            return new ShardedStreamable<TKey, TPayload>(unionResults);
        }

        /// <summary>
        /// Calculate new payloads based on old payloads and sharding keys
        /// </summary>
        /// <typeparam name="TNewPayload">The type of the new payload</typeparam>
        /// <param name="selector">Selector function for new payloads based on the old payloads and the sharding key</param>
        /// <returns>A new sharded streamable with the same key and new payload type</returns>
        public IShardedStreamable<TKey, TNewPayload> SelectKey<TNewPayload>(Expression<Func<TKey, TPayload, TNewPayload>> selector)
        {
            var result = new IStreamable<TKey, TNewPayload>[this.streamables.Length];

            for (int i = 0; i < this.streamables.Length; i++)
                result[i] = new SelectStreamable<TKey, TPayload, TNewPayload>(this.streamables[i], selector, false, true);

            return new ShardedStreamable<TKey, TNewPayload>(result);
        }

        /// <summary>
        /// Re-key the data stream shards
        /// </summary>
        /// <typeparam name="TNewKey">The type of the new key</typeparam>
        /// <param name="keySelector">Selector function for assigning new keys based on event payload</param>
        /// <returns>A new sharded streamable with the new key type and values</returns>
        public IShardedStreamable<TNewKey, TPayload> ReKey<TNewKey>(Expression<Func<TPayload, TNewKey>> keySelector)
        {
            var result = new IStreamable<TNewKey, TPayload>[this.streamables.Length];

            for (int i = 0; i < this.streamables.Length; i++)
                result[i] = new GroupStreamable<TKey, TPayload, TNewKey>(this.streamables[i], keySelector);

            return new ShardedStreamable<TNewKey, TPayload>(result);
        }

        /// <summary>
        /// Re-distribute the data across shards
        /// </summary>
        /// <param name="newLocation">Assign an optional new location descriptor</param>
        /// <returns>A new sharded streamable with the new key type and values</returns>
        public IShardedStreamable<TKey, TPayload> ReDistribute(ILocationDescriptor newLocation = null)
        {
            int newShardArity = (newLocation != null && (int)newLocation.GetLocation() > 0) ? (int)newLocation.GetLocation() : this.Streamables.Length;

            var shuffleL1Results = new ShuffleSameKeyStreamable<TKey, TPayload, TKey>[this.Streamables.Length];
            for (int i = 0; i < this.Streamables.Length; i++)
                shuffleL1Results[i] = new ShuffleSameKeyStreamable<TKey, TPayload, TKey>(this.Streamables[i], newShardArity, i);

            var shuffleResults = new IStreamable<TKey, TPayload>[newShardArity];
            for (int i = 0; i < newShardArity; i++)
                shuffleResults[i] = new MultiUnionStreamable<TKey, TPayload>(shuffleL1Results);
            return new ShardedStreamable<TKey, TPayload>(shuffleResults);
        }

        /// <summary>
        /// Multicast operation on a sharded streamable
        /// </summary>
        /// <param name="destination">A destination desciption for the multicast operation</param>
        /// <param name="newLocation">Assign an optional new location descriptor</param>
        /// <returns>A new sharded streamable post-multicast</returns>
        public IShardedStreamable<TKey, TPayload> Multicast(IDestinationDescriptor destination, ILocationDescriptor newLocation = null)
        {
            int newShardArity = (newLocation != null && (int)newLocation.GetLocation() > 0) ? (int)newLocation.GetLocation() : this.Streamables.Length;

            var destinationSelector = (Expression<Func<TKey, int, TPayload, int[]>>)destination.GetDestination();

            var shufflecastL1Results = new ShufflecastStreamable<TPayload, TKey>[this.Streamables.Length];
            for (int i = 0; i < this.Streamables.Length; i++)
                shufflecastL1Results[i] = new ShufflecastStreamable<TPayload, TKey>(this.Streamables[i], newShardArity, destinationSelector);

            var shuffleResults = new IStreamable<TKey, TPayload>[newShardArity];
            for (int i = 0; i < newShardArity; i++)
                shuffleResults[i] = new MultiUnionStreamable<TKey, TPayload>(shufflecastL1Results);
            return new ShardedStreamable<TKey, TPayload>(shuffleResults);
        }

        /// <summary>
        /// Break up a sharded streamable into an array of sharded streamables, each consisting of only one shard
        /// </summary>
        /// <returns>Array of sharded streamables</returns>
        public IShardedStreamable<TKey, TPayload>[] Split()
        {
            var result = new IShardedStreamable<TKey, TPayload>[this.Streamables.Length];

            for (int i = 0; i < result.Length; i++)
            {
                var singletonArray = new IStreamable<TKey, TPayload>[1];
                singletonArray[0] = this.Streamables[i];
                result[i] = new ShardedStreamable<TKey, TPayload>(singletonArray);
            }
            return result;
        }

        /// <summary>
        /// Currently for internal use only - do not use
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public IStreamable<TKey, TPayload>[] Streamables => this.streamables;
    }
}
