// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.IO;
using System.Linq.Expressions;
using Microsoft.StreamProcessing.Serializer;

namespace Microsoft.StreamProcessing.Sharding
{
    /// <summary>
    /// Interface abstraction for a streamable that is physically sharded
    /// </summary>
    /// <typeparam name="TKey">Grouping key type for data in the query</typeparam>
    /// <typeparam name="TPayload">Event payload type for data in the query</typeparam>
    public interface IShardedStreamable<TKey, TPayload>
    {
        /// <summary>
        /// Execute a unary query on all shards
        /// </summary>
        /// <typeparam name="TOutput">The type of event payload in the output</typeparam>
        /// <param name="query">The query to evaluate</param>
        /// <returns>A new sharded streamable comprised of the output of the query</returns>
        IShardedStreamable<TKey, TOutput> Query<TOutput>(Expression<Func<IStreamable<TKey, TPayload>, IStreamable<TKey, TOutput>>> query);

        /// <summary>
        /// Execute a binary query on all shards
        /// </summary>
        /// <typeparam name="TPayload2">Event payload type for data from the second input</typeparam>
        /// <typeparam name="TOutput">The type of event payload in the output</typeparam>
        /// <param name="input2">The second input to the binary query</param>
        /// <param name="query">The query to evaluate</param>
        /// <returns>A new sharded streamable comprised of the output of the query</returns>
        IShardedStreamable<TKey, TOutput> Query<TPayload2, TOutput>(
            IShardedStreamable<TKey, TPayload2> input2,
            Expression<Func<IStreamable<TKey, TPayload>, IStreamable<TKey, TPayload2>, IStreamable<TKey, TOutput>>> query);

        /// <summary>
        /// Re-key the data stream shards
        /// </summary>
        /// <typeparam name="TNewKey">The type of the new key</typeparam>
        /// <param name="keySelector">Selector function for assigning new keys based on event payload</param>
        /// <returns>A new sharded streamable with the new key type and values</returns>
        IShardedStreamable<TNewKey, TPayload> ReKey<TNewKey>(Expression<Func<TPayload, TNewKey>> keySelector);

        /// <summary>
        /// Re-distribute the data across shards
        /// </summary>
        /// <param name="newLocation">Assign an optional new location descriptor</param>
        /// <returns>A new sharded streamable with the new key type and values</returns>
        IShardedStreamable<TKey, TPayload> ReDistribute(ILocationDescriptor newLocation = null);

        /// <summary>
        /// Reshard the sharded streamable
        /// </summary>
        /// <param name="newLocation">Assign an optional new location descriptor</param>
        /// <returns>A new sharded streamable with the new key type and values</returns>
        IShardedStreamable<TKey, TPayload> ReShard(ILocationDescriptor newLocation = null);

        /// <summary>
        /// Broadcast operation on a sharded streamable
        /// </summary>
        /// <param name="newLocation">Assign an optional new location descriptor</param>
        /// <returns>A new sharded streamable post-broadcast</returns>
        IShardedStreamable<TKey, TPayload> Broadcast(ILocationDescriptor newLocation = null);

        /// <summary>
        /// Multicast operation on a sharded streamable
        /// </summary>
        /// <param name="destination">A destination desciption for the multicast operation</param>
        /// <param name="newLocation">Assign an optional new location descriptor</param>
        /// <returns>A new sharded streamable post-multicast</returns>
        IShardedStreamable<TKey, TPayload> Multicast(IDestinationDescriptor destination, ILocationDescriptor newLocation = null);

        /// <summary>
        /// Calculate new payloads based on old payloads and sharding keys
        /// </summary>
        /// <typeparam name="TNewPayload">The type of the new payload</typeparam>
        /// <param name="selector">Selector function for new payloads based on the old payloads and the sharding key</param>
        /// <returns>A new sharded streamable with the same key and new payload type</returns>
        IShardedStreamable<TKey, TNewPayload> SelectKey<TNewPayload>(Expression<Func<TKey, TPayload, TNewPayload>> selector);
    }

    /// <summary>
    /// Extension methods on IShardedStreamable interface
    /// </summary>
    public static class ShardedStreamableExtensions
    {
        /// <summary>
        /// Shuffle operation on a sharded streamable
        /// </summary>
        /// <typeparam name="TKey">Grouping/sharding key type for input data</typeparam>
        /// <typeparam name="TNewKey">Grouping/sharding key type for output data</typeparam>
        /// <typeparam name="TPayload">Payload type for data flowing through the shuffle operation</typeparam>
        /// <param name="streamable">Input sharded streamable for the shuffle operation</param>
        /// <param name="shuffleSelector">Selector function to determine new shard keys</param>
        /// <param name="newLocation">Assign an optional new location descriptor</param>
        /// <returns>A new sharded streamable post-shuffle</returns>
        public static IShardedStreamable<TNewKey, TPayload> Shuffle<TKey, TNewKey, TPayload>(
            this IShardedStreamable<TKey, TPayload> streamable,
            Expression<Func<TPayload, TNewKey>> shuffleSelector,
            ILocationDescriptor newLocation = null)
        {
            return streamable.ReKey(shuffleSelector).ReDistribute(newLocation);
        }

        /// <summary>
        /// Unshuffle operation on a sharded streamable (essentially, a unifying shuffle operation shuffling to a unity shard
        /// </summary>
        /// <typeparam name="TKey">Grouping/sharding key type for input data</typeparam>
        /// <typeparam name="TPayload">Payload type for data flowing through the shuffle operation</typeparam>
        /// <param name="streamable">Input sharded streamable for the shuffle operation</param>
        /// <returns>A new sharded streamable post-shuffle</returns>
        public static IShardedStreamable<Empty, TPayload> Unshuffle<TKey, TPayload>(this IShardedStreamable<TKey, TPayload> streamable)
        {
            return streamable.ReKey(e => Empty.Default);
        }

        /// <summary>
        /// Write stream properties to specified .NET stream
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="streamable"></param>
        /// <param name="stream"></param>
        public static void WritePropertiesToStream<TKey, TPayload>(this IShardedStreamable<TKey, TPayload> streamable, Stream stream)
        {
            var properties = StreamProperties<TKey, TPayload>.Default;
            var shardedStreamable = (ShardedStreamable<TKey, TPayload>)streamable;
            if (shardedStreamable != null)
            {
                if (shardedStreamable.Streamables.Length > 0)
                {
                    properties = shardedStreamable.Streamables[0].Properties;
                }
            }
            var propSer = StreamableSerializer.Create<SerializedProperties>();
            propSer.Serialize(stream, SerializedProperties.FromStreamProperties(properties));
        }
    }

    /// <summary>
    /// Interface that provides to an operation on an IShardedStreamable, a description of the location
    /// for the shards produced as a result of the operation. For example, on a multi-core machine, the
    /// location may be the number of result shards.
    /// </summary>
    public interface ILocationDescriptor
    {
        /// <summary>
        /// Gets a representation of the location
        /// </summary>
        /// <returns>A representation of the location</returns>
        object GetLocation();
    }

    /// <summary>
    /// Interface that provides to the "Multicast" operation, a description of how individual events
    /// should be routed, i.e., to which destination output stream(s) a given event should be routed to.
    /// </summary>
    public interface IDestinationDescriptor
    {
        /// <summary>
        /// Gets a representation of the destination
        /// </summary>
        /// <returns>A representation of the destination</returns>
        object GetDestination();
    }

    /// <summary>
    /// Local location descriptor that identifies the number of shards that the data is shuffled into.
    /// </summary>
    public class LocalLocationDescriptor : ILocationDescriptor
    {
        private readonly int numShards = 0;

        /// <summary>
        /// Create a local location descriptor for use within a machine.
        /// </summary>
        /// <param name="numShards">Number of shards</param>
        public LocalLocationDescriptor(int numShards = 0)
        {
            this.numShards = numShards;
        }

        /// <summary>
        /// Required for Shuffle
        /// </summary>
        /// <returns>Number of shards as an object</returns>
        public object GetLocation() => this.numShards;
    }

    /// <summary>
    /// Local location descriptor that identifies the number of shards that the data is shuffled into.
    /// </summary>
    public class LocalDestinationDescriptor<TKey, TPayload> : IDestinationDescriptor
    {
        private readonly Expression<Func<TKey, int, TPayload, int[]>> destinationSelector;

        /// <summary>
        /// Create a local location descriptor for use within a machine.
        /// </summary>
        /// <param name="destinationSelector">Expression that takes a key, a hash value for the key,
        /// and a payload, and returns the set of destinations (as an array of integer offsets into the
        /// array of destination streams)</param>
        public LocalDestinationDescriptor(Expression<Func<TKey, int, TPayload, int[]>> destinationSelector)
        {
            this.destinationSelector = destinationSelector;
        }

        /// <summary>
        /// Required for Shuffle
        /// </summary>
        /// <returns>Number of shards as an object</returns>
        public object GetDestination() => this.destinationSelector;
    }
}
