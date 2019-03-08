// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************

namespace Microsoft.StreamProcessing.Sharding
{
    /// <summary>
    /// Additional extension methods to provide sharding capabilities on top of streamables
    /// </summary>
    public static partial class Streamable
    {
        /// <summary>
        /// Shard a streamable
        /// </summary>
        /// <typeparam name="TPayload">The event payload type</typeparam>
        /// <param name="source">The stream to shard</param>
        /// <param name="shardArity">The number of shards to create</param>
        /// <returns>A sharded stream across <paramref name="shardArity"/> shards</returns>
        public static IShardedStreamable<Empty, TPayload> Shard<TPayload>(this IStreamable<Empty, TPayload> source, int shardArity = -1)
        {
            if (shardArity == -1)
            {
                shardArity = Config.StreamScheduler.scheduler.MapArity;
            }

            var spray = new SprayGroupImportStreamable<Empty, TPayload>(source, shardArity, false, null);

            var streamables = new IStreamable<Empty, TPayload>[shardArity];
            for (int i = 0; i < shardArity; i++)
                streamables[i] = new PassthroughStreamable<Empty, TPayload>(spray);

            return new ShardedStreamable<Empty, TPayload>(streamables);
        }

        /// <summary>
        /// Unshard operation on a non-partitioned stream
        /// </summary>
        /// <typeparam name="TPayload">The event payload type</typeparam>
        /// <param name="source">The stream to be returned from sharded to strictly a unified stream</param>
        /// <returns>A streamable brought together from all shards</returns>
        public static IStreamable<Empty, TPayload> Unshard<TPayload>(this IShardedStreamable<Empty, TPayload> source)
            => new MultiUnionStreamable<Empty, TPayload>(((ShardedStreamable<Empty, TPayload>)source).Streamables, false);

        /// <summary>
        /// Unshard operation on a partitioned stream
        /// </summary>
        /// <typeparam name="TKey">The partition key type</typeparam>
        /// <typeparam name="TPayload">The event payload type</typeparam>
        /// <param name="source">The stream to be returned from sharded to strictly a unified stream</param>
        /// <returns>A streamable brought together from all shards</returns>
        public static IStreamable<PartitionKey<TKey>, TPayload> Unshard<TKey, TPayload>(this IShardedStreamable<PartitionKey<TKey>, TPayload> source)
            => new MultiUnionStreamable<PartitionKey<TKey>, TPayload>(((ShardedStreamable<PartitionKey<TKey>, TPayload>)source).Streamables, false);
    }
}
