// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.IO;
using System.Threading;
using Microsoft.StreamProcessing.Serializer;

namespace Microsoft.StreamProcessing.Sharding
{
    /// <summary>
    /// Serializer class for serializing data from sharded streams
    /// </summary>
    /// <typeparam name="TKey">Grouping key type for data in the query</typeparam>
    /// <typeparam name="TPayload">Event payload type for data in the query</typeparam>
    internal sealed class ShardedStreamSerializer<TKey, TPayload>
    {
        /// <summary>
        /// Constructor to take a sharded streamable and provide serialization features
        /// </summary>
        /// <param name="source">The sharded streamable from which data shall be serialized</param>
        /// <param name="destinations">The sinks to which serialized data should be written</param>
        /// <param name="aSync">States whether serialization should be able to be done asynchronously</param>
        /// <param name="writePropertiesToStream">States whether streams properties should be written to the binary stream</param>
        public ShardedStreamSerializer(IShardedStreamable<TKey, TPayload> source, Stream[] destinations, bool aSync = false, bool writePropertiesToStream = false)
        {
            var shardedSource = (ShardedStreamable<TKey, TPayload>)source;

            var observers = new ShardedSerializerObserver<TKey, TPayload>[shardedSource.Streamables.Length];

            for (int i = 0; i < shardedSource.Streamables.Length; i++)
            {
                observers[i] = new ShardedSerializerObserver<TKey, TPayload>(destinations[i], shardedSource.Streamables[i].Properties, writePropertiesToStream);
                shardedSource.Streamables[i].ToStreamMessageObservable().Subscribe(observers[i]);
            }

            if (!aSync)
            {
                for (int i = 0; i < observers.Length; i++)
                {
                    observers[i].Wait();
                }
            }
            return;
        }
    }

    internal sealed class ShardedSerializerObserver<TKey, TPayload> : IObserver<StreamMessage<TKey, TPayload>>, IDisposable
    {
        private Exception e = null;

        private readonly StateSerializer<QueuedMessage<StreamMessage<TKey, TPayload>>> serializer;

        private readonly Stream destination;
        private readonly AutoResetEvent done = new AutoResetEvent(false);

        // TODO: This appears to be copied code from Binary egress - can we unify?
        public ShardedSerializerObserver(Stream destination, StreamProperties<TKey, TPayload> sourceProps, bool writePropertiesToStream = false)
        {
            this.destination = destination;
            if (writePropertiesToStream)
            {
                var propSer = StreamableSerializer.Create<SerializedProperties>();
                propSer.Serialize(destination, SerializedProperties.FromStreamProperties(sourceProps));
            }

            this.serializer = StreamableSerializer.Create<QueuedMessage<StreamMessage<TKey, TPayload>>>(new SerializerSettings());
        }

        public void Dispose()
        {
            this.done.Set();
            this.done.Dispose();
        }

        public void OnCompleted()
        {
            var completed = new QueuedMessage<StreamMessage<TKey, TPayload>> { Kind = MessageKind.Completed };
            this.serializer.Serialize(this.destination, completed);
            this.destination.Flush();
            this.done.Set();
        }

        public void OnError(Exception error)
        {
            this.e = error;
            this.done.Set();
        }

        public void OnNext(StreamMessage<TKey, TPayload> value)
        {
            this.serializer.Serialize(this.destination, new QueuedMessage<StreamMessage<TKey, TPayload>> { Kind = MessageKind.DataBatch, Message = value });
            value.Free();
        }

        public void Wait()
        {
            this.done.WaitOne();
            if (this.e != null) throw this.e;
        }
    }

    /// <summary>
    /// Static class to provide deserialization services to sharded streamable from and to binary streams
    /// </summary>
    public static class ShardedStreamableSerializer
    {
        /// <summary>
        /// Deserialize data from binary streams to a sharded streamable
        /// </summary>
        /// <typeparam name="TKey">Grouping key type for data in the query</typeparam>
        /// <typeparam name="TPayload">Event payload type for data in the query</typeparam>
        /// <param name="binaryStream">Streams from which to deserialize binary data to sharded streams</param>
        /// <param name="scheduler"></param>
        /// <param name="readPropertiesFromStream"></param>
        /// <param name="inputProperties"></param>
        /// <returns>A sharded streamable hydrated from the data in the binary streams</returns>
        public static IShardedStreamable<TKey, TPayload> FromBinaryStream<TKey, TPayload>(Stream[] binaryStream, IIngressScheduler scheduler = null, bool readPropertiesFromStream = false, StreamProperties<TKey, TPayload> inputProperties = null)
        {
            var streams = new BinaryIngressStreamable<TKey, TPayload>[binaryStream.Length];
            for (int i = 0; i < binaryStream.Length; i++)
                streams[i] = new BinaryIngressStreamable<TKey, TPayload>(binaryStream[i], 0, scheduler, inputProperties, readPropertiesFromStream, null, null);

            return new ShardedStreamable<TKey, TPayload>(streams);
        }

        /// <summary>
        /// Deserialize data from binary streams to a sharded streamable
        /// </summary>
        /// <typeparam name="TKey">Grouping key type for data in the query</typeparam>
        /// <typeparam name="TPayload">Event payload type for data in the query</typeparam>
        /// <param name="binaryStream">Streams from which to deserialize binary data to sharded streams</param>
        /// <param name="readPropertiesFromStream"></param>
        /// <param name="inputProperties"></param>
        /// <returns>A sharded streamable hydrated from the data in the binary streams</returns>
        public static IShardedStreamable<TKey, TPayload> FromBinaryStreamPassive<TKey, TPayload>(Stream[] binaryStream, bool readPropertiesFromStream = false, StreamProperties<TKey, TPayload> inputProperties = null)
        {
            var streams = new BinaryIngressStreamablePassive<TKey, TPayload>[binaryStream.Length];
            for (int i = 0; i < binaryStream.Length; i++)
                streams[i] = new BinaryIngressStreamablePassive<TKey, TPayload>(binaryStream[i], inputProperties, readPropertiesFromStream, null, null);

            return new ShardedStreamable<TKey, TPayload>(streams);
        }

        /// <summary>
        /// Stream data from a sharded streamable to binary streams
        /// </summary>
        /// <typeparam name="TKey">Grouping key type for data in the query</typeparam>
        /// <typeparam name="TPayload">Event payload type for data in the query</typeparam>
        /// <param name="source">The sharded streamable from which data shall be serialized</param>
        /// <param name="destinations">The sinks to which serialized data should be written</param>
        /// <param name="async">States whether serialization should be able to be done asynchronously</param>
        /// <param name="writePropertiesToStream">Write stream properties to the binary stream</param>
        public static void ToBinaryStream<TKey, TPayload>(this IShardedStreamable<TKey, TPayload> source, Stream[] destinations, bool async = false, bool writePropertiesToStream = false)
            => new ShardedStreamSerializer<TKey, TPayload>(source, destinations, async, writePropertiesToStream);
    }
}
