// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Concurrent;
using Microsoft.StreamProcessing.Serializer.Serializers;

namespace Microsoft.StreamProcessing.Serializer
{
    /// <summary>
    /// Serializer to read/write objects from/to streams
    /// </summary>
    public static class StreamSerializer
    {
        private static readonly ConcurrentDictionary<Tuple<Type, SerializerSettings>, object> TypedSerializers
            = new ConcurrentDictionary<Tuple<Type, SerializerSettings>, object>();

        /// <summary>
        /// Create instance of serializer for given object type
        /// </summary>
        /// <typeparam name="T">Type of object</typeparam>
        /// <returns>StateSerializer</returns>
        public static StateSerializer<T> Create<T>() => Create<T>(new SerializerSettings());

        /// <summary>
        /// Creates a serializer that allows serializing types attributed with <see cref="T:System.Runtime.Serialization.DataContractAttribute" />.
        /// </summary>
        /// <typeparam name="T">The type of objects to serialize.</typeparam>
        /// <param name="settings">The serialization settings.</param>
        /// <returns> A serializer. </returns>
        /// <exception cref="System.ArgumentNullException">Thrown if <paramref name="settings"/> is null.</exception>
        /// <remarks>
        /// This function can cause in-memory runtime code generation if the type <typeparamref name="T"/> has not used seen before.
        /// Otherwise, a cached version of the serializer is given to the user.
        /// </remarks>
        public static StateSerializer<T> Create<T>(SerializerSettings settings)
        {
            if (settings == null) throw new ArgumentNullException(nameof(settings));

            var key = Tuple.Create(typeof(T), settings);
            lock (TypedSerializers)
            {
                if (TypedSerializers.TryGetValue(key, out object serializer) && settings.UseCache)
                    return (StateSerializer<T>)serializer;
            }

            var reader = new ReflectionSchemaBuilder(settings).BuildSchema(typeof(T));
            var serializerTyped = new StateSerializer<T>(reader);

            if (settings.UseCache) TypedSerializers.TryAdd(key, serializerTyped);
            return serializerTyped;
        }
    }
}
