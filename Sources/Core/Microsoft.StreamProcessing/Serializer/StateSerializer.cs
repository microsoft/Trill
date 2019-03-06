// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.IO;
using System.Linq.Expressions;
using Microsoft.StreamProcessing.Serializer.Serializers;

namespace Microsoft.StreamProcessing.Serializer
{
    internal sealed class StateSerializer<T>
    {
        private readonly ObjectSerializerBase schema;
        private readonly Lazy<Action<BinaryEncoder, T>> serialize;
        private readonly Lazy<Func<BinaryDecoder, T>> deserialize;

        internal StateSerializer(ObjectSerializerBase schema)
        {
            this.schema = schema ?? throw new ArgumentNullException(nameof(schema));
            this.serialize = new Lazy<Action<BinaryEncoder, T>>(GenerateSerializer);
            this.deserialize = new Lazy<Func<BinaryDecoder, T>>(GenerateDeserializer);
        }

        public void Serialize(Stream stream, T obj) => this.serialize.Value(new BinaryEncoder(stream), obj);

        public T Deserialize(Stream stream) => this.deserialize.Value(new BinaryDecoder(stream));

        private Action<BinaryEncoder, T> GenerateSerializer()
        {
            var instance = Expression.Parameter(typeof(T), "instance");
            var encoder = Expression.Parameter(typeof(BinaryEncoder), "encoder");

            var result = this.schema.BuildSerializer(encoder, instance);
            var lambda = Expression.Lambda<Action<BinaryEncoder, T>>(result, encoder, instance);
            return lambda.Compile();
        }

        private Func<BinaryDecoder, T> GenerateDeserializer()
        {
            var decoder = Expression.Parameter(typeof(BinaryDecoder), "decoder");

            var result = this.schema.BuildDeserializer(decoder);
            var lambda = Expression.Lambda<Func<BinaryDecoder, T>>(result, decoder);
            return lambda.Compile();
        }
    }
}
