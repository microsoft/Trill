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
        private Action<BinaryEncoder, T> serialize;
        private Func<BinaryDecoder, T> deserialize;

        internal StateSerializer(ObjectSerializerBase schema) => this.schema = schema ?? throw new ArgumentNullException(nameof(schema));

        public void Serialize(Stream stream, T obj)
        {
            if (this.serialize == null) GenerateSerializer();
            this.serialize(new BinaryEncoder(stream), obj);
        }

        public T Deserialize(Stream stream)
        {
            if (this.deserialize == null) GenerateDeserializer();
            return this.deserialize(new BinaryDecoder(stream));
        }

        private void GenerateSerializer()
        {
            var instance = Expression.Parameter(typeof(T), "instance");
            var encoder = Expression.Parameter(typeof(BinaryEncoder), "encoder");

            var result = this.schema.BuildSerializer(encoder, instance);
            var lambda = Expression.Lambda<Action<BinaryEncoder, T>>(result, encoder, instance);
            this.serialize = lambda.Compile();
        }

        private void GenerateDeserializer()
        {
            var decoder = Expression.Parameter(typeof(BinaryDecoder), "decoder");

            var result = this.schema.BuildDeserializer(decoder);
            var lambda = Expression.Lambda<Func<BinaryDecoder, T>>(result, decoder);
            this.deserialize = lambda.Compile();
        }
    }
}
