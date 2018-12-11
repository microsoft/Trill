// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Serializer.Serializers
{
    internal abstract class ObjectSerializerBase
    {
        protected static readonly Expression<Action<BinaryEncoder>> EncodeArrayZeroLength = e => e.EncodeArrayChunk(0);
        protected static readonly Expression<Action<BinaryEncoder, int>> EncodeArrayChunkMethod = (e, i) => e.EncodeArrayChunk(i);
        protected static readonly Expression<Func<BinaryDecoder, int>> DecodeArrayChunkMethod = d => d.DecodeArrayChunk();

        protected static readonly Expression ConstantZero = Expression.Constant(0);

        protected ObjectSerializerBase(Type baseType) : base() => this.RuntimeType = baseType;

        public Expression BuildSerializer(Expression encoder, Expression value)
            => BuildSerializerSafe(
                encoder ?? throw new ArgumentNullException(nameof(encoder)),
                value ?? throw new ArgumentNullException(nameof(value)));

        public Expression BuildDeserializer(Expression decoder)
            => BuildDeserializerSafe(decoder ?? throw new ArgumentNullException(nameof(decoder)));

        protected abstract Expression BuildSerializerSafe(Expression encoder, Expression value);
        protected abstract Expression BuildDeserializerSafe(Expression decoder);

        public Type RuntimeType { get; }
    }
}
