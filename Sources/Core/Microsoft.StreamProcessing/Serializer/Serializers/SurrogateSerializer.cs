// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using System.Reflection;

namespace Microsoft.StreamProcessing.Serializer.Serializers
{
    internal sealed class SurrogateSerializer : ObjectSerializerBase
    {
        private readonly MethodInfo serialize;
        private readonly MethodInfo deserialize;

        public SurrogateSerializer(SerializerSettings settings, Type originalType, MethodInfo serialize, MethodInfo deserialize) : base(originalType)
        {
            this.Surrogate = (settings ?? throw new ArgumentNullException(nameof(settings))).Surrogate;
            this.serialize = serialize;
            this.deserialize = deserialize;
        }

        protected override Expression BuildSerializerSafe(Expression encoder, Expression value)
        {
            var surrogate = Expression.Constant(this.Surrogate);
            var stream = Expression.Field(encoder, "stream");
            return Expression.Call(surrogate, this.serialize, new[] { value, stream });
        }

        protected override Expression BuildDeserializerSafe(Expression decoder)
        {
            var surrogate = Expression.Constant(this.Surrogate);
            var stream = Expression.Field(decoder, "stream");
            return Expression.Call(surrogate, this.deserialize, new[] { stream });
        }

        private ISurrogate Surrogate { get; }
    }
}
