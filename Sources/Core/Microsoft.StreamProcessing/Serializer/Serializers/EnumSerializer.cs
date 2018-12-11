// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Serializer.Serializers
{
    internal sealed class EnumSerializer : ObjectSerializerBase
    {
        private static readonly Expression<Action<BinaryEncoder, long>> encoderExpression = (e, v) => e.Encode(v);
        private static readonly Expression<Func<BinaryDecoder, long>> decoderExpression = d => d.DecodeLong();

        public EnumSerializer(Type type) : base(type) { }

        protected override Expression BuildSerializerSafe(Expression encoder, Expression value)
            => encoderExpression.ReplaceParametersInBody(
                encoder,
                value.Type == typeof(long) ? value : Expression.Convert(value, typeof(long)));

        protected override Expression BuildDeserializerSafe(Expression decoder)
            => Expression.Convert(
                decoderExpression.ReplaceParametersInBody(decoder), this.RuntimeType);
    }
}
