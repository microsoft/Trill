// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Globalization;
using System.Linq.Expressions;
using Microsoft.StreamProcessing.Internal;

namespace Microsoft.StreamProcessing.Serializer.Serializers
{
    internal static class PrimitiveSerializer
    {
        public static ObjectSerializerBase Boolean = new BooleanSerializer();
        public static ObjectSerializerBase Byte = new ByteSerializer();
        public static ObjectSerializerBase ByteArray = new ByteArraySerializer();
        public static ObjectSerializerBase Char = new CharSerializer();
        public static ObjectSerializerBase CharArray = new CharArraySerializer();
        public static ObjectSerializerBase DateTime = new DateTimeSerializer();
        public static ObjectSerializerBase DateTimeOffset = new DateTimeOffsetSerializer();
        public static ObjectSerializerBase Decimal = new DecimalSerializer();
        public static ObjectSerializerBase Double = new DoubleSerializer();
        public static ObjectSerializerBase Float = new FloatSerializer();
        public static ObjectSerializerBase Guid = new GuidSerializer();
        public static ObjectSerializerBase Int = new IntSerializer();
        public static ObjectSerializerBase Long = new LongSerializer();
        public static ObjectSerializerBase SByte = new SByteSerializer();
        public static ObjectSerializerBase Short = new ShortSerializer();
        public static ObjectSerializerBase String = new StringSerializer();
        public static ObjectSerializerBase StringArray = new StringArraySerializer();
        public static ObjectSerializerBase StringColumnBatch = new StringColumnBatchSerializer();
        public static ObjectSerializerBase TimeSpan = new TimeSpanSerializer();
        public static ObjectSerializerBase UInt = new UIntSerializer();
        public static ObjectSerializerBase ULong = new ULongSerializer();
        public static ObjectSerializerBase Uri = new UriSerializer();
        public static ObjectSerializerBase UShort = new UShortSerializer();

        public static ObjectSerializerBase CreateForArray<T>() where T : struct => new PrimitiveArraySerializer<T>();
        public static ObjectSerializerBase CreateForColumnBatch<T>() where T : struct => new PrimitiveColumnBatchSerializer<T>();

        private sealed class BooleanSerializer : PrimitiveSerializer<bool>
        {
            public BooleanSerializer() : base((e, v) => e.Encode(v), d => d.DecodeBool()) { }
        }

        private sealed class ByteSerializer : PrimitiveSerializer<byte>
        {
            public ByteSerializer() : base((e, v) => e.Encode(v), d => d.DecodeByte()) { }
        }

        private sealed class ByteArraySerializer : PrimitiveSerializer<byte[]>
        {
            public ByteArraySerializer() : base((e, v) => e.Encode(v), d => d.DecodeByteArray()) { }
        }

        private sealed class CharSerializer : PrimitiveSerializer<char>
        {
            public CharSerializer() : base((e, v) => e.Encode(v), d => (char)d.DecodeUShort()) { }
        }

        private sealed class CharArraySerializer : PrimitiveSerializer<CharArrayWrapper>
        {
            public CharArraySerializer() : base((e, v) => e.Encode(v), d => d.Decode_CharArrayWrapper()) { }
        }

        private sealed class DateTimeSerializer : PrimitiveSerializer<DateTime>
        {
            public DateTimeSerializer() : base((e, v) => e.Encode(v.ToBinary()), d => System.DateTime.FromBinary(d.DecodeLong())) { }
        }

        private sealed class DateTimeOffsetSerializer : PrimitiveSerializer<DateTimeOffset>
        {
            public DateTimeOffsetSerializer() : base((e, v) => e.Encode(v.ToString("o")), d => System.DateTimeOffset.Parse(d.DecodeString(), null, DateTimeStyles.RoundtripKind)) { }
        }

        private sealed class DecimalSerializer : PrimitiveSerializer<decimal>
        {
            public DecimalSerializer() : base((e, v) => e.Encode(v.ToString()), d => decimal.Parse(d.DecodeString())) { }
        }

        private sealed class DoubleSerializer : PrimitiveSerializer<double>
        {
            public DoubleSerializer() : base((e, v) => e.Encode(v), d => d.DecodeDouble()) { }
        }

        private sealed class FloatSerializer : PrimitiveSerializer<float>
        {
            public FloatSerializer() : base((e, v) => e.Encode(v), d => d.DecodeFloat()) { }
        }

        private sealed class GuidSerializer : PrimitiveSerializer<Guid>
        {
            public GuidSerializer() : base((e, v) => e.Encode(v), d => d.DecodeGuid()) { }
        }

        private sealed class LongSerializer : PrimitiveSerializer<long>
        {
            public LongSerializer() : base((e, v) => e.Encode(v), d => d.DecodeLong()) { }
        }

        private sealed class IntSerializer : PrimitiveSerializer<int>
        {
            public IntSerializer() : base((e, v) => e.Encode(v), d => d.DecodeInt()) { }
        }

        private sealed class SByteSerializer : PrimitiveSerializer<sbyte>
        {
            public SByteSerializer() : base((e, v) => e.Encode(v), d => d.DecodeSByte()) { }
        }

        private sealed class ShortSerializer : PrimitiveSerializer<short>
        {
            public ShortSerializer() : base((e, v) => e.Encode(v), d => d.DecodeShort()) { }
        }

        private sealed class StringSerializer : PrimitiveSerializer<string>
        {
            public StringSerializer() : base((e, v) => e.Encode(v), d => d.DecodeString()) { }
        }

        private sealed class StringArraySerializer : PrimitiveSerializer<string[]>
        {
            public StringArraySerializer() : base((e, v) => e.Encode(v), d => d.DecodeStringArray()) { }
        }

        private sealed class StringColumnBatchSerializer : PrimitiveSerializer<ColumnBatch<string>>
        {
            public StringColumnBatchSerializer() : base((e, v) => e.Encode(v), d => d.Decode_ColumnBatch_String()) { }
        }

        private sealed class TimeSpanSerializer : PrimitiveSerializer<TimeSpan>
        {
            public TimeSpanSerializer() : base((e, v) => e.Encode(v.Ticks), d => new TimeSpan(d.DecodeLong())) { }
        }

        private sealed class UIntSerializer : PrimitiveSerializer<uint>
        {
            public UIntSerializer() : base((e, v) => e.Encode(v), d => d.DecodeUInt()) { }
        }

        private sealed class ULongSerializer : PrimitiveSerializer<ulong>
        {
            public ULongSerializer() : base((e, v) => e.Encode(v), d => d.DecodeULong()) { }
        }

        private sealed class UriSerializer : PrimitiveSerializer<Uri>
        {
            public UriSerializer() : base((e, v) => e.Encode(v.OriginalString), d => new Uri(d.DecodeString())) { }
        }

        private sealed class UShortSerializer : PrimitiveSerializer<ushort>
        {
            public UShortSerializer() : base((e, v) => e.Encode(v), d => d.DecodeUShort()) { }
        }

        private sealed class PrimitiveArraySerializer<T> : PrimitiveSerializer<T[]> where T : struct
        {
            public PrimitiveArraySerializer() : base((e, v) => e.Encode(v), d => d.DecodeArray<T>()) { }
        }

        private sealed class PrimitiveColumnBatchSerializer<T> : PrimitiveSerializer<ColumnBatch<T>> where T : struct
        {
            public PrimitiveColumnBatchSerializer() : base((e, v) => e.Encode(v), d => d.DecodeColumnBatch<T>()) { }
        }
    }

    internal abstract class PrimitiveSerializer<T> : ObjectSerializerBase
    {
        private readonly Expression<Action<BinaryEncoder, T>> encoderExpression;
        private readonly Expression<Func<BinaryDecoder, T>> decoderExpression;

        public PrimitiveSerializer(Expression<Action<BinaryEncoder, T>> encoderExpression, Expression<Func<BinaryDecoder, T>> decoderExpression)
            : base(typeof(T))
        {
            this.encoderExpression = encoderExpression;
            this.decoderExpression = decoderExpression;
        }

        protected sealed override Expression BuildSerializerSafe(Expression encoder, Expression value)
            => this.encoderExpression.ReplaceParametersInBody(encoder, value);

        protected sealed override Expression BuildDeserializerSafe(Expression decoder)
            => this.decoderExpression.ReplaceParametersInBody(decoder);
    }
}
