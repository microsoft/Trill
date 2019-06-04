// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing.Serializer.Serializers
{
    internal abstract class ClassSerializer : ObjectSerializerBase
    {
        protected readonly List<RecordFieldSerializer> fields = new List<RecordFieldSerializer>();

        protected ClassSerializer(Type runtimeType) : base(runtimeType) { }

        internal void AddField(RecordFieldSerializer field) => this.fields.Add(field ?? throw new ArgumentNullException(nameof(field)));

        public static ClassSerializer Create(Type runtimeType)
        {
            var serializerType = typeof(ClassSerializerTyped<>).MakeGenericType(runtimeType);
            return (ClassSerializer)Activator.CreateInstance(serializerType);
        }

        private sealed class ClassSerializerTyped<T> : ClassSerializer
        {
            private static Action<T, BinaryEncoder> cachedSerializer;
            private static Func<BinaryDecoder, T> cachedDeserializer;
            private static readonly Expression<Action<StreamMessage>> ensureConsistency = s => s.EnsureConsistency();
            private static readonly Expression<Action<StreamMessage>> inflate = s => s.Inflate();
            private static readonly Expression<Action<StreamMessage>> deflate = s => s.Deflate();
            private static readonly Expression<Func<StreamMessage, bool>> refreshCount = s => s.RefreshCount();
            private static readonly Expression<Action<T, BinaryEncoder>> cachedSerializerExpression = (v, e) => cachedSerializer(v, e);
            private static readonly Expression<Func<BinaryDecoder, T>> cachedDeserializerExpression = d => cachedDeserializer(d);

            public ClassSerializerTyped() : base(typeof(T)) { }

            protected override Expression BuildSerializerSafe(Expression encoder, Expression value)
            {
                if (cachedSerializer != null) return cachedSerializerExpression.ReplaceParametersInBody(value, encoder);

                // For handling potential recursive types.
                cachedSerializer = (o, e) => { };
                cachedSerializer = GenerateCachedSerializer();

                // For performance reasons we do not use a cached serializer
                // for the first encounter of the type in the schema tree.
                return SerializeFields(encoder, value);
            }

            protected override Expression BuildDeserializerSafe(Expression decoder)
            {
                if (cachedDeserializer != null) return cachedDeserializerExpression.ReplaceParametersInBody(decoder);

                // For handling potential recursive types.
                cachedDeserializer = d => default;
                var deserializeLambda = GenerateCachedDeserializer();
                cachedDeserializer = deserializeLambda.Compile();

                return Expression.Invoke(deserializeLambda, decoder);
            }

            private Expression<Func<BinaryDecoder, T>> GenerateCachedDeserializer()
            {
                var decoderParam = Expression.Parameter(typeof(BinaryDecoder), "decoder");
                var instance = Expression.Variable(this.RuntimeType, "instance");

                var body = new List<Expression>();
                if (this.RuntimeType.HasSupportedParameterizedConstructor())
                {
                    // Cannot create an object beforehand. Have to call a constructor with parameters.
                    var properties = this.fields.Select(f => f.Schema.BuildDeserializer(decoderParam));
                    var ctor = this.RuntimeType.GetTypeInfo()
                        .GetConstructors()
                        .Single(c => c.GetParameters().Select(p => p.ParameterType).SequenceEqual(this.fields.Select(f => f.Schema.RuntimeType)));
                    body.Add(Expression.Assign(instance, Expression.New(ctor, properties)));
                }
                else
                {
                    body.Add(Expression.Assign(instance, Expression.New(this.RuntimeType)));
                    body.AddRange(this.fields.Select(f => f.BuildDeserializer(decoderParam, instance)));
                    if (typeof(StreamMessage).IsAssignableFrom(this.RuntimeType))
                        body.Add(inflate.ReplaceParametersInBody(instance));
                }
                body.Add(instance);

                var result = Expression.Block(new[] { instance }, body);
                return Expression.Lambda<Func<BinaryDecoder, T>>(result, decoderParam);
            }

            private Action<T, BinaryEncoder> GenerateCachedSerializer()
            {
                var instanceParam = Expression.Parameter(this.RuntimeType, "instance");
                var encoderParam = Expression.Parameter(typeof(BinaryEncoder), "encoder");
                var block = SerializeFields(encoderParam, instanceParam);
                var lambda = Expression.Lambda<Action<T, BinaryEncoder>>(block, instanceParam, encoderParam);
                return lambda.Compile();
            }

            private Expression SerializeFields(Expression encoder, Expression value)
            {
                var body = new List<Expression>();
                if (typeof(StreamMessage).IsAssignableFrom(this.RuntimeType))
                {
                    body.Add(refreshCount.ReplaceParametersInBody(value));
                    body.Add(deflate.ReplaceParametersInBody(value));
                    body.Add(ensureConsistency.ReplaceParametersInBody(value));
                }

                // Check for null.
                if (!this.RuntimeType.GetTypeInfo().IsValueType)
                {
                    body.Add(Expression.IfThen(
                        Expression.Equal(value, Expression.Constant(null)),
                        ThrowUnexpectedNullCheckExpression(this.RuntimeType)));
                }

                body.AddRange(this.fields.Select(field => field.BuildSerializer(encoder, value)));

                if (typeof(StreamMessage).IsAssignableFrom(this.RuntimeType))
                    body.Add(inflate.ReplaceParametersInBody(value));
                return body.Count != 0
                    ? (Expression)Expression.Block(body)
                    : Expression.Empty();
            }
        }

        public static Expression ThrowUnexpectedNullCheckExpression(Type type)
        {
            Expression<Func<Type, Exception>> exceptionMethod = (t) => UnexpectedNullValueException(t);
            return Expression.Throw(exceptionMethod.ReplaceParametersInBody(Expression.Constant(type)));
        }

        private static Exception UnexpectedNullValueException(Type type)
            => new SerializationException($"Unexpected null value for the object of type '{type}'. Please check the schema.");
    }
}