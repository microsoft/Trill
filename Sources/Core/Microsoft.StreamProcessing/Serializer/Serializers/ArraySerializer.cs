// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing.Serializer.Serializers
{
    internal sealed class ArraySerializer : ObjectSerializerBase
    {
        public ArraySerializer(ObjectSerializerBase item, Type runtimeType) : base(runtimeType) => this.ItemSchema = item;

        protected override Expression BuildSerializerSafe(Expression encoder, Expression value)
        {
            var getLength = this.RuntimeType.GetTypeInfo().GetProperty("Length");
            if (getLength == null)
            {
                throw new SerializationException($"Runtime type '{this.RuntimeType}' is being serialized as array, but does not have 'Length' property.");
            }

            var body = new List<Expression>();

            var arrayLength = Expression.Variable(typeof(int), "arrayLength");
            body.Add(Expression.Assign(arrayLength, Expression.Property(value, getLength)));
            body.Add(EncodeArrayChunkMethod.ReplaceParametersInBody(encoder, arrayLength));

            var label = Expression.Label();
            var counter = Expression.Variable(typeof(int), "counter");
            body.Add(Expression.Assign(counter, ConstantZero));

            body.Add(
                Expression.Loop(
                    Expression.Block(
                        Expression.IfThen(Expression.GreaterThanOrEqual(counter, arrayLength), Expression.Break(label)), this.ItemSchema.BuildSerializer(encoder, Expression.ArrayAccess(value, counter)),
                        Expression.PreIncrementAssign(counter)),
                    label));

            body.Add(
                Expression.IfThen(
                    Expression.NotEqual(arrayLength, ConstantZero),
                    EncodeArrayZeroLength.ReplaceParametersInBody(encoder)));

            return Expression.Block(new[] { arrayLength, counter }, body);
        }

        protected override Expression BuildDeserializerSafe(Expression decoder)
        {
            var arrayType = this.RuntimeType;

            var resize = typeof(Array).GetTypeInfo().GetMethod("Resize").MakeGenericMethod(arrayType.GetElementType());
            var body = new List<Expression>();

            var result = Expression.Variable(arrayType, "result");
            body.Add(Expression.Assign(result, Expression.NewArrayBounds(arrayType.GetElementType(), ConstantZero)));

            var index = Expression.Variable(typeof(int), "index");
            body.Add(Expression.Assign(index, ConstantZero));

            var chunkSize = Expression.Variable(typeof(int), "chunkSize");
            var counter = Expression.Variable(typeof(int), "counter");

            var chunkReadLoop = Expression.Label();
            var arrayReadLoop = Expression.Label();

            body.Add(
                Expression.Loop(
                    Expression.Block(
                        Expression.Assign(chunkSize, DecodeArrayChunkMethod.ReplaceParametersInBody(decoder)),
                        Expression.IfThen(Expression.Equal(chunkSize, ConstantZero), Expression.Break(arrayReadLoop)),
                        Expression.Call(resize, result, Expression.Add(index, chunkSize)),
                        Expression.Assign(counter, ConstantZero),
                        Expression.Loop(
                            Expression.Block(
                                Expression.IfThen(
                                    Expression.GreaterThanOrEqual(counter, chunkSize), Expression.Break(chunkReadLoop)),
                                Expression.Assign(
                                    Expression.ArrayAccess(result, index), this.ItemSchema.BuildDeserializer(decoder)),
                                Expression.PreIncrementAssign(index),
                                Expression.PreIncrementAssign(counter)),
                            chunkReadLoop)),
                    arrayReadLoop));
            body.Add(result);
            return Expression.Block(new[] { result, index, chunkSize, counter }, body);
        }

        public ObjectSerializerBase ItemSchema { get; }
    }
}
