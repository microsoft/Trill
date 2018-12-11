// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Globalization;
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
            PropertyInfo getLength = this.RuntimeType.GetTypeInfo().GetProperty("Length");
            if (getLength == null)
            {
                throw new SerializationException(
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "Runtime type '{0}' is being serialized as array, but does not have 'Length' property.", this.RuntimeType));
            }

            var body = new List<Expression>();

            ParameterExpression arrayLength = Expression.Variable(typeof(int), "arrayLength");
            body.Add(Expression.Assign(arrayLength, Expression.Property(value, getLength)));
            body.Add(EncodeArrayChunkMethod.ReplaceParametersInBody(encoder, arrayLength));

            LabelTarget label = Expression.Label();
            ParameterExpression counter = Expression.Variable(typeof(int), "counter");
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
            Type arrayType = this.RuntimeType;

            MethodInfo resize = typeof(Array).GetTypeInfo().GetMethod("Resize").MakeGenericMethod(arrayType.GetElementType());
            var body = new List<Expression>();

            ParameterExpression result = Expression.Variable(arrayType, "result");
            body.Add(Expression.Assign(result, Expression.NewArrayBounds(arrayType.GetElementType(), ConstantZero)));

            ParameterExpression index = Expression.Variable(typeof(int), "index");
            body.Add(Expression.Assign(index, ConstantZero));

            ParameterExpression chunkSize = Expression.Variable(typeof(int), "chunkSize");
            ParameterExpression counter = Expression.Variable(typeof(int), "counter");

            LabelTarget chunkReadLoop = Expression.Label();
            LabelTarget arrayReadLoop = Expression.Label();

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
