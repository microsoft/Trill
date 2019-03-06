// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace Microsoft.StreamProcessing.Serializer.Serializers
{
    /// <summary>
    /// Serialization of C# multidimensional array as array of arrays.
    /// </summary>
    internal sealed class MultidimensionalArraySerializer : ObjectSerializerBase
    {
        public MultidimensionalArraySerializer(ObjectSerializerBase item, Type runtimeType) : base(runtimeType) => this.ItemSchema = item;

        protected override Expression BuildSerializerSafe(Expression encoder, Expression value)
        {
            int rank = this.RuntimeType.GetArrayRank();
            return BuildSerializerImpl(new List<Expression>(), encoder, value, 0, rank);
        }

        private Expression BuildSerializerImpl(
            List<Expression> indexes,
            Expression encoder,
            Expression value,
            int currentRank,
            int maxRank)
        {
            var body = new List<Expression>();
            if (currentRank == maxRank)
                return this.ItemSchema.BuildSerializer(encoder, Expression.ArrayIndex(value, indexes));

            var getLength = this.RuntimeType.GetTypeInfo().GetMethod("GetLength");
            var length = Expression.Variable(typeof(int), "length");
            body.Add(Expression.Assign(length, Expression.Call(value, getLength, new Expression[] { Expression.Constant(currentRank) })));
            body.Add(EncodeArrayChunkMethod.ReplaceParametersInBody(encoder, length));

            var label = Expression.Label();
            var counter = Expression.Variable(typeof(int), "counter");
            body.Add(Expression.Assign(counter, ConstantZero));
            indexes.Add(counter);

            body.Add(
                Expression.Loop(
                    Expression.Block(
                        Expression.IfThen(Expression.GreaterThanOrEqual(counter, length), Expression.Break(label)),
                        BuildSerializerImpl(indexes, encoder, value, currentRank + 1, maxRank),
                        Expression.PreIncrementAssign(counter)),
                    label));

            body.Add(
                Expression.IfThen(
                    Expression.NotEqual(length, ConstantZero),
                    EncodeArrayZeroLength.ReplaceParametersInBody(encoder)));
            return Expression.Block(new[] { counter, length }, body);
        }

        protected override Expression BuildDeserializerSafe(Expression decoder)
        {
            var body = new List<Expression>();

            var type = this.RuntimeType;
            var jaggedType = GenerateJaggedType(this.RuntimeType);

            var deserialized = Expression.Variable(jaggedType, "deserialized");
            body.Add(Expression.Assign(deserialized, GenerateBuildJaggedDeserializer(decoder, jaggedType, 0, type.GetArrayRank())));

            var lengths = new List<Expression>();
            Expression currentObject = deserialized;
            for (int i = 0; i < type.GetArrayRank(); i++)
            {
                lengths.Add(Expression.Property(currentObject, "Count"));
                currentObject = Expression.Property(currentObject, "Item", new Expression[] { ConstantZero });
            }

            var result = Expression.Variable(type, "result");
            body.Add(Expression.Assign(result, Expression.NewArrayBounds(type.GetElementType(), lengths)));
            body.Add(GenerateCopy(new List<Expression>(), result, deserialized, 0, type.GetArrayRank()));
            body.Add(result);
            return Expression.Block(new[] { deserialized, result }, body);
        }

        private static Type GenerateJaggedType(Type sourceType)
        {
            int rank = sourceType.GetArrayRank();
            var elementType = sourceType.GetElementType();
            var result = elementType;
            for (int i = 0; i < rank; i++)
            {
                result = typeof(List<>).MakeGenericType(result);
            }
            return result;
        }

        private Expression GenerateBuildJaggedDeserializer(Expression decoder, Type valueType, int currentRank, int maxRank)
        {
            var body = new List<Expression>();
            if (currentRank == maxRank)
            {
                return this.ItemSchema.BuildDeserializer(decoder);
            }

            var result = Expression.Variable(valueType, "result");
            var index = Expression.Variable(typeof(int), "index");
            var chunkSize = Expression.Variable(typeof(int), "chunkSize");

            body.Add(Expression.Assign(result, Expression.New(valueType)));
            body.Add(Expression.Assign(index, ConstantZero));

            var internalLoopLabel = Expression.Label();
            var counter = Expression.Variable(typeof(int));
            body.Add(Expression.Assign(counter, ConstantZero));

            var allRead = Expression.Label();

            body.Add(
                Expression.Loop(
                    Expression.Block(
                        Expression.Assign(chunkSize, DecodeArrayChunkMethod.ReplaceParametersInBody(decoder)),
                        Expression.IfThen(Expression.Equal(chunkSize, ConstantZero), Expression.Break(allRead)),
                        Expression.Assign(Expression.Property(result, "Capacity"), Expression.Add(index, chunkSize)),
                        Expression.Assign(counter, ConstantZero),
                        Expression.Loop(
                            Expression.Block(
                                Expression.IfThen(
                                    Expression.GreaterThanOrEqual(counter, chunkSize),
                                    Expression.Break(internalLoopLabel)),
                                Expression.Call(
                                    result,
                                    valueType.GetTypeInfo().GetMethod("Add"),
                                    new[]
                                    {
                                        GenerateBuildJaggedDeserializer(
                                            decoder,
                                            valueType.GetTypeInfo().GetGenericArguments()[0],
                                            currentRank + 1,
                                            maxRank)
                                    }),
                                Expression.PreIncrementAssign(index),
                                Expression.PreIncrementAssign(counter)),
                            internalLoopLabel)),
                    allRead));
            body.Add(result);
            return Expression.Block(new[] { result, index, chunkSize, counter }, body);
        }

        private Expression GenerateCopy(List<Expression> indexes, Expression destination, Expression source, int currentRank, int maxRank)
        {
            var body = new List<Expression>();
            if (currentRank == maxRank)
            {
                return Expression.Assign(Expression.ArrayAccess(destination, indexes), source);
            }

            var length = Expression.Variable(typeof(int), "length");
            body.Add(Expression.Assign(length, Expression.Property(source, "Count")));

            var label = Expression.Label();
            var counter = Expression.Variable(typeof(int), "counter");
            body.Add(Expression.Assign(counter, ConstantZero));
            indexes.Add(counter);

            body.Add(
                Expression.Loop(
                    Expression.Block(
                        Expression.IfThen(Expression.GreaterThanOrEqual(counter, length), Expression.Break(label)),
                        GenerateCopy(
                            indexes,
                            destination,
                            Expression.Property(source, "Item", new Expression[] { counter }),
                            currentRank + 1,
                            maxRank),
                        Expression.PreIncrementAssign(counter)),
                    label));

            return Expression.Block(new[] { counter, length }, body);
        }

        public ObjectSerializerBase ItemSchema { get; }
    }
}
