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
    internal static class EnumerableSerializer
    {
        public static ObjectSerializerBase Create(Type collectionType, Type itemType, ObjectSerializerBase itemSerializer)
        {
            var serializerType = typeof(EnumerableSerializer<,>).MakeGenericType(collectionType, itemType);
            return (ObjectSerializerBase)Activator.CreateInstance(serializerType, itemSerializer);
        }
    }

    internal sealed class EnumerableSerializer<TCollection, TItem> : ObjectSerializerBase where TCollection : IEnumerable<TItem>, new()
    {
        private static readonly Expression<Func<IEnumerable<TItem>, IEnumerator<TItem>>> GetEnumeratorExpression = c => c.GetEnumerator();
        private static readonly Expression<Func<IEnumerator<TItem>, bool>> MoveNextExpression = c => c.MoveNext();
        private static readonly Expression<Action<List<TItem>>> ListClearExpression = c => c.Clear();
        private static readonly Expression<Func<int, List<TItem>>> ListConstructorExpression = i => new List<TItem>(i);
        private static readonly Expression<Func<TCollection>> CollectionConstructorExpression = () => new TCollection();
        private static readonly Expression<Action<List<TItem>, TItem>> ListAddExpression = (c, i) => c.Add(i);
        private static readonly Expression<Action<ICollection<TItem>, TItem>> CollectionAddExpression = (c, i) => c.Add(i);

        private readonly ObjectSerializerBase itemSchema;

        public EnumerableSerializer(ObjectSerializerBase item) : base(typeof(TCollection)) => this.itemSchema = item;

        /// <summary>
        /// Builds the serialization expression for enumerables.
        /// Serialization happens in chunks of 1024 elements and uses a buffer to avoid memory allocations.
        /// </summary>
        /// <param name="encoder">The encoder.</param>
        /// <param name="value">The value.</param>
        /// <returns>Expression, serializing an enumerable.</returns>
        protected override Expression BuildSerializerSafe(Expression encoder, Expression value)
            => typeof(IList<TItem>).GetTypeInfo().IsAssignableFrom(typeof(TCollection).GetTypeInfo())
                ? BuildSerializerForList(encoder, value)
                : typeof(ICollection<TItem>).GetTypeInfo().IsAssignableFrom(typeof(TCollection).GetTypeInfo())
                    ? BuildSerializerForCollection(encoder, value)
                    : BuildSerializerForEnumerable(encoder, value);

        private Expression BuildSerializerForEnumerable(Expression encoder, Expression value)
        {
            var buffer = Expression.Variable(typeof(List<TItem>), "buffer");
            var counter = Expression.Variable(typeof(int), "counter");
            var item = Expression.Variable(typeof(TItem), "item");
            var enumerator = Expression.Variable(typeof(IEnumerator<TItem>), "enumerator");
            var chunkCounter = Expression.Variable(typeof(int), "chunkCounter");

            var arrayBreak = Expression.Label("arrayBreak");
            var chunkBreak = Expression.Label("chunkBreak");
            var lastChunkBreak = Expression.Label("lastChunkBreak");

            return Expression.Block(
                new[]
                {
                    buffer,
                    counter,
                    item,
                    enumerator,
                    chunkCounter
                },
                Expression.Assign(buffer, ListConstructorExpression.ReplaceParametersInBody(Expression.Constant(1024))),
                Expression.Assign(counter, ConstantZero),
                Expression.Assign(enumerator, GetEnumeratorExpression.ReplaceParametersInBody(value)),
                Expression.Loop(
                    Expression.IfThenElse(
                        Expression.NotEqual(MoveNextExpression.ReplaceParametersInBody(enumerator), Expression.Constant(false)),
                        Expression.Block(
                            Expression.Assign(item, Expression.Property(enumerator, "Current")),
                            Expression.IfThen(
                                Expression.Equal(counter, Expression.Constant(1024)),
                                Expression.Block(
                                    EncodeArrayChunkMethod.ReplaceParametersInBody(encoder, Expression.Constant(1024)),
                                    Expression.Assign(counter, ConstantZero),
                                    Expression.Assign(chunkCounter, ConstantZero),
                                    Expression.Loop(
                                        Expression.Block(
                                            Expression.IfThen(Expression.GreaterThanOrEqual(chunkCounter, Expression.Property(buffer, "Count")), Expression.Break(chunkBreak)), this.itemSchema.BuildSerializer(encoder, Expression.Property(buffer, "Item", chunkCounter)),
                                            Expression.PreIncrementAssign(chunkCounter)),
                                            chunkBreak),
                                    ListClearExpression.ReplaceParametersInBody(buffer))),
                            ListAddExpression.ReplaceParametersInBody(buffer, item),
                            Expression.PreIncrementAssign(counter)),
                        Expression.Break(arrayBreak)),
                    arrayBreak),
                EncodeArrayChunkMethod.ReplaceParametersInBody(encoder, Expression.Property(buffer, "Count")),
                Expression.Assign(counter, ConstantZero),
                Expression.Assign(chunkCounter, ConstantZero),
                Expression.Loop(
                    Expression.Block(
                        Expression.IfThen(Expression.GreaterThanOrEqual(chunkCounter, Expression.Property(buffer, "Count")), Expression.Break(lastChunkBreak)), this.itemSchema.BuildSerializer(encoder, Expression.Property(buffer, "Item", chunkCounter)),
                        Expression.PreIncrementAssign(chunkCounter)),
                    lastChunkBreak),
                Expression.IfThen(
                    Expression.NotEqual(Expression.Property(buffer, "Count"), ConstantZero),
                    EncodeArrayZeroLength.ReplaceParametersInBody(encoder)));
        }

        private Expression BuildSerializerForCollection(Expression encoder, Expression value)
        {
            var body = new List<Expression>();

            var count = Expression.Variable(typeof(int), "count");
            var enumerator = Expression.Variable(typeof(IEnumerator<TItem>), "enumerator");

            body.Add(Expression.Assign(count, Expression.Property(value, "Count")));
            body.Add(EncodeArrayChunkMethod.ReplaceParametersInBody(encoder, count));
            body.Add(Expression.Assign(enumerator, GetEnumeratorExpression.ReplaceParametersInBody(value)));

            var label = Expression.Label();
            body.Add(
                Expression.Loop(
                    Expression.Block(
                        Expression.IfThen(
                            Expression.Equal(MoveNextExpression.ReplaceParametersInBody(enumerator), Expression.Constant(false)),
                            Expression.Break(label)), this.itemSchema.BuildSerializer(encoder, Expression.Property(enumerator, "Current"))),
                    label));

            return Expression.Block(new[] { enumerator, count }, body);
        }

        private Expression BuildSerializerForList(Expression encoder, Expression value)
        {
            var body = new List<Expression>();
            var listCount = Expression.Variable(typeof(int), "count");

            body.Add(Expression.Assign(listCount, Expression.Property(value, "Count")));
            body.Add(EncodeArrayChunkMethod.ReplaceParametersInBody(encoder, listCount));

            var counter = Expression.Variable(typeof(int), "counter");
            body.Add(Expression.Assign(counter, ConstantZero));

            var label = Expression.Label();
            body.Add(
                Expression.Loop(
                    Expression.Block(
                        Expression.IfThen(Expression.GreaterThanOrEqual(counter, listCount), Expression.Break(label)), this.itemSchema.BuildSerializer(encoder, Expression.Property(value, "Item", counter)),
                        Expression.PreIncrementAssign(counter)),
                    label));

            return Expression.Block(new[] { counter, listCount }, body);
        }

        protected override Expression BuildDeserializerSafe(Expression decoder)
            => typeof(ICollection<TItem>).GetTypeInfo().IsAssignableFrom(typeof(TCollection).GetTypeInfo())
                ? BuildDeserializerForCollection(decoder)
                : BuildDeserializerForEnumerable(decoder);

        private Expression BuildDeserializerForEnumerable(Expression decoder)
        {
            var addElement = GetAddMethod();

            var result = Expression.Variable(typeof(TCollection), "result");
            var index = Expression.Variable(typeof(int), "index");
            var chunkSize = Expression.Variable(typeof(int), "chunkSize");
            var counter = Expression.Variable(typeof(int), "counter");

            var chunkLoop = Expression.Label();
            var enumerableLoop = Expression.Label();

            return Expression.Block(
                new[] { result, index, chunkSize, counter },
                Expression.Assign(result, CollectionConstructorExpression.Body),
                Expression.Assign(index, ConstantZero),
                Expression.Loop(
                    Expression.Block(
                        Expression.Assign(chunkSize, DecodeArrayChunkMethod.ReplaceParametersInBody(decoder)),
                        Expression.IfThen(Expression.Equal(chunkSize, ConstantZero), Expression.Break(enumerableLoop)),
                        Expression.Assign(counter, ConstantZero),
                        Expression.Loop(
                            Expression.Block(
                                Expression.IfThen(
                                    Expression.GreaterThanOrEqual(counter, chunkSize), Expression.Break(chunkLoop)),
                                Expression.Call(result, addElement, new[] { this.itemSchema.BuildDeserializer(decoder) }),
                                Expression.PreIncrementAssign(index),
                                Expression.PreIncrementAssign(counter)),
                            chunkLoop)),
                    enumerableLoop),
                result);
        }

        private Expression BuildDeserializerForCollection(Expression decoder)
        {
            var result = Expression.Variable(typeof(TCollection));
            var currentNumberOfElements = Expression.Variable(typeof(int));

            var counter = Expression.Variable(typeof(int));
            var internalLoopLabel = Expression.Label();

            // For types that have a count property, we encode as a single chunk and can thus decode in a single pass.
            var body = Expression.Block(
                new[] { result, currentNumberOfElements, counter },
                Expression.Assign(currentNumberOfElements, DecodeArrayChunkMethod.ReplaceParametersInBody(decoder)),
                Expression.Assign(result, typeof(List<TItem>) == typeof(TCollection)
                    ? ListConstructorExpression.ReplaceParametersInBody(currentNumberOfElements)
                    : CollectionConstructorExpression.Body),
                Expression.Assign(counter, ConstantZero),
                Expression.Loop(
                    Expression.Block(
                        Expression.IfThen(
                            Expression.GreaterThanOrEqual(counter, currentNumberOfElements), Expression.Break(internalLoopLabel)),
                        CollectionAddExpression.ReplaceParametersInBody(result, this.itemSchema.BuildDeserializer(decoder)),
                        Expression.PreIncrementAssign(counter)),
                    internalLoopLabel),
                result);
            return body;
        }

        private MethodInfo GetAddMethod()
        {
            var result = this.RuntimeType.GetMethodByName("Add", typeof(TItem))
                      ?? this.RuntimeType.GetMethodByName("Enqueue", typeof(TItem))
                      ?? this.RuntimeType.GetMethodByName("Push", typeof(TItem));

            if (result == null)
                throw new SerializationException($"Collection type '{this.RuntimeType}' does not have Add/Enqueue/Push method.");
            return result;
        }
    }
}
