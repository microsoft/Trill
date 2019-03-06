// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing.Serializer.Serializers
{
    /// <summary>
    /// Serializer of Union, used for reference types and interfaces/abstract classes.
    /// </summary>
    internal sealed class UnionSerializer : ObjectSerializerBase
    {
        private static readonly Expression<Action<BinaryEncoder, int>> encoderExpression = (e, v) => e.Encode(v);
        private static readonly Expression<Func<BinaryDecoder, int>> decoderExpression = d => d.DecodeInt();
        private readonly List<ObjectSerializerBase> itemSchemas;

        public UnionSerializer(IEnumerable<ObjectSerializerBase> schemas, Type runtimeType) : base(runtimeType)
            => this.itemSchemas = schemas.Select((o, i) => Tuple.Create(o, i)).OrderBy(x => x, Comparer<Tuple<ObjectSerializerBase, int>>.Create(MoreSpecializedTypesFirst)).Select(x => x.Item1).ToList();

        protected override Expression BuildSerializerSafe(Expression encoder, Expression value)
        {
            Expression elseBranch;

            if (this.itemSchemas.Count == 1)
            {
                var otherRuntimeSchema = this.itemSchemas[0];

                elseBranch = Expression.Block(
                        encoderExpression.ReplaceParametersInBody(encoder, Expression.Constant(1)),
                        otherRuntimeSchema.BuildSerializer(
                            encoder,
                            value.Type == otherRuntimeSchema.RuntimeType
                                ? value
                                : (otherRuntimeSchema.RuntimeType.GetTypeInfo().IsValueType
                                    ? (Expression)Expression.Property(value, "Value")
                                    : Expression.TypeAs(value, otherRuntimeSchema.RuntimeType))));
            }
            else
            {
                Expression<Func<object, Exception>> messageFunc = objectType => new SerializationException(string.Format("Object type {0} does not match any item schema of the union: {1}", objectType == null ? null : objectType.GetType(), string.Join(",", this.itemSchemas.Select(s => s.RuntimeType))));
                elseBranch = Expression.Throw(Expression.Invoke(messageFunc, value));
                ConditionalExpression conditions = null;
                for (int i = this.itemSchemas.Count - 1; i >= 0; i--)
                {
                    var schema = this.itemSchemas[i];
                    conditions = Expression.IfThenElse(
                        Expression.TypeIs(value, schema.RuntimeType),
                        Expression.Block(
                            encoderExpression.ReplaceParametersInBody(encoder, Expression.Constant(i + 1)),
                            schema.BuildSerializer(
                                encoder,
                                value.Type == schema.RuntimeType ? value : Expression.Convert(value, schema.RuntimeType))),
                        elseBranch);
                    elseBranch = conditions;
                }
            }

            return Expression.IfThenElse(
                Expression.Equal(value, Expression.Constant(null)),
                encoderExpression.ReplaceParametersInBody(encoder, ConstantZero),
                elseBranch);
        }

        protected override Expression BuildDeserializerSafe(Expression decoder)
        {
            var resultParameter = Expression.Variable(this.RuntimeType, "result");
            var unionTypeParameter = Expression.Variable(typeof(int), "unionType");
            var assignUnionType = Expression.Assign(
                unionTypeParameter,
                decoderExpression.ReplaceParametersInBody(decoder));

            Expression elseBranch = Expression.Empty();
            ConditionalExpression conditions = null;
            for (int i = this.itemSchemas.Count; i >= 0; i--)
            {
                conditions = Expression.IfThenElse(
                    Expression.Equal(unionTypeParameter, Expression.Constant(i)),
                    i == 0
                        ? Expression.Assign(resultParameter, Expression.Constant(null, this.RuntimeType))
                        : Expression.Assign(
                            resultParameter,
                            Expression.Convert(this.itemSchemas[i - 1].BuildDeserializer(decoder), this.RuntimeType)),
                    elseBranch);
                elseBranch = conditions;
            }
            return Expression.Block(new[] { resultParameter, unionTypeParameter }, new Expression[] { assignUnionType, conditions, resultParameter });
        }

        private static int MoreSpecializedTypesFirst(Tuple<ObjectSerializerBase, int> s1, Tuple<ObjectSerializerBase, int> s2)
        {
            if (s1.Item1.RuntimeType.GetTypeInfo().IsAssignableFrom(s2.Item1.RuntimeType)) return 1;
            if (s2.Item1.RuntimeType.GetTypeInfo().IsAssignableFrom(s1.Item1.RuntimeType)) return -1;

            return s1.Item2.CompareTo(s2.Item2);
        }
    }
}