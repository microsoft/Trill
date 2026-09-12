// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using System.Reflection;

namespace Microsoft.StreamProcessing.Serializer.Serializers
{
    internal sealed class RecordFieldSerializer
    {
        public ObjectSerializerBase Schema { get; }

        private MyFieldInfo MemberInfo { get; }

        public RecordFieldSerializer(ObjectSerializerBase schema, MyFieldInfo info)
        {
            this.Schema = schema;
            this.MemberInfo = info;
        }

        public Expression BuildSerializer(Expression encoder, Expression @object)
        {
            ArgumentNullException.ThrowIfNull(encoder);
            ArgumentNullException.ThrowIfNull(@object);

            var member = this.GetMember(@object);
            if (this.Schema.RuntimeType.IsValueType || this.MemberInfo.isField)
            {
                return this.Schema.BuildSerializer(encoder, member);
            }

            var tmp = Expression.Variable(this.Schema.RuntimeType, Guid.NewGuid().ToString());
            var assignment = Expression.Assign(tmp, member);
            var serialized = this.Schema.BuildSerializer(encoder, tmp);
            return Expression.Block(new[] { tmp }, [assignment, serialized]);
        }

        public Expression BuildDeserializer(Expression decoder, Expression @object)
        {
            ArgumentNullException.ThrowIfNull(decoder);
            ArgumentNullException.ThrowIfNull(@object);

            var value = this.Schema.BuildDeserializer(decoder);
            var member = this.GetMember(@object);
            if (@object.Type.IsValueType)
            {
                var tmp = Expression.Variable(value.Type);
                return Expression.Block(
                    new[] { tmp },
                    Expression.Assign(tmp, value),
                    Expression.Assign(member, tmp));
            }

            return Expression.Assign(member, value);
        }

        private Expression GetMember(Expression @object)
            => this.MemberInfo.isField
                ? Expression.Field(@object, this.MemberInfo.DeclaringType, this.MemberInfo.OriginalName)
                : Expression.Property(@object, this.MemberInfo.DeclaringType, this.MemberInfo.OriginalName);
    }
}
