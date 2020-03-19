// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;

namespace Microsoft.StreamProcessing.Serializer.Serializers
{
    internal sealed class ReflectionSchemaBuilder
    {
        private static readonly Dictionary<Type, Func<ObjectSerializerBase>> RuntimeTypeToSerializer =
            new Dictionary<Type, Func<ObjectSerializerBase>>
            {
                { typeof(char),           () => PrimitiveSerializer.Char },
                { typeof(byte),           () => PrimitiveSerializer.Byte },
                { typeof(sbyte),          () => PrimitiveSerializer.SByte },
                { typeof(short),          () => PrimitiveSerializer.Short },
                { typeof(ushort),         () => PrimitiveSerializer.UShort },
                { typeof(int),            () => PrimitiveSerializer.Int },
                { typeof(uint),           () => PrimitiveSerializer.UInt },
                { typeof(bool),           () => PrimitiveSerializer.Boolean },
                { typeof(long),           () => PrimitiveSerializer.Long },
                { typeof(ulong),          () => PrimitiveSerializer.ULong },
                { typeof(float),          () => PrimitiveSerializer.Float },
                { typeof(double),         () => PrimitiveSerializer.Double },
                { typeof(decimal),        () => PrimitiveSerializer.Decimal },
                { typeof(string),         () => PrimitiveSerializer.String },
                { typeof(Uri),            () => PrimitiveSerializer.Uri },
                { typeof(TimeSpan),       () => PrimitiveSerializer.TimeSpan },
                { typeof(DateTime),       () => PrimitiveSerializer.DateTime },
                { typeof(DateTimeOffset), () => PrimitiveSerializer.DateTimeOffset },
                { typeof(Guid),           () => PrimitiveSerializer.Guid },

                { typeof(char[]),         () => PrimitiveSerializer.CreateForArray<char>() },
                { typeof(byte[]),         () => PrimitiveSerializer.ByteArray },
                { typeof(short[]),        () => PrimitiveSerializer.CreateForArray<short>() },
                { typeof(ushort[]),       () => PrimitiveSerializer.CreateForArray<ushort>() },
                { typeof(int[]),          () => PrimitiveSerializer.CreateForArray<int>() },
                { typeof(uint[]),         () => PrimitiveSerializer.CreateForArray<uint>() },
                { typeof(long[]),         () => PrimitiveSerializer.CreateForArray<long>() },
                { typeof(ulong[]),        () => PrimitiveSerializer.CreateForArray<ulong>() },
                { typeof(float[]),        () => PrimitiveSerializer.CreateForArray<float>() },
                { typeof(double[]),       () => PrimitiveSerializer.CreateForArray<double>() },
                { typeof(string[]),       () => PrimitiveSerializer.StringArray },

                { typeof(ColumnBatch<char>),     () => PrimitiveSerializer.CreateForColumnBatch<char>() },
                { typeof(ColumnBatch<short>),    () => PrimitiveSerializer.CreateForColumnBatch<short>() },
                { typeof(ColumnBatch<ushort>),   () => PrimitiveSerializer.CreateForColumnBatch<ushort>() },
                { typeof(ColumnBatch<int>),      () => PrimitiveSerializer.CreateForColumnBatch<int>() },
                { typeof(ColumnBatch<uint>),     () => PrimitiveSerializer.CreateForColumnBatch<uint>() },
                { typeof(ColumnBatch<long>),     () => PrimitiveSerializer.CreateForColumnBatch<long>() },
                { typeof(ColumnBatch<ulong>),    () => PrimitiveSerializer.CreateForColumnBatch<ulong>() },
                { typeof(ColumnBatch<float>),    () => PrimitiveSerializer.CreateForColumnBatch<float>() },
                { typeof(ColumnBatch<double>),   () => PrimitiveSerializer.CreateForColumnBatch<double>() },
                { typeof(ColumnBatch<string>),   () => PrimitiveSerializer.StringColumnBatch },

                { typeof(CharArrayWrapper),      () => PrimitiveSerializer.CharArray },
            };

        private readonly SerializerSettings settings;
        private readonly HashSet<Type> knownTypes;
        private readonly Dictionary<Type, ObjectSerializerBase> seenTypes = new Dictionary<Type, ObjectSerializerBase>();

        public ReflectionSchemaBuilder(SerializerSettings settings)
        {
            this.settings = settings ?? throw new ArgumentNullException(nameof(settings));
            this.knownTypes = new HashSet<Type>(this.settings.KnownTypes);
        }

        public ObjectSerializerBase BuildSchema(Type type)
        {
            if (type == null) throw new ArgumentNullException(nameof(type));

            this.knownTypes.UnionWith(type.GetAllKnownTypes() ?? new List<Type>());
            return CreateSchema(type, 0);
        }

        private ObjectSerializerBase CreateSchema(Type type, uint currentDepth)
        {
            if (currentDepth == this.settings.MaxItemsInSchemaTree)
                throw new SerializationException("Maximum depth of object graph reached.");

            var surrogate = this.settings.Surrogate;
            if (surrogate != null)
            {
                if (surrogate.IsSupportedType(type, out var serialize, out var deserialize))
                {
                    return new SurrogateSerializer(this.settings, type, serialize, deserialize);
                }

                if (type.IsUnsupported())
                {
                    throw new SerializationException($"Type '{type}' is not supported.");
                }
            }

            return type.ValidateTypeForSerializer().CanContainNull()
                ? CreateNullableSchema(type, currentDepth)
                : CreateNotNullableSchema(type, currentDepth);
        }

        private ObjectSerializerBase CreateNullableSchema(Type type, uint currentDepth)
        {
            if (type.GetTypeInfo().IsInterface || type.GetTypeInfo().IsAbstract || HasApplicableKnownType(type))
                return new UnionSerializer(FindKnownTypes(type, currentDepth).ToList(), type);

            var typeSchemas = new List<ObjectSerializerBase>();
            var notNullableSchema = CreateNotNullableSchema(Nullable.GetUnderlyingType(type) ?? type, currentDepth);

            typeSchemas.Add(notNullableSchema);

            return new UnionSerializer(typeSchemas, type);
        }

        private ObjectSerializerBase CreateNotNullableSchema(Type type, uint currentDepth)
        {
            if (RuntimeTypeToSerializer.TryGetValue(type, out var p)) return p();
            if (this.seenTypes.TryGetValue(type, out var schema)) return schema;

            var typeInfo = type.GetTypeInfo();
            if (typeInfo.IsEnum) return BuildEnumTypeSchema(type);

            // Array
            if (type.IsArray || type == typeof(Array)) return BuildArrayTypeSchema(type, currentDepth);

            // Enumerable
            var enumerableType = typeInfo
                .GetInterfaces()
                .SingleOrDefault(t => t.GetTypeInfo().IsGenericType && t.GetGenericTypeDefinition() == typeof(IEnumerable<>));
            if (enumerableType != null)
            {
                var itemType = enumerableType.GetTypeInfo().GetGenericArguments()[0];
                return EnumerableSerializer.Create(type, itemType, CreateSchema(itemType, currentDepth + 1));
            }

            // Others
            if (typeInfo.IsClass || typeInfo.IsValueType) return BuildRecordTypeSchema(type, currentDepth);

            throw new SerializationException($"Type '{type}' is not supported.");
        }

        private ObjectSerializerBase BuildEnumTypeSchema(Type type)
        {
            var result = new EnumSerializer(type);
            this.seenTypes.Add(type, result);
            return result;
        }

        private ObjectSerializerBase BuildArrayTypeSchema(Type type, uint currentDepth)
        {
            var elementSchema = CreateSchema(type.GetElementType(), currentDepth + 1);

            return type == typeof(Array) || type.GetArrayRank() == 1
                ? new ArraySerializer(elementSchema, type)
                : (ObjectSerializerBase)new MultidimensionalArraySerializer(elementSchema, type);
        }

        private ObjectSerializerBase BuildRecordTypeSchema(Type type, uint currentDepth)
        {
            var record = ClassSerializer.Create(type);
            this.seenTypes.Add(type, record);

            var members = type.ResolveMembers();
            foreach (var info in members)
            {
                var fieldSchema = CreateSchema(info.Type, currentDepth + 1);

                var recordField = new RecordFieldSerializer(fieldSchema, info);
                record.AddField(recordField);
            }

            return record;
        }

        private IEnumerable<ObjectSerializerBase> FindKnownTypes(Type type, uint currentDepth)
        {
            var applicable = this.knownTypes.Where(t => t.CanBeKnownTypeOf(type)).ToList();
            if (applicable.Count == 0)
            {
                throw new SerializationException($"Could not find any matching known type for '{type}'.");
            }

            if (!type.IsAbstract && !type.IsInterface && !this.knownTypes.Contains(type)) applicable.Add(type);
            return applicable.Select(t => CreateNotNullableSchema(t, currentDepth));
        }

        private bool HasApplicableKnownType(Type type) => this.knownTypes.Where(t => t.CanBeKnownTypeOf(type)).Any();
    }
}
