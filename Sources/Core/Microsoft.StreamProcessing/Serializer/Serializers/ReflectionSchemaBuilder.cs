// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;

namespace Microsoft.StreamProcessing.Serializer.Serializers
{
    internal sealed class ReflectionSchemaBuilder
    {
        private static readonly ConcurrentDictionary<Type, Func<ObjectSerializerBase>> RuntimeTypeToSerializer =
            new();

        private readonly SerializerSettings settings;
        private readonly HashSet<Type> knownTypes;
        private readonly Dictionary<Type, ObjectSerializerBase> seenTypes = [];

        static ReflectionSchemaBuilder()
        {
            RuntimeTypeToSerializer[typeof(char)] = () => PrimitiveSerializer.Char;
            RuntimeTypeToSerializer[typeof(byte)] = () => PrimitiveSerializer.Byte;
            RuntimeTypeToSerializer[typeof(sbyte)] = () => PrimitiveSerializer.SByte;
            RuntimeTypeToSerializer[typeof(short)] = () => PrimitiveSerializer.Short;
            RuntimeTypeToSerializer[typeof(ushort)] = () => PrimitiveSerializer.UShort;
            RuntimeTypeToSerializer[typeof(int)] = () => PrimitiveSerializer.Int;
            RuntimeTypeToSerializer[typeof(uint)] = () => PrimitiveSerializer.UInt;
            RuntimeTypeToSerializer[typeof(bool)] = () => PrimitiveSerializer.Boolean;
            RuntimeTypeToSerializer[typeof(long)] = () => PrimitiveSerializer.Long;
            RuntimeTypeToSerializer[typeof(ulong)] = () => PrimitiveSerializer.ULong;
            RuntimeTypeToSerializer[typeof(float)] = () => PrimitiveSerializer.Float;
            RuntimeTypeToSerializer[typeof(double)] = () => PrimitiveSerializer.Double;
            RuntimeTypeToSerializer[typeof(decimal)] = () => PrimitiveSerializer.Decimal;
            RuntimeTypeToSerializer[typeof(string)] = () => PrimitiveSerializer.String;
            RuntimeTypeToSerializer[typeof(Uri)] = () => PrimitiveSerializer.Uri;
            RuntimeTypeToSerializer[typeof(TimeSpan)] = () => PrimitiveSerializer.TimeSpan;
            RuntimeTypeToSerializer[typeof(DateTime)] = () => PrimitiveSerializer.DateTime;
            RuntimeTypeToSerializer[typeof(DateTimeOffset)] = () => PrimitiveSerializer.DateTimeOffset;
            RuntimeTypeToSerializer[typeof(Guid)] = () => PrimitiveSerializer.Guid;

            RuntimeTypeToSerializer[typeof(char[])] = PrimitiveSerializer.CreateForArray<char>;
            RuntimeTypeToSerializer[typeof(byte[])] = () => PrimitiveSerializer.ByteArray;
            RuntimeTypeToSerializer[typeof(short[])] = PrimitiveSerializer.CreateForArray<short>;
            RuntimeTypeToSerializer[typeof(ushort[])] = PrimitiveSerializer.CreateForArray<ushort>;
            RuntimeTypeToSerializer[typeof(int[])] = PrimitiveSerializer.CreateForArray<int>;
            RuntimeTypeToSerializer[typeof(uint[])] = PrimitiveSerializer.CreateForArray<uint>;
            RuntimeTypeToSerializer[typeof(long[])] = PrimitiveSerializer.CreateForArray<long>;
            RuntimeTypeToSerializer[typeof(ulong[])] = PrimitiveSerializer.CreateForArray<ulong>;
            RuntimeTypeToSerializer[typeof(float[])] = PrimitiveSerializer.CreateForArray<float>;
            RuntimeTypeToSerializer[typeof(double[])] = PrimitiveSerializer.CreateForArray<double>;
            RuntimeTypeToSerializer[typeof(string[])] = () => PrimitiveSerializer.StringArray;

            RuntimeTypeToSerializer[typeof(ColumnBatch<char>)] = PrimitiveSerializer.CreateForColumnBatch<char>;
            RuntimeTypeToSerializer[typeof(ColumnBatch<short>)] = PrimitiveSerializer.CreateForColumnBatch<short>;
            RuntimeTypeToSerializer[typeof(ColumnBatch<ushort>)] = PrimitiveSerializer.CreateForColumnBatch<ushort>;
            RuntimeTypeToSerializer[typeof(ColumnBatch<int>)] = PrimitiveSerializer.CreateForColumnBatch<int>;
            RuntimeTypeToSerializer[typeof(ColumnBatch<uint>)] = PrimitiveSerializer.CreateForColumnBatch<uint>;
            RuntimeTypeToSerializer[typeof(ColumnBatch<long>)] = PrimitiveSerializer.CreateForColumnBatch<long>;
            RuntimeTypeToSerializer[typeof(ColumnBatch<ulong>)] = PrimitiveSerializer.CreateForColumnBatch<ulong>;
            RuntimeTypeToSerializer[typeof(ColumnBatch<float>)] = PrimitiveSerializer.CreateForColumnBatch<float>;
            RuntimeTypeToSerializer[typeof(ColumnBatch<double>)] = PrimitiveSerializer.CreateForColumnBatch<double>;
            RuntimeTypeToSerializer[typeof(ColumnBatch<string>)] = () => PrimitiveSerializer.StringColumnBatch;

            RuntimeTypeToSerializer[typeof(CharArrayWrapper)] = () => PrimitiveSerializer.CharArray;
        }

        public ReflectionSchemaBuilder(SerializerSettings settings)
        {
            this.settings = settings ?? throw new ArgumentNullException(nameof(settings));
            this.knownTypes = new HashSet<Type>(this.settings.KnownTypes);
        }

        public ObjectSerializerBase BuildSchema(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);

            this.knownTypes.UnionWith(type.GetAllKnownTypes() ?? new List<Type>());
            return this.CreateSchema(type, 0);
        }

        private ObjectSerializerBase CreateSchema(Type type, uint currentDepth)
        {
            if (currentDepth == this.settings.MaxItemsInSchemaTree)
                throw new SerializationException("Maximum depth of object graph reached.");

            this.knownTypes.UnionWith(type.GetAllKnownTypes() ?? new List<Type>());

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
                ? this.CreateNullableSchema(type, currentDepth)
                : this.CreateNotNullableSchema(type, currentDepth);
        }

        private ObjectSerializerBase CreateNullableSchema(Type type, uint currentDepth)
        {
            if (type.IsInterface || type.IsAbstract || this.HasApplicableKnownType(type))
                return new UnionSerializer(this.FindKnownTypes(type, currentDepth).ToList(), type);

            var typeSchemas = new List<ObjectSerializerBase>();
            var notNullableSchema = this.CreateNotNullableSchema(Nullable.GetUnderlyingType(type) ?? type, currentDepth);

            typeSchemas.Add(notNullableSchema);

            return new UnionSerializer(typeSchemas, type);
        }

        private ObjectSerializerBase CreateNotNullableSchema(Type type, uint currentDepth)
        {
            if (RuntimeTypeToSerializer.TryGetValue(type, out var p)) return p();
            if (this.seenTypes.TryGetValue(type, out var schema)) return schema;

            if (type.IsEnum) return this.BuildEnumTypeSchema(type);

            // Array
            if (type.IsArray || type == typeof(Array)) return this.BuildArrayTypeSchema(type, currentDepth);

            // Enumerable
            var enumerableType = type
                .GetInterfaces()
                .SingleOrDefault(t => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IEnumerable<>));
            if (enumerableType != null)
            {
                var itemType = enumerableType.GetGenericArguments()[0];
                return EnumerableSerializer.Create(type, itemType, this.CreateSchema(itemType, currentDepth + 1));
            }

            // Others
            if (type.IsClass || type.IsValueType) return this.BuildRecordTypeSchema(type, currentDepth);

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
            var elementSchema = this.CreateSchema(type.GetElementType(), currentDepth + 1);

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
                var fieldSchema = this.CreateSchema(info.Type, currentDepth + 1);

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
            return applicable.Select(t => this.CreateNotNullableSchema(t, currentDepth));
        }

        private bool HasApplicableKnownType(Type type) => this.knownTypes.Where(t => t.CanBeKnownTypeOf(type)).Any();
    }
}
