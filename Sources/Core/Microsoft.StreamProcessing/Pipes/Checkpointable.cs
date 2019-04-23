// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Represents an object that can be checkpointed and restored
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class Checkpointable
    {
        private readonly Lazy<MethodInfo> serializerMethod;
        private readonly Lazy<MethodInfo> deserializerMethod;
        private readonly Lazy<List<FieldInfo>> serializationFields;
        private readonly Lazy<List<FieldInfo>> schemaFields;
        private readonly Lazy<int> schemaHashCode;
        internal readonly bool? isColumnar;
        internal readonly QueryContainer container;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public Guid ClassId { get; } = Guid.NewGuid();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected Checkpointable(bool? isColumnar, QueryContainer container)
        {
            this.isColumnar = isColumnar;
            this.container = container;
            this.schemaHashCode = new Lazy<int>(GetSchemaHashCode);
            this.serializerMethod = new Lazy<MethodInfo>(GetSerializerMethod);
            this.deserializerMethod = new Lazy<MethodInfo>(GetDeserializerMethod);
            this.schemaFields = new Lazy<List<FieldInfo>>(GetSchemaFields);
            this.serializationFields = new Lazy<List<FieldInfo>>(GetSerializationFields);
        }

        private int GetSchemaHashCode()
            => this.schemaFields.Value.Aggregate(GetType().ToString().StableHash(), (a, f) => a ^ (f.GetValue(this) ?? string.Empty).ToString().StableHash());

        private object Serializer => this.container?.GetOrCreateSerializer(GetType());

        private MethodInfo GetSerializerMethod()
            => this.Serializer.GetType().GetTypeInfo().GetMethod("Serialize", new Type[] { typeof(Stream), GetType() });

        private MethodInfo GetDeserializerMethod()
            => this.Serializer.GetType().GetTypeInfo().GetMethod("Deserialize", new Type[] { typeof(Stream) });

        private List<FieldInfo> GetSerializationFields()
            => GetType().GetAllFields().Where(f => f.IsDefined(typeof(DataMemberAttribute))).ToList();

        private List<FieldInfo> GetSchemaFields()
            => GetType().GetAllFields().Where(f => f.IsDefined(typeof(SchemaSerializationAttribute))).ToList();

        private void Serialize(Stream stream)
            => this.serializerMethod.Value.Invoke(this.Serializer, new object[] { stream, this });

        private void Deserialize(Stream stream)
        {
            object newObject = this.deserializerMethod.Value.Invoke(this.Serializer, new object[] { stream });

            foreach (var field in this.serializationFields.Value) field.SetValue(this, field.GetValue(newObject));

            UpdatePointers();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual void Checkpoint(Stream stream)
        {
            CheckpointSchema(stream);
            Serialize(stream);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected void CheckpointSchema(Stream stream) => stream.Write(BitConverter.GetBytes(this.schemaHashCode.Value), 0, sizeof(int));

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual void Restore(Stream stream)
        {
            ValidateSchema(stream);
            Deserialize(stream);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual void Reset() { }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected void ValidateSchema(Stream stream)
        {
            byte[] hashCodeBytes = new byte[sizeof(int)];
            try
            {
                stream.ReadAllRequiredBytes(hashCodeBytes, 0, sizeof(int));
                int hashCode = BitConverter.ToInt32(hashCodeBytes, 0);
                if (this.schemaHashCode.Value != hashCode)
                {
                    throw new StreamProcessingException("The input serialization state does not match the schema of the query.");
                }
            }
            catch (Exception e)
            {
                throw new StreamProcessingException("The input serialization state does not appear to be formed correctly.", e);
            }
        }

        /// <summary>
        /// Cleanup method called during restoration to ensure that graph pointers refer to the right places
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected virtual void UpdatePointers() { }
    }
}