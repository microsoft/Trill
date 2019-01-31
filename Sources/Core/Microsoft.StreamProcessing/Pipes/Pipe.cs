// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics.Contracts;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
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

    /// <summary>
    /// Data processor bound to a particular stream operator. When you subscribe to an operator tree,
    /// a pipe tree is constructed to process data.
    /// </summary>
    /// <typeparam name="TKey">Group key type.</typeparam>
    /// <typeparam name="TPayload">Output payload type.</typeparam>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class Pipe<TKey, TPayload> : Checkpointable, IQueryObject, IDisposable
    {
        private bool disposed = false;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Used to avoid creating redundant readonly property.")]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public readonly IStreamObserver<TKey, TPayload> Observer;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public Guid Id => this.id;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected Guid id;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected Pipe() : base(false, null) { }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected Pipe(IStreamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer)
            : base(stream.Properties.IsColumnar, stream.Properties.QueryContainer)
        {
            Contract.Requires(stream != null);
            Contract.Requires(observer != null);

            this.Observer = observer;
            this.id = Guid.NewGuid();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void Checkpoint(Stream stream)
        {
            base.Checkpoint(stream);
            this.Observer.Checkpoint(stream);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void Restore(Stream stream)
        {
            base.Restore(stream);
            this.Observer.Restore(stream);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual void OnCompleted()
        {
            Dispose();
            this.Observer.OnCompleted();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual void OnError(Exception exception)
        {
            Dispose();
            this.Observer.OnError(exception);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Dispose()
        {
            if (!this.disposed)
            {
                DisposeState();
                this.disposed = true;
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected virtual void DisposeState() { }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual void OnFlush()
        {
            FlushContents();
            this.Observer.OnFlush();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected virtual void FlushContents() { }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public abstract int CurrentlyBufferedOutputCount { get; }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public abstract int CurrentlyBufferedInputCount { get; }

        /// <summary>
        /// Get a query plan of the actively running query rooted at this query node.
        /// </summary>
        /// <param name="previous">The previous node in the query plan.</param>
        /// <returns>The query plan node representing the current query artifact.</returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public abstract void ProduceQueryPlan(PlanNode previous);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected static Func<TGroupKey, TPartition> GetPartitionExtractor<TPartition, TGroupKey>()
        {
            var groupKeyType = typeof(TGroupKey);
            var param = Expression.Parameter(groupKeyType);
            Expression working = param;
            while (groupKeyType.GetGenericTypeDefinition() != typeof(PartitionKey<>))
            {
                // We have a CompoundGroupKey
                working = Expression.PropertyOrField(working, "outerGroup");
                groupKeyType = groupKeyType.GenericTypeArguments[0];
            }
            working = Expression.PropertyOrField(working, "Key");

            return Expression.Lambda<Func<TGroupKey, TPartition>>(working, param).Compile();
        }
    }
}