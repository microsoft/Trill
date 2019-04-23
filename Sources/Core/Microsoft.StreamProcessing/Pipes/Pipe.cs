// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.ComponentModel;
using System.Diagnostics.Contracts;
using System.IO;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
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