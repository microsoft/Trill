// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.ComponentModel;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing.Internal
{
    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class EgressBoundary<TKey, TPayload, TResult> : Checkpointable, IStreamObserver<TKey, TPayload>, IEgressStreamObserver
    {
        private string identifier;
        private Process activeProcess;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        protected readonly IObserver<TResult> observer;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [Obsolete("Used only by serialization. Do not call directly.")]
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected EgressBoundary() : base(false, null) { }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="observer"></param>
        /// <param name="container"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected EgressBoundary(IObserver<TResult> observer, QueryContainer container) : base(false, container)
            => this.observer = observer;

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
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="previous"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual void ProduceQueryPlan(PlanNode previous)
            => this.activeProcess.RegisterQueryPlan(this.identifier, new EgressPlanNode(
                previous, this, typeof(TKey), typeof(TPayload), false, null));

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="batch"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public abstract void OnNext(StreamMessage<TKey, TPayload> batch);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="error"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void OnError(Exception error)
        {
            if (this is IDisposable disposable) disposable.Dispose();
            this.observer.OnError(error);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual void OnCompleted()
        {
            if (this is IDisposable disposable) disposable.Dispose();
            this.observer.OnCompleted();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual void OnFlush() { }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="identifier"></param>
        /// <param name="p"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void AttachProcess(string identifier, Process p)
        {
            this.identifier = identifier;
            this.activeProcess = p;
        }
    }
}
