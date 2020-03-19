// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.IO;

namespace Microsoft.StreamProcessing
{
    internal sealed class FusedObservablePipe<TInput> : IObserver<TInput>, IIngressStreamObserver, IEgressStreamObserver
    {
        private readonly Action onCompleted;
        private readonly Action<Exception> onError;
        private readonly Action<TInput> onNext;
        private string egressIdentifier;
        private Process activeProcess;

        public FusedObservablePipe(Action onCompleted, Action<Exception> onError, Action<TInput> onNext, string ingressIdentifier)
        {
            this.onCompleted = onCompleted;
            this.onError = onError;
            this.onNext = onNext;
            this.IngressSiteIdentifier = ingressIdentifier;
        }

        public Guid ClassId { get; } = Guid.NewGuid();

        public int CurrentlyBufferedOutputCount => 0;

        public int CurrentlyBufferedInputCount => 0;

        public int CurrentlyBufferedReorderCount => 0;

        public int CurrentlyBufferedStartEdgeCount => 0;

        public string IngressSiteIdentifier { get; }

        public IDisposable DelayedDisposable => Utility.EmptyDisposable;

        public void AttachProcess(string identifier, Process p)
        {
            this.egressIdentifier = identifier;
            this.activeProcess = p;
        }

        public void Checkpoint(Stream stream) { }

        public void Dispose() { }

        public void OnFlush() { }

        public void Enable() { }

        public void OnCompleted() => this.onCompleted();

        public void OnError(Exception error) => this.onError(error);

        public void OnNext(TInput value) => this.onNext(value);

        public void ProduceQueryPlan(PlanNode previous)
            => this.activeProcess.RegisterQueryPlan(this.egressIdentifier, new FusedPlanNode(
                this, typeof(Empty), typeof(TInput), false, null));

        public void Reset() { }

        public void Restore(Stream stream) { }
    }
}
