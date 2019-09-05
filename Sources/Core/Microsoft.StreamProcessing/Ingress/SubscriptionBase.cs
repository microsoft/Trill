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
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing.Internal
{
    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    /// <typeparam name="TIngressStructure"></typeparam>
    /// <typeparam name="TPayload"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class DisorderedObserverSubscriptionBase<TIngressStructure, TPayload, TResult> : DisorderedSubscriptionBase<TIngressStructure, TPayload, TResult>, IObserver<TIngressStructure>
    {
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        private readonly Func<IDisposable> primaryAction;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected DisorderedObserverSubscriptionBase() { }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="observable"></param>
        /// <param name="identifier"></param>
        /// <param name="streamable"></param>
        /// <param name="observer"></param>
        /// <param name="disorderPolicy"></param>
        /// <param name="punctuationPolicy"></param>
        /// <param name="onCompletedPolicy"></param>
        /// <param name="flushPolicy"></param>
        /// <param name="diagnosticOutput"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected DisorderedObserverSubscriptionBase(
            IObservable<TIngressStructure> observable,
            string identifier,
            IStreamable<Empty, TResult> streamable,
            IStreamObserver<Empty, TResult> observer,
            DisorderPolicy disorderPolicy,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderStreamEvent<TPayload>> diagnosticOutput) : base(
                identifier,
                streamable,
                observer,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                diagnosticOutput)
        {
            Contract.Requires(observable != null);

            this.primaryAction = () => observable.Subscribe(this);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected override IDisposable PrimaryAction() => this.primaryAction();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void Enable() => this.subscription.Enable();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="input"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public abstract void OnNext(TIngressStructure input);
    }

    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    /// <typeparam name="TIngressStructure"></typeparam>
    /// <typeparam name="TPayload"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class DisorderedSubscriptionBase<TIngressStructure, TPayload, TResult> : Pipe<Empty, TResult>, IIngressStreamObserver
    {
        private readonly string errorMessages;
        private new readonly bool isColumnar;

        [SchemaSerialization]
        private readonly string disorderString;
        [SchemaSerialization]
        private readonly string punctuationString;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        internal readonly DisorderPolicyType disorderPolicyType;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly long reorderLatency;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [SchemaSerialization]
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly FlushPolicy flushPolicy;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        internal readonly PeriodicPunctuationPolicyType punctuationPolicyType;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly ulong punctuationGenerationPeriod;

        [SchemaSerialization]
        [EditorBrowsable(EditorBrowsableState.Never)]
        private readonly OnCompletedPolicy onCompletedPolicy;

        [EditorBrowsable(EditorBrowsableState.Never)]
        private readonly MemoryPool<Empty, TResult> pool;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected StreamMessage<Empty, TResult> currentBatch;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected long currentTime = StreamEvent.MinSyncTime;

#if DEBUG
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// Used for debug purposes only - very close to currentTime, but is only updated due to real events/punctuations/low watermarks processed, and not the reorder buffer latency
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected long lastEventTime = StreamEvent.MinSyncTime;
#endif

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected Dictionary<Tuple<long, TResult>, ElasticCircularBuffer<AdjustInfo>> startEventInformation = new Dictionary<Tuple<long, TResult>, ElasticCircularBuffer<AdjustInfo>>(1);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        protected long lastPunctuationTime = StreamEvent.MinSyncTime;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected long highWatermark = 0;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected ImpatienceSorter<TResult> impatienceSorter = null;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected StreamEventPriorityQueue<TResult> priorityQueueSorter = null;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected IObserver<OutOfOrderStreamEvent<TPayload>> diagnosticOutput;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected DisorderedSubscriptionBase() { }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected DelayedSubscription subscription;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="identifier"></param>
        /// <param name="streamable"></param>
        /// <param name="observer"></param>
        /// <param name="disorderPolicy"></param>
        /// <param name="flushPolicy"></param>
        /// <param name="punctuationPolicy"></param>
        /// <param name="onCompletedPolicy"></param>
        /// <param name="diagnosticOutput"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected DisorderedSubscriptionBase(
            string identifier,
            IStreamable<Empty, TResult> streamable,
            IStreamObserver<Empty, TResult> observer,
            DisorderPolicy disorderPolicy,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderStreamEvent<TPayload>> diagnosticOutput)
            : base(streamable, observer)
        {
            Contract.Requires(observer != null);
            Contract.Requires(punctuationPolicy != null);

            this.IngressSiteIdentifier = identifier;
            this.disorderString = disorderPolicy.ToString();
            this.disorderPolicyType = disorderPolicy.type;
            this.reorderLatency = disorderPolicy == null ? 0 : disorderPolicy.reorderLatency;
            this.punctuationString = punctuationPolicy.ToString();
            this.punctuationPolicyType = punctuationPolicy.type;
            this.punctuationGenerationPeriod = punctuationPolicy == null ? 0 : punctuationPolicy.generationPeriod;
            this.onCompletedPolicy = onCompletedPolicy;
            this.flushPolicy = flushPolicy;
            this.diagnosticOutput = diagnosticOutput;

            if (Config.IngressSortingTechnique == SortingTechnique.PriorityQueue)
                this.priorityQueueSorter = new StreamEventPriorityQueue<TResult>();
            else
                this.impatienceSorter = new ImpatienceSorter<TResult>();

            this.isColumnar = streamable.Properties.IsColumnar;
            this.pool = MemoryManager.GetMemoryPool<Empty, TResult>(this.isColumnar);
            this.pool.Get(out this.currentBatch);
            this.currentBatch.Allocate();
            this.errorMessages = streamable.ErrorMessages;

            this.subscription = new DelayedSubscription(PrimaryAction);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected override void FlushContents()
        {
            if (this.currentBatch == null || this.currentBatch.Count == 0) return;
            this.currentBatch.Seal();
            this.Observer.OnNext(this.currentBatch);
            this.pool.Get(out this.currentBatch);
            this.currentBatch.Allocate();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="value"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected void OnPunctuation(StreamEvent<TResult> value)
        {
            Contract.Requires(value.IsPunctuation);

            this.lastPunctuationTime = Math.Max(
                value.SyncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod),
                this.lastPunctuationTime);

            var count = this.currentBatch.Count;
            this.currentBatch.vsync.col[count] = value.SyncTime;
            this.currentBatch.vother.col[count] = value.OtherTime;
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            this.currentBatch.key.col[count] = Empty.Default;
            this.currentBatch.hash.col[count] = 0;
            this.currentBatch[count] = default;
            this.currentBatch.Count = count + 1;

            if (this.flushPolicy == FlushPolicy.FlushOnPunctuation ||
                (this.flushPolicy == FlushPolicy.FlushOnBatchBoundary && this.currentBatch.Count == Config.DataBatchSize))
            {
                OnFlush();
            }
            else if (this.currentBatch.Count == Config.DataBatchSize)
            {
                FlushContents();
            }
        }

#if DEBUG
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected long LastEventTime()
        {
            return this.lastEventTime;
        }
#endif

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void OnCompleted()
        {
            if (this.onCompletedPolicy != OnCompletedPolicy.None)
            {
                OnCompleted(this.onCompletedPolicy == OnCompletedPolicy.Flush ? this.currentTime : StreamEvent.InfinitySyncTime);
            }

            base.OnCompleted();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="punctuationTime"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected abstract void OnCompleted(long punctuationTime);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="previous"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new IngressPlanNode(
                this,
                typeof(Empty), typeof(TPayload), this.isColumnar, this.errorMessages));

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public string IngressSiteIdentifier { get; private set; }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public IDisposable DelayedDisposable => this.subscription;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1063:ImplementIDisposableCorrectly")]
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected override void DisposeState()
        {
            this.subscription?.Dispose();
            this.impatienceSorter?.Dispose();
            this.currentBatch?.Free();
            this.currentBatch = null;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="managed"></param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1063:ImplementIDisposableCorrectly")]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual void Dispose(bool managed) => Dispose();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected abstract IDisposable PrimaryAction();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public abstract void Enable();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int CurrentlyBufferedStartEdgeCount => this.startEventInformation.Count;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override int CurrentlyBufferedOutputCount => this.currentBatch.Count;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override int CurrentlyBufferedInputCount => this.CurrentlyBufferedStartEdgeCount + this.CurrentlyBufferedReorderCount;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int CurrentlyBufferedReorderCount => (this.priorityQueueSorter != null ? this.priorityQueueSorter.Count() : 0) + (this.impatienceSorter != null ? this.impatienceSorter.Count() : 0);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        // needs to be a class so it can be updated while it is sitting in a queue
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataContract]
        protected sealed class AdjustInfo
        {
            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            [DataMember]
            public long modifiedStartTime;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            [DataMember]
            public uint numberOfOccurrences;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public AdjustInfo() { }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public AdjustInfo(long modifiedStartTime)
            {
                this.modifiedStartTime = modifiedStartTime;
                this.numberOfOccurrences = 1;
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="stream"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void Restore(Stream stream)
        {
            if (stream != null)
            {
                base.Restore(stream);
            }

            this.subscription.Enable();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected sealed class DelayedSubscription : IDisposable
        {
            private IDisposable inner;
            private readonly Func<IDisposable> func;
            private bool disposed;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="func"></param>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public DelayedSubscription(Func<IDisposable> func) => this.func = func;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public void Enable()
            {
                this.inner = this.func();
                if (this.disposed) DisposeInternal();
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public void Dispose()
            {
                if (this.inner != null) DisposeInternal();
                this.disposed = true;
            }

            private void DisposeInternal() => this.inner.Dispose();
        }
    }

    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    /// <typeparam name="TIngressStructure"></typeparam>
    /// <typeparam name="TPayload"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class ObserverSubscriptionBase<TIngressStructure, TPayload, TResult> : SubscriptionBase<TIngressStructure, TPayload, TResult>, IObserver<TIngressStructure>
    {
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        private readonly Func<IDisposable> primaryAction;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected ObserverSubscriptionBase() { }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="observable"></param>
        /// <param name="identifier"></param>
        /// <param name="streamable"></param>
        /// <param name="observer"></param>
        /// <param name="disorderPolicy"></param>
        /// <param name="punctuationPolicy"></param>
        /// <param name="onCompletedPolicy"></param>
        /// <param name="flushPolicy"></param>
        /// <param name="diagnosticOutput"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected ObserverSubscriptionBase(
            IObservable<TIngressStructure> observable,
            string identifier,
            IStreamable<Empty, TResult> streamable,
            IStreamObserver<Empty, TResult> observer,
            DisorderPolicy disorderPolicy,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderStreamEvent<TPayload>> diagnosticOutput) : base(
                identifier,
                streamable,
                observer,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                diagnosticOutput)
        {
            Contract.Requires(observable != null);

            this.primaryAction = () => observable.Subscribe(this);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected override IDisposable PrimaryAction() => this.primaryAction();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void Enable() => this.subscription.Enable();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="input"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public abstract void OnNext(TIngressStructure input);
    }

    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    /// <typeparam name="TIngressStructure"></typeparam>
    /// <typeparam name="TPayload"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class SubscriptionBase<TIngressStructure, TPayload, TResult> : Pipe<Empty, TResult>, IIngressStreamObserver
    {
        private readonly string errorMessages;
        private new readonly bool isColumnar;

        [SchemaSerialization]
        private readonly string disorderString;
        [SchemaSerialization]
        private readonly string punctuationString;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        internal readonly DisorderPolicyType disorderPolicyType;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly long reorderLatency;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [SchemaSerialization]
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly FlushPolicy flushPolicy;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        internal readonly PeriodicPunctuationPolicyType punctuationPolicyType;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly ulong punctuationGenerationPeriod;

        [SchemaSerialization]
        [EditorBrowsable(EditorBrowsableState.Never)]
        private readonly OnCompletedPolicy onCompletedPolicy;

        [EditorBrowsable(EditorBrowsableState.Never)]
        private readonly MemoryPool<Empty, TResult> pool;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected StreamMessage<Empty, TResult> currentBatch;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected long currentTime = StreamEvent.MinSyncTime;

#if DEBUG
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// Used for debug purposes only - very close to currentTime, but is only updated due to real events/punctuations/low watermarks processed, and not the reorder buffer latency
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected long lastEventTime = StreamEvent.MinSyncTime;
#endif

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected Dictionary<Tuple<long, TPayload>, ElasticCircularBuffer<AdjustInfo>> startEventInformation = new Dictionary<Tuple<long, TPayload>, ElasticCircularBuffer<AdjustInfo>>(1);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        protected long lastPunctuationTime = StreamEvent.MinSyncTime;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected long highWatermark = 0;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected ImpatienceSorter<TPayload> impatienceSorter = null;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected StreamEventPriorityQueue<TPayload> priorityQueueSorter = null;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected IObserver<OutOfOrderStreamEvent<TPayload>> diagnosticOutput;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected SubscriptionBase() { }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected DelayedSubscription subscription;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="identifier"></param>
        /// <param name="streamable"></param>
        /// <param name="observer"></param>
        /// <param name="disorderPolicy"></param>
        /// <param name="flushPolicy"></param>
        /// <param name="punctuationPolicy"></param>
        /// <param name="onCompletedPolicy"></param>
        /// <param name="diagnosticOutput"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected SubscriptionBase(
            string identifier,
            IStreamable<Empty, TResult> streamable,
            IStreamObserver<Empty, TResult> observer,
            DisorderPolicy disorderPolicy,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderStreamEvent<TPayload>> diagnosticOutput)
            : base(streamable, observer)
        {
            Contract.Requires(observer != null);
            Contract.Requires(punctuationPolicy != null);

            this.IngressSiteIdentifier = identifier;
            this.disorderString = disorderPolicy.ToString();
            this.disorderPolicyType = disorderPolicy.type;
            this.reorderLatency = disorderPolicy == null ? 0 : disorderPolicy.reorderLatency;
            this.punctuationString = punctuationPolicy.ToString();
            this.punctuationPolicyType = punctuationPolicy.type;
            this.punctuationGenerationPeriod = punctuationPolicy == null ? 0 : punctuationPolicy.generationPeriod;
            this.onCompletedPolicy = onCompletedPolicy;
            this.flushPolicy = flushPolicy;
            this.diagnosticOutput = diagnosticOutput;

            if (Config.IngressSortingTechnique == SortingTechnique.PriorityQueue)
                this.priorityQueueSorter = new StreamEventPriorityQueue<TPayload>();
            else
                this.impatienceSorter = new ImpatienceSorter<TPayload>();

            this.isColumnar = streamable.Properties.IsColumnar;
            this.pool = MemoryManager.GetMemoryPool<Empty, TResult>(this.isColumnar);
            this.pool.Get(out this.currentBatch);
            this.currentBatch.Allocate();
            this.errorMessages = streamable.ErrorMessages;

            this.subscription = new DelayedSubscription(PrimaryAction);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected override void FlushContents()
        {
            if (this.currentBatch == null || this.currentBatch.Count == 0) return;
            this.currentBatch.Seal();
            this.Observer.OnNext(this.currentBatch);
            this.pool.Get(out this.currentBatch);
            this.currentBatch.Allocate();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="value"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected void OnPunctuation(StreamEvent<TPayload> value)
        {
            Contract.Requires(value.IsPunctuation);

            this.lastPunctuationTime = Math.Max(
                value.SyncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod),
                this.lastPunctuationTime);

            var count = this.currentBatch.Count;
            this.currentBatch.vsync.col[count] = value.SyncTime;
            this.currentBatch.vother.col[count] = value.OtherTime;
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            this.currentBatch.key.col[count] = Empty.Default;
            this.currentBatch.hash.col[count] = 0;
            this.currentBatch[count] = default;
            this.currentBatch.Count = count + 1;

            if (this.flushPolicy == FlushPolicy.FlushOnPunctuation ||
                (this.flushPolicy == FlushPolicy.FlushOnBatchBoundary && this.currentBatch.Count == Config.DataBatchSize))
            {
                OnFlush();
            }
            else if (this.currentBatch.Count == Config.DataBatchSize)
            {
                FlushContents();
            }
        }

#if DEBUG
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected long LastEventTime()
        {
            return this.lastEventTime;
        }
#endif

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void OnCompleted()
        {
            if (this.onCompletedPolicy != OnCompletedPolicy.None)
            {
                OnCompleted(this.onCompletedPolicy == OnCompletedPolicy.Flush ? this.currentTime : StreamEvent.InfinitySyncTime);
            }

            base.OnCompleted();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="punctuationTime"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected abstract void OnCompleted(long punctuationTime);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="previous"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new IngressPlanNode(
                this,
                typeof(Empty), typeof(TPayload), this.isColumnar, this.errorMessages));

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public string IngressSiteIdentifier { get; private set; }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public IDisposable DelayedDisposable => this.subscription;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1063:ImplementIDisposableCorrectly")]
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected override void DisposeState()
        {
            this.subscription?.Dispose();
            this.impatienceSorter?.Dispose();
            this.currentBatch?.Free();
            this.currentBatch = null;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="managed"></param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1063:ImplementIDisposableCorrectly")]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual void Dispose(bool managed) => Dispose();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected abstract IDisposable PrimaryAction();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public abstract void Enable();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int CurrentlyBufferedStartEdgeCount => this.startEventInformation.Count;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override int CurrentlyBufferedOutputCount => this.currentBatch.Count;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override int CurrentlyBufferedInputCount => this.CurrentlyBufferedStartEdgeCount + this.CurrentlyBufferedReorderCount;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int CurrentlyBufferedReorderCount => (this.priorityQueueSorter != null ? this.priorityQueueSorter.Count() : 0) + (this.impatienceSorter != null ? this.impatienceSorter.Count() : 0);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        // needs to be a class so it can be updated while it is sitting in a queue
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataContract]
        protected sealed class AdjustInfo
        {
            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            [DataMember]
            public long modifiedStartTime;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            [DataMember]
            public uint numberOfOccurrences;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public AdjustInfo() { }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public AdjustInfo(long modifiedStartTime)
            {
                this.modifiedStartTime = modifiedStartTime;
                this.numberOfOccurrences = 1;
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="stream"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void Restore(Stream stream)
        {
            if (stream != null)
            {
                base.Restore(stream);
            }

            this.subscription.Enable();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected sealed class DelayedSubscription : IDisposable
        {
            private IDisposable inner;
            private readonly Func<IDisposable> func;
            private bool disposed;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="func"></param>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public DelayedSubscription(Func<IDisposable> func) => this.func = func;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public void Enable()
            {
                this.inner = this.func();
                if (this.disposed) DisposeInternal();
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public void Dispose()
            {
                if (this.inner != null) DisposeInternal();
                this.disposed = true;
            }

            private void DisposeInternal() => this.inner.Dispose();
        }
    }

    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TIngressStructure"></typeparam>
    /// <typeparam name="TPayload"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class DisorderedPartitionedObserverSubscriptionBase<TKey, TIngressStructure, TPayload, TResult> : DisorderedPartitionedSubscriptionBase<TKey, TIngressStructure, TPayload, TResult>, IObserver<TIngressStructure>
    {
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        private readonly Func<IDisposable> primaryAction;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected DisorderedPartitionedObserverSubscriptionBase() { }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="observable"></param>
        /// <param name="identifier"></param>
        /// <param name="streamable"></param>
        /// <param name="observer"></param>
        /// <param name="disorderPolicy"></param>
        /// <param name="punctuationPolicy"></param>
        /// <param name="lowWatermarkPolicy"></param>
        /// <param name="onCompletedPolicy"></param>
        /// <param name="flushPolicy"></param>
        /// <param name="diagnosticOutput"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected DisorderedPartitionedObserverSubscriptionBase(
            IObservable<TIngressStructure> observable,
            string identifier,
            IStreamable<PartitionKey<TKey>, TResult> streamable,
            IStreamObserver<PartitionKey<TKey>, TResult> observer,
            DisorderPolicy disorderPolicy,
            PartitionedFlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            PeriodicLowWatermarkPolicy lowWatermarkPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>> diagnosticOutput) : base(
                identifier,
                streamable,
                observer,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                diagnosticOutput)
        {
            Contract.Requires(observable != null);

            this.primaryAction = () => observable.Subscribe(this);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected override IDisposable PrimaryAction() => this.primaryAction();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void Enable() => this.subscription.Enable();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="input"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public abstract void OnNext(TIngressStructure input);
    }

    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TIngressStructure"></typeparam>
    /// <typeparam name="TPayload"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    [EditorBrowsable(EditorBrowsableState.Never)]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("StyleCop.CSharp.SpacingRules", "SA1008:OpeningParenthesisMustBeSpacedCorrectly", Justification = "ValueTuples")]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("StyleCop.CSharp.SpacingRules", "SA1009:ClosingParenthesisMustBeSpacedCorrectly", Justification = "ValueTuples")]
    public abstract class DisorderedPartitionedSubscriptionBase<TKey, TIngressStructure, TPayload, TResult> : Pipe<PartitionKey<TKey>, TResult>, IIngressStreamObserver
    {
        private readonly string errorMessages;
        private new readonly bool isColumnar;

        [SchemaSerialization]
        private readonly string disorderString;
        [SchemaSerialization]
        private readonly string punctuationString;
        [SchemaSerialization]
        private readonly string lowWatermarkString;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        internal readonly DisorderPolicyType disorderPolicyType;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly long reorderLatency;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [SchemaSerialization]
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly PartitionedFlushPolicy flushPolicy;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        internal readonly PeriodicPunctuationPolicyType punctuationPolicyType;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly ulong punctuationGenerationPeriod;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        internal readonly PeriodicLowWatermarkPolicyType lowWatermarkPolicyType;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly ulong lowWatermarkGenerationPeriod;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly long lowWatermarkTimestampLag;

        [SchemaSerialization]
        [EditorBrowsable(EditorBrowsableState.Never)]
        private readonly OnCompletedPolicy onCompletedPolicy;

        [EditorBrowsable(EditorBrowsableState.Never)]
        private readonly MemoryPool<PartitionKey<TKey>, TResult> pool;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected StreamMessage<PartitionKey<TKey>, TResult> currentBatch;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected Dictionary<TKey, long> currentTime = new Dictionary<TKey, long>();

#if DEBUG
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// Used for debug purposes only - very close to currentTime, but is only updated due to real events/punctuations/low watermarks processed, and not the reorder buffer latency
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected Dictionary<TKey, long> lastEventTime = new Dictionary<TKey, long>();
#endif

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected Dictionary<Tuple<long, TResult>, ElasticCircularBuffer<AdjustInfo>> startEventInformation = new Dictionary<Tuple<long, TResult>, ElasticCircularBuffer<AdjustInfo>>(1);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        protected Dictionary<TKey, (long lastPunctuation, long lastPunctuationQuantized)> lastPunctuationTime = new Dictionary<TKey, (long, long)>();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected long highWatermark = 0;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// Tracks each partition's high watermark, which, unlike currentTime, does not include the reorderLatency.
        /// This is only used for partitioned streams with latency (i.e. nonzero reorderLatency)
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected Dictionary<TKey, long> partitionHighWatermarks = new Dictionary<TKey, long>();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// Inverse of partitionHighWatermarks - maps highWatermark times to sets of partitions that have that value as their high watermark.
        /// This is only used for partitioned streams with latency (i.e. nonzero reorderLatency)
        /// NB: Do not mark as DataMember or as state managed: this is an inversion of existing data in field partitionHighWatermarks.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected SortedDictionary<long, HashSet<TKey>> highWatermarkToPartitionsMap = new SortedDictionary<long, HashSet<TKey>>();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// Tracks the three possible low watermark values:
        /// 1. rawValue - the raw low watermark value
        /// 2. quantizedForLowWatermarkGeneration - raw low watermark value quantized for low watermark generation
        /// 2. quanitzedForPunctuationGeneration - raw low watermark value quantized for punctuation generation
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected (long rawValue, long quantizedForLowWatermarkGeneration, long quantizedForPunctuationGeneration) lowWatermark = (0, 0, 0);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// Baseline low watermark value used for low watermark and punctuation generation policies. This value will be
        /// quantized to lowWatermarkGenerationPeriod boundaries.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected long baselineLowWatermarkForPolicy = 0;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected PartitionedImpatienceSorter<TKey, TResult> impatienceSorter = null;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected PartitionedStreamEventPriorityQueue<TKey, TResult> priorityQueueSorter = null;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>> diagnosticOutput;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected DisorderedPartitionedSubscriptionBase() { }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected DelayedSubscription subscription;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="identifier"></param>
        /// <param name="streamable"></param>
        /// <param name="observer"></param>
        /// <param name="disorderPolicy"></param>
        /// <param name="flushPolicy"></param>
        /// <param name="punctuationPolicy"></param>
        /// <param name="lowWatermarkPolicy"></param>
        /// <param name="onCompletedPolicy"></param>
        /// <param name="diagnosticOutput"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected DisorderedPartitionedSubscriptionBase(
            string identifier,
            IStreamable<PartitionKey<TKey>, TResult> streamable,
            IStreamObserver<PartitionKey<TKey>, TResult> observer,
            DisorderPolicy disorderPolicy,
            PartitionedFlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            PeriodicLowWatermarkPolicy lowWatermarkPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>> diagnosticOutput)
            : base(streamable, observer)
        {
            Contract.Requires(observer != null);
            Contract.Requires(punctuationPolicy != null);
            Contract.Requires(lowWatermarkPolicy != null);

            this.IngressSiteIdentifier = identifier;
            this.disorderString = disorderPolicy.ToString();
            this.disorderPolicyType = disorderPolicy.type;
            this.reorderLatency = disorderPolicy == null ? 0 : disorderPolicy.reorderLatency;
            this.punctuationString = punctuationPolicy.ToString();
            this.punctuationPolicyType = punctuationPolicy.type;
            this.punctuationGenerationPeriod = punctuationPolicy == null ? 0 : punctuationPolicy.generationPeriod;
            this.lowWatermarkString = lowWatermarkPolicy.ToString();
            this.lowWatermarkPolicyType = lowWatermarkPolicy.type;
            this.lowWatermarkGenerationPeriod = lowWatermarkPolicy == null ? 0 : lowWatermarkPolicy.generationPeriod;
            this.lowWatermarkTimestampLag = lowWatermarkPolicy == null ? 0 : lowWatermarkPolicy.lowWatermarkTimestampLag;
            this.onCompletedPolicy = onCompletedPolicy;
            this.flushPolicy = flushPolicy;
            this.diagnosticOutput = diagnosticOutput;

            if (Config.IngressSortingTechnique == SortingTechnique.PriorityQueue)
                this.priorityQueueSorter = new PartitionedStreamEventPriorityQueue<TKey, TResult>();
            else
                this.impatienceSorter = new PartitionedImpatienceSorter<TKey, TResult>();

            this.isColumnar = streamable.Properties.IsColumnar;
            this.pool = MemoryManager.GetMemoryPool<PartitionKey<TKey>, TResult>(this.isColumnar);
            this.pool.Get(out this.currentBatch);
            this.currentBatch.Allocate();
            this.errorMessages = streamable.ErrorMessages;

            this.subscription = new DelayedSubscription(PrimaryAction);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected override void FlushContents()
        {
            if (this.currentBatch == null || this.currentBatch.Count == 0) return;
            this.currentBatch.Seal();
            this.Observer.OnNext(this.currentBatch);
            this.pool.Get(out this.currentBatch);
            this.currentBatch.Allocate();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="value"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected void OnPunctuation(PartitionedStreamEvent<TKey, TResult> value)
        {
            Contract.Requires(value.IsPunctuation);

            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                UpdatePunctuation(value.PartitionKey, value.SyncTime);
            }

            var count = this.currentBatch.Count;
            this.currentBatch.Add(value.SyncTime, value.OtherTime, new PartitionKey<TKey>(value.PartitionKey), default);
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            if (this.currentBatch.Count == Config.DataBatchSize)
            {
                if (this.flushPolicy == PartitionedFlushPolicy.FlushOnBatchBoundary) OnFlush();
                else FlushContents();
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="time"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void UpdatePunctuation(TKey partitionKey, long time)
        {
            this.lastPunctuationTime[partitionKey] = (time, time.SnapToLeftBoundary((long)this.punctuationGenerationPeriod));
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="time"></param>
        /// <param name="timeQuantized"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void UpdatePunctuation(TKey partitionKey, long time, long timeQuantized)
        {
            this.lastPunctuationTime[partitionKey] = (time, timeQuantized);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="time"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void UpdateLowWatermark(long time)
        {
            this.lowWatermark.rawValue = time;
            if (this.lowWatermarkPolicyType == PeriodicLowWatermarkPolicyType.Time)
            {
                this.lowWatermark.quantizedForLowWatermarkGeneration = time.SnapToLeftBoundary((long)this.lowWatermarkGenerationPeriod);
            }
            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                this.lowWatermark.quantizedForPunctuationGeneration = time.SnapToLeftBoundary((long)this.punctuationGenerationPeriod);
            }
        }

#if DEBUG
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="partitionKey"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected long LastEventTime(TKey partitionKey)
        {
            return this.lastEventTime.ContainsKey(partitionKey) ? this.lastEventTime[partitionKey] : 0;
        }
#endif

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void OnCompleted()
        {
            if (this.onCompletedPolicy != OnCompletedPolicy.None)
            {
                OnCompleted(this.onCompletedPolicy == OnCompletedPolicy.Flush ? this.currentTime.Values.Max() : StreamEvent.InfinitySyncTime);
            }

            base.OnCompleted();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="punctuationTime"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected abstract void OnCompleted(long punctuationTime);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="previous"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new IngressPlanNode(
                this,
                typeof(PartitionKey<TKey>), typeof(TPayload), this.isColumnar, this.errorMessages));

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public string IngressSiteIdentifier { get; private set; }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public IDisposable DelayedDisposable => this.subscription;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1063:ImplementIDisposableCorrectly")]
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected override void DisposeState()
        {
            this.subscription?.Dispose();
            this.impatienceSorter?.Dispose();
            this.currentBatch?.Free();
            this.currentBatch = null;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="managed"></param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1063:ImplementIDisposableCorrectly")]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual void Dispose(bool managed) => Dispose();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected abstract IDisposable PrimaryAction();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public abstract void Enable();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int CurrentlyBufferedStartEdgeCount => this.startEventInformation.Count;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override int CurrentlyBufferedOutputCount => this.currentBatch.Count;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override int CurrentlyBufferedInputCount => this.CurrentlyBufferedStartEdgeCount + this.CurrentlyBufferedReorderCount;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int CurrentlyBufferedReorderCount => (this.priorityQueueSorter != null ? this.priorityQueueSorter.Count() : 0) + (this.impatienceSorter != null ? this.impatienceSorter.Count() : 0);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        // needs to be a class so it can be updated while it is sitting in a queue
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataContract]
        protected sealed class AdjustInfo
        {
            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            [DataMember]
            public long modifiedStartTime;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            [DataMember]
            public uint numberOfOccurrences;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public AdjustInfo() { }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public AdjustInfo(long modifiedStartTime)
            {
                this.modifiedStartTime = modifiedStartTime;
                this.numberOfOccurrences = 1;
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="stream"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void Restore(Stream stream)
        {
            if (stream != null)
            {
                base.Restore(stream);
            }

            this.subscription.Enable();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected sealed class DelayedSubscription : IDisposable
        {
            private IDisposable inner;
            private readonly Func<IDisposable> func;
            private bool disposed;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="func"></param>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public DelayedSubscription(Func<IDisposable> func) => this.func = func;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public void Enable()
            {
                this.inner = this.func();
                if (this.disposed) DisposeInternal();
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public void Dispose()
            {
                if (this.inner != null) DisposeInternal();
                this.disposed = true;
            }

            private void DisposeInternal() => this.inner.Dispose();
        }
    }

    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TIngressStructure"></typeparam>
    /// <typeparam name="TPayload"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class PartitionedObserverSubscriptionBase<TKey, TIngressStructure, TPayload, TResult> : PartitionedSubscriptionBase<TKey, TIngressStructure, TPayload, TResult>, IObserver<TIngressStructure>
    {
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        private readonly Func<IDisposable> primaryAction;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected PartitionedObserverSubscriptionBase() { }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="observable"></param>
        /// <param name="identifier"></param>
        /// <param name="streamable"></param>
        /// <param name="observer"></param>
        /// <param name="disorderPolicy"></param>
        /// <param name="punctuationPolicy"></param>
        /// <param name="lowWatermarkPolicy"></param>
        /// <param name="onCompletedPolicy"></param>
        /// <param name="flushPolicy"></param>
        /// <param name="diagnosticOutput"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected PartitionedObserverSubscriptionBase(
            IObservable<TIngressStructure> observable,
            string identifier,
            IStreamable<PartitionKey<TKey>, TResult> streamable,
            IStreamObserver<PartitionKey<TKey>, TResult> observer,
            DisorderPolicy disorderPolicy,
            PartitionedFlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            PeriodicLowWatermarkPolicy lowWatermarkPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>> diagnosticOutput) : base(
                identifier,
                streamable,
                observer,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                lowWatermarkPolicy,
                onCompletedPolicy,
                diagnosticOutput)
        {
            Contract.Requires(observable != null);

            this.primaryAction = () => observable.Subscribe(this);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected override IDisposable PrimaryAction() => this.primaryAction();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void Enable() => this.subscription.Enable();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="input"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public abstract void OnNext(TIngressStructure input);
    }

    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TIngressStructure"></typeparam>
    /// <typeparam name="TPayload"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    [EditorBrowsable(EditorBrowsableState.Never)]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("StyleCop.CSharp.SpacingRules", "SA1008:OpeningParenthesisMustBeSpacedCorrectly", Justification = "ValueTuples")]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("StyleCop.CSharp.SpacingRules", "SA1009:ClosingParenthesisMustBeSpacedCorrectly", Justification = "ValueTuples")]
    public abstract class PartitionedSubscriptionBase<TKey, TIngressStructure, TPayload, TResult> : Pipe<PartitionKey<TKey>, TResult>, IIngressStreamObserver
    {
        private readonly string errorMessages;
        private new readonly bool isColumnar;

        [SchemaSerialization]
        private readonly string disorderString;
        [SchemaSerialization]
        private readonly string punctuationString;
        [SchemaSerialization]
        private readonly string lowWatermarkString;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        internal readonly DisorderPolicyType disorderPolicyType;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly long reorderLatency;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [SchemaSerialization]
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly PartitionedFlushPolicy flushPolicy;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        internal readonly PeriodicPunctuationPolicyType punctuationPolicyType;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly ulong punctuationGenerationPeriod;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        internal readonly PeriodicLowWatermarkPolicyType lowWatermarkPolicyType;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly ulong lowWatermarkGenerationPeriod;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly long lowWatermarkTimestampLag;

        [SchemaSerialization]
        [EditorBrowsable(EditorBrowsableState.Never)]
        private readonly OnCompletedPolicy onCompletedPolicy;

        [EditorBrowsable(EditorBrowsableState.Never)]
        private readonly MemoryPool<PartitionKey<TKey>, TResult> pool;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected StreamMessage<PartitionKey<TKey>, TResult> currentBatch;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected Dictionary<TKey, long> currentTime = new Dictionary<TKey, long>();

#if DEBUG
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// Used for debug purposes only - very close to currentTime, but is only updated due to real events/punctuations/low watermarks processed, and not the reorder buffer latency
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected Dictionary<TKey, long> lastEventTime = new Dictionary<TKey, long>();
#endif

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected Dictionary<Tuple<long, TPayload>, ElasticCircularBuffer<AdjustInfo>> startEventInformation = new Dictionary<Tuple<long, TPayload>, ElasticCircularBuffer<AdjustInfo>>(1);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        protected Dictionary<TKey, (long lastPunctuation, long lastPunctuationQuantized)> lastPunctuationTime = new Dictionary<TKey, (long, long)>();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected long highWatermark = 0;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// Tracks each partition's high watermark, which, unlike currentTime, does not include the reorderLatency.
        /// This is only used for partitioned streams with latency (i.e. nonzero reorderLatency)
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected Dictionary<TKey, long> partitionHighWatermarks = new Dictionary<TKey, long>();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// Inverse of partitionHighWatermarks - maps highWatermark times to sets of partitions that have that value as their high watermark.
        /// This is only used for partitioned streams with latency (i.e. nonzero reorderLatency)
        /// NB: Do not mark as DataMember or as state managed: this is an inversion of existing data in field partitionHighWatermarks.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected SortedDictionary<long, HashSet<TKey>> highWatermarkToPartitionsMap = new SortedDictionary<long, HashSet<TKey>>();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// Tracks the three possible low watermark values:
        /// 1. rawValue - the raw low watermark value
        /// 2. quantizedForLowWatermarkGeneration - raw low watermark value quantized for low watermark generation
        /// 2. quanitzedForPunctuationGeneration - raw low watermark value quantized for punctuation generation
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected (long rawValue, long quantizedForLowWatermarkGeneration, long quantizedForPunctuationGeneration) lowWatermark = (0, 0, 0);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// Baseline low watermark value used for low watermark and punctuation generation policies. This value will be
        /// quantized to lowWatermarkGenerationPeriod boundaries.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected long baselineLowWatermarkForPolicy = 0;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected PartitionedImpatienceSorter<TKey, TPayload> impatienceSorter = null;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataMember]
        protected PartitionedStreamEventPriorityQueue<TKey, TPayload> priorityQueueSorter = null;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>> diagnosticOutput;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected PartitionedSubscriptionBase() { }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected DelayedSubscription subscription;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="identifier"></param>
        /// <param name="streamable"></param>
        /// <param name="observer"></param>
        /// <param name="disorderPolicy"></param>
        /// <param name="flushPolicy"></param>
        /// <param name="punctuationPolicy"></param>
        /// <param name="lowWatermarkPolicy"></param>
        /// <param name="onCompletedPolicy"></param>
        /// <param name="diagnosticOutput"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected PartitionedSubscriptionBase(
            string identifier,
            IStreamable<PartitionKey<TKey>, TResult> streamable,
            IStreamObserver<PartitionKey<TKey>, TResult> observer,
            DisorderPolicy disorderPolicy,
            PartitionedFlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            PeriodicLowWatermarkPolicy lowWatermarkPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>> diagnosticOutput)
            : base(streamable, observer)
        {
            Contract.Requires(observer != null);
            Contract.Requires(punctuationPolicy != null);
            Contract.Requires(lowWatermarkPolicy != null);

            this.IngressSiteIdentifier = identifier;
            this.disorderString = disorderPolicy.ToString();
            this.disorderPolicyType = disorderPolicy.type;
            this.reorderLatency = disorderPolicy == null ? 0 : disorderPolicy.reorderLatency;
            this.punctuationString = punctuationPolicy.ToString();
            this.punctuationPolicyType = punctuationPolicy.type;
            this.punctuationGenerationPeriod = punctuationPolicy == null ? 0 : punctuationPolicy.generationPeriod;
            this.lowWatermarkString = lowWatermarkPolicy.ToString();
            this.lowWatermarkPolicyType = lowWatermarkPolicy.type;
            this.lowWatermarkGenerationPeriod = lowWatermarkPolicy == null ? 0 : lowWatermarkPolicy.generationPeriod;
            this.lowWatermarkTimestampLag = lowWatermarkPolicy == null ? 0 : lowWatermarkPolicy.lowWatermarkTimestampLag;
            this.onCompletedPolicy = onCompletedPolicy;
            this.flushPolicy = flushPolicy;
            this.diagnosticOutput = diagnosticOutput;

            if (Config.IngressSortingTechnique == SortingTechnique.PriorityQueue)
                this.priorityQueueSorter = new PartitionedStreamEventPriorityQueue<TKey, TPayload>();
            else
                this.impatienceSorter = new PartitionedImpatienceSorter<TKey, TPayload>();

            this.isColumnar = streamable.Properties.IsColumnar;
            this.pool = MemoryManager.GetMemoryPool<PartitionKey<TKey>, TResult>(this.isColumnar);
            this.pool.Get(out this.currentBatch);
            this.currentBatch.Allocate();
            this.errorMessages = streamable.ErrorMessages;

            this.subscription = new DelayedSubscription(PrimaryAction);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected override void FlushContents()
        {
            if (this.currentBatch == null || this.currentBatch.Count == 0) return;
            this.currentBatch.Seal();
            this.Observer.OnNext(this.currentBatch);
            this.pool.Get(out this.currentBatch);
            this.currentBatch.Allocate();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="value"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected void OnPunctuation(PartitionedStreamEvent<TKey, TPayload> value)
        {
            Contract.Requires(value.IsPunctuation);

            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                UpdatePunctuation(value.PartitionKey, value.SyncTime);
            }

            var count = this.currentBatch.Count;
            this.currentBatch.Add(value.SyncTime, value.OtherTime, new PartitionKey<TKey>(value.PartitionKey), default);
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            if (this.currentBatch.Count == Config.DataBatchSize)
            {
                if (this.flushPolicy == PartitionedFlushPolicy.FlushOnBatchBoundary) OnFlush();
                else FlushContents();
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="time"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void UpdatePunctuation(TKey partitionKey, long time)
        {
            this.lastPunctuationTime[partitionKey] = (time, time.SnapToLeftBoundary((long)this.punctuationGenerationPeriod));
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="time"></param>
        /// <param name="timeQuantized"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void UpdatePunctuation(TKey partitionKey, long time, long timeQuantized)
        {
            this.lastPunctuationTime[partitionKey] = (time, timeQuantized);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="time"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void UpdateLowWatermark(long time)
        {
            this.lowWatermark.rawValue = time;
            if (this.lowWatermarkPolicyType == PeriodicLowWatermarkPolicyType.Time)
            {
                this.lowWatermark.quantizedForLowWatermarkGeneration = time.SnapToLeftBoundary((long)this.lowWatermarkGenerationPeriod);
            }
            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                this.lowWatermark.quantizedForPunctuationGeneration = time.SnapToLeftBoundary((long)this.punctuationGenerationPeriod);
            }
        }

#if DEBUG
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="partitionKey"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected long LastEventTime(TKey partitionKey)
        {
            return this.lastEventTime.ContainsKey(partitionKey) ? this.lastEventTime[partitionKey] : 0;
        }
#endif

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void OnCompleted()
        {
            if (this.onCompletedPolicy != OnCompletedPolicy.None)
            {
                OnCompleted(this.onCompletedPolicy == OnCompletedPolicy.Flush ? this.currentTime.Values.Max() : StreamEvent.InfinitySyncTime);
            }

            base.OnCompleted();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="punctuationTime"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected abstract void OnCompleted(long punctuationTime);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="previous"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new IngressPlanNode(
                this,
                typeof(PartitionKey<TKey>), typeof(TPayload), this.isColumnar, this.errorMessages));

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public string IngressSiteIdentifier { get; private set; }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public IDisposable DelayedDisposable => this.subscription;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1063:ImplementIDisposableCorrectly")]
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected override void DisposeState()
        {
            this.subscription?.Dispose();
            this.impatienceSorter?.Dispose();
            this.currentBatch?.Free();
            this.currentBatch = null;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="managed"></param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1063:ImplementIDisposableCorrectly")]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual void Dispose(bool managed) => Dispose();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected abstract IDisposable PrimaryAction();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public abstract void Enable();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int CurrentlyBufferedStartEdgeCount => this.startEventInformation.Count;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override int CurrentlyBufferedOutputCount => this.currentBatch.Count;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override int CurrentlyBufferedInputCount => this.CurrentlyBufferedStartEdgeCount + this.CurrentlyBufferedReorderCount;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int CurrentlyBufferedReorderCount => (this.priorityQueueSorter != null ? this.priorityQueueSorter.Count() : 0) + (this.impatienceSorter != null ? this.impatienceSorter.Count() : 0);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        // needs to be a class so it can be updated while it is sitting in a queue
        [EditorBrowsable(EditorBrowsableState.Never)]
        [DataContract]
        protected sealed class AdjustInfo
        {
            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            [DataMember]
            public long modifiedStartTime;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            [DataMember]
            public uint numberOfOccurrences;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public AdjustInfo() { }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public AdjustInfo(long modifiedStartTime)
            {
                this.modifiedStartTime = modifiedStartTime;
                this.numberOfOccurrences = 1;
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="stream"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void Restore(Stream stream)
        {
            if (stream != null)
            {
                base.Restore(stream);
            }

            this.subscription.Enable();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected sealed class DelayedSubscription : IDisposable
        {
            private IDisposable inner;
            private readonly Func<IDisposable> func;
            private bool disposed;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="func"></param>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public DelayedSubscription(Func<IDisposable> func) => this.func = func;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public void Enable()
            {
                this.inner = this.func();
                if (this.disposed) DisposeInternal();
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public void Dispose()
            {
                if (this.inner != null) DisposeInternal();
                this.disposed = true;
            }

            private void DisposeInternal() => this.inner.Dispose();
        }
    }

}