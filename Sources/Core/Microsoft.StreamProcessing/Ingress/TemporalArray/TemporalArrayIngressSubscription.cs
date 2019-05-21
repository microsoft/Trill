// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
/*\
    * Spec:
    *
    * Apply punctuation policy first. This means that even dropped events
    * count towards the number of events seen since the last punctuation.
    * For a Time punctuation policy, insert the punctuation before the
    * event whose start time causes the insertion.
    *
    * Most complicated part is the bookkeeping. If the disorder policy is
    * not Throw, then any events that are dropped or adjusted must be kept
    * track of so that matching end events are dealt with.
    *
    * DisorderPolicy.Throw:
    *   Don't drop any events.
    *
    * DisorderPolicy.Drop:
    *   Start event: drop if it is out of order and remember it so corresponding
    *   end event is also dropped.
    *   End event: drop if its corresponding start event was dropped or if it is out
    *   of order (latter may leave dangling start events that never have an end event).
    *   Interval event: drop if it is out of order
    *   Punctuation event: ??
    *
    * DisorderPolicy.Adjust:
    *   Start event: if out of order, update start time to start time of previous event.
    *   Remember event so corresponding end event is also modified.
    *   End event: if out of order, update start time to start time of previous event.
    *   If not out of order, check to see if corresponding start event had been modified.
    *   If so, then update its end time to the modified start time of the start event.
    *   (This is needed so that its end time matches the start time of the corresponding
    *   start event.)
    *   Interval event: if out of order, update start time to start time of previous event.
    *   If its updated start time is now equal to or greater than its end time, drop it.
\*/

using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;

namespace Microsoft.StreamProcessing
{
    internal static class StreamEventArraySubscriptionCreator<TPayload>
    {
        public static ObserverSubscriptionBase<ArraySegment<StreamEvent<TPayload>>, TPayload, TPayload> CreateSubscription(
            IObservable<ArraySegment<StreamEvent<TPayload>>> observable,
            string identifier,
            Streamable<Empty, TPayload> streamable,
            IStreamObserver<Empty, TPayload> observer,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderStreamEvent<TPayload>> diagnosticOutput)
        {
            return new StreamEventArraySubscriptionThrowNone<TPayload>(observable, identifier, streamable, observer, onCompletedPolicy, diagnosticOutput);
        }
    }

    internal static class IntervalArraySubscriptionCreator<TPayload>
    {
        public static ObserverSubscriptionBase<ArraySegment<TPayload>, TPayload, TPayload> CreateSubscription(
            IObservable<ArraySegment<TPayload>> observable,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            string identifier,
            Streamable<Empty, TPayload> streamable,
            IStreamObserver<Empty, TPayload> observer,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderStreamEvent<TPayload>> diagnosticOutput)
        {
            return new IntervalArraySubscriptionThrowNone<TPayload>(observable, startEdgeExtractor, endEdgeExtractor, identifier, streamable, observer, onCompletedPolicy, diagnosticOutput);
        }
    }

    internal static class PartitionedStreamEventArraySubscriptionCreator<TKey, TPayload>
    {
        public static PartitionedObserverSubscriptionBase<TKey, ArraySegment<PartitionedStreamEvent<TKey, TPayload>>, TPayload, TPayload> CreateSubscription(
            IObservable<ArraySegment<PartitionedStreamEvent<TKey, TPayload>>> observable,
            string identifier,
            Streamable<PartitionKey<TKey>, TPayload> streamable,
            IStreamObserver<PartitionKey<TKey>, TPayload> observer,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>> diagnosticOutput)
        {
            return new PartitionedStreamEventArraySubscriptionThrowNone<TKey, TPayload>(observable, identifier, streamable, observer, onCompletedPolicy, diagnosticOutput);
        }
    }

    internal static class PartitionedIntervalArraySubscriptionCreator<TKey, TPayload>
    {
        public static PartitionedObserverSubscriptionBase<TKey, ArraySegment<TPayload>, TPayload, TPayload> CreateSubscription(
            IObservable<ArraySegment<TPayload>> observable,
            Expression<Func<TPayload, TKey>> partitionExtractor,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            string identifier,
            Streamable<PartitionKey<TKey>, TPayload> streamable,
            IStreamObserver<PartitionKey<TKey>, TPayload> observer,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>> diagnosticOutput)
        {
            return new PartitionedIntervalArraySubscriptionThrowNone<TKey, TPayload>(observable, partitionExtractor, startEdgeExtractor, endEdgeExtractor, identifier, streamable, observer, onCompletedPolicy, diagnosticOutput);
        }
    }

    [DataContract]
    internal sealed class StreamEventArraySubscriptionThrowNone<TPayload> : ObserverSubscriptionBase<ArraySegment<StreamEvent<TPayload>>, TPayload, TPayload>
    {
        public StreamEventArraySubscriptionThrowNone() { }

        public StreamEventArraySubscriptionThrowNone(
            IObservable<ArraySegment<StreamEvent<TPayload>>> observable,
            string identifier,
            Streamable<Empty, TPayload> streamable,
            IStreamObserver<Empty, TPayload> observer,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderStreamEvent<TPayload>> diagnosticOutput)
            : base(
                observable,
                identifier,
                streamable,
                observer,
                DisorderPolicy.Throw(),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.None(),
                onCompletedPolicy,
                diagnosticOutput)
        {
        }

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(this.startEventInformation != null);
            Contract.Invariant(this.currentTime >= StreamEvent.MinSyncTime);
        }

        public override void OnNext(ArraySegment<StreamEvent<TPayload>> value)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            int n = value.Count + value.Offset;
            if (n > value.Array.Length)
            {
                throw new IngressException(
                    $"Invalid array segment. Offset: {value.Offset} Count: {value.Count} Length: {value.Array.Length}");
            }

            int offset = value.Offset;

            while (offset < n)
            {
                var full = this.currentBatch.Add(
                    value,
                        ref this.currentTime,
                    ref offset,
                    out bool hitAPunctuation);
                if (hitAPunctuation)
                {
                    var current = value.Array[offset];
                    if (current.SyncTime < this.currentTime) current = StreamEvent.CreatePunctuation<TPayload>(this.currentTime);
                    Array.Clear(this.currentBatch.hash.col, 0, this.currentBatch.hash.col.Length);
                    OnPunctuation(current);
                    offset++;
                }
                else if (full)
                {
                    Array.Clear(this.currentBatch.hash.col, 0, this.currentBatch.hash.col.Length);
                    FlushContents();
                }
            }
        }

        protected override void OnCompleted(long punctuationTime)
        {
            OnNext(new ArraySegment<StreamEvent<TPayload>>(new[] { StreamEvent.CreatePunctuation<TPayload>(punctuationTime) }));
        }
    }

    [DataContract]
    internal sealed class IntervalArraySubscriptionThrowNone<TPayload> : ObserverSubscriptionBase<ArraySegment<TPayload>, TPayload, TPayload>
    {
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, long>> startEdgeExtractor;
        private readonly Func<TPayload, long> startEdgeFunction;
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, long>> endEdgeExtractor;
        private readonly Func<TPayload, long> endEdgeFunction;
        public IntervalArraySubscriptionThrowNone() { }

        public IntervalArraySubscriptionThrowNone(
            IObservable<ArraySegment<TPayload>> observable,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            string identifier,
            Streamable<Empty, TPayload> streamable,
            IStreamObserver<Empty, TPayload> observer,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderStreamEvent<TPayload>> diagnosticOutput)
            : base(
                observable,
                identifier,
                streamable,
                observer,
                DisorderPolicy.Throw(),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.None(),
                onCompletedPolicy,
                diagnosticOutput)
        {
            this.startEdgeExtractor = startEdgeExtractor;
            this.startEdgeFunction = startEdgeExtractor.Compile();
            this.endEdgeExtractor = endEdgeExtractor;
            this.endEdgeFunction = endEdgeExtractor.Compile();
        }

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(this.startEventInformation != null);
            Contract.Invariant(this.currentTime >= StreamEvent.MinSyncTime);
        }

        public override void OnNext(ArraySegment<TPayload> value)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            int n = value.Count + value.Offset;
            if (n > value.Array.Length)
            {
                throw new IngressException(
                    $"Invalid array segment. Offset: {value.Offset} Count: {value.Count} Length: {value.Array.Length}");
            }

            int offset = value.Offset;

            while (offset < n)
            {
                var full = this.currentBatch.Add(
                    value,
                        ref this.currentTime,
                    ref offset,
                    this.startEdgeFunction,
                    this.endEdgeFunction);

                if (full)
                {
                    Array.Clear(this.currentBatch.hash.col, 0, this.currentBatch.hash.col.Length);
                    FlushContents();
                }
            }
        }

        protected override void OnCompleted(long punctuationTime)
        {
            this.currentBatch.AddPunctuation(punctuationTime);
            OnFlush();
        }
    }

    [DataContract]
    internal sealed class PartitionedStreamEventArraySubscriptionThrowNone<TKey, TPayload> : PartitionedObserverSubscriptionBase<TKey, ArraySegment<PartitionedStreamEvent<TKey, TPayload>>, TPayload, TPayload>
    {
        public PartitionedStreamEventArraySubscriptionThrowNone() { }

        public PartitionedStreamEventArraySubscriptionThrowNone(
            IObservable<ArraySegment<PartitionedStreamEvent<TKey, TPayload>>> observable,
            string identifier,
            Streamable<PartitionKey<TKey>, TPayload> streamable,
            IStreamObserver<PartitionKey<TKey>, TPayload> observer,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>> diagnosticOutput)
            : base(
                observable,
                identifier,
                streamable,
                observer,
                DisorderPolicy.Throw(),
                PartitionedFlushPolicy.FlushOnLowWatermark,
                PeriodicPunctuationPolicy.None(),
                PeriodicLowWatermarkPolicy.None(),
                onCompletedPolicy,
                diagnosticOutput)
        {
        }

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(this.startEventInformation != null);
        }

        public override void OnNext(ArraySegment<PartitionedStreamEvent<TKey, TPayload>> value)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            int n = value.Count + value.Offset;
            if (n > value.Array.Length)
            {
                throw new IngressException(
                    $"Invalid array segment. Offset: {value.Offset} Count: {value.Count} Length: {value.Array.Length}");
            }

            int offset = value.Offset;

            while (offset < n)
            {
                var full = this.currentBatch.Add(
                    value,
                    (t) => new PartitionKey<TKey>(t),
                        this.currentTime,
                    ref offset);

                if (full)
                {
                    Array.Clear(this.currentBatch.hash.col, 0, this.currentBatch.hash.col.Length);
                    FlushContents();
                }
            }
        }

        protected override void OnCompleted(long punctuationTime)
        {
            OnNext(new ArraySegment<PartitionedStreamEvent<TKey, TPayload>>(new[] { PartitionedStreamEvent.CreateLowWatermark<TKey, TPayload>(punctuationTime) }));
        }
    }

    [DataContract]
    internal sealed class PartitionedIntervalArraySubscriptionThrowNone<TKey, TPayload> : PartitionedObserverSubscriptionBase<TKey, ArraySegment<TPayload>, TPayload, TPayload>
    {
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, TKey>> partitionExtractor;
        private readonly Func<TPayload, TKey> partitionFunction;
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, long>> startEdgeExtractor;
        private readonly Func<TPayload, long> startEdgeFunction;
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, long>> endEdgeExtractor;
        private readonly Func<TPayload, long> endEdgeFunction;
        public PartitionedIntervalArraySubscriptionThrowNone() { }

        public PartitionedIntervalArraySubscriptionThrowNone(
            IObservable<ArraySegment<TPayload>> observable,
            Expression<Func<TPayload, TKey>> partitionExtractor,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            string identifier,
            Streamable<PartitionKey<TKey>, TPayload> streamable,
            IStreamObserver<PartitionKey<TKey>, TPayload> observer,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>> diagnosticOutput)
            : base(
                observable,
                identifier,
                streamable,
                observer,
                DisorderPolicy.Throw(),
                PartitionedFlushPolicy.FlushOnLowWatermark,
                PeriodicPunctuationPolicy.None(),
                PeriodicLowWatermarkPolicy.None(),
                onCompletedPolicy,
                diagnosticOutput)
        {
            this.partitionExtractor = partitionExtractor;
            this.partitionFunction = partitionExtractor.Compile();
            this.startEdgeExtractor = startEdgeExtractor;
            this.startEdgeFunction = startEdgeExtractor.Compile();
            this.endEdgeExtractor = endEdgeExtractor;
            this.endEdgeFunction = endEdgeExtractor.Compile();
        }

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(this.startEventInformation != null);
        }

        public override void OnNext(ArraySegment<TPayload> value)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            int n = value.Count + value.Offset;
            if (n > value.Array.Length)
            {
                throw new IngressException(
                    $"Invalid array segment. Offset: {value.Offset} Count: {value.Count} Length: {value.Array.Length}");
            }

            int offset = value.Offset;

            while (offset < n)
            {
                var full = this.currentBatch.Add(
                    value,
                    this.partitionFunction,
                    (t) => new PartitionKey<TKey>(t),
                        this.currentTime,
                    ref offset,
                    this.startEdgeFunction,
                    this.endEdgeFunction);

                if (full)
                {
                    Array.Clear(this.currentBatch.hash.col, 0, this.currentBatch.hash.col.Length);
                    FlushContents();
                }
            }
        }

        protected override void OnCompleted(long punctuationTime)
        {
            this.currentBatch.AddLowWatermark(punctuationTime);
            OnFlush();
        }
    }

}