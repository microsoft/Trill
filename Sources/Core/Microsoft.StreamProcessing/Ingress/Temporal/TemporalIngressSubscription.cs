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
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    internal static class StreamEventSubscriptionCreator<TPayload, TResult>
    {
        public static IIngressStreamObserver CreateSubscription(
            IObservable<StreamEvent<TPayload>> observable,
            string identifier,
            Streamable<Empty, TResult> streamable,
            IStreamObserver<Empty, TResult> observer,
            DisorderPolicy disorderPolicy,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderStreamEvent<TPayload>> diagnosticOutput,
            FuseModule fuseModule)
        {
            if (punctuationPolicy.type != PeriodicPunctuationPolicyType.Time && punctuationPolicy.type != PeriodicPunctuationPolicyType.None)
            {
                throw new InvalidOperationException("Invalid punctuation policy: " + punctuationPolicy.ToString());
            }

            if (disorderPolicy.reorderLatency > 0)
            {
                if (fuseModule.IsEmpty)
                    return new SimpleStreamEventSubscriptionWithLatency<TPayload>(observable, identifier, (Streamable<Empty, TPayload>)(object)streamable, (IStreamObserver<Empty, TPayload>)(object)observer, disorderPolicy, flushPolicy, punctuationPolicy, onCompletedPolicy, diagnosticOutput);
                else if (Config.AllowFloatingReorderPolicy)
                    return new DisorderedStreamEventSubscriptionWithLatency<TPayload, TResult>(observable, fuseModule, identifier, streamable, observer, disorderPolicy, flushPolicy, punctuationPolicy, onCompletedPolicy, diagnosticOutput);
                else
                    return new FusedStreamEventSubscriptionWithLatency<TPayload, TResult>(observable, fuseModule, identifier, streamable, observer, disorderPolicy, flushPolicy, punctuationPolicy, onCompletedPolicy, diagnosticOutput);
            }
            else
            {
                if (fuseModule.IsEmpty)
                    return new SimpleStreamEventSubscription<TPayload>(observable, identifier, (Streamable<Empty, TPayload>)(object)streamable, (IStreamObserver<Empty, TPayload>)(object)observer, disorderPolicy, flushPolicy, punctuationPolicy, onCompletedPolicy, diagnosticOutput);
                else if (Config.AllowFloatingReorderPolicy)
                    return new DisorderedStreamEventSubscription<TPayload, TResult>(observable, fuseModule, identifier, streamable, observer, disorderPolicy, flushPolicy, punctuationPolicy, onCompletedPolicy, diagnosticOutput);
                else
                    return new FusedStreamEventSubscription<TPayload, TResult>(observable, fuseModule, identifier, streamable, observer, disorderPolicy, flushPolicy, punctuationPolicy, onCompletedPolicy, diagnosticOutput);
            }
        }
    }

    internal static class IntervalSubscriptionCreator<TPayload, TResult>
    {
        public static IIngressStreamObserver CreateSubscription(
            IObservable<TPayload> observable,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            string identifier,
            Streamable<Empty, TResult> streamable,
            IStreamObserver<Empty, TResult> observer,
            DisorderPolicy disorderPolicy,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderStreamEvent<TPayload>> diagnosticOutput,
            FuseModule fuseModule)
        {
            if (punctuationPolicy.type != PeriodicPunctuationPolicyType.Time && punctuationPolicy.type != PeriodicPunctuationPolicyType.None)
            {
                throw new InvalidOperationException("Invalid punctuation policy: " + punctuationPolicy.ToString());
            }

            if (disorderPolicy.reorderLatency > 0)
            {
                if (fuseModule.IsEmpty)
                    return new SimpleIntervalSubscriptionWithLatency<TPayload>(observable, startEdgeExtractor, endEdgeExtractor, identifier, (Streamable<Empty, TPayload>)(object)streamable, (IStreamObserver<Empty, TPayload>)(object)observer, disorderPolicy, flushPolicy, punctuationPolicy, onCompletedPolicy, diagnosticOutput);
                else if (Config.AllowFloatingReorderPolicy)
                    return new DisorderedIntervalSubscriptionWithLatency<TPayload, TResult>(observable, fuseModule, startEdgeExtractor, endEdgeExtractor, identifier, streamable, observer, disorderPolicy, flushPolicy, punctuationPolicy, onCompletedPolicy, diagnosticOutput);
                else
                    return new FusedIntervalSubscriptionWithLatency<TPayload, TResult>(observable, fuseModule, startEdgeExtractor, endEdgeExtractor, identifier, streamable, observer, disorderPolicy, flushPolicy, punctuationPolicy, onCompletedPolicy, diagnosticOutput);
            }
            else
            {
                if (fuseModule.IsEmpty)
                    return new SimpleIntervalSubscription<TPayload>(observable, startEdgeExtractor, endEdgeExtractor, identifier, (Streamable<Empty, TPayload>)(object)streamable, (IStreamObserver<Empty, TPayload>)(object)observer, disorderPolicy, flushPolicy, punctuationPolicy, onCompletedPolicy, diagnosticOutput);
                else if (Config.AllowFloatingReorderPolicy)
                    return new DisorderedIntervalSubscription<TPayload, TResult>(observable, fuseModule, startEdgeExtractor, endEdgeExtractor, identifier, streamable, observer, disorderPolicy, flushPolicy, punctuationPolicy, onCompletedPolicy, diagnosticOutput);
                else
                    return new FusedIntervalSubscription<TPayload, TResult>(observable, fuseModule, startEdgeExtractor, endEdgeExtractor, identifier, streamable, observer, disorderPolicy, flushPolicy, punctuationPolicy, onCompletedPolicy, diagnosticOutput);
            }
        }
    }

    internal static class PartitionedStreamEventSubscriptionCreator<TKey, TPayload, TResult>
    {
        public static IIngressStreamObserver CreateSubscription(
            IObservable<PartitionedStreamEvent<TKey, TPayload>> observable,
            string identifier,
            Streamable<PartitionKey<TKey>, TResult> streamable,
            IStreamObserver<PartitionKey<TKey>, TResult> observer,
            DisorderPolicy disorderPolicy,
            PartitionedFlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            PeriodicLowWatermarkPolicy lowWatermarkPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>> diagnosticOutput,
            FuseModule fuseModule)
        {
            if (punctuationPolicy.type != PeriodicPunctuationPolicyType.Time && punctuationPolicy.type != PeriodicPunctuationPolicyType.None)
            {
                throw new InvalidOperationException("Invalid punctuation policy: " + punctuationPolicy.ToString());
            }

            if (lowWatermarkPolicy.type != PeriodicLowWatermarkPolicyType.Time && lowWatermarkPolicy.type != PeriodicLowWatermarkPolicyType.None)
            {
                throw new InvalidOperationException("Invalid punctuation policy: " + lowWatermarkPolicy.ToString());
            }

            if (disorderPolicy.reorderLatency > 0)
            {
                if (fuseModule.IsEmpty)
                    return new SimplePartitionedStreamEventSubscriptionWithLatency<TKey, TPayload>(observable, identifier, (Streamable<PartitionKey<TKey>, TPayload>)(object)streamable, (IStreamObserver<PartitionKey<TKey>, TPayload>)(object)observer, disorderPolicy, flushPolicy, punctuationPolicy, lowWatermarkPolicy, onCompletedPolicy, diagnosticOutput);
                else if (Config.AllowFloatingReorderPolicy)
                    return new DisorderedPartitionedStreamEventSubscriptionWithLatency<TKey, TPayload, TResult>(observable, fuseModule, identifier, streamable, observer, disorderPolicy, flushPolicy, punctuationPolicy, lowWatermarkPolicy, onCompletedPolicy, diagnosticOutput);
                else
                    return new FusedPartitionedStreamEventSubscriptionWithLatency<TKey, TPayload, TResult>(observable, fuseModule, identifier, streamable, observer, disorderPolicy, flushPolicy, punctuationPolicy, lowWatermarkPolicy, onCompletedPolicy, diagnosticOutput);
            }
            else
            {
                if (fuseModule.IsEmpty)
                    return new SimplePartitionedStreamEventSubscription<TKey, TPayload>(observable, identifier, (Streamable<PartitionKey<TKey>, TPayload>)(object)streamable, (IStreamObserver<PartitionKey<TKey>, TPayload>)(object)observer, disorderPolicy, flushPolicy, punctuationPolicy, lowWatermarkPolicy, onCompletedPolicy, diagnosticOutput);
                else if (Config.AllowFloatingReorderPolicy)
                    return new DisorderedPartitionedStreamEventSubscription<TKey, TPayload, TResult>(observable, fuseModule, identifier, streamable, observer, disorderPolicy, flushPolicy, punctuationPolicy, lowWatermarkPolicy, onCompletedPolicy, diagnosticOutput);
                else
                    return new FusedPartitionedStreamEventSubscription<TKey, TPayload, TResult>(observable, fuseModule, identifier, streamable, observer, disorderPolicy, flushPolicy, punctuationPolicy, lowWatermarkPolicy, onCompletedPolicy, diagnosticOutput);
            }
        }
    }

    internal static class PartitionedIntervalSubscriptionCreator<TKey, TPayload, TResult>
    {
        public static IIngressStreamObserver CreateSubscription(
            IObservable<TPayload> observable,
            Expression<Func<TPayload, TKey>> partitionExtractor,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            string identifier,
            Streamable<PartitionKey<TKey>, TResult> streamable,
            IStreamObserver<PartitionKey<TKey>, TResult> observer,
            DisorderPolicy disorderPolicy,
            PartitionedFlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            PeriodicLowWatermarkPolicy lowWatermarkPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>> diagnosticOutput,
            FuseModule fuseModule)
        {
            if (punctuationPolicy.type != PeriodicPunctuationPolicyType.Time && punctuationPolicy.type != PeriodicPunctuationPolicyType.None)
            {
                throw new InvalidOperationException("Invalid punctuation policy: " + punctuationPolicy.ToString());
            }

            if (lowWatermarkPolicy.type != PeriodicLowWatermarkPolicyType.Time && lowWatermarkPolicy.type != PeriodicLowWatermarkPolicyType.None)
            {
                throw new InvalidOperationException("Invalid punctuation policy: " + lowWatermarkPolicy.ToString());
            }

            if (disorderPolicy.reorderLatency > 0)
            {
                if (fuseModule.IsEmpty)
                    return new SimplePartitionedIntervalSubscriptionWithLatency<TKey, TPayload>(observable, partitionExtractor, startEdgeExtractor, endEdgeExtractor, identifier, (Streamable<PartitionKey<TKey>, TPayload>)(object)streamable, (IStreamObserver<PartitionKey<TKey>, TPayload>)(object)observer, disorderPolicy, flushPolicy, punctuationPolicy, lowWatermarkPolicy, onCompletedPolicy, diagnosticOutput);
                else if (Config.AllowFloatingReorderPolicy)
                    return new DisorderedPartitionedIntervalSubscriptionWithLatency<TKey, TPayload, TResult>(observable, fuseModule, partitionExtractor, startEdgeExtractor, endEdgeExtractor, identifier, streamable, observer, disorderPolicy, flushPolicy, punctuationPolicy, lowWatermarkPolicy, onCompletedPolicy, diagnosticOutput);
                else
                    return new FusedPartitionedIntervalSubscriptionWithLatency<TKey, TPayload, TResult>(observable, fuseModule, partitionExtractor, startEdgeExtractor, endEdgeExtractor, identifier, streamable, observer, disorderPolicy, flushPolicy, punctuationPolicy, lowWatermarkPolicy, onCompletedPolicy, diagnosticOutput);
            }
            else
            {
                if (fuseModule.IsEmpty)
                    return new SimplePartitionedIntervalSubscription<TKey, TPayload>(observable, partitionExtractor, startEdgeExtractor, endEdgeExtractor, identifier, (Streamable<PartitionKey<TKey>, TPayload>)(object)streamable, (IStreamObserver<PartitionKey<TKey>, TPayload>)(object)observer, disorderPolicy, flushPolicy, punctuationPolicy, lowWatermarkPolicy, onCompletedPolicy, diagnosticOutput);
                else if (Config.AllowFloatingReorderPolicy)
                    return new DisorderedPartitionedIntervalSubscription<TKey, TPayload, TResult>(observable, fuseModule, partitionExtractor, startEdgeExtractor, endEdgeExtractor, identifier, streamable, observer, disorderPolicy, flushPolicy, punctuationPolicy, lowWatermarkPolicy, onCompletedPolicy, diagnosticOutput);
                else
                    return new FusedPartitionedIntervalSubscription<TKey, TPayload, TResult>(observable, fuseModule, partitionExtractor, startEdgeExtractor, endEdgeExtractor, identifier, streamable, observer, disorderPolicy, flushPolicy, punctuationPolicy, lowWatermarkPolicy, onCompletedPolicy, diagnosticOutput);
            }
        }
    }

    [DataContract]
    internal sealed class SimpleStreamEventSubscriptionWithLatency<TPayload> : ObserverSubscriptionBase<StreamEvent<TPayload>, TPayload, TPayload>
    {
        public SimpleStreamEventSubscriptionWithLatency() { }

        public SimpleStreamEventSubscriptionWithLatency(
            IObservable<StreamEvent<TPayload>> observable,
            string identifier,
            Streamable<Empty, TPayload> streamable,
            IStreamObserver<Empty, TPayload> observer,
            DisorderPolicy disorderPolicy,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderStreamEvent<TPayload>> diagnosticOutput)
            : base(
                observable,
                identifier,
                streamable,
                observer,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
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

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        public override void OnNext(StreamEvent<TPayload> value)
        {

            if (value.IsPunctuation)
            {
                GenerateAndProcessPunctuation(value.SyncTime);
                return;
            }

            long moveFrom = this.currentTime;
            long moveTo = moveFrom;

            // Events at the reorder boundary or earlier - are handled using default processing policies
            if (value.SyncTime <= moveFrom)
            {
                Process(ref value);
                return;
            }

            var oldTime = this.highWatermark;
            if (value.SyncTime > oldTime)
            {
                this.highWatermark = value.SyncTime;

                moveTo = value.SyncTime - this.reorderLatency;
                if (moveTo < StreamEvent.MinSyncTime) moveTo = StreamEvent.MinSyncTime;
                if (moveTo < moveFrom) moveTo = moveFrom;
            }

            if (moveTo > moveFrom)
            {
                if (this.priorityQueueSorter != null)
                {
                    StreamEvent<TPayload> resultEvent;

                    while ((!this.priorityQueueSorter.IsEmpty()) && this.priorityQueueSorter.Peek().SyncTime <= moveTo)
                    {
                        resultEvent = this.priorityQueueSorter.Dequeue();
                        Process(ref resultEvent);
                    }
                }
                else
                {
                    // Extract and process data in-order from impatience, until timestamp of moveTo
                    PooledElasticCircularBuffer<StreamEvent<TPayload>> streamEvents = this.impatienceSorter.DequeueUntil(moveTo, out bool recheck);

                    if (streamEvents != null)
                    {
                        StreamEvent<TPayload> resultEvent;
                        while ((streamEvents.Count > 0) && ((!recheck) || (streamEvents.PeekFirst().SyncTime <= moveTo)))
                        {
                            resultEvent = streamEvents.Dequeue();
                            Process(ref resultEvent);
                        }
                    }

                    if (!recheck && (streamEvents != null))
                        this.impatienceSorter.Return(streamEvents);
                }
            }

            if (value.SyncTime == moveTo)
            {
                Process(ref value);
                return;
            }

            // Enqueue value into impatience
            if (this.priorityQueueSorter != null) this.priorityQueueSorter.Enqueue(value);
            else this.impatienceSorter.Enqueue(ref value);

            UpdateCurrentTime(moveTo, fromEvent: false);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void Process(ref StreamEvent<TPayload> value)
        {
            Contract.Assume(value.SyncTime != value.OtherTime);

            if (value.IsPunctuation)
            {
                GenerateAndProcessPunctuation(value.SyncTime);
                return;
            }

            long current = this.currentTime;

            var outOfOrder = value.SyncTime < current;
            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                // out of order events shouldn't count, and if the disorder policy adjusts their sync time, then it
                // will be made equal to a timestamp already seen earlier in the sequence and this would have triggered
                // (if necessary) when that timestamp was seen.
                ulong delta = (ulong)(value.SyncTime - this.lastPunctuationTime);
                if (!outOfOrder && delta >= this.punctuationGenerationPeriod)
                {
                    // SyncTime is sufficiently high to generate a new punctuation, but first snap it to the nearest generationPeriod boundary
                    var punctuationTimeQuantized = value.SyncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod);
#if DEBUG
                    Debug.Assert(punctuationTimeQuantized >= LastEventTime(), "Bug in punctuation quantization logic");
#endif
                    OnPunctuation(StreamEvent.CreatePunctuation<TPayload>(punctuationTimeQuantized));
                }
            }

            // check for out of order event
            if (this.disorderPolicyType == DisorderPolicyType.Throw)
            {
                if (outOfOrder)
                {
                    throw new IngressException($"Out-of-order event encountered during ingress, under a disorder policy of Throw: value.SyncTime: {value.SyncTime}, current:{current}");
                }
            }
            else
            {
                // end events and interval events just get dropped
                Tuple<long, TPayload> key;
                ElasticCircularBuffer<AdjustInfo> q;
                switch (value.Kind)
                {
                    case StreamEventKind.Start:
                        if (outOfOrder)
                        {
                            key = Tuple.Create(value.SyncTime, value.Payload);
                            if (!this.startEventInformation.TryGetValue(key, out q))
                            {
                                q = new ElasticCircularBuffer<AdjustInfo>();
                                this.startEventInformation.Add(key, q);
                                var x = new AdjustInfo(current);
                                q.Enqueue(ref x);
                            }
                            else
                            {
                                var last = q.PeekLast();
                                if (last.modifiedStartTime == current) last.numberOfOccurrences++;
                                else
                                {
                                    var x = new AdjustInfo(current);
                                    q.Enqueue(ref x);
                                }
                            }

                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                return; // drop
                            }
                            else
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                value = new StreamEvent<TPayload>(current, StreamEvent.InfinitySyncTime, value.Payload);
                            }
                        }
                        break;

                    case StreamEventKind.Interval:
                        if (outOfOrder)
                        {
                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                return; // drop
                            }
                            else
                            {
                                if (current >= value.OtherTime)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                    return; // drop
                                }

                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                value = new StreamEvent<TPayload>(current, value.OtherTime, value.Payload);
                            }
                        }
                        break;

                    case StreamEventKind.End:
                        // it may not be out of order, but did we drop/adjust the corresponding start event?
                        key = Tuple.Create(value.OtherTime, value.Payload);
                        if (this.startEventInformation.TryGetValue(key, out q))
                        {
                            Contract.Assume(!q.IsEmpty());
                            var firstElement = q.PeekFirst();
                            firstElement.numberOfOccurrences--;
                            if (firstElement.numberOfOccurrences == 0)
                            {
                                q.Dequeue(); // throw away returned value
                                if (q.Count == 0) this.startEventInformation.Remove(key);
                            }
                            var adjustedTime = firstElement.modifiedStartTime;

                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                return; // drop
                            }
                            else
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                value = new StreamEvent<TPayload>(outOfOrder ? current : value.SyncTime, adjustedTime, value.Payload);
                            }
                        }
                        else if (outOfOrder)
                        {
                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                return; // drop
                            }
                            else
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                value = new StreamEvent<TPayload>(current, value.OtherTime, value.Payload);
                            }
                        }

                        break;
                    default:
                        Contract.Assert(false, "switch meant to be exhaustive");
                        throw new InvalidOperationException("Unsupported stream event kind: " + value.Kind.ToString());
                }
            }

            this.currentBatch.Add(value.SyncTime, value.OtherTime, Empty.Default, value.Payload);
            if (this.currentBatch.Count == Config.DataBatchSize)
            {
                if (this.flushPolicy == FlushPolicy.FlushOnBatchBoundary) OnFlush();
                else FlushContents();
            }

            UpdateCurrentTime(value.SyncTime);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateCurrentTime(long time, bool fromEvent = true)
        {
            if (this.currentTime < time)
            {
                this.currentTime = time;
            }
#if DEBUG
            if (fromEvent && this.lastEventTime < time)
            {
                this.lastEventTime = time;
            }
#endif
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void GenerateAndProcessPunctuation(long syncTime)
        {
            if (syncTime <= this.lastPunctuationTime) return;

            // Update the Punctuation to be at least the currentTime, so the Punctuation
            // is not before the preceding data event.
            // Note that currentTime only reflects events already processed, and excludes events in the reorder buffer.
            syncTime = Math.Max(syncTime, this.currentTime);

            // Process events queued for reorderLatency up to the Punctuation syncTime
            if (this.priorityQueueSorter != null)
            {
                StreamEvent<TPayload> resultEvent;
                while ((!this.priorityQueueSorter.IsEmpty()) && this.priorityQueueSorter.Peek().SyncTime <= syncTime)
                {
                    resultEvent = this.priorityQueueSorter.Dequeue();
                    Process(ref resultEvent);
                }
            }
            else
            {
                var streamEvents = this.impatienceSorter.DequeueUntil(syncTime, out bool recheck);
                if (streamEvents != null)
                {
                    StreamEvent<TPayload> resultEvent;
                    while ((streamEvents.Count > 0) && ((!recheck) || (streamEvents.PeekFirst().SyncTime <= syncTime)))
                    {
                        resultEvent = streamEvents.Dequeue();
                        Process(ref resultEvent);
                    }
                    if (!recheck) this.impatienceSorter.Return(streamEvents);
                }
            }

            // Update cached global times
            this.highWatermark = Math.Max(syncTime, this.highWatermark);
            UpdateCurrentTime(syncTime);
            this.lastPunctuationTime = Math.Max(
                syncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod),
                this.lastPunctuationTime);

            // Add Punctuation to batch
            var count = this.currentBatch.Count;
            this.currentBatch.vsync.col[count] = syncTime;
            this.currentBatch.vother.col[count] = StreamEvent.PunctuationOtherTime;
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            this.currentBatch.key.col[count] = default;
            this.currentBatch[count] = default;
            this.currentBatch.hash.col[count] = 0;
            this.currentBatch.Count = count + 1;

            // Flush if necessary
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

        protected override void OnCompleted(long punctuationTime)
        {
            GenerateAndProcessPunctuation(punctuationTime);

            // Flush, but if we just flushed due to the punctuation generated above
            if (this.flushPolicy != FlushPolicy.FlushOnPunctuation)
                OnFlush();
        }
    }

    [DataContract]
    internal sealed class FusedStreamEventSubscriptionWithLatency<TPayload, TResult> : ObserverSubscriptionBase<StreamEvent<TPayload>, TPayload, TResult>
    {
        [SchemaSerialization]
        private readonly FuseModule fuseModule;
        private readonly Action<long, long, TPayload, Empty> action;
        public FusedStreamEventSubscriptionWithLatency() { }

        public FusedStreamEventSubscriptionWithLatency(
            IObservable<StreamEvent<TPayload>> observable,
            FuseModule fuseModule,
            string identifier,
            Streamable<Empty, TResult> streamable,
            IStreamObserver<Empty, TResult> observer,
            DisorderPolicy disorderPolicy,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderStreamEvent<TPayload>> diagnosticOutput)
            : base(
                observable,
                identifier,
                streamable,
                observer,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                diagnosticOutput)
        {
            this.fuseModule = fuseModule;
            Expression<Action<long, long, TResult, Empty>> statement = (s, e, p, k) => this.currentBatch.Add(s, e, k, p);
            Expression<Action> flush = () => FlushContents();
            Expression<Func<bool>> test = () => this.currentBatch.Count == Config.DataBatchSize;
            var full = Expression.Lambda<Action<long, long, TResult, Empty>>(
                Expression.Block(
                    statement.Body,
                    Expression.IfThen(test.Body, flush.Body)),
                statement.Parameters);
            var actionExp = fuseModule.Coalesce<TPayload, TResult, Empty>(full);
            this.action = actionExp.Compile();
        }

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(this.startEventInformation != null);
            Contract.Invariant(this.currentTime >= StreamEvent.MinSyncTime);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        public override void OnNext(StreamEvent<TPayload> value)
        {

            if (value.IsPunctuation)
            {
                GenerateAndProcessPunctuation(value.SyncTime);
                return;
            }

            long moveFrom = this.currentTime;
            long moveTo = moveFrom;

            // Events at the reorder boundary or earlier - are handled using default processing policies
            if (value.SyncTime <= moveFrom)
            {
                Process(ref value);
                return;
            }

            var oldTime = this.highWatermark;
            if (value.SyncTime > oldTime)
            {
                this.highWatermark = value.SyncTime;

                moveTo = value.SyncTime - this.reorderLatency;
                if (moveTo < StreamEvent.MinSyncTime) moveTo = StreamEvent.MinSyncTime;
                if (moveTo < moveFrom) moveTo = moveFrom;
            }

            if (moveTo > moveFrom)
            {
                if (this.priorityQueueSorter != null)
                {
                    StreamEvent<TPayload> resultEvent;

                    while ((!this.priorityQueueSorter.IsEmpty()) && this.priorityQueueSorter.Peek().SyncTime <= moveTo)
                    {
                        resultEvent = this.priorityQueueSorter.Dequeue();
                        Process(ref resultEvent);
                    }
                }
                else
                {
                    // Extract and process data in-order from impatience, until timestamp of moveTo
                    PooledElasticCircularBuffer<StreamEvent<TPayload>> streamEvents = this.impatienceSorter.DequeueUntil(moveTo, out bool recheck);

                    if (streamEvents != null)
                    {
                        StreamEvent<TPayload> resultEvent;
                        while ((streamEvents.Count > 0) && ((!recheck) || (streamEvents.PeekFirst().SyncTime <= moveTo)))
                        {
                            resultEvent = streamEvents.Dequeue();
                            Process(ref resultEvent);
                        }
                    }

                    if (!recheck && (streamEvents != null))
                        this.impatienceSorter.Return(streamEvents);
                }
            }

            if (value.SyncTime == moveTo)
            {
                Process(ref value);
                return;
            }

            // Enqueue value into impatience
            if (this.priorityQueueSorter != null) this.priorityQueueSorter.Enqueue(value);
            else this.impatienceSorter.Enqueue(ref value);

            UpdateCurrentTime(moveTo, fromEvent: false);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void Process(ref StreamEvent<TPayload> value)
        {
            Contract.Assume(value.SyncTime != value.OtherTime);

            if (value.IsPunctuation)
            {
                GenerateAndProcessPunctuation(value.SyncTime);
                return;
            }

            long current = this.currentTime;

            var outOfOrder = value.SyncTime < current;
            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                // out of order events shouldn't count, and if the disorder policy adjusts their sync time, then it
                // will be made equal to a timestamp already seen earlier in the sequence and this would have triggered
                // (if necessary) when that timestamp was seen.
                ulong delta = (ulong)(value.SyncTime - this.lastPunctuationTime);
                if (!outOfOrder && delta >= this.punctuationGenerationPeriod)
                {
                    // SyncTime is sufficiently high to generate a new punctuation, but first snap it to the nearest generationPeriod boundary
                    var punctuationTimeQuantized = value.SyncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod);
#if DEBUG
                    Debug.Assert(punctuationTimeQuantized >= LastEventTime(), "Bug in punctuation quantization logic");
#endif
                    OnPunctuation(StreamEvent.CreatePunctuation<TPayload>(punctuationTimeQuantized));
                }
            }

            // check for out of order event
            if (this.disorderPolicyType == DisorderPolicyType.Throw)
            {
                if (outOfOrder)
                {
                    throw new IngressException($"Out-of-order event encountered during ingress, under a disorder policy of Throw: value.SyncTime: {value.SyncTime}, current:{current}");
                }
            }
            else
            {
                // end events and interval events just get dropped
                Tuple<long, TPayload> key;
                ElasticCircularBuffer<AdjustInfo> q;
                switch (value.Kind)
                {
                    case StreamEventKind.Start:
                        if (outOfOrder)
                        {
                            key = Tuple.Create(value.SyncTime, value.Payload);
                            if (!this.startEventInformation.TryGetValue(key, out q))
                            {
                                q = new ElasticCircularBuffer<AdjustInfo>();
                                this.startEventInformation.Add(key, q);
                                var x = new AdjustInfo(current);
                                q.Enqueue(ref x);
                            }
                            else
                            {
                                var last = q.PeekLast();
                                if (last.modifiedStartTime == current) last.numberOfOccurrences++;
                                else
                                {
                                    var x = new AdjustInfo(current);
                                    q.Enqueue(ref x);
                                }
                            }

                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                return; // drop
                            }
                            else
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                value = new StreamEvent<TPayload>(current, StreamEvent.InfinitySyncTime, value.Payload);
                            }
                        }
                        break;

                    case StreamEventKind.Interval:
                        if (outOfOrder)
                        {
                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                return; // drop
                            }
                            else
                            {
                                if (current >= value.OtherTime)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                    return; // drop
                                }

                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                value = new StreamEvent<TPayload>(current, value.OtherTime, value.Payload);
                            }
                        }
                        break;

                    case StreamEventKind.End:
                        // it may not be out of order, but did we drop/adjust the corresponding start event?
                        key = Tuple.Create(value.OtherTime, value.Payload);
                        if (this.startEventInformation.TryGetValue(key, out q))
                        {
                            Contract.Assume(!q.IsEmpty());
                            var firstElement = q.PeekFirst();
                            firstElement.numberOfOccurrences--;
                            if (firstElement.numberOfOccurrences == 0)
                            {
                                q.Dequeue(); // throw away returned value
                                if (q.Count == 0) this.startEventInformation.Remove(key);
                            }
                            var adjustedTime = firstElement.modifiedStartTime;

                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                return; // drop
                            }
                            else
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                value = new StreamEvent<TPayload>(outOfOrder ? current : value.SyncTime, adjustedTime, value.Payload);
                            }
                        }
                        else if (outOfOrder)
                        {
                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                return; // drop
                            }
                            else
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                value = new StreamEvent<TPayload>(current, value.OtherTime, value.Payload);
                            }
                        }

                        break;
                    default:
                        Contract.Assert(false, "switch meant to be exhaustive");
                        throw new InvalidOperationException("Unsupported stream event kind: " + value.Kind.ToString());
                }
            }

            this.action(value.SyncTime, value.OtherTime, value.Payload, Empty.Default);

            UpdateCurrentTime(value.SyncTime);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateCurrentTime(long time, bool fromEvent = true)
        {
            if (this.currentTime < time)
            {
                this.currentTime = time;
            }
#if DEBUG
            if (fromEvent && this.lastEventTime < time)
            {
                this.lastEventTime = time;
            }
#endif
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void GenerateAndProcessPunctuation(long syncTime)
        {
            if (syncTime <= this.lastPunctuationTime) return;

            // Update the Punctuation to be at least the currentTime, so the Punctuation
            // is not before the preceding data event.
            // Note that currentTime only reflects events already processed, and excludes events in the reorder buffer.
            syncTime = Math.Max(syncTime, this.currentTime);

            // Process events queued for reorderLatency up to the Punctuation syncTime
            if (this.priorityQueueSorter != null)
            {
                StreamEvent<TPayload> resultEvent;
                while ((!this.priorityQueueSorter.IsEmpty()) && this.priorityQueueSorter.Peek().SyncTime <= syncTime)
                {
                    resultEvent = this.priorityQueueSorter.Dequeue();
                    Process(ref resultEvent);
                }
            }
            else
            {
                var streamEvents = this.impatienceSorter.DequeueUntil(syncTime, out bool recheck);
                if (streamEvents != null)
                {
                    StreamEvent<TPayload> resultEvent;
                    while ((streamEvents.Count > 0) && ((!recheck) || (streamEvents.PeekFirst().SyncTime <= syncTime)))
                    {
                        resultEvent = streamEvents.Dequeue();
                        Process(ref resultEvent);
                    }
                    if (!recheck) this.impatienceSorter.Return(streamEvents);
                }
            }

            // Update cached global times
            this.highWatermark = Math.Max(syncTime, this.highWatermark);
            UpdateCurrentTime(syncTime);
            this.lastPunctuationTime = Math.Max(
                syncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod),
                this.lastPunctuationTime);

            // Add Punctuation to batch
            var count = this.currentBatch.Count;
            this.currentBatch.vsync.col[count] = syncTime;
            this.currentBatch.vother.col[count] = StreamEvent.PunctuationOtherTime;
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            this.currentBatch.key.col[count] = default;
            this.currentBatch[count] = default;
            this.currentBatch.hash.col[count] = 0;
            this.currentBatch.Count = count + 1;

            // Flush if necessary
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

        protected override void OnCompleted(long punctuationTime)
        {
            GenerateAndProcessPunctuation(punctuationTime);

            // Flush, but if we just flushed due to the punctuation generated above
            if (this.flushPolicy != FlushPolicy.FlushOnPunctuation)
                OnFlush();
        }
    }

    [DataContract]
    internal sealed class DisorderedStreamEventSubscriptionWithLatency<TPayload, TResult> : DisorderedObserverSubscriptionBase<StreamEvent<TPayload>, TPayload, TResult>
    {
        [SchemaSerialization]
        private readonly FuseModule fuseModule;
        private readonly Action<long, long, TPayload, Empty> action;
        public DisorderedStreamEventSubscriptionWithLatency() { }

        public DisorderedStreamEventSubscriptionWithLatency(
            IObservable<StreamEvent<TPayload>> observable,
            FuseModule fuseModule,
            string identifier,
            Streamable<Empty, TResult> streamable,
            IStreamObserver<Empty, TResult> observer,
            DisorderPolicy disorderPolicy,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderStreamEvent<TPayload>> diagnosticOutput)
            : base(
                observable,
                identifier,
                streamable,
                observer,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                diagnosticOutput)
        {
            this.fuseModule = fuseModule;
            Expression<Action<long, long, TResult, Empty>> statement = (s, e, p, k) => Action(s, e, p, k);
            var actionExp = fuseModule.Coalesce<TPayload, TResult, Empty>(statement, true);
            this.action = actionExp.Compile();
        }

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(this.startEventInformation != null);
            Contract.Invariant(this.currentTime >= StreamEvent.MinSyncTime);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        public override void OnNext(StreamEvent<TPayload> value)
        {
            this.action(value.SyncTime, value.OtherTime, value.Payload, Empty.Default);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void Action(long start, long end, TResult payload, Empty actionKey)
        {
            var value = new StreamEvent<TResult>(start, end, payload);

            if (value.IsPunctuation)
            {
                GenerateAndProcessPunctuation(value.SyncTime);
                return;
            }

            long moveFrom = this.currentTime;
            long moveTo = moveFrom;

            // Events at the reorder boundary or earlier - are handled using default processing policies
            if (value.SyncTime <= moveFrom)
            {
                Process(ref value);
                return;
            }

            var oldTime = this.highWatermark;
            if (value.SyncTime > oldTime)
            {
                this.highWatermark = value.SyncTime;

                moveTo = value.SyncTime - this.reorderLatency;
                if (moveTo < StreamEvent.MinSyncTime) moveTo = StreamEvent.MinSyncTime;
                if (moveTo < moveFrom) moveTo = moveFrom;
            }

            if (moveTo > moveFrom)
            {
                if (this.priorityQueueSorter != null)
                {
                    StreamEvent<TResult> resultEvent;

                    while ((!this.priorityQueueSorter.IsEmpty()) && this.priorityQueueSorter.Peek().SyncTime <= moveTo)
                    {
                        resultEvent = this.priorityQueueSorter.Dequeue();
                        Process(ref resultEvent);
                    }
                }
                else
                {
                    // Extract and process data in-order from impatience, until timestamp of moveTo
                    PooledElasticCircularBuffer<StreamEvent<TResult>> streamEvents = this.impatienceSorter.DequeueUntil(moveTo, out bool recheck);

                    if (streamEvents != null)
                    {
                        StreamEvent<TResult> resultEvent;
                        while ((streamEvents.Count > 0) && ((!recheck) || (streamEvents.PeekFirst().SyncTime <= moveTo)))
                        {
                            resultEvent = streamEvents.Dequeue();
                            Process(ref resultEvent);
                        }
                    }

                    if (!recheck && (streamEvents != null))
                        this.impatienceSorter.Return(streamEvents);
                }
            }

            if (value.SyncTime == moveTo)
            {
                Process(ref value);
                return;
            }

            // Enqueue value into impatience
            if (this.priorityQueueSorter != null) this.priorityQueueSorter.Enqueue(value);
            else this.impatienceSorter.Enqueue(ref value);

            UpdateCurrentTime(moveTo, fromEvent: false);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void Process(ref StreamEvent<TResult> value)
        {
            Contract.Assume(value.SyncTime != value.OtherTime);

            if (value.IsPunctuation)
            {
                GenerateAndProcessPunctuation(value.SyncTime);
                return;
            }

            long current = this.currentTime;

            var outOfOrder = value.SyncTime < current;
            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                // out of order events shouldn't count, and if the disorder policy adjusts their sync time, then it
                // will be made equal to a timestamp already seen earlier in the sequence and this would have triggered
                // (if necessary) when that timestamp was seen.
                ulong delta = (ulong)(value.SyncTime - this.lastPunctuationTime);
                if (!outOfOrder && delta >= this.punctuationGenerationPeriod)
                {
                    // SyncTime is sufficiently high to generate a new punctuation, but first snap it to the nearest generationPeriod boundary
                    var punctuationTimeQuantized = value.SyncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod);
#if DEBUG
                    Debug.Assert(punctuationTimeQuantized >= LastEventTime(), "Bug in punctuation quantization logic");
#endif
                    OnPunctuation(StreamEvent.CreatePunctuation<TResult>(punctuationTimeQuantized));
                }
            }

            // check for out of order event
            if (this.disorderPolicyType == DisorderPolicyType.Throw)
            {
                if (outOfOrder)
                {
                    throw new IngressException($"Out-of-order event encountered during ingress, under a disorder policy of Throw: value.SyncTime: {value.SyncTime}, current:{current}");
                }
            }
            else
            {
                // end events and interval events just get dropped
                Tuple<long, TResult> key;
                ElasticCircularBuffer<AdjustInfo> q;
                switch (value.Kind)
                {
                    case StreamEventKind.Start:
                        if (outOfOrder)
                        {
                            key = Tuple.Create(value.SyncTime, value.Payload);
                            if (!this.startEventInformation.TryGetValue(key, out q))
                            {
                                q = new ElasticCircularBuffer<AdjustInfo>();
                                this.startEventInformation.Add(key, q);
                                var x = new AdjustInfo(current);
                                q.Enqueue(ref x);
                            }
                            else
                            {
                                var last = q.PeekLast();
                                if (last.modifiedStartTime == current) last.numberOfOccurrences++;
                                else
                                {
                                    var x = new AdjustInfo(current);
                                    q.Enqueue(ref x);
                                }
                            }

                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(new StreamEvent<TPayload>(value.SyncTime, value.OtherTime, default), default));
                                return; // drop
                            }
                            else
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(new StreamEvent<TPayload>(value.SyncTime, value.OtherTime, default), new long?(current - value.SyncTime)));
                                value = new StreamEvent<TResult>(current, StreamEvent.InfinitySyncTime, value.Payload);
                            }
                        }
                        break;

                    case StreamEventKind.Interval:
                        if (outOfOrder)
                        {
                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(new StreamEvent<TPayload>(value.SyncTime, value.OtherTime, default), default));
                                return; // drop
                            }
                            else
                            {
                                if (current >= value.OtherTime)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(new StreamEvent<TPayload>(value.SyncTime, value.OtherTime, default), default));
                                    return; // drop
                                }

                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(new StreamEvent<TPayload>(value.SyncTime, value.OtherTime, default), new long?(current - value.SyncTime)));
                                value = new StreamEvent<TResult>(current, value.OtherTime, value.Payload);
                            }
                        }
                        break;

                    case StreamEventKind.End:
                        // it may not be out of order, but did we drop/adjust the corresponding start event?
                        key = Tuple.Create(value.OtherTime, value.Payload);
                        if (this.startEventInformation.TryGetValue(key, out q))
                        {
                            Contract.Assume(!q.IsEmpty());
                            var firstElement = q.PeekFirst();
                            firstElement.numberOfOccurrences--;
                            if (firstElement.numberOfOccurrences == 0)
                            {
                                q.Dequeue(); // throw away returned value
                                if (q.Count == 0) this.startEventInformation.Remove(key);
                            }
                            var adjustedTime = firstElement.modifiedStartTime;

                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(new StreamEvent<TPayload>(value.SyncTime, value.OtherTime, default), default));
                                return; // drop
                            }
                            else
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(new StreamEvent<TPayload>(value.SyncTime, value.OtherTime, default), new long?(current - value.SyncTime)));
                                value = new StreamEvent<TResult>(outOfOrder ? current : value.SyncTime, adjustedTime, value.Payload);
                            }
                        }
                        else if (outOfOrder)
                        {
                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(new StreamEvent<TPayload>(value.SyncTime, value.OtherTime, default), default));
                                return; // drop
                            }
                            else
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(new StreamEvent<TPayload>(value.SyncTime, value.OtherTime, default), new long?(current - value.SyncTime)));
                                value = new StreamEvent<TResult>(current, value.OtherTime, value.Payload);
                            }
                        }

                        break;
                    default:
                        Contract.Assert(false, "switch meant to be exhaustive");
                        throw new InvalidOperationException("Unsupported stream event kind: " + value.Kind.ToString());
                }
            }

            this.currentBatch.Add(value.SyncTime, value.OtherTime, Empty.Default, value.Payload);
            if (this.currentBatch.Count == Config.DataBatchSize)
            {
                if (this.flushPolicy == FlushPolicy.FlushOnBatchBoundary) OnFlush();
                else FlushContents();
            }

            UpdateCurrentTime(value.SyncTime);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateCurrentTime(long time, bool fromEvent = true)
        {
            if (this.currentTime < time)
            {
                this.currentTime = time;
            }
#if DEBUG
            if (fromEvent && this.lastEventTime < time)
            {
                this.lastEventTime = time;
            }
#endif
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void GenerateAndProcessPunctuation(long syncTime)
        {
            if (syncTime <= this.lastPunctuationTime) return;

            // Update the Punctuation to be at least the currentTime, so the Punctuation
            // is not before the preceding data event.
            // Note that currentTime only reflects events already processed, and excludes events in the reorder buffer.
            syncTime = Math.Max(syncTime, this.currentTime);

            // Process events queued for reorderLatency up to the Punctuation syncTime
            if (this.priorityQueueSorter != null)
            {
                StreamEvent<TResult> resultEvent;
                while ((!this.priorityQueueSorter.IsEmpty()) && this.priorityQueueSorter.Peek().SyncTime <= syncTime)
                {
                    resultEvent = this.priorityQueueSorter.Dequeue();
                    Process(ref resultEvent);
                }
            }
            else
            {
                var streamEvents = this.impatienceSorter.DequeueUntil(syncTime, out bool recheck);
                if (streamEvents != null)
                {
                    StreamEvent<TResult> resultEvent;
                    while ((streamEvents.Count > 0) && ((!recheck) || (streamEvents.PeekFirst().SyncTime <= syncTime)))
                    {
                        resultEvent = streamEvents.Dequeue();
                        Process(ref resultEvent);
                    }
                    if (!recheck) this.impatienceSorter.Return(streamEvents);
                }
            }

            // Update cached global times
            this.highWatermark = Math.Max(syncTime, this.highWatermark);
            UpdateCurrentTime(syncTime);
            this.lastPunctuationTime = Math.Max(
                syncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod),
                this.lastPunctuationTime);

            // Add Punctuation to batch
            var count = this.currentBatch.Count;
            this.currentBatch.vsync.col[count] = syncTime;
            this.currentBatch.vother.col[count] = StreamEvent.PunctuationOtherTime;
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            this.currentBatch.key.col[count] = default;
            this.currentBatch[count] = default;
            this.currentBatch.hash.col[count] = 0;
            this.currentBatch.Count = count + 1;

            // Flush if necessary
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

        protected override void OnCompleted(long punctuationTime)
        {
            GenerateAndProcessPunctuation(punctuationTime);

            // Flush, but if we just flushed due to the punctuation generated above
            if (this.flushPolicy != FlushPolicy.FlushOnPunctuation)
                OnFlush();
        }
    }

    [DataContract]
    internal sealed class SimpleStreamEventSubscription<TPayload> : ObserverSubscriptionBase<StreamEvent<TPayload>, TPayload, TPayload>
    {
        public SimpleStreamEventSubscription() { }

        public SimpleStreamEventSubscription(
            IObservable<StreamEvent<TPayload>> observable,
            string identifier,
            Streamable<Empty, TPayload> streamable,
            IStreamObserver<Empty, TPayload> observer,
            DisorderPolicy disorderPolicy,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderStreamEvent<TPayload>> diagnosticOutput)
            : base(
                observable,
                identifier,
                streamable,
                observer,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
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

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        public override void OnNext(StreamEvent<TPayload> value)
        {

            if (value.IsPunctuation)
            {
                GenerateAndProcessPunctuation(value.SyncTime);
                return;
            }

            Contract.Assume(value.SyncTime != value.OtherTime);

            long current = this.currentTime;

            var outOfOrder = value.SyncTime < current;
            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                // out of order events shouldn't count, and if the disorder policy adjusts their sync time, then it
                // will be made equal to a timestamp already seen earlier in the sequence and this would have triggered
                // (if necessary) when that timestamp was seen.
                ulong delta = (ulong)(value.SyncTime - this.lastPunctuationTime);
                if (!outOfOrder && delta >= this.punctuationGenerationPeriod)
                {
                    // SyncTime is sufficiently high to generate a new punctuation, but first snap it to the nearest generationPeriod boundary
                    var punctuationTimeQuantized = value.SyncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod);
#if DEBUG
                    Debug.Assert(punctuationTimeQuantized >= LastEventTime(), "Bug in punctuation quantization logic");
#endif
                    OnPunctuation(StreamEvent.CreatePunctuation<TPayload>(punctuationTimeQuantized));
                }
            }

            // check for out of order event
            if (this.disorderPolicyType == DisorderPolicyType.Throw)
            {
                if (outOfOrder)
                {
                    throw new IngressException($"Out-of-order event encountered during ingress, under a disorder policy of Throw: value.SyncTime: {value.SyncTime}, current:{current}");
                }
            }
            else
            {
                // end events and interval events just get dropped
                Tuple<long, TPayload> key;
                ElasticCircularBuffer<AdjustInfo> q;
                switch (value.Kind)
                {
                    case StreamEventKind.Start:
                        if (outOfOrder)
                        {
                            key = Tuple.Create(value.SyncTime, value.Payload);
                            if (!this.startEventInformation.TryGetValue(key, out q))
                            {
                                q = new ElasticCircularBuffer<AdjustInfo>();
                                this.startEventInformation.Add(key, q);
                                var x = new AdjustInfo(current);
                                q.Enqueue(ref x);
                            }
                            else
                            {
                                var last = q.PeekLast();
                                if (last.modifiedStartTime == current) last.numberOfOccurrences++;
                                else
                                {
                                    var x = new AdjustInfo(current);
                                    q.Enqueue(ref x);
                                }
                            }

                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                return; // drop
                            }
                            else
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                value = new StreamEvent<TPayload>(current, StreamEvent.InfinitySyncTime, value.Payload);
                            }
                        }
                        break;

                    case StreamEventKind.Interval:
                        if (outOfOrder)
                        {
                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                return; // drop
                            }
                            else
                            {
                                if (current >= value.OtherTime)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                    return; // drop
                                }

                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                value = new StreamEvent<TPayload>(current, value.OtherTime, value.Payload);
                            }
                        }
                        break;

                    case StreamEventKind.End:
                        // it may not be out of order, but did we drop/adjust the corresponding start event?
                        key = Tuple.Create(value.OtherTime, value.Payload);
                        if (this.startEventInformation.TryGetValue(key, out q))
                        {
                            Contract.Assume(!q.IsEmpty());
                            var firstElement = q.PeekFirst();
                            firstElement.numberOfOccurrences--;
                            if (firstElement.numberOfOccurrences == 0)
                            {
                                q.Dequeue(); // throw away returned value
                                if (q.Count == 0) this.startEventInformation.Remove(key);
                            }
                            var adjustedTime = firstElement.modifiedStartTime;

                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                return; // drop
                            }
                            else
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                value = new StreamEvent<TPayload>(outOfOrder ? current : value.SyncTime, adjustedTime, value.Payload);
                            }
                        }
                        else if (outOfOrder)
                        {
                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                return; // drop
                            }
                            else
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                value = new StreamEvent<TPayload>(current, value.OtherTime, value.Payload);
                            }
                        }

                        break;
                    default:
                        Contract.Assert(false, "switch meant to be exhaustive");
                        throw new InvalidOperationException("Unsupported stream event kind: " + value.Kind.ToString());
                }
            }

            this.currentBatch.Add(value.SyncTime, value.OtherTime, Empty.Default, value.Payload);
            if (this.currentBatch.Count == Config.DataBatchSize)
            {
                if (this.flushPolicy == FlushPolicy.FlushOnBatchBoundary) OnFlush();
                else FlushContents();
            }

            UpdateCurrentTime(value.SyncTime);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateCurrentTime(long time, bool fromEvent = true)
        {
            if (this.currentTime < time)
            {
                this.currentTime = time;
            }
#if DEBUG
            if (fromEvent && this.lastEventTime < time)
            {
                this.lastEventTime = time;
            }
#endif
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void GenerateAndProcessPunctuation(long syncTime)
        {
            if (syncTime <= this.lastPunctuationTime) return;

            // Update the Punctuation to be at least the currentTime, so the Punctuation
            // is not before the preceding data event.
            syncTime = Math.Max(syncTime, this.currentTime);

            // Update cached global times
            this.highWatermark = Math.Max(syncTime, this.highWatermark);
            UpdateCurrentTime(syncTime);
            this.lastPunctuationTime = Math.Max(
                syncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod),
                this.lastPunctuationTime);

            // Add Punctuation to batch
            var count = this.currentBatch.Count;
            this.currentBatch.vsync.col[count] = syncTime;
            this.currentBatch.vother.col[count] = StreamEvent.PunctuationOtherTime;
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            this.currentBatch.key.col[count] = default;
            this.currentBatch[count] = default;
            this.currentBatch.hash.col[count] = 0;
            this.currentBatch.Count = count + 1;

            // Flush if necessary
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

        protected override void OnCompleted(long punctuationTime)
        {
            GenerateAndProcessPunctuation(punctuationTime);

            // Flush, but if we just flushed due to the punctuation generated above
            if (this.flushPolicy != FlushPolicy.FlushOnPunctuation)
                OnFlush();
        }
    }

    [DataContract]
    internal sealed class FusedStreamEventSubscription<TPayload, TResult> : ObserverSubscriptionBase<StreamEvent<TPayload>, TPayload, TResult>
    {
        [SchemaSerialization]
        private readonly FuseModule fuseModule;
        private readonly Action<long, long, TPayload, Empty> action;
        public FusedStreamEventSubscription() { }

        public FusedStreamEventSubscription(
            IObservable<StreamEvent<TPayload>> observable,
            FuseModule fuseModule,
            string identifier,
            Streamable<Empty, TResult> streamable,
            IStreamObserver<Empty, TResult> observer,
            DisorderPolicy disorderPolicy,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderStreamEvent<TPayload>> diagnosticOutput)
            : base(
                observable,
                identifier,
                streamable,
                observer,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                diagnosticOutput)
        {
            this.fuseModule = fuseModule;
            Expression<Action<long, long, TResult, Empty>> statement = (s, e, p, k) => this.currentBatch.Add(s, e, k, p);
            Expression<Action> flush = () => FlushContents();
            Expression<Func<bool>> test = () => this.currentBatch.Count == Config.DataBatchSize;
            var full = Expression.Lambda<Action<long, long, TResult, Empty>>(
                Expression.Block(
                    statement.Body,
                    Expression.IfThen(test.Body, flush.Body)),
                statement.Parameters);
            var actionExp = fuseModule.Coalesce<TPayload, TResult, Empty>(full);
            this.action = actionExp.Compile();
        }

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(this.startEventInformation != null);
            Contract.Invariant(this.currentTime >= StreamEvent.MinSyncTime);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        public override void OnNext(StreamEvent<TPayload> value)
        {

            if (value.IsPunctuation)
            {
                GenerateAndProcessPunctuation(value.SyncTime);
                return;
            }

            Contract.Assume(value.SyncTime != value.OtherTime);

            long current = this.currentTime;

            var outOfOrder = value.SyncTime < current;
            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                // out of order events shouldn't count, and if the disorder policy adjusts their sync time, then it
                // will be made equal to a timestamp already seen earlier in the sequence and this would have triggered
                // (if necessary) when that timestamp was seen.
                ulong delta = (ulong)(value.SyncTime - this.lastPunctuationTime);
                if (!outOfOrder && delta >= this.punctuationGenerationPeriod)
                {
                    // SyncTime is sufficiently high to generate a new punctuation, but first snap it to the nearest generationPeriod boundary
                    var punctuationTimeQuantized = value.SyncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod);
#if DEBUG
                    Debug.Assert(punctuationTimeQuantized >= LastEventTime(), "Bug in punctuation quantization logic");
#endif
                    OnPunctuation(StreamEvent.CreatePunctuation<TPayload>(punctuationTimeQuantized));
                }
            }

            // check for out of order event
            if (this.disorderPolicyType == DisorderPolicyType.Throw)
            {
                if (outOfOrder)
                {
                    throw new IngressException($"Out-of-order event encountered during ingress, under a disorder policy of Throw: value.SyncTime: {value.SyncTime}, current:{current}");
                }
            }
            else
            {
                // end events and interval events just get dropped
                Tuple<long, TPayload> key;
                ElasticCircularBuffer<AdjustInfo> q;
                switch (value.Kind)
                {
                    case StreamEventKind.Start:
                        if (outOfOrder)
                        {
                            key = Tuple.Create(value.SyncTime, value.Payload);
                            if (!this.startEventInformation.TryGetValue(key, out q))
                            {
                                q = new ElasticCircularBuffer<AdjustInfo>();
                                this.startEventInformation.Add(key, q);
                                var x = new AdjustInfo(current);
                                q.Enqueue(ref x);
                            }
                            else
                            {
                                var last = q.PeekLast();
                                if (last.modifiedStartTime == current) last.numberOfOccurrences++;
                                else
                                {
                                    var x = new AdjustInfo(current);
                                    q.Enqueue(ref x);
                                }
                            }

                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                return; // drop
                            }
                            else
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                value = new StreamEvent<TPayload>(current, StreamEvent.InfinitySyncTime, value.Payload);
                            }
                        }
                        break;

                    case StreamEventKind.Interval:
                        if (outOfOrder)
                        {
                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                return; // drop
                            }
                            else
                            {
                                if (current >= value.OtherTime)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                    return; // drop
                                }

                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                value = new StreamEvent<TPayload>(current, value.OtherTime, value.Payload);
                            }
                        }
                        break;

                    case StreamEventKind.End:
                        // it may not be out of order, but did we drop/adjust the corresponding start event?
                        key = Tuple.Create(value.OtherTime, value.Payload);
                        if (this.startEventInformation.TryGetValue(key, out q))
                        {
                            Contract.Assume(!q.IsEmpty());
                            var firstElement = q.PeekFirst();
                            firstElement.numberOfOccurrences--;
                            if (firstElement.numberOfOccurrences == 0)
                            {
                                q.Dequeue(); // throw away returned value
                                if (q.Count == 0) this.startEventInformation.Remove(key);
                            }
                            var adjustedTime = firstElement.modifiedStartTime;

                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                return; // drop
                            }
                            else
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                value = new StreamEvent<TPayload>(outOfOrder ? current : value.SyncTime, adjustedTime, value.Payload);
                            }
                        }
                        else if (outOfOrder)
                        {
                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                return; // drop
                            }
                            else
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                value = new StreamEvent<TPayload>(current, value.OtherTime, value.Payload);
                            }
                        }

                        break;
                    default:
                        Contract.Assert(false, "switch meant to be exhaustive");
                        throw new InvalidOperationException("Unsupported stream event kind: " + value.Kind.ToString());
                }
            }

            this.action(value.SyncTime, value.OtherTime, value.Payload, Empty.Default);

            UpdateCurrentTime(value.SyncTime);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateCurrentTime(long time, bool fromEvent = true)
        {
            if (this.currentTime < time)
            {
                this.currentTime = time;
            }
#if DEBUG
            if (fromEvent && this.lastEventTime < time)
            {
                this.lastEventTime = time;
            }
#endif
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void GenerateAndProcessPunctuation(long syncTime)
        {
            if (syncTime <= this.lastPunctuationTime) return;

            // Update the Punctuation to be at least the currentTime, so the Punctuation
            // is not before the preceding data event.
            syncTime = Math.Max(syncTime, this.currentTime);

            // Update cached global times
            this.highWatermark = Math.Max(syncTime, this.highWatermark);
            UpdateCurrentTime(syncTime);
            this.lastPunctuationTime = Math.Max(
                syncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod),
                this.lastPunctuationTime);

            // Add Punctuation to batch
            var count = this.currentBatch.Count;
            this.currentBatch.vsync.col[count] = syncTime;
            this.currentBatch.vother.col[count] = StreamEvent.PunctuationOtherTime;
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            this.currentBatch.key.col[count] = default;
            this.currentBatch[count] = default;
            this.currentBatch.hash.col[count] = 0;
            this.currentBatch.Count = count + 1;

            // Flush if necessary
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

        protected override void OnCompleted(long punctuationTime)
        {
            GenerateAndProcessPunctuation(punctuationTime);

            // Flush, but if we just flushed due to the punctuation generated above
            if (this.flushPolicy != FlushPolicy.FlushOnPunctuation)
                OnFlush();
        }
    }

    [DataContract]
    internal sealed class DisorderedStreamEventSubscription<TPayload, TResult> : DisorderedObserverSubscriptionBase<StreamEvent<TPayload>, TPayload, TResult>
    {
        [SchemaSerialization]
        private readonly FuseModule fuseModule;
        private readonly Action<long, long, TPayload, Empty> action;
        public DisorderedStreamEventSubscription() { }

        public DisorderedStreamEventSubscription(
            IObservable<StreamEvent<TPayload>> observable,
            FuseModule fuseModule,
            string identifier,
            Streamable<Empty, TResult> streamable,
            IStreamObserver<Empty, TResult> observer,
            DisorderPolicy disorderPolicy,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderStreamEvent<TPayload>> diagnosticOutput)
            : base(
                observable,
                identifier,
                streamable,
                observer,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                diagnosticOutput)
        {
            this.fuseModule = fuseModule;
            Expression<Action<long, long, TResult, Empty>> statement = (s, e, p, k) => Action(s, e, p, k);
            var actionExp = fuseModule.Coalesce<TPayload, TResult, Empty>(statement, true);
            this.action = actionExp.Compile();
        }

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(this.startEventInformation != null);
            Contract.Invariant(this.currentTime >= StreamEvent.MinSyncTime);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        public override void OnNext(StreamEvent<TPayload> value)
        {
            this.action(value.SyncTime, value.OtherTime, value.Payload, Empty.Default);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void Action(long start, long end, TResult payload, Empty actionKey)
        {
            var value = new StreamEvent<TResult>(start, end, payload);

            if (value.IsPunctuation)
            {
                GenerateAndProcessPunctuation(value.SyncTime);
                return;
            }

            Contract.Assume(value.SyncTime != value.OtherTime);

            long current = this.currentTime;

            var outOfOrder = value.SyncTime < current;
            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                // out of order events shouldn't count, and if the disorder policy adjusts their sync time, then it
                // will be made equal to a timestamp already seen earlier in the sequence and this would have triggered
                // (if necessary) when that timestamp was seen.
                ulong delta = (ulong)(value.SyncTime - this.lastPunctuationTime);
                if (!outOfOrder && delta >= this.punctuationGenerationPeriod)
                {
                    // SyncTime is sufficiently high to generate a new punctuation, but first snap it to the nearest generationPeriod boundary
                    var punctuationTimeQuantized = value.SyncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod);
#if DEBUG
                    Debug.Assert(punctuationTimeQuantized >= LastEventTime(), "Bug in punctuation quantization logic");
#endif
                    OnPunctuation(StreamEvent.CreatePunctuation<TResult>(punctuationTimeQuantized));
                }
            }

            // check for out of order event
            if (this.disorderPolicyType == DisorderPolicyType.Throw)
            {
                if (outOfOrder)
                {
                    throw new IngressException($"Out-of-order event encountered during ingress, under a disorder policy of Throw: value.SyncTime: {value.SyncTime}, current:{current}");
                }
            }
            else
            {
                // end events and interval events just get dropped
                Tuple<long, TResult> key;
                ElasticCircularBuffer<AdjustInfo> q;
                switch (value.Kind)
                {
                    case StreamEventKind.Start:
                        if (outOfOrder)
                        {
                            key = Tuple.Create(value.SyncTime, value.Payload);
                            if (!this.startEventInformation.TryGetValue(key, out q))
                            {
                                q = new ElasticCircularBuffer<AdjustInfo>();
                                this.startEventInformation.Add(key, q);
                                var x = new AdjustInfo(current);
                                q.Enqueue(ref x);
                            }
                            else
                            {
                                var last = q.PeekLast();
                                if (last.modifiedStartTime == current) last.numberOfOccurrences++;
                                else
                                {
                                    var x = new AdjustInfo(current);
                                    q.Enqueue(ref x);
                                }
                            }

                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(new StreamEvent<TPayload>(value.SyncTime, value.OtherTime, default), default));
                                return; // drop
                            }
                            else
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(new StreamEvent<TPayload>(value.SyncTime, value.OtherTime, default), new long?(current - value.SyncTime)));
                                value = new StreamEvent<TResult>(current, StreamEvent.InfinitySyncTime, value.Payload);
                            }
                        }
                        break;

                    case StreamEventKind.Interval:
                        if (outOfOrder)
                        {
                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(new StreamEvent<TPayload>(value.SyncTime, value.OtherTime, default), default));
                                return; // drop
                            }
                            else
                            {
                                if (current >= value.OtherTime)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(new StreamEvent<TPayload>(value.SyncTime, value.OtherTime, default), default));
                                    return; // drop
                                }

                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(new StreamEvent<TPayload>(value.SyncTime, value.OtherTime, default), new long?(current - value.SyncTime)));
                                value = new StreamEvent<TResult>(current, value.OtherTime, value.Payload);
                            }
                        }
                        break;

                    case StreamEventKind.End:
                        // it may not be out of order, but did we drop/adjust the corresponding start event?
                        key = Tuple.Create(value.OtherTime, value.Payload);
                        if (this.startEventInformation.TryGetValue(key, out q))
                        {
                            Contract.Assume(!q.IsEmpty());
                            var firstElement = q.PeekFirst();
                            firstElement.numberOfOccurrences--;
                            if (firstElement.numberOfOccurrences == 0)
                            {
                                q.Dequeue(); // throw away returned value
                                if (q.Count == 0) this.startEventInformation.Remove(key);
                            }
                            var adjustedTime = firstElement.modifiedStartTime;

                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(new StreamEvent<TPayload>(value.SyncTime, value.OtherTime, default), default));
                                return; // drop
                            }
                            else
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(new StreamEvent<TPayload>(value.SyncTime, value.OtherTime, default), new long?(current - value.SyncTime)));
                                value = new StreamEvent<TResult>(outOfOrder ? current : value.SyncTime, adjustedTime, value.Payload);
                            }
                        }
                        else if (outOfOrder)
                        {
                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(new StreamEvent<TPayload>(value.SyncTime, value.OtherTime, default), default));
                                return; // drop
                            }
                            else
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(new StreamEvent<TPayload>(value.SyncTime, value.OtherTime, default), new long?(current - value.SyncTime)));
                                value = new StreamEvent<TResult>(current, value.OtherTime, value.Payload);
                            }
                        }

                        break;
                    default:
                        Contract.Assert(false, "switch meant to be exhaustive");
                        throw new InvalidOperationException("Unsupported stream event kind: " + value.Kind.ToString());
                }
            }

            this.currentBatch.Add(value.SyncTime, value.OtherTime, Empty.Default, value.Payload);
            if (this.currentBatch.Count == Config.DataBatchSize)
            {
                if (this.flushPolicy == FlushPolicy.FlushOnBatchBoundary) OnFlush();
                else FlushContents();
            }

            UpdateCurrentTime(value.SyncTime);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateCurrentTime(long time, bool fromEvent = true)
        {
            if (this.currentTime < time)
            {
                this.currentTime = time;
            }
#if DEBUG
            if (fromEvent && this.lastEventTime < time)
            {
                this.lastEventTime = time;
            }
#endif
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void GenerateAndProcessPunctuation(long syncTime)
        {
            if (syncTime <= this.lastPunctuationTime) return;

            // Update the Punctuation to be at least the currentTime, so the Punctuation
            // is not before the preceding data event.
            syncTime = Math.Max(syncTime, this.currentTime);

            // Update cached global times
            this.highWatermark = Math.Max(syncTime, this.highWatermark);
            UpdateCurrentTime(syncTime);
            this.lastPunctuationTime = Math.Max(
                syncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod),
                this.lastPunctuationTime);

            // Add Punctuation to batch
            var count = this.currentBatch.Count;
            this.currentBatch.vsync.col[count] = syncTime;
            this.currentBatch.vother.col[count] = StreamEvent.PunctuationOtherTime;
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            this.currentBatch.key.col[count] = default;
            this.currentBatch[count] = default;
            this.currentBatch.hash.col[count] = 0;
            this.currentBatch.Count = count + 1;

            // Flush if necessary
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

        protected override void OnCompleted(long punctuationTime)
        {
            GenerateAndProcessPunctuation(punctuationTime);

            // Flush, but if we just flushed due to the punctuation generated above
            if (this.flushPolicy != FlushPolicy.FlushOnPunctuation)
                OnFlush();
        }
    }

    [DataContract]
    internal sealed class SimpleIntervalSubscriptionWithLatency<TPayload> : ObserverSubscriptionBase<TPayload, TPayload, TPayload>
    {
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, long>> startEdgeExtractor;
        private readonly Func<TPayload, long> startEdgeFunction;
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, long>> endEdgeExtractor;
        private readonly Func<TPayload, long> endEdgeFunction;
        public SimpleIntervalSubscriptionWithLatency() { }

        public SimpleIntervalSubscriptionWithLatency(
            IObservable<TPayload> observable,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            string identifier,
            Streamable<Empty, TPayload> streamable,
            IStreamObserver<Empty, TPayload> observer,
            DisorderPolicy disorderPolicy,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderStreamEvent<TPayload>> diagnosticOutput)
            : base(
                observable,
                identifier,
                streamable,
                observer,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
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

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        public override void OnNext(TPayload inputValue)
        {
            var value = StreamEvent.CreateInterval(this.startEdgeFunction(inputValue), this.endEdgeFunction(inputValue), inputValue);

            if (value.IsPunctuation)
            {
                GenerateAndProcessPunctuation(value.SyncTime);
                return;
            }

            long moveFrom = this.currentTime;
            long moveTo = moveFrom;

            // Events at the reorder boundary or earlier - are handled using default processing policies
            if (value.SyncTime <= moveFrom)
            {
                Process(ref value);
                return;
            }

            var oldTime = this.highWatermark;
            if (value.SyncTime > oldTime)
            {
                this.highWatermark = value.SyncTime;

                moveTo = value.SyncTime - this.reorderLatency;
                if (moveTo < StreamEvent.MinSyncTime) moveTo = StreamEvent.MinSyncTime;
                if (moveTo < moveFrom) moveTo = moveFrom;
            }

            if (moveTo > moveFrom)
            {
                if (this.priorityQueueSorter != null)
                {
                    StreamEvent<TPayload> resultEvent;

                    while ((!this.priorityQueueSorter.IsEmpty()) && this.priorityQueueSorter.Peek().SyncTime <= moveTo)
                    {
                        resultEvent = this.priorityQueueSorter.Dequeue();
                        Process(ref resultEvent);
                    }
                }
                else
                {
                    // Extract and process data in-order from impatience, until timestamp of moveTo
                    PooledElasticCircularBuffer<StreamEvent<TPayload>> streamEvents = this.impatienceSorter.DequeueUntil(moveTo, out bool recheck);

                    if (streamEvents != null)
                    {
                        StreamEvent<TPayload> resultEvent;
                        while ((streamEvents.Count > 0) && ((!recheck) || (streamEvents.PeekFirst().SyncTime <= moveTo)))
                        {
                            resultEvent = streamEvents.Dequeue();
                            Process(ref resultEvent);
                        }
                    }

                    if (!recheck && (streamEvents != null))
                        this.impatienceSorter.Return(streamEvents);
                }
            }

            if (value.SyncTime == moveTo)
            {
                Process(ref value);
                return;
            }

            // Enqueue value into impatience
            if (this.priorityQueueSorter != null) this.priorityQueueSorter.Enqueue(value);
            else this.impatienceSorter.Enqueue(ref value);

            UpdateCurrentTime(moveTo, fromEvent: false);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void Process(ref StreamEvent<TPayload> value)
        {
            long current = this.currentTime;

            var outOfOrder = value.SyncTime < current;
            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                // out of order events shouldn't count, and if the disorder policy adjusts their sync time, then it
                // will be made equal to a timestamp already seen earlier in the sequence and this would have triggered
                // (if necessary) when that timestamp was seen.
                ulong delta = (ulong)(value.SyncTime - this.lastPunctuationTime);
                if (!outOfOrder && delta >= this.punctuationGenerationPeriod)
                {
                    // SyncTime is sufficiently high to generate a new punctuation, but first snap it to the nearest generationPeriod boundary
                    var punctuationTimeQuantized = value.SyncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod);
#if DEBUG
                    Debug.Assert(punctuationTimeQuantized >= LastEventTime(), "Bug in punctuation quantization logic");
#endif
                    OnPunctuation(StreamEvent.CreatePunctuation<TPayload>(punctuationTimeQuantized));
                }
            }

            if (this.disorderPolicyType == DisorderPolicyType.Throw)
            {
                if (outOfOrder)
                {
                    throw new IngressException($"Out-of-order event encountered during ingress, under a disorder policy of Throw: value.SyncTime: {value.SyncTime}, current:{current}");
                }
            }
            else
            {
                        if (outOfOrder)
                        {
                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                return; // drop
                            }
                            else
                            {
                                if (current >= value.OtherTime)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                    return; // drop
                                }

                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                value = new StreamEvent<TPayload>(current, value.OtherTime, value.Payload);
                            }
                        }
            }

            this.currentBatch.Add(value.SyncTime, value.OtherTime, Empty.Default, value.Payload);
            if (this.currentBatch.Count == Config.DataBatchSize)
            {
                if (this.flushPolicy == FlushPolicy.FlushOnBatchBoundary) OnFlush();
                else FlushContents();
            }

            UpdateCurrentTime(value.SyncTime);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateCurrentTime(long time, bool fromEvent = true)
        {
            if (this.currentTime < time)
            {
                this.currentTime = time;
            }
#if DEBUG
            if (fromEvent && this.lastEventTime < time)
            {
                this.lastEventTime = time;
            }
#endif
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void GenerateAndProcessPunctuation(long syncTime)
        {
            if (syncTime <= this.lastPunctuationTime) return;

            // Update the Punctuation to be at least the currentTime, so the Punctuation
            // is not before the preceding data event.
            // Note that currentTime only reflects events already processed, and excludes events in the reorder buffer.
            syncTime = Math.Max(syncTime, this.currentTime);

            // Process events queued for reorderLatency up to the Punctuation syncTime
            if (this.priorityQueueSorter != null)
            {
                StreamEvent<TPayload> resultEvent;
                while ((!this.priorityQueueSorter.IsEmpty()) && this.priorityQueueSorter.Peek().SyncTime <= syncTime)
                {
                    resultEvent = this.priorityQueueSorter.Dequeue();
                    Process(ref resultEvent);
                }
            }
            else
            {
                var streamEvents = this.impatienceSorter.DequeueUntil(syncTime, out bool recheck);
                if (streamEvents != null)
                {
                    StreamEvent<TPayload> resultEvent;
                    while ((streamEvents.Count > 0) && ((!recheck) || (streamEvents.PeekFirst().SyncTime <= syncTime)))
                    {
                        resultEvent = streamEvents.Dequeue();
                        Process(ref resultEvent);
                    }
                    if (!recheck) this.impatienceSorter.Return(streamEvents);
                }
            }

            // Update cached global times
            this.highWatermark = Math.Max(syncTime, this.highWatermark);
            UpdateCurrentTime(syncTime);
            this.lastPunctuationTime = Math.Max(
                syncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod),
                this.lastPunctuationTime);

            // Add Punctuation to batch
            var count = this.currentBatch.Count;
            this.currentBatch.vsync.col[count] = syncTime;
            this.currentBatch.vother.col[count] = StreamEvent.PunctuationOtherTime;
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            this.currentBatch.key.col[count] = default;
            this.currentBatch[count] = default;
            this.currentBatch.hash.col[count] = 0;
            this.currentBatch.Count = count + 1;

            // Flush if necessary
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

        protected override void OnCompleted(long punctuationTime)
        {
            GenerateAndProcessPunctuation(punctuationTime);

            // Flush, but if we just flushed due to the punctuation generated above
            if (this.flushPolicy != FlushPolicy.FlushOnPunctuation)
                OnFlush();
        }
    }

    [DataContract]
    internal sealed class FusedIntervalSubscriptionWithLatency<TPayload, TResult> : ObserverSubscriptionBase<TPayload, TPayload, TResult>
    {
        [SchemaSerialization]
        private readonly FuseModule fuseModule;
        private readonly Action<long, long, TPayload, Empty> action;
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, long>> startEdgeExtractor;
        private readonly Func<TPayload, long> startEdgeFunction;
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, long>> endEdgeExtractor;
        private readonly Func<TPayload, long> endEdgeFunction;
        public FusedIntervalSubscriptionWithLatency() { }

        public FusedIntervalSubscriptionWithLatency(
            IObservable<TPayload> observable,
            FuseModule fuseModule,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            string identifier,
            Streamable<Empty, TResult> streamable,
            IStreamObserver<Empty, TResult> observer,
            DisorderPolicy disorderPolicy,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderStreamEvent<TPayload>> diagnosticOutput)
            : base(
                observable,
                identifier,
                streamable,
                observer,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                diagnosticOutput)
        {
            this.fuseModule = fuseModule;
            Expression<Action<long, long, TResult, Empty>> statement = (s, e, p, k) => this.currentBatch.Add(s, e, k, p);
            Expression<Action> flush = () => FlushContents();
            Expression<Func<bool>> test = () => this.currentBatch.Count == Config.DataBatchSize;
            var full = Expression.Lambda<Action<long, long, TResult, Empty>>(
                Expression.Block(
                    statement.Body,
                    Expression.IfThen(test.Body, flush.Body)),
                statement.Parameters);
            var actionExp = fuseModule.Coalesce<TPayload, TResult, Empty>(full);
            this.action = actionExp.Compile();
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

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        public override void OnNext(TPayload inputValue)
        {
            var value = StreamEvent.CreateInterval(this.startEdgeFunction(inputValue), this.endEdgeFunction(inputValue), inputValue);

            if (value.IsPunctuation)
            {
                GenerateAndProcessPunctuation(value.SyncTime);
                return;
            }

            long moveFrom = this.currentTime;
            long moveTo = moveFrom;

            // Events at the reorder boundary or earlier - are handled using default processing policies
            if (value.SyncTime <= moveFrom)
            {
                Process(ref value);
                return;
            }

            var oldTime = this.highWatermark;
            if (value.SyncTime > oldTime)
            {
                this.highWatermark = value.SyncTime;

                moveTo = value.SyncTime - this.reorderLatency;
                if (moveTo < StreamEvent.MinSyncTime) moveTo = StreamEvent.MinSyncTime;
                if (moveTo < moveFrom) moveTo = moveFrom;
            }

            if (moveTo > moveFrom)
            {
                if (this.priorityQueueSorter != null)
                {
                    StreamEvent<TPayload> resultEvent;

                    while ((!this.priorityQueueSorter.IsEmpty()) && this.priorityQueueSorter.Peek().SyncTime <= moveTo)
                    {
                        resultEvent = this.priorityQueueSorter.Dequeue();
                        Process(ref resultEvent);
                    }
                }
                else
                {
                    // Extract and process data in-order from impatience, until timestamp of moveTo
                    PooledElasticCircularBuffer<StreamEvent<TPayload>> streamEvents = this.impatienceSorter.DequeueUntil(moveTo, out bool recheck);

                    if (streamEvents != null)
                    {
                        StreamEvent<TPayload> resultEvent;
                        while ((streamEvents.Count > 0) && ((!recheck) || (streamEvents.PeekFirst().SyncTime <= moveTo)))
                        {
                            resultEvent = streamEvents.Dequeue();
                            Process(ref resultEvent);
                        }
                    }

                    if (!recheck && (streamEvents != null))
                        this.impatienceSorter.Return(streamEvents);
                }
            }

            if (value.SyncTime == moveTo)
            {
                Process(ref value);
                return;
            }

            // Enqueue value into impatience
            if (this.priorityQueueSorter != null) this.priorityQueueSorter.Enqueue(value);
            else this.impatienceSorter.Enqueue(ref value);

            UpdateCurrentTime(moveTo, fromEvent: false);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void Process(ref StreamEvent<TPayload> value)
        {
            long current = this.currentTime;

            var outOfOrder = value.SyncTime < current;
            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                // out of order events shouldn't count, and if the disorder policy adjusts their sync time, then it
                // will be made equal to a timestamp already seen earlier in the sequence and this would have triggered
                // (if necessary) when that timestamp was seen.
                ulong delta = (ulong)(value.SyncTime - this.lastPunctuationTime);
                if (!outOfOrder && delta >= this.punctuationGenerationPeriod)
                {
                    // SyncTime is sufficiently high to generate a new punctuation, but first snap it to the nearest generationPeriod boundary
                    var punctuationTimeQuantized = value.SyncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod);
#if DEBUG
                    Debug.Assert(punctuationTimeQuantized >= LastEventTime(), "Bug in punctuation quantization logic");
#endif
                    OnPunctuation(StreamEvent.CreatePunctuation<TPayload>(punctuationTimeQuantized));
                }
            }

            if (this.disorderPolicyType == DisorderPolicyType.Throw)
            {
                if (outOfOrder)
                {
                    throw new IngressException($"Out-of-order event encountered during ingress, under a disorder policy of Throw: value.SyncTime: {value.SyncTime}, current:{current}");
                }
            }
            else
            {
                        if (outOfOrder)
                        {
                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                return; // drop
                            }
                            else
                            {
                                if (current >= value.OtherTime)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                    return; // drop
                                }

                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                value = new StreamEvent<TPayload>(current, value.OtherTime, value.Payload);
                            }
                        }
            }

            this.action(value.SyncTime, value.OtherTime, value.Payload, Empty.Default);

            UpdateCurrentTime(value.SyncTime);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateCurrentTime(long time, bool fromEvent = true)
        {
            if (this.currentTime < time)
            {
                this.currentTime = time;
            }
#if DEBUG
            if (fromEvent && this.lastEventTime < time)
            {
                this.lastEventTime = time;
            }
#endif
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void GenerateAndProcessPunctuation(long syncTime)
        {
            if (syncTime <= this.lastPunctuationTime) return;

            // Update the Punctuation to be at least the currentTime, so the Punctuation
            // is not before the preceding data event.
            // Note that currentTime only reflects events already processed, and excludes events in the reorder buffer.
            syncTime = Math.Max(syncTime, this.currentTime);

            // Process events queued for reorderLatency up to the Punctuation syncTime
            if (this.priorityQueueSorter != null)
            {
                StreamEvent<TPayload> resultEvent;
                while ((!this.priorityQueueSorter.IsEmpty()) && this.priorityQueueSorter.Peek().SyncTime <= syncTime)
                {
                    resultEvent = this.priorityQueueSorter.Dequeue();
                    Process(ref resultEvent);
                }
            }
            else
            {
                var streamEvents = this.impatienceSorter.DequeueUntil(syncTime, out bool recheck);
                if (streamEvents != null)
                {
                    StreamEvent<TPayload> resultEvent;
                    while ((streamEvents.Count > 0) && ((!recheck) || (streamEvents.PeekFirst().SyncTime <= syncTime)))
                    {
                        resultEvent = streamEvents.Dequeue();
                        Process(ref resultEvent);
                    }
                    if (!recheck) this.impatienceSorter.Return(streamEvents);
                }
            }

            // Update cached global times
            this.highWatermark = Math.Max(syncTime, this.highWatermark);
            UpdateCurrentTime(syncTime);
            this.lastPunctuationTime = Math.Max(
                syncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod),
                this.lastPunctuationTime);

            // Add Punctuation to batch
            var count = this.currentBatch.Count;
            this.currentBatch.vsync.col[count] = syncTime;
            this.currentBatch.vother.col[count] = StreamEvent.PunctuationOtherTime;
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            this.currentBatch.key.col[count] = default;
            this.currentBatch[count] = default;
            this.currentBatch.hash.col[count] = 0;
            this.currentBatch.Count = count + 1;

            // Flush if necessary
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

        protected override void OnCompleted(long punctuationTime)
        {
            GenerateAndProcessPunctuation(punctuationTime);

            // Flush, but if we just flushed due to the punctuation generated above
            if (this.flushPolicy != FlushPolicy.FlushOnPunctuation)
                OnFlush();
        }
    }

    [DataContract]
    internal sealed class DisorderedIntervalSubscriptionWithLatency<TPayload, TResult> : DisorderedObserverSubscriptionBase<TPayload, TPayload, TResult>
    {
        [SchemaSerialization]
        private readonly FuseModule fuseModule;
        private readonly Action<long, long, TPayload, Empty> action;
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, long>> startEdgeExtractor;
        private readonly Func<TPayload, long> startEdgeFunction;
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, long>> endEdgeExtractor;
        private readonly Func<TPayload, long> endEdgeFunction;
        public DisorderedIntervalSubscriptionWithLatency() { }

        public DisorderedIntervalSubscriptionWithLatency(
            IObservable<TPayload> observable,
            FuseModule fuseModule,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            string identifier,
            Streamable<Empty, TResult> streamable,
            IStreamObserver<Empty, TResult> observer,
            DisorderPolicy disorderPolicy,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderStreamEvent<TPayload>> diagnosticOutput)
            : base(
                observable,
                identifier,
                streamable,
                observer,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                diagnosticOutput)
        {
            this.fuseModule = fuseModule;
            Expression<Action<long, long, TResult, Empty>> statement = (s, e, p, k) => Action(s, e, p, k);
            var actionExp = fuseModule.Coalesce<TPayload, TResult, Empty>(statement);
            this.action = actionExp.Compile();
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

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        public override void OnNext(TPayload inputValue)
        {
            this.action(this.startEdgeFunction(inputValue), this.endEdgeFunction(inputValue), inputValue, Empty.Default);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void Action(long start, long end, TResult payload, Empty actionKey)
        {
            var value = new StreamEvent<TResult>(start, end, payload);

            if (value.IsPunctuation)
            {
                GenerateAndProcessPunctuation(value.SyncTime);
                return;
            }

            long moveFrom = this.currentTime;
            long moveTo = moveFrom;

            // Events at the reorder boundary or earlier - are handled using default processing policies
            if (value.SyncTime <= moveFrom)
            {
                Process(ref value);
                return;
            }

            var oldTime = this.highWatermark;
            if (value.SyncTime > oldTime)
            {
                this.highWatermark = value.SyncTime;

                moveTo = value.SyncTime - this.reorderLatency;
                if (moveTo < StreamEvent.MinSyncTime) moveTo = StreamEvent.MinSyncTime;
                if (moveTo < moveFrom) moveTo = moveFrom;
            }

            if (moveTo > moveFrom)
            {
                if (this.priorityQueueSorter != null)
                {
                    StreamEvent<TResult> resultEvent;

                    while ((!this.priorityQueueSorter.IsEmpty()) && this.priorityQueueSorter.Peek().SyncTime <= moveTo)
                    {
                        resultEvent = this.priorityQueueSorter.Dequeue();
                        Process(ref resultEvent);
                    }
                }
                else
                {
                    // Extract and process data in-order from impatience, until timestamp of moveTo
                    PooledElasticCircularBuffer<StreamEvent<TResult>> streamEvents = this.impatienceSorter.DequeueUntil(moveTo, out bool recheck);

                    if (streamEvents != null)
                    {
                        StreamEvent<TResult> resultEvent;
                        while ((streamEvents.Count > 0) && ((!recheck) || (streamEvents.PeekFirst().SyncTime <= moveTo)))
                        {
                            resultEvent = streamEvents.Dequeue();
                            Process(ref resultEvent);
                        }
                    }

                    if (!recheck && (streamEvents != null))
                        this.impatienceSorter.Return(streamEvents);
                }
            }

            if (value.SyncTime == moveTo)
            {
                Process(ref value);
                return;
            }

            // Enqueue value into impatience
            if (this.priorityQueueSorter != null) this.priorityQueueSorter.Enqueue(value);
            else this.impatienceSorter.Enqueue(ref value);

            UpdateCurrentTime(moveTo, fromEvent: false);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void Process(ref StreamEvent<TResult> value)
        {
            long current = this.currentTime;

            var outOfOrder = value.SyncTime < current;
            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                // out of order events shouldn't count, and if the disorder policy adjusts their sync time, then it
                // will be made equal to a timestamp already seen earlier in the sequence and this would have triggered
                // (if necessary) when that timestamp was seen.
                ulong delta = (ulong)(value.SyncTime - this.lastPunctuationTime);
                if (!outOfOrder && delta >= this.punctuationGenerationPeriod)
                {
                    // SyncTime is sufficiently high to generate a new punctuation, but first snap it to the nearest generationPeriod boundary
                    var punctuationTimeQuantized = value.SyncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod);
#if DEBUG
                    Debug.Assert(punctuationTimeQuantized >= LastEventTime(), "Bug in punctuation quantization logic");
#endif
                    OnPunctuation(StreamEvent.CreatePunctuation<TResult>(punctuationTimeQuantized));
                }
            }

            if (this.disorderPolicyType == DisorderPolicyType.Throw)
            {
                if (outOfOrder)
                {
                    throw new IngressException($"Out-of-order event encountered during ingress, under a disorder policy of Throw: value.SyncTime: {value.SyncTime}, current:{current}");
                }
            }
            else
            {
                        if (outOfOrder)
                        {
                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(new StreamEvent<TPayload>(value.SyncTime, value.OtherTime, default), default));
                                return; // drop
                            }
                            else
                            {
                                if (current >= value.OtherTime)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(new StreamEvent<TPayload>(value.SyncTime, value.OtherTime, default), default));
                                    return; // drop
                                }

                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(new StreamEvent<TPayload>(value.SyncTime, value.OtherTime, default), new long?(current - value.SyncTime)));
                                value = new StreamEvent<TResult>(current, value.OtherTime, value.Payload);
                            }
                        }
            }

            this.currentBatch.Add(value.SyncTime, value.OtherTime, Empty.Default, value.Payload);
            if (this.currentBatch.Count == Config.DataBatchSize)
            {
                if (this.flushPolicy == FlushPolicy.FlushOnBatchBoundary) OnFlush();
                else FlushContents();
            }

            UpdateCurrentTime(value.SyncTime);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateCurrentTime(long time, bool fromEvent = true)
        {
            if (this.currentTime < time)
            {
                this.currentTime = time;
            }
#if DEBUG
            if (fromEvent && this.lastEventTime < time)
            {
                this.lastEventTime = time;
            }
#endif
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void GenerateAndProcessPunctuation(long syncTime)
        {
            if (syncTime <= this.lastPunctuationTime) return;

            // Update the Punctuation to be at least the currentTime, so the Punctuation
            // is not before the preceding data event.
            // Note that currentTime only reflects events already processed, and excludes events in the reorder buffer.
            syncTime = Math.Max(syncTime, this.currentTime);

            // Process events queued for reorderLatency up to the Punctuation syncTime
            if (this.priorityQueueSorter != null)
            {
                StreamEvent<TResult> resultEvent;
                while ((!this.priorityQueueSorter.IsEmpty()) && this.priorityQueueSorter.Peek().SyncTime <= syncTime)
                {
                    resultEvent = this.priorityQueueSorter.Dequeue();
                    Process(ref resultEvent);
                }
            }
            else
            {
                var streamEvents = this.impatienceSorter.DequeueUntil(syncTime, out bool recheck);
                if (streamEvents != null)
                {
                    StreamEvent<TResult> resultEvent;
                    while ((streamEvents.Count > 0) && ((!recheck) || (streamEvents.PeekFirst().SyncTime <= syncTime)))
                    {
                        resultEvent = streamEvents.Dequeue();
                        Process(ref resultEvent);
                    }
                    if (!recheck) this.impatienceSorter.Return(streamEvents);
                }
            }

            // Update cached global times
            this.highWatermark = Math.Max(syncTime, this.highWatermark);
            UpdateCurrentTime(syncTime);
            this.lastPunctuationTime = Math.Max(
                syncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod),
                this.lastPunctuationTime);

            // Add Punctuation to batch
            var count = this.currentBatch.Count;
            this.currentBatch.vsync.col[count] = syncTime;
            this.currentBatch.vother.col[count] = StreamEvent.PunctuationOtherTime;
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            this.currentBatch.key.col[count] = default;
            this.currentBatch[count] = default;
            this.currentBatch.hash.col[count] = 0;
            this.currentBatch.Count = count + 1;

            // Flush if necessary
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

        protected override void OnCompleted(long punctuationTime)
        {
            GenerateAndProcessPunctuation(punctuationTime);

            // Flush, but if we just flushed due to the punctuation generated above
            if (this.flushPolicy != FlushPolicy.FlushOnPunctuation)
                OnFlush();
        }
    }

    [DataContract]
    internal sealed class SimpleIntervalSubscription<TPayload> : ObserverSubscriptionBase<TPayload, TPayload, TPayload>
    {
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, long>> startEdgeExtractor;
        private readonly Func<TPayload, long> startEdgeFunction;
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, long>> endEdgeExtractor;
        private readonly Func<TPayload, long> endEdgeFunction;
        public SimpleIntervalSubscription() { }

        public SimpleIntervalSubscription(
            IObservable<TPayload> observable,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            string identifier,
            Streamable<Empty, TPayload> streamable,
            IStreamObserver<Empty, TPayload> observer,
            DisorderPolicy disorderPolicy,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderStreamEvent<TPayload>> diagnosticOutput)
            : base(
                observable,
                identifier,
                streamable,
                observer,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
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

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        public override void OnNext(TPayload inputValue)
        {
            var value = StreamEvent.CreateInterval(this.startEdgeFunction(inputValue), this.endEdgeFunction(inputValue), inputValue);

            if (value.IsPunctuation)
            {
                GenerateAndProcessPunctuation(value.SyncTime);
                return;
            }

            long current = this.currentTime;

            var outOfOrder = value.SyncTime < current;
            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                // out of order events shouldn't count, and if the disorder policy adjusts their sync time, then it
                // will be made equal to a timestamp already seen earlier in the sequence and this would have triggered
                // (if necessary) when that timestamp was seen.
                ulong delta = (ulong)(value.SyncTime - this.lastPunctuationTime);
                if (!outOfOrder && delta >= this.punctuationGenerationPeriod)
                {
                    // SyncTime is sufficiently high to generate a new punctuation, but first snap it to the nearest generationPeriod boundary
                    var punctuationTimeQuantized = value.SyncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod);
#if DEBUG
                    Debug.Assert(punctuationTimeQuantized >= LastEventTime(), "Bug in punctuation quantization logic");
#endif
                    OnPunctuation(StreamEvent.CreatePunctuation<TPayload>(punctuationTimeQuantized));
                }
            }

            if (this.disorderPolicyType == DisorderPolicyType.Throw)
            {
                if (outOfOrder)
                {
                    throw new IngressException($"Out-of-order event encountered during ingress, under a disorder policy of Throw: value.SyncTime: {value.SyncTime}, current:{current}");
                }
            }
            else
            {
                        if (outOfOrder)
                        {
                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                return; // drop
                            }
                            else
                            {
                                if (current >= value.OtherTime)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                    return; // drop
                                }

                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                value = new StreamEvent<TPayload>(current, value.OtherTime, value.Payload);
                            }
                        }
            }

            this.currentBatch.Add(value.SyncTime, value.OtherTime, Empty.Default, value.Payload);
            if (this.currentBatch.Count == Config.DataBatchSize)
            {
                if (this.flushPolicy == FlushPolicy.FlushOnBatchBoundary) OnFlush();
                else FlushContents();
            }

            UpdateCurrentTime(value.SyncTime);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateCurrentTime(long time, bool fromEvent = true)
        {
            if (this.currentTime < time)
            {
                this.currentTime = time;
            }
#if DEBUG
            if (fromEvent && this.lastEventTime < time)
            {
                this.lastEventTime = time;
            }
#endif
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void GenerateAndProcessPunctuation(long syncTime)
        {
            if (syncTime <= this.lastPunctuationTime) return;

            // Update the Punctuation to be at least the currentTime, so the Punctuation
            // is not before the preceding data event.
            syncTime = Math.Max(syncTime, this.currentTime);

            // Update cached global times
            this.highWatermark = Math.Max(syncTime, this.highWatermark);
            UpdateCurrentTime(syncTime);
            this.lastPunctuationTime = Math.Max(
                syncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod),
                this.lastPunctuationTime);

            // Add Punctuation to batch
            var count = this.currentBatch.Count;
            this.currentBatch.vsync.col[count] = syncTime;
            this.currentBatch.vother.col[count] = StreamEvent.PunctuationOtherTime;
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            this.currentBatch.key.col[count] = default;
            this.currentBatch[count] = default;
            this.currentBatch.hash.col[count] = 0;
            this.currentBatch.Count = count + 1;

            // Flush if necessary
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

        protected override void OnCompleted(long punctuationTime)
        {
            GenerateAndProcessPunctuation(punctuationTime);

            // Flush, but if we just flushed due to the punctuation generated above
            if (this.flushPolicy != FlushPolicy.FlushOnPunctuation)
                OnFlush();
        }
    }

    [DataContract]
    internal sealed class FusedIntervalSubscription<TPayload, TResult> : ObserverSubscriptionBase<TPayload, TPayload, TResult>
    {
        [SchemaSerialization]
        private readonly FuseModule fuseModule;
        private readonly Action<long, long, TPayload, Empty> action;
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, long>> startEdgeExtractor;
        private readonly Func<TPayload, long> startEdgeFunction;
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, long>> endEdgeExtractor;
        private readonly Func<TPayload, long> endEdgeFunction;
        public FusedIntervalSubscription() { }

        public FusedIntervalSubscription(
            IObservable<TPayload> observable,
            FuseModule fuseModule,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            string identifier,
            Streamable<Empty, TResult> streamable,
            IStreamObserver<Empty, TResult> observer,
            DisorderPolicy disorderPolicy,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderStreamEvent<TPayload>> diagnosticOutput)
            : base(
                observable,
                identifier,
                streamable,
                observer,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                diagnosticOutput)
        {
            this.fuseModule = fuseModule;
            Expression<Action<long, long, TResult, Empty>> statement = (s, e, p, k) => this.currentBatch.Add(s, e, k, p);
            Expression<Action> flush = () => FlushContents();
            Expression<Func<bool>> test = () => this.currentBatch.Count == Config.DataBatchSize;
            var full = Expression.Lambda<Action<long, long, TResult, Empty>>(
                Expression.Block(
                    statement.Body,
                    Expression.IfThen(test.Body, flush.Body)),
                statement.Parameters);
            var actionExp = fuseModule.Coalesce<TPayload, TResult, Empty>(full);
            this.action = actionExp.Compile();
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

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        public override void OnNext(TPayload inputValue)
        {
            var value = StreamEvent.CreateInterval(this.startEdgeFunction(inputValue), this.endEdgeFunction(inputValue), inputValue);

            if (value.IsPunctuation)
            {
                GenerateAndProcessPunctuation(value.SyncTime);
                return;
            }

            long current = this.currentTime;

            var outOfOrder = value.SyncTime < current;
            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                // out of order events shouldn't count, and if the disorder policy adjusts their sync time, then it
                // will be made equal to a timestamp already seen earlier in the sequence and this would have triggered
                // (if necessary) when that timestamp was seen.
                ulong delta = (ulong)(value.SyncTime - this.lastPunctuationTime);
                if (!outOfOrder && delta >= this.punctuationGenerationPeriod)
                {
                    // SyncTime is sufficiently high to generate a new punctuation, but first snap it to the nearest generationPeriod boundary
                    var punctuationTimeQuantized = value.SyncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod);
#if DEBUG
                    Debug.Assert(punctuationTimeQuantized >= LastEventTime(), "Bug in punctuation quantization logic");
#endif
                    OnPunctuation(StreamEvent.CreatePunctuation<TPayload>(punctuationTimeQuantized));
                }
            }

            if (this.disorderPolicyType == DisorderPolicyType.Throw)
            {
                if (outOfOrder)
                {
                    throw new IngressException($"Out-of-order event encountered during ingress, under a disorder policy of Throw: value.SyncTime: {value.SyncTime}, current:{current}");
                }
            }
            else
            {
                        if (outOfOrder)
                        {
                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                return; // drop
                            }
                            else
                            {
                                if (current >= value.OtherTime)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, default));
                                    return; // drop
                                }

                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                value = new StreamEvent<TPayload>(current, value.OtherTime, value.Payload);
                            }
                        }
            }

            this.action(value.SyncTime, value.OtherTime, value.Payload, Empty.Default);

            UpdateCurrentTime(value.SyncTime);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateCurrentTime(long time, bool fromEvent = true)
        {
            if (this.currentTime < time)
            {
                this.currentTime = time;
            }
#if DEBUG
            if (fromEvent && this.lastEventTime < time)
            {
                this.lastEventTime = time;
            }
#endif
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void GenerateAndProcessPunctuation(long syncTime)
        {
            if (syncTime <= this.lastPunctuationTime) return;

            // Update the Punctuation to be at least the currentTime, so the Punctuation
            // is not before the preceding data event.
            syncTime = Math.Max(syncTime, this.currentTime);

            // Update cached global times
            this.highWatermark = Math.Max(syncTime, this.highWatermark);
            UpdateCurrentTime(syncTime);
            this.lastPunctuationTime = Math.Max(
                syncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod),
                this.lastPunctuationTime);

            // Add Punctuation to batch
            var count = this.currentBatch.Count;
            this.currentBatch.vsync.col[count] = syncTime;
            this.currentBatch.vother.col[count] = StreamEvent.PunctuationOtherTime;
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            this.currentBatch.key.col[count] = default;
            this.currentBatch[count] = default;
            this.currentBatch.hash.col[count] = 0;
            this.currentBatch.Count = count + 1;

            // Flush if necessary
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

        protected override void OnCompleted(long punctuationTime)
        {
            GenerateAndProcessPunctuation(punctuationTime);

            // Flush, but if we just flushed due to the punctuation generated above
            if (this.flushPolicy != FlushPolicy.FlushOnPunctuation)
                OnFlush();
        }
    }

    [DataContract]
    internal sealed class DisorderedIntervalSubscription<TPayload, TResult> : DisorderedObserverSubscriptionBase<TPayload, TPayload, TResult>
    {
        [SchemaSerialization]
        private readonly FuseModule fuseModule;
        private readonly Action<long, long, TPayload, Empty> action;
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, long>> startEdgeExtractor;
        private readonly Func<TPayload, long> startEdgeFunction;
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, long>> endEdgeExtractor;
        private readonly Func<TPayload, long> endEdgeFunction;
        public DisorderedIntervalSubscription() { }

        public DisorderedIntervalSubscription(
            IObservable<TPayload> observable,
            FuseModule fuseModule,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            string identifier,
            Streamable<Empty, TResult> streamable,
            IStreamObserver<Empty, TResult> observer,
            DisorderPolicy disorderPolicy,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderStreamEvent<TPayload>> diagnosticOutput)
            : base(
                observable,
                identifier,
                streamable,
                observer,
                disorderPolicy,
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                diagnosticOutput)
        {
            this.fuseModule = fuseModule;
            Expression<Action<long, long, TResult, Empty>> statement = (s, e, p, k) => Action(s, e, p, k);
            var actionExp = fuseModule.Coalesce<TPayload, TResult, Empty>(statement);
            this.action = actionExp.Compile();
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

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        public override void OnNext(TPayload inputValue)
        {
            this.action(this.startEdgeFunction(inputValue), this.endEdgeFunction(inputValue), inputValue, Empty.Default);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void Action(long start, long end, TResult payload, Empty actionKey)
        {
            var value = new StreamEvent<TResult>(start, end, payload);

            if (value.IsPunctuation)
            {
                GenerateAndProcessPunctuation(value.SyncTime);
                return;
            }

            long current = this.currentTime;

            var outOfOrder = value.SyncTime < current;
            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                // out of order events shouldn't count, and if the disorder policy adjusts their sync time, then it
                // will be made equal to a timestamp already seen earlier in the sequence and this would have triggered
                // (if necessary) when that timestamp was seen.
                ulong delta = (ulong)(value.SyncTime - this.lastPunctuationTime);
                if (!outOfOrder && delta >= this.punctuationGenerationPeriod)
                {
                    // SyncTime is sufficiently high to generate a new punctuation, but first snap it to the nearest generationPeriod boundary
                    var punctuationTimeQuantized = value.SyncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod);
#if DEBUG
                    Debug.Assert(punctuationTimeQuantized >= LastEventTime(), "Bug in punctuation quantization logic");
#endif
                    OnPunctuation(StreamEvent.CreatePunctuation<TResult>(punctuationTimeQuantized));
                }
            }

            if (this.disorderPolicyType == DisorderPolicyType.Throw)
            {
                if (outOfOrder)
                {
                    throw new IngressException($"Out-of-order event encountered during ingress, under a disorder policy of Throw: value.SyncTime: {value.SyncTime}, current:{current}");
                }
            }
            else
            {
                        if (outOfOrder)
                        {
                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(new StreamEvent<TPayload>(value.SyncTime, value.OtherTime, default), default));
                                return; // drop
                            }
                            else
                            {
                                if (current >= value.OtherTime)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(new StreamEvent<TPayload>(value.SyncTime, value.OtherTime, default), default));
                                    return; // drop
                                }

                                this.diagnosticOutput?.OnNext(OutOfOrderStreamEvent.Create(new StreamEvent<TPayload>(value.SyncTime, value.OtherTime, default), new long?(current - value.SyncTime)));
                                value = new StreamEvent<TResult>(current, value.OtherTime, value.Payload);
                            }
                        }
            }

            this.currentBatch.Add(value.SyncTime, value.OtherTime, Empty.Default, value.Payload);
            if (this.currentBatch.Count == Config.DataBatchSize)
            {
                if (this.flushPolicy == FlushPolicy.FlushOnBatchBoundary) OnFlush();
                else FlushContents();
            }

            UpdateCurrentTime(value.SyncTime);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateCurrentTime(long time, bool fromEvent = true)
        {
            if (this.currentTime < time)
            {
                this.currentTime = time;
            }
#if DEBUG
            if (fromEvent && this.lastEventTime < time)
            {
                this.lastEventTime = time;
            }
#endif
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void GenerateAndProcessPunctuation(long syncTime)
        {
            if (syncTime <= this.lastPunctuationTime) return;

            // Update the Punctuation to be at least the currentTime, so the Punctuation
            // is not before the preceding data event.
            syncTime = Math.Max(syncTime, this.currentTime);

            // Update cached global times
            this.highWatermark = Math.Max(syncTime, this.highWatermark);
            UpdateCurrentTime(syncTime);
            this.lastPunctuationTime = Math.Max(
                syncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod),
                this.lastPunctuationTime);

            // Add Punctuation to batch
            var count = this.currentBatch.Count;
            this.currentBatch.vsync.col[count] = syncTime;
            this.currentBatch.vother.col[count] = StreamEvent.PunctuationOtherTime;
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            this.currentBatch.key.col[count] = default;
            this.currentBatch[count] = default;
            this.currentBatch.hash.col[count] = 0;
            this.currentBatch.Count = count + 1;

            // Flush if necessary
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

        protected override void OnCompleted(long punctuationTime)
        {
            GenerateAndProcessPunctuation(punctuationTime);

            // Flush, but if we just flushed due to the punctuation generated above
            if (this.flushPolicy != FlushPolicy.FlushOnPunctuation)
                OnFlush();
        }
    }

    [DataContract]
    internal sealed class SimplePartitionedStreamEventSubscriptionWithLatency<TKey, TPayload> : PartitionedObserverSubscriptionBase<TKey, PartitionedStreamEvent<TKey, TPayload>, TPayload, TPayload>
    {
        public SimplePartitionedStreamEventSubscriptionWithLatency() { }

        public SimplePartitionedStreamEventSubscriptionWithLatency(
            IObservable<PartitionedStreamEvent<TKey, TPayload>> observable,
            string identifier,
            Streamable<PartitionKey<TKey>, TPayload> streamable,
            IStreamObserver<PartitionKey<TKey>, TPayload> observer,
            DisorderPolicy disorderPolicy,
            PartitionedFlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            PeriodicLowWatermarkPolicy lowWatermarkPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>> diagnosticOutput)
            : base(
                observable,
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
        }

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(this.startEventInformation != null);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        public override void OnNext(PartitionedStreamEvent<TKey, TPayload> value)
        {

            if (value.IsLowWatermark)
            {
                GenerateAndProcessLowWatermark(value.SyncTime);
                return;
            }

            // Check to see if we need to generate a low watermark due to PeriodicLowWatermarkPolicy
            if (this.lowWatermarkPolicyType == PeriodicLowWatermarkPolicyType.Time &&
                value.SyncTime > this.lowWatermarkTimestampLag)
            {
                var newLowWatermark = value.SyncTime - this.lowWatermarkTimestampLag;
                if ((ulong)(newLowWatermark - this.lowWatermark.quantizedForLowWatermarkGeneration) >= this.lowWatermarkGenerationPeriod)
                {
                    // SyncTime is sufficiently high to generate a new watermark, but first snap it to the nearest generationPeriod boundary
                    var newLowWatermarkSnapped = newLowWatermark.SnapToLeftBoundary((long)this.lowWatermarkGenerationPeriod);
                    GenerateAndProcessLowWatermark(newLowWatermarkSnapped);
                }
            }

            if (!this.currentTime.TryGetValue(value.PartitionKey, out long moveFrom) || moveFrom < this.lowWatermark.rawValue)
            {
                moveFrom = this.lowWatermark.rawValue;
            }

            if (!this.partitionHighWatermarks.ContainsKey(value.PartitionKey))
            {
                this.partitionHighWatermarks.Add(value.PartitionKey, this.lowWatermark.rawValue);

                if (this.highWatermarkToPartitionsMap.TryGetValue(this.lowWatermark.rawValue, out HashSet<TKey> keySet)) keySet.Add(value.PartitionKey);
                else this.highWatermarkToPartitionsMap.Add(this.lowWatermark.rawValue, new HashSet<TKey> { value.PartitionKey });
            }
            long moveTo = moveFrom;

            // Events at the reorder boundary or earlier - are handled using default processing policies
            if (value.SyncTime <= moveFrom)
            {
                Process(ref value);
                return;
            }

            var oldTime = this.partitionHighWatermarks[value.PartitionKey];
            if (value.SyncTime > oldTime)
            {
                this.partitionHighWatermarks[value.PartitionKey] = value.SyncTime;

                var oldSet = this.highWatermarkToPartitionsMap[oldTime];
                if (oldSet.Count <= 1) this.highWatermarkToPartitionsMap.Remove(oldTime);
                else oldSet.Remove(value.PartitionKey);

                if (this.highWatermarkToPartitionsMap.TryGetValue(value.SyncTime, out HashSet<TKey> set)) set.Add(value.PartitionKey);
                else this.highWatermarkToPartitionsMap.Add(value.SyncTime, new HashSet<TKey> { value.PartitionKey });

                if (value.IsData)
                {
                    // moveTo for punctuations is updated below
                    moveTo = value.SyncTime - this.reorderLatency;
                    if (moveTo < StreamEvent.MinSyncTime) moveTo = StreamEvent.MinSyncTime;
                    if (moveTo < moveFrom) moveTo = moveFrom;
                }
            }

            if (value.IsPunctuation)
            {
                moveTo = Math.Max(moveFrom, value.SyncTime);
            }

            if (moveTo > moveFrom)
            {
                if (this.priorityQueueSorter != null)
                {
                    PartitionedStreamEvent<TKey, TPayload> resultEvent;

                    while ((!this.priorityQueueSorter.IsEmpty()) && this.priorityQueueSorter.Peek().SyncTime <= moveTo)
                    {
                        resultEvent = this.priorityQueueSorter.Dequeue();
                        Process(ref resultEvent);
                    }
                }
                else
                {
                    // Extract and process data in-order from impatience, until timestamp of moveTo
                    PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>> streamEvents = this.impatienceSorter.DequeueUntil(value.PartitionKey, moveTo, out bool recheck);

                    if (streamEvents != null)
                    {
                        PartitionedStreamEvent<TKey, TPayload> resultEvent;
                        while ((streamEvents.Count > 0) && ((!recheck) || (streamEvents.PeekFirst().SyncTime <= moveTo)))
                        {
                            resultEvent = streamEvents.Dequeue();
                            Process(ref resultEvent);
                        }
                    }

                    if (!recheck && (streamEvents != null))
                        this.impatienceSorter.Return(value.PartitionKey, streamEvents);
                }
            }

            if (value.SyncTime == moveTo)
            {
                Process(ref value);
                return;
            }

            // Enqueue value into impatience
            if (this.priorityQueueSorter != null) this.priorityQueueSorter.Enqueue(value);
            else this.impatienceSorter.Enqueue(ref value);

            UpdateCurrentTime(value.PartitionKey, moveTo, fromEvent: false);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void Process(ref PartitionedStreamEvent<TKey, TPayload> value)
        {
            Contract.Assume(value.SyncTime != value.OtherTime);

            if (value.IsLowWatermark)
            {
                GenerateAndProcessLowWatermark(value.SyncTime);
                return;
            }

            // Update global high water mark if necessary
            this.highWatermark = Math.Max(this.highWatermark, value.SyncTime);

            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time && !this.lastPunctuationTime.ContainsKey(value.PartitionKey))
                UpdatePunctuation(value.PartitionKey, this.lowWatermark.rawValue, this.lowWatermark.quantizedForPunctuationGeneration);

            // Retrieve current time for this partition, updating currentTime if necessary
            if (!this.currentTime.TryGetValue(value.PartitionKey, out long current))
            {
                current = this.lowWatermark.rawValue;
            }
            else if (current < this.lowWatermark.rawValue)
            {
                current = this.lowWatermark.rawValue;
                UpdateCurrentTime(value.PartitionKey, this.lowWatermark.rawValue);
            }

            var outOfOrder = value.SyncTime < current;
            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                // Track punctuation
                if (value.IsPunctuation && value.SyncTime > this.lastPunctuationTime[value.PartitionKey].lastPunctuation)
                    UpdatePunctuation(value.PartitionKey, value.SyncTime);

                // out of order events shouldn't count, and if the disorder policy adjusts their sync time, then it
                // will be made equal to a timestamp already seen earlier in the sequence and this would have triggered
                // (if necessary) when that timestamp was seen.
                if (!outOfOrder && this.punctuationGenerationPeriod > 0)
                {
                    // We use lowWatermark as the baseline in the delta computation because a low watermark implies
                    // punctuations for all partitions
                    var prevPunctuation = Math.Max(this.lastPunctuationTime[value.PartitionKey].lastPunctuationQuantized, this.lowWatermark.quantizedForPunctuationGeneration);
                    if ((ulong)(value.SyncTime - prevPunctuation) >= this.punctuationGenerationPeriod)
                    {
                        // SyncTime is sufficiently high to generate a new punctuation, but first snap it to the nearest generationPeriod boundary
                        var punctuationTimeQuantized = value.SyncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod);
#if DEBUG
                        Debug.Assert(punctuationTimeQuantized >= LastEventTime(value.PartitionKey), "Bug in punctuation quantization logic");
#endif
                        OnPunctuation(value.CreatePunctuation(punctuationTimeQuantized));
                    }
                }
            }

            // check for out of order event
            if (value.IsPunctuation)
            {
                OnPunctuation(value.CreatePunctuation(outOfOrder ? current : value.SyncTime));
            }
            else
            {
                if (this.disorderPolicyType == DisorderPolicyType.Throw)
                {
                    if (outOfOrder)
                    {
                        throw new IngressException($"Out-of-order event encountered during ingress, under a disorder policy of Throw: value.SyncTime: {value.SyncTime}, current:{current}");
                    }
                }
                else
                {
                    // end events and interval events just get dropped
                    Tuple<long, TPayload> key;
                    ElasticCircularBuffer<AdjustInfo> q;
                    switch (value.Kind)
                    {
                        case StreamEventKind.Start:
                            if (outOfOrder)
                            {
                                key = Tuple.Create(value.SyncTime, value.Payload);
                                if (!this.startEventInformation.TryGetValue(key, out q))
                                {
                                    q = new ElasticCircularBuffer<AdjustInfo>();
                                    this.startEventInformation.Add(key, q);
                                    var x = new AdjustInfo(current);
                                    q.Enqueue(ref x);
                                }
                                else
                                {
                                    var last = q.PeekLast();
                                    if (last.modifiedStartTime == current) last.numberOfOccurrences++;
                                    else
                                    {
                                        var x = new AdjustInfo(current);
                                        q.Enqueue(ref x);
                                    }
                                }
    
                                if (this.disorderPolicyType == DisorderPolicyType.Drop)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                    return; // drop
                                }
                                else
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                    value = new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, current, StreamEvent.InfinitySyncTime, value.Payload);
                                }
                            }
                            break;
    
                        case StreamEventKind.Interval:
                            if (outOfOrder)
                            {
                                if (this.disorderPolicyType == DisorderPolicyType.Drop)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                    return; // drop
                                }
                                else
                                {
                                    if (current >= value.OtherTime)
                                    {
                                        this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                        return; // drop
                                    }
    
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                    value = new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, current, value.OtherTime, value.Payload);
                                }
                            }
                            break;
    
                        case StreamEventKind.End:
                            // it may not be out of order, but did we drop/adjust the corresponding start event?
                            key = Tuple.Create(value.OtherTime, value.Payload);
                            if (this.startEventInformation.TryGetValue(key, out q))
                            {
                                Contract.Assume(!q.IsEmpty());
                                var firstElement = q.PeekFirst();
                                firstElement.numberOfOccurrences--;
                                if (firstElement.numberOfOccurrences == 0)
                                {
                                    q.Dequeue(); // throw away returned value
                                    if (q.Count == 0) this.startEventInformation.Remove(key);
                                }
                                var adjustedTime = firstElement.modifiedStartTime;
    
                                if (this.disorderPolicyType == DisorderPolicyType.Drop)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                    return; // drop
                                }
                                else
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                    value = new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, outOfOrder ? current : value.SyncTime, adjustedTime, value.Payload);
                                }
                            }
                            else if (outOfOrder)
                            {
                                if (this.disorderPolicyType == DisorderPolicyType.Drop)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                    return; // drop
                                }
                                else
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                    value = new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, current, value.OtherTime, value.Payload);
                                }
                            }
    
                            break;
                        default:
                            Contract.Assert(false, "switch meant to be exhaustive");
                            throw new InvalidOperationException("Unsupported stream event kind: " + value.Kind.ToString());
                    }
                }
    
                this.currentBatch.Add(value.SyncTime, value.OtherTime, new PartitionKey<TKey>(value.PartitionKey), value.Payload);
                if (this.currentBatch.Count == Config.DataBatchSize)
                {
                    if (this.flushPolicy == PartitionedFlushPolicy.FlushOnBatchBoundary) OnFlush();
                    else FlushContents();
                }
            }

            UpdateCurrentTime(value.PartitionKey, value.SyncTime);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateCurrentTime(TKey partitionKey, long time, bool fromEvent = true)
        {
            if (!this.currentTime.TryGetValue(partitionKey, out long oldCurrentTime) || oldCurrentTime < time)
            {
                this.currentTime[partitionKey] = time;
            }
#if DEBUG
            if (fromEvent && (!this.lastEventTime.TryGetValue(partitionKey, out long oldEventTime) || oldEventTime < time))
            {
                this.lastEventTime[partitionKey] = time;
            }
#endif
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void GenerateAndProcessLowWatermark(long syncTime)
        {
            if (syncTime <= this.lowWatermark.rawValue) return;

            // Process events queued for reorderLatency up to the LowWatermark syncTime
            if (this.priorityQueueSorter != null)
            {
                PartitionedStreamEvent<TKey, TPayload> resultEvent;
                while ((!this.priorityQueueSorter.IsEmpty()) && this.priorityQueueSorter.Peek().SyncTime <= syncTime)
                {
                    resultEvent = this.priorityQueueSorter.Dequeue();
                    Process(ref resultEvent);
                }
            }
            else
            {
                bool recheck;
                var events = this.impatienceSorter.DequeueUntil(syncTime);

                int index = FastDictionary<TKey, Tuple<bool, PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>>>>.IteratorStart;
                while (events.Iterate(ref index))
                {
                    var entry = events.entries[index];
                    recheck = entry.value.Item1;
                    var streamEvents = entry.value.Item2;
                    if (streamEvents != null)
                    {
                        PartitionedStreamEvent<TKey, TPayload> resultEvent;
                        while ((streamEvents.Count > 0) && ((!recheck) || (streamEvents.PeekFirst().SyncTime <= syncTime)))
                        {
                            resultEvent = streamEvents.Dequeue();
                            Process(ref resultEvent);
                        }
                        if (!recheck) this.impatienceSorter.Return(entry.key, streamEvents);
                    }
                }
            }

            // Update cached global times
            this.highWatermark = Math.Max(syncTime, this.highWatermark);
            if (this.lowWatermark.rawValue < syncTime)
            {
                UpdateLowWatermark(syncTime);

                // Gather keys whose high watermarks are before the new low watermark
                var expiredWatermarkKVPs = new List<KeyValuePair<long, HashSet<TKey>>>();
                foreach (var keyValuePair in this.highWatermarkToPartitionsMap)
                {
                    // Since highWatermarkToPartitionsMap is sorted, we can stop as soon as we reach the threshold
                    if (keyValuePair.Key >= this.lowWatermark.rawValue) break;

                    expiredWatermarkKVPs.Add(keyValuePair);
                }

                // Clean up state from expired partitions
                foreach (var expiredWatermarkKVP in expiredWatermarkKVPs)
                {
                    var expiredWatermark = expiredWatermarkKVP.Key;
                    this.highWatermarkToPartitionsMap.Remove(expiredWatermark);

                    var expiredKeys = expiredWatermarkKVP.Value;
                    foreach (var expiredKey in expiredKeys)
                    {
                        this.lastPunctuationTime.Remove(expiredKey);
                        this.partitionHighWatermarks.Remove(expiredKey);
                        this.currentTime.Remove(expiredKey);
#if DEBUG
                        this.lastEventTime.Remove(expiredKey);
#endif
                    }
                }
            }

            // Add LowWatermark to batch
            var count = this.currentBatch.Count;
            this.currentBatch.vsync.col[count] = syncTime;
            this.currentBatch.vother.col[count] = PartitionedStreamEvent.LowWatermarkOtherTime;
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            this.currentBatch.key.col[count] = default;
            this.currentBatch[count] = default;
            this.currentBatch.hash.col[count] = 0;
            this.currentBatch.Count = count + 1;

            // Flush if necessary
            if (this.flushPolicy == PartitionedFlushPolicy.FlushOnLowWatermark ||
                (this.flushPolicy == PartitionedFlushPolicy.FlushOnBatchBoundary && this.currentBatch.Count == Config.DataBatchSize))
            {
                OnFlush();
            }
            else if (this.currentBatch.Count == Config.DataBatchSize)
            {
                FlushContents();
            }
        }

        protected override void UpdatePointers()
        {
            foreach (var kvp in this.partitionHighWatermarks)
            {
                if (this.highWatermarkToPartitionsMap.TryGetValue(kvp.Value, out HashSet<TKey> set))
                    set.Add(kvp.Key);
                else
                    this.highWatermarkToPartitionsMap.Add(kvp.Value, new HashSet<TKey> { kvp.Key });
            }
        }

        protected override void OnCompleted(long punctuationTime)
        {
            GenerateAndProcessLowWatermark(punctuationTime);

            // Flush, but if we just flushed due to the punctuation generated above
            if (this.flushPolicy != PartitionedFlushPolicy.FlushOnLowWatermark)
                OnFlush();
        }
    }

    [DataContract]
    internal sealed class FusedPartitionedStreamEventSubscriptionWithLatency<TKey, TPayload, TResult> : PartitionedObserverSubscriptionBase<TKey, PartitionedStreamEvent<TKey, TPayload>, TPayload, TResult>
    {
        [SchemaSerialization]
        private readonly FuseModule fuseModule;
        private readonly Action<long, long, TPayload, PartitionKey<TKey>> action;
        public FusedPartitionedStreamEventSubscriptionWithLatency() { }

        public FusedPartitionedStreamEventSubscriptionWithLatency(
            IObservable<PartitionedStreamEvent<TKey, TPayload>> observable,
            FuseModule fuseModule,
            string identifier,
            Streamable<PartitionKey<TKey>, TResult> streamable,
            IStreamObserver<PartitionKey<TKey>, TResult> observer,
            DisorderPolicy disorderPolicy,
            PartitionedFlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            PeriodicLowWatermarkPolicy lowWatermarkPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>> diagnosticOutput)
            : base(
                observable,
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
            this.fuseModule = fuseModule;
            Expression<Action<long, long, TResult, PartitionKey<TKey>>> statement = (s, e, p, k) => this.currentBatch.Add(s, e, k, p);
            Expression<Action> flush = () => FlushContents();
            Expression<Func<bool>> test = () => this.currentBatch.Count == Config.DataBatchSize;
            var full = Expression.Lambda<Action<long, long, TResult, PartitionKey<TKey>>>(
                Expression.Block(
                    statement.Body,
                    Expression.IfThen(test.Body, flush.Body)),
                statement.Parameters);
            var actionExp = fuseModule.Coalesce<TPayload, TResult, PartitionKey<TKey>>(full);
            this.action = actionExp.Compile();
        }

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(this.startEventInformation != null);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        public override void OnNext(PartitionedStreamEvent<TKey, TPayload> value)
        {

            if (value.IsLowWatermark)
            {
                GenerateAndProcessLowWatermark(value.SyncTime);
                return;
            }

            // Check to see if we need to generate a low watermark due to PeriodicLowWatermarkPolicy
            if (this.lowWatermarkPolicyType == PeriodicLowWatermarkPolicyType.Time &&
                value.SyncTime > this.lowWatermarkTimestampLag)
            {
                var newLowWatermark = value.SyncTime - this.lowWatermarkTimestampLag;
                if ((ulong)(newLowWatermark - this.lowWatermark.quantizedForLowWatermarkGeneration) >= this.lowWatermarkGenerationPeriod)
                {
                    // SyncTime is sufficiently high to generate a new watermark, but first snap it to the nearest generationPeriod boundary
                    var newLowWatermarkSnapped = newLowWatermark.SnapToLeftBoundary((long)this.lowWatermarkGenerationPeriod);
                    GenerateAndProcessLowWatermark(newLowWatermarkSnapped);
                }
            }

            if (!this.currentTime.TryGetValue(value.PartitionKey, out long moveFrom) || moveFrom < this.lowWatermark.rawValue)
            {
                moveFrom = this.lowWatermark.rawValue;
            }

            if (!this.partitionHighWatermarks.ContainsKey(value.PartitionKey))
            {
                this.partitionHighWatermarks.Add(value.PartitionKey, this.lowWatermark.rawValue);

                if (this.highWatermarkToPartitionsMap.TryGetValue(this.lowWatermark.rawValue, out HashSet<TKey> keySet)) keySet.Add(value.PartitionKey);
                else this.highWatermarkToPartitionsMap.Add(this.lowWatermark.rawValue, new HashSet<TKey> { value.PartitionKey });
            }
            long moveTo = moveFrom;

            // Events at the reorder boundary or earlier - are handled using default processing policies
            if (value.SyncTime <= moveFrom)
            {
                Process(ref value);
                return;
            }

            var oldTime = this.partitionHighWatermarks[value.PartitionKey];
            if (value.SyncTime > oldTime)
            {
                this.partitionHighWatermarks[value.PartitionKey] = value.SyncTime;

                var oldSet = this.highWatermarkToPartitionsMap[oldTime];
                if (oldSet.Count <= 1) this.highWatermarkToPartitionsMap.Remove(oldTime);
                else oldSet.Remove(value.PartitionKey);

                if (this.highWatermarkToPartitionsMap.TryGetValue(value.SyncTime, out HashSet<TKey> set)) set.Add(value.PartitionKey);
                else this.highWatermarkToPartitionsMap.Add(value.SyncTime, new HashSet<TKey> { value.PartitionKey });

                if (value.IsData)
                {
                    // moveTo for punctuations is updated below
                    moveTo = value.SyncTime - this.reorderLatency;
                    if (moveTo < StreamEvent.MinSyncTime) moveTo = StreamEvent.MinSyncTime;
                    if (moveTo < moveFrom) moveTo = moveFrom;
                }
            }

            if (value.IsPunctuation)
            {
                moveTo = Math.Max(moveFrom, value.SyncTime);
            }

            if (moveTo > moveFrom)
            {
                if (this.priorityQueueSorter != null)
                {
                    PartitionedStreamEvent<TKey, TPayload> resultEvent;

                    while ((!this.priorityQueueSorter.IsEmpty()) && this.priorityQueueSorter.Peek().SyncTime <= moveTo)
                    {
                        resultEvent = this.priorityQueueSorter.Dequeue();
                        Process(ref resultEvent);
                    }
                }
                else
                {
                    // Extract and process data in-order from impatience, until timestamp of moveTo
                    PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>> streamEvents = this.impatienceSorter.DequeueUntil(value.PartitionKey, moveTo, out bool recheck);

                    if (streamEvents != null)
                    {
                        PartitionedStreamEvent<TKey, TPayload> resultEvent;
                        while ((streamEvents.Count > 0) && ((!recheck) || (streamEvents.PeekFirst().SyncTime <= moveTo)))
                        {
                            resultEvent = streamEvents.Dequeue();
                            Process(ref resultEvent);
                        }
                    }

                    if (!recheck && (streamEvents != null))
                        this.impatienceSorter.Return(value.PartitionKey, streamEvents);
                }
            }

            if (value.SyncTime == moveTo)
            {
                Process(ref value);
                return;
            }

            // Enqueue value into impatience
            if (this.priorityQueueSorter != null) this.priorityQueueSorter.Enqueue(value);
            else this.impatienceSorter.Enqueue(ref value);

            UpdateCurrentTime(value.PartitionKey, moveTo, fromEvent: false);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void Process(ref PartitionedStreamEvent<TKey, TPayload> value)
        {
            Contract.Assume(value.SyncTime != value.OtherTime);

            if (value.IsLowWatermark)
            {
                GenerateAndProcessLowWatermark(value.SyncTime);
                return;
            }

            // Update global high water mark if necessary
            this.highWatermark = Math.Max(this.highWatermark, value.SyncTime);

            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time && !this.lastPunctuationTime.ContainsKey(value.PartitionKey))
                UpdatePunctuation(value.PartitionKey, this.lowWatermark.rawValue, this.lowWatermark.quantizedForPunctuationGeneration);

            // Retrieve current time for this partition, updating currentTime if necessary
            if (!this.currentTime.TryGetValue(value.PartitionKey, out long current))
            {
                current = this.lowWatermark.rawValue;
            }
            else if (current < this.lowWatermark.rawValue)
            {
                current = this.lowWatermark.rawValue;
                UpdateCurrentTime(value.PartitionKey, this.lowWatermark.rawValue);
            }

            var outOfOrder = value.SyncTime < current;
            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                // Track punctuation
                if (value.IsPunctuation && value.SyncTime > this.lastPunctuationTime[value.PartitionKey].lastPunctuation)
                    UpdatePunctuation(value.PartitionKey, value.SyncTime);

                // out of order events shouldn't count, and if the disorder policy adjusts their sync time, then it
                // will be made equal to a timestamp already seen earlier in the sequence and this would have triggered
                // (if necessary) when that timestamp was seen.
                if (!outOfOrder && this.punctuationGenerationPeriod > 0)
                {
                    // We use lowWatermark as the baseline in the delta computation because a low watermark implies
                    // punctuations for all partitions
                    var prevPunctuation = Math.Max(this.lastPunctuationTime[value.PartitionKey].lastPunctuationQuantized, this.lowWatermark.quantizedForPunctuationGeneration);
                    if ((ulong)(value.SyncTime - prevPunctuation) >= this.punctuationGenerationPeriod)
                    {
                        // SyncTime is sufficiently high to generate a new punctuation, but first snap it to the nearest generationPeriod boundary
                        var punctuationTimeQuantized = value.SyncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod);
#if DEBUG
                        Debug.Assert(punctuationTimeQuantized >= LastEventTime(value.PartitionKey), "Bug in punctuation quantization logic");
#endif
                        OnPunctuation(value.CreatePunctuation(punctuationTimeQuantized));
                    }
                }
            }

            // check for out of order event
            if (value.IsPunctuation)
            {
                OnPunctuation(value.CreatePunctuation(outOfOrder ? current : value.SyncTime));
            }
            else
            {
                if (this.disorderPolicyType == DisorderPolicyType.Throw)
                {
                    if (outOfOrder)
                    {
                        throw new IngressException($"Out-of-order event encountered during ingress, under a disorder policy of Throw: value.SyncTime: {value.SyncTime}, current:{current}");
                    }
                }
                else
                {
                    // end events and interval events just get dropped
                    Tuple<long, TPayload> key;
                    ElasticCircularBuffer<AdjustInfo> q;
                    switch (value.Kind)
                    {
                        case StreamEventKind.Start:
                            if (outOfOrder)
                            {
                                key = Tuple.Create(value.SyncTime, value.Payload);
                                if (!this.startEventInformation.TryGetValue(key, out q))
                                {
                                    q = new ElasticCircularBuffer<AdjustInfo>();
                                    this.startEventInformation.Add(key, q);
                                    var x = new AdjustInfo(current);
                                    q.Enqueue(ref x);
                                }
                                else
                                {
                                    var last = q.PeekLast();
                                    if (last.modifiedStartTime == current) last.numberOfOccurrences++;
                                    else
                                    {
                                        var x = new AdjustInfo(current);
                                        q.Enqueue(ref x);
                                    }
                                }
    
                                if (this.disorderPolicyType == DisorderPolicyType.Drop)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                    return; // drop
                                }
                                else
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                    value = new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, current, StreamEvent.InfinitySyncTime, value.Payload);
                                }
                            }
                            break;
    
                        case StreamEventKind.Interval:
                            if (outOfOrder)
                            {
                                if (this.disorderPolicyType == DisorderPolicyType.Drop)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                    return; // drop
                                }
                                else
                                {
                                    if (current >= value.OtherTime)
                                    {
                                        this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                        return; // drop
                                    }
    
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                    value = new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, current, value.OtherTime, value.Payload);
                                }
                            }
                            break;
    
                        case StreamEventKind.End:
                            // it may not be out of order, but did we drop/adjust the corresponding start event?
                            key = Tuple.Create(value.OtherTime, value.Payload);
                            if (this.startEventInformation.TryGetValue(key, out q))
                            {
                                Contract.Assume(!q.IsEmpty());
                                var firstElement = q.PeekFirst();
                                firstElement.numberOfOccurrences--;
                                if (firstElement.numberOfOccurrences == 0)
                                {
                                    q.Dequeue(); // throw away returned value
                                    if (q.Count == 0) this.startEventInformation.Remove(key);
                                }
                                var adjustedTime = firstElement.modifiedStartTime;
    
                                if (this.disorderPolicyType == DisorderPolicyType.Drop)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                    return; // drop
                                }
                                else
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                    value = new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, outOfOrder ? current : value.SyncTime, adjustedTime, value.Payload);
                                }
                            }
                            else if (outOfOrder)
                            {
                                if (this.disorderPolicyType == DisorderPolicyType.Drop)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                    return; // drop
                                }
                                else
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                    value = new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, current, value.OtherTime, value.Payload);
                                }
                            }
    
                            break;
                        default:
                            Contract.Assert(false, "switch meant to be exhaustive");
                            throw new InvalidOperationException("Unsupported stream event kind: " + value.Kind.ToString());
                    }
                }
    
                this.action(value.SyncTime, value.OtherTime, value.Payload, new PartitionKey<TKey>(value.PartitionKey));
            }

            UpdateCurrentTime(value.PartitionKey, value.SyncTime);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateCurrentTime(TKey partitionKey, long time, bool fromEvent = true)
        {
            if (!this.currentTime.TryGetValue(partitionKey, out long oldCurrentTime) || oldCurrentTime < time)
            {
                this.currentTime[partitionKey] = time;
            }
#if DEBUG
            if (fromEvent && (!this.lastEventTime.TryGetValue(partitionKey, out long oldEventTime) || oldEventTime < time))
            {
                this.lastEventTime[partitionKey] = time;
            }
#endif
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void GenerateAndProcessLowWatermark(long syncTime)
        {
            if (syncTime <= this.lowWatermark.rawValue) return;

            // Process events queued for reorderLatency up to the LowWatermark syncTime
            if (this.priorityQueueSorter != null)
            {
                PartitionedStreamEvent<TKey, TPayload> resultEvent;
                while ((!this.priorityQueueSorter.IsEmpty()) && this.priorityQueueSorter.Peek().SyncTime <= syncTime)
                {
                    resultEvent = this.priorityQueueSorter.Dequeue();
                    Process(ref resultEvent);
                }
            }
            else
            {
                bool recheck;
                var events = this.impatienceSorter.DequeueUntil(syncTime);

                int index = FastDictionary<TKey, Tuple<bool, PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>>>>.IteratorStart;
                while (events.Iterate(ref index))
                {
                    var entry = events.entries[index];
                    recheck = entry.value.Item1;
                    var streamEvents = entry.value.Item2;
                    if (streamEvents != null)
                    {
                        PartitionedStreamEvent<TKey, TPayload> resultEvent;
                        while ((streamEvents.Count > 0) && ((!recheck) || (streamEvents.PeekFirst().SyncTime <= syncTime)))
                        {
                            resultEvent = streamEvents.Dequeue();
                            Process(ref resultEvent);
                        }
                        if (!recheck) this.impatienceSorter.Return(entry.key, streamEvents);
                    }
                }
            }

            // Update cached global times
            this.highWatermark = Math.Max(syncTime, this.highWatermark);
            if (this.lowWatermark.rawValue < syncTime)
            {
                UpdateLowWatermark(syncTime);

                // Gather keys whose high watermarks are before the new low watermark
                var expiredWatermarkKVPs = new List<KeyValuePair<long, HashSet<TKey>>>();
                foreach (var keyValuePair in this.highWatermarkToPartitionsMap)
                {
                    // Since highWatermarkToPartitionsMap is sorted, we can stop as soon as we reach the threshold
                    if (keyValuePair.Key >= this.lowWatermark.rawValue) break;

                    expiredWatermarkKVPs.Add(keyValuePair);
                }

                // Clean up state from expired partitions
                foreach (var expiredWatermarkKVP in expiredWatermarkKVPs)
                {
                    var expiredWatermark = expiredWatermarkKVP.Key;
                    this.highWatermarkToPartitionsMap.Remove(expiredWatermark);

                    var expiredKeys = expiredWatermarkKVP.Value;
                    foreach (var expiredKey in expiredKeys)
                    {
                        this.lastPunctuationTime.Remove(expiredKey);
                        this.partitionHighWatermarks.Remove(expiredKey);
                        this.currentTime.Remove(expiredKey);
#if DEBUG
                        this.lastEventTime.Remove(expiredKey);
#endif
                    }
                }
            }

            // Add LowWatermark to batch
            var count = this.currentBatch.Count;
            this.currentBatch.vsync.col[count] = syncTime;
            this.currentBatch.vother.col[count] = PartitionedStreamEvent.LowWatermarkOtherTime;
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            this.currentBatch.key.col[count] = default;
            this.currentBatch[count] = default;
            this.currentBatch.hash.col[count] = 0;
            this.currentBatch.Count = count + 1;

            // Flush if necessary
            if (this.flushPolicy == PartitionedFlushPolicy.FlushOnLowWatermark ||
                (this.flushPolicy == PartitionedFlushPolicy.FlushOnBatchBoundary && this.currentBatch.Count == Config.DataBatchSize))
            {
                OnFlush();
            }
            else if (this.currentBatch.Count == Config.DataBatchSize)
            {
                FlushContents();
            }
        }

        protected override void UpdatePointers()
        {
            foreach (var kvp in this.partitionHighWatermarks)
            {
                if (this.highWatermarkToPartitionsMap.TryGetValue(kvp.Value, out HashSet<TKey> set))
                    set.Add(kvp.Key);
                else
                    this.highWatermarkToPartitionsMap.Add(kvp.Value, new HashSet<TKey> { kvp.Key });
            }
        }

        protected override void OnCompleted(long punctuationTime)
        {
            GenerateAndProcessLowWatermark(punctuationTime);

            // Flush, but if we just flushed due to the punctuation generated above
            if (this.flushPolicy != PartitionedFlushPolicy.FlushOnLowWatermark)
                OnFlush();
        }
    }

    [DataContract]
    internal sealed class DisorderedPartitionedStreamEventSubscriptionWithLatency<TKey, TPayload, TResult> : DisorderedPartitionedObserverSubscriptionBase<TKey, PartitionedStreamEvent<TKey, TPayload>, TPayload, TResult>
    {
        [SchemaSerialization]
        private readonly FuseModule fuseModule;
        private readonly Action<long, long, TPayload, PartitionKey<TKey>> action;
        public DisorderedPartitionedStreamEventSubscriptionWithLatency() { }

        public DisorderedPartitionedStreamEventSubscriptionWithLatency(
            IObservable<PartitionedStreamEvent<TKey, TPayload>> observable,
            FuseModule fuseModule,
            string identifier,
            Streamable<PartitionKey<TKey>, TResult> streamable,
            IStreamObserver<PartitionKey<TKey>, TResult> observer,
            DisorderPolicy disorderPolicy,
            PartitionedFlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            PeriodicLowWatermarkPolicy lowWatermarkPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>> diagnosticOutput)
            : base(
                observable,
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
            this.fuseModule = fuseModule;
            Expression<Action<long, long, TResult, PartitionKey<TKey>>> statement = (s, e, p, k) => Action(s, e, p, k);
            var actionExp = fuseModule.Coalesce<TPayload, TResult, PartitionKey<TKey>>(statement, true);
            this.action = actionExp.Compile();
        }

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(this.startEventInformation != null);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        public override void OnNext(PartitionedStreamEvent<TKey, TPayload> value)
        {
            this.action(value.SyncTime, value.OtherTime, value.Payload, new PartitionKey<TKey>(value.PartitionKey));
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void Action(long start, long end, TResult payload, PartitionKey<TKey> actionKey)
        {
            var value = new PartitionedStreamEvent<TKey, TResult>(actionKey.Key, start, end, payload);

            if (value.IsLowWatermark)
            {
                GenerateAndProcessLowWatermark(value.SyncTime);
                return;
            }

            // Check to see if we need to generate a low watermark due to PeriodicLowWatermarkPolicy
            if (this.lowWatermarkPolicyType == PeriodicLowWatermarkPolicyType.Time &&
                value.SyncTime > this.lowWatermarkTimestampLag)
            {
                var newLowWatermark = value.SyncTime - this.lowWatermarkTimestampLag;
                if ((ulong)(newLowWatermark - this.lowWatermark.quantizedForLowWatermarkGeneration) >= this.lowWatermarkGenerationPeriod)
                {
                    // SyncTime is sufficiently high to generate a new watermark, but first snap it to the nearest generationPeriod boundary
                    var newLowWatermarkSnapped = newLowWatermark.SnapToLeftBoundary((long)this.lowWatermarkGenerationPeriod);
                    GenerateAndProcessLowWatermark(newLowWatermarkSnapped);
                }
            }

            if (!this.currentTime.TryGetValue(value.PartitionKey, out long moveFrom) || moveFrom < this.lowWatermark.rawValue)
            {
                moveFrom = this.lowWatermark.rawValue;
            }

            if (!this.partitionHighWatermarks.ContainsKey(value.PartitionKey))
            {
                this.partitionHighWatermarks.Add(value.PartitionKey, this.lowWatermark.rawValue);

                if (this.highWatermarkToPartitionsMap.TryGetValue(this.lowWatermark.rawValue, out HashSet<TKey> keySet)) keySet.Add(value.PartitionKey);
                else this.highWatermarkToPartitionsMap.Add(this.lowWatermark.rawValue, new HashSet<TKey> { value.PartitionKey });
            }
            long moveTo = moveFrom;

            // Events at the reorder boundary or earlier - are handled using default processing policies
            if (value.SyncTime <= moveFrom)
            {
                Process(ref value);
                return;
            }

            var oldTime = this.partitionHighWatermarks[value.PartitionKey];
            if (value.SyncTime > oldTime)
            {
                this.partitionHighWatermarks[value.PartitionKey] = value.SyncTime;

                var oldSet = this.highWatermarkToPartitionsMap[oldTime];
                if (oldSet.Count <= 1) this.highWatermarkToPartitionsMap.Remove(oldTime);
                else oldSet.Remove(value.PartitionKey);

                if (this.highWatermarkToPartitionsMap.TryGetValue(value.SyncTime, out HashSet<TKey> set)) set.Add(value.PartitionKey);
                else this.highWatermarkToPartitionsMap.Add(value.SyncTime, new HashSet<TKey> { value.PartitionKey });

                if (value.IsData)
                {
                    // moveTo for punctuations is updated below
                    moveTo = value.SyncTime - this.reorderLatency;
                    if (moveTo < StreamEvent.MinSyncTime) moveTo = StreamEvent.MinSyncTime;
                    if (moveTo < moveFrom) moveTo = moveFrom;
                }
            }

            if (value.IsPunctuation)
            {
                moveTo = Math.Max(moveFrom, value.SyncTime);
            }

            if (moveTo > moveFrom)
            {
                if (this.priorityQueueSorter != null)
                {
                    PartitionedStreamEvent<TKey, TResult> resultEvent;

                    while ((!this.priorityQueueSorter.IsEmpty()) && this.priorityQueueSorter.Peek().SyncTime <= moveTo)
                    {
                        resultEvent = this.priorityQueueSorter.Dequeue();
                        Process(ref resultEvent);
                    }
                }
                else
                {
                    // Extract and process data in-order from impatience, until timestamp of moveTo
                    PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TResult>> streamEvents = this.impatienceSorter.DequeueUntil(value.PartitionKey, moveTo, out bool recheck);

                    if (streamEvents != null)
                    {
                        PartitionedStreamEvent<TKey, TResult> resultEvent;
                        while ((streamEvents.Count > 0) && ((!recheck) || (streamEvents.PeekFirst().SyncTime <= moveTo)))
                        {
                            resultEvent = streamEvents.Dequeue();
                            Process(ref resultEvent);
                        }
                    }

                    if (!recheck && (streamEvents != null))
                        this.impatienceSorter.Return(value.PartitionKey, streamEvents);
                }
            }

            if (value.SyncTime == moveTo)
            {
                Process(ref value);
                return;
            }

            // Enqueue value into impatience
            if (this.priorityQueueSorter != null) this.priorityQueueSorter.Enqueue(value);
            else this.impatienceSorter.Enqueue(ref value);

            UpdateCurrentTime(value.PartitionKey, moveTo, fromEvent: false);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void Process(ref PartitionedStreamEvent<TKey, TResult> value)
        {
            Contract.Assume(value.SyncTime != value.OtherTime);

            if (value.IsLowWatermark)
            {
                GenerateAndProcessLowWatermark(value.SyncTime);
                return;
            }

            // Update global high water mark if necessary
            this.highWatermark = Math.Max(this.highWatermark, value.SyncTime);

            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time && !this.lastPunctuationTime.ContainsKey(value.PartitionKey))
                UpdatePunctuation(value.PartitionKey, this.lowWatermark.rawValue, this.lowWatermark.quantizedForPunctuationGeneration);

            // Retrieve current time for this partition, updating currentTime if necessary
            if (!this.currentTime.TryGetValue(value.PartitionKey, out long current))
            {
                current = this.lowWatermark.rawValue;
            }
            else if (current < this.lowWatermark.rawValue)
            {
                current = this.lowWatermark.rawValue;
                UpdateCurrentTime(value.PartitionKey, this.lowWatermark.rawValue);
            }

            var outOfOrder = value.SyncTime < current;
            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                // Track punctuation
                if (value.IsPunctuation && value.SyncTime > this.lastPunctuationTime[value.PartitionKey].lastPunctuation)
                    UpdatePunctuation(value.PartitionKey, value.SyncTime);

                // out of order events shouldn't count, and if the disorder policy adjusts their sync time, then it
                // will be made equal to a timestamp already seen earlier in the sequence and this would have triggered
                // (if necessary) when that timestamp was seen.
                if (!outOfOrder && this.punctuationGenerationPeriod > 0)
                {
                    // We use lowWatermark as the baseline in the delta computation because a low watermark implies
                    // punctuations for all partitions
                    var prevPunctuation = Math.Max(this.lastPunctuationTime[value.PartitionKey].lastPunctuationQuantized, this.lowWatermark.quantizedForPunctuationGeneration);
                    if ((ulong)(value.SyncTime - prevPunctuation) >= this.punctuationGenerationPeriod)
                    {
                        // SyncTime is sufficiently high to generate a new punctuation, but first snap it to the nearest generationPeriod boundary
                        var punctuationTimeQuantized = value.SyncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod);
#if DEBUG
                        Debug.Assert(punctuationTimeQuantized >= LastEventTime(value.PartitionKey), "Bug in punctuation quantization logic");
#endif
                        OnPunctuation(value.CreatePunctuation(punctuationTimeQuantized));
                    }
                }
            }

            // check for out of order event
            if (value.IsPunctuation)
            {
                OnPunctuation(value.CreatePunctuation(outOfOrder ? current : value.SyncTime));
            }
            else
            {
                if (this.disorderPolicyType == DisorderPolicyType.Throw)
                {
                    if (outOfOrder)
                    {
                        throw new IngressException($"Out-of-order event encountered during ingress, under a disorder policy of Throw: value.SyncTime: {value.SyncTime}, current:{current}");
                    }
                }
                else
                {
                    // end events and interval events just get dropped
                    Tuple<long, TResult> key;
                    ElasticCircularBuffer<AdjustInfo> q;
                    switch (value.Kind)
                    {
                        case StreamEventKind.Start:
                            if (outOfOrder)
                            {
                                key = Tuple.Create(value.SyncTime, value.Payload);
                                if (!this.startEventInformation.TryGetValue(key, out q))
                                {
                                    q = new ElasticCircularBuffer<AdjustInfo>();
                                    this.startEventInformation.Add(key, q);
                                    var x = new AdjustInfo(current);
                                    q.Enqueue(ref x);
                                }
                                else
                                {
                                    var last = q.PeekLast();
                                    if (last.modifiedStartTime == current) last.numberOfOccurrences++;
                                    else
                                    {
                                        var x = new AdjustInfo(current);
                                        q.Enqueue(ref x);
                                    }
                                }
    
                                if (this.disorderPolicyType == DisorderPolicyType.Drop)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, value.SyncTime, value.OtherTime, default), default));
                                    return; // drop
                                }
                                else
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, value.SyncTime, value.OtherTime, default), new long?(current - value.SyncTime)));
                                    value = new PartitionedStreamEvent<TKey, TResult>(value.PartitionKey, current, StreamEvent.InfinitySyncTime, value.Payload);
                                }
                            }
                            break;
    
                        case StreamEventKind.Interval:
                            if (outOfOrder)
                            {
                                if (this.disorderPolicyType == DisorderPolicyType.Drop)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, value.SyncTime, value.OtherTime, default), default));
                                    return; // drop
                                }
                                else
                                {
                                    if (current >= value.OtherTime)
                                    {
                                        this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, value.SyncTime, value.OtherTime, default), default));
                                        return; // drop
                                    }
    
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, value.SyncTime, value.OtherTime, default), new long?(current - value.SyncTime)));
                                    value = new PartitionedStreamEvent<TKey, TResult>(value.PartitionKey, current, value.OtherTime, value.Payload);
                                }
                            }
                            break;
    
                        case StreamEventKind.End:
                            // it may not be out of order, but did we drop/adjust the corresponding start event?
                            key = Tuple.Create(value.OtherTime, value.Payload);
                            if (this.startEventInformation.TryGetValue(key, out q))
                            {
                                Contract.Assume(!q.IsEmpty());
                                var firstElement = q.PeekFirst();
                                firstElement.numberOfOccurrences--;
                                if (firstElement.numberOfOccurrences == 0)
                                {
                                    q.Dequeue(); // throw away returned value
                                    if (q.Count == 0) this.startEventInformation.Remove(key);
                                }
                                var adjustedTime = firstElement.modifiedStartTime;
    
                                if (this.disorderPolicyType == DisorderPolicyType.Drop)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, value.SyncTime, value.OtherTime, default), default));
                                    return; // drop
                                }
                                else
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, value.SyncTime, value.OtherTime, default), new long?(current - value.SyncTime)));
                                    value = new PartitionedStreamEvent<TKey, TResult>(value.PartitionKey, outOfOrder ? current : value.SyncTime, adjustedTime, value.Payload);
                                }
                            }
                            else if (outOfOrder)
                            {
                                if (this.disorderPolicyType == DisorderPolicyType.Drop)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, value.SyncTime, value.OtherTime, default), default));
                                    return; // drop
                                }
                                else
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, value.SyncTime, value.OtherTime, default), new long?(current - value.SyncTime)));
                                    value = new PartitionedStreamEvent<TKey, TResult>(value.PartitionKey, current, value.OtherTime, value.Payload);
                                }
                            }
    
                            break;
                        default:
                            Contract.Assert(false, "switch meant to be exhaustive");
                            throw new InvalidOperationException("Unsupported stream event kind: " + value.Kind.ToString());
                    }
                }
    
                this.currentBatch.Add(value.SyncTime, value.OtherTime, new PartitionKey<TKey>(value.PartitionKey), value.Payload);
                if (this.currentBatch.Count == Config.DataBatchSize)
                {
                    if (this.flushPolicy == PartitionedFlushPolicy.FlushOnBatchBoundary) OnFlush();
                    else FlushContents();
                }
            }

            UpdateCurrentTime(value.PartitionKey, value.SyncTime);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateCurrentTime(TKey partitionKey, long time, bool fromEvent = true)
        {
            if (!this.currentTime.TryGetValue(partitionKey, out long oldCurrentTime) || oldCurrentTime < time)
            {
                this.currentTime[partitionKey] = time;
            }
#if DEBUG
            if (fromEvent && (!this.lastEventTime.TryGetValue(partitionKey, out long oldEventTime) || oldEventTime < time))
            {
                this.lastEventTime[partitionKey] = time;
            }
#endif
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void GenerateAndProcessLowWatermark(long syncTime)
        {
            if (syncTime <= this.lowWatermark.rawValue) return;

            // Process events queued for reorderLatency up to the LowWatermark syncTime
            if (this.priorityQueueSorter != null)
            {
                PartitionedStreamEvent<TKey, TResult> resultEvent;
                while ((!this.priorityQueueSorter.IsEmpty()) && this.priorityQueueSorter.Peek().SyncTime <= syncTime)
                {
                    resultEvent = this.priorityQueueSorter.Dequeue();
                    Process(ref resultEvent);
                }
            }
            else
            {
                bool recheck;
                var events = this.impatienceSorter.DequeueUntil(syncTime);

                int index = FastDictionary<TKey, Tuple<bool, PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>>>>.IteratorStart;
                while (events.Iterate(ref index))
                {
                    var entry = events.entries[index];
                    recheck = entry.value.Item1;
                    var streamEvents = entry.value.Item2;
                    if (streamEvents != null)
                    {
                        PartitionedStreamEvent<TKey, TResult> resultEvent;
                        while ((streamEvents.Count > 0) && ((!recheck) || (streamEvents.PeekFirst().SyncTime <= syncTime)))
                        {
                            resultEvent = streamEvents.Dequeue();
                            Process(ref resultEvent);
                        }
                        if (!recheck) this.impatienceSorter.Return(entry.key, streamEvents);
                    }
                }
            }

            // Update cached global times
            this.highWatermark = Math.Max(syncTime, this.highWatermark);
            if (this.lowWatermark.rawValue < syncTime)
            {
                UpdateLowWatermark(syncTime);

                // Gather keys whose high watermarks are before the new low watermark
                var expiredWatermarkKVPs = new List<KeyValuePair<long, HashSet<TKey>>>();
                foreach (var keyValuePair in this.highWatermarkToPartitionsMap)
                {
                    // Since highWatermarkToPartitionsMap is sorted, we can stop as soon as we reach the threshold
                    if (keyValuePair.Key >= this.lowWatermark.rawValue) break;

                    expiredWatermarkKVPs.Add(keyValuePair);
                }

                // Clean up state from expired partitions
                foreach (var expiredWatermarkKVP in expiredWatermarkKVPs)
                {
                    var expiredWatermark = expiredWatermarkKVP.Key;
                    this.highWatermarkToPartitionsMap.Remove(expiredWatermark);

                    var expiredKeys = expiredWatermarkKVP.Value;
                    foreach (var expiredKey in expiredKeys)
                    {
                        this.lastPunctuationTime.Remove(expiredKey);
                        this.partitionHighWatermarks.Remove(expiredKey);
                        this.currentTime.Remove(expiredKey);
#if DEBUG
                        this.lastEventTime.Remove(expiredKey);
#endif
                    }
                }
            }

            // Add LowWatermark to batch
            var count = this.currentBatch.Count;
            this.currentBatch.vsync.col[count] = syncTime;
            this.currentBatch.vother.col[count] = PartitionedStreamEvent.LowWatermarkOtherTime;
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            this.currentBatch.key.col[count] = default;
            this.currentBatch[count] = default;
            this.currentBatch.hash.col[count] = 0;
            this.currentBatch.Count = count + 1;

            // Flush if necessary
            if (this.flushPolicy == PartitionedFlushPolicy.FlushOnLowWatermark ||
                (this.flushPolicy == PartitionedFlushPolicy.FlushOnBatchBoundary && this.currentBatch.Count == Config.DataBatchSize))
            {
                OnFlush();
            }
            else if (this.currentBatch.Count == Config.DataBatchSize)
            {
                FlushContents();
            }
        }

        protected override void UpdatePointers()
        {
            foreach (var kvp in this.partitionHighWatermarks)
            {
                if (this.highWatermarkToPartitionsMap.TryGetValue(kvp.Value, out HashSet<TKey> set))
                    set.Add(kvp.Key);
                else
                    this.highWatermarkToPartitionsMap.Add(kvp.Value, new HashSet<TKey> { kvp.Key });
            }
        }

        protected override void OnCompleted(long punctuationTime)
        {
            GenerateAndProcessLowWatermark(punctuationTime);

            // Flush, but if we just flushed due to the punctuation generated above
            if (this.flushPolicy != PartitionedFlushPolicy.FlushOnLowWatermark)
                OnFlush();
        }
    }

    [DataContract]
    internal sealed class SimplePartitionedStreamEventSubscription<TKey, TPayload> : PartitionedObserverSubscriptionBase<TKey, PartitionedStreamEvent<TKey, TPayload>, TPayload, TPayload>
    {
        public SimplePartitionedStreamEventSubscription() { }

        public SimplePartitionedStreamEventSubscription(
            IObservable<PartitionedStreamEvent<TKey, TPayload>> observable,
            string identifier,
            Streamable<PartitionKey<TKey>, TPayload> streamable,
            IStreamObserver<PartitionKey<TKey>, TPayload> observer,
            DisorderPolicy disorderPolicy,
            PartitionedFlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            PeriodicLowWatermarkPolicy lowWatermarkPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>> diagnosticOutput)
            : base(
                observable,
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
        }

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(this.startEventInformation != null);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        public override void OnNext(PartitionedStreamEvent<TKey, TPayload> value)
        {

            if (value.IsLowWatermark)
            {
                GenerateAndProcessLowWatermark(value.SyncTime);
                return;
            }

            // Check to see if we need to generate a low watermark due to PeriodicLowWatermarkPolicy
            if (this.lowWatermarkPolicyType == PeriodicLowWatermarkPolicyType.Time &&
                value.SyncTime > this.lowWatermarkTimestampLag)
            {
                var newLowWatermark = value.SyncTime - this.lowWatermarkTimestampLag;
                if ((ulong)(newLowWatermark - this.lowWatermark.quantizedForLowWatermarkGeneration) >= this.lowWatermarkGenerationPeriod)
                {
                    // SyncTime is sufficiently high to generate a new watermark, but first snap it to the nearest generationPeriod boundary
                    var newLowWatermarkSnapped = newLowWatermark.SnapToLeftBoundary((long)this.lowWatermarkGenerationPeriod);
                    GenerateAndProcessLowWatermark(newLowWatermarkSnapped);
                }
            }

            Contract.Assume(value.SyncTime != value.OtherTime);

            // Update global high water mark if necessary
            this.highWatermark = Math.Max(this.highWatermark, value.SyncTime);

            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time && !this.lastPunctuationTime.ContainsKey(value.PartitionKey))
                UpdatePunctuation(value.PartitionKey, this.lowWatermark.rawValue, this.lowWatermark.quantizedForPunctuationGeneration);

            // Retrieve current time for this partition, updating currentTime if necessary
            if (!this.currentTime.TryGetValue(value.PartitionKey, out long current))
            {
                current = this.lowWatermark.rawValue;
            }
            else if (current < this.lowWatermark.rawValue)
            {
                current = this.lowWatermark.rawValue;
                UpdateCurrentTime(value.PartitionKey, this.lowWatermark.rawValue);
            }

            var outOfOrder = value.SyncTime < current;
            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                // Track punctuation
                if (value.IsPunctuation && value.SyncTime > this.lastPunctuationTime[value.PartitionKey].lastPunctuation)
                    UpdatePunctuation(value.PartitionKey, value.SyncTime);

                // out of order events shouldn't count, and if the disorder policy adjusts their sync time, then it
                // will be made equal to a timestamp already seen earlier in the sequence and this would have triggered
                // (if necessary) when that timestamp was seen.
                if (!outOfOrder && this.punctuationGenerationPeriod > 0)
                {
                    // We use lowWatermark as the baseline in the delta computation because a low watermark implies
                    // punctuations for all partitions
                    var prevPunctuation = Math.Max(this.lastPunctuationTime[value.PartitionKey].lastPunctuationQuantized, this.lowWatermark.quantizedForPunctuationGeneration);
                    if ((ulong)(value.SyncTime - prevPunctuation) >= this.punctuationGenerationPeriod)
                    {
                        // SyncTime is sufficiently high to generate a new punctuation, but first snap it to the nearest generationPeriod boundary
                        var punctuationTimeQuantized = value.SyncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod);
#if DEBUG
                        Debug.Assert(punctuationTimeQuantized >= LastEventTime(value.PartitionKey), "Bug in punctuation quantization logic");
#endif
                        OnPunctuation(value.CreatePunctuation(punctuationTimeQuantized));
                    }
                }
            }

            // check for out of order event
            if (value.IsPunctuation)
            {
                OnPunctuation(value.CreatePunctuation(outOfOrder ? current : value.SyncTime));
            }
            else
            {
                if (this.disorderPolicyType == DisorderPolicyType.Throw)
                {
                    if (outOfOrder)
                    {
                        throw new IngressException($"Out-of-order event encountered during ingress, under a disorder policy of Throw: value.SyncTime: {value.SyncTime}, current:{current}");
                    }
                }
                else
                {
                    // end events and interval events just get dropped
                    Tuple<long, TPayload> key;
                    ElasticCircularBuffer<AdjustInfo> q;
                    switch (value.Kind)
                    {
                        case StreamEventKind.Start:
                            if (outOfOrder)
                            {
                                key = Tuple.Create(value.SyncTime, value.Payload);
                                if (!this.startEventInformation.TryGetValue(key, out q))
                                {
                                    q = new ElasticCircularBuffer<AdjustInfo>();
                                    this.startEventInformation.Add(key, q);
                                    var x = new AdjustInfo(current);
                                    q.Enqueue(ref x);
                                }
                                else
                                {
                                    var last = q.PeekLast();
                                    if (last.modifiedStartTime == current) last.numberOfOccurrences++;
                                    else
                                    {
                                        var x = new AdjustInfo(current);
                                        q.Enqueue(ref x);
                                    }
                                }
    
                                if (this.disorderPolicyType == DisorderPolicyType.Drop)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                    return; // drop
                                }
                                else
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                    value = new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, current, StreamEvent.InfinitySyncTime, value.Payload);
                                }
                            }
                            break;
    
                        case StreamEventKind.Interval:
                            if (outOfOrder)
                            {
                                if (this.disorderPolicyType == DisorderPolicyType.Drop)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                    return; // drop
                                }
                                else
                                {
                                    if (current >= value.OtherTime)
                                    {
                                        this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                        return; // drop
                                    }
    
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                    value = new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, current, value.OtherTime, value.Payload);
                                }
                            }
                            break;
    
                        case StreamEventKind.End:
                            // it may not be out of order, but did we drop/adjust the corresponding start event?
                            key = Tuple.Create(value.OtherTime, value.Payload);
                            if (this.startEventInformation.TryGetValue(key, out q))
                            {
                                Contract.Assume(!q.IsEmpty());
                                var firstElement = q.PeekFirst();
                                firstElement.numberOfOccurrences--;
                                if (firstElement.numberOfOccurrences == 0)
                                {
                                    q.Dequeue(); // throw away returned value
                                    if (q.Count == 0) this.startEventInformation.Remove(key);
                                }
                                var adjustedTime = firstElement.modifiedStartTime;
    
                                if (this.disorderPolicyType == DisorderPolicyType.Drop)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                    return; // drop
                                }
                                else
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                    value = new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, outOfOrder ? current : value.SyncTime, adjustedTime, value.Payload);
                                }
                            }
                            else if (outOfOrder)
                            {
                                if (this.disorderPolicyType == DisorderPolicyType.Drop)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                    return; // drop
                                }
                                else
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                    value = new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, current, value.OtherTime, value.Payload);
                                }
                            }
    
                            break;
                        default:
                            Contract.Assert(false, "switch meant to be exhaustive");
                            throw new InvalidOperationException("Unsupported stream event kind: " + value.Kind.ToString());
                    }
                }
    
                this.currentBatch.Add(value.SyncTime, value.OtherTime, new PartitionKey<TKey>(value.PartitionKey), value.Payload);
                if (this.currentBatch.Count == Config.DataBatchSize)
                {
                    if (this.flushPolicy == PartitionedFlushPolicy.FlushOnBatchBoundary) OnFlush();
                    else FlushContents();
                }
            }

            UpdateCurrentTime(value.PartitionKey, value.SyncTime);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateCurrentTime(TKey partitionKey, long time, bool fromEvent = true)
        {
            if (!this.currentTime.TryGetValue(partitionKey, out long oldCurrentTime) || oldCurrentTime < time)
            {
                this.currentTime[partitionKey] = time;
            }
#if DEBUG
            if (fromEvent && (!this.lastEventTime.TryGetValue(partitionKey, out long oldEventTime) || oldEventTime < time))
            {
                this.lastEventTime[partitionKey] = time;
            }
#endif
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void GenerateAndProcessLowWatermark(long syncTime)
        {
            if (syncTime <= this.lowWatermark.rawValue) return;

            // Update cached global times
            this.highWatermark = Math.Max(syncTime, this.highWatermark);
            if (this.lowWatermark.rawValue < syncTime)
            {
                UpdateLowWatermark(syncTime);
            }

            // Add LowWatermark to batch
            var count = this.currentBatch.Count;
            this.currentBatch.vsync.col[count] = syncTime;
            this.currentBatch.vother.col[count] = PartitionedStreamEvent.LowWatermarkOtherTime;
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            this.currentBatch.key.col[count] = default;
            this.currentBatch[count] = default;
            this.currentBatch.hash.col[count] = 0;
            this.currentBatch.Count = count + 1;

            // Flush if necessary
            if (this.flushPolicy == PartitionedFlushPolicy.FlushOnLowWatermark ||
                (this.flushPolicy == PartitionedFlushPolicy.FlushOnBatchBoundary && this.currentBatch.Count == Config.DataBatchSize))
            {
                OnFlush();
            }
            else if (this.currentBatch.Count == Config.DataBatchSize)
            {
                FlushContents();
            }
        }

        protected override void OnCompleted(long punctuationTime)
        {
            GenerateAndProcessLowWatermark(punctuationTime);

            // Flush, but if we just flushed due to the punctuation generated above
            if (this.flushPolicy != PartitionedFlushPolicy.FlushOnLowWatermark)
                OnFlush();
        }
    }

    [DataContract]
    internal sealed class FusedPartitionedStreamEventSubscription<TKey, TPayload, TResult> : PartitionedObserverSubscriptionBase<TKey, PartitionedStreamEvent<TKey, TPayload>, TPayload, TResult>
    {
        [SchemaSerialization]
        private readonly FuseModule fuseModule;
        private readonly Action<long, long, TPayload, PartitionKey<TKey>> action;
        public FusedPartitionedStreamEventSubscription() { }

        public FusedPartitionedStreamEventSubscription(
            IObservable<PartitionedStreamEvent<TKey, TPayload>> observable,
            FuseModule fuseModule,
            string identifier,
            Streamable<PartitionKey<TKey>, TResult> streamable,
            IStreamObserver<PartitionKey<TKey>, TResult> observer,
            DisorderPolicy disorderPolicy,
            PartitionedFlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            PeriodicLowWatermarkPolicy lowWatermarkPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>> diagnosticOutput)
            : base(
                observable,
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
            this.fuseModule = fuseModule;
            Expression<Action<long, long, TResult, PartitionKey<TKey>>> statement = (s, e, p, k) => this.currentBatch.Add(s, e, k, p);
            Expression<Action> flush = () => FlushContents();
            Expression<Func<bool>> test = () => this.currentBatch.Count == Config.DataBatchSize;
            var full = Expression.Lambda<Action<long, long, TResult, PartitionKey<TKey>>>(
                Expression.Block(
                    statement.Body,
                    Expression.IfThen(test.Body, flush.Body)),
                statement.Parameters);
            var actionExp = fuseModule.Coalesce<TPayload, TResult, PartitionKey<TKey>>(full);
            this.action = actionExp.Compile();
        }

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(this.startEventInformation != null);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        public override void OnNext(PartitionedStreamEvent<TKey, TPayload> value)
        {

            if (value.IsLowWatermark)
            {
                GenerateAndProcessLowWatermark(value.SyncTime);
                return;
            }

            // Check to see if we need to generate a low watermark due to PeriodicLowWatermarkPolicy
            if (this.lowWatermarkPolicyType == PeriodicLowWatermarkPolicyType.Time &&
                value.SyncTime > this.lowWatermarkTimestampLag)
            {
                var newLowWatermark = value.SyncTime - this.lowWatermarkTimestampLag;
                if ((ulong)(newLowWatermark - this.lowWatermark.quantizedForLowWatermarkGeneration) >= this.lowWatermarkGenerationPeriod)
                {
                    // SyncTime is sufficiently high to generate a new watermark, but first snap it to the nearest generationPeriod boundary
                    var newLowWatermarkSnapped = newLowWatermark.SnapToLeftBoundary((long)this.lowWatermarkGenerationPeriod);
                    GenerateAndProcessLowWatermark(newLowWatermarkSnapped);
                }
            }

            Contract.Assume(value.SyncTime != value.OtherTime);

            // Update global high water mark if necessary
            this.highWatermark = Math.Max(this.highWatermark, value.SyncTime);

            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time && !this.lastPunctuationTime.ContainsKey(value.PartitionKey))
                UpdatePunctuation(value.PartitionKey, this.lowWatermark.rawValue, this.lowWatermark.quantizedForPunctuationGeneration);

            // Retrieve current time for this partition, updating currentTime if necessary
            if (!this.currentTime.TryGetValue(value.PartitionKey, out long current))
            {
                current = this.lowWatermark.rawValue;
            }
            else if (current < this.lowWatermark.rawValue)
            {
                current = this.lowWatermark.rawValue;
                UpdateCurrentTime(value.PartitionKey, this.lowWatermark.rawValue);
            }

            var outOfOrder = value.SyncTime < current;
            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                // Track punctuation
                if (value.IsPunctuation && value.SyncTime > this.lastPunctuationTime[value.PartitionKey].lastPunctuation)
                    UpdatePunctuation(value.PartitionKey, value.SyncTime);

                // out of order events shouldn't count, and if the disorder policy adjusts their sync time, then it
                // will be made equal to a timestamp already seen earlier in the sequence and this would have triggered
                // (if necessary) when that timestamp was seen.
                if (!outOfOrder && this.punctuationGenerationPeriod > 0)
                {
                    // We use lowWatermark as the baseline in the delta computation because a low watermark implies
                    // punctuations for all partitions
                    var prevPunctuation = Math.Max(this.lastPunctuationTime[value.PartitionKey].lastPunctuationQuantized, this.lowWatermark.quantizedForPunctuationGeneration);
                    if ((ulong)(value.SyncTime - prevPunctuation) >= this.punctuationGenerationPeriod)
                    {
                        // SyncTime is sufficiently high to generate a new punctuation, but first snap it to the nearest generationPeriod boundary
                        var punctuationTimeQuantized = value.SyncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod);
#if DEBUG
                        Debug.Assert(punctuationTimeQuantized >= LastEventTime(value.PartitionKey), "Bug in punctuation quantization logic");
#endif
                        OnPunctuation(value.CreatePunctuation(punctuationTimeQuantized));
                    }
                }
            }

            // check for out of order event
            if (value.IsPunctuation)
            {
                OnPunctuation(value.CreatePunctuation(outOfOrder ? current : value.SyncTime));
            }
            else
            {
                if (this.disorderPolicyType == DisorderPolicyType.Throw)
                {
                    if (outOfOrder)
                    {
                        throw new IngressException($"Out-of-order event encountered during ingress, under a disorder policy of Throw: value.SyncTime: {value.SyncTime}, current:{current}");
                    }
                }
                else
                {
                    // end events and interval events just get dropped
                    Tuple<long, TPayload> key;
                    ElasticCircularBuffer<AdjustInfo> q;
                    switch (value.Kind)
                    {
                        case StreamEventKind.Start:
                            if (outOfOrder)
                            {
                                key = Tuple.Create(value.SyncTime, value.Payload);
                                if (!this.startEventInformation.TryGetValue(key, out q))
                                {
                                    q = new ElasticCircularBuffer<AdjustInfo>();
                                    this.startEventInformation.Add(key, q);
                                    var x = new AdjustInfo(current);
                                    q.Enqueue(ref x);
                                }
                                else
                                {
                                    var last = q.PeekLast();
                                    if (last.modifiedStartTime == current) last.numberOfOccurrences++;
                                    else
                                    {
                                        var x = new AdjustInfo(current);
                                        q.Enqueue(ref x);
                                    }
                                }
    
                                if (this.disorderPolicyType == DisorderPolicyType.Drop)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                    return; // drop
                                }
                                else
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                    value = new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, current, StreamEvent.InfinitySyncTime, value.Payload);
                                }
                            }
                            break;
    
                        case StreamEventKind.Interval:
                            if (outOfOrder)
                            {
                                if (this.disorderPolicyType == DisorderPolicyType.Drop)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                    return; // drop
                                }
                                else
                                {
                                    if (current >= value.OtherTime)
                                    {
                                        this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                        return; // drop
                                    }
    
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                    value = new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, current, value.OtherTime, value.Payload);
                                }
                            }
                            break;
    
                        case StreamEventKind.End:
                            // it may not be out of order, but did we drop/adjust the corresponding start event?
                            key = Tuple.Create(value.OtherTime, value.Payload);
                            if (this.startEventInformation.TryGetValue(key, out q))
                            {
                                Contract.Assume(!q.IsEmpty());
                                var firstElement = q.PeekFirst();
                                firstElement.numberOfOccurrences--;
                                if (firstElement.numberOfOccurrences == 0)
                                {
                                    q.Dequeue(); // throw away returned value
                                    if (q.Count == 0) this.startEventInformation.Remove(key);
                                }
                                var adjustedTime = firstElement.modifiedStartTime;
    
                                if (this.disorderPolicyType == DisorderPolicyType.Drop)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                    return; // drop
                                }
                                else
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                    value = new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, outOfOrder ? current : value.SyncTime, adjustedTime, value.Payload);
                                }
                            }
                            else if (outOfOrder)
                            {
                                if (this.disorderPolicyType == DisorderPolicyType.Drop)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                    return; // drop
                                }
                                else
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                    value = new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, current, value.OtherTime, value.Payload);
                                }
                            }
    
                            break;
                        default:
                            Contract.Assert(false, "switch meant to be exhaustive");
                            throw new InvalidOperationException("Unsupported stream event kind: " + value.Kind.ToString());
                    }
                }
    
                this.action(value.SyncTime, value.OtherTime, value.Payload, new PartitionKey<TKey>(value.PartitionKey));
            }

            UpdateCurrentTime(value.PartitionKey, value.SyncTime);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateCurrentTime(TKey partitionKey, long time, bool fromEvent = true)
        {
            if (!this.currentTime.TryGetValue(partitionKey, out long oldCurrentTime) || oldCurrentTime < time)
            {
                this.currentTime[partitionKey] = time;
            }
#if DEBUG
            if (fromEvent && (!this.lastEventTime.TryGetValue(partitionKey, out long oldEventTime) || oldEventTime < time))
            {
                this.lastEventTime[partitionKey] = time;
            }
#endif
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void GenerateAndProcessLowWatermark(long syncTime)
        {
            if (syncTime <= this.lowWatermark.rawValue) return;

            // Update cached global times
            this.highWatermark = Math.Max(syncTime, this.highWatermark);
            if (this.lowWatermark.rawValue < syncTime)
            {
                UpdateLowWatermark(syncTime);
            }

            // Add LowWatermark to batch
            var count = this.currentBatch.Count;
            this.currentBatch.vsync.col[count] = syncTime;
            this.currentBatch.vother.col[count] = PartitionedStreamEvent.LowWatermarkOtherTime;
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            this.currentBatch.key.col[count] = default;
            this.currentBatch[count] = default;
            this.currentBatch.hash.col[count] = 0;
            this.currentBatch.Count = count + 1;

            // Flush if necessary
            if (this.flushPolicy == PartitionedFlushPolicy.FlushOnLowWatermark ||
                (this.flushPolicy == PartitionedFlushPolicy.FlushOnBatchBoundary && this.currentBatch.Count == Config.DataBatchSize))
            {
                OnFlush();
            }
            else if (this.currentBatch.Count == Config.DataBatchSize)
            {
                FlushContents();
            }
        }

        protected override void OnCompleted(long punctuationTime)
        {
            GenerateAndProcessLowWatermark(punctuationTime);

            // Flush, but if we just flushed due to the punctuation generated above
            if (this.flushPolicy != PartitionedFlushPolicy.FlushOnLowWatermark)
                OnFlush();
        }
    }

    [DataContract]
    internal sealed class DisorderedPartitionedStreamEventSubscription<TKey, TPayload, TResult> : DisorderedPartitionedObserverSubscriptionBase<TKey, PartitionedStreamEvent<TKey, TPayload>, TPayload, TResult>
    {
        [SchemaSerialization]
        private readonly FuseModule fuseModule;
        private readonly Action<long, long, TPayload, PartitionKey<TKey>> action;
        public DisorderedPartitionedStreamEventSubscription() { }

        public DisorderedPartitionedStreamEventSubscription(
            IObservable<PartitionedStreamEvent<TKey, TPayload>> observable,
            FuseModule fuseModule,
            string identifier,
            Streamable<PartitionKey<TKey>, TResult> streamable,
            IStreamObserver<PartitionKey<TKey>, TResult> observer,
            DisorderPolicy disorderPolicy,
            PartitionedFlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            PeriodicLowWatermarkPolicy lowWatermarkPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>> diagnosticOutput)
            : base(
                observable,
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
            this.fuseModule = fuseModule;
            Expression<Action<long, long, TResult, PartitionKey<TKey>>> statement = (s, e, p, k) => Action(s, e, p, k);
            var actionExp = fuseModule.Coalesce<TPayload, TResult, PartitionKey<TKey>>(statement, true);
            this.action = actionExp.Compile();
        }

        [ContractInvariantMethod]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic", Justification = "Required for code contracts.")]
        private void ObjectInvariant()
        {
            Contract.Invariant(this.startEventInformation != null);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        public override void OnNext(PartitionedStreamEvent<TKey, TPayload> value)
        {
            this.action(value.SyncTime, value.OtherTime, value.Payload, new PartitionKey<TKey>(value.PartitionKey));
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void Action(long start, long end, TResult payload, PartitionKey<TKey> actionKey)
        {
            var value = new PartitionedStreamEvent<TKey, TResult>(actionKey.Key, start, end, payload);

            if (value.IsLowWatermark)
            {
                GenerateAndProcessLowWatermark(value.SyncTime);
                return;
            }

            // Check to see if we need to generate a low watermark due to PeriodicLowWatermarkPolicy
            if (this.lowWatermarkPolicyType == PeriodicLowWatermarkPolicyType.Time &&
                value.SyncTime > this.lowWatermarkTimestampLag)
            {
                var newLowWatermark = value.SyncTime - this.lowWatermarkTimestampLag;
                if ((ulong)(newLowWatermark - this.lowWatermark.quantizedForLowWatermarkGeneration) >= this.lowWatermarkGenerationPeriod)
                {
                    // SyncTime is sufficiently high to generate a new watermark, but first snap it to the nearest generationPeriod boundary
                    var newLowWatermarkSnapped = newLowWatermark.SnapToLeftBoundary((long)this.lowWatermarkGenerationPeriod);
                    GenerateAndProcessLowWatermark(newLowWatermarkSnapped);
                }
            }

            Contract.Assume(value.SyncTime != value.OtherTime);

            // Update global high water mark if necessary
            this.highWatermark = Math.Max(this.highWatermark, value.SyncTime);

            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time && !this.lastPunctuationTime.ContainsKey(value.PartitionKey))
                UpdatePunctuation(value.PartitionKey, this.lowWatermark.rawValue, this.lowWatermark.quantizedForPunctuationGeneration);

            // Retrieve current time for this partition, updating currentTime if necessary
            if (!this.currentTime.TryGetValue(value.PartitionKey, out long current))
            {
                current = this.lowWatermark.rawValue;
            }
            else if (current < this.lowWatermark.rawValue)
            {
                current = this.lowWatermark.rawValue;
                UpdateCurrentTime(value.PartitionKey, this.lowWatermark.rawValue);
            }

            var outOfOrder = value.SyncTime < current;
            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                // Track punctuation
                if (value.IsPunctuation && value.SyncTime > this.lastPunctuationTime[value.PartitionKey].lastPunctuation)
                    UpdatePunctuation(value.PartitionKey, value.SyncTime);

                // out of order events shouldn't count, and if the disorder policy adjusts their sync time, then it
                // will be made equal to a timestamp already seen earlier in the sequence and this would have triggered
                // (if necessary) when that timestamp was seen.
                if (!outOfOrder && this.punctuationGenerationPeriod > 0)
                {
                    // We use lowWatermark as the baseline in the delta computation because a low watermark implies
                    // punctuations for all partitions
                    var prevPunctuation = Math.Max(this.lastPunctuationTime[value.PartitionKey].lastPunctuationQuantized, this.lowWatermark.quantizedForPunctuationGeneration);
                    if ((ulong)(value.SyncTime - prevPunctuation) >= this.punctuationGenerationPeriod)
                    {
                        // SyncTime is sufficiently high to generate a new punctuation, but first snap it to the nearest generationPeriod boundary
                        var punctuationTimeQuantized = value.SyncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod);
#if DEBUG
                        Debug.Assert(punctuationTimeQuantized >= LastEventTime(value.PartitionKey), "Bug in punctuation quantization logic");
#endif
                        OnPunctuation(value.CreatePunctuation(punctuationTimeQuantized));
                    }
                }
            }

            // check for out of order event
            if (value.IsPunctuation)
            {
                OnPunctuation(value.CreatePunctuation(outOfOrder ? current : value.SyncTime));
            }
            else
            {
                if (this.disorderPolicyType == DisorderPolicyType.Throw)
                {
                    if (outOfOrder)
                    {
                        throw new IngressException($"Out-of-order event encountered during ingress, under a disorder policy of Throw: value.SyncTime: {value.SyncTime}, current:{current}");
                    }
                }
                else
                {
                    // end events and interval events just get dropped
                    Tuple<long, TResult> key;
                    ElasticCircularBuffer<AdjustInfo> q;
                    switch (value.Kind)
                    {
                        case StreamEventKind.Start:
                            if (outOfOrder)
                            {
                                key = Tuple.Create(value.SyncTime, value.Payload);
                                if (!this.startEventInformation.TryGetValue(key, out q))
                                {
                                    q = new ElasticCircularBuffer<AdjustInfo>();
                                    this.startEventInformation.Add(key, q);
                                    var x = new AdjustInfo(current);
                                    q.Enqueue(ref x);
                                }
                                else
                                {
                                    var last = q.PeekLast();
                                    if (last.modifiedStartTime == current) last.numberOfOccurrences++;
                                    else
                                    {
                                        var x = new AdjustInfo(current);
                                        q.Enqueue(ref x);
                                    }
                                }
    
                                if (this.disorderPolicyType == DisorderPolicyType.Drop)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, value.SyncTime, value.OtherTime, default), default));
                                    return; // drop
                                }
                                else
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, value.SyncTime, value.OtherTime, default), new long?(current - value.SyncTime)));
                                    value = new PartitionedStreamEvent<TKey, TResult>(value.PartitionKey, current, StreamEvent.InfinitySyncTime, value.Payload);
                                }
                            }
                            break;
    
                        case StreamEventKind.Interval:
                            if (outOfOrder)
                            {
                                if (this.disorderPolicyType == DisorderPolicyType.Drop)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, value.SyncTime, value.OtherTime, default), default));
                                    return; // drop
                                }
                                else
                                {
                                    if (current >= value.OtherTime)
                                    {
                                        this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, value.SyncTime, value.OtherTime, default), default));
                                        return; // drop
                                    }
    
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, value.SyncTime, value.OtherTime, default), new long?(current - value.SyncTime)));
                                    value = new PartitionedStreamEvent<TKey, TResult>(value.PartitionKey, current, value.OtherTime, value.Payload);
                                }
                            }
                            break;
    
                        case StreamEventKind.End:
                            // it may not be out of order, but did we drop/adjust the corresponding start event?
                            key = Tuple.Create(value.OtherTime, value.Payload);
                            if (this.startEventInformation.TryGetValue(key, out q))
                            {
                                Contract.Assume(!q.IsEmpty());
                                var firstElement = q.PeekFirst();
                                firstElement.numberOfOccurrences--;
                                if (firstElement.numberOfOccurrences == 0)
                                {
                                    q.Dequeue(); // throw away returned value
                                    if (q.Count == 0) this.startEventInformation.Remove(key);
                                }
                                var adjustedTime = firstElement.modifiedStartTime;
    
                                if (this.disorderPolicyType == DisorderPolicyType.Drop)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, value.SyncTime, value.OtherTime, default), default));
                                    return; // drop
                                }
                                else
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, value.SyncTime, value.OtherTime, default), new long?(current - value.SyncTime)));
                                    value = new PartitionedStreamEvent<TKey, TResult>(value.PartitionKey, outOfOrder ? current : value.SyncTime, adjustedTime, value.Payload);
                                }
                            }
                            else if (outOfOrder)
                            {
                                if (this.disorderPolicyType == DisorderPolicyType.Drop)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, value.SyncTime, value.OtherTime, default), default));
                                    return; // drop
                                }
                                else
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, value.SyncTime, value.OtherTime, default), new long?(current - value.SyncTime)));
                                    value = new PartitionedStreamEvent<TKey, TResult>(value.PartitionKey, current, value.OtherTime, value.Payload);
                                }
                            }
    
                            break;
                        default:
                            Contract.Assert(false, "switch meant to be exhaustive");
                            throw new InvalidOperationException("Unsupported stream event kind: " + value.Kind.ToString());
                    }
                }
    
                this.currentBatch.Add(value.SyncTime, value.OtherTime, new PartitionKey<TKey>(value.PartitionKey), value.Payload);
                if (this.currentBatch.Count == Config.DataBatchSize)
                {
                    if (this.flushPolicy == PartitionedFlushPolicy.FlushOnBatchBoundary) OnFlush();
                    else FlushContents();
                }
            }

            UpdateCurrentTime(value.PartitionKey, value.SyncTime);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateCurrentTime(TKey partitionKey, long time, bool fromEvent = true)
        {
            if (!this.currentTime.TryGetValue(partitionKey, out long oldCurrentTime) || oldCurrentTime < time)
            {
                this.currentTime[partitionKey] = time;
            }
#if DEBUG
            if (fromEvent && (!this.lastEventTime.TryGetValue(partitionKey, out long oldEventTime) || oldEventTime < time))
            {
                this.lastEventTime[partitionKey] = time;
            }
#endif
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void GenerateAndProcessLowWatermark(long syncTime)
        {
            if (syncTime <= this.lowWatermark.rawValue) return;

            // Update cached global times
            this.highWatermark = Math.Max(syncTime, this.highWatermark);
            if (this.lowWatermark.rawValue < syncTime)
            {
                UpdateLowWatermark(syncTime);
            }

            // Add LowWatermark to batch
            var count = this.currentBatch.Count;
            this.currentBatch.vsync.col[count] = syncTime;
            this.currentBatch.vother.col[count] = PartitionedStreamEvent.LowWatermarkOtherTime;
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            this.currentBatch.key.col[count] = default;
            this.currentBatch[count] = default;
            this.currentBatch.hash.col[count] = 0;
            this.currentBatch.Count = count + 1;

            // Flush if necessary
            if (this.flushPolicy == PartitionedFlushPolicy.FlushOnLowWatermark ||
                (this.flushPolicy == PartitionedFlushPolicy.FlushOnBatchBoundary && this.currentBatch.Count == Config.DataBatchSize))
            {
                OnFlush();
            }
            else if (this.currentBatch.Count == Config.DataBatchSize)
            {
                FlushContents();
            }
        }

        protected override void OnCompleted(long punctuationTime)
        {
            GenerateAndProcessLowWatermark(punctuationTime);

            // Flush, but if we just flushed due to the punctuation generated above
            if (this.flushPolicy != PartitionedFlushPolicy.FlushOnLowWatermark)
                OnFlush();
        }
    }

    [DataContract]
    internal sealed class SimplePartitionedIntervalSubscriptionWithLatency<TKey, TPayload> : PartitionedObserverSubscriptionBase<TKey, TPayload, TPayload, TPayload>
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
        public SimplePartitionedIntervalSubscriptionWithLatency() { }

        public SimplePartitionedIntervalSubscriptionWithLatency(
            IObservable<TPayload> observable,
            Expression<Func<TPayload, TKey>> partitionExtractor,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            string identifier,
            Streamable<PartitionKey<TKey>, TPayload> streamable,
            IStreamObserver<PartitionKey<TKey>, TPayload> observer,
            DisorderPolicy disorderPolicy,
            PartitionedFlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            PeriodicLowWatermarkPolicy lowWatermarkPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>> diagnosticOutput)
            : base(
                observable,
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

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        public override void OnNext(TPayload inputValue)
        {
            var value = PartitionedStreamEvent.CreateInterval(this.partitionFunction(inputValue), this.startEdgeFunction(inputValue), this.endEdgeFunction(inputValue), inputValue);

            if (value.IsLowWatermark)
            {
                GenerateAndProcessLowWatermark(value.SyncTime);
                return;
            }

            // Check to see if we need to generate a low watermark due to PeriodicLowWatermarkPolicy
            if (this.lowWatermarkPolicyType == PeriodicLowWatermarkPolicyType.Time &&
                value.SyncTime > this.lowWatermarkTimestampLag)
            {
                var newLowWatermark = value.SyncTime - this.lowWatermarkTimestampLag;
                if ((ulong)(newLowWatermark - this.lowWatermark.quantizedForLowWatermarkGeneration) >= this.lowWatermarkGenerationPeriod)
                {
                    // SyncTime is sufficiently high to generate a new watermark, but first snap it to the nearest generationPeriod boundary
                    var newLowWatermarkSnapped = newLowWatermark.SnapToLeftBoundary((long)this.lowWatermarkGenerationPeriod);
                    GenerateAndProcessLowWatermark(newLowWatermarkSnapped);
                }
            }

            if (!this.currentTime.TryGetValue(value.PartitionKey, out long moveFrom) || moveFrom < this.lowWatermark.rawValue)
            {
                moveFrom = this.lowWatermark.rawValue;
            }

            if (!this.partitionHighWatermarks.ContainsKey(value.PartitionKey))
            {
                this.partitionHighWatermarks.Add(value.PartitionKey, this.lowWatermark.rawValue);

                if (this.highWatermarkToPartitionsMap.TryGetValue(this.lowWatermark.rawValue, out HashSet<TKey> keySet)) keySet.Add(value.PartitionKey);
                else this.highWatermarkToPartitionsMap.Add(this.lowWatermark.rawValue, new HashSet<TKey> { value.PartitionKey });
            }
            long moveTo = moveFrom;

            // Events at the reorder boundary or earlier - are handled using default processing policies
            if (value.SyncTime <= moveFrom)
            {
                Process(ref value);
                return;
            }

            var oldTime = this.partitionHighWatermarks[value.PartitionKey];
            if (value.SyncTime > oldTime)
            {
                this.partitionHighWatermarks[value.PartitionKey] = value.SyncTime;

                var oldSet = this.highWatermarkToPartitionsMap[oldTime];
                if (oldSet.Count <= 1) this.highWatermarkToPartitionsMap.Remove(oldTime);
                else oldSet.Remove(value.PartitionKey);

                if (this.highWatermarkToPartitionsMap.TryGetValue(value.SyncTime, out HashSet<TKey> set)) set.Add(value.PartitionKey);
                else this.highWatermarkToPartitionsMap.Add(value.SyncTime, new HashSet<TKey> { value.PartitionKey });

                moveTo = value.SyncTime - this.reorderLatency;
                if (moveTo < StreamEvent.MinSyncTime) moveTo = StreamEvent.MinSyncTime;
                if (moveTo < moveFrom) moveTo = moveFrom;
            }

            if (moveTo > moveFrom)
            {
                if (this.priorityQueueSorter != null)
                {
                    PartitionedStreamEvent<TKey, TPayload> resultEvent;

                    while ((!this.priorityQueueSorter.IsEmpty()) && this.priorityQueueSorter.Peek().SyncTime <= moveTo)
                    {
                        resultEvent = this.priorityQueueSorter.Dequeue();
                        Process(ref resultEvent);
                    }
                }
                else
                {
                    // Extract and process data in-order from impatience, until timestamp of moveTo
                    PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>> streamEvents = this.impatienceSorter.DequeueUntil(value.PartitionKey, moveTo, out bool recheck);

                    if (streamEvents != null)
                    {
                        PartitionedStreamEvent<TKey, TPayload> resultEvent;
                        while ((streamEvents.Count > 0) && ((!recheck) || (streamEvents.PeekFirst().SyncTime <= moveTo)))
                        {
                            resultEvent = streamEvents.Dequeue();
                            Process(ref resultEvent);
                        }
                    }

                    if (!recheck && (streamEvents != null))
                        this.impatienceSorter.Return(value.PartitionKey, streamEvents);
                }
            }

            if (value.SyncTime == moveTo)
            {
                Process(ref value);
                return;
            }

            // Enqueue value into impatience
            if (this.priorityQueueSorter != null) this.priorityQueueSorter.Enqueue(value);
            else this.impatienceSorter.Enqueue(ref value);

            UpdateCurrentTime(value.PartitionKey, moveTo, fromEvent: false);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void Process(ref PartitionedStreamEvent<TKey, TPayload> value)
        {
            // Update global high water mark if necessary
            this.highWatermark = Math.Max(this.highWatermark, value.SyncTime);

            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time && !this.lastPunctuationTime.ContainsKey(value.PartitionKey))
                UpdatePunctuation(value.PartitionKey, this.lowWatermark.rawValue, this.lowWatermark.quantizedForPunctuationGeneration);

            // Retrieve current time for this partition, updating currentTime if necessary
            if (!this.currentTime.TryGetValue(value.PartitionKey, out long current))
            {
                current = this.lowWatermark.rawValue;
            }
            else if (current < this.lowWatermark.rawValue)
            {
                current = this.lowWatermark.rawValue;
                UpdateCurrentTime(value.PartitionKey, this.lowWatermark.rawValue);
            }

            var outOfOrder = value.SyncTime < current;
            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                // out of order events shouldn't count, and if the disorder policy adjusts their sync time, then it
                // will be made equal to a timestamp already seen earlier in the sequence and this would have triggered
                // (if necessary) when that timestamp was seen.
                if (!outOfOrder && this.punctuationGenerationPeriod > 0)
                {
                    // We use lowWatermark as the baseline in the delta computation because a low watermark implies
                    // punctuations for all partitions
                    var prevPunctuation = Math.Max(this.lastPunctuationTime[value.PartitionKey].lastPunctuationQuantized, this.lowWatermark.quantizedForPunctuationGeneration);
                    if ((ulong)(value.SyncTime - prevPunctuation) >= this.punctuationGenerationPeriod)
                    {
                        // SyncTime is sufficiently high to generate a new punctuation, but first snap it to the nearest generationPeriod boundary
                        var punctuationTimeQuantized = value.SyncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod);
#if DEBUG
                        Debug.Assert(punctuationTimeQuantized >= LastEventTime(value.PartitionKey), "Bug in punctuation quantization logic");
#endif
                        OnPunctuation(value.CreatePunctuation(punctuationTimeQuantized));
                    }
                }
            }

            if (this.disorderPolicyType == DisorderPolicyType.Throw)
            {
                if (outOfOrder)
                {
                    throw new IngressException($"Out-of-order event encountered during ingress, under a disorder policy of Throw: value.SyncTime: {value.SyncTime}, current:{current}");
                }
            }
            else
            {
                        if (outOfOrder)
                        {
                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                return; // drop
                            }
                            else
                            {
                                if (current >= value.OtherTime)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                    return; // drop
                                }

                                this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                value = new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, current, value.OtherTime, value.Payload);
                            }
                        }
            }

            this.currentBatch.Add(value.SyncTime, value.OtherTime, new PartitionKey<TKey>(value.PartitionKey), value.Payload);
            if (this.currentBatch.Count == Config.DataBatchSize)
            {
                if (this.flushPolicy == PartitionedFlushPolicy.FlushOnBatchBoundary) OnFlush();
                else FlushContents();
            }

            UpdateCurrentTime(value.PartitionKey, value.SyncTime);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateCurrentTime(TKey partitionKey, long time, bool fromEvent = true)
        {
            if (!this.currentTime.TryGetValue(partitionKey, out long oldCurrentTime) || oldCurrentTime < time)
            {
                this.currentTime[partitionKey] = time;
            }
#if DEBUG
            if (fromEvent && (!this.lastEventTime.TryGetValue(partitionKey, out long oldEventTime) || oldEventTime < time))
            {
                this.lastEventTime[partitionKey] = time;
            }
#endif
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void GenerateAndProcessLowWatermark(long syncTime)
        {
            if (syncTime <= this.lowWatermark.rawValue) return;

            // Process events queued for reorderLatency up to the LowWatermark syncTime
            if (this.priorityQueueSorter != null)
            {
                PartitionedStreamEvent<TKey, TPayload> resultEvent;
                while ((!this.priorityQueueSorter.IsEmpty()) && this.priorityQueueSorter.Peek().SyncTime <= syncTime)
                {
                    resultEvent = this.priorityQueueSorter.Dequeue();
                    Process(ref resultEvent);
                }
            }
            else
            {
                bool recheck;
                var events = this.impatienceSorter.DequeueUntil(syncTime);

                int index = FastDictionary<TKey, Tuple<bool, PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>>>>.IteratorStart;
                while (events.Iterate(ref index))
                {
                    var entry = events.entries[index];
                    recheck = entry.value.Item1;
                    var streamEvents = entry.value.Item2;
                    if (streamEvents != null)
                    {
                        PartitionedStreamEvent<TKey, TPayload> resultEvent;
                        while ((streamEvents.Count > 0) && ((!recheck) || (streamEvents.PeekFirst().SyncTime <= syncTime)))
                        {
                            resultEvent = streamEvents.Dequeue();
                            Process(ref resultEvent);
                        }
                        if (!recheck) this.impatienceSorter.Return(entry.key, streamEvents);
                    }
                }
            }

            // Update cached global times
            this.highWatermark = Math.Max(syncTime, this.highWatermark);
            if (this.lowWatermark.rawValue < syncTime)
            {
                UpdateLowWatermark(syncTime);

                // Gather keys whose high watermarks are before the new low watermark
                var expiredWatermarkKVPs = new List<KeyValuePair<long, HashSet<TKey>>>();
                foreach (var keyValuePair in this.highWatermarkToPartitionsMap)
                {
                    // Since highWatermarkToPartitionsMap is sorted, we can stop as soon as we reach the threshold
                    if (keyValuePair.Key >= this.lowWatermark.rawValue) break;

                    expiredWatermarkKVPs.Add(keyValuePair);
                }

                // Clean up state from expired partitions
                foreach (var expiredWatermarkKVP in expiredWatermarkKVPs)
                {
                    var expiredWatermark = expiredWatermarkKVP.Key;
                    this.highWatermarkToPartitionsMap.Remove(expiredWatermark);

                    var expiredKeys = expiredWatermarkKVP.Value;
                    foreach (var expiredKey in expiredKeys)
                    {
                        this.lastPunctuationTime.Remove(expiredKey);
                        this.partitionHighWatermarks.Remove(expiredKey);
                        this.currentTime.Remove(expiredKey);
#if DEBUG
                        this.lastEventTime.Remove(expiredKey);
#endif
                    }
                }
            }

            // Add LowWatermark to batch
            var count = this.currentBatch.Count;
            this.currentBatch.vsync.col[count] = syncTime;
            this.currentBatch.vother.col[count] = PartitionedStreamEvent.LowWatermarkOtherTime;
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            this.currentBatch.key.col[count] = default;
            this.currentBatch[count] = default;
            this.currentBatch.hash.col[count] = 0;
            this.currentBatch.Count = count + 1;

            // Flush if necessary
            if (this.flushPolicy == PartitionedFlushPolicy.FlushOnLowWatermark ||
                (this.flushPolicy == PartitionedFlushPolicy.FlushOnBatchBoundary && this.currentBatch.Count == Config.DataBatchSize))
            {
                OnFlush();
            }
            else if (this.currentBatch.Count == Config.DataBatchSize)
            {
                FlushContents();
            }
        }

        protected override void UpdatePointers()
        {
            foreach (var kvp in this.partitionHighWatermarks)
            {
                if (this.highWatermarkToPartitionsMap.TryGetValue(kvp.Value, out HashSet<TKey> set))
                    set.Add(kvp.Key);
                else
                    this.highWatermarkToPartitionsMap.Add(kvp.Value, new HashSet<TKey> { kvp.Key });
            }
        }

        protected override void OnCompleted(long punctuationTime)
        {
            GenerateAndProcessLowWatermark(punctuationTime);

            // Flush, but if we just flushed due to the punctuation generated above
            if (this.flushPolicy != PartitionedFlushPolicy.FlushOnLowWatermark)
                OnFlush();
        }
    }

    [DataContract]
    internal sealed class FusedPartitionedIntervalSubscriptionWithLatency<TKey, TPayload, TResult> : PartitionedObserverSubscriptionBase<TKey, TPayload, TPayload, TResult>
    {
        [SchemaSerialization]
        private readonly FuseModule fuseModule;
        private readonly Action<long, long, TPayload, PartitionKey<TKey>> action;
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, TKey>> partitionExtractor;
        private readonly Func<TPayload, TKey> partitionFunction;
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, long>> startEdgeExtractor;
        private readonly Func<TPayload, long> startEdgeFunction;
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, long>> endEdgeExtractor;
        private readonly Func<TPayload, long> endEdgeFunction;
        public FusedPartitionedIntervalSubscriptionWithLatency() { }

        public FusedPartitionedIntervalSubscriptionWithLatency(
            IObservable<TPayload> observable,
            FuseModule fuseModule,
            Expression<Func<TPayload, TKey>> partitionExtractor,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            string identifier,
            Streamable<PartitionKey<TKey>, TResult> streamable,
            IStreamObserver<PartitionKey<TKey>, TResult> observer,
            DisorderPolicy disorderPolicy,
            PartitionedFlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            PeriodicLowWatermarkPolicy lowWatermarkPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>> diagnosticOutput)
            : base(
                observable,
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
            this.fuseModule = fuseModule;
            Expression<Action<long, long, TResult, PartitionKey<TKey>>> statement = (s, e, p, k) => this.currentBatch.Add(s, e, k, p);
            Expression<Action> flush = () => FlushContents();
            Expression<Func<bool>> test = () => this.currentBatch.Count == Config.DataBatchSize;
            var full = Expression.Lambda<Action<long, long, TResult, PartitionKey<TKey>>>(
                Expression.Block(
                    statement.Body,
                    Expression.IfThen(test.Body, flush.Body)),
                statement.Parameters);
            var actionExp = fuseModule.Coalesce<TPayload, TResult, PartitionKey<TKey>>(full);
            this.action = actionExp.Compile();
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

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        public override void OnNext(TPayload inputValue)
        {
            var value = PartitionedStreamEvent.CreateInterval(this.partitionFunction(inputValue), this.startEdgeFunction(inputValue), this.endEdgeFunction(inputValue), inputValue);

            if (value.IsLowWatermark)
            {
                GenerateAndProcessLowWatermark(value.SyncTime);
                return;
            }

            // Check to see if we need to generate a low watermark due to PeriodicLowWatermarkPolicy
            if (this.lowWatermarkPolicyType == PeriodicLowWatermarkPolicyType.Time &&
                value.SyncTime > this.lowWatermarkTimestampLag)
            {
                var newLowWatermark = value.SyncTime - this.lowWatermarkTimestampLag;
                if ((ulong)(newLowWatermark - this.lowWatermark.quantizedForLowWatermarkGeneration) >= this.lowWatermarkGenerationPeriod)
                {
                    // SyncTime is sufficiently high to generate a new watermark, but first snap it to the nearest generationPeriod boundary
                    var newLowWatermarkSnapped = newLowWatermark.SnapToLeftBoundary((long)this.lowWatermarkGenerationPeriod);
                    GenerateAndProcessLowWatermark(newLowWatermarkSnapped);
                }
            }

            if (!this.currentTime.TryGetValue(value.PartitionKey, out long moveFrom) || moveFrom < this.lowWatermark.rawValue)
            {
                moveFrom = this.lowWatermark.rawValue;
            }

            if (!this.partitionHighWatermarks.ContainsKey(value.PartitionKey))
            {
                this.partitionHighWatermarks.Add(value.PartitionKey, this.lowWatermark.rawValue);

                if (this.highWatermarkToPartitionsMap.TryGetValue(this.lowWatermark.rawValue, out HashSet<TKey> keySet)) keySet.Add(value.PartitionKey);
                else this.highWatermarkToPartitionsMap.Add(this.lowWatermark.rawValue, new HashSet<TKey> { value.PartitionKey });
            }
            long moveTo = moveFrom;

            // Events at the reorder boundary or earlier - are handled using default processing policies
            if (value.SyncTime <= moveFrom)
            {
                Process(ref value);
                return;
            }

            var oldTime = this.partitionHighWatermarks[value.PartitionKey];
            if (value.SyncTime > oldTime)
            {
                this.partitionHighWatermarks[value.PartitionKey] = value.SyncTime;

                var oldSet = this.highWatermarkToPartitionsMap[oldTime];
                if (oldSet.Count <= 1) this.highWatermarkToPartitionsMap.Remove(oldTime);
                else oldSet.Remove(value.PartitionKey);

                if (this.highWatermarkToPartitionsMap.TryGetValue(value.SyncTime, out HashSet<TKey> set)) set.Add(value.PartitionKey);
                else this.highWatermarkToPartitionsMap.Add(value.SyncTime, new HashSet<TKey> { value.PartitionKey });

                moveTo = value.SyncTime - this.reorderLatency;
                if (moveTo < StreamEvent.MinSyncTime) moveTo = StreamEvent.MinSyncTime;
                if (moveTo < moveFrom) moveTo = moveFrom;
            }

            if (moveTo > moveFrom)
            {
                if (this.priorityQueueSorter != null)
                {
                    PartitionedStreamEvent<TKey, TPayload> resultEvent;

                    while ((!this.priorityQueueSorter.IsEmpty()) && this.priorityQueueSorter.Peek().SyncTime <= moveTo)
                    {
                        resultEvent = this.priorityQueueSorter.Dequeue();
                        Process(ref resultEvent);
                    }
                }
                else
                {
                    // Extract and process data in-order from impatience, until timestamp of moveTo
                    PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>> streamEvents = this.impatienceSorter.DequeueUntil(value.PartitionKey, moveTo, out bool recheck);

                    if (streamEvents != null)
                    {
                        PartitionedStreamEvent<TKey, TPayload> resultEvent;
                        while ((streamEvents.Count > 0) && ((!recheck) || (streamEvents.PeekFirst().SyncTime <= moveTo)))
                        {
                            resultEvent = streamEvents.Dequeue();
                            Process(ref resultEvent);
                        }
                    }

                    if (!recheck && (streamEvents != null))
                        this.impatienceSorter.Return(value.PartitionKey, streamEvents);
                }
            }

            if (value.SyncTime == moveTo)
            {
                Process(ref value);
                return;
            }

            // Enqueue value into impatience
            if (this.priorityQueueSorter != null) this.priorityQueueSorter.Enqueue(value);
            else this.impatienceSorter.Enqueue(ref value);

            UpdateCurrentTime(value.PartitionKey, moveTo, fromEvent: false);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void Process(ref PartitionedStreamEvent<TKey, TPayload> value)
        {
            // Update global high water mark if necessary
            this.highWatermark = Math.Max(this.highWatermark, value.SyncTime);

            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time && !this.lastPunctuationTime.ContainsKey(value.PartitionKey))
                UpdatePunctuation(value.PartitionKey, this.lowWatermark.rawValue, this.lowWatermark.quantizedForPunctuationGeneration);

            // Retrieve current time for this partition, updating currentTime if necessary
            if (!this.currentTime.TryGetValue(value.PartitionKey, out long current))
            {
                current = this.lowWatermark.rawValue;
            }
            else if (current < this.lowWatermark.rawValue)
            {
                current = this.lowWatermark.rawValue;
                UpdateCurrentTime(value.PartitionKey, this.lowWatermark.rawValue);
            }

            var outOfOrder = value.SyncTime < current;
            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                // out of order events shouldn't count, and if the disorder policy adjusts their sync time, then it
                // will be made equal to a timestamp already seen earlier in the sequence and this would have triggered
                // (if necessary) when that timestamp was seen.
                if (!outOfOrder && this.punctuationGenerationPeriod > 0)
                {
                    // We use lowWatermark as the baseline in the delta computation because a low watermark implies
                    // punctuations for all partitions
                    var prevPunctuation = Math.Max(this.lastPunctuationTime[value.PartitionKey].lastPunctuationQuantized, this.lowWatermark.quantizedForPunctuationGeneration);
                    if ((ulong)(value.SyncTime - prevPunctuation) >= this.punctuationGenerationPeriod)
                    {
                        // SyncTime is sufficiently high to generate a new punctuation, but first snap it to the nearest generationPeriod boundary
                        var punctuationTimeQuantized = value.SyncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod);
#if DEBUG
                        Debug.Assert(punctuationTimeQuantized >= LastEventTime(value.PartitionKey), "Bug in punctuation quantization logic");
#endif
                        OnPunctuation(value.CreatePunctuation(punctuationTimeQuantized));
                    }
                }
            }

            if (this.disorderPolicyType == DisorderPolicyType.Throw)
            {
                if (outOfOrder)
                {
                    throw new IngressException($"Out-of-order event encountered during ingress, under a disorder policy of Throw: value.SyncTime: {value.SyncTime}, current:{current}");
                }
            }
            else
            {
                        if (outOfOrder)
                        {
                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                return; // drop
                            }
                            else
                            {
                                if (current >= value.OtherTime)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                    return; // drop
                                }

                                this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                value = new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, current, value.OtherTime, value.Payload);
                            }
                        }
            }

            this.action(value.SyncTime, value.OtherTime, value.Payload, new PartitionKey<TKey>(value.PartitionKey));

            UpdateCurrentTime(value.PartitionKey, value.SyncTime);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateCurrentTime(TKey partitionKey, long time, bool fromEvent = true)
        {
            if (!this.currentTime.TryGetValue(partitionKey, out long oldCurrentTime) || oldCurrentTime < time)
            {
                this.currentTime[partitionKey] = time;
            }
#if DEBUG
            if (fromEvent && (!this.lastEventTime.TryGetValue(partitionKey, out long oldEventTime) || oldEventTime < time))
            {
                this.lastEventTime[partitionKey] = time;
            }
#endif
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void GenerateAndProcessLowWatermark(long syncTime)
        {
            if (syncTime <= this.lowWatermark.rawValue) return;

            // Process events queued for reorderLatency up to the LowWatermark syncTime
            if (this.priorityQueueSorter != null)
            {
                PartitionedStreamEvent<TKey, TPayload> resultEvent;
                while ((!this.priorityQueueSorter.IsEmpty()) && this.priorityQueueSorter.Peek().SyncTime <= syncTime)
                {
                    resultEvent = this.priorityQueueSorter.Dequeue();
                    Process(ref resultEvent);
                }
            }
            else
            {
                bool recheck;
                var events = this.impatienceSorter.DequeueUntil(syncTime);

                int index = FastDictionary<TKey, Tuple<bool, PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>>>>.IteratorStart;
                while (events.Iterate(ref index))
                {
                    var entry = events.entries[index];
                    recheck = entry.value.Item1;
                    var streamEvents = entry.value.Item2;
                    if (streamEvents != null)
                    {
                        PartitionedStreamEvent<TKey, TPayload> resultEvent;
                        while ((streamEvents.Count > 0) && ((!recheck) || (streamEvents.PeekFirst().SyncTime <= syncTime)))
                        {
                            resultEvent = streamEvents.Dequeue();
                            Process(ref resultEvent);
                        }
                        if (!recheck) this.impatienceSorter.Return(entry.key, streamEvents);
                    }
                }
            }

            // Update cached global times
            this.highWatermark = Math.Max(syncTime, this.highWatermark);
            if (this.lowWatermark.rawValue < syncTime)
            {
                UpdateLowWatermark(syncTime);

                // Gather keys whose high watermarks are before the new low watermark
                var expiredWatermarkKVPs = new List<KeyValuePair<long, HashSet<TKey>>>();
                foreach (var keyValuePair in this.highWatermarkToPartitionsMap)
                {
                    // Since highWatermarkToPartitionsMap is sorted, we can stop as soon as we reach the threshold
                    if (keyValuePair.Key >= this.lowWatermark.rawValue) break;

                    expiredWatermarkKVPs.Add(keyValuePair);
                }

                // Clean up state from expired partitions
                foreach (var expiredWatermarkKVP in expiredWatermarkKVPs)
                {
                    var expiredWatermark = expiredWatermarkKVP.Key;
                    this.highWatermarkToPartitionsMap.Remove(expiredWatermark);

                    var expiredKeys = expiredWatermarkKVP.Value;
                    foreach (var expiredKey in expiredKeys)
                    {
                        this.lastPunctuationTime.Remove(expiredKey);
                        this.partitionHighWatermarks.Remove(expiredKey);
                        this.currentTime.Remove(expiredKey);
#if DEBUG
                        this.lastEventTime.Remove(expiredKey);
#endif
                    }
                }
            }

            // Add LowWatermark to batch
            var count = this.currentBatch.Count;
            this.currentBatch.vsync.col[count] = syncTime;
            this.currentBatch.vother.col[count] = PartitionedStreamEvent.LowWatermarkOtherTime;
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            this.currentBatch.key.col[count] = default;
            this.currentBatch[count] = default;
            this.currentBatch.hash.col[count] = 0;
            this.currentBatch.Count = count + 1;

            // Flush if necessary
            if (this.flushPolicy == PartitionedFlushPolicy.FlushOnLowWatermark ||
                (this.flushPolicy == PartitionedFlushPolicy.FlushOnBatchBoundary && this.currentBatch.Count == Config.DataBatchSize))
            {
                OnFlush();
            }
            else if (this.currentBatch.Count == Config.DataBatchSize)
            {
                FlushContents();
            }
        }

        protected override void UpdatePointers()
        {
            foreach (var kvp in this.partitionHighWatermarks)
            {
                if (this.highWatermarkToPartitionsMap.TryGetValue(kvp.Value, out HashSet<TKey> set))
                    set.Add(kvp.Key);
                else
                    this.highWatermarkToPartitionsMap.Add(kvp.Value, new HashSet<TKey> { kvp.Key });
            }
        }

        protected override void OnCompleted(long punctuationTime)
        {
            GenerateAndProcessLowWatermark(punctuationTime);

            // Flush, but if we just flushed due to the punctuation generated above
            if (this.flushPolicy != PartitionedFlushPolicy.FlushOnLowWatermark)
                OnFlush();
        }
    }

    [DataContract]
    internal sealed class DisorderedPartitionedIntervalSubscriptionWithLatency<TKey, TPayload, TResult> : DisorderedPartitionedObserverSubscriptionBase<TKey, TPayload, TPayload, TResult>
    {
        [SchemaSerialization]
        private readonly FuseModule fuseModule;
        private readonly Action<long, long, TPayload, PartitionKey<TKey>> action;
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, TKey>> partitionExtractor;
        private readonly Func<TPayload, TKey> partitionFunction;
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, long>> startEdgeExtractor;
        private readonly Func<TPayload, long> startEdgeFunction;
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, long>> endEdgeExtractor;
        private readonly Func<TPayload, long> endEdgeFunction;
        public DisorderedPartitionedIntervalSubscriptionWithLatency() { }

        public DisorderedPartitionedIntervalSubscriptionWithLatency(
            IObservable<TPayload> observable,
            FuseModule fuseModule,
            Expression<Func<TPayload, TKey>> partitionExtractor,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            string identifier,
            Streamable<PartitionKey<TKey>, TResult> streamable,
            IStreamObserver<PartitionKey<TKey>, TResult> observer,
            DisorderPolicy disorderPolicy,
            PartitionedFlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            PeriodicLowWatermarkPolicy lowWatermarkPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>> diagnosticOutput)
            : base(
                observable,
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
            this.fuseModule = fuseModule;
            Expression<Action<long, long, TResult, PartitionKey<TKey>>> statement = (s, e, p, k) => Action(s, e, p, k);
            var actionExp = fuseModule.Coalesce<TPayload, TResult, PartitionKey<TKey>>(statement);
            this.action = actionExp.Compile();
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

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        public override void OnNext(TPayload inputValue)
        {
            this.action(this.startEdgeFunction(inputValue), this.endEdgeFunction(inputValue), inputValue, new PartitionKey<TKey>(this.partitionFunction(inputValue)));
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void Action(long start, long end, TResult payload, PartitionKey<TKey> actionKey)
        {
            var value = new PartitionedStreamEvent<TKey, TResult>(actionKey.Key, start, end, payload);

            if (value.IsLowWatermark)
            {
                GenerateAndProcessLowWatermark(value.SyncTime);
                return;
            }

            // Check to see if we need to generate a low watermark due to PeriodicLowWatermarkPolicy
            if (this.lowWatermarkPolicyType == PeriodicLowWatermarkPolicyType.Time &&
                value.SyncTime > this.lowWatermarkTimestampLag)
            {
                var newLowWatermark = value.SyncTime - this.lowWatermarkTimestampLag;
                if ((ulong)(newLowWatermark - this.lowWatermark.quantizedForLowWatermarkGeneration) >= this.lowWatermarkGenerationPeriod)
                {
                    // SyncTime is sufficiently high to generate a new watermark, but first snap it to the nearest generationPeriod boundary
                    var newLowWatermarkSnapped = newLowWatermark.SnapToLeftBoundary((long)this.lowWatermarkGenerationPeriod);
                    GenerateAndProcessLowWatermark(newLowWatermarkSnapped);
                }
            }

            if (!this.currentTime.TryGetValue(value.PartitionKey, out long moveFrom) || moveFrom < this.lowWatermark.rawValue)
            {
                moveFrom = this.lowWatermark.rawValue;
            }

            if (!this.partitionHighWatermarks.ContainsKey(value.PartitionKey))
            {
                this.partitionHighWatermarks.Add(value.PartitionKey, this.lowWatermark.rawValue);

                if (this.highWatermarkToPartitionsMap.TryGetValue(this.lowWatermark.rawValue, out HashSet<TKey> keySet)) keySet.Add(value.PartitionKey);
                else this.highWatermarkToPartitionsMap.Add(this.lowWatermark.rawValue, new HashSet<TKey> { value.PartitionKey });
            }
            long moveTo = moveFrom;

            // Events at the reorder boundary or earlier - are handled using default processing policies
            if (value.SyncTime <= moveFrom)
            {
                Process(ref value);
                return;
            }

            var oldTime = this.partitionHighWatermarks[value.PartitionKey];
            if (value.SyncTime > oldTime)
            {
                this.partitionHighWatermarks[value.PartitionKey] = value.SyncTime;

                var oldSet = this.highWatermarkToPartitionsMap[oldTime];
                if (oldSet.Count <= 1) this.highWatermarkToPartitionsMap.Remove(oldTime);
                else oldSet.Remove(value.PartitionKey);

                if (this.highWatermarkToPartitionsMap.TryGetValue(value.SyncTime, out HashSet<TKey> set)) set.Add(value.PartitionKey);
                else this.highWatermarkToPartitionsMap.Add(value.SyncTime, new HashSet<TKey> { value.PartitionKey });

                moveTo = value.SyncTime - this.reorderLatency;
                if (moveTo < StreamEvent.MinSyncTime) moveTo = StreamEvent.MinSyncTime;
                if (moveTo < moveFrom) moveTo = moveFrom;
            }

            if (moveTo > moveFrom)
            {
                if (this.priorityQueueSorter != null)
                {
                    PartitionedStreamEvent<TKey, TResult> resultEvent;

                    while ((!this.priorityQueueSorter.IsEmpty()) && this.priorityQueueSorter.Peek().SyncTime <= moveTo)
                    {
                        resultEvent = this.priorityQueueSorter.Dequeue();
                        Process(ref resultEvent);
                    }
                }
                else
                {
                    // Extract and process data in-order from impatience, until timestamp of moveTo
                    PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TResult>> streamEvents = this.impatienceSorter.DequeueUntil(value.PartitionKey, moveTo, out bool recheck);

                    if (streamEvents != null)
                    {
                        PartitionedStreamEvent<TKey, TResult> resultEvent;
                        while ((streamEvents.Count > 0) && ((!recheck) || (streamEvents.PeekFirst().SyncTime <= moveTo)))
                        {
                            resultEvent = streamEvents.Dequeue();
                            Process(ref resultEvent);
                        }
                    }

                    if (!recheck && (streamEvents != null))
                        this.impatienceSorter.Return(value.PartitionKey, streamEvents);
                }
            }

            if (value.SyncTime == moveTo)
            {
                Process(ref value);
                return;
            }

            // Enqueue value into impatience
            if (this.priorityQueueSorter != null) this.priorityQueueSorter.Enqueue(value);
            else this.impatienceSorter.Enqueue(ref value);

            UpdateCurrentTime(value.PartitionKey, moveTo, fromEvent: false);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void Process(ref PartitionedStreamEvent<TKey, TResult> value)
        {
            // Update global high water mark if necessary
            this.highWatermark = Math.Max(this.highWatermark, value.SyncTime);

            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time && !this.lastPunctuationTime.ContainsKey(value.PartitionKey))
                UpdatePunctuation(value.PartitionKey, this.lowWatermark.rawValue, this.lowWatermark.quantizedForPunctuationGeneration);

            // Retrieve current time for this partition, updating currentTime if necessary
            if (!this.currentTime.TryGetValue(value.PartitionKey, out long current))
            {
                current = this.lowWatermark.rawValue;
            }
            else if (current < this.lowWatermark.rawValue)
            {
                current = this.lowWatermark.rawValue;
                UpdateCurrentTime(value.PartitionKey, this.lowWatermark.rawValue);
            }

            var outOfOrder = value.SyncTime < current;
            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                // out of order events shouldn't count, and if the disorder policy adjusts their sync time, then it
                // will be made equal to a timestamp already seen earlier in the sequence and this would have triggered
                // (if necessary) when that timestamp was seen.
                if (!outOfOrder && this.punctuationGenerationPeriod > 0)
                {
                    // We use lowWatermark as the baseline in the delta computation because a low watermark implies
                    // punctuations for all partitions
                    var prevPunctuation = Math.Max(this.lastPunctuationTime[value.PartitionKey].lastPunctuationQuantized, this.lowWatermark.quantizedForPunctuationGeneration);
                    if ((ulong)(value.SyncTime - prevPunctuation) >= this.punctuationGenerationPeriod)
                    {
                        // SyncTime is sufficiently high to generate a new punctuation, but first snap it to the nearest generationPeriod boundary
                        var punctuationTimeQuantized = value.SyncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod);
#if DEBUG
                        Debug.Assert(punctuationTimeQuantized >= LastEventTime(value.PartitionKey), "Bug in punctuation quantization logic");
#endif
                        OnPunctuation(value.CreatePunctuation(punctuationTimeQuantized));
                    }
                }
            }

            if (this.disorderPolicyType == DisorderPolicyType.Throw)
            {
                if (outOfOrder)
                {
                    throw new IngressException($"Out-of-order event encountered during ingress, under a disorder policy of Throw: value.SyncTime: {value.SyncTime}, current:{current}");
                }
            }
            else
            {
                        if (outOfOrder)
                        {
                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, value.SyncTime, value.OtherTime, default), default));
                                return; // drop
                            }
                            else
                            {
                                if (current >= value.OtherTime)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, value.SyncTime, value.OtherTime, default), default));
                                    return; // drop
                                }

                                this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, value.SyncTime, value.OtherTime, default), new long?(current - value.SyncTime)));
                                value = new PartitionedStreamEvent<TKey, TResult>(value.PartitionKey, current, value.OtherTime, value.Payload);
                            }
                        }
            }

            this.currentBatch.Add(value.SyncTime, value.OtherTime, new PartitionKey<TKey>(value.PartitionKey), value.Payload);
            if (this.currentBatch.Count == Config.DataBatchSize)
            {
                if (this.flushPolicy == PartitionedFlushPolicy.FlushOnBatchBoundary) OnFlush();
                else FlushContents();
            }

            UpdateCurrentTime(value.PartitionKey, value.SyncTime);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateCurrentTime(TKey partitionKey, long time, bool fromEvent = true)
        {
            if (!this.currentTime.TryGetValue(partitionKey, out long oldCurrentTime) || oldCurrentTime < time)
            {
                this.currentTime[partitionKey] = time;
            }
#if DEBUG
            if (fromEvent && (!this.lastEventTime.TryGetValue(partitionKey, out long oldEventTime) || oldEventTime < time))
            {
                this.lastEventTime[partitionKey] = time;
            }
#endif
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void GenerateAndProcessLowWatermark(long syncTime)
        {
            if (syncTime <= this.lowWatermark.rawValue) return;

            // Process events queued for reorderLatency up to the LowWatermark syncTime
            if (this.priorityQueueSorter != null)
            {
                PartitionedStreamEvent<TKey, TResult> resultEvent;
                while ((!this.priorityQueueSorter.IsEmpty()) && this.priorityQueueSorter.Peek().SyncTime <= syncTime)
                {
                    resultEvent = this.priorityQueueSorter.Dequeue();
                    Process(ref resultEvent);
                }
            }
            else
            {
                bool recheck;
                var events = this.impatienceSorter.DequeueUntil(syncTime);

                int index = FastDictionary<TKey, Tuple<bool, PooledElasticCircularBuffer<PartitionedStreamEvent<TKey, TPayload>>>>.IteratorStart;
                while (events.Iterate(ref index))
                {
                    var entry = events.entries[index];
                    recheck = entry.value.Item1;
                    var streamEvents = entry.value.Item2;
                    if (streamEvents != null)
                    {
                        PartitionedStreamEvent<TKey, TResult> resultEvent;
                        while ((streamEvents.Count > 0) && ((!recheck) || (streamEvents.PeekFirst().SyncTime <= syncTime)))
                        {
                            resultEvent = streamEvents.Dequeue();
                            Process(ref resultEvent);
                        }
                        if (!recheck) this.impatienceSorter.Return(entry.key, streamEvents);
                    }
                }
            }

            // Update cached global times
            this.highWatermark = Math.Max(syncTime, this.highWatermark);
            if (this.lowWatermark.rawValue < syncTime)
            {
                UpdateLowWatermark(syncTime);

                // Gather keys whose high watermarks are before the new low watermark
                var expiredWatermarkKVPs = new List<KeyValuePair<long, HashSet<TKey>>>();
                foreach (var keyValuePair in this.highWatermarkToPartitionsMap)
                {
                    // Since highWatermarkToPartitionsMap is sorted, we can stop as soon as we reach the threshold
                    if (keyValuePair.Key >= this.lowWatermark.rawValue) break;

                    expiredWatermarkKVPs.Add(keyValuePair);
                }

                // Clean up state from expired partitions
                foreach (var expiredWatermarkKVP in expiredWatermarkKVPs)
                {
                    var expiredWatermark = expiredWatermarkKVP.Key;
                    this.highWatermarkToPartitionsMap.Remove(expiredWatermark);

                    var expiredKeys = expiredWatermarkKVP.Value;
                    foreach (var expiredKey in expiredKeys)
                    {
                        this.lastPunctuationTime.Remove(expiredKey);
                        this.partitionHighWatermarks.Remove(expiredKey);
                        this.currentTime.Remove(expiredKey);
#if DEBUG
                        this.lastEventTime.Remove(expiredKey);
#endif
                    }
                }
            }

            // Add LowWatermark to batch
            var count = this.currentBatch.Count;
            this.currentBatch.vsync.col[count] = syncTime;
            this.currentBatch.vother.col[count] = PartitionedStreamEvent.LowWatermarkOtherTime;
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            this.currentBatch.key.col[count] = default;
            this.currentBatch[count] = default;
            this.currentBatch.hash.col[count] = 0;
            this.currentBatch.Count = count + 1;

            // Flush if necessary
            if (this.flushPolicy == PartitionedFlushPolicy.FlushOnLowWatermark ||
                (this.flushPolicy == PartitionedFlushPolicy.FlushOnBatchBoundary && this.currentBatch.Count == Config.DataBatchSize))
            {
                OnFlush();
            }
            else if (this.currentBatch.Count == Config.DataBatchSize)
            {
                FlushContents();
            }
        }

        protected override void UpdatePointers()
        {
            foreach (var kvp in this.partitionHighWatermarks)
            {
                if (this.highWatermarkToPartitionsMap.TryGetValue(kvp.Value, out HashSet<TKey> set))
                    set.Add(kvp.Key);
                else
                    this.highWatermarkToPartitionsMap.Add(kvp.Value, new HashSet<TKey> { kvp.Key });
            }
        }

        protected override void OnCompleted(long punctuationTime)
        {
            GenerateAndProcessLowWatermark(punctuationTime);

            // Flush, but if we just flushed due to the punctuation generated above
            if (this.flushPolicy != PartitionedFlushPolicy.FlushOnLowWatermark)
                OnFlush();
        }
    }

    [DataContract]
    internal sealed class SimplePartitionedIntervalSubscription<TKey, TPayload> : PartitionedObserverSubscriptionBase<TKey, TPayload, TPayload, TPayload>
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
        public SimplePartitionedIntervalSubscription() { }

        public SimplePartitionedIntervalSubscription(
            IObservable<TPayload> observable,
            Expression<Func<TPayload, TKey>> partitionExtractor,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            string identifier,
            Streamable<PartitionKey<TKey>, TPayload> streamable,
            IStreamObserver<PartitionKey<TKey>, TPayload> observer,
            DisorderPolicy disorderPolicy,
            PartitionedFlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            PeriodicLowWatermarkPolicy lowWatermarkPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>> diagnosticOutput)
            : base(
                observable,
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

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        public override void OnNext(TPayload inputValue)
        {
            var value = PartitionedStreamEvent.CreateInterval(this.partitionFunction(inputValue), this.startEdgeFunction(inputValue), this.endEdgeFunction(inputValue), inputValue);

            if (value.IsLowWatermark)
            {
                GenerateAndProcessLowWatermark(value.SyncTime);
                return;
            }

            // Check to see if we need to generate a low watermark due to PeriodicLowWatermarkPolicy
            if (this.lowWatermarkPolicyType == PeriodicLowWatermarkPolicyType.Time &&
                value.SyncTime > this.lowWatermarkTimestampLag)
            {
                var newLowWatermark = value.SyncTime - this.lowWatermarkTimestampLag;
                if ((ulong)(newLowWatermark - this.lowWatermark.quantizedForLowWatermarkGeneration) >= this.lowWatermarkGenerationPeriod)
                {
                    // SyncTime is sufficiently high to generate a new watermark, but first snap it to the nearest generationPeriod boundary
                    var newLowWatermarkSnapped = newLowWatermark.SnapToLeftBoundary((long)this.lowWatermarkGenerationPeriod);
                    GenerateAndProcessLowWatermark(newLowWatermarkSnapped);
                }
            }

            // Update global high water mark if necessary
            this.highWatermark = Math.Max(this.highWatermark, value.SyncTime);

            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time && !this.lastPunctuationTime.ContainsKey(value.PartitionKey))
                UpdatePunctuation(value.PartitionKey, this.lowWatermark.rawValue, this.lowWatermark.quantizedForPunctuationGeneration);

            // Retrieve current time for this partition, updating currentTime if necessary
            if (!this.currentTime.TryGetValue(value.PartitionKey, out long current))
            {
                current = this.lowWatermark.rawValue;
            }
            else if (current < this.lowWatermark.rawValue)
            {
                current = this.lowWatermark.rawValue;
                UpdateCurrentTime(value.PartitionKey, this.lowWatermark.rawValue);
            }

            var outOfOrder = value.SyncTime < current;
            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                // out of order events shouldn't count, and if the disorder policy adjusts their sync time, then it
                // will be made equal to a timestamp already seen earlier in the sequence and this would have triggered
                // (if necessary) when that timestamp was seen.
                if (!outOfOrder && this.punctuationGenerationPeriod > 0)
                {
                    // We use lowWatermark as the baseline in the delta computation because a low watermark implies
                    // punctuations for all partitions
                    var prevPunctuation = Math.Max(this.lastPunctuationTime[value.PartitionKey].lastPunctuationQuantized, this.lowWatermark.quantizedForPunctuationGeneration);
                    if ((ulong)(value.SyncTime - prevPunctuation) >= this.punctuationGenerationPeriod)
                    {
                        // SyncTime is sufficiently high to generate a new punctuation, but first snap it to the nearest generationPeriod boundary
                        var punctuationTimeQuantized = value.SyncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod);
#if DEBUG
                        Debug.Assert(punctuationTimeQuantized >= LastEventTime(value.PartitionKey), "Bug in punctuation quantization logic");
#endif
                        OnPunctuation(value.CreatePunctuation(punctuationTimeQuantized));
                    }
                }
            }

            if (this.disorderPolicyType == DisorderPolicyType.Throw)
            {
                if (outOfOrder)
                {
                    throw new IngressException($"Out-of-order event encountered during ingress, under a disorder policy of Throw: value.SyncTime: {value.SyncTime}, current:{current}");
                }
            }
            else
            {
                        if (outOfOrder)
                        {
                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                return; // drop
                            }
                            else
                            {
                                if (current >= value.OtherTime)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                    return; // drop
                                }

                                this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                value = new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, current, value.OtherTime, value.Payload);
                            }
                        }
            }

            this.currentBatch.Add(value.SyncTime, value.OtherTime, new PartitionKey<TKey>(value.PartitionKey), value.Payload);
            if (this.currentBatch.Count == Config.DataBatchSize)
            {
                if (this.flushPolicy == PartitionedFlushPolicy.FlushOnBatchBoundary) OnFlush();
                else FlushContents();
            }

            UpdateCurrentTime(value.PartitionKey, value.SyncTime);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateCurrentTime(TKey partitionKey, long time, bool fromEvent = true)
        {
            if (!this.currentTime.TryGetValue(partitionKey, out long oldCurrentTime) || oldCurrentTime < time)
            {
                this.currentTime[partitionKey] = time;
            }
#if DEBUG
            if (fromEvent && (!this.lastEventTime.TryGetValue(partitionKey, out long oldEventTime) || oldEventTime < time))
            {
                this.lastEventTime[partitionKey] = time;
            }
#endif
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void GenerateAndProcessLowWatermark(long syncTime)
        {
            if (syncTime <= this.lowWatermark.rawValue) return;

            // Update cached global times
            this.highWatermark = Math.Max(syncTime, this.highWatermark);
            if (this.lowWatermark.rawValue < syncTime)
            {
                UpdateLowWatermark(syncTime);
            }

            // Add LowWatermark to batch
            var count = this.currentBatch.Count;
            this.currentBatch.vsync.col[count] = syncTime;
            this.currentBatch.vother.col[count] = PartitionedStreamEvent.LowWatermarkOtherTime;
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            this.currentBatch.key.col[count] = default;
            this.currentBatch[count] = default;
            this.currentBatch.hash.col[count] = 0;
            this.currentBatch.Count = count + 1;

            // Flush if necessary
            if (this.flushPolicy == PartitionedFlushPolicy.FlushOnLowWatermark ||
                (this.flushPolicy == PartitionedFlushPolicy.FlushOnBatchBoundary && this.currentBatch.Count == Config.DataBatchSize))
            {
                OnFlush();
            }
            else if (this.currentBatch.Count == Config.DataBatchSize)
            {
                FlushContents();
            }
        }

        protected override void OnCompleted(long punctuationTime)
        {
            GenerateAndProcessLowWatermark(punctuationTime);

            // Flush, but if we just flushed due to the punctuation generated above
            if (this.flushPolicy != PartitionedFlushPolicy.FlushOnLowWatermark)
                OnFlush();
        }
    }

    [DataContract]
    internal sealed class FusedPartitionedIntervalSubscription<TKey, TPayload, TResult> : PartitionedObserverSubscriptionBase<TKey, TPayload, TPayload, TResult>
    {
        [SchemaSerialization]
        private readonly FuseModule fuseModule;
        private readonly Action<long, long, TPayload, PartitionKey<TKey>> action;
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, TKey>> partitionExtractor;
        private readonly Func<TPayload, TKey> partitionFunction;
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, long>> startEdgeExtractor;
        private readonly Func<TPayload, long> startEdgeFunction;
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, long>> endEdgeExtractor;
        private readonly Func<TPayload, long> endEdgeFunction;
        public FusedPartitionedIntervalSubscription() { }

        public FusedPartitionedIntervalSubscription(
            IObservable<TPayload> observable,
            FuseModule fuseModule,
            Expression<Func<TPayload, TKey>> partitionExtractor,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            string identifier,
            Streamable<PartitionKey<TKey>, TResult> streamable,
            IStreamObserver<PartitionKey<TKey>, TResult> observer,
            DisorderPolicy disorderPolicy,
            PartitionedFlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            PeriodicLowWatermarkPolicy lowWatermarkPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>> diagnosticOutput)
            : base(
                observable,
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
            this.fuseModule = fuseModule;
            Expression<Action<long, long, TResult, PartitionKey<TKey>>> statement = (s, e, p, k) => this.currentBatch.Add(s, e, k, p);
            Expression<Action> flush = () => FlushContents();
            Expression<Func<bool>> test = () => this.currentBatch.Count == Config.DataBatchSize;
            var full = Expression.Lambda<Action<long, long, TResult, PartitionKey<TKey>>>(
                Expression.Block(
                    statement.Body,
                    Expression.IfThen(test.Body, flush.Body)),
                statement.Parameters);
            var actionExp = fuseModule.Coalesce<TPayload, TResult, PartitionKey<TKey>>(full);
            this.action = actionExp.Compile();
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

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        public override void OnNext(TPayload inputValue)
        {
            var value = PartitionedStreamEvent.CreateInterval(this.partitionFunction(inputValue), this.startEdgeFunction(inputValue), this.endEdgeFunction(inputValue), inputValue);

            if (value.IsLowWatermark)
            {
                GenerateAndProcessLowWatermark(value.SyncTime);
                return;
            }

            // Check to see if we need to generate a low watermark due to PeriodicLowWatermarkPolicy
            if (this.lowWatermarkPolicyType == PeriodicLowWatermarkPolicyType.Time &&
                value.SyncTime > this.lowWatermarkTimestampLag)
            {
                var newLowWatermark = value.SyncTime - this.lowWatermarkTimestampLag;
                if ((ulong)(newLowWatermark - this.lowWatermark.quantizedForLowWatermarkGeneration) >= this.lowWatermarkGenerationPeriod)
                {
                    // SyncTime is sufficiently high to generate a new watermark, but first snap it to the nearest generationPeriod boundary
                    var newLowWatermarkSnapped = newLowWatermark.SnapToLeftBoundary((long)this.lowWatermarkGenerationPeriod);
                    GenerateAndProcessLowWatermark(newLowWatermarkSnapped);
                }
            }

            // Update global high water mark if necessary
            this.highWatermark = Math.Max(this.highWatermark, value.SyncTime);

            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time && !this.lastPunctuationTime.ContainsKey(value.PartitionKey))
                UpdatePunctuation(value.PartitionKey, this.lowWatermark.rawValue, this.lowWatermark.quantizedForPunctuationGeneration);

            // Retrieve current time for this partition, updating currentTime if necessary
            if (!this.currentTime.TryGetValue(value.PartitionKey, out long current))
            {
                current = this.lowWatermark.rawValue;
            }
            else if (current < this.lowWatermark.rawValue)
            {
                current = this.lowWatermark.rawValue;
                UpdateCurrentTime(value.PartitionKey, this.lowWatermark.rawValue);
            }

            var outOfOrder = value.SyncTime < current;
            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                // out of order events shouldn't count, and if the disorder policy adjusts their sync time, then it
                // will be made equal to a timestamp already seen earlier in the sequence and this would have triggered
                // (if necessary) when that timestamp was seen.
                if (!outOfOrder && this.punctuationGenerationPeriod > 0)
                {
                    // We use lowWatermark as the baseline in the delta computation because a low watermark implies
                    // punctuations for all partitions
                    var prevPunctuation = Math.Max(this.lastPunctuationTime[value.PartitionKey].lastPunctuationQuantized, this.lowWatermark.quantizedForPunctuationGeneration);
                    if ((ulong)(value.SyncTime - prevPunctuation) >= this.punctuationGenerationPeriod)
                    {
                        // SyncTime is sufficiently high to generate a new punctuation, but first snap it to the nearest generationPeriod boundary
                        var punctuationTimeQuantized = value.SyncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod);
#if DEBUG
                        Debug.Assert(punctuationTimeQuantized >= LastEventTime(value.PartitionKey), "Bug in punctuation quantization logic");
#endif
                        OnPunctuation(value.CreatePunctuation(punctuationTimeQuantized));
                    }
                }
            }

            if (this.disorderPolicyType == DisorderPolicyType.Throw)
            {
                if (outOfOrder)
                {
                    throw new IngressException($"Out-of-order event encountered during ingress, under a disorder policy of Throw: value.SyncTime: {value.SyncTime}, current:{current}");
                }
            }
            else
            {
                        if (outOfOrder)
                        {
                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                return; // drop
                            }
                            else
                            {
                                if (current >= value.OtherTime)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, default));
                                    return; // drop
                                }

                                this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(value, new long?(current - value.SyncTime)));
                                value = new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, current, value.OtherTime, value.Payload);
                            }
                        }
            }

            this.action(value.SyncTime, value.OtherTime, value.Payload, new PartitionKey<TKey>(value.PartitionKey));

            UpdateCurrentTime(value.PartitionKey, value.SyncTime);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateCurrentTime(TKey partitionKey, long time, bool fromEvent = true)
        {
            if (!this.currentTime.TryGetValue(partitionKey, out long oldCurrentTime) || oldCurrentTime < time)
            {
                this.currentTime[partitionKey] = time;
            }
#if DEBUG
            if (fromEvent && (!this.lastEventTime.TryGetValue(partitionKey, out long oldEventTime) || oldEventTime < time))
            {
                this.lastEventTime[partitionKey] = time;
            }
#endif
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void GenerateAndProcessLowWatermark(long syncTime)
        {
            if (syncTime <= this.lowWatermark.rawValue) return;

            // Update cached global times
            this.highWatermark = Math.Max(syncTime, this.highWatermark);
            if (this.lowWatermark.rawValue < syncTime)
            {
                UpdateLowWatermark(syncTime);
            }

            // Add LowWatermark to batch
            var count = this.currentBatch.Count;
            this.currentBatch.vsync.col[count] = syncTime;
            this.currentBatch.vother.col[count] = PartitionedStreamEvent.LowWatermarkOtherTime;
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            this.currentBatch.key.col[count] = default;
            this.currentBatch[count] = default;
            this.currentBatch.hash.col[count] = 0;
            this.currentBatch.Count = count + 1;

            // Flush if necessary
            if (this.flushPolicy == PartitionedFlushPolicy.FlushOnLowWatermark ||
                (this.flushPolicy == PartitionedFlushPolicy.FlushOnBatchBoundary && this.currentBatch.Count == Config.DataBatchSize))
            {
                OnFlush();
            }
            else if (this.currentBatch.Count == Config.DataBatchSize)
            {
                FlushContents();
            }
        }

        protected override void OnCompleted(long punctuationTime)
        {
            GenerateAndProcessLowWatermark(punctuationTime);

            // Flush, but if we just flushed due to the punctuation generated above
            if (this.flushPolicy != PartitionedFlushPolicy.FlushOnLowWatermark)
                OnFlush();
        }
    }

    [DataContract]
    internal sealed class DisorderedPartitionedIntervalSubscription<TKey, TPayload, TResult> : DisorderedPartitionedObserverSubscriptionBase<TKey, TPayload, TPayload, TResult>
    {
        [SchemaSerialization]
        private readonly FuseModule fuseModule;
        private readonly Action<long, long, TPayload, PartitionKey<TKey>> action;
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, TKey>> partitionExtractor;
        private readonly Func<TPayload, TKey> partitionFunction;
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, long>> startEdgeExtractor;
        private readonly Func<TPayload, long> startEdgeFunction;
        [SchemaSerialization]
        private readonly Expression<Func<TPayload, long>> endEdgeExtractor;
        private readonly Func<TPayload, long> endEdgeFunction;
        public DisorderedPartitionedIntervalSubscription() { }

        public DisorderedPartitionedIntervalSubscription(
            IObservable<TPayload> observable,
            FuseModule fuseModule,
            Expression<Func<TPayload, TKey>> partitionExtractor,
            Expression<Func<TPayload, long>> startEdgeExtractor,
            Expression<Func<TPayload, long>> endEdgeExtractor,
            string identifier,
            Streamable<PartitionKey<TKey>, TResult> streamable,
            IStreamObserver<PartitionKey<TKey>, TResult> observer,
            DisorderPolicy disorderPolicy,
            PartitionedFlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            PeriodicLowWatermarkPolicy lowWatermarkPolicy,
            OnCompletedPolicy onCompletedPolicy,
            IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>> diagnosticOutput)
            : base(
                observable,
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
            this.fuseModule = fuseModule;
            Expression<Action<long, long, TResult, PartitionKey<TKey>>> statement = (s, e, p, k) => Action(s, e, p, k);
            var actionExp = fuseModule.Coalesce<TPayload, TResult, PartitionKey<TKey>>(statement);
            this.action = actionExp.Compile();
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

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        public override void OnNext(TPayload inputValue)
        {
            this.action(this.startEdgeFunction(inputValue), this.endEdgeFunction(inputValue), inputValue, new PartitionKey<TKey>(this.partitionFunction(inputValue)));
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void Action(long start, long end, TResult payload, PartitionKey<TKey> actionKey)
        {
            var value = new PartitionedStreamEvent<TKey, TResult>(actionKey.Key, start, end, payload);

            if (value.IsLowWatermark)
            {
                GenerateAndProcessLowWatermark(value.SyncTime);
                return;
            }

            // Check to see if we need to generate a low watermark due to PeriodicLowWatermarkPolicy
            if (this.lowWatermarkPolicyType == PeriodicLowWatermarkPolicyType.Time &&
                value.SyncTime > this.lowWatermarkTimestampLag)
            {
                var newLowWatermark = value.SyncTime - this.lowWatermarkTimestampLag;
                if ((ulong)(newLowWatermark - this.lowWatermark.quantizedForLowWatermarkGeneration) >= this.lowWatermarkGenerationPeriod)
                {
                    // SyncTime is sufficiently high to generate a new watermark, but first snap it to the nearest generationPeriod boundary
                    var newLowWatermarkSnapped = newLowWatermark.SnapToLeftBoundary((long)this.lowWatermarkGenerationPeriod);
                    GenerateAndProcessLowWatermark(newLowWatermarkSnapped);
                }
            }

            // Update global high water mark if necessary
            this.highWatermark = Math.Max(this.highWatermark, value.SyncTime);

            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time && !this.lastPunctuationTime.ContainsKey(value.PartitionKey))
                UpdatePunctuation(value.PartitionKey, this.lowWatermark.rawValue, this.lowWatermark.quantizedForPunctuationGeneration);

            // Retrieve current time for this partition, updating currentTime if necessary
            if (!this.currentTime.TryGetValue(value.PartitionKey, out long current))
            {
                current = this.lowWatermark.rawValue;
            }
            else if (current < this.lowWatermark.rawValue)
            {
                current = this.lowWatermark.rawValue;
                UpdateCurrentTime(value.PartitionKey, this.lowWatermark.rawValue);
            }

            var outOfOrder = value.SyncTime < current;
            if (this.punctuationPolicyType == PeriodicPunctuationPolicyType.Time)
            {
                // out of order events shouldn't count, and if the disorder policy adjusts their sync time, then it
                // will be made equal to a timestamp already seen earlier in the sequence and this would have triggered
                // (if necessary) when that timestamp was seen.
                if (!outOfOrder && this.punctuationGenerationPeriod > 0)
                {
                    // We use lowWatermark as the baseline in the delta computation because a low watermark implies
                    // punctuations for all partitions
                    var prevPunctuation = Math.Max(this.lastPunctuationTime[value.PartitionKey].lastPunctuationQuantized, this.lowWatermark.quantizedForPunctuationGeneration);
                    if ((ulong)(value.SyncTime - prevPunctuation) >= this.punctuationGenerationPeriod)
                    {
                        // SyncTime is sufficiently high to generate a new punctuation, but first snap it to the nearest generationPeriod boundary
                        var punctuationTimeQuantized = value.SyncTime.SnapToLeftBoundary((long)this.punctuationGenerationPeriod);
#if DEBUG
                        Debug.Assert(punctuationTimeQuantized >= LastEventTime(value.PartitionKey), "Bug in punctuation quantization logic");
#endif
                        OnPunctuation(value.CreatePunctuation(punctuationTimeQuantized));
                    }
                }
            }

            if (this.disorderPolicyType == DisorderPolicyType.Throw)
            {
                if (outOfOrder)
                {
                    throw new IngressException($"Out-of-order event encountered during ingress, under a disorder policy of Throw: value.SyncTime: {value.SyncTime}, current:{current}");
                }
            }
            else
            {
                        if (outOfOrder)
                        {
                            if (this.disorderPolicyType == DisorderPolicyType.Drop)
                            {
                                this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, value.SyncTime, value.OtherTime, default), default));
                                return; // drop
                            }
                            else
                            {
                                if (current >= value.OtherTime)
                                {
                                    this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, value.SyncTime, value.OtherTime, default), default));
                                    return; // drop
                                }

                                this.diagnosticOutput?.OnNext(OutOfOrderPartitionedStreamEvent.Create(new PartitionedStreamEvent<TKey, TPayload>(value.PartitionKey, value.SyncTime, value.OtherTime, default), new long?(current - value.SyncTime)));
                                value = new PartitionedStreamEvent<TKey, TResult>(value.PartitionKey, current, value.OtherTime, value.Payload);
                            }
                        }
            }

            this.currentBatch.Add(value.SyncTime, value.OtherTime, new PartitionKey<TKey>(value.PartitionKey), value.Payload);
            if (this.currentBatch.Count == Config.DataBatchSize)
            {
                if (this.flushPolicy == PartitionedFlushPolicy.FlushOnBatchBoundary) OnFlush();
                else FlushContents();
            }

            UpdateCurrentTime(value.PartitionKey, value.SyncTime);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateCurrentTime(TKey partitionKey, long time, bool fromEvent = true)
        {
            if (!this.currentTime.TryGetValue(partitionKey, out long oldCurrentTime) || oldCurrentTime < time)
            {
                this.currentTime[partitionKey] = time;
            }
#if DEBUG
            if (fromEvent && (!this.lastEventTime.TryGetValue(partitionKey, out long oldEventTime) || oldEventTime < time))
            {
                this.lastEventTime[partitionKey] = time;
            }
#endif
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Microsoft.StyleCop.CSharp.DocumentationRules",
            "SA1028:CodeMustNotContainTrailingWhitespace",
            Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
        private void GenerateAndProcessLowWatermark(long syncTime)
        {
            if (syncTime <= this.lowWatermark.rawValue) return;

            // Update cached global times
            this.highWatermark = Math.Max(syncTime, this.highWatermark);
            if (this.lowWatermark.rawValue < syncTime)
            {
                UpdateLowWatermark(syncTime);
            }

            // Add LowWatermark to batch
            var count = this.currentBatch.Count;
            this.currentBatch.vsync.col[count] = syncTime;
            this.currentBatch.vother.col[count] = PartitionedStreamEvent.LowWatermarkOtherTime;
            this.currentBatch.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
            this.currentBatch.key.col[count] = default;
            this.currentBatch[count] = default;
            this.currentBatch.hash.col[count] = 0;
            this.currentBatch.Count = count + 1;

            // Flush if necessary
            if (this.flushPolicy == PartitionedFlushPolicy.FlushOnLowWatermark ||
                (this.flushPolicy == PartitionedFlushPolicy.FlushOnBatchBoundary && this.currentBatch.Count == Config.DataBatchSize))
            {
                OnFlush();
            }
            else if (this.currentBatch.Count == Config.DataBatchSize)
            {
                FlushContents();
            }
        }

        protected override void OnCompleted(long punctuationTime)
        {
            GenerateAndProcessLowWatermark(punctuationTime);

            // Flush, but if we just flushed due to the punctuation generated above
            if (this.flushPolicy != PartitionedFlushPolicy.FlushOnLowWatermark)
                OnFlush();
        }
    }

}