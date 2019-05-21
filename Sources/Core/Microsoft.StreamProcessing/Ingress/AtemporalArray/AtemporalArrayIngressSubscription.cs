// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class MonotonicArraySubscriptionWallClock<TPayload> : ObserverSubscriptionBase<ArraySegment<TPayload>, TPayload, TPayload>
    {

        public MonotonicArraySubscriptionWallClock() { }

        public MonotonicArraySubscriptionWallClock(
            IObservable<ArraySegment<TPayload>> observable,
            string identifier,
            Streamable<Empty, TPayload> streamable,
            IStreamObserver<Empty, TPayload> observer,
            OnCompletedPolicy onCompletedPolicy,
            TimelinePolicy timelinePolicy)
            : base(
                observable,
                identifier,
                streamable,
                observer,
                DisorderPolicy.Throw(),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.None(),
                onCompletedPolicy,
                null)
        { }

        public override void OnNext(ArraySegment<TPayload> value)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            int n = value.Count + value.Offset;

            // Sanity check
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
                    ref offset);

                if (full)
                {
                    System.Array.Clear(this.currentBatch.hash.col, 0, this.currentBatch.hash.col.Length);
                    FlushContents();
                }
            }
        }

        protected override void OnCompleted(long punctuationTime)
        {
            FlushContents();
            OnPunctuation(StreamEvent.CreatePunctuation<TPayload>(punctuationTime));
        }
    }

    [DataContract]
    internal sealed class MonotonicArraySubscriptionSequence<TPayload> : ObserverSubscriptionBase<ArraySegment<TPayload>, TPayload, TPayload>
    {
        [SchemaSerialization]
        private readonly int eventsPerSample;
        [DataMember]
        private int currentSync = 0;
        [DataMember]
        private int eventCount = 0;

        public MonotonicArraySubscriptionSequence() { }

        public MonotonicArraySubscriptionSequence(
            IObservable<ArraySegment<TPayload>> observable,
            string identifier,
            Streamable<Empty, TPayload> streamable,
            IStreamObserver<Empty, TPayload> observer,
            OnCompletedPolicy onCompletedPolicy,
            TimelinePolicy timelinePolicy)
            : base(
                observable,
                identifier,
                streamable,
                observer,
                DisorderPolicy.Throw(),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.None(),
                onCompletedPolicy,
                null)
            => this.eventsPerSample = timelinePolicy.sampleSize;

        public override void OnNext(ArraySegment<TPayload> value)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

            int n = value.Count + value.Offset;

            // Sanity check
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
                    this.eventsPerSample,
                    ref this.currentSync,
                    ref this.eventCount,
                    out bool hitAPunctuation);
                if (hitAPunctuation)
                {
                    var current = StreamEvent.CreatePunctuation<TPayload>(this.currentTime);
                    System.Array.Clear(this.currentBatch.hash.col, 0, this.currentBatch.hash.col.Length);
                    OnPunctuation(current);
                    offset++;
                }
                else if (full)
                {
                    System.Array.Clear(this.currentBatch.hash.col, 0, this.currentBatch.hash.col.Length);
                    FlushContents();
                }
            }
        }

        protected override void OnCompleted(long punctuationTime)
        {
            FlushContents();
            OnPunctuation(StreamEvent.CreatePunctuation<TPayload>(punctuationTime));
        }
    }

}
