// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Runtime.Serialization;
using System.Threading;
using Microsoft.StreamProcessing.Internal;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class MonotonicSubscriptionWallClock<TPayload> : ObserverSubscriptionBase<TPayload, TPayload, TPayload>
    {
        private readonly Lock sentinel = new();
        private IDisposable timer;

        public MonotonicSubscriptionWallClock() { }

        public MonotonicSubscriptionWallClock(
            IObservable<TPayload> observable,
            string identifier,
            Streamable<Empty, TPayload> streamable,
            IStreamObserver<Empty, TPayload> observer,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            OnCompletedPolicy onCompletedPolicy,
            TimelinePolicy timelinePolicy)
            : base(
                observable,
                identifier,
                streamable,
                observer,
                DisorderPolicy.Adjust(),
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                null)
        {
            if (timelinePolicy.punctuationInterval > default(TimeSpan))
                this.timer = new Timer(this.EmitPunctuation, null, new TimeSpan(0), timelinePolicy.punctuationInterval);
        }

        public override void OnCompleted()
        {
            using (sentinel.EnterScope())
            {
                this.timer?.Dispose();
                this.timer = null;

                base.OnCompleted();
            }
        }

        public override void OnNext(TPayload value)
        {
            Contract.EnsuresOnThrow<IngressException>(true);
            using (sentinel.EnterScope())
            {
                this.currentTime = Math.Max(DateTimeOffset.UtcNow.Ticks, this.currentTime);

                this.currentBatch.Add(this.currentTime, StreamEvent.InfinitySyncTime, Empty.Default, value);
                if (this.currentBatch.Count == Config.DataBatchSize) this.FlushContents();
            }
        }

        private void EmitPunctuation(object state)
        {
            using (sentinel.EnterScope())
            {
                if (this.timer != null)
                {
                    var time = DateTimeOffset.UtcNow.Ticks;
                    this.currentTime = Math.Max(time, this.currentTime);
                    this.OnPunctuation(StreamEvent.CreatePunctuation<TPayload>(this.currentTime));
                }
            }
        }

        protected override void DisposeState()
        {
            using (sentinel.EnterScope())
            {
                this.timer?.Dispose();
                this.timer = null;

                base.DisposeState();
            }
        }

        protected override void OnCompleted(long punctuationTime)
        {
            this.FlushContents();
            this.OnPunctuation(StreamEvent.CreatePunctuation<TPayload>(punctuationTime));
            if (this.flushPolicy != FlushPolicy.FlushOnPunctuation)
            {
                this.OnFlush();
            }
        }
    }

    [DataContract]
    internal sealed class MonotonicSubscriptionSequence<TPayload> : ObserverSubscriptionBase<TPayload, TPayload, TPayload>
    {
        [SchemaSerialization]
        private readonly int eventsPerSample;
        [DataMember]
        private int eventCount = 0;

        public MonotonicSubscriptionSequence() { }

        public MonotonicSubscriptionSequence(
            IObservable<TPayload> observable,
            string identifier,
            Streamable<Empty, TPayload> streamable,
            IStreamObserver<Empty, TPayload> observer,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            OnCompletedPolicy onCompletedPolicy,
            TimelinePolicy timelinePolicy)
            : base(
                observable,
                identifier,
                streamable,
                observer,
                DisorderPolicy.Adjust(),
                flushPolicy,
                punctuationPolicy,
                onCompletedPolicy,
                null)
        {
            this.eventsPerSample = timelinePolicy.sampleSize;
            this.currentTime = 0;
        }

        public override void OnNext(TPayload value)
        {
            Contract.EnsuresOnThrow<IngressException>(true);

                this.currentBatch.Add(this.currentTime, StreamEvent.InfinitySyncTime, Empty.Default, value);
                if (this.currentBatch.Count == Config.DataBatchSize) this.FlushContents();
                this.eventCount++;

                if (this.eventCount == this.eventsPerSample)
                {
                    this.eventCount = 0;
                    this.currentTime++;

                    this.FlushContents();
                    this.OnPunctuation(StreamEvent.CreatePunctuation<TPayload>(this.currentTime));
                }
        }

        protected override void OnCompleted(long punctuationTime)
        {
            this.FlushContents();
            this.OnPunctuation(StreamEvent.CreatePunctuation<TPayload>(punctuationTime));
            if (this.flushPolicy != FlushPolicy.FlushOnPunctuation)
            {
                this.OnFlush();
            }
        }
    }

}
