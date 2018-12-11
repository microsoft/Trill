// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
namespace ComponentTesting
{
    using System;
    using System.Linq;
    using System.Reactive.Linq;
    using Microsoft.StreamProcessing;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    public static class TestUtils
    {
        public static IStreamable<Empty, TPayload> ToCleanStreamable<TPayload>(this StreamEvent<TPayload>[] input)
        {
            Invariant.IsNotNull(input, "input");
            return input.OrderBy(v => v.SyncTime).ToArray().ToStreamable();
        }

        public static IStreamable<Empty, TPayload> ToStreamable<TPayload>(this StreamEvent<TPayload>[] input)
        {
            Invariant.IsNotNull(input, "input");

            return input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation);
        }

        public static StreamEvent<TPayload>[] ToStreamEventArray<TPayload>(this IStreamable<Empty, TPayload> input)
        {
            Invariant.IsNotNull(input, "input");
            return input.ToStreamEventObservable().ToEnumerable().ToArray();
        }

        public static void Print<TKey, TPayload>(this IStreamable<TKey, TPayload> input)
        {
            Invariant.IsNotNull(input, "input");
            input.ToStreamMessageObservable().ForEachAsync(b => b.Print()).Wait();
        }

        public static bool IsEquivalentTo<TPayload>(this IStreamable<Empty, TPayload> input, StreamEvent<TPayload>[] comparison)
        {
            Invariant.IsNotNull(input, "input");
            Invariant.IsNotNull(comparison, "comparison");
            var events = input.ToStreamEventArray();
            var orderedEvents = events.OrderBy(e => e.SyncTime).ThenBy(e => e.OtherTime).ThenBy(e => e.Payload).ToArray();
            var orderedComparison = comparison.OrderBy(e => e.SyncTime).ThenBy(e => e.OtherTime).ThenBy(e => e.Payload).ToArray();

            return orderedEvents.SequenceEqual(orderedComparison);
        }

        public static bool IsOrderSensitiveEquivalentTo<TPayload>(this IStreamable<Empty, TPayload> input, StreamEvent<TPayload>[] comparison)
        {
            Invariant.IsNotNull(input, "input");
            Invariant.IsNotNull(comparison, "comparison");
            var events = input.ToStreamEventArray();

            // Reodrer stream events with the same timstamp
            var orderedEvents = (StreamEvent<TPayload>[])events.Clone();
            int start = 0;
            while (start < events.Length)
            {
                int end = start + 1;
                while (end < events.Length && events[end].SyncTime == events[start].SyncTime && !events[start].IsPunctuation && !events[end].IsPunctuation)
                    end++;

                var segment = new ArraySegment<StreamEvent<TPayload>>(events, start, end - start);
                segment.OrderBy(e => e.OtherTime).ThenBy(e => e.Payload).ToArray().CopyTo(orderedEvents, start);
                start = end;
            }

            // Reodrer stream events with the same timstamp
            var orderedComparison = (StreamEvent<TPayload>[])comparison.Clone();
            start = 0;
            while (start < comparison.Length)
            {
                int end = start + 1;
                while (end < comparison.Length && comparison[end].SyncTime == comparison[start].SyncTime && !comparison[start].IsPunctuation && !comparison[end].IsPunctuation)
                    end++;

                var segment = new ArraySegment<StreamEvent<TPayload>>(comparison, start, end - start);
                segment.OrderBy(e => e.OtherTime).ThenBy(e => e.Payload).ToArray().CopyTo(orderedComparison, start);
                start = end;
            }

            return orderedEvents.SequenceEqual(orderedComparison);
        }
    }

    public abstract class TestWithConfigSettingsAndMemoryLeakDetection
    {
        private IDisposable confmod = null;
        private ConfigModifier modifier;

        protected TestWithConfigSettingsAndMemoryLeakDetection() : this(new ConfigModifier()) { }

        internal TestWithConfigSettingsAndMemoryLeakDetection(ConfigModifier modifier)
        {
            this.modifier = modifier;
        }

        [TestInitialize]
        public virtual void Setup()
        {
            this.confmod = this.modifier.Modify();
            MemoryManager.Free(true);
        }

        [TestCleanup]
        public virtual void TearDown()
        {
            var cm = System.Threading.Interlocked.Exchange(ref this.confmod, null);
            if (cm != null) { cm.Dispose(); }
            var leaked = MemoryManager.Leaked().Any();
            var msg = string.Empty;
            if (leaked)
            {
                msg = "Memory Leak!\n" + MemoryManager.GetStatusReport();
            }
            Assert.IsFalse(leaked, msg);
        }
    }

}
