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

        public static bool IsEquivalentTo<TPayload>(this IStreamable<Empty, TPayload> input, StreamEvent<TPayload>[] comparison)
        {
            Invariant.IsNotNull(input, "input");
            Invariant.IsNotNull(comparison, "comparison");
            var events = input.ToStreamEventArray();
            var orderedEvents = events.OrderBy(e => e.SyncTime).ThenBy(e => e.OtherTime).ThenBy(e => e.Payload).ToArray();
            var orderedComparison = comparison.OrderBy(e => e.SyncTime).ThenBy(e => e.OtherTime).ThenBy(e => e.Payload).ToArray();

            return orderedEvents.SequenceEqual(orderedComparison);
        }
    }

}
