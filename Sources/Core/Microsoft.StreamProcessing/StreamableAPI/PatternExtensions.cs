// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Extensions to support streaming pattern detection
    /// </summary>
    public static partial class Streamable
    {
        /// <summary>
        /// Finds patterns of A followed immediately by B (with no other intermediate events), occurring within (strictly less than) a given time duration
        /// </summary>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TPayload">Payload type</typeparam>
        /// <typeparam name="TResult">Result type</typeparam>
        /// <param name="stream">Input stream</param>
        /// <param name="firstMatch">First element in pattern</param>
        /// <param name="secondMatch">Second element in pattern</param>
        /// <param name="resultSelector">Compose result tuple using matching input events</param>
        /// <param name="withinDuration">Pattern occurs within (strictly less than) given time duration</param>
        /// <returns>Pattern result stream</returns>
        public static IStreamable<TKey, TResult> FollowedByImmediate<TKey, TPayload, TResult>(
            this IStreamable<TKey, TPayload> stream,
            Expression<Func<TPayload, bool>> firstMatch,
            Expression<Func<TPayload, bool>> secondMatch,
            Expression<Func<TPayload, TPayload, TResult>> resultSelector,
            long withinDuration)
        {
            if (withinDuration >= 0)
            {
                // Set match duration
                if (withinDuration < 2) throw new ArgumentException("Duration has to be at least 2 chronons");
                if (withinDuration > StreamEvent.MaxSyncTime) throw new ArgumentException("Duration is too large");
                stream = stream.AlterEventDuration(withinDuration - 1);
            }

            // Clip the stream with itself, to make it a signal stream
            var clippedStream = stream.Multicast(xs => xs.ClipEventDuration(xs));

            // Shift the left side (first match) by one chronon, set right side (second match) to one chronon duration, join to find matches
            var result = clippedStream.Multicast(xs => xs.Where(firstMatch).ShiftEventLifetime(1).Join(xs.Where(secondMatch).AlterEventDuration(1), resultSelector));

            return result;
        }

        /// <summary>
        /// Finds patterns of events that are repeated consecutively
        /// </summary>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TPayload">Payload type</typeparam>
        /// <typeparam name="TResult">Result type</typeparam>
        /// <param name="stream">Input stream</param>
        /// <param name="resultSelector">Compose result tuple using matching input events</param>
        /// <returns>Pattern result stream</returns>
        public static IStreamable<TKey, TResult> ConsecutivePairs<TKey, TPayload, TResult>(
            this IStreamable<TKey, TPayload> stream,
            Expression<Func<TPayload, TPayload, TResult>> resultSelector)
        {
            // Clip the stream with itself, to make it a signal stream
            var clippedStream = stream.AlterEventDuration(StreamEvent.InfinitySyncTime).Multicast(xs => xs.ClipEventDuration(xs));

            // Shift the left side (first match) by one chronon, set right side (second match) to one chronon duration, join to find pairs
            var result = clippedStream.Multicast(xs => xs.ShiftEventLifetime(1).Join(xs.AlterEventDuration(1), resultSelector));

            return result;
        }

        /// <summary>
        /// Finds patterns of A on the left followed immediately by B on the right (with no other intermediate events), occurring within (strictly less than) a given time duration
        /// </summary>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TPayload">Payload type for left input</typeparam>
        /// <typeparam name="TPayload2">Payload type for right input</typeparam>
        /// <typeparam name="TResult">Result type</typeparam>
        /// <param name="stream">Left input stream</param>
        /// <param name="stream2">Right input stream</param>
        /// <param name="firstMatch">First element in pattern</param>
        /// <param name="secondMatch">Second element in pattern</param>
        /// <param name="resultSelector">Compose result tuple using matching input events</param>
        /// <param name="withinDuration">Pattern occurs within (strictly less than) given time duration</param>
        /// <returns>Pattern result stream</returns>
        public static IStreamable<TKey, TResult> FollowedByImmediate<TKey, TPayload, TPayload2, TResult>(
            this IStreamable<TKey, TPayload> stream,
            IStreamable<TKey, TPayload2> stream2,
            Expression<Func<TPayload, bool>> firstMatch,
            Expression<Func<TPayload2, bool>> secondMatch,
            Expression<Func<TPayload, TPayload2, TResult>> resultSelector,
            long withinDuration)
        {
            if (withinDuration >= 0)
            {
                // Set match duration
                if (withinDuration < 2) throw new ArgumentException("Duration has to be at least 2 chronons");
                if (withinDuration > StreamEvent.MaxSyncTime) throw new ArgumentException("Duration is too large");
                stream = stream.AlterEventDuration(withinDuration - 1);
            }

            // Clip the stream with itself, to make it a signal stream
            var clippedStream = stream.ClipEventDuration(stream2);

            // Shift the left side (first match) by one chronon, set right side (second match) to one chronon duration, join to find matches
            var result = clippedStream.Multicast(xs => xs.Where(firstMatch).ShiftEventLifetime(1).Join(stream2.Where(secondMatch).AlterEventDuration(1), resultSelector));

            return result;
        }
    }
}
