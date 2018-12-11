// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Stream event helpers that do not depend on generic arguments.
    /// </summary>
    public static class StreamEvent
    {
        /// <summary>
        /// Sentinel value for punctuation other time
        /// </summary>
        public const long PunctuationOtherTime = long.MinValue;

        /// <summary>
        /// The time value associated with infinity, or a value beyond all other possible concrete time values.
        /// </summary>
        public static readonly long InfinitySyncTime = DateTimeOffset.MaxValue.UtcTicks;

        /// <summary>
        /// The minimum possible time value processed by the system.
        /// </summary>
        public static readonly long MinSyncTime = DateTimeOffset.MinValue.UtcTicks;

        /// <summary>
        /// The maximum possible time value processed by the system.
        /// </summary>
        public static readonly long MaxSyncTime = InfinitySyncTime - 1;

        /// <summary>
        /// Create a punctuation.
        /// </summary>
        /// <param name="punctuationTime">Timestamp of punctuation</param>
        /// <returns>An event instance</returns>
        public static StreamEvent<TPayload> CreatePunctuation<TPayload>(long punctuationTime)
        {
            Contract.Requires(punctuationTime >= MinSyncTime && punctuationTime <= InfinitySyncTime);

            return new StreamEvent<TPayload>(punctuationTime, PunctuationOtherTime, default);
        }

        /// <summary>
        /// Create a start-edge event.
        /// </summary>
        /// <param name="startTime">Timestamp of the event</param>
        /// <param name="payload">Payload of the event</param>
        /// <returns>An event instance</returns>
        public static StreamEvent<TPayload> CreateStart<TPayload>(long startTime, TPayload payload)
        {
            Contract.Requires(startTime >= MinSyncTime && startTime < InfinitySyncTime);

            return new StreamEvent<TPayload>(startTime, InfinitySyncTime, payload);
        }

        /// <summary>
        /// Create an end-edge event.
        /// </summary>
        /// <param name="endTime">End timestamp of the event</param>
        /// <param name="originalStartTime">Original start timestamp of the event</param>
        /// <param name="payload">Payload of the event</param>
        /// <returns>An event instance</returns>
        public static StreamEvent<TPayload> CreateEnd<TPayload>(long endTime, long originalStartTime, TPayload payload)
        {
            Contract.Requires(originalStartTime >= MinSyncTime && originalStartTime < InfinitySyncTime);
            Contract.Requires(endTime >= MinSyncTime && endTime < InfinitySyncTime);
            Contract.Requires(endTime > originalStartTime);

            return new StreamEvent<TPayload>(endTime, originalStartTime, payload);
        }

        /// <summary>
        /// Create an interval event.
        /// </summary>
        /// <param name="startTime">Start timestamp of the event</param>
        /// <param name="endTime">End timestamp of the event</param>
        /// <param name="payload">Payload of the event</param>
        /// <returns>An event instance</returns>
        public static StreamEvent<TPayload> CreateInterval<TPayload>(long startTime, long endTime, TPayload payload)
        {
            Contract.Requires(startTime >= MinSyncTime && startTime < InfinitySyncTime);
            Contract.Requires(endTime >= MinSyncTime && endTime < InfinitySyncTime);
            Contract.Requires(startTime < endTime);

            return new StreamEvent<TPayload>(startTime, endTime, payload);
        }

        /// <summary>
        /// Create a point event (an interval event of length one).
        /// </summary>
        /// <param name="startTime">Timestamp of the point event</param>
        /// <param name="payload">Payload of the event</param>
        /// <returns>An event instance</returns>
        public static StreamEvent<TPayload> CreatePoint<TPayload>(long startTime, TPayload payload)
        {
            Contract.Requires(startTime >= MinSyncTime && startTime < InfinitySyncTime);
            Contract.Requires((startTime + 1) >= MinSyncTime && (startTime + 1) < InfinitySyncTime);

            return new StreamEvent<TPayload>(startTime, startTime + 1, payload);
        }

        /// <summary>
        /// Create a punctuation, given an existing stream event as a template for the type of the event payload.
        /// </summary>
        /// <param name="existingEvent">An existing event from which to draw the type information for a new punctuation.</param>
        /// <param name="punctuationTime">Timestamp of punctuation</param>
        /// <returns>An event instance</returns>
        public static StreamEvent<TPayload> CreatePunctuation<TPayload>(this StreamEvent<TPayload> existingEvent, long punctuationTime)
        {
            Contract.Requires(punctuationTime >= MinSyncTime && punctuationTime <= InfinitySyncTime);

            return new StreamEvent<TPayload>(punctuationTime, PunctuationOtherTime, default);
        }
    }

    /// <summary>
    /// Partitioned stream event helpers that do not depend on generic arguments.
    /// </summary>
    public static class PartitionedStreamEvent
    {
        /// <summary>
        /// Sentinel value for punctuation other time
        /// </summary>
        public const long PunctuationOtherTime = long.MinValue;

        /// <summary>
        /// Sentinel value for low watermark other time
        /// </summary>
        public const long LowWatermarkOtherTime = -1;

        /// <summary>
        /// The time value associated with infinity, or a value beyond all other possible concrete time values.
        /// </summary>
        public static readonly long InfinitySyncTime = StreamEvent.InfinitySyncTime;

        /// <summary>
        /// The minimum possible time value processed by the system.
        /// </summary>
        public static readonly long MinSyncTime = StreamEvent.MinSyncTime;

        /// <summary>
        /// The maximum possible time value processed by the system.
        /// </summary>
        public static readonly long MaxSyncTime = StreamEvent.MaxSyncTime;

        /// <summary>
        /// Create a punctuation.
        /// </summary>
        /// <param name="key">The partition to which the event belongs</param>
        /// <param name="punctuationTime">Timestamp of punctuation</param>
        /// <typeparam name="TKey">The type of the partition key for this event.</typeparam>
        /// <typeparam name="TPayload">The type of the underlying event.</typeparam>
        /// <returns>An event instance</returns>
        public static PartitionedStreamEvent<TKey, TPayload> CreatePunctuation<TKey, TPayload>(TKey key, long punctuationTime)
        {
            Contract.Requires(punctuationTime >= MinSyncTime && punctuationTime <= InfinitySyncTime);

            return new PartitionedStreamEvent<TKey, TPayload>(key, punctuationTime, PunctuationOtherTime, default);
        }

        /// <summary>
        /// Create a low watermark.
        /// </summary>
        /// <param name="lowWatermarkTime">Timestamp of low watermark</param>
        /// <typeparam name="TKey">The type of the partition key for this stream.</typeparam>
        /// <typeparam name="TPayload">The type of the stream payload.</typeparam>
        /// <returns>An event instance</returns>
        public static PartitionedStreamEvent<TKey, TPayload> CreateLowWatermark<TKey, TPayload>(long lowWatermarkTime)
        {
            Contract.Requires(lowWatermarkTime >= MinSyncTime && lowWatermarkTime <= InfinitySyncTime);

            return new PartitionedStreamEvent<TKey, TPayload>(default, lowWatermarkTime, LowWatermarkOtherTime, default);
        }

        /// <summary>
        /// Create a start-edge event.
        /// </summary>
        /// <param name="key">The partition to which the event belongs</param>
        /// <param name="startTime">Timestamp of the event</param>
        /// <param name="payload">Payload of the event</param>
        /// <typeparam name="TKey">The type of the partition key for this event.</typeparam>
        /// <typeparam name="TPayload">The type of the underlying event.</typeparam>
        /// <returns>An event instance</returns>
        public static PartitionedStreamEvent<TKey, TPayload> CreateStart<TKey, TPayload>(TKey key, long startTime, TPayload payload)
        {
            Contract.Requires(startTime >= MinSyncTime && startTime < InfinitySyncTime);

            return new PartitionedStreamEvent<TKey, TPayload>(key, startTime, InfinitySyncTime, payload);
        }

        /// <summary>
        /// Create an end-edge event.
        /// </summary>
        /// <param name="key">The partition to which the event belongs</param>
        /// <param name="endTime">End timestamp of the event</param>
        /// <param name="originalStartTime">Original start timestamp of the event</param>
        /// <param name="payload">Payload of the event</param>
        /// <typeparam name="TKey">The type of the partition key for this event.</typeparam>
        /// <typeparam name="TPayload">The type of the underlying event.</typeparam>
        /// <returns>An event instance</returns>
        public static PartitionedStreamEvent<TKey, TPayload> CreateEnd<TKey, TPayload>(TKey key, long endTime, long originalStartTime, TPayload payload)
        {
            Contract.Requires(originalStartTime >= MinSyncTime && originalStartTime < InfinitySyncTime);
            Contract.Requires(endTime >= MinSyncTime && endTime < InfinitySyncTime);
            Contract.Requires(endTime > originalStartTime);

            return new PartitionedStreamEvent<TKey, TPayload>(key, endTime, originalStartTime, payload);
        }

        /// <summary>
        /// Create an interval event.
        /// </summary>
        /// <param name="key">The partition to which the event belongs</param>
        /// <param name="startTime">Start timestamp of the event</param>
        /// <param name="endTime">End timestamp of the event</param>
        /// <param name="payload">Payload of the event</param>
        /// <typeparam name="TKey">The type of the partition key for this event.</typeparam>
        /// <typeparam name="TPayload">The type of the underlying event.</typeparam>
        /// <returns>An event instance</returns>
        public static PartitionedStreamEvent<TKey, TPayload> CreateInterval<TKey, TPayload>(TKey key, long startTime, long endTime, TPayload payload)
        {
            Contract.Requires(startTime >= MinSyncTime && startTime < InfinitySyncTime);
            Contract.Requires(endTime >= MinSyncTime && endTime < InfinitySyncTime);
            Contract.Requires(startTime < endTime);

            return new PartitionedStreamEvent<TKey, TPayload>(key, startTime, endTime, payload);
        }

        /// <summary>
        /// Create a point event (an interval event of length one).
        /// </summary>
        /// <param name="key">The partition to which the event belongs</param>
        /// <param name="startTime">Timestamp of the point event</param>
        /// <param name="payload">Payload of the event</param>
        /// <typeparam name="TKey">The type of the partition key for this event.</typeparam>
        /// <typeparam name="TPayload">The type of the underlying event.</typeparam>
        /// <returns>An event instance</returns>
        public static PartitionedStreamEvent<TKey, TPayload> CreatePoint<TKey, TPayload>(TKey key, long startTime, TPayload payload)
        {
            Contract.Requires(startTime >= MinSyncTime && startTime < InfinitySyncTime);
            Contract.Requires((startTime + 1) >= MinSyncTime && (startTime + 1) < InfinitySyncTime);

            return new PartitionedStreamEvent<TKey, TPayload>(key, startTime, startTime + 1, payload);
        }

        /// <summary>
        /// Create a punctuation, given an existing stream event as a template for the type of the event payload and the value of the partition key.
        /// </summary>
        /// <param name="existingEvent">An existing event from which to draw the type information for a new punctuation.</param>
        /// <param name="punctuationTime">Timestamp of punctuation</param>
        /// <typeparam name="TKey">The type of the partition key for this event.</typeparam>
        /// <typeparam name="TPayload">The type of the underlying event.</typeparam>
        /// <returns>An event instance</returns>
        public static PartitionedStreamEvent<TKey, TPayload> CreatePunctuation<TKey, TPayload>(this PartitionedStreamEvent<TKey, TPayload> existingEvent, long punctuationTime)
        {
            Contract.Requires(punctuationTime >= MinSyncTime && punctuationTime <= InfinitySyncTime);

            return new PartitionedStreamEvent<TKey, TPayload>(existingEvent.PartitionKey, punctuationTime, PunctuationOtherTime, default);
        }

        /// <summary>
        /// Create a low watermark, given an existing stream event as a template for the types of the event payload and partition key.
        /// </summary>
        /// <param name="existingEvent">An existing event from which to draw the type information for a new low watermark.</param>
        /// <param name="lowWatermarkTime">Timestamp of low watermark</param>
        /// <typeparam name="TKey">The type of the partition key for this event.</typeparam>
        /// <typeparam name="TPayload">The type of the underlying event.</typeparam>
        /// <returns>An event instance</returns>
        public static PartitionedStreamEvent<TKey, TPayload> CreateLowWatermark<TKey, TPayload>(this PartitionedStreamEvent<TKey, TPayload> existingEvent, long lowWatermarkTime)
        {
            Contract.Requires(lowWatermarkTime >= MinSyncTime && lowWatermarkTime <= InfinitySyncTime);

            return new PartitionedStreamEvent<TKey, TPayload>(default, lowWatermarkTime, LowWatermarkOtherTime, default);
        }
    }
}
