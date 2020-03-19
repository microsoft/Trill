// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing
{

    internal sealed class StreamEventSyncTimeComparer<TPayload> : IComparer<StreamEvent<TPayload>>
    {
        public int Compare(StreamEvent<TPayload> x, StreamEvent<TPayload> y)
            => x.SyncTime.CompareTo(y.SyncTime);
    }

    /// <summary>
    /// Represents an unpartitioned Stream event
    /// </summary>
    /// <typeparam name="TPayload">Type of payload for the event</typeparam>
    [DataContract]
    public struct StreamEvent<TPayload>
    {

        /// <summary>
        /// Start-time for the event
        /// For a start edge, sync-time is the start time of the event (other-time is set to StreamEvent.InfinitySyncTime)
        /// For an end edge, sync-time is the end-time and other-time is the original start time for the event
        /// For an interval event, sync-time and other-time refer to the start and end times for the event
        /// For a punctuation, sync-time is set to the timetamp of the punctuation, while other-time is set to a negative value
        /// </summary>
        [DataMember]
        internal long SyncTime;

        /// <summary>
        /// End-time for the event
        /// For a start edge, sync-time is the start time of the event (other-time is set to StreamEvent.InfinitySyncTime)
        /// For an end edge, sync-time is the end-time and other-time is the original start time for the event
        /// For an interval event, sync-time and other-time refer to the start and end times for the event
        /// For a punctuation, sync-time is set to the timetamp of the punctuation, while other-time is set to a negative value
        /// </summary>
        [DataMember]
        internal long OtherTime;

        /// <summary>
        /// Payload of the event
        /// </summary>
        [DataMember]
        public TPayload Payload;

        /// <summary>
        /// Kind of the event
        /// </summary>
        public StreamEventKind Kind
        {
            get
            {
                if (this.OtherTime == StreamEvent.InfinitySyncTime)
                {
                    return StreamEventKind.Start;
                }
                else if (this.OtherTime == StreamEvent.PunctuationOtherTime)
                {
                    return StreamEventKind.Punctuation;
                }
                else if (this.OtherTime == PartitionedStreamEvent.LowWatermarkOtherTime)
                {
                    return StreamEventKind.LowWatermark;
                }
                else if (this.SyncTime < this.OtherTime)
                {
                    return StreamEventKind.Interval;
                }
                else
                {
                    return StreamEventKind.End;
                }
            }
        }

        /// <summary>
        /// Check if the event is a punctuation
        /// </summary>
        public bool IsPunctuation => this.OtherTime == StreamEvent.PunctuationOtherTime;

        /// <summary>
        /// Check if the event is data (start edge, end edge, or interval)
        /// </summary>
        public bool IsData => this.OtherTime >= 0;

        /// <summary>
        /// Check is the event is a start edge
        /// </summary>
        public bool IsStart => this.OtherTime == StreamEvent.InfinitySyncTime;

        /// <summary>
        /// Check if the event is an end-edge
        /// </summary>
        public bool IsEnd => this.OtherTime >= 0 && this.SyncTime > this.OtherTime;

        /// <summary>
        /// Check if the event is an interval event
        /// </summary>
        public bool IsInterval => this.OtherTime != StreamEvent.InfinitySyncTime && this.SyncTime < this.OtherTime;

        /// <summary>
        /// Check if the event is a point event (an interval event of length one)
        /// </summary>
        public bool IsPoint => this.OtherTime != StreamEvent.InfinitySyncTime && (this.OtherTime == this.SyncTime + 1);

        /// <summary>
        /// Get the start time of the logical event interval for this event
        /// </summary>
        public long StartTime => this.IsEnd ? this.OtherTime : this.SyncTime;

        /// <summary>
        /// Get the end time of the logical event interval for this event
        /// </summary>
        public long EndTime => this.IsEnd ? this.SyncTime : this.OtherTime;

        /// <summary>
        /// Creates a new stream event with the given temporal parameters.
        /// </summary>
        /// <param name="syncTime">The sync time for this event. This value corresponds to the start time for a start edge or interval and the end time for an end edge.</param>
        /// <param name="otherTime">The other associated time for this events. For intervals, this value is the end time. For an end edge, this value identifies when the value started.</param>
        /// <param name="payload">The actual event associated with these temporal parameters.</param>
        public StreamEvent(long syncTime, long otherTime, TPayload payload)
        {
            this.SyncTime = syncTime;
            this.OtherTime = otherTime;
            this.Payload = payload;
        }

        /// <summary>
        /// Return a string version of the event
        /// </summary>
        /// <returns>A string representing the event for display</returns>
        public override string ToString()
        {
            switch (this.Kind)
            {
                case StreamEventKind.Start:
                    return $"[{this.Kind}: {TimeAsString(this.SyncTime)},{this.Payload}]";
                case StreamEventKind.End:
                    return $"[{this.Kind}: {TimeAsString(this.SyncTime)},{TimeAsString(this.OtherTime)},{this.Payload}]";
                case StreamEventKind.Interval:
                    return $"[{this.Kind}: {TimeAsString(this.SyncTime)}-{TimeAsString(this.OtherTime)},{this.Payload}]";
                case StreamEventKind.Punctuation:
                    return $"[{this.Kind}: {TimeAsString(this.SyncTime)}]";
                case StreamEventKind.LowWatermark:
                    return $"[{this.Kind}: {TimeAsString(this.SyncTime)}]";
            }
            return string.Empty;
        }

        private static string TimeAsString(long t)
        {
            if (t == StreamEvent.InfinitySyncTime) return "+inf";
            if (t == StreamEvent.MinSyncTime) return "min";
            if (t == StreamEvent.MaxSyncTime) return "max";
            return t.ToString(CultureInfo.InvariantCulture);
        }

    }


    internal sealed class PartitionedStreamEventSyncTimeComparer<TKey, TPayload> : IComparer<PartitionedStreamEvent<TKey, TPayload>>
    {
        public int Compare(PartitionedStreamEvent<TKey, TPayload> x, PartitionedStreamEvent<TKey, TPayload> y)
            => x.SyncTime.CompareTo(y.SyncTime);
    }

    /// <summary>
    /// Represents a partitioned Stream event
    /// </summary>
    /// <typeparam name="TKey">Type of payload for the event</typeparam>
    /// <typeparam name="TPayload">Type of payload for the event</typeparam>
    [DataContract]
    public struct PartitionedStreamEvent<TKey, TPayload>
    {
        /// <summary>
        /// Partition key for the event
        /// </summary>
        [DataMember]
        public TKey PartitionKey;

        /// <summary>
        /// Start-time for the event
        /// For a start edge, sync-time is the start time of the event (other-time is set to StreamEvent.InfinitySyncTime)
        /// For an end edge, sync-time is the end-time and other-time is the original start time for the event
        /// For an interval event, sync-time and other-time refer to the start and end times for the event
        /// For a punctuation, sync-time is set to the timetamp of the punctuation, while other-time is set to a negative value
        /// </summary>
        [DataMember]
        internal long SyncTime;

        /// <summary>
        /// End-time for the event
        /// For a start edge, sync-time is the start time of the event (other-time is set to StreamEvent.InfinitySyncTime)
        /// For an end edge, sync-time is the end-time and other-time is the original start time for the event
        /// For an interval event, sync-time and other-time refer to the start and end times for the event
        /// For a punctuation, sync-time is set to the timetamp of the punctuation, while other-time is set to a negative value
        /// </summary>
        [DataMember]
        internal long OtherTime;

        /// <summary>
        /// Payload of the event
        /// </summary>
        [DataMember]
        public TPayload Payload;

        /// <summary>
        /// Kind of the event
        /// </summary>
        public StreamEventKind Kind
        {
            get
            {
                if (this.OtherTime == StreamEvent.InfinitySyncTime)
                {
                    return StreamEventKind.Start;
                }
                else if (this.OtherTime == StreamEvent.PunctuationOtherTime)
                {
                    return StreamEventKind.Punctuation;
                }
                else if (this.OtherTime == PartitionedStreamEvent.LowWatermarkOtherTime)
                {
                    return StreamEventKind.LowWatermark;
                }
                else if (this.SyncTime < this.OtherTime)
                {
                    return StreamEventKind.Interval;
                }
                else
                {
                    return StreamEventKind.End;
                }
            }
        }

        /// <summary>
        /// Check if the event is a punctuation
        /// </summary>
        public bool IsPunctuation => this.OtherTime == StreamEvent.PunctuationOtherTime;

        /// <summary>
        /// Check if the event is a punctuation
        /// </summary>
        public bool IsLowWatermark => this.OtherTime == PartitionedStreamEvent.LowWatermarkOtherTime;

        /// <summary>
        /// Check if the event is data (start edge, end edge, or interval)
        /// </summary>
        public bool IsData => this.OtherTime >= 0;

        /// <summary>
        /// Check is the event is a start edge
        /// </summary>
        public bool IsStart => this.OtherTime == StreamEvent.InfinitySyncTime;

        /// <summary>
        /// Check if the event is an end-edge
        /// </summary>
        public bool IsEnd => this.OtherTime >= 0 && this.SyncTime > this.OtherTime;

        /// <summary>
        /// Check if the event is an interval event
        /// </summary>
        public bool IsInterval => this.OtherTime != StreamEvent.InfinitySyncTime && this.SyncTime < this.OtherTime;

        /// <summary>
        /// Check if the event is a point event (an interval event of length one)
        /// </summary>
        public bool IsPoint => this.OtherTime != StreamEvent.InfinitySyncTime && (this.OtherTime == this.SyncTime + 1);

        /// <summary>
        /// Get the start time of the logical event interval for this event
        /// </summary>
        public long StartTime => this.IsEnd ? this.OtherTime : this.SyncTime;

        /// <summary>
        /// Get the end time of the logical event interval for this event
        /// </summary>
        public long EndTime => this.IsEnd ? this.SyncTime : this.OtherTime;

        /// <summary>
        /// Creates a new stream event with the given temporal parameters.
        /// </summary>
        /// <param name="key">The partition key value to which this event belongs.</param>
        /// <param name="syncTime">The sync time for this event. This value corresponds to the start time for a start edge or interval and the end time for an end edge.</param>
        /// <param name="otherTime">The other associated time for this events. For intervals, this value is the end time. For an end edge, this value identifies when the value started.</param>
        /// <param name="payload">The actual event associated with these temporal parameters.</param>
        public PartitionedStreamEvent(TKey key, long syncTime, long otherTime, TPayload payload)
        {
            this.PartitionKey = key;
            this.SyncTime = syncTime;
            this.OtherTime = otherTime;
            this.Payload = payload;
        }

        /// <summary>
        /// Return a string version of the event
        /// </summary>
        /// <returns>A string representing the event for display</returns>
        public override string ToString()
        {
            switch (this.Kind)
            {
                case StreamEventKind.Start:
                    return $"({this.PartitionKey})[{this.Kind}: {TimeAsString(this.SyncTime)},{this.Payload}]";
                case StreamEventKind.End:
                    return $"({this.PartitionKey})[{this.Kind}: {TimeAsString(this.SyncTime)},{TimeAsString(this.OtherTime)},{this.Payload}]";
                case StreamEventKind.Interval:
                    return $"({this.PartitionKey})[{this.Kind}: {TimeAsString(this.SyncTime)}-{TimeAsString(this.OtherTime)},{this.Payload}]";
                case StreamEventKind.Punctuation:
                    return $"({this.PartitionKey})[{this.Kind}: {TimeAsString(this.SyncTime)}]";
                case StreamEventKind.LowWatermark:
                    return $"[{this.Kind}: {TimeAsString(this.SyncTime)}]";
            }
            return string.Empty;
        }

        private static string TimeAsString(long t)
        {
            if (t == StreamEvent.InfinitySyncTime) return "+inf";
            if (t == StreamEvent.MinSyncTime) return "min";
            if (t == StreamEvent.MaxSyncTime) return "max";
            return t.ToString(CultureInfo.InvariantCulture);
        }

    }

}
