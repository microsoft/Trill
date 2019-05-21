// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal enum TimelineEnum
    {
        /// <summary>
        /// Indicates that the assigned timeline should be derived from the CPU's notion of time
        /// </summary>
        WallClock,

        /// <summary>
        /// Indicates that the assigned timeline should be derived by a monotonically increasing counter
        /// </summary>
        Sequence,
    }

    /// <summary>
    /// Specifies how to assign a monotonically increasing temporal component to intrinsically atemporal data.
    /// </summary>
    [DataContract]
    public class TimelinePolicy
    {
        [DataMember]
        internal TimelineEnum timelineEnum;
        [DataMember]
        internal TimeSpan punctuationInterval;
        [DataMember]
        internal int sampleSize;

        /// <summary>
        /// Empty constructor for serialization purposes.
        /// </summary>
        public TimelinePolicy() { }

        internal TimelinePolicy(TimelineEnum timelineEnum)
        {
            this.timelineEnum = timelineEnum;
        }

        /// <summary>
        /// Create a new Timeline Policy based on assigning temporal values to events based on the time in the system clock.
        /// </summary>
        /// <param name="punctuationInterval">Describes how much time should progress before creating a punctuation event and flushing contents.</param>
        /// <returns>A policy object to be used in ingress methods.</returns>
        public static TimelinePolicy WallClock(TimeSpan punctuationInterval = default)
            => new TimelinePolicy(TimelineEnum.WallClock) { punctuationInterval = punctuationInterval };

        /// <summary>
        /// Create a new Timeline Policy based on assigning temporal values to events based on a monotonically increasing counter.
        /// </summary>
        /// <param name="sampleSize">Describes how many events will be given the same sequence value before incrementing it.</param>
        /// <returns>A policy object to be used in ingress methods.</returns>
        public static TimelinePolicy Sequence(int sampleSize)
        {
            if (sampleSize <= 0) throw new ArgumentException("Sample size must be greater than zero.");
            return new TimelinePolicy(TimelineEnum.Sequence) { sampleSize = sampleSize };
        }
    }
}