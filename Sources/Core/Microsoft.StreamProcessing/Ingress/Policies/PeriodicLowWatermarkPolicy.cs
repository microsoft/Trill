// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal enum PeriodicLowWatermarkPolicyType
    {
        None = 0,
        Time = 1,
    }

    /// <summary>
    /// Specifies how to generate low watermarks into the partitioned stream.
    /// Low watermarks set a lower bound on all partitions in the stream, and any event ingressed
    /// before that lower bound will be considered out of order.
    /// </summary>
    [DataContract]
    public sealed class PeriodicLowWatermarkPolicy
    {
        [DataMember]
        internal PeriodicLowWatermarkPolicyType type = PeriodicLowWatermarkPolicyType.None;
        [DataMember]
        internal ulong generationPeriod = 0;
        [DataMember]
        internal long lowWatermarkTimestampLag = 0;

        /// <summary>
        /// Empty constructor for serialization purposes.
        /// </summary>
        public PeriodicLowWatermarkPolicy() { }

        internal PeriodicLowWatermarkPolicy(PeriodicLowWatermarkPolicyType type, ulong generationPeriod, long lowWatermarkTimestampLag)
        {
            this.type = type;
            this.generationPeriod = generationPeriod;
            this.lowWatermarkTimestampLag = lowWatermarkTimestampLag;
        }

        /// <summary>
        /// Don't inject any low watermarks. This is the default policy.
        /// </summary>
        /// <returns>An instance of PeriodicLowWatermarkPolicy</returns>
        public static PeriodicLowWatermarkPolicy None() => new PeriodicLowWatermarkPolicy();

        /// <summary>
        /// Inject low watermarks every <paramref name="generationPeriod"/> time ticks, rounded down to the previous
        /// <paramref name="generationPeriod"/> multiple, based on the highest event time ingressed into the stream.
        /// Low watermark timestamp is calculated as (highest event time - <paramref name="lowWatermarkTimestampLag"/>).
        /// Highest ingressed event time is calculated before any buffering such as specified in other policies, <see cref="DisorderPolicy"/>.
        /// </summary>
        /// <param name="generationPeriod">Period of time ticks that must lapse since last low watermark timestamp before
        /// generating a new low watermark</param>
        /// <param name="lowWatermarkTimestampLag">The amount of time that the generated low watermarks will lag behind
        /// the highest event time in the stream.
        /// Low watermark timestamp = (highest event time - <paramref name="lowWatermarkTimestampLag"/>), rounded down
        /// to the previous <paramref name="generationPeriod"/> multiple</param>
        /// <returns>An instance of PeriodicLowWatermarkPolicy</returns>
        public static PeriodicLowWatermarkPolicy Time(ulong generationPeriod, ulong lowWatermarkTimestampLag)
        {
            Contract.Requires(lowWatermarkTimestampLag <= long.MaxValue);

            if (lowWatermarkTimestampLag > long.MaxValue)
            {
                throw new ArgumentOutOfRangeException(nameof(lowWatermarkTimestampLag));
            }

            return new PeriodicLowWatermarkPolicy(PeriodicLowWatermarkPolicyType.Time, generationPeriod, (long)lowWatermarkTimestampLag);
        }

        /// <summary>
        /// Provides a hash code for the PeriodicLowWatermarkPolicy object.
        /// </summary>
        /// <returns>A hash code for the PeriodicLowWatermarkPolicy object.</returns>
        public override int GetHashCode()
            => this.type.GetHashCode() ^ this.generationPeriod.GetHashCode() ^ this.lowWatermarkTimestampLag.GetHashCode();

        /// <summary>
        /// Provides a string representation for the PeriodicLowWatermarkPolicy object.
        /// </summary>
        /// <returns>A string representation for the PeriodicLowWatermarkPolicy object.</returns>
        public override string ToString()
        {
            switch (this.type)
            {
                case PeriodicLowWatermarkPolicyType.None:
                    return "PeriodicLowWatermarkPolicy.None";
                case PeriodicLowWatermarkPolicyType.Time:
                    return $"PeriodicLowWatermarkPolicy.Time({this.generationPeriod}, {this.lowWatermarkTimestampLag})";
                default:
                    return "Unknown PeriodicLowWatermarkPolicy (" + this.type.ToString() + ")";
            }
        }
    }
}