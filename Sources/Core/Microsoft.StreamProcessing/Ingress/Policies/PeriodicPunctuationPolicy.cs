// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Diagnostics.Contracts;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal enum PeriodicPunctuationPolicyType
    {
        None = 0,
        Time = 1,
    }

    /// <summary>
    /// Specifies how to inject punctuations into the resulting stream.
    /// Since punctuations force output, this exposes a throughput/latency tradeoff.
    /// </summary>
    [DataContract]
    public sealed class PeriodicPunctuationPolicy
    {
        [DataMember]
        internal PeriodicPunctuationPolicyType type;
        [DataMember]
        internal ulong generationPeriod;

        /// <summary>
        /// Empty constructor for serialization purposes.
        /// </summary>
        public PeriodicPunctuationPolicy() { }

        internal PeriodicPunctuationPolicy(PeriodicPunctuationPolicyType type, ulong generationPeriod)
        {
            this.type = type;
            this.generationPeriod = generationPeriod;
        }

        /// <summary>
        /// Don't inject any punctuations.
        /// This is the default policy.
        /// </summary>
        /// <returns>An instance of the punctuation policy</returns>
        public static PeriodicPunctuationPolicy None()
            => new PeriodicPunctuationPolicy(PeriodicPunctuationPolicyType.None, 0);

        /// <summary>
        /// Inject punctuations every <paramref name="generationPeriod"/> time ticks, rounded down to the previous
        /// <paramref name="generationPeriod"/> multiple.
        /// </summary>
        /// <param name="generationPeriod">Frequency of punctuations in time ticks (n)</param>
        /// <returns>An instance of the punctuation policy</returns>
        public static PeriodicPunctuationPolicy Time(ulong generationPeriod)
        {
            Contract.Requires(generationPeriod > 0);
            return new PeriodicPunctuationPolicy(PeriodicPunctuationPolicyType.Time, generationPeriod);
        }

        /// <summary>
        /// Provides a hash code for the PeriodicPunctuationPolicy object.
        /// </summary>
        /// <returns>A hash code for the PeriodicPunctuationPolicy object.</returns>
        public override int GetHashCode() => this.type.GetHashCode() ^ this.generationPeriod.GetHashCode();

        /// <summary>
        /// Provides a string representation for the PeriodicPunctuationPolicy object.
        /// </summary>
        /// <returns>A string representation for the PeriodicPunctuationPolicy object.</returns>
        public override string ToString()
        {
            string kind;
            switch (this.type)
            {
                case PeriodicPunctuationPolicyType.None: kind = "None"; break;
                case PeriodicPunctuationPolicyType.Time: kind = "Time"; break;
                default: kind = "Unknown(" + this.type.ToString() + ")"; break;
            }
            return $"PeriodicPunctuationPolicy.{kind}({this.generationPeriod.ToString()})";
        }
    }
}