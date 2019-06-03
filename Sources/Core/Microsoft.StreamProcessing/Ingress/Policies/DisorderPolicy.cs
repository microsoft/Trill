// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal enum DisorderPolicyType
    {
        Throw = 0,
        Adjust = 1,
        Drop = 2,
    }

    /// <summary>
    /// Specifies how to handle out-of-order events
    /// </summary>
    [DataContract]
    public sealed class DisorderPolicy
    {
        [DataMember]
        internal DisorderPolicyType type;
        [DataMember]
        internal long reorderLatency;

        /// <summary>
        /// Empty constructor required for Data Contract serializability.
        /// </summary>
        public DisorderPolicy() { }

        internal DisorderPolicy(DisorderPolicyType type)
        {
            this.type = type;
            this.reorderLatency = 0;
        }

        internal DisorderPolicy(DisorderPolicyType type, long reorderLatency)
        {
            this.type = type;
            this.reorderLatency = reorderLatency;
        }

        /// <summary>
        /// Throw if you see disordered events. Use this when you know that your input
        /// events are well-ordered.
        /// </summary>
        /// <returns>An instance of the disorder policy</returns>
        public static DisorderPolicy Throw() => new DisorderPolicy(DisorderPolicyType.Throw);

        /// <summary>
        /// Throw if you see disordered events. Use this when you know that your input
        /// events are well-ordered.
        /// </summary>
        /// <param name="reorderLatency">Tolerable latency bound (in application time) for reordering data at ingress</param>
        /// <returns>An instance of the disorder policy</returns>
        public static DisorderPolicy Throw(long reorderLatency) => new DisorderPolicy(DisorderPolicyType.Throw, reorderLatency);

        /// <summary>
        /// When an out-of-order event appears in the stream, adjust its
        /// start time to be the start time of immediately previous event.
        /// </summary>
        /// <returns>An instance of the disorder policy</returns>
        public static DisorderPolicy Adjust() => new DisorderPolicy(DisorderPolicyType.Adjust);

        /// <summary>
        /// When an out-of-order event appears in the stream, adjust its
        /// start time to be the start time of immediately previous event.
        /// </summary>
        /// <param name="reorderLatency">Tolerable latency bound (in application time) for reordering data at ingress</param>
        /// <returns>An instance of the disorder policy</returns>
        public static DisorderPolicy Adjust(long reorderLatency) => new DisorderPolicy(DisorderPolicyType.Adjust, reorderLatency);

        /// <summary>
        /// When an out-of-order event appears in the stream,
        /// drop it and don't include it in the output.
        /// </summary>
        /// <returns>An instance of the disorder policy</returns>
        public static DisorderPolicy Drop() => new DisorderPolicy(DisorderPolicyType.Drop);

        /// <summary>
        /// When an out-of-order event appears in the stream,
        /// drop it and don't include it in the output.
        /// </summary>
        /// <param name="reorderLatency">Tolerable latency bound (in application time) for reordering data at ingress</param>
        /// <returns>An instance of the disorder policy</returns>
        public static DisorderPolicy Drop(long reorderLatency) => new DisorderPolicy(DisorderPolicyType.Drop, reorderLatency);

        /// <summary>
        /// Determines whether two Disorder Policies have the same constituent parts.
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
            => (obj is DisorderPolicy d) && this.type == d.type && this.reorderLatency == d.reorderLatency;

        /// <summary>
        /// Provides a hash code for the Disorder Policy object.
        /// </summary>
        /// <returns>A hash code for the Disorder Policy object.</returns>
        public override int GetHashCode() => this.type.GetHashCode() ^ (this.reorderLatency.GetHashCode() << 2);

        /// <summary>
        /// Do *not* include the value for the reorder latency so that the string can be used as a key for the
        /// dictionary containing the generated pipes.
        /// </summary>
        public override string ToString()
        {
            switch (this.type)
            {
                case DisorderPolicyType.Adjust: return "DisorderPolicy.Adjust(" + this.reorderLatency.ToString() + ")";
                case DisorderPolicyType.Drop: return "DisorderPolicy.Drop(" + this.reorderLatency.ToString() + ")";
                case DisorderPolicyType.Throw: return "DisorderPolicy.Throw(" + this.reorderLatency.ToString() + ")";
                default: return "Unknown disorder policy: " + this.type;
            }
        }
    }
}