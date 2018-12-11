// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// A type representing the empty struct, similar to Unit in other libraries and languages.
    /// </summary>
    [DataContract]
    public struct Empty : IEquatable<Empty>
    {
        /// <summary>
        /// Determines whether the argument Empty value is equal to the receiver. Because Empty has a single value, this always returns true.
        /// </summary>
        /// <param name="other">An Empty value to compare to the current Empty value.</param>
        /// <returns>Because there is only one value of type Empty, this always returns true.</returns>
        public bool Equals(Empty other) => true;

        /// <summary>
        /// Determines whether the specified System.Object is equal to the current Empty.
        /// </summary>
        /// <param name="obj">The System.Object to compare with the current Empty.</param>
        /// <returns>true if the specified System.Object is a Empty value; otherwise, false.</returns>
        public override bool Equals(object obj) => obj is Empty;

        /// <summary>
        /// Returns the hash code for the Empty value.
        /// </summary>
        /// <returns>A hash code for the Empty value.</returns>
        public override int GetHashCode() => 0;

        /// <summary>
        /// Returns a string representation of the Empty value.
        /// </summary>
        /// <returns>String representation of the Empty value.</returns>
        public override string ToString() => "()";

        /// <summary>
        /// Determines whether the two specified Emtpy values are equal. Because Empty has a single value, this always returns true.
        /// </summary>
        /// <param name="first">The first Empty value to compare.</param>
        /// <param name="second">The second Empty value to compare.</param>
        /// <returns>Because Empty has a single value, this always returns true.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", MessageId = "first", Justification = "Parameter required for operator overloading."), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", MessageId = "second", Justification = "Parameter required for operator overloading.")]
        public static bool operator ==(Empty first, Empty second) => true;

        /// <summary>
        /// Determines whether the two specified Empty values are not equal. Because Empty has a single value, this always returns false.
        /// </summary>
        /// <param name="first">The first Empty value to compare.</param>
        /// <param name="second">The second Empty value to compare.</param>
        /// <returns>Because Empty has a single value, this always returns false.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", MessageId = "first", Justification = "Parameter required for operator overloading."), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", MessageId = "second", Justification = "Parameter required for operator overloading.")]
        public static bool operator !=(Empty first, Empty second) => false;

        /// <summary>
        /// The single Empty value.
        /// </summary>
        public static Empty Default => default;
    }
}
