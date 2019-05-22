// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Exception specific to an error occurring during data ingress.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2237:MarkISerializableTypesWithSerializable", Justification = "SerializableAttribute not yet supported by .Net Core")]
    public sealed class IngressException : Exception
    {
        /// <summary>
        /// Create an Ingress exception with no underlying message.
        /// </summary>
        public IngressException() { }

        /// <summary>
        /// Create an Ingress exception with the given message.
        /// </summary>
        /// <param name="message">The error message associated with the exception.</param>
        public IngressException(string message) : base(message) { }

        /// <summary>
        /// Create an Ingress exception with the given message.
        /// </summary>
        /// <param name="message">The error message associated with the exception.</param>
        /// <param name="innerException">The additional exception being bundled with this exception.</param>
        public IngressException(string message, Exception innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// Represents a user exception somewhere within the
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2237:MarkISerializableTypesWithSerializable", Justification = "SerializableAttribute not yet supported by .Net Core")]
    public sealed class StreamProcessingException : Exception
    {
        /// <summary>
        /// Create a Stream Processing exception with no underlying message.
        /// </summary>
        public StreamProcessingException() : base() { }

        /// <summary>
        /// Create a Stream Processing exception with the given message.
        /// </summary>
        /// <param name="message">The error message associated with the exception.</param>
        public StreamProcessingException(string message) : base(message) { }

        /// <summary>
        /// Create a Stream Processing exception with the given message and given inner exception.
        /// </summary>
        /// <param name="message">The error message associated with the exception.</param>
        /// <param name="innerException">The additional exception being bundled with this exception.</param>
        public StreamProcessingException(string message, Exception innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// Exception when Trill detects an out of order event outside of ingress
    /// </summary>
    public sealed class StreamProcessingOutOfOrderException : Exception
    {
        /// <summary>
        /// Create ann out-of-order exception with no underlying message.
        /// </summary>
        public StreamProcessingOutOfOrderException() : base() { }

        /// <summary>
        /// Create an out-of-order exception with the given message.
        /// </summary>
        /// <param name="message">The error message associated with the exception.</param>
        public StreamProcessingOutOfOrderException(string message) : base(message) { }

        /// <summary>
        /// Create an out-of-order exception with the given message and given inner exception.
        /// </summary>
        /// <param name="message">The error message associated with the exception.</param>
        /// <param name="innerException">The additional exception being bundled with this exception.</param>
        public StreamProcessingOutOfOrderException(string message, Exception innerException) : base(message, innerException) { }
    }
}
