// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Reflection;

namespace Microsoft.StreamProcessing.Serializer
{
    /// <summary>
    ///     Provides the methods needed to substitute one type for another by the ISerializer
    ///     during serialization and deserialization of C# types.
    /// </summary>
    public interface ISurrogate
    {
        /// <summary>
        /// Returns whether type is supported by surrogate
        /// </summary>
        bool IsSupportedType(Type type, out MethodInfo serialize, out MethodInfo deserialize);
    }
}
