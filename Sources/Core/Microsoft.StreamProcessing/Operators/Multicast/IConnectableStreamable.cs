// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
namespace Microsoft.StreamProcessing
{
    using System;

    /// <summary>
    /// Interface for dynamically connecting new streamable outputs to an existing stream
    /// </summary>
    /// <typeparam name="TKey">Grouping key type for data flowing through the system</typeparam>
    /// <typeparam name="TPayload">Event payload type for data flowing through the system</typeparam>
    public interface IConnectableStreamable<TKey, TPayload> : IStreamable<TKey, TPayload>
    {
        /// <summary>
        /// Connect establishes upstream stream processing.
        /// </summary>
        /// <returns></returns>
        IDisposable Connect();
    }
}
