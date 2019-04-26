// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************

namespace Microsoft.StreamProcessing.Provider
{
    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TPayload"></typeparam>
    public interface IGroupedWindow<out TKey, out TPayload>
    {
        /// <summary>
        ///
        /// </summary>
        TKey Key { get; }

        /// <summary>
        ///
        /// </summary>
        IWindow<TPayload> Window { get; }
    }
}
