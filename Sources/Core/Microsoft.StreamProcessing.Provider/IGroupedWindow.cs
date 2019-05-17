// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************

namespace Microsoft.StreamProcessing.Provider
{
    /// <summary>
    /// A streaming analogue to the framework interface IGrouping
    /// </summary>
    /// <typeparam name="TKey">The type of the grouping key</typeparam>
    /// <typeparam name="TPayload">The type of the payload being grouped</typeparam>
    public interface IGroupedWindow<out TKey, out TPayload>
    {
        /// <summary>
        /// A concrete reference to the key of the group
        /// </summary>
        TKey Key { get; }

        /// <summary>
        /// A reference to the payload data contained in the group
        /// </summary>
        IWindow<TPayload> Window { get; }
    }
}
