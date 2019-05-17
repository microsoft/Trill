// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************

namespace Microsoft.StreamProcessing.Provider
{
    /// <summary>
    /// A window of data in a grouped window, similar to the group in an IGrouping.
    /// </summary>
    /// <typeparam name="TPayload">The type of the underlying payload in the window of an IGroupedWindow.</typeparam>
    public interface IWindow<out TPayload>
    {
        /// <summary>
        /// Take the count of the number of elements in the current window.
        /// </summary>
        int Count();
    }
}
