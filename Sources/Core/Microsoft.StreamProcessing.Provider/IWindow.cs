// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************

namespace Microsoft.StreamProcessing.Provider
{
    /// <summary>
    ///
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    public interface IWindow<out TPayload>
    {
        /// <summary>
        ///
        /// </summary>
        int Count();
    }
}
