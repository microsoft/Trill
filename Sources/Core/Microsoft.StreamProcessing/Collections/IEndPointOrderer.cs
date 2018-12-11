// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.ComponentModel;

namespace Microsoft.StreamProcessing.Internal.Collections
{
    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public interface IEndPointOrderer
    {
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        int Count { get; }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        bool IsEmpty { get; }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        int Capacity { get; }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="time"></param>
        /// <param name="value"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        void Insert(long time, int value);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="time"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        bool TryPeekNext(out long time, out int value);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="time"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        bool TryGetNext(out long time, out int value);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="maxTime"></param>
        /// <param name="time"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        bool TryGetNextInclusive(long maxTime, out long time, out int value);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="maxTime"></param>
        /// <param name="time"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        bool TryGetNextExclusive(long maxTime, out long time, out int value);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        void RemoveTop();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        void Clear();
    }
}
