// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Concurrent;
using System.ComponentModel;
using System.Runtime.CompilerServices;

namespace Microsoft.StreamProcessing.Internal.Collections
{
    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public sealed class DataStructurePool<T> : IDisposable where T : new()
    {
        private readonly ConcurrentQueue<T> queue;
        private readonly Func<T> creator;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public DataStructurePool()
        {
            this.queue = new ConcurrentQueue<T>();
            this.creator = () => new T();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="creator"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public DataStructurePool(Func<T> creator)
        {
            this.queue = new ConcurrentQueue<T>();
            this.creator = creator;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="item"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Return(T item)
        {
            if (!Config.DisableMemoryPooling) this.queue.Enqueue(item);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="result"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Get(out T result)
        {
            if (!this.queue.TryDequeue(out result)) result = this.creator();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Dispose()
        {
            foreach (var q in this.queue)
            {
                if (q is IDisposable d) d.Dispose();
            }
        }
    }
}
