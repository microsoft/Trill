// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Threading;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing.Internal
{
    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public sealed class ColumnBatch<T>
    {
        /// <summary>
        /// Used to make sure this class is thread-safe when it makes decisions
        /// about the reference count. (See <see cref="MakeWritable"/>.
        /// </summary>
        private readonly Lock columnBatchLock = new();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public T[] col;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int UsedLength;

        internal ColumnPool<T> pool;
        internal int RefCount;

        internal ColumnBatch() => this.RefCount = 1;

        internal ColumnBatch(int size)
        {
            this.pool = null;
            this.col = new T[size];
            this.UsedLength = 0;
            this.RefCount = 1;
        }

        internal ColumnBatch(ColumnPool<T> pool, int size)
        {
            this.pool = pool;
            this.col = new T[size];
            this.RefCount = 1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void IncrementRefCount(int cnt)
        {
            using (this.columnBatchLock.EnterScope())
            {
                Interlocked.Add(ref this.RefCount, cnt);
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="pool"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public ColumnBatch<T> MakeWritable(ColumnPool<T> pool)
        {
            using (this.columnBatchLock.EnterScope())
            {
                if (this.RefCount == 1)
                {
                    return this;
                }
                else
                {
                    pool.Get(out var result);
                    this.col.AsSpan().CopyTo(result.col);
                    result.UsedLength = this.UsedLength;
                    Interlocked.Decrement(ref this.RefCount);
                    return result;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Return()
        {
            using (this.columnBatchLock.EnterScope())
            {
                int localRefCount = Interlocked.Decrement(ref this.RefCount);

                if (localRefCount == 0)
                {
                    if (Config.ClearColumnsOnReturn)
                    {
                        this.col.AsSpan().Clear();
                    }
                    if ((this.pool != null) && (!Config.DisableMemoryPooling))
                    {
                        this.UsedLength = 0;
                        this.pool.Return(this);
                    }
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ReturnClear()
        {
            using (this.columnBatchLock.EnterScope())
            {
                int localRefCount = Interlocked.Decrement(ref this.RefCount);

                if (localRefCount == 0)
                {
                    this.col.AsSpan().Clear();
                    if (this.pool != null && !Config.DisableMemoryPooling)
                    {
                        this.UsedLength = 0;
                        this.pool.Return(this);
                    }
                }
            }
        }

    }
}