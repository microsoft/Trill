#if COLUMNAR
// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.ComponentModel;
using System.Runtime.CompilerServices;

namespace Microsoft.StreamProcessing.Internal.Collections
{
    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TPayload"></typeparam>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public class MemoryPool<TKey, TPayload>
    {
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool IsColumnar;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public ColumnPool<long> longPool;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public ColumnPool<TKey> keyPool;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public ColumnPool<TPayload> payloadPool;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public ColumnPool<int> intPool;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public ColumnPool<short> shortPool;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public ColumnPool<byte> bytePool;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public ColumnPool<string> stringPool;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public ColumnPool<long> bitvectorPool;
        private readonly StreamMessagePool<TKey, TPayload> eventBatchPool;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        internal CharArrayPool charArrayPool;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="isColumnar"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public MemoryPool(bool isColumnar = true)
        {
            this.IsColumnar = isColumnar;
            this.longPool = MemoryManager.GetColumnPool<long>();
            this.keyPool = MemoryManager.GetColumnPool<TKey>();
            this.payloadPool = MemoryManager.GetColumnPool<TPayload>();
            this.intPool = MemoryManager.GetColumnPool<int>();
            this.shortPool = MemoryManager.GetColumnPool<short>();
            this.bytePool = MemoryManager.GetColumnPool<byte>();
            this.stringPool = MemoryManager.GetColumnPool<string>();
            this.bitvectorPool = MemoryManager.GetBVPool(1 + (Config.DataBatchSize >> 6));
            this.eventBatchPool = MemoryManager.GetStreamMessagePool(this, this.IsColumnar);
            this.charArrayPool = MemoryManager.GetCharArrayPool();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="memoryPoolName"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void GetQueueReport(string memoryPoolName)
            => Console.WriteLine(
                "Memory Pool Name {0} == LongPool {1}, KeyPool {2}, PayloadPool {3}, IntPool {4}, StringPool {5}, BVPool {6}, StreamMessagePool {7}",
                memoryPoolName, this.longPool.GetStatusReport(), this.keyPool.GetStatusReport(), this.payloadPool.GetStatusReport(), this.intPool.GetStatusReport(), this.stringPool.GetStatusReport(), this.bitvectorPool.GetStatusReport(), this.eventBatchPool.GetStatusReport());

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="result"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool Get(out StreamMessage<TKey, TPayload> result) => this.eventBatchPool.Get(out result);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="result"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool GetKey(out ColumnBatch<TKey> result) => this.keyPool.Get(out result);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="result"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool GetPayload(out ColumnBatch<TPayload> result) => this.payloadPool.Get(out result);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="result"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool Get(out ColumnBatch<long> result) => this.longPool.Get(out result);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="result"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool Get(out ColumnBatch<int> result) => this.intPool.Get(out result);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="result"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool Get(out ColumnBatch<byte> result) => this.bytePool.Get(out result);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="result"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool Get(out ColumnBatch<string> result) => this.stringPool.Get(out result);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="result"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool GetBV(out ColumnBatch<long> result) => this.bitvectorPool.Get(out result);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="result"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool Get(out MultiString result)
        {
            result = new MultiString(this.charArrayPool, this.intPool, this.shortPool, this.bitvectorPool);
            return true;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="streamMessage"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Return(StreamMessage<TKey, TPayload> streamMessage) => this.eventBatchPool.Return(streamMessage);
    }

}
#endif
