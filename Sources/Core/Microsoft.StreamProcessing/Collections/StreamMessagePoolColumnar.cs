#if COLUMNAR
// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Collections.Concurrent;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Microsoft.StreamProcessing.Internal.Collections
{
    /// <summary>
    /// A pool for StreamMessages.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TPayload"></typeparam>
    internal sealed class StreamMessagePool<TKey, TPayload> : ColumnPoolBase
    {
        private readonly MemoryPool<TKey, TPayload> memoryPool;
        private readonly bool IsColumnar;
        private readonly ConcurrentQueue<StreamMessage<TKey, TPayload>> batchQueue;

        private long createdObjects;

        public StreamMessagePool(MemoryPool<TKey, TPayload> memoryPool, bool isColumnar = true)
        {
            this.batchQueue = new ConcurrentQueue<StreamMessage<TKey, TPayload>>();
            this.memoryPool = memoryPool;
            this.IsColumnar = isColumnar;
        }

        public override string GetStatusReport()
            => string.Format(CultureInfo.InvariantCulture, "[{0}] Objects Created - {1,5} - Queue Size - {2,5}\t<{3},{4}>", this.createdObjects == this.batchQueue.Count ? " " : "X", this.createdObjects, this.batchQueue.Count, typeof(TKey).GetCSharpSourceSyntax(), typeof(TPayload).GetCSharpSourceSyntax());

        public override ColumnPoolBase Leaked => this.createdObjects != this.batchQueue.Count ? this : null;

        public override void Free(bool reset = false)
        {
            while (this.batchQueue.TryDequeue(out _))
            {
                Interlocked.Decrement(ref this.createdObjects);
            }
            if (reset)
                Interlocked.Exchange(ref this.createdObjects, 0);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Return(StreamMessage<TKey, TPayload> item)
        {
            item.Count = 0;
            item.iter = 0;
            this.batchQueue.Enqueue(item);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Get(out StreamMessage<TKey, TPayload> result)
        {
            if (!this.batchQueue.TryDequeue(out result))
            {
                result = this.IsColumnar
                    ? StreamMessageManager.GetStreamMessage(this.memoryPool)
                    : new StreamMessage<TKey, TPayload>(this.memoryPool);

                Interlocked.Increment(ref this.createdObjects);
            }

            return true;
        }
    }
}
#endif