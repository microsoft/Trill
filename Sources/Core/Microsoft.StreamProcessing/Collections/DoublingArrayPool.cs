// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Microsoft.StreamProcessing.Internal.Collections
{
    /// <summary>
    /// Encapsulates a memory pool of ColumnBatches.
    /// </summary>
    /// <typeparam name="T">
    /// The type of element stored within each ColumnBatch.
    /// </typeparam>
    internal sealed class DoublingArrayPool<T> : ColumnPoolBase
    {
        private readonly ConcurrentQueue<T[]>[] pool;
        private long createdObjects;

        public DoublingArrayPool() => this.pool = new ConcurrentQueue<T[]>[32];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Return(T[] item)
        {
            if (!IsPowerOfTwo(item.Length)) throw new InvalidOperationException("Invalid array to return to SizedColumnPool");
            int pos = GetLogBase2(item.Length);

            if (this.pool[pos] == null) this.pool[pos] = new ConcurrentQueue<T[]>();
            this.pool[pos].Enqueue(item);
        }

        private static readonly int[] MultiplyDeBruijnBitPosition2 = new int[32]
        {
            0, 1, 28, 2, 29, 14, 24, 3, 30, 22, 20, 15, 25, 17, 4, 8,
            31, 27, 13, 23, 21, 19, 16, 7, 26, 12, 18, 6, 11, 5, 10, 9
        };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsPowerOfTwo(long x) => (x > 0) && ((x & (x - 1)) == 0);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetLogBase2(int x) => MultiplyDeBruijnBitPosition2[(uint)(x * 0x077CB531U) >> 27];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Get(out T[] result, int size)
        {
            int pos = (int)Math.Ceiling(Math.Log(size, 2));

            if (this.pool[pos] == null) this.pool[pos] = new ConcurrentQueue<T[]>();

            if (!this.pool[pos].TryDequeue(out result))
            {
                result = new T[1 << pos];
                Interlocked.Increment(ref this.createdObjects);
            }

            return true;
        }

        public override string GetStatusReport()
        {
            int totalCount = 0;
            foreach (var entry in this.pool)
            {
                if (entry != null)
                    totalCount += entry.Count;
            }

            return string.Format(CultureInfo.InvariantCulture, "[{0}] Objects Created - {1,5} - Queue Size - {2,5}\t{3}", this.createdObjects == totalCount ? " " : "X", this.createdObjects, totalCount, typeof(T).GetCSharpSourceSyntax());
        }

        public override ColumnPoolBase Leaked
        {
            get
            {
                int totalCount = 0;
                foreach (var entry in this.pool)
                {
                    if (entry != null)
                        totalCount += entry.Count;
                }
                return ((!Config.DisableMemoryPooling) && (this.createdObjects != totalCount)) ? this : null;
            }
        }

        public override void Free(bool reset = false)
        {
            foreach (var queue in this.pool)
            {
                if (queue != null)
                {
                    while (queue.TryDequeue(out T[] result))
                    {
                        result = null;
                        Interlocked.Decrement(ref this.createdObjects);
                    }
                }
            }
            if (reset)
            {
                Interlocked.Exchange(ref this.createdObjects, 0);
            }
        }
    }
}
