// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Microsoft.StreamProcessing.Internal.Collections
{
    internal sealed class CharArrayPool : ColumnPoolBase
    {
        private readonly ConcurrentQueue<CharArrayWrapper>[] pool;
        private long createdObjects;

        public CharArrayPool() => this.pool = new ConcurrentQueue<CharArrayWrapper>[32];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void Return(CharArrayWrapper caw)
        {
            int pos = (int)Math.Ceiling(Math.Log(caw.UsedLength, 2));
            if (this.pool[pos] == null) this.pool[pos] = new ConcurrentQueue<CharArrayWrapper>();
            caw.UsedLength = caw.charArray.content.Length;
            this.pool[pos].Enqueue(caw);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool Get(out CharArrayWrapper caw, int size)
        {
            if (size == 0)
            {
                caw = null;
                return false;
            }

            int pos = (int)Math.Ceiling(Math.Log(size, 2));

            if (this.pool[pos] == null) this.pool[pos] = new ConcurrentQueue<CharArrayWrapper>();

            if (this.pool[pos].TryDequeue(out caw))
            {
                caw.IncrementRefCount(1);
                caw.UsedLength = size;
            }
            else
            {
                caw = new CharArrayWrapper(this, (int)Math.Pow(2, pos), size);
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

            return string.Format(CultureInfo.InvariantCulture, "[{0}] Objects Created - {1,5} - Queue Size - {2,5}\t{3}",
                !SomethingIsWrong() ? " " : "X", this.createdObjects, totalCount, "CharArray");
        }

        public override ColumnPoolBase Leaked
            => ((!Config.DisableMemoryPooling) && SomethingIsWrong()) ? this : null;

        private bool SomethingIsWrong()
        {
            int totalCount = 0;
            bool incorrectRefCount = false;
            foreach (var entry in this.pool)
            {
                if (entry != null)
                {
                    totalCount += entry.Count;
                    if (entry.Any(caw => caw.RefCount != 0))
                        incorrectRefCount = true;
                }
            }
            return this.createdObjects != totalCount || incorrectRefCount;
        }

        public override void Free(bool reset = false)
        {
            foreach (var queue in this.pool)
            {
                if (queue != null)
                {
                    while (queue.TryDequeue(out var result))
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