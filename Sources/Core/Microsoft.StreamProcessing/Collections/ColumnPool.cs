// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Collections.Concurrent;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Microsoft.StreamProcessing.Internal.Collections
{
    /// <summary>
    /// The base class for all column pools.
    /// Non-generic so it can be the base class for
    /// classes of different genericity as well as
    /// making it easy to have collections of them.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class ColumnPoolBase
    {
        /// <summary>
        /// Returns a formatted report of how many objects have been created
        /// and how many have been returned.
        /// </summary>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public abstract string GetStatusReport();

        /// <summary>
        /// If there are any unreturned memory blocks, this returns the column pool itself (so
        /// that it can be further queried). Otherwise it returns null
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public abstract ColumnPoolBase Leaked { get; }

        /// <summary>
        /// Frees the queue and re-initializes the pool. If the reset flag is set, then it also zeros out
        /// the number of objects it has created.
        /// </summary>
        /// <param name="reset"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public abstract void Free(bool reset = false);
    }

    /// <summary>
    /// Encapsulates a memory pool of ColumnBatches.
    /// </summary>
    /// <typeparam name="T">
    /// The type of element stored within each ColumnBatch.
    /// </typeparam>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public class ColumnPool<T> : ColumnPoolBase
    {
        private readonly ConcurrentQueue<ColumnBatch<T>> queue = new ConcurrentQueue<ColumnBatch<T>>();
        private long createdObjects;
        private readonly int size;

        internal ColumnPool() => this.size = Config.DataBatchSize;

        internal ColumnPool(int size) => this.size = size;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="item"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Return(ColumnBatch<T> item) => this.queue.Enqueue(item);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="result"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool Get(out ColumnBatch<T> result)
        {
            if (this.queue.TryDequeue(out result))
            {
                result.IncrementRefCount(1);
                return true;
            }
            result = new ColumnBatch<T>(this, this.size);
            Interlocked.Increment(ref this.createdObjects);

            return true;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override string GetStatusReport()
            => string.Format(CultureInfo.InvariantCulture, "[{0}] Objects Created - {1,5} - Queue Size - {2,5}\t{3}",
                !SomethingIsWrong() ? " " : "X", this.createdObjects, this.queue.Count, typeof(T).GetCSharpSourceSyntax());

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override ColumnPoolBase Leaked
            => ((!Config.DisableMemoryPooling) && SomethingIsWrong()) ? this : null;

        private bool SomethingIsWrong()
            => (this.createdObjects != this.queue.Count) || this.queue.Any(cb => cb.RefCount != 0);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="reset"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void Free(bool reset = false)
        {
            while (this.queue.TryDequeue(out var result))
            {
                result.pool = null;
                result.col = null;
                result = null;
                Interlocked.Decrement(ref this.createdObjects);
            }
            if (reset)
            {
                Interlocked.Exchange(ref this.createdObjects, 0);
            }
        }
    }
}
