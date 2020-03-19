#define REFCOUNT
// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Text;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class StreamMessage
    {
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public abstract long MaxTimestamp { get; }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public abstract long MinTimestamp { get; }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public abstract void Free();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public abstract bool RefreshCount();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public abstract void EnsureConsistency();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public abstract void Inflate();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public abstract void Deflate();
    }

    /// <summary>
    /// Represents an insert, retract or CTI event.
    /// </summary>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public class StreamMessage<TKey, TPayload> : StreamMessage
    {
        private const int M1 = 0x5555;

        private const int M2 = 0x3333;

        private const int M4 = 0x0F0F;

        // The four values below are used for ComputeCount
        private static readonly byte[] SixteenBitHammingWeights = PreCalculateHammingWeights();

        /// <summary>
        /// A one time pre-computation of the hamming weights for 16 bit fields, since we aren't exactly short of
        /// memory we can use this to vastly speed up our calculations of hamming weights on the 64
        /// bit vector fields at a miniscule cost in memory.
        /// </summary>
        /// <returns></returns>
        private static byte[] PreCalculateHammingWeights()
        {
            var sixteenBitHammingWeights = new byte[65536];
            for (int i = 0; i < 65536; ++i)
            {
                // See http://en.wikipedia.org/wiki/Hamming_weight
                var currentValue = i;
                currentValue -= (currentValue >> 1) & M1;
                currentValue = (currentValue & M2) + ((currentValue >> 2) & M2);
                currentValue = (currentValue + (currentValue >> 4)) & M4;
                currentValue += currentValue >> 8;
                sixteenBitHammingWeights[i] = (byte)(currentValue & 0x7f);
            }

            return sixteenBitHammingWeights;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        protected static readonly Func<TKey, int> HashCode = EqualityComparerExpression<TKey>.DefaultGetHashCodeFunction;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        protected static readonly bool IsPartitioned = typeof(TKey) != typeof(Empty);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public ColumnBatch<long> vsync;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public ColumnBatch<long> vother;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public ColumnBatch<TKey> key;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public ColumnBatch<int> hash;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public ColumnBatch<long> bitvector;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public ColumnBatch<TPayload> payload;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public MemoryPool<TKey, TPayload> memPool;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int Count;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int iter;

        [DataMember]
        private bool isSealed;

        /// <summary>
        /// This constructor is meant to be used only for serialization/deserialization.
        /// Do not call it directly!
        /// </summary>
        [Obsolete("Used only by serialization. Do not call directly.")]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public StreamMessage() => Contract.Ensures(!this.IsSealed);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Allocate()
        {
            this.memPool.Get(out this.vsync);
            this.memPool.Get(out this.vother);
            this.memPool.Get(out this.hash);
            this.memPool.GetBV(out this.bitvector);
            this.memPool.GetKey(out this.key);
            AllocatePayload();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="memPool"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual void AssignPool(MemoryPool<TKey, TPayload> memPool)
        {
            this.vsync.pool = memPool.longPool;
            this.vother.pool = memPool.longPool;
            this.key.pool = memPool.keyPool;
            this.hash.pool = memPool.intPool;
            this.bitvector.pool = memPool.bitvectorPool;

            AssignPayloadPool(memPool);
        }

        /// <summary>
        /// Adds a single row to the StreamMessage.
        /// </summary>
        /// <param name="vsync">The start time for the row.</param>
        /// <param name="vother">The other time for the row.</param>
        /// <param name="key">The grouping key for the row.</param>
        /// <param name="payload">The actual data of the row.</param>
        /// <returns>True iff the StreamMessage is full (after adding the row).</returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual bool Add(long vsync, long vother, TKey key, TPayload payload)
        {
            Contract.Requires(!this.IsSealed);

            this.vsync.col[this.Count] = vsync;
            this.vother.col[this.Count] = vother;
            if (this.key != null) this.key.col[this.Count] = key;
            if (this.payload != null) this.payload.col[this.Count] = payload;
            this.hash.col[this.Count] = IsPartitioned ? HashCode(key) : 0;
            this.Count++;
            return this.Count == this.vsync.col.Length;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="largeBatch"></param>
        /// <param name="currentTime"></param>
        /// <param name="offset"></param>
        /// <param name="encounteredPunctuation"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe virtual bool Add(
            ArraySegment<StreamEvent<TPayload>> largeBatch,
            ref long currentTime,
            ref int offset,
            out bool encounteredPunctuation)
        {
            Contract.Requires(!this.IsSealed);

            var n = largeBatch.Offset + largeBatch.Count;
            var count = this.Count;
            var localOffset = offset;
            encounteredPunctuation = false; // let's be optimistic!

            fixed (long* vsync = this.vsync.col)
            {
                fixed (long* vother = this.vother.col)
                {
                    while ((count < Config.DataBatchSize) && (localOffset < n))
                    {
                        if (largeBatch.Array[localOffset].OtherTime == StreamEvent.PunctuationOtherTime)
                        {
                            // BUGBUG: see StreamEvent<T>.IsPunctuation if that gets inlined, then use it here
                            encounteredPunctuation = true;
                            this.Count = count;
                            offset = localOffset;
                            return false;
                        }
                        if (largeBatch.Array[localOffset].SyncTime < currentTime)
                        {
                            throw new IngressException("Out-of-order event encountered during ingress, under a disorder policy of Throw");
                        }
                        currentTime = largeBatch.Array[localOffset].SyncTime;
                        vsync[count] = largeBatch.Array[localOffset].SyncTime;
                        vother[count] = largeBatch.Array[localOffset].OtherTime;
                        this.payload.col[count] = largeBatch.Array[localOffset].Payload;
                        localOffset++;
                        count++;
                    }
                }
            }

            this.Count = count;
            offset = localOffset;
            return count == this.vsync.col.Length;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="largeBatch"></param>
        /// <param name="currentTime"></param>
        /// <param name="offset"></param>
        /// <param name="startEdgeExtractor"></param>
        /// <param name="endEdgeExtractor"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe virtual bool Add(
            ArraySegment<TPayload> largeBatch,
            ref long currentTime,
            ref int offset,
            Func<TPayload, long> startEdgeExtractor,
            Func<TPayload, long> endEdgeExtractor)
        {
            Contract.Requires(!this.IsSealed);

            var n = largeBatch.Offset + largeBatch.Count;
            var count = this.Count;
            var localOffset = offset;

            fixed (long* vsync = this.vsync.col)
            {
                fixed (long* vother = this.vother.col)
                {
                    while ((count < Config.DataBatchSize) && (localOffset < n))
                    {
                        var start = startEdgeExtractor(largeBatch.Array[localOffset]);
                        if (start < currentTime)
                        {
                            throw new IngressException("Out-of-order event encountered during ingress, under a disorder policy of Throw");
                        }
                        currentTime = start;
                        vsync[count] = start;
                        vother[count] = endEdgeExtractor(largeBatch.Array[localOffset]);
                        this.payload.col[count] = largeBatch.Array[localOffset];
                        localOffset++;
                        count++;
                    }
                }
            }

            this.Count = count;
            offset = localOffset;
            return count == this.vsync.col.Length;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="largeBatch"></param>
        /// <param name="partitionConstructor"></param>
        /// <param name="currentTime"></param>
        /// <param name="offset"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe virtual bool Add<T>(
            ArraySegment<PartitionedStreamEvent<T, TPayload>> largeBatch,
            Func<T, TKey> partitionConstructor,
            Dictionary<T, long> currentTime,
            ref int offset)
        {
            Contract.Requires(!this.IsSealed);

            var n = largeBatch.Offset + largeBatch.Count;
            var count = this.Count;
            var localOffset = offset;

            fixed (long* vsync = this.vsync.col)
            {
                fixed (long* vother = this.vother.col)
                {
                    while ((count < Config.DataBatchSize) && (localOffset < n))
                    {
                        var partition = largeBatch.Array[localOffset].PartitionKey;
                        if (currentTime.ContainsKey(partition) && largeBatch.Array[localOffset].SyncTime < currentTime[partition])
                        {
                            throw new IngressException("Out-of-order event encountered during ingress, under a disorder policy of Throw");
                        }
                        currentTime[largeBatch.Array[localOffset].PartitionKey] = largeBatch.Array[localOffset].SyncTime;
                        vsync[count] = largeBatch.Array[localOffset].SyncTime;
                        vother[count] = largeBatch.Array[localOffset].OtherTime;
                        this.payload.col[count] = largeBatch.Array[localOffset].Payload;
                        this.key.col[count] = partitionConstructor(largeBatch.Array[localOffset].PartitionKey);
                        this.hash.col[count] = HashCode(this.key.col[count]);
                        if (largeBatch.Array[localOffset].OtherTime < 0)
                        {
                            this.bitvector.col[count >> 6] |= (1L << (count & 0x3f));
                        }
                        localOffset++;
                        count++;
                    }
                }
            }

            this.Count = count;
            offset = localOffset;
            return count == this.vsync.col.Length;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="largeBatch"></param>
        /// <param name="partitionExtractor"></param>
        /// <param name="partitionConstructor"></param>
        /// <param name="currentTime"></param>
        /// <param name="offset"></param>
        /// <param name="startEdgeExtractor"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe virtual bool Add<T>(
            ArraySegment<TPayload> largeBatch,
            Func<TPayload, T> partitionExtractor,
            Func<T, TKey> partitionConstructor,
            Dictionary<T, long> currentTime,
            ref int offset,
            Func<TPayload, long> startEdgeExtractor)
        {
            Contract.Requires(!this.IsSealed);

            var n = largeBatch.Offset + largeBatch.Count;
            var count = this.Count;
            var localOffset = offset;

            fixed (long* vsync = this.vsync.col)
            {
                fixed (long* vother = this.vother.col)
                {
                    while ((count < Config.DataBatchSize) && (localOffset < n))
                    {
                        var partition = partitionExtractor(largeBatch.Array[localOffset]);
                        var start = startEdgeExtractor(largeBatch.Array[localOffset]);
                        if (currentTime.ContainsKey(partition) && start < currentTime[partition])
                        {
                            throw new IngressException("Out-of-order event encountered during ingress, under a disorder policy of Throw");
                        }
                        currentTime[partition] = start;
                        vsync[count] = start;
                        vother[count] = StreamEvent.InfinitySyncTime;
                        this.payload.col[count] = largeBatch.Array[localOffset];
                        var p = partitionConstructor(partition);
                        this.key.col[count] = p;
                        this.hash.col[count] = HashCode(p);
                        localOffset++;
                        count++;
                    }
                }
            }

            this.Count = count;
            offset = localOffset;
            return count == this.vsync.col.Length;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="largeBatch"></param>
        /// <param name="partitionExtractor"></param>
        /// <param name="partitionConstructor"></param>
        /// <param name="currentTime"></param>
        /// <param name="offset"></param>
        /// <param name="startEdgeExtractor"></param>
        /// <param name="endEdgeExtractor"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe virtual bool Add<T>(
            ArraySegment<TPayload> largeBatch,
            Func<TPayload, T> partitionExtractor,
            Func<T, TKey> partitionConstructor,
            Dictionary<T, long> currentTime,
            ref int offset,
            Func<TPayload, long> startEdgeExtractor,
            Func<TPayload, long> endEdgeExtractor)
        {
            Contract.Requires(!this.IsSealed);

            var n = largeBatch.Offset + largeBatch.Count;
            var count = this.Count;
            var localOffset = offset;

            fixed (long* vsync = this.vsync.col)
            fixed (long* vother = this.vother.col)
            {
                while ((count < Config.DataBatchSize) && (localOffset < n))
                {
                    var partition = partitionExtractor(largeBatch.Array[localOffset]);
                    var start = startEdgeExtractor(largeBatch.Array[localOffset]);
                    if (currentTime.ContainsKey(partition) && start < currentTime[partition])
                    {
                        throw new IngressException("Out-of-order event encountered during ingress, under a disorder policy of Throw");
                    }
                    currentTime[partition] = start;
                    vsync[count] = start;
                    vother[count] = endEdgeExtractor(largeBatch.Array[localOffset]);
                    this.payload.col[count] = largeBatch.Array[localOffset];
                    var p = partitionConstructor(partition);
                    this.key.col[count] = p;
                    this.hash.col[count] = HashCode(p);
                    localOffset++;
                    count++;
                }
            }

            this.Count = count;
            offset = localOffset;
            return count == this.vsync.col.Length;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="largeBatch"></param>
        /// <param name="currentTime"></param>
        /// <param name="offset"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe virtual bool Add(
            ArraySegment<TPayload> largeBatch,
            ref long currentTime,
            ref int offset)
        {
            Contract.Requires(!this.IsSealed);

            var n = largeBatch.Offset + largeBatch.Count;
            var count = this.Count;
            var localOffset = offset;

            fixed (long* vsync = this.vsync.col)
            fixed (long* vother = this.vother.col)
            {
                while ((count < Config.DataBatchSize) && (localOffset < n))
                {
                    currentTime = DateTimeOffset.UtcNow.Ticks;
                    vsync[count] = currentTime;
                    vother[count] = StreamEvent.InfinitySyncTime;
                    this.payload.col[count] = largeBatch.Array[localOffset];
                    localOffset++;
                    count++;
                }
            }

            this.Count = count;
            offset = localOffset;
            return count == this.vsync.col.Length;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="largeBatch"></param>
        /// <param name="currentTime"></param>
        /// <param name="offset"></param>
        /// <param name="eventsPerSample"></param>
        /// <param name="currentSync"></param>
        /// <param name="eventCount"></param>
        /// <param name="encounteredPunctuation"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe virtual bool Add(
            ArraySegment<TPayload> largeBatch,
            ref long currentTime,
            ref int offset,
            int eventsPerSample,
            ref int currentSync,
            ref int eventCount,
            out bool encounteredPunctuation)
        {
            Contract.Requires(!this.IsSealed);

            var n = largeBatch.Offset + largeBatch.Count;
            var count = this.Count;
            var localOffset = offset;
            encounteredPunctuation = false; // let's be optimistic!

            fixed (long* vsync = this.vsync.col)
            fixed (long* vother = this.vother.col)
            {
                while ((count < Config.DataBatchSize) && (localOffset < n))
                {
                    currentTime = currentSync;
                    vsync[count] = currentTime;
                    vother[count] = StreamEvent.InfinitySyncTime;
                    this.payload.col[count] = largeBatch.Array[localOffset];
                    localOffset++;
                    count++;
                    eventCount++;

                    if (eventCount == eventsPerSample)
                    {
                        eventCount = 0;
                        currentSync++;
                        encounteredPunctuation = true;
                        break;
                    }
                }
            }

            this.Count = count;
            offset = localOffset;
            return count == this.vsync.col.Length;
        }

        /// <summary>
        /// Adds a single row containing a LowWatermark to the StreamMessage. Only applicable to partitioned streams.
        /// </summary>
        /// <param name="vsync">The start time for the row.</param>
        /// <returns>True iff the StreamMessage is full (after adding the row).</returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual bool AddLowWatermark(long vsync)
        {
            Contract.Requires(!this.IsSealed);

            this.vsync.col[this.Count] = vsync;
            this.vother.col[this.Count] = PartitionedStreamEvent.LowWatermarkOtherTime;
            if (this.key != null) this.key.col[this.Count] = default;
            if (this.payload != null) this.payload.col[this.Count] = default;
            this.hash.col[this.Count] = 0;
            this.Count++;
            return this.Count == this.vsync.col.Length;
        }

        /// <summary>
        /// Adds a single row containing a punctuation to the StreamMessage.
        /// </summary>
        /// <param name="vsync">The start time for the row.</param>
        /// <returns>True iff the StreamMessage is full (after adding the row).</returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual bool AddPunctuation(long vsync)
        {
            Contract.Requires(!this.IsSealed);

            this.vsync.col[this.Count] = vsync;
            this.vother.col[this.Count] = StreamEvent.PunctuationOtherTime;
            if (this.key != null) this.key.col[this.Count] = default;
            if (this.payload != null) this.payload.col[this.Count] = default;
            this.hash.col[this.Count] = 0;
            this.bitvector.col[this.Count >> 6] |= 1L << (this.Count & 0x3f);

            this.Count++;
            return this.Count == this.vsync.col.Length;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool IsSealed => this.isSealed;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual void Seal()
        {
            Contract.Ensures(this.IsSealed);
            this.isSealed = true;

            EnsureConsistency();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// This method is used in checkpoint creation and restoration.
        /// It delegates the current row count in the message to the individual columns for better local understanding in serialization.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void EnsureConsistency()
        {
            /* Copy counts */
            this.vsync.UsedLength = this.Count;
            this.vother.UsedLength = this.Count;
            if (this.payload != null) this.payload.UsedLength = this.Count;
            if (this.key != null) this.key.UsedLength = this.Count;
            if (this.hash != null) this.hash.UsedLength = this.Count;
            this.bitvector.UsedLength = (1 + (this.Count >> 6));
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// Apply optimizations that reduce the size of the message for serialization purposes.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void Deflate()
        {
            if (typeof(TKey) == typeof(Empty))
            {
                this.key.Return();
                this.key = null;
                this.hash.Return();
                this.hash = null;
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// Reverses the optimizations applied using the Deflate method.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void Inflate()
        {
            if (typeof(TKey) == typeof(Empty))
            {
                if (this.memPool != null)
                {
                    if (this.key == null) this.memPool.GetKey(out this.key);
                    if (this.hash == null) this.memPool.Get(out this.hash);
                }
                else
                {
                    if (this.key == null) MemoryManager.GetColumnPool<TKey>().Get(out this.key);
                    if (this.hash == null) MemoryManager.GetColumnPool<int>().Get(out this.hash);
                    Array.Clear(this.hash.col, 0, this.hash.col.Length);
                }
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void Free()
        {
            Release();
            Return();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual TPayload this[int index]
        {
            get
            {
                Contract.Requires(index < this.Count);
                return this.payload.col[index];
            }
            set
            {
                Contract.Requires(!this.IsSealed);
                this.payload.col[index] = value;
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.AppendFormat(CultureInfo.InvariantCulture, "Batch has {0} rows:\n", ComputeCount());
            sb.AppendFormat(CultureInfo.InvariantCulture, " ###:   vSync  vOther        Key    Payload\n");
            for (int row = 0; row < this.Count; row++)
            {
                if ((this.bitvector.col[row >> 6] & (1L << (row & 0x3f))) != 0)
                {
                    sb.AppendFormat(CultureInfo.InvariantCulture, " {0,3}: row is hidden\n", row);
                    continue;
                }

                sb.AppendFormat(
                    CultureInfo.InvariantCulture,
                    " {0,3}: {1,7} {2,7} {3,10} {4,10}\n",
                    row,
                    FriendlyTime(this.vsync.col[row]),
                    FriendlyTime(this.vother.col[row]), this.key.col[row],
                    this[row]);
            }

            return sb.ToString();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Print() => Console.WriteLine(ToString());

        /* TODO: Everything after this should be made internal - how to do that in the presence of generated subclasses? */

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="pool"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public StreamMessage(MemoryPool<TKey, TPayload> pool)
        {
            Contract.Ensures(!this.IsSealed);

            this.memPool = pool;
            this.Count = 0;
            this.isSealed = false;
            this.iter = 0;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual void AllocatePayload()
        {
            if (!this.memPool.IsColumnar) this.memPool.GetPayload(out this.payload);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="memPool"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected virtual void AssignPayloadPool(MemoryPool<TKey, TPayload> memPool)
        {
            if ((!memPool.IsColumnar) && (this.payload != null)) this.payload.pool = memPool.payloadPool;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="value"></param>
        /// <param name="swing"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual void CloneFrom(StreamMessage<TKey, TPayload> value, bool swing = false)
        {
            if (value.payload != null) this.payload = value.payload;

            if (!swing && value.payload != null)
            {
                value.payload.IncrementRefCount(1);
            }

            CloneFromNoPayload(value, swing);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <typeparam name="TForeignPayload"></typeparam>
        /// <param name="value"></param>
        /// <param name="swing"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual void CloneFromNoPayload<TForeignPayload>(StreamMessage<TKey, TForeignPayload> value, bool swing = false)
        {
            this.vsync = value.vsync;
            this.vother = value.vother;

            this.key = value.key;
            this.hash = value.hash;
            this.bitvector = value.bitvector;

            this.Count = value.Count;
            this.iter = value.iter;

            if (swing) return;

            value.vsync.IncrementRefCount(1);
            value.vother.IncrementRefCount(1);

            // TODO in derived class:
            // GENERATE: payload = value.payload;
            // GENERATE: value.payload.IncrementRefCount(1);
            if (value.key != null)
                value.key.IncrementRefCount(1);
            value.hash.IncrementRefCount(1);
            value.bitvector.IncrementRefCount(1);
        }

        /// <summary>
        /// Returns a count of the number of rows in the message that are present (e.g. count - number of filtered out rows)
        /// </summary>
        [Pure]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual int ComputeCount()
        {
            Contract.Ensures(Contract.Result<int>() <= this.Count);

            return this.Count == 0 ? 0 : ComputeCount(0, this.Count - 1);
        }

        /// <summary>
        /// Return a count of the number of rows in the message that are present starting at startIndex and going to (and including) endIndex. Both startIndex and endIndex must be less than
        /// or equal to Count and Count must be greater than 0.
        /// </summary>
        [Pure]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe virtual int ComputeCount(int startIndex, int endIndex)
        {
            Contract.Requires(startIndex >= 0);
            Contract.Requires(startIndex <= endIndex);
            Contract.Requires(endIndex < this.Count);
            Contract.Ensures(Contract.Result<int>() <= (endIndex - startIndex + 1));

            var hammingWeight = 0;

            fixed (long* bitVectorColumnLong = this.bitvector.col)
            {
                ulong* bitVectorColumn = (ulong*)bitVectorColumnLong;
                fixed (byte* sixteenBitHammingWeights = SixteenBitHammingWeights)
                {
                    var startIndexBits = startIndex & 0x3f;
                    var startIndexBitVectorIndex = startIndex >> 6;

                    var endIndexBits = endIndex & 0x3f;
                    var endIndexBitVectorIndex = endIndex >> 6;

                    if (startIndexBitVectorIndex == endIndexBitVectorIndex)
                    {
                        var startMask = -1L << startIndexBits;
                        var endMask = (long)(0xFFFFFFFFFFFFFFFFUL >> (0x3f - endIndexBits));
                        var combinedMask = (ulong)(startMask & endMask);
                        hammingWeight =
                            CalculateHammingWeight(
                                bitVectorColumn[startIndexBitVectorIndex] & combinedMask,
                                sixteenBitHammingWeights);
                    }
                    else
                    {
                        int bitVectorIndex = startIndexBits == 0 ? startIndexBitVectorIndex : startIndexBitVectorIndex + 1;
                        int exit = endIndexBits == 0x3f ? endIndexBitVectorIndex + 1 : endIndexBitVectorIndex;

                        for (; bitVectorIndex < exit; ++bitVectorIndex)
                        {
                            hammingWeight += CalculateHammingWeight(bitVectorColumn[bitVectorIndex], sixteenBitHammingWeights);
                        }

                        if (startIndexBits > 0)
                        {
                            hammingWeight +=
                                CalculateHammingWeight(
                                    bitVectorColumn[startIndexBitVectorIndex] & (ulong)(-1L << startIndexBits), sixteenBitHammingWeights);
                        }

                        if (endIndexBits < 0x3f)
                        {
                            hammingWeight +=
                                CalculateHammingWeight(bitVectorColumn[endIndexBitVectorIndex] & (0xFFFFFFFFFFFFFFFFUL >> (0x3f - endIndexBits)), sixteenBitHammingWeights);
                        }
                    }

                    return endIndex - startIndex + 1 - hammingWeight;
                }
            }
        }

        // I check for 0 because I'm betting in most cases the vectors are 0
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe int CalculateHammingWeight(ulong bitVectorLong, byte* sixteenBitHammingWeights)
            => bitVectorLong > 0
                       ? sixteenBitHammingWeights[bitVectorLong & 0xFFFF]
                         + sixteenBitHammingWeights[bitVectorLong >> 16 & 0xFFFF]
                         + sixteenBitHammingWeights[bitVectorLong >> 32 & 0xFFFF]
                         + sixteenBitHammingWeights[bitVectorLong >> 48]
                       : 0;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="rowNumber"></param>
        /// <returns></returns>
        [Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual bool IsFiltered(int rowNumber) => (this.bitvector.col[rowNumber >> 6] & (1L << (rowNumber & 0x3f))) != 0;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual void Release()
        {
            this.vsync.Return();
            this.vother.Return();
            ReleaseKey();
            this.hash.Return();
            this.bitvector.ReturnClear();
            ReleasePayload();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual void ReleaseKey()
        {
            if (this.key != null) this.key.Return();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual void ReleasePayload()
        {
            if (this.payload != null) this.payload.Return();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Return()
        {
            if (this.memPool != null)
            {
                this.Count = 0;
                this.isSealed = false;
                this.iter = 0;
                this.memPool.Return(this);
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override bool RefreshCount()
        {
            int i = this.Count - 1;
            var bv = this.bitvector.col;
            var o = this.vother.col;

            while ((i >= 0) && ((bv[i >> 6] & (1L << (i & 0x3f))) != 0) && o[i] >= 0)
            {
                i--;
            }

            if (this.Count != i + 1)
            {
                this.Count = i + 1;
                return true;
            }
            return false;
        }

        internal int CountDeleted()
        {
            if (this.Count == 0) return 0;

            int arrayCount = ((this.Count - 1) >> 6) + 1;
            int sum = 0;
            var bv = this.bitvector.col;

            for (int i = 0; i < arrayCount; i++)
            {
                long cell = bv[i];
                if (cell == 0) continue;
                long mask = 1L;
                for (int j = 0; j < 64; j++)
                {
                    if ((cell & mask) != 0) sum++;
                    mask <<= 1;
                }
            }

            return sum;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override long MinTimestamp
        {
            get
            {
                if (typeof(TKey).GetPartitionType() != null)
                {
                    // Find min vsync among all non-filtered-out rows
                    var min = long.MaxValue;
                    int cur = 0;
                    while (cur < this.Count)
                    {
                        if (((this.bitvector.col[cur >> 6] & (1L << (cur & 0x3f))) == 0 || this.vother.col[cur] < 0) && min > this.vsync.col[cur])
                        {
                            min = this.vsync.col[cur];
                        }

                        cur++;
                    }

                    return min == long.MaxValue ? StreamEvent.MinSyncTime : min;
                }
                else
                {
                    // Find the first element with non-filtered-out row
                    int cur = 0;
                    while (cur < this.Count && ((this.bitvector.col[cur >> 6] & (1L << (cur & 0x3f))) != 0) && this.vother.col[cur] >= 0)
                    {
                        cur++;
                    }

                    return cur == this.Count ? StreamEvent.MinSyncTime : this.vsync.col[cur];
                }
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override long MaxTimestamp
        {
            get
            {
                if (typeof(TKey).GetPartitionType() != null)
                {
                    // Find max vsync among all non-filtered-out rows
                    var max = long.MinValue;
                    int cur = 0;
                    while (cur < this.Count)
                    {
                        if (((this.bitvector.col[cur >> 6] & (1L << (cur & 0x3f))) == 0 || this.vother.col[cur] < 0) && max < this.vsync.col[cur])
                        {
                            max = this.vsync.col[cur];
                        }

                        cur++;
                    }

                    return max == long.MinValue ? StreamEvent.MaxSyncTime : max;
                }
                else
                {
                    // Find the last element with non-filtered-out row
                    int cur = this.Count - 1;
                    while (cur >= 0 && ((this.bitvector.col[cur >> 6] & (1L << (cur & 0x3f))) != 0) && this.vother.col[cur] >= 0)
                    {
                        cur--;
                    }

                    return cur < 0 ? StreamEvent.MaxSyncTime : this.vsync.col[cur];
                }
            }
        }

        /* Private members */
        private static string FriendlyTime(long time)
        {
            if (time == StreamEvent.InfinitySyncTime)
            {
                return "INF";
            }
            else if (time == StreamEvent.PunctuationOtherTime)
            {
                return "PUNC";
            }
            else if (time == PartitionedStreamEvent.LowWatermarkOtherTime)
            {
                return "LOWMARK";
            }

            return time.ToString(CultureInfo.InvariantCulture);
        }
    }
}