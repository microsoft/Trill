// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Memory manager for Stream Processing.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public static class MemoryManager
    {
        private static readonly SafeConcurrentDictionary<ColumnPoolBase> doublingArrayPools = new SafeConcurrentDictionary<ColumnPoolBase>();
        private static readonly SafeConcurrentDictionary<ColumnPoolBase> columnPools = new SafeConcurrentDictionary<ColumnPoolBase>();
        private static readonly SafeConcurrentDictionary<CharArrayPool> charArrayPools = new SafeConcurrentDictionary<CharArrayPool>();
        private static readonly SafeConcurrentDictionary<ColumnPool<long>> bitvectorPools = new SafeConcurrentDictionary<ColumnPool<long>>();
        private static readonly SafeConcurrentDictionary<ColumnPoolBase> eventBatchPools = new SafeConcurrentDictionary<ColumnPoolBase>();
        private static readonly SafeConcurrentDictionary<object> memoryPools = new SafeConcurrentDictionary<object>();

        /// <summary>
        /// Maps pairs TKey, TPayload to the generated memory pool type
        /// </summary>
        private static readonly SafeConcurrentDictionary<Type> cachedMemoryPools = new SafeConcurrentDictionary<Type>();

        internal static DoublingArrayPool<T> GetDoublingArrayPool<T>()
            => (DoublingArrayPool<T>)doublingArrayPools.GetOrAdd(CacheKey.Create(typeof(T)), key => new DoublingArrayPool<T>());

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="size"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static ColumnPool<T> GetColumnPool<T>(int size = -1)
            => (ColumnPool<T>)columnPools.GetOrAdd(
                size < 0 || size == Config.DataBatchSize
                    ? CacheKey.Create(typeof(T))
                    : CacheKey.Create(typeof(T), size),
                key => new ColumnPool<T>(size < 0 ? Config.DataBatchSize : size));

        internal static CharArrayPool GetCharArrayPool()
            => charArrayPools.GetOrAdd(CacheKey.Create(), key => new CharArrayPool());

        internal static ColumnPool<long> GetBVPool(int size)
            => bitvectorPools.GetOrAdd(CacheKey.Create(size), key => new ColumnPool<long>(size));

        internal static StreamMessagePool<TKey, TPayload> GetStreamMessagePool<TKey, TPayload>(MemoryPool<TKey, TPayload> memoryPool, bool isColumnar)
            => (StreamMessagePool<TKey, TPayload>)eventBatchPools.GetOrAdd(
                CacheKey.Create(typeof(TKey), typeof(TPayload), isColumnar),
                key => new StreamMessagePool<TKey, TPayload>(memoryPool, isColumnar));

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="isColumnar"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static MemoryPool<TKey, TPayload> GetMemoryPool<TKey, TPayload>(bool isColumnar = true)
        {
            if (Config.ForceRowBasedExecution) isColumnar = false;

            var typeOfTKey = typeof(TKey);
            var typeOfTPayload = typeof(TPayload);

            var cacheKey = CacheKey.Create(typeof(TKey), typeof(TPayload), isColumnar);

            if (!isColumnar ||
                (!typeOfTKey.KeyTypeNeedsGeneratedMemoryPool() &&
                typeOfTPayload.MemoryPoolHasGetMethodFor()))
            {
                return (MemoryPool<TKey, TPayload>)memoryPools.GetOrAdd(cacheKey, key => new MemoryPool<TKey, TPayload>(isColumnar));
            }
            if (!typeOfTPayload.CanRepresentAsColumnar())
            {
                return (MemoryPool<TKey, TPayload>)memoryPools.GetOrAdd(cacheKey, key => new MemoryPool<TKey, TPayload>(false));
            }
            var lookupKey = CacheKey.Create(typeOfTKey, typeOfTPayload);

            var generatedMemoryPool = cachedMemoryPools.GetOrAdd(lookupKey, key => Transformer.GenerateMemoryPoolClass<TKey, TPayload>());

            return (MemoryPool<TKey, TPayload>)memoryPools.GetOrAdd(cacheKey, t => Activator.CreateInstance(generatedMemoryPool));
        }

        /// <summary>
        /// Free pooled memory resources (use in case of memory pressure).
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static void Free(bool reset = false)
        {
            foreach (var kvp in doublingArrayPools)
            {
                kvp.Value.Free(reset);
            }

            foreach (var kvp in columnPools)
            {
                kvp.Value.Free(reset);
            }

            foreach (var kvp in bitvectorPools)
            {
                kvp.Value.Free(reset);
            }

            foreach (var kvp in eventBatchPools)
            {
                kvp.Value.Free(reset);
            }

            foreach (var kvp in charArrayPools)
            {
                kvp.Value.Free(reset);
            }
        }

        /// <summary>
        /// Provide a human readable report of the status of all pools.
        /// </summary>
        /// <returns>A string reporting the status</returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static string GetStatusReport()
        {
            var ret = new System.Text.StringBuilder();
            ret.AppendLine("Column Pools");
            foreach (var kvp in columnPools)
            {
                ret.AppendLine(kvp.Value.GetStatusReport());
            }

            ret.AppendLine("Bit Vector Pools");
            foreach (var kvp in bitvectorPools)
            {
                ret.AppendLine(kvp.Value.GetStatusReport());
            }

            ret.AppendLine("Event Batch Pools");
            foreach (var kvp in eventBatchPools)
            {
                ret.AppendLine(kvp.Value.GetStatusReport());
            }

            return ret.ToString();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public struct MsgStats
        {
            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public int Count;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public int RealCount;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public int DataMsgs;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public long MinTicks;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public long MaxTicks;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public DateTime MinTime;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public DateTime MaxTime;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="msgs"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static MsgStats GetMsgStats<TKey, TPayload>(IEnumerable<StreamMessage<TKey, TPayload>> msgs)
        {
            var res = new MsgStats
            {
                Count = msgs.Sum(e => e.Count),
                DataMsgs = msgs.Count(),
                RealCount = msgs.Sum(e => e.ComputeCount()),
                MinTicks = msgs.Min(e => e.MinTimestamp),
                MaxTicks = msgs.Max(e => e.MaxTimestamp)
            };
            res.MaxTime = new DateTime(res.MaxTicks).ToUniversalTime();
            res.MinTime = new DateTime(res.MinTicks).ToUniversalTime();
            return res;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static IEnumerable<ColumnPoolBase> Leaked()
        {
            var result = new List<ColumnPoolBase>();
            foreach (var kvp in doublingArrayPools)
            {
                var l = kvp.Value.Leaked;
                if (l != null) result.Add(l);
            }
            foreach (var kvp in columnPools)
            {
                var l = kvp.Value.Leaked;
                if (l != null) result.Add(l);
            }
            foreach (var kvp in bitvectorPools)
            {
                var l = kvp.Value.Leaked;
                if (l != null) result.Add(l);
            }
            foreach (var kvp in eventBatchPools)
            {
                var l = kvp.Value.Leaked;
                if (l != null) result.Add(l);
            }
            return result;
        }
    }
}