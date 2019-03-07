#if COLUMNAR

// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Manager for producing StreamMessage instances on demand
    /// </summary>
    internal static class StreamMessageManager
    {
        /// <summary>
        /// Maps pair TKey,TPayload to the generated batch type
        /// </summary>
        private static readonly SafeConcurrentDictionary<Type> cachedObjects = new SafeConcurrentDictionary<Type>();
        public static IEnumerable<Type> GeneratedTypes()
        {
            var enumerator = cachedObjects.GetEnumerator();
            while (enumerator.MoveNext()) yield return enumerator.Current.Value;
        }

        public static Type GetStreamMessageType<TKey, TPayload>()
        {
            if (Config.ForceRowBasedExecution) return typeof(StreamMessage<TKey, TPayload>);
            if (!typeof(TPayload).CanRepresentAsColumnar()) return typeof(StreamMessage<TKey, TPayload>);

            var typeOfTKey = typeof(TKey);
            var typeOfTPayload = typeof(TPayload);

            var lookupKey = CacheKey.Create(typeOfTKey, typeOfTPayload);

            return cachedObjects.GetOrAdd(lookupKey, key => Transformer.GenerateBatchClass<TKey, TPayload>());
        }

        public static StreamMessage<TKey, TPayload> GetStreamMessage<TKey, TPayload>(MemoryPool<TKey, TPayload> pool)
        {
            if (Config.ForceRowBasedExecution) return new StreamMessage<TKey, TPayload>(pool);
            if (!typeof(TPayload).CanRepresentAsColumnar()) return new StreamMessage<TKey, TPayload>(pool);

            var generatedBatchType = GetStreamMessageType<TKey, TPayload>();

            var instance = Activator.CreateInstance(generatedBatchType, pool);
            var returnValue = (StreamMessage<TKey, TPayload>)instance;
            return returnValue;
        }
    }
}

#endif