// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Concurrent;
using System.IO;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing.Serializer
{
    internal abstract class BinaryBase
    {
        protected Stream stream;

        private static readonly ConcurrentDictionary<Tuple<Type, int>, object> columnPools = new ConcurrentDictionary<Tuple<Type, int>, object>();
        private static readonly ConcurrentDictionary<int, object> bitVectorPools = new ConcurrentDictionary<int, object>();

        protected BinaryBase(Stream stream) => this.stream = stream ?? throw new ArgumentNullException(nameof(stream));

        protected static ColumnBatch<T> AllocateColumnBatch<T>(int size)
        {
            if (size == Config.DataBatchSize)
            {
                var pool = (ColumnPool<T>)columnPools.GetOrAdd(Tuple.Create(typeof(T), Config.DataBatchSize), t => MemoryManager.GetColumnPool<T>(t.Item2));
                pool.Get(out var result);
                return result;
            }
            else if (typeof(T) == typeof(long) && size == (1 + (Config.DataBatchSize >> 6))) // bitvector
            {
                var pool = (ColumnPool<T>)bitVectorPools.GetOrAdd(size, t => MemoryManager.GetBVPool(size)); // TODO: Push magic incantation "1 + (Config.DataBatchSize >> 6)" into method call
                pool.Get(out var result);
                return result;
            }
            return new ColumnBatch<T>(size);
        }

        protected static T[] AllocateArray<T>(int size)
        {
            if (size == Config.DataBatchSize)
            {
                var pool = (ColumnPool<T>)columnPools.GetOrAdd(Tuple.Create(typeof(T), Config.DataBatchSize), t => MemoryManager.GetColumnPool<T>(t.Item2));
                pool.Get(out var result);
                return result.col;
            }
            else if (typeof(T) == typeof(long) && size == (1 + (Config.DataBatchSize >> 6))) // bitvector
            {
                var pool = (ColumnPool<T>)bitVectorPools.GetOrAdd(size, t => MemoryManager.GetBVPool(size)); // TODO: Push magic incantation "1 + (Config.DataBatchSize >> 6)" into method call
                pool.Get(out var result);
                return result.col;
            }
            return new T[size];
        }
    }
}
