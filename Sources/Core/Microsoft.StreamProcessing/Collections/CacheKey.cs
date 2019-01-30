// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace Microsoft.StreamProcessing.Internal.Collections
{
    internal class CacheKey
    {
        protected bool useMultiString;
        protected int batchSize;

        protected CacheKey()
        {
            this.useMultiString = Config.UseMultiString;
            this.batchSize = Config.DataBatchSize;
        }

        public static CacheKey Create() => new CacheKey();

        public static CacheKey<T> Create<T>(T keyData) => new CacheKey<T>(keyData);
        public static CacheKey<Tuple<T1, T2>> Create<T1, T2>(T1 t1, T2 t2) => new CacheKey<Tuple<T1, T2>>(Tuple.Create(t1, t2));
        public static CacheKey<Tuple<T1, T2, T3>> Create<T1, T2, T3>(T1 t1, T2 t2, T3 t3) => new CacheKey<Tuple<T1, T2, T3>>(Tuple.Create(t1, t2, t3));
        public static CacheKey<Tuple<T1, T2, T3, T4>> Create<T1, T2, T3, T4>(T1 t1, T2 t2, T3 t3, T4 t4) => new CacheKey<Tuple<T1, T2, T3, T4>>(Tuple.Create(t1, t2, t3, t4));
        public static CacheKey<Tuple<T1, T2, T3, T4, T5>> Create<T1, T2, T3, T4, T5>(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) => new CacheKey<Tuple<T1, T2, T3, T4, T5>>(Tuple.Create(t1, t2, t3, t4, t5));

        public override int GetHashCode() => this.batchSize ^ this.useMultiString.GetHashCode();

        public override bool Equals(object obj)
            => !(obj is CacheKey o)
                ? false
                : this.batchSize == o.batchSize && this.useMultiString == o.useMultiString;
    }

    internal sealed class CacheKey<T> : CacheKey
    {
        private T keyData;

        public CacheKey(T keyData) => this.keyData = keyData;

        public override int GetHashCode() => this.keyData.GetHashCode() ^ this.batchSize ^ this.useMultiString.GetHashCode();

        public override bool Equals(object obj)
            => !(obj is CacheKey<T> o)
                ? false
                : this.batchSize == o.batchSize && this.useMultiString == o.useMultiString && this.keyData.Equals(o.keyData);
    }
}
