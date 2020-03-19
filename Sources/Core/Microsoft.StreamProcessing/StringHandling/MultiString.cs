#define UNSAFE
// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Text.RegularExpressions;

namespace Microsoft.StreamProcessing.Internal.Collections
{
    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public sealed class MultiString : IDisposable
    {
        [DataMember]
        internal CharArrayWrapper col;
        [DataMember]
        private ColumnBatch<int> starts;
        [DataMember]
        private ColumnBatch<short> lengths;
        [DataMember]
        private string stage;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int Count;

        [DataMember]
        internal int EndOffset;

        [DataMember]
        internal int MaxStringSize;

        [DataMember]
        private MyStringBuilder msb;

        private readonly CharArrayPool charArrayPool;
        private readonly ColumnPool<int> intPool;
        private readonly ColumnPool<short> shortPool;
        private readonly ColumnPool<long> bitvectorPool;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataContract]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public enum MultiStringState
        {
            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            Unsealed,

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            Sealed,

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            Disposed,
        }

        [DataMember]
        private MultiStringState state;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public MultiStringState State => this.state;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public MultiString()
            : this(MemoryManager.GetCharArrayPool(), MemoryManager.GetColumnPool<int>(), MemoryManager.GetColumnPool<short>(), MemoryManager.GetBVPool(1 + (Config.DataBatchSize >> 6)))
        { }

        internal MultiString(CharArrayPool caPool, ColumnPool<int> intPool, ColumnPool<short> shortPool, ColumnPool<long> bitvectorPool)
        {
            Contract.Ensures(this.State == MultiStringState.Unsealed);

            this.charArrayPool = caPool;
            this.intPool = intPool;
            this.shortPool = shortPool;
            this.bitvectorPool = bitvectorPool;

            this.Count = 0;
            this.MaxStringSize = 0;
            this.EndOffset = 0;
            this.state = MultiStringState.Unsealed;
        }

        #region Relational Operators

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="inBV"></param>
        /// <param name="pool"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static unsafe ColumnBatch<long> GreaterThan(ColumnBatch<int> left, int right, ColumnBatch<long> inBV, ColumnPool<long> pool)
        {
            pool.Get(out var result);

            var bv = inBV.col;
            var leftCol = left.col;
            var resultCol = result.col;

            for (int i = 0; i < left.UsedLength; i++)
            {
                if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                {
                    if (!(leftCol[i] > right))
                    {
                        resultCol[i >> 6] |= 1L << (i & 0x3f);
                    }
                }
            }
            return result;
        }
        #endregion

        #region Arithmetic Operators

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="inBV"></param>
        /// <param name="pool"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static unsafe ColumnBatch<int> Subtract(ColumnBatch<int> left, int right, ColumnBatch<long> inBV, ColumnPool<int> pool)
        {
            pool.Get(out var result);

            fixed (long* bv = inBV.col)
            fixed (int* leftCol = left.col)
            fixed (int* resultCol = result.col)
                for (int i = 0; i < left.UsedLength; i++)
                {
                    if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                    {
                        resultCol[i] = leftCol[i] - right;
                    }
                }
            return result;
        }
        #endregion

        #region BitVector Operators
#if DEBUG
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="bv"></param>
        /// <param name="i"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static bool IsRowAlive(ColumnBatch<long> bv, int i)
            => (bv.col[i >> 6] & (1L << (i & 0x3f))) == 0;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="bv"></param>
        /// <param name="n"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static bool[] LiveRows(ColumnBatch<long> bv, int n)
        {
            var result = new bool[n];
            for (int i = 0; i < n; i++)
                result[i] = IsRowAlive(bv, i);
            return result;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="bv"></param>
        /// <param name="n"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static ICollection<int> LiveIndices(ColumnBatch<long> bv, int n)
        {
            var result = new List<int>();
            for (int i = 0; i < n; i++)
                if (IsRowAlive(bv, i)) result.Add(i);
            return result;

        }
#endif

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="pool"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static unsafe ColumnBatch<long> InvertLeftThenOrWithRight(ColumnBatch<long> left, ColumnBatch<long> right, ColumnPool<long> pool)
        {
            Contract.Requires(left != null);
            Contract.Requires(right != null);
            Contract.Requires(left.UsedLength == right.UsedLength);

            pool.Get(out var result);

            fixed (long* leftCol = left.col)
            fixed (long* rightCol = right.col)
            fixed (long* resultCol = result.col)
            {
                for (int i = 0; i < left.UsedLength; i++)
                {
                    resultCol[i] = ~leftCol[i] | rightCol[i];
                }
            }
            result.UsedLength = left.UsedLength;
            return result;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static unsafe void AndEquals(ColumnBatch<long> left, ColumnBatch<long> right)
        {
            Contract.Requires(left != null);
            Contract.Requires(right != null);
            Contract.Requires(left.UsedLength == right.UsedLength);

            fixed (long* leftCol = left.col)
            fixed (long* rightCol = right.col)
            {
                for (int i = 0; i < left.UsedLength; i++)
                {
                    leftCol[i] &= rightCol[i];
                }
            }
            return;
        }

        #endregion

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// Clone the multi-string shell only
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public MultiString CloneShell()
        {
            Contract.Requires(this.State != MultiStringState.Disposed);
            Contract.Ensures(Contract.Result<MultiString>().State == MultiStringState.Unsealed);

            return new MultiString(this.charArrayPool, this.intPool, this.shortPool, this.bitvectorPool);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// Clone the multi-string, refcounting the bulky contents
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public MultiString Clone()
        {
            Contract.Requires(this.State == MultiStringState.Sealed);
            Contract.Ensures(this.State == MultiStringState.Sealed);
            Contract.Ensures(Contract.Result<MultiString>().State == MultiStringState.Sealed);

            var result = new MultiString(this.charArrayPool, this.intPool, this.shortPool, this.bitvectorPool)
            {
                Count = this.Count,
                EndOffset = this.EndOffset,
                MaxStringSize = this.MaxStringSize
            };

            this.starts.IncrementRefCount(1);
            result.starts = this.starts;

            this.lengths.IncrementRefCount(1);
            result.lengths = this.lengths;

            this.col.IncrementRefCount(1);
            result.col = this.col;

            // clone does not share the stage
            char[] temp = new char[this.MaxStringSize];
            result.stage = new string(temp);

            result.state = MultiStringState.Sealed;

            return result;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Dispose()
        {
            Contract.Requires(this.State != MultiStringState.Disposed);
            Contract.Ensures(this.State == MultiStringState.Disposed);

            if (this.msb != null) this.msb.Dispose();

            if (this.col != null) this.col.Return();
            if (this.starts != null) this.starts.Return();
            if (this.lengths != null) this.lengths.Return();
            this.state = MultiStringState.Disposed;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="str"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void AddString(string str)
        {
            Contract.Requires(this.State == MultiStringState.Unsealed);
            Contract.Ensures(this.State == MultiStringState.Unsealed);

            if (str == null) str = string.Empty;
            Initialize(-1);

            this.msb.Append(str);
            this.starts.col[this.Count] = this.EndOffset;
            var len = str.Length;
            if (len > 1 << 15) throw new InvalidOperationException("String too long");
            this.lengths.col[this.Count] = (short)len;
            this.EndOffset += len;
            this.Count++;
            if (len > this.MaxStringSize) this.MaxStringSize = len;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="size"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Initialize(int size = -1)
        {
            if (this.starts == null) this.intPool.Get(out this.starts);

            if (this.lengths == null) this.shortPool.Get(out this.lengths);

            if (this.msb == null)
            {
                this.msb = size < 1
                    ? new MyStringBuilder(this.charArrayPool)
                    : new MyStringBuilder(size, this.charArrayPool);
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="other"></param>
        /// <param name="index"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe void AddString(MultiString other, int index)
        {
            Contract.Requires(this.State == MultiStringState.Unsealed);
            Contract.Requires(other != null);
            Contract.Requires(other.State == MultiStringState.Sealed);
            Contract.Requires(index >= 0 && index < other.Count);

            Contract.Ensures(this.State == MultiStringState.Unsealed);

            Contract.Assume(this.starts != null);
            Contract.Assume(this.lengths != null);
            Contract.Assume(this.msb != null);

            var otherLength = other.lengths.col[index];
            fixed (char* c = &other.col.charArray.content[other.starts.col[index]])
            {
                this.msb.Append(c, otherLength);
            }

            this.starts.col[this.Count] = this.EndOffset;
            this.lengths.col[this.Count] = otherLength;
            this.EndOffset += otherLength;
            this.Count++;
            if (otherLength > this.MaxStringSize) this.MaxStringSize = otherLength;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe MultiString FromColumnBatch(ColumnBatch<string> columnOfStrings, ColumnBatch<long> livenessBitVector, CharArrayPool caPool, ColumnPool<int> intPool, ColumnPool<short> shortPool, ColumnPool<long> bitvectorPool)
        {
            Contract.Requires(columnOfStrings != null);
            Contract.Requires(livenessBitVector != null);
            Contract.Requires(caPool != null);
            Contract.Requires(intPool != null);
            Contract.Requires(bitvectorPool != null);
            Contract.Ensures(Contract.Result<MultiString>().State == MultiStringState.Sealed);

            var multiString = new MultiString(caPool, intPool, shortPool, bitvectorPool);

            var col = columnOfStrings.col;
            fixed (long* src_bv = livenessBitVector.col)
            {
                int totalLength = 0;
                for (int i = 0; i < columnOfStrings.UsedLength; i++)
                {
                    if ((src_bv[i >> 6] & (1L << (i & 0x3f))) != 0) continue;
                    totalLength += col[i] == null ? 0 : col[i].Length;
                }

                multiString.Initialize(totalLength);

                for (int i = 0; i < columnOfStrings.UsedLength; i++)
                {
                    if ((src_bv[i >> 6] & (1L << (i & 0x3f))) != 0) continue;
                    multiString.AddString(col[i]);
                }
            }
            multiString.Seal();
            return multiString;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="stringPool"></param>
        /// <param name="livenessBitVector"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe ColumnBatch<string> ToColumnBatch(ColumnPool<string> stringPool, ColumnBatch<long> livenessBitVector)
        {
            Contract.Requires(stringPool != null);
            Contract.Requires(this.State == MultiStringState.Sealed);

            stringPool.Get(out var stringColumn);
            var col = stringColumn.col;
            var n = this.Count;
            fixed (long* src_bv = livenessBitVector.col)
            {
                for (int i = 0; i < n; i++)
                {
                    if ((src_bv[i >> 6] & (1L << (i & 0x3f))) != 0) continue;
                    col[i] = this.col.charArray.contentString.Substring(this.starts.col[i] + 2, this.lengths.col[i]);
                }
            }
            stringColumn.UsedLength = n;
            return stringColumn;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Seal()
        {
            Contract.Ensures(this.State == MultiStringState.Sealed);

            if (this.col == null) // null check so method is idempotent
            {
                Initialize(-1); // degenerate case: no string was ever added

                this.col = this.msb.ToCharArrayWrapperAndDispose();
                this.msb = null;

                // clone does not share the stage
                char[] temp = new char[this.MaxStringSize];
                this.stage = new string(temp);
                this.starts.UsedLength = this.Count;
                this.lengths.UsedLength = this.Count;
            }

            this.state = MultiStringState.Sealed;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public string this[int index]
        {
            get
            {
                Contract.Requires(this.State == MultiStringState.Sealed);
                Contract.Ensures(this.State == MultiStringState.Sealed);

                return this.col.charArray.contentString.Substring(this.starts.col[index] + 2, this.lengths.col[index]);
            }
        }

        #region Apply Operations

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// Apply a boolean expression, returns a bitvector (e.g., StartsWith)
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="inBV"></param>
        /// <param name="inPlace"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe ColumnBatch<long> ApplyBoolean(Expression<Func<string, bool>> expression, ColumnBatch<long> inBV, bool inPlace = true)
        {
            Contract.Requires(this.State == MultiStringState.Sealed);
            Contract.Ensures(this.State == MultiStringState.Sealed);

            ColumnBatch<long> result;

            if (inPlace)
                result = inBV.MakeWritable(this.bitvectorPool);
            else
            {
                this.bitvectorPool.Get(out result);
                Array.Copy(inBV.col, result.col, inBV.col.Length);
                result.UsedLength = inBV.UsedLength;
            }
            var func = expression.Compile();
            var bv = inBV.col;
            var rbv = result.col;
            var startscol = this.starts.col;
            var lengthscol = this.lengths.col;

            fixed (char* dest = this.stage)
            {
                fixed (char* src = this.col.charArray.content)
                {
                    int orig = *((int*)dest - 1);
                    for (int i = 0; i < this.Count; i++)
                    {
                        if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                        {
                            // Copy string from col to stage
                            MyStringBuilder.Wstrcpy(dest, src + startscol[i], lengthscol[i] + 1);
                            *((int*)dest - 1) = lengthscol[i];
                            *(dest + lengthscol[i]) = '\0';
                            if (!func(this.stage))
                            {
                                rbv[i >> 6] |= (1L << (i & 0x3f));
                            }
                        }
                    }
                    *((int*)dest - 1) = orig;
                }
            }
            return result;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// Apply an expression that returns non-string
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="expression"></param>
        /// <param name="tPool"></param>
        /// <param name="tBatch"></param>
        /// <param name="inBV"></param>
        /// <param name="inPlace"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe ColumnBatch<TResult> Apply<TResult>(Expression<Func<string, TResult>> expression, ColumnPool<TResult> tPool, ColumnBatch<TResult> tBatch, ColumnBatch<long> inBV, bool inPlace = true)
        {
            Contract.Requires(this.State == MultiStringState.Sealed);
            Contract.Ensures(this.State == MultiStringState.Sealed);

            ColumnBatch<TResult> result;

            if (inPlace)
                result = tBatch.MakeWritable(tPool);
            else
                tPool.Get(out result);

            var func = expression.Compile();
            var bv = inBV.col;
            var ret = result.col;
            var startscol = this.starts.col;
            var lengthscol = this.lengths.col;

            fixed (char* dest = this.stage)
            {
                fixed (char* src = this.col.charArray.content)
                {
                    int orig = *((int*)dest - 1);
                    for (int i = 0; i < this.Count; i++)
                    {
                        if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                        {
                            // Copy string from col to stage
                            MyStringBuilder.Wstrcpy(dest, src + startscol[i], lengthscol[i]);
                            *((int*)dest - 1) = lengthscol[i];
                            *(dest + lengthscol[i]) = '\0';
                            ret[i] = func(this.stage);
                        }
                    }
                    *((int*)dest - 1) = orig;
                }
            }
            return result;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// Apply an expression that returns another single string (e.g., Substring) per input stream
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="inBV"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe MultiString ApplyString(Expression<Func<string, string>> expression, ColumnBatch<long> inBV)
        {
            Contract.Requires(this.State == MultiStringState.Sealed);
            Contract.Ensures(this.State == MultiStringState.Sealed);

            MultiString result = CloneShell();
            var func = expression.Compile();
            var bv = inBV.col;
            var startscol = this.starts.col;
            var lengthscol = this.lengths.col;

            fixed (char* dest = this.stage)
            {
                fixed (char* src = this.col.charArray.content)
                {
                    int orig = *((int*)dest - 1);
                    for (int i = 0; i < this.Count; i++)
                    {
                        if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                        {
                            // Copy string from col to stage
                            MyStringBuilder.Wstrcpy(dest, src + startscol[i], lengthscol[i] + 1);
                            *((int*)dest - 1) = lengthscol[i];
                            *(dest + lengthscol[i]) = '\0';
                            result.AddString(func(this.stage));
                        }
                    }
                    *((int*)dest - 1) = orig;
                }
            }
            result.Seal();
            return result;
        }
        #endregion

        #region Contains

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="str"></param>
        /// <param name="inBV"></param>
        /// <param name="inPlace"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe ColumnBatch<long> Contains(string str, ColumnBatch<long> inBV, bool inPlace = true)
        {
            Contract.Requires(this.State == MultiStringState.Sealed);
            Contract.Ensures(this.State == MultiStringState.Sealed);

            var reg = new Regex(Regex.Escape(str));
            return IsMatch(reg, 0, inBV, inPlace);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="str"></param>
        /// <param name="inBV"></param>
        /// <param name="inPlace"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe ColumnBatch<long> Contains2(string str, ColumnBatch<long> inBV, bool inPlace = true)
        {
            Contract.Requires(this.State == MultiStringState.Sealed);
            Contract.Ensures(this.State == MultiStringState.Sealed);

            ColumnBatch<long> result;

            if (inPlace)
                result = inBV.MakeWritable(this.bitvectorPool);
            else
            {
                this.bitvectorPool.Get(out result);
                Array.Copy(inBV.col, result.col, inBV.col.Length);
                result.UsedLength = inBV.UsedLength;
            }
            var bv = inBV.col;
            var rbv = result.col;
            var startscol = this.starts.col;
            var lengthscol = this.lengths.col;
            var largestr = this.col.charArray.contentString;

            for (int i = 0; i < this.Count; i++)
            {
                if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                {
                    if (!(largestr.IndexOf(str, startscol[i] + 2, lengthscol[i], StringComparison.Ordinal) >= 0))
                    {
                        rbv[i >> 6] |= (1L << (i & 0x3f));
                    }
                }
            }

            return result;
        }

        #endregion

        #region IndexOf and LastIndexOf

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="str"></param>
        /// <param name="startIndex"></param>
        /// <param name="count"></param>
        /// <param name="comparisonType"></param>
        /// <param name="inBV"></param>
        /// <param name="ignore"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe ColumnBatch<int> IndexOf(string str, int startIndex, int count, StringComparison comparisonType, ColumnBatch<long> inBV, bool ignore)
        {
            Contract.Requires(this.State == MultiStringState.Sealed);
            Contract.Ensures(this.State == MultiStringState.Sealed);

            this.intPool.Get(out var result);

            var bv = inBV.col;
            var startscol = this.starts.col;
            var largestr = this.col.charArray.contentString;
            var resultcol = result.col;

            for (int i = 0; i < this.Count; i++)
            {
                if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                {
                    var startIndexInBigString = startscol[i] + 2;
                    var realResult = largestr.IndexOf(str, startIndexInBigString + startIndex, count, comparisonType);
                    resultcol[i] = realResult == -1 ? -1 : realResult - startIndexInBigString;
                }
            }
            result.UsedLength = this.Count;
            return result;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="str"></param>
        /// <param name="startIndex"></param>
        /// <param name="comparisonType"></param>
        /// <param name="inBV"></param>
        /// <param name="ignore"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe ColumnBatch<int> IndexOf(string str, int startIndex, StringComparison comparisonType, ColumnBatch<long> inBV, bool ignore)
        {
            Contract.Requires(this.State == MultiStringState.Sealed);
            Contract.Ensures(this.State == MultiStringState.Sealed);

            this.intPool.Get(out var result);

            var bv = inBV.col;
            var startscol = this.starts.col;
            var lengthscol = this.lengths.col;
            var largestr = this.col.charArray.contentString;
            var resultcol = result.col;

            for (int i = 0; i < this.Count; i++)
            {
                if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                {
                    var startIndexInBigString = startscol[i] + 2;
                    var realResult = largestr.IndexOf(str, startIndexInBigString + startIndex, lengthscol[i] - startIndex, comparisonType);
                    resultcol[i] = realResult == -1 ? -1 : realResult - startIndexInBigString;
                }
            }
            result.UsedLength = this.Count;
            return result;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="str"></param>
        /// <param name="startIndex"></param>
        /// <param name="count"></param>
        /// <param name="comparisonType"></param>
        /// <param name="inBV"></param>
        /// <param name="ignore"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe ColumnBatch<int> LastIndexOf(string str, int startIndex, int count, StringComparison comparisonType, ColumnBatch<long> inBV, bool ignore)
        {
            Contract.Requires(this.State == MultiStringState.Sealed);
            Contract.Ensures(this.State == MultiStringState.Sealed);

            this.intPool.Get(out var result);

            var bv = inBV.col;
            var startscol = this.starts.col;
            var largestr = this.col.charArray.contentString;
            var resultcol = result.col;

            for (int i = 0; i < this.Count; i++)
            {
                if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                {
                    var startIndexInBigString = startscol[i] + 2;
                    var realResult = largestr.LastIndexOf(str, startIndexInBigString + startIndex, count, comparisonType);
                    resultcol[i] = realResult == -1 ? -1 : realResult - startIndexInBigString;
                }
            }
            result.UsedLength = this.Count;
            return result;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="str"></param>
        /// <param name="startIndex"></param>
        /// <param name="comparisonType"></param>
        /// <param name="inBV"></param>
        /// <param name="ignore"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe ColumnBatch<int> LastIndexOf(string str, int startIndex, StringComparison comparisonType, ColumnBatch<long> inBV, bool ignore)
        {
            Contract.Requires(this.State == MultiStringState.Sealed);
            Contract.Ensures(this.State == MultiStringState.Sealed);

            this.intPool.Get(out var result);

            var bv = inBV.col;
            var startscol = this.starts.col;
            var lengthscol = this.lengths.col;
            var largestr = this.col.charArray.contentString;
            var resultcol = result.col;

            for (int i = 0; i < this.Count; i++)
            {
                if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                {
                    var startIndexInBigString = startscol[i] + 2;
                    var realResult = largestr.LastIndexOf(str, startIndexInBigString + startIndex, lengthscol[i] - startIndex, comparisonType);
                    resultcol[i] = realResult == -1 ? -1 : realResult - startIndexInBigString;
                }
            }
            result.UsedLength = this.Count;
            return result;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="str"></param>
        /// <param name="startIndices"></param>
        /// <param name="comparisonType"></param>
        /// <param name="inBV"></param>
        /// <param name="ignore"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe ColumnBatch<int> LastIndexOf(string str, ColumnBatch<int> startIndices, StringComparison comparisonType, ColumnBatch<long> inBV, bool ignore)
        {
            Contract.Requires(this.State == MultiStringState.Sealed);
            Contract.Ensures(this.State == MultiStringState.Sealed);

            this.intPool.Get(out var result);

            var bv = inBV.col;
            var startscol = this.starts.col;
            var lengthscol = this.lengths.col;
            var largestr = this.col.charArray.contentString;
            var resultcol = result.col;

            fixed (int* startIndicesCol = startIndices.col)
            {
                for (int i = 0; i < this.Count; i++)
                {
                    if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                    {
                        var startIndexInBigString = startscol[i] + 2;
                        var startIndex = startIndicesCol[i];
                        var realResult = largestr.LastIndexOf(str, startIndexInBigString + startIndex, lengthscol[i] - startIndex, comparisonType);
                        resultcol[i] = realResult == -1 ? -1 : realResult - startIndexInBigString;
                    }
                }
            }
            result.UsedLength = this.Count;
            return result;
        }

        #endregion

        #region RegEx

        private static bool IsSimpleRegex(string pattern)
        {
            // ignore escaped quantifiers
            pattern = pattern.Replace("\\*", "A");
            pattern = pattern.Replace("\\+", "A");
            pattern = pattern.Replace("\\?", "A");
            pattern = pattern.Replace("\\}", "A");
            pattern = pattern.Replace("\\{", "A");

            var isGreedy = new Regex(@"([\*\+\}](?!\?))|([^\*\+\}\?]\?(?!\?))");

            return !isGreedy.IsMatch(pattern)
                && !pattern.StartsWith("^", StringComparison.Ordinal)
                && !pattern.EndsWith("$", StringComparison.Ordinal)
                && !pattern.Contains("\\A")
                && !pattern.Contains("\\Z")
                && !pattern.Contains("\\z");
        }

        private unsafe ColumnBatch<long> IsMatchBackoff(Regex regex, int startat, ColumnBatch<long> inBV, bool inPlace = true)
        {
            if ((regex.Options & RegexOptions.RightToLeft) == RegexOptions.RightToLeft)
            {
                return ApplyBoolean(e => regex.IsMatch(e), inBV, inPlace);
            }

            ColumnBatch<long> result;

            if (inPlace)
                result = inBV.MakeWritable(this.bitvectorPool);
            else
            {
                this.bitvectorPool.Get(out result);
                Array.Copy(inBV.col, result.col, inBV.col.Length);
                result.UsedLength = inBV.UsedLength;
            }

            var bv = inBV.col;
            var rbv = result.col;
            var startscol = this.starts.col;
            var lengthscol = this.lengths.col;
            var largestr = this.col.charArray.contentString;

            fixed (char* src = this.col.charArray.content)
            {
                for (int i = 0; i < this.Count; i++)
                {
                    if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                    {
                        if (!regex.Match(largestr, startscol[i] + 2 + startat, lengthscol[i] - startat).Success)
                        {
                            rbv[i >> 6] |= (1L << (i & 0x3f));
                        }
                    }
                }
            }
            return result;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="regex"></param>
        /// <param name="inBV"></param>
        /// <param name="inPlace"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe ColumnBatch<long> IsMatch(string regex, ColumnBatch<long> inBV, bool inPlace = true)
        {
            Contract.Requires(this.State == MultiStringState.Sealed);
            Contract.Ensures(this.State == MultiStringState.Sealed);

            var reg = new Regex(regex);
            return IsMatch(reg, 0, inBV, inPlace);
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="regex"></param>
        /// <param name="startat"></param>
        /// <param name="inBV"></param>
        /// <param name="inPlace"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe ColumnBatch<long> IsMatch(Regex regex, int startat, ColumnBatch<long> inBV, bool inPlace = true)
        {
            Contract.Requires(this.State == MultiStringState.Sealed);
            Contract.Ensures(this.State == MultiStringState.Sealed);

            if ((regex.Options & RegexOptions.RightToLeft) == RegexOptions.RightToLeft)
            {
                return ApplyBoolean(e => regex.IsMatch(e), inBV, false);
            }

            if (!IsSimpleRegex(regex.ToString()))
            {
                return IsMatchBackoff(regex, startat, inBV, false);
            }

            ColumnBatch<long> result;

            if (inPlace)
                result = inBV.MakeWritable(this.bitvectorPool);
            else
            {
                this.bitvectorPool.Get(out result);
                Array.Copy(inBV.col, result.col, inBV.col.Length);
                result.UsedLength = inBV.UsedLength;
            }

            var bv = inBV.col;
            var rbv = result.col;
            var startscol = this.starts.col;
            var lengthscol = this.lengths.col;
            var largestr = this.col.charArray.contentString;

            int cur_i = 0;

            fixed (char* src = this.col.charArray.content)
            {
                Match m = regex.Match(largestr, 2, this.col.UsedLength - 2);

                while (m.Success)
                {
                    int pattern_startoffset = m.Index - 2; // position of the first character in the match
                    int pattern_endoffset = m.Index - 2 + m.Length - 1; // position of the last character in the match

                    while ((cur_i < this.starts.UsedLength - 1) && (startscol[cur_i + 1] <= pattern_startoffset))
                    {
                        rbv[cur_i >> 6] |= (1L << (cur_i & 0x3f));
                        cur_i++;
                    }

                    int start_index = cur_i;
                    while ((cur_i < this.starts.UsedLength - 1) && (startscol[cur_i + 1] <= pattern_endoffset))
                    {
                        rbv[cur_i >> 6] |= (1L << (cur_i & 0x3f));
                        cur_i++;
                    }
                    int end_index = cur_i;

                    if ((cur_i == this.starts.UsedLength - 1) && (startscol[cur_i] + lengthscol[cur_i] <= pattern_endoffset)) // if last entry
                    {
                        rbv[cur_i >> 6] |= (1L << (cur_i & 0x3f));
                        break;
                    }

                    if ((bv[end_index >> 6] & (1L << (end_index & 0x3f))) == 0)
                    {
                        if (start_index != end_index)
                        {
                            var newM = regex.Match(largestr, startscol[end_index] + 2 + startat, lengthscol[end_index] - startat);
                            if (!newM.Success)
                            {
                                rbv[end_index >> 6] |= (1L << (end_index & 0x3f));
                            }
                        }
                    }

                    cur_i++;
                    if (cur_i >= this.starts.UsedLength) break;
                    m = regex.Match(largestr, startscol[cur_i] + 2 + startat, this.col.UsedLength - (startscol[cur_i] + 2 + startat));
                }

                while (cur_i < this.starts.UsedLength)
                {
                    rbv[cur_i >> 6] |= (1L << (cur_i & 0x3f));
                    cur_i++;
                }
            }
            return result;
        }

        #endregion

        #region Case Manipulation

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="inPlace"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe MultiString ToLower(bool inPlace = true)
        {
            Contract.Requires(this.State == MultiStringState.Sealed);
            Contract.Ensures(this.State == MultiStringState.Sealed);

            var textInfo = CultureInfo.CurrentCulture.TextInfo;
            if (inPlace)
            {
                fixed (char* src = this.col.charArray.content)
                {
                    char* p = src;
                    char* end = src + this.EndOffset;
                    while (p < end)
                    {
                        if (*p < '\x0080') // ascii
                        {
                            if ((*p <= 'Z') && (*p >= 'A'))
                            {
                                *p = (char)(*p | ' ');
                            }
                        }
                        else
                        {
                            *p = textInfo.ToLower(*p);
                        }
                        p++;
                    }
                }
                return this;
            }
            else
            {
                MultiString result = Clone();
                result.col.MakeWritable(result.charArrayPool, false);

                fixed (char* src = this.col.charArray.content)
                {
                    fixed (char* dest = result.col.charArray.content)
                    {
                        char* p = src;
                        char* q = dest;
                        char* end = src + this.EndOffset;
                        while (p < end)
                        {
                            *q = *p < '\x0080' ? (*p <= 'Z') && (*p >= 'A') ? (char)(*p | ' ') : *p : textInfo.ToLower(*p);
                            p++;
                            q++;
                        }
                    }
                }
                return result;
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="inPlace"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe MultiString ToUpper(bool inPlace = true)
        {
            Contract.Requires(this.State == MultiStringState.Sealed);
            Contract.Ensures(this.State == MultiStringState.Sealed);

            var textInfo = CultureInfo.CurrentCulture.TextInfo;
            if (inPlace)
            {
                fixed (char* src = this.col.charArray.content)
                {
                    char* p = src;
                    char* end = src + this.EndOffset;
                    while (p < end)
                    {
                        if (*p < '\x0080') // ascii
                        {
                            if ((*p >= 'a') && (*p <= 'z'))
                            {
                                *p = (char)(*p & '￟');
                            }
                        }
                        else
                        {
                            *p = textInfo.ToUpper(*p);
                        }
                        p++;
                    }
                }
                return this;
            }
            else
            {
                MultiString result = Clone();
                result.col.MakeWritable(result.charArrayPool, false);

                fixed (char* src = this.col.charArray.content)
                {
                    fixed (char* dest = result.col.charArray.content)
                    {
                        char* p = src;
                        char* q = dest;
                        char* end = src + this.EndOffset;
                        while (p < end)
                        {
                            *q = *p < '\x0080'
                                ? (*p >= 'a') && (*p <= 'z') ? (char)(*p & '￟') : *p
                                : textInfo.ToUpper(*p);
                            p++;
                            q++;
                        }
                    }
                }
                return result;
            }
        }
        #endregion

        #region Split

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="regex"></param>
        /// <param name="inBV"></param>
        /// <param name="multiplicity"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe ICollection<MultiString> Split(Regex regex, ColumnBatch<long> inBV, out ColumnBatch<int> multiplicity)
        {
            Contract.Requires(this.State == MultiStringState.Sealed);
            Contract.Ensures(this.State == MultiStringState.Sealed);

            if ((regex.Options & RegexOptions.RightToLeft) == RegexOptions.RightToLeft)
                throw new NotSupportedException("Right-to-left is not supported for split");

            if (!IsSimpleRegex(regex.ToString()))
                throw new NotSupportedException("Split is not currently supported with this RegEx");

            var resultList = new List<MultiString>();
            this.intPool.Get(out multiplicity);

            var current = Clone();
            current.Count = 0;
            current.starts = current.starts.MakeWritable(this.intPool);
            current.lengths = current.lengths.MakeWritable(this.shortPool);
            var rstartscol = current.starts.col;
            var rlengthscol = current.lengths.col;

            var bv = inBV.col;

            var startscol = this.starts.col;
            var lengthscol = this.lengths.col;
            var largestr = this.col.charArray.contentString;

            // start match at the next active bit
            int end_index = 0;
            Match m = null;
            while (((bv[end_index >> 6] & (1L << (end_index & 0x3f))) != 0) && (end_index < startscol.Length))
            {
                end_index++;
            }
            if (end_index < startscol.Length)
            {
                m = regex.Match(largestr, startscol[end_index] + 2, this.col.UsedLength - (startscol[end_index] + 2));
            }

            while ((m != null) && m.Success)
            {
                int pattern_startoffset = m.Index - 2; // position of the first character in the match
                int pattern_endoffset = m.Index - 2 + m.Length - 1; // position of the last character in the match

                while ((end_index < this.starts.UsedLength - 1) && (startscol[end_index + 1] <= pattern_startoffset))
                {
                    // report full string match
                    rstartscol[current.Count] = startscol[end_index];
                    rlengthscol[current.Count] = lengthscol[end_index];
                    current.Count++;
                    if (current.Count == rstartscol.Length)
                    {
                        resultList.Add(current);
                        current = Clone();
                        current.Count = 0;
                        current.starts = current.starts.MakeWritable(this.intPool);
                        current.lengths = current.lengths.MakeWritable(this.shortPool);
                        rstartscol = current.starts.col;
                        rlengthscol = current.lengths.col;
                    }
                    multiplicity.col[end_index] = 1;
                    end_index++;
                }
                int start_index = end_index;
                while ((end_index < this.starts.UsedLength - 1) && (startscol[end_index + 1] <= pattern_endoffset))
                {
                    // report full string match
                    rstartscol[current.Count] = startscol[end_index];
                    rlengthscol[current.Count] = lengthscol[end_index];
                    current.Count++;
                    if (current.Count == rstartscol.Length)
                    {
                        resultList.Add(current);
                        current = Clone();
                        current.Count = 0;
                        current.starts = current.starts.MakeWritable(this.intPool);
                        current.lengths = current.lengths.MakeWritable(this.shortPool);
                        rstartscol = current.starts.col;
                        rlengthscol = current.lengths.col;
                    }
                    multiplicity.col[end_index] = 1;
                    end_index++;
                }

                if ((end_index == this.starts.UsedLength - 1) && (startscol[end_index] + lengthscol[end_index] <= pattern_endoffset)) // if last entry
                {
                    break;
                }

                if ((bv[end_index >> 6] & (1L << (end_index & 0x3f))) == 0)
                {
                    if (start_index == end_index)
                    {
                        int prev_end = startscol[start_index];
                        bool done = false;
                        int localcount = 0;
                        while (true)
                        {
                            if ((!m.Success) || (startscol[end_index] + lengthscol[end_index] < m.Index - 2 + m.Length)) done = true;
                            rstartscol[current.Count] = prev_end;
                            rlengthscol[current.Count] = (short)Math.Min(m.Index - 2 - prev_end, startscol[end_index] + lengthscol[end_index] - prev_end);
                            current.Count++;
                            localcount++;
                            if (current.Count == rstartscol.Length)
                            {
                                resultList.Add(current);
                                current = Clone();
                                current.Count = 0;
                                current.starts = current.starts.MakeWritable(this.intPool);
                                current.lengths = current.lengths.MakeWritable(this.shortPool);
                                rstartscol = current.starts.col;
                                rlengthscol = current.lengths.col;
                            }
                            if (done) break;
                            prev_end = m.Index - 2 + m.Length;
                            m = m.NextMatch();
                        }
                        multiplicity.col[end_index] = localcount;
                        end_index++;
                    }
                    else
                    {
                        m = regex.Match(largestr, startscol[end_index] + 2, this.col.UsedLength - (startscol[end_index] + 2));
                    }
                }
                else
                {
                    // restart match at the next active bit
                    while (((bv[end_index >> 6] & (1L << (end_index & 0x3f))) != 0) && (end_index < startscol.Length))
                    {
                        end_index++;
                    }
                    if (end_index < startscol.Length)
                    {
                        m = regex.Match(largestr, startscol[end_index] + 2, this.col.UsedLength - (startscol[end_index] + 2));
                    }
                }
            }

            if (current.Count > 0)
            {
                resultList.Add(current);
            }
            else
            {
                current.Dispose();
            }
            return resultList;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="separator"></param>
        /// <param name="inBV"></param>
        /// <param name="multiplicity"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe ICollection<MultiString> Split(char separator, ColumnBatch<long> inBV, out ColumnBatch<int> multiplicity)
        {
            Contract.Requires(this.State == MultiStringState.Sealed);
            Contract.Ensures(this.State == MultiStringState.Sealed);

            var resultList = new List<MultiString>();
            this.intPool.Get(out multiplicity);

            var current = Clone();
            current.Count = 0;
            current.starts = current.starts.MakeWritable(this.intPool);
            current.lengths = current.lengths.MakeWritable(this.shortPool);
            var rstartscol = current.starts.col;
            var rlengthscol = current.lengths.col;

            var bv = inBV.col;

            var startscol = this.starts.col;
            var lengthscol = this.lengths.col;

            fixed (char* src = this.col.charArray.content)
            {
                for (int i = 0; i < this.Count; i++)
                {
                    if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                    {
                        char* start = src + startscol[i];
                        char* curr = start;
                        char* end = start + lengthscol[i];
                        char* prev = start;
                        int localcount = 0;
                        int count = current.Count;

                        while (curr < end)
                        {
                            if (*curr == separator)
                            {
                                // report a match
                                rstartscol[count] = (int)(prev - start);
                                rlengthscol[count] = (short)(curr - prev);
                                count++;
                                localcount++;
                                if (count == rstartscol.Length)
                                {
                                    current.Count = count;
                                    resultList.Add(current);
                                    current = Clone();
                                    current.Count = 0;
                                    count = 0;
                                    current.starts = current.starts.MakeWritable(this.intPool);
                                    current.lengths = current.lengths.MakeWritable(this.shortPool);
                                    rstartscol = current.starts.col;
                                    rlengthscol = current.lengths.col;
                                }
                                prev = curr + 1;
                            }
                            curr++;
                        }

                        // report a match
                        rstartscol[count] = (int)(prev - start);
                        rlengthscol[count] = (short)(curr - prev);
                        count++;
                        localcount++;
                        if (count == rstartscol.Length)
                        {
                            current.Count = count;
                            resultList.Add(current);
                            current = Clone();
                            current.Count = 0;
                            count = 0;
                            current.starts = current.starts.MakeWritable(this.intPool);
                            current.lengths = current.lengths.MakeWritable(this.shortPool);
                            rstartscol = current.starts.col;
                            rlengthscol = current.lengths.col;
                        }
                        multiplicity.col[i] = localcount;
                        current.Count = count;
                    }
                }
            }
            if (current.Count > 0)
            {
                resultList.Add(current);
            }
            else
            {
                current.Dispose();
            }
            return resultList;
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="inBV"></param>
        /// <param name="multiplicity"></param>
        /// <param name="separator"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public ICollection<MultiString> Split(ColumnBatch<long> inBV, out ColumnBatch<int> multiplicity, params char[] separator)
        {
            Contract.Requires(this.State == MultiStringState.Sealed);
            Contract.Ensures(this.State == MultiStringState.Sealed);

            string pattern = char.ToString(separator[0]);
            for (int i = 1; i < separator.Length; i++)
            {
                pattern += "|" + separator[i];
            }
            var reg = new Regex(pattern);

            return Split(reg, inBV, out multiplicity);
        }

        #endregion

        #region Substring

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="startIndex"></param>
        /// <param name="length"></param>
        /// <param name="inBV"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public MultiString Substring(int startIndex, int length, ColumnBatch<long> inBV)
        {
            Contract.Requires(this.State == MultiStringState.Sealed);
            Contract.Ensures(this.State == MultiStringState.Sealed);

            MultiString result = Clone();
            result.Count = 0;
            result.EndOffset = 0;
            var bv = inBV.col;
            var startscol = this.starts.col;
            var lengthscol = this.lengths.col;

            result.starts = result.starts.MakeWritable(this.intPool);
            result.lengths = result.lengths.MakeWritable(this.shortPool);

            var rstartscol = result.starts.col;
            var rlengthscol = result.lengths.col;

            for (int i = 0; i < this.Count; i++)
            {
                if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                {
                    if (startIndex >= lengthscol[i]) continue;
                    rstartscol[result.Count] = startscol[i] + startIndex;
                    rlengthscol[result.Count] = (short)((lengthscol[i] - startIndex) < length ? (lengthscol[i] - startIndex) : length);
                    result.Count++;
                }
            }

            if (result.Count > 0)
            {
                result.EndOffset = rstartscol[result.Count - 1] + rlengthscol[result.Count - 1] + 1;
            }
            return result;
        }
        #endregion

        #region Equals

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="otherString"></param>
        /// <param name="inBV"></param>
        /// <param name="inPlace"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe ColumnBatch<long> Equals(string otherString, ColumnBatch<long> inBV, bool inPlace = true)
        {
            Contract.Requires(this.State == MultiStringState.Sealed);
            Contract.Ensures(this.State == MultiStringState.Sealed);

            ColumnBatch<long> result;

            if (inPlace)
                result = inBV.MakeWritable(this.bitvectorPool);
            else
            {
                this.bitvectorPool.Get(out result);
                Array.Copy(inBV.col, result.col, inBV.col.Length);
                result.UsedLength = inBV.UsedLength;
            }

            var bv = inBV.col;
            var rbv = result.col;
            var startscol = this.starts.col;
            var lengthscol = this.lengths.col;
            var otherlength = otherString.Length;

            int length;
            fixed (char* charContent = this.col.charArray.content)
            fixed (char* otherContent = otherString)
            {
                for (int i = 0; i < this.Count; i++)
                {
                    if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                    {
                        length = lengthscol[i];
                        if (length != otherlength)
                        {
                            rbv[i >> 6] |= 1L << (i & 0x3f);
                        }
                        else
                        {
                            char* charIter = charContent + startscol[i];
                            char* otherIter = otherContent;
                            bool done = false;
                            while (length >= 12)
                            {
                                if (*(long*)charIter != *(long*)otherIter)
                                {
                                    rbv[i >> 6] |= 1L << (i & 0x3f);
                                    done = true;
                                    break;
                                }
                                if (*(long*)(charIter + 4) != *(long*)(otherIter + 4))
                                {
                                    rbv[i >> 6] |= 1L << (i & 0x3f);
                                    done = true;
                                    break;
                                }
                                if (*(long*)(charIter + 8) != *(long*)(otherIter + 8))
                                {
                                    rbv[i >> 6] |= 1L << (i & 0x3f);
                                    done = true;
                                    break;
                                }
                                charIter += 12;
                                otherIter += 12;
                                length -= 12;
                            }
                            if (done) continue;
                            while (length > 1)
                            {
                                if (*(int*)charIter != *(int*)otherIter)
                                {
                                    break;
                                }
                                charIter += 2;
                                otherIter += 2;
                                length -= 2;
                            }
                            if (length == 1)
                            {
                                if (*charIter != *otherIter)
                                    rbv[i >> 6] |= 1L << (i & 0x3f);
                            }
                            else if (length > 0)
                            {
                                rbv[i >> 6] |= 1L << (i & 0x3f);
                            }
                        }
                    }
                }
            }
            return result;
        }

        #endregion

        #region GetHashCode

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="inBV"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public unsafe ColumnBatch<int> GetHashCode(ColumnBatch<long> inBV)
        {
            Contract.Requires(this.State == MultiStringState.Sealed);
            Contract.Ensures(this.State == MultiStringState.Sealed);

            this.intPool.Get(out var result);
            var bv = inBV.col;
            var startscol = this.starts.col;
            var lengthscol = this.lengths.col;
            var resultcol = result.col;

            fixed (char* src = this.col.charArray.content)
            {
                for (int i = 0; i < this.Count; i++)
                {
                    if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                    {
                        char* stringStart = src + startscol[i];
                        var stringLength = lengthscol[i];
                        resultcol[i] = Utility.StableHashUnsafe(stringStart, stringLength);
                    }
                }
            }
            return result;
        }
        #endregion

        #region Length

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="inBV"></param>
        /// <param name="ignore"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public ColumnBatch<short> Length(ColumnBatch<long> inBV, bool ignore)
        {
            Contract.Requires(this.State == MultiStringState.Sealed);
            Contract.Ensures(this.State == MultiStringState.Sealed);

            this.lengths.IncrementRefCount(1);
            return this.lengths;
        }
        #endregion

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public struct MultiStringWrapper
        {
            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public MultiString theActualMultiString;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public int rowIndex;

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="m"></param>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public MultiStringWrapper(MultiString m)
            {
                this.theActualMultiString = m;
                this.rowIndex = 0;
            }
            private int StartIndex => this.theActualMultiString.starts.col[this.rowIndex] + 2;

            /// <summary>
            /// Used internally, but also is the wrapper implementation for String.Length
            /// </summary>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public int Length => this.theActualMultiString.lengths.col[this.rowIndex];
            private string String => this.theActualMultiString.col.charArray.contentString;

            #region IndexOf

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="value"></param>
            /// <returns></returns>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public int IndexOf(char value)
            {
                var realResult = this.String.IndexOf(value, this.StartIndex, this.Length);
                return realResult == -1 ? -1 : realResult - this.StartIndex;
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="value"></param>
            /// <returns></returns>
            [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Globalization", "CA1307:SpecifyStringComparison",
                MessageId = "System.String.IndexOf(System.String,System.Int32,System.Int32)",
                Justification = "This is CLR API substitution, additional intent should not be added on behalf of the user of the API.")]
            [EditorBrowsable(EditorBrowsableState.Never)]
            public int IndexOf(string value)
            {
                var realResult = this.String.IndexOf(value, this.StartIndex, this.Length);
                return realResult == -1 ? -1 : realResult - this.StartIndex;
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="value"></param>
            /// <param name="startIndex"></param>
            /// <returns></returns>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public int IndexOf(char value, int startIndex)
            {
                var realResult = this.String.IndexOf(value, this.StartIndex + startIndex, this.Length - startIndex);
                return realResult == -1 ? -1 : realResult - this.StartIndex;
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="value"></param>
            /// <param name="startIndex"></param>
            /// <returns></returns>
            [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Globalization", "CA1307:SpecifyStringComparison",
                MessageId = "System.String.IndexOf(System.String,System.Int32,System.Int32)",
                Justification = "This is CLR API substitution, additional intent should not be added on behalf of the user of the API.")]
            [EditorBrowsable(EditorBrowsableState.Never)]
            public int IndexOf(string value, int startIndex)
            {
                var realResult = this.String.IndexOf(value, this.StartIndex + startIndex, this.Length - startIndex);
                return realResult == -1 ? -1 : realResult - this.StartIndex;
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="value"></param>
            /// <param name="comparisonType"></param>
            /// <returns></returns>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public int IndexOf(string value, StringComparison comparisonType)
            {
                var realResult = this.String.IndexOf(value, this.StartIndex, this.Length, comparisonType);
                return realResult == -1 ? -1 : realResult - this.StartIndex;
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="value"></param>
            /// <param name="startIndex"></param>
            /// <param name="count"></param>
            /// <returns></returns>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public int IndexOf(char value, int startIndex, int count)
            {
                var realResult = this.String.IndexOf(value, this.StartIndex + startIndex, count);
                return realResult == -1 ? -1 : realResult - this.StartIndex;
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="value"></param>
            /// <param name="startIndex"></param>
            /// <param name="count"></param>
            /// <returns></returns>
            [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Globalization", "CA1307:SpecifyStringComparison",
                MessageId = "System.String.IndexOf(System.String,System.Int32,System.Int32)",
                Justification = "This is CLR API substitution, additional intent should not be added on behalf of the user of the API.")]
            [EditorBrowsable(EditorBrowsableState.Never)]
            public int IndexOf(string value, int startIndex, int count)
            {
                var realResult = this.String.IndexOf(value, this.StartIndex + startIndex, count);
                return realResult == -1 ? -1 : realResult - this.StartIndex;
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="value"></param>
            /// <param name="startIndex"></param>
            /// <param name="comparisonType"></param>
            /// <returns></returns>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public int IndexOf(string value, int startIndex, StringComparison comparisonType)
            {
                var realResult = this.String.IndexOf(value, this.StartIndex + startIndex, this.Length - startIndex, comparisonType);
                return realResult == -1 ? -1 : realResult - this.StartIndex;
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="value"></param>
            /// <param name="startIndex"></param>
            /// <param name="count"></param>
            /// <param name="comparisonType"></param>
            /// <returns></returns>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public int IndexOf(string value, int startIndex, int count, StringComparison comparisonType)
            {
                var realResult = this.String.IndexOf(value, this.StartIndex + startIndex, count, comparisonType);
                return realResult == -1 ? -1 : realResult - this.StartIndex;
            }
            #endregion

            #region LastIndexOf

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="value"></param>
            /// <returns></returns>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public int LastIndexOf(char value)
            {
                var realResult = this.String.LastIndexOf(value, this.StartIndex + this.Length - 1, this.Length);
                return realResult == -1 ? -1 : realResult - this.StartIndex;
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="value"></param>
            /// <returns></returns>
            [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Globalization", "CA1307:SpecifyStringComparison",
                MessageId = "System.String.LastIndexOf(System.String,System.Int32,System.Int32)",
                Justification = "This is CLR API substitution, additional intent should not be added on behalf of the user of the API.")]
            [EditorBrowsable(EditorBrowsableState.Never)]
            public int LastIndexOf(string value)
            {
                var realResult = this.String.LastIndexOf(value, this.StartIndex + this.Length - 1, this.Length);
                return realResult == -1 ? -1 : realResult - this.StartIndex;
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="value"></param>
            /// <param name="startIndex"></param>
            /// <returns></returns>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public int LastIndexOf(char value, int startIndex)
            {
                var realResult = this.String.LastIndexOf(value, this.StartIndex + startIndex, this.Length - startIndex);
                return realResult == -1 ? -1 : realResult - this.StartIndex;
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="value"></param>
            /// <param name="startIndex"></param>
            /// <returns></returns>
            [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Globalization", "CA1307:SpecifyStringComparison",
                MessageId = "System.String.LastIndexOf(System.String,System.Int32,System.Int32)",
                Justification = "This is CLR API substitution, additional intent should not be added on behalf of the user of the API.")]
            [EditorBrowsable(EditorBrowsableState.Never)]
            public int LastIndexOf(string value, int startIndex)
            {
                var realResult = this.String.LastIndexOf(value, this.StartIndex + startIndex, this.Length - startIndex);
                return realResult == -1 ? -1 : realResult - this.StartIndex;
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="value"></param>
            /// <param name="comparisonType"></param>
            /// <returns></returns>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public int LastIndexOf(string value, StringComparison comparisonType)
            {
                var realResult = this.String.LastIndexOf(value, this.StartIndex + this.Length - 1, this.Length, comparisonType);
                return realResult == -1 ? -1 : realResult - this.StartIndex;
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="value"></param>
            /// <param name="startIndex"></param>
            /// <param name="count"></param>
            /// <returns></returns>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public int LastIndexOf(char value, int startIndex, int count)
            {
                var realResult = this.String.LastIndexOf(value, this.StartIndex + startIndex, count);
                return realResult == -1 ? -1 : realResult - this.StartIndex;
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="value"></param>
            /// <param name="startIndex"></param>
            /// <param name="count"></param>
            /// <returns></returns>
            [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Globalization", "CA1307:SpecifyStringComparison",
                MessageId = "System.String.LastIndexOf(System.String,System.Int32,System.Int32)",
                Justification = "This is CLR API substitution, additional intent should not be added on behalf of the user of the API.")]
            [EditorBrowsable(EditorBrowsableState.Never)]
            public int LastIndexOf(string value, int startIndex, int count)
            {
                var realResult = this.String.LastIndexOf(value, this.StartIndex + startIndex, count);
                return realResult == -1 ? -1 : realResult - this.StartIndex;
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="value"></param>
            /// <param name="startIndex"></param>
            /// <param name="comparisonType"></param>
            /// <returns></returns>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public int LastIndexOf(string value, int startIndex, StringComparison comparisonType)
            {
                var realResult = this.String.LastIndexOf(value, this.StartIndex + startIndex, this.Length - startIndex, comparisonType);
                return realResult == -1 ? -1 : realResult - this.StartIndex;
            }

            /// <summary>
            /// Currently for internal use only - do not use directly.
            /// </summary>
            /// <param name="value"></param>
            /// <param name="startIndex"></param>
            /// <param name="count"></param>
            /// <param name="comparisonType"></param>
            /// <returns></returns>
            [EditorBrowsable(EditorBrowsableState.Never)]
            public int LastIndexOf(string value, int startIndex, int count, StringComparison comparisonType)
            {
                var realResult = this.String.LastIndexOf(value, this.StartIndex + startIndex, count, comparisonType);
                return realResult == -1 ? -1 : realResult - this.StartIndex;
            }
            #endregion
        }
    }
}