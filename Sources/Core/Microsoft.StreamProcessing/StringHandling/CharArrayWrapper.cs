// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Serialization;
using System.Threading;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class CharArrayWrapper
    {
        [DataMember]
        public int UsedLength;
        [DataMember]
        public CharArray charArray;
        public CharArrayPool pool;
        internal int RefCount;

        public CharArrayWrapper(CharArrayPool pool, int size, int requestedSize)
        {
            this.UsedLength = requestedSize;
            this.pool = pool;
            this.charArray = new CharArray(size);
            this.RefCount = 1;
        }

        public CharArrayWrapper() => this.RefCount = 1;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementRefCount(int cnt) => Interlocked.Add(ref this.RefCount, cnt);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe CharArrayWrapper MakeWritable(CharArrayPool pool, bool copyData)
        {
            if (this.RefCount == 1) return this;
            else
            {
                Interlocked.Decrement(ref this.RefCount);
                pool.Get(out var result, this.UsedLength);
                if (copyData)
                {
                    fixed (char* dest = result.charArray.content)
                    fixed (char* src = this.charArray.content)
                    {
                        MyStringBuilder.Wstrcpy(dest, src, this.UsedLength);
                    }
                }
                return result;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Return()
        {
            int localRefCount = Interlocked.Decrement(ref this.RefCount);

            if (localRefCount == 0 && this.pool != null) this.pool.Return(this);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReturnClear()
        {
            int localRefCount = Interlocked.Decrement(ref this.RefCount);

            if (localRefCount == 0)
            {
                Array.Clear(this.charArray.content, 0, this.UsedLength);
                if (this.pool != null) this.pool.Return(this);
            }
        }
    }

    [StructLayout(layoutKind: LayoutKind.Explicit, Pack = 1)]
    internal struct CharArray
    {
        [FieldOffset(0)]
        public char[] content;
        [FieldOffset(0)]
        public string contentString;

        public CharArray(int length)
        {
            this.contentString = null;
            this.content = new char[length];
        }
    }
}