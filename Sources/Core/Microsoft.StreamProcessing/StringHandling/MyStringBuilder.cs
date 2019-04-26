// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class MyStringBuilder : IDisposable
    {
        internal const int DefaultCapacity = 0x10;
        internal const int MaxChunkSize = 0x1f40;
        [DataMember]
        internal char[] m_ChunkChars;
        [DataMember]
        internal int m_ChunkLength;
        [DataMember]
        internal int m_ChunkOffset;
        [DataMember]
        internal MyStringBuilder m_ChunkPrevious;
        [DataMember]
        internal int m_MaxCapacity;

        private readonly CharArrayPool cap;

        [DataMember]
        internal CharArrayWrapper m_ChunkCharsWrapper;

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2213:DisposableFieldsShouldBeDisposed", MessageId = "m_ChunkPrevious", Justification = "Calling dispose on the previous node is both inefficient and also likely to cause stack overflow.")]
        public void Dispose()
        {
            var msb = this;
            while (msb != null)
            {
                if (msb.m_ChunkCharsWrapper != null)
                    msb.m_ChunkCharsWrapper.Return();
                msb = msb.m_ChunkPrevious;
            }
        }

        [Obsolete("Used only by serialization. Do not call directly.")]
        public MyStringBuilder() : this(DefaultCapacity, null) { }

        public MyStringBuilder(CharArrayPool pool) : this(DefaultCapacity, pool) { }

        public MyStringBuilder(int capacity, CharArrayPool pool = null) : this(string.Empty, capacity, pool) { }

        private MyStringBuilder(MyStringBuilder from)
        {
            this.m_ChunkLength = from.m_ChunkLength;
            this.m_ChunkOffset = from.m_ChunkOffset;
            this.m_ChunkChars = from.m_ChunkChars;
            this.m_ChunkPrevious = from.m_ChunkPrevious;
            this.m_MaxCapacity = from.m_MaxCapacity;

            this.m_ChunkCharsWrapper = from.m_ChunkCharsWrapper;
            this.cap = from.cap;
        }

        public MyStringBuilder(string value, int capacity, CharArrayPool pool = null)
            : this(value, 0, (value != null) ? value.Length : 0, capacity, pool) { }

        public unsafe MyStringBuilder(string value, int startIndex, int length, int capacity, CharArrayPool pool = null)
        {
            this.cap = pool;

            if (capacity < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(capacity));
            }
            else if (length < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(length));
            }
            else if (startIndex < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(startIndex));
            }
            else if (value == null)
            {
                value = string.Empty;
            }
            else if (startIndex > (value.Length - length))
            {
                throw new ArgumentOutOfRangeException(nameof(length));
            }

            this.m_MaxCapacity = 0x7fffffff;
            if (capacity == 0)
            {
                capacity = 0x10;
            }

            if (capacity < length)
            {
                capacity = length;
            }

            if (this.cap != null)
            {
                this.cap.Get(out this.m_ChunkCharsWrapper, capacity);
                this.m_ChunkChars = this.m_ChunkCharsWrapper.charArray.content;
            }
            else
            {
                this.m_ChunkChars = new char[capacity];
            }

            this.m_ChunkLength = length;
            fixed (char* str = value)
            {
                ThreadSafeCopy(str + startIndex, this.m_ChunkChars, 0, length);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe void ThreadSafeCopy(char* sourcePtr, char[] destination, int destinationIndex, int count)
        {
            if (count > 0)
            {
                fixed (char* charRef = &(destination[destinationIndex]))
                {
                    Wstrcpy(charRef, sourcePtr, count);
                }
            }
        }

        public unsafe MyStringBuilder Append(string value)
        {
            if (value != null)
            {
                char[] chunkChars = this.m_ChunkChars;
                int chunkLength = this.m_ChunkLength;
                int length = value.Length;
                int num3 = chunkLength + length;
                if (num3 < chunkChars.Length)
                {
                    if (length <= 2)
                    {
                        if (length > 0)
                        {
                            chunkChars[chunkLength] = value[0];
                        }
                        if (length > 1)
                        {
                            chunkChars[chunkLength + 1] = value[1];
                        }
                    }
                    else
                    {
                        fixed (char* str = value)
                        {
                            char* smem = str;
                            fixed (char* charRef = &(chunkChars[chunkLength]))
                            {
                                Wstrcpy(charRef, smem, length);
                            }
                        }
                    }

                    this.m_ChunkLength = num3;
                }
                else
                {
                    AppendHelper(value);
                }
            }
            return this;
        }

        public MyStringBuilder Append(char value, int repeatCount)
        {
            if (repeatCount < 0) throw new ArgumentOutOfRangeException(nameof(repeatCount));
            if (repeatCount != 0)
            {
                int chunkLength = this.m_ChunkLength;
                while (repeatCount > 0)
                {
                    if (chunkLength < this.m_ChunkChars.Length)
                    {
                        this.m_ChunkChars[chunkLength++] = value;
                        repeatCount--;
                    }
                    else
                    {
                        this.m_ChunkLength = chunkLength;
                        ExpandByABlock(repeatCount);
                        chunkLength = 0;
                    }
                }

                this.m_ChunkLength = chunkLength;
            }
            return this;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe MyStringBuilder Append(char* value, int valueCount)
        {
            int num = valueCount + this.m_ChunkLength;
            if (num <= this.m_ChunkChars.Length)
            {
                ThreadSafeCopy(value, this.m_ChunkChars, this.m_ChunkLength, valueCount);
                this.m_ChunkLength = num;
            }
            else
            {
                int count = this.m_ChunkChars.Length - this.m_ChunkLength;
                if (count > 0)
                {
                    ThreadSafeCopy(value, this.m_ChunkChars, this.m_ChunkLength, count);
                    this.m_ChunkLength = this.m_ChunkChars.Length;
                }
                int minBlockCharCount = valueCount - count;
                ExpandByABlock(minBlockCharCount);
                ThreadSafeCopy(value + count, this.m_ChunkChars, 0, minBlockCharCount);
                this.m_ChunkLength = minBlockCharCount;
            }
            return this;
        }

        private unsafe void AppendHelper(string value)
        {
            fixed (char* str = value)
            {
                char* charPtr = str;
                Append(charPtr, value.Length);
            }
        }

        private void ExpandByABlock(int minBlockCharCount)
        {
            if ((minBlockCharCount + this.Length) > this.m_MaxCapacity) throw new ArgumentOutOfRangeException("requiredLength");

            int num = Math.Max(minBlockCharCount, Math.Min(this.Length, 0x1f40));
            this.m_ChunkPrevious = new MyStringBuilder(this);
            this.m_ChunkOffset += this.m_ChunkLength;
            this.m_ChunkLength = 0;
            if ((this.m_ChunkOffset + num) < num)
            {
                this.m_ChunkChars = null;
                throw new OutOfMemoryException();
            }
            if (this.cap != null)
            {
                this.cap.Get(out this.m_ChunkCharsWrapper, num);
                this.m_ChunkChars = this.m_ChunkCharsWrapper.charArray.content;
            }
            else
            {
                this.m_ChunkChars = new char[num];
            }
        }

        private MyStringBuilder FindChunkForIndex(int index)
        {
            var chunkPrevious = this;
            while (chunkPrevious.m_ChunkOffset > index)
            {
                chunkPrevious = chunkPrevious.m_ChunkPrevious;
            }
            return chunkPrevious;
        }

        public unsafe CharArrayWrapper ToCharArrayWrapper()
        {
            CharArrayWrapper resultCaw;
            if (this.cap != null)
            {
                this.cap.Get(out resultCaw, this.Length + 2);
                this.m_ChunkChars = this.m_ChunkCharsWrapper.charArray.content;
            }
            else
            {
                resultCaw = new CharArrayWrapper(null, this.Length + 2, this.Length + 2);
            }

            var result = resultCaw.charArray.content;

            result[this.Length] = '\0';
            result[this.Length + 1] = '\0';

            if (this.Length == 0) return resultCaw;

            var chunkPrevious = this;
            fixed (char* str2 = result)
            {
                char* charPtr = str2;
                do
                {
                    if (chunkPrevious.m_ChunkLength > 0)
                    {
                        char[] chunkChars = chunkPrevious.m_ChunkChars;
                        int chunkOffset = chunkPrevious.m_ChunkOffset;
                        int chunkLength = chunkPrevious.m_ChunkLength;
                        if ((((ulong)(chunkLength + chunkOffset)) > (ulong)(result.Length - 1)) || (chunkLength > chunkChars.Length))
                            throw new ArgumentOutOfRangeException("chunkLength");
                        fixed (char* charRef = chunkChars)
                        {
                            Wstrcpy(charPtr + chunkOffset, charRef, chunkLength);
                        }
                    }
                    chunkPrevious = chunkPrevious.m_ChunkPrevious;
                }
                while (chunkPrevious != null);
            }
            return resultCaw;
        }

        public unsafe CharArrayWrapper ToCharArrayWrapperAndDispose()
        {
            if ((this.m_ChunkPrevious == null) && (this.m_ChunkChars.Length >= this.Length + 2))
            {
                this.m_ChunkCharsWrapper.UsedLength = this.Length + 2;
                this.m_ChunkChars[this.Length] = '\0';
                this.m_ChunkChars[this.Length + 1] = '\0';

                return this.m_ChunkCharsWrapper;
            }
            else
            {
                var result = ToCharArrayWrapper();
                Dispose();
                return result;
            }
        }

        public int Capacity
        {
            get => this.m_ChunkChars.Length + this.m_ChunkOffset;

            set
            {
                if (value < 0)
                    throw new ArgumentOutOfRangeException(nameof(value));
                if (value > this.MaxCapacity)
                    throw new ArgumentOutOfRangeException(nameof(value));
                if (value < this.Length)
                    throw new ArgumentOutOfRangeException(nameof(value));
                if (this.Capacity != value)
                {
                    int num = value - this.m_ChunkOffset;
                    char[] destinationArray = new char[num];
                    Array.Copy(this.m_ChunkChars, destinationArray, this.m_ChunkLength);
                    this.m_ChunkChars = destinationArray;
                }
            }
        }

        public int Length
        {
            get => this.m_ChunkOffset + this.m_ChunkLength;

            set
            {
                if (value < 0 || value > this.MaxCapacity)
                    throw new ArgumentOutOfRangeException(nameof(value));
                int capacity = this.Capacity;
                if ((value == 0) && (this.m_ChunkPrevious == null))
                {
                    this.m_ChunkLength = 0;
                    this.m_ChunkOffset = 0;
                }
                else
                {
                    int repeatCount = value - this.Length;
                    if (repeatCount > 0)
                    {
                        Append('\0', repeatCount);
                    }
                    else
                    {
                        var builder = FindChunkForIndex(value);
                        if (builder != this)
                        {
                            int num3 = capacity - builder.m_ChunkOffset;
                            char[] destinationArray = new char[num3];
                            Array.Copy(builder.m_ChunkChars, destinationArray, builder.m_ChunkLength);
                            this.m_ChunkChars = destinationArray;
                            this.m_ChunkPrevious = builder.m_ChunkPrevious;
                            this.m_ChunkOffset = builder.m_ChunkOffset;
                        }

                        this.m_ChunkLength = value - builder.m_ChunkOffset;
                    }
                }
            }
        }

        public int MaxCapacity => this.m_MaxCapacity;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe void Wstrcpy(char* dmem, char* smem, int charCount)
        {
            if (charCount > 0)
            {
                if (((((int)dmem) | ((int)smem)) & 1) == 0)
                {
                    if ((((int)dmem) & 2) != 0)
                    {
                        dmem[0] = smem[0];
                        dmem++;
                        smem++;
                        charCount--;
                    }
                    if (((((int)dmem) & 4) != 0) && (charCount >= 2))
                    {
                        *(uint*)dmem = *(uint*)smem;
                        dmem += 2;
                        smem += 2;
                        charCount -= 2;
                    }
                    while (charCount >= 0x10)
                    {
                        *(long*)dmem = *(long*)smem;
                        *(long*)(dmem + 4) = *(long*)(smem + 4);
                        *(long*)(dmem + 8) = *(long*)(smem + 8);
                        *(long*)(dmem + 12) = *(long*)(smem + 12);
                        dmem += 0x10;
                        smem += 0x10;
                        charCount -= 0x10;
                    }
                    if ((charCount & 8) != 0)
                    {
                        *(long*)dmem = *(long*)smem;
                        *(long*)(dmem + 4) = *(long*)(smem + 4);
                        dmem += 8;
                        smem += 8;
                    }
                    if ((charCount & 4) != 0)
                    {
                        *(long*)dmem = *(long*)smem;
                        dmem += 4;
                        smem += 4;
                    }
                    if ((charCount & 2) != 0)
                    {
                        *(uint*)dmem = *(uint*)smem;
                        dmem += 2;
                        smem += 2;
                    }
                    if ((charCount & 1) != 0)
                    {
                        dmem[0] = smem[0];
                    }
                }
                else
                {
                    do
                    {
                        *(byte*)dmem = *(byte*)smem;
                        *(byte*)(dmem + 1) = *(byte*)(smem + 1);
                        charCount--;
                        dmem++;
                        smem++;
                    }
                    while (charCount > 0);
                }
            }
        }
    }
}
