// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.IO;
using System.Text;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing.Serializer
{
    internal sealed partial class BinaryEncoder : BinaryBase
    {
        public BinaryEncoder(Stream stream) : base(stream) { }

        public void Encode(bool value) => this.stream.WriteByte(value ? (byte)1 : (byte)0);

        public void Encode(byte value) => this.stream.WriteByte(value);

        public void Encode(sbyte value) => this.stream.WriteByte(unchecked((byte)value));

        public void Encode(short value)
        {
            var valueCast = unchecked((ushort)value);
            this.stream.WriteByte((byte)(valueCast & 0xFFU));
            this.stream.WriteByte((byte)(valueCast >> 0x8));
        }

        public void Encode(ushort value)
        {
            this.stream.WriteByte((byte)(value & 0xFFU));
            this.stream.WriteByte((byte)(value >> 0x8));
        }

        public void Encode(int value)
        {
            var zigZagEncoded = unchecked((uint)((value << 1) ^ (value >> 31)));
            while ((zigZagEncoded & ~0x7FU) != 0)
            {
                this.stream.WriteByte((byte)((zigZagEncoded | 0x80U) & 0xFF));
                zigZagEncoded >>= 7;
            }

            this.stream.WriteByte((byte)zigZagEncoded);
        }

        public void Encode(uint value)
        {
            var zigZagEncoded = unchecked((value << 1) ^ (value >> 31));
            while ((zigZagEncoded & ~0x7FU) != 0)
            {
                this.stream.WriteByte((byte)((zigZagEncoded | 0x80U) & 0xFF));
                zigZagEncoded >>= 7;
            }

            this.stream.WriteByte((byte)zigZagEncoded);
        }

        public void Encode(long value)
        {
            var zigZagEncoded = unchecked((ulong)((value << 1) ^ (value >> 63)));
            while ((zigZagEncoded & ~0x7FUL) != 0)
            {
                this.stream.WriteByte((byte)((zigZagEncoded | 0x80UL) & 0xFFUL));
                zigZagEncoded >>= 7;
            }

            this.stream.WriteByte((byte)zigZagEncoded);
        }

        public void Encode(ulong value)
        {
            var zigZagEncoded = unchecked((value << 1) ^ (value >> 63));
            while ((zigZagEncoded & ~0x7FUL) != 0)
            {
                this.stream.WriteByte((byte)((zigZagEncoded | 0x80UL) & 0xFFUL));
                zigZagEncoded >>= 7;
            }

            this.stream.WriteByte((byte)zigZagEncoded);
        }

        public void Encode(float value)
        {
            byte[] bytes = BitConverter.GetBytes(value);
            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            this.stream.Write(bytes, 0, bytes.Length);
        }

        public void Encode(double value)
        {
            long encodedValue = BitConverter.DoubleToInt64Bits(value);
            this.stream.WriteByte((byte)(encodedValue & 0xFF));
            this.stream.WriteByte((byte)((encodedValue >> 0x8) & 0xFF));
            this.stream.WriteByte((byte)((encodedValue >> 0x10) & 0xFF));
            this.stream.WriteByte((byte)((encodedValue >> 0x18) & 0xFF));
            this.stream.WriteByte((byte)((encodedValue >> 0x20) & 0xFF));
            this.stream.WriteByte((byte)((encodedValue >> 0x28) & 0xFF));
            this.stream.WriteByte((byte)((encodedValue >> 0x30) & 0xFF));
            this.stream.WriteByte((byte)((encodedValue >> 0x38) & 0xFF));
        }

        public void Encode(byte[] value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));

            Encode(value.Length);
            if (value.Length > 0) this.stream.Write(value, 0, value.Length);
        }

        public void Encode(string value)
            => Encode(Encoding.UTF8.GetBytes(value ?? throw new ArgumentNullException(nameof(value))));

        public void EncodeArrayChunk(int size) => Encode(size);

        public void Encode(Guid value) => this.stream.Write(value.ToByteArray(), 0, 16);

        private void WriteIntFixed(int encodedValue)
        {
            this.stream.WriteByte((byte)(encodedValue & 0xFF));
            this.stream.WriteByte((byte)((encodedValue >> 0x8) & 0xFF));
            this.stream.WriteByte((byte)((encodedValue >> 0x10) & 0xFF));
            this.stream.WriteByte((byte)((encodedValue >> 0x18) & 0xFF));
        }

        public void Encode<T>(T[] value) where T : struct
        {
            long sizeInBytes = value.Length * typeof(T).GetSizeOf();
            Encode(sizeInBytes);

            var buffer = AllocateColumnBatch<byte>((int)sizeInBytes);
            ToStream(value, value.Length, buffer.col);
            buffer.Return();
        }

        public void Encode<T>(ColumnBatch<T> value) where T : struct
        {
            long sizeInBytes = value.UsedLength * typeof(T).GetSizeOf() + sizeof(int);
            Encode(sizeInBytes);

            WriteIntFixed(value.col.Length);

            var buffer = AllocateColumnBatch<byte>(value.UsedLength * typeof(T).GetSizeOf());
            ToStream(value.col, value.UsedLength, buffer.col);
            buffer.Return();
        }

        private void ToStream<T>(T[] blob, int numElements, byte[] buffer) where T : struct
        {
            if (numElements == 0) return;
            if (numElements < 0) numElements = blob.Length;

            if ((numElements <= 0) || (numElements > blob.Length))
                throw new InvalidOperationException("Out of bounds");

            Buffer.BlockCopy(blob, 0, buffer, 0, buffer.Length);
            this.stream.Write(buffer, 0, buffer.Length);
        }

        private static readonly Lazy<DoublingArrayPool<byte>> bytePool = new Lazy<DoublingArrayPool<byte>>(MemoryManager.GetDoublingArrayPool<byte>);

        public unsafe void Encode(CharArrayWrapper value)
        {
            if (Config.SerializationCompressionLevel.HasFlag(SerializationCompressionLevel.CharArrayToUTF8))
                Encode(1);
            else
                Encode(0); // header for version number

            Encode(value.UsedLength);
            Encode(value.charArray.content.Length);

            if (Config.SerializationCompressionLevel.HasFlag(SerializationCompressionLevel.CharArrayToUTF8))
            {
                byte[] result;
                int encodedSize = Encoding.UTF8.GetByteCount(value.charArray.content, 0, value.UsedLength);
                Encode(encodedSize);

                if (encodedSize > 0)
                    bytePool.Value.Get(out result, encodedSize);
                else
                    bytePool.Value.Get(out result, 1);

                Encoding.UTF8.GetBytes(value.charArray.content, 0, value.UsedLength, result, 0);

                this.stream.Write(result, 0, encodedSize);
                bytePool.Value.Return(result);
            }
            else
            {
                var buffer = AllocateColumnBatch<byte>(value.UsedLength * sizeof(char));
                ToStream(value.charArray.content, value.UsedLength, buffer.col);
                buffer.Return();
            }
        }

        public unsafe void Encode(string[] value)
        {
            var temp = new ColumnBatch<string>
            {
                UsedLength = value.Length,
                col = value
            };

            Encode(temp);
        }

        private static CharArrayPool charPool;
        private static ColumnPool<int> intPool;
        private static ColumnPool<short> shortPool;
        private static int batchSize = -1;

        public unsafe void Encode(ColumnBatch<string> value)
        {
            if (charPool == null || intPool == null || shortPool == null || batchSize != Config.DataBatchSize)
            {
                charPool = MemoryManager.GetCharArrayPool();
                intPool = MemoryManager.GetColumnPool<int>();
                shortPool = MemoryManager.GetColumnPool<short>();
                batchSize = Config.DataBatchSize;
            }

            WriteIntFixed(value.UsedLength);
            if (value.UsedLength == 0) return;

            int totalChars = 0;
            int maxLength = 0;
            for (int i = 0; i < value.UsedLength; i++)
            {
                if (value.col[i] != null)
                {
                    var length = value.col[i].Length;
                    totalChars += length;
                    if (length > maxLength) maxLength = length;
                }
            }
            WriteIntFixed(maxLength);

            if (maxLength <= short.MaxValue)
            {
                shortPool.Get(out var lengths);
                lengths.UsedLength = value.UsedLength;

                CharArrayWrapper caw;

                if (totalChars > 0)
                    charPool.Get(out caw, totalChars + 2);
                else
                    charPool.Get(out caw, 1 + 2);

                int offset = 0;
                fixed (char* dest = caw.charArray.content)
                {
                    for (int i = 0; i < value.UsedLength; i++)
                    {
                        if (value.col[i] != null)
                        {
                            var length = value.col[i].Length;
                            fixed (char* src = value.col[i])
                            {
                                MyStringBuilder.Wstrcpy(dest + offset, src, length);
                            }
                            lengths.col[i] = (short)length;
                            offset += length;
                        }
                        else
                            lengths.col[i] = -1;
                    }
                }

                Encode(lengths);
                lengths.Return();

                Encode(caw);

                caw.Return();
            }
            else
            {
                WriteIntFixed(value.UsedLength);
                for (int i = 0; i < value.UsedLength; i++)
                {
                    Encode(value.col[i]);
                }
            }
        }
    }
}
