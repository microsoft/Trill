// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.IO;
using System.Runtime.Serialization;
using System.Text;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing.Serializer
{
    internal sealed partial class BinaryDecoder : BinaryBase
    {
        public BinaryDecoder(Stream stream) : base(stream) { }

        public bool DecodeBool() => this.stream.ReadByte() != 0;

        public byte DecodeByte() => (byte)this.stream.ReadByte();

        public sbyte DecodeSByte() => (sbyte)this.stream.ReadByte();

        public short DecodeShort()
        {
            var lowByte = unchecked((uint)this.stream.ReadByte());
            var highByte = unchecked((uint)this.stream.ReadByte());
            return unchecked((short)(lowByte + (highByte << 8)));
        }

        public ushort DecodeUShort()
        {
            var lowByte = unchecked((uint)this.stream.ReadByte());
            var highByte = unchecked((uint)this.stream.ReadByte());
            return unchecked((ushort)(lowByte + (highByte << 8)));
        }

        public int DecodeInt()
        {
            var currentByte = (uint)this.stream.ReadByte();
            byte read = 1;
            uint result = currentByte & 0x7FU;
            int shift = 7;
            while ((currentByte & 0x80) != 0)
            {
                currentByte = (uint)this.stream.ReadByte();
                read++;
                result |= (currentByte & 0x7FU) << shift;
                shift += 7;
                if (read > 5)
                    throw new SerializationException("Invalid integer value in the input stream.");
            }
            return (int)((-(result & 0x1U)) ^ ((result >> 1) & 0x7FFFFFFFU));
        }

        public uint DecodeUInt()
        {
            var currentByte = (uint)this.stream.ReadByte();
            byte read = 1;
            uint result = currentByte & 0x7FU;
            int shift = 7;
            while ((currentByte & 0x80) != 0)
            {
                currentByte = (uint)this.stream.ReadByte();
                read++;
                result |= (currentByte & 0x7FU) << shift;
                shift += 7;
                if (read > 5)
                    throw new SerializationException("Invalid unsigned integer value in the input stream.");
            }
            return ((unchecked(0U - (result & 0x1U))) ^ ((result >> 1) & 0x7FFFFFFFU));
        }

        public long DecodeLong()
        {
            var value = (uint)this.stream.ReadByte();
            byte read = 1;
            ulong result = value & 0x7FUL;
            int shift = 7;
            while ((value & 0x80) != 0)
            {
                value = (uint)this.stream.ReadByte();
                read++;
                result |= (value & 0x7FUL) << shift;
                shift += 7;
                if (read > 10)
                    throw new SerializationException("Invalid integer long in the input stream.");
            }
            var tmp = unchecked((long)result);
            return (-(tmp & 0x1L)) ^ ((tmp >> 1) & 0x7FFFFFFFFFFFFFFFL);
        }

        public ulong DecodeULong()
        {
            var value = (uint)this.stream.ReadByte();
            byte read = 1;
            ulong result = value & 0x7FUL;
            int shift = 7;
            while ((value & 0x80) != 0)
            {
                value = (uint)this.stream.ReadByte();
                read++;
                result |= (value & 0x7FUL) << shift;
                shift += 7;
                if (read > 10)
                    throw new SerializationException("Invalid unsigned integer long in the input stream.");
            }
            return unchecked(0UL - (result & 0x1L)) ^ ((result >> 1) & 0x7FFFFFFFFFFFFFFFL);
        }

        public float DecodeFloat()
        {
            var value = new byte[4];
            ReadAllRequiredBytes(value);
            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(value);
            }
            return BitConverter.ToSingle(value, 0);
        }

        public double DecodeDouble()
        {
            var value = new byte[8];
            ReadAllRequiredBytes(value);
            long longValue = value[0]
                | (long)value[1] << 0x8
                | (long)value[2] << 0x10
                | (long)value[3] << 0x18
                | (long)value[4] << 0x20
                | (long)value[5] << 0x28
                | (long)value[6] << 0x30
                | (long)value[7] << 0x38;
            return BitConverter.Int64BitsToDouble(longValue);
        }

        public byte[] DecodeByteArray()
        {
            int arraySize = DecodeInt();
            var array = new byte[arraySize];
            if (arraySize > 0) ReadAllRequiredBytes(array);
            return array;
        }

        public string DecodeString() => Encoding.UTF8.GetString(DecodeByteArray());

        public int DecodeArrayChunk()
        {
            int result = DecodeInt();
            if (result < 0)
            {
                DecodeLong();
                result = -result;
            }
            return result;
        }

        public Guid DecodeGuid()
        {
            var array = new byte[16];
            ReadAllRequiredBytes(array);
            return new Guid(array);
        }

        private void ReadAllRequiredBytes(byte[] array)
        {
            int read = this.stream.ReadAllRequiredBytes(array, 0, array.Length);
            if (read != array.Length)
            {
                throw new SerializationException(
                    $"Unexpected end of stream: '{array.Length - read}' bytes missing.");
            }
        }

        private int ReadIntFixed()
        {
            var value = new byte[4];
            this.stream.ReadAllRequiredBytes(value, 0, value.Length);
            int intValue = value[0]
                | value[1] << 0x8
                | value[2] << 0x10
                | value[3] << 0x18;
            return intValue;
        }

        public unsafe T[] DecodeArray<T>() where T : struct
        {
            long sizeInBytes = DecodeLong();
            int length = (int)(sizeInBytes / typeof(T).GetSizeOf());

            var buffer = AllocateColumnBatch<byte>((int)sizeInBytes);
            var result = AllocateArray<T>(length);
            FromStream(result, length, buffer.col);
            buffer.Return();

            return result;
        }

        public ColumnBatch<T> DecodeColumnBatch<T>() where T : struct
        {
            long sizeInBytes = DecodeLong();
            int usedlength = (int)((sizeInBytes - sizeof(int)) / typeof(T).GetSizeOf());

            int length = ReadIntFixed();

            var buffer = AllocateColumnBatch<byte>((int)(sizeInBytes - sizeof(int)));
            var result = AllocateColumnBatch<T>(length);
            result.UsedLength = usedlength;

            FromStream(result.col, result.UsedLength, buffer.col);
            buffer.Return();

            return result;
        }

        private void FromStream<T>(T[] blob, int numElements, byte[] buffer)
        {
            if (numElements == 0) return;
            if (numElements < 0) numElements = blob.Length;

            if ((numElements <= 0) || (numElements > blob.Length)) throw new InvalidOperationException("Out of bounds");

            this.stream.ReadAllRequiredBytes(buffer, 0, numElements * typeof(T).GetSizeOf());
            Buffer.BlockCopy(buffer, 0, blob, 0, buffer.Length);
        }

        private static readonly Lazy<DoublingArrayPool<byte>> bytePool = new Lazy<DoublingArrayPool<byte>>(MemoryManager.GetDoublingArrayPool<byte>);
        private static readonly Lazy<CharArrayPool> charArrayPool = new Lazy<CharArrayPool>(MemoryManager.GetCharArrayPool);

        public CharArrayWrapper Decode_CharArrayWrapper()
        {
            int header = DecodeInt();
            int usedLength = DecodeInt();
            int length = DecodeInt();

            CharArrayWrapper result;
            if (usedLength == 0)
                charArrayPool.Value.Get(out result, 1);
            else
                charArrayPool.Value.Get(out result, usedLength);

            if (header == 0)
            {
                var buffer = AllocateColumnBatch<byte>(result.UsedLength * sizeof(char));
                FromStream(result.charArray.content, result.UsedLength, buffer.col);
                buffer.Return();
            }
            else
            {
                var encodedSize = DecodeInt();
                byte[] buffer;

                if (encodedSize > 0)
                    bytePool.Value.Get(out buffer, encodedSize);
                else
                    bytePool.Value.Get(out buffer, 1);

                this.stream.ReadAllRequiredBytes(buffer, 0, encodedSize);
                Encoding.UTF8.GetChars(buffer, 0, encodedSize, result.charArray.content, 0);
                bytePool.Value.Return(buffer);
            }

            return result;
        }

        public string[] DecodeStringArray()
        {
            var cb = Decode_ColumnBatch_String();
            string[] result = new string[cb.UsedLength];
            cb.col.CopyTo(result, 0);
            cb.ReturnClear();
            return result;
        }

        public ColumnBatch<string> Decode_ColumnBatch_String()
        {
            var usedLength = ReadIntFixed();
            if (usedLength == 0)
            {
                var result = AllocateColumnBatch<string>(Config.DataBatchSize);
                result.UsedLength = usedLength;
                return result;
            }

            int maxLength = ReadIntFixed();

            if (maxLength <= short.MaxValue)
            {
                var lengths = DecodeColumnBatch<short>();
                var caw = Decode_CharArrayWrapper();

                var result = AllocateColumnBatch<string>(Config.DataBatchSize);
                result.UsedLength = lengths.UsedLength;

                int offset = 0;
                for (int i = 0; i < lengths.UsedLength; i++)
                {
                    if (lengths.col[i] == -1)
                    {
                        result.col[i] = null;
                    }
                    else
                    {
                        result.col[i] = caw.charArray.contentString.Substring(offset + 2, lengths.col[i]);
                        offset += lengths.col[i];
                    }
                }

                caw.Return();
                lengths.Return();
                return result;
            }
            else
            {
                var result = AllocateColumnBatch<string>(Config.DataBatchSize);
                result.UsedLength = ReadIntFixed();
                for (int i = 0; i < result.UsedLength; i++)
                {
                    result.col[i] = DecodeString();
                }
                return result;
            }
        }
    }
}
