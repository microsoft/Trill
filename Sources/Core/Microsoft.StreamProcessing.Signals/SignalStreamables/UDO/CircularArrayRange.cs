// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing.Signal.UDO
{
    [DataContract]
    internal struct ArrayRange
    {
        [DataMember]
        public int Head;
        [DataMember]
        public int Tail;

        public int Length
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Tail - Head;
        }
    }

    [DataContract]
    internal struct CircularArrayRange
    {
        [DataMember]
        public ArrayRange First;
        [DataMember]
        public ArrayRange Second;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public CircularArrayRange(int length, int head, int tail)
        {
            if (head <= tail)
            {
                First = new ArrayRange { Head = head, Tail = tail };
                Second = new ArrayRange { Head = tail, Tail = tail };
            }
            else
            {
                First = new ArrayRange { Head = head, Tail = length };
                Second = new ArrayRange { Head = 0, Tail = tail };
            }
        }

        public int Length
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => First.Length + Second.Length;
        }

        public int IteratorStart
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => First.Head - 1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Iterate(ref int index)
        {
            if (++index >= First.Tail)
            {
                index = Second.Head;
            }
            return index != Second.Tail;
        }
    }
}
