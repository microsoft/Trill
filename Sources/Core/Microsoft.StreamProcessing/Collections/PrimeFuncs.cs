// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Runtime.CompilerServices;

namespace Microsoft.StreamProcessing.Internal.Collections
{
    internal static class PrimeFuncs
    {
        public static readonly int[] Primes =
        {
            1, 2, 5, 11, 17, 37, 67, 131, 257, 521, 1031, 2053, 4099, 8209, 16411,
            32771, 65537, 131101, 262147, 524309, 1048583, 2097169, 4194319, 8388617,
            16777259, 33554467, 67108879, 134217757, 268435459, 536870923,
            1073741827, 2147483629
        };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int FindIndexOfPrimeGreaterOrEqualTo(int min)
        {
            Contract.Requires(min >= 0 && min < Primes[Primes.Length - 1]);

            int index = Array.BinarySearch(Primes, min);
            if (index < 0)
            {
                index = ~index;
            }

            return index;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int FindPrimeGreaterOrEqualTo(int min)
        {
            int index = FindIndexOfPrimeGreaterOrEqualTo(min);
            return Primes[index];
        }
    }
}
