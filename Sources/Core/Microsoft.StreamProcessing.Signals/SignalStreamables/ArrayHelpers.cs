// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq;
using System.Numerics;

namespace Microsoft.StreamProcessing.Signal
{
    /// <summary>
    /// Static class for providing additional methods for arrays
    /// </summary>
    internal static class ArrayExtensions
    {
        /// <summary>
        /// Reverse the order of elements in an array into a new array
        /// </summary>
        /// <typeparam name="T">The underlying array item type</typeparam>
        /// <param name="array">The array to reverse</param>
        /// <returns>A new array whose items are the reverse of the input array</returns>
        public static T[] Reverse<T>(this T[] array)
        {
            if (array == null) return null;

            var newArray = new T[array.Length];
            for (int i = 0, j = array.Length - 1; i < array.Length; i++, j--)
            {
                newArray[i] = array[j];
            }
            return newArray;
        }

        /// <summary>
        /// Functional version of the Array.Clear method (clears the elements in an array)
        /// </summary>
        /// <typeparam name="T">The underlying array item type</typeparam>
        /// <param name="array">The array to clear</param>
        /// <returns>The same array as the input, but whose items have all been cleared</returns>
        public static T[] Clear<T>(this T[] array)
        {
            Array.Clear(array, 0, array.Length);
            return array;
        }

        /// <summary>
        /// Creates a string representation of the array
        /// </summary>
        /// <typeparam name="T">The underlying array item type</typeparam>
        /// <param name="array">The array to be serialized into a string</param>
        /// <returns>The string representation of the array</returns>
        public static string MakeString<T>(this T[] array)
        {
            if (array == null) return string.Empty;
            return "[" + string.Join(", ", array) + "]";
        }

        /// <summary>
        /// Set every entry in an array to be the given value
        /// </summary>
        /// <typeparam name="T">The underlying array item type</typeparam>
        /// <param name="array">The array to populate</param>
        /// <param name="value">The value to use as every element in the array (no deep copy)</param>
        /// <returns>The input array, having all of its entries set</returns>
        public static T[] Populate<T>(this T[] array, T value)
        {
            if (array == null) return null;
            for (int i = 0; i < array.Length; i++) { array[i] = value; }
            return array;
        }
    }

    /// <summary>
    /// Static class for providing additional methods for arrays of doubles
    /// </summary>
    internal static class DoubleArrayExtensions
    {
        public static void AutoCorrelation(this double[] a, double[] c) => CrossCorrelation(a, a, c);

        public static void Convolution(this double[] a, double[] b, double[] c) => CrossCorrelation(a, b.Reverse(), c);

        public static void CrossCorrelation(this double[] a, double[] b, double[] c)
        {
            if (c.Length < a.Length + b.Length - 1) throw new ArgumentException("Invalid output array size in correlation.");

            for (int count = 1; count < b.Length; count++)
            {
                var arrayBOffset = b.Length - count;
                var sum = 0.0;
                for (int j = 0; j < count; j++)
                {
                    sum += a[j] * b[arrayBOffset + j];
                }
                c[count - 1] = sum;
            }

            for (int arrayAOffset = 0; arrayAOffset <= a.Length - b.Length; arrayAOffset++)
            {
                var sum = 0.0;
                for (int j = 0; j < b.Length; j++)
                {
                    sum += a[arrayAOffset + j] * b[j];
                }
                c[b.Length + arrayAOffset - 1] = sum;
            }

            var index = a.Length;
            for (int count = b.Length - 1; count > 0; count--)
            {
                var arrayAOffset = a.Length - count;
                var sum = 0.0;
                for (int j = 0; j < count; j++)
                {
                    sum += a[arrayAOffset + j] * b[j];
                }
                c[index++] = sum;
            }
        }

        public static void TopK(this double[] input, int topK, double[] output)
        {
            if (input == null || output == null) return;
            if (topK > input.Length || topK > output.Length) throw new ArgumentOutOfRangeException("Top-k value greater than the array size");

            var topKIndexes =
                input.Select((val, index) => new { Value = val, Index = index })
                     .OrderBy(e => e.Value)
                     .Select(e => e.Index)
                     .Take(topK)
                     .ToArray();

            for (int i = 0; i < topK; i++)
            {
                output[i] = input[topKIndexes[i]];
            }
        }

        public static void FilterTopK(this double[] input, int topK, double[] output)
        {
            if (input == null || output == null) return;
            if (input.Length != output.Length) throw new ArgumentOutOfRangeException("Output array size must match input size");
            if (topK > input.Length) throw new ArgumentOutOfRangeException("Top-k value greater than the array size");

            var topKIndexes =
                input.Select((cval, index) => new { Value = cval, Index = index })
                     .OrderBy(e => e.Value)
                     .Select(e => e.Index)
                     .Take(topK)
                     .ToArray();

            for (int i = 0; i < topKIndexes.Length; i++)
            {
                output[topKIndexes[i]] = input[topKIndexes[i]];
            }
        }

        public static double[] Sqrt(this double[] array)
        {
            if (array == null) return null;

            for (int i = 0; i < array.Length; i++)
            {
                array[i] = Math.Sqrt(array[i]);
            }
            return array;
        }
    }

    internal static class ComplexArrayExtensions
    {
        public static void AutoCorrelation(this Complex[] a, Complex[] c) => CrossCorrelation(a, a, c);

        public static void Convolution(this Complex[] a, Complex[] b, Complex[] c) => CrossCorrelation(a, b.Reverse(), c);

        public static void CrossCorrelation(this Complex[] a, Complex[] b, Complex[] c)
        {
            if (c.Length < a.Length + b.Length - 1)
            {
                throw new ArgumentException("Invalid output array size in correlation.");
            }

            for (int count = 1; count < b.Length; count++)
            {
                var arrayBOffset = b.Length - count;
                Complex sum = 0.0;
                for (int j = 0; j < count; j++)
                {
                    sum += a[j] * b[arrayBOffset + j];
                }
                c[count - 1] = sum;
            }

            for (int arrayAOffset = 0; arrayAOffset <= a.Length - b.Length; arrayAOffset++)
            {
                Complex sum = 0.0;
                for (int j = 0; j < b.Length; j++)
                {
                    sum += a[arrayAOffset + j] * b[j];
                }
                c[b.Length + arrayAOffset - 1] = sum;
            }

            var index = a.Length;
            for (int count = b.Length - 1; count > 0; count--)
            {
                var arrayAOffset = a.Length - count;
                Complex sum = 0.0;
                for (int j = 0; j < count; j++)
                {
                    sum += a[arrayAOffset + j] * b[j];
                }
                c[index++] = sum;
            }
        }

        public static void TopK(this Complex[] input, int topK, Complex[] output)
        {
            if (input == null || output == null) return;

            if (topK > input.Length || topK > output.Length)
            {
                throw new ArgumentOutOfRangeException("Top-k value greater than the array size");
            }

            var topKIndexes =
                input.Select((val, index) => new { Value = val.Magnitude, Index = index })
                     .OrderBy(e => e.Value)
                     .Select(e => e.Index)
                     .Take(topK)
                     .ToArray();

            for (int i = 0; i < topK; i++)
            {
                output[i] = input[topKIndexes[i]];
            }
        }

        public static void FilterTopK(this Complex[] input, int topK, Complex[] output)
        {
            if (input == null || output == null) return;

            if (input.Length != output.Length)
            {
                throw new ArgumentOutOfRangeException("Output array size must match input size");
            }

            if (topK > input.Length)
            {
                throw new ArgumentOutOfRangeException("Top-k value greater than the array size");
            }

            var topKIndexes =
                input.Select((val, index) => new { Value = val.Magnitude, Index = index })
                     .OrderBy(e => e.Value)
                     .Select(e => e.Index)
                     .Take(topK)
                     .ToArray();

            for (int i = 0; i < topKIndexes.Length; i++)
            {
                output[topKIndexes[i]] = input[topKIndexes[i]];
            }
        }

        public static Complex[] Power(this Complex[] spectrum)
        {
            if (spectrum == null) return null;

            var powerSpectrum = new Complex[spectrum.Length];
            for (int i = 0; i < spectrum.Length; i++)
            {
                var m = spectrum[i].Magnitude;
                powerSpectrum[i] = m * m / spectrum.Length;
            }

            return powerSpectrum;
        }

        public static Complex[] SpectrumCoherence(this Complex[] left, Complex[] right)
        {
            if (left == null || right == null) return null;

            if (left.Length != right.Length)
            {
                throw new ArgumentException("Different spectrum sizes.");
            }

            // TODO: Check if correct
            var spectrum = new Complex[left.Length];
            for (int i = 0; i < left.Length; i++)
            {
                spectrum[i] = (left[i] * right[i]).Magnitude;
            }

            return spectrum;
        }
    }
}
