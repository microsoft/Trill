// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;

namespace Microsoft.StreamProcessing.Signal
{
    // For more details about window functions see https://en.wikipedia.org/wiki/Window_function
    /// <summary>
    /// Static class that provides helper methods for creating window arrays
    /// </summary>
    public static class WindowFunctionHelper
    {
        /// <summary>
        /// Create a rectangular window array of the given length
        /// </summary>
        /// <param name="length">The length of the desired array</param>
        /// <returns>Rectangular window array of the given length</returns>
        public static double[] RectangularWindow(this int length)
        {
            Contract.Requires(length > 0);

            double[] result = new double[length];

            for (int i = 0; i < length; i++)
            {
                result[i] = 1.0;
            }

            return result;
        }

        /// <summary>
        /// Create a triangular window array of the given length
        /// </summary>
        /// <param name="length">The length of the desired array</param>
        /// <returns>Triangular window array of the given length</returns>
        public static double[] TriangularWindow(this int length)
        {
            Contract.Requires(length > 0);

            double[] result = new double[length];
            double denominator = ((double)length - 1) / 2;

            for (int i = 0; i < length; i++)
            {
                result[i] = 1.0 - Math.Abs(i / denominator - 1);
            }

            return result;
        }

        /// <summary>
        /// Create a Hann window array of the given length
        /// </summary>
        /// <param name="length">The length of the desired array</param>
        /// <returns>Hann window array of the given length</returns>
        public static double[] HannWindow(this int length) => HammingWindow(length, 0.5, 0.5);

        /// <summary>
        /// Create a Hamming window array of the given length
        /// </summary>
        /// <param name="length">The length of the desired array</param>
        /// <returns>Hamming window array of the given length</returns>
        public static double[] HammingWindow(this int length) => HammingWindow(length, 25.0 / 46, 21.0 / 46);

        private static double[] HammingWindow(this int length, double alpha, double beta)
        {
            Contract.Requires(length > 0);

            double[] result = new double[length];
            double p = 2 * Math.PI / (length - 1);

            for (int i = 0; i < length; i++)
            {
                result[i] = alpha - beta * Math.Cos(p * i);
            }

            return result;
        }

        /// <summary>
        /// Create a Gaussian window array of the given length
        /// </summary>
        /// <param name="length">The length of the desired array</param>
        /// <param name="sigma">The sigma value for the Gaussian window</param>
        /// <returns>Gaussian window array of the given length</returns>
        public static double[] GaussianWindow(this int length, double sigma)
        {
            Contract.Requires(length > 0);
            Contract.Requires(sigma > 0 && sigma <= 0.5);

            double[] result = new double[length];
            double invSigma = 1 / sigma;
            double denominator = sigma * (length - 1) / 2;

            for (int i = 0; i < length; i++)
            {
                double exp = i / denominator - invSigma;
                result[i] = Math.Exp(-0.5 * exp * exp);
            }

            return result;
        }

        /// <summary>
        /// Create a random window array of the given length
        /// </summary>
        /// <param name="length">The length of the desired array</param>
        /// <returns>Random window array of the given length</returns>
        public static double[] RandomWindow(this int length)
        {
            Contract.Requires(length > 0);

            var r = new Random();
            double[] result = new double[length];

            for (int i = 0; i < length; i++)
            {
                result[i] = r.NextDouble();
            }

            return result;
        }
    }
}
