// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace Microsoft.StreamProcessing.Signal.UDO
{
    internal sealed class LinearFeedforwardDoubleFilter : IDigitalFilter<double>
    {
        public readonly double[] feedForwardCoefficients;

        public readonly bool setMissingDataToNull;

        public LinearFeedforwardDoubleFilter(double[] feedForwardCoefficients, bool setMissingDataToNull)
        {
            this.feedForwardCoefficients = feedForwardCoefficients;
            this.setMissingDataToNull = setMissingDataToNull;
        }

        public Expression<Func<bool>> SetMissingDataToNull() => () => setMissingDataToNull;

        public Expression<Func<int>> GetFeedForwardSize() => () => feedForwardCoefficients.Length;

        public Expression<Func<int>> GetFeedBackwardSize() => () => 0;

        public Expression<Func<BaseWindow<double>, BaseWindow<double>, double>> Compute()
            => (inputWindow, outputWindow) => Compute(inputWindow, outputWindow);

        public double Compute(BaseWindow<double> input, BaseWindow<double> output)
        {
            double sum = 0.0;
            var range = input.CurrentRange();
            int j = 0;

            for (int i = range.First.Head; i < range.First.Tail; i++, j++)
            {
                sum += input.Items[i] * feedForwardCoefficients[j];
            }

            for (int i = range.Second.Head; i < range.Second.Tail; i++, j++)
            {
                sum += input.Items[i] * feedForwardCoefficients[j];
            }

            return sum;
        }

        public unsafe double ComputeUnsafe(BaseWindow<double> input, BaseWindow<double> output)
        {
            double sum = 0.0;
            var range = input.CurrentRange();

            fixed (double* pa = input.Items)
            fixed (double* pb = feedForwardCoefficients)
            {
                double* ptrB = pb;
                double* ptrA = pa + range.First.Head;
                double* ptrEnd = pa + range.First.Tail;

                for (; ptrA != ptrEnd; ptrA++, ptrB++)
                {
                    sum += (*ptrA) * (*ptrB);
                }

                ptrA = pa + range.Second.Head;
                ptrEnd = pa + range.Second.Tail;
                for (; ptrA != ptrEnd; ptrA++, ptrB++)
                {
                    sum += (*ptrA) * (*ptrB);
                }
            }

            return sum;
        }
    }

    internal sealed class LinearDoubleFilter : IDigitalFilter<double>
    {
        public readonly double[] feedForwardCoefficients;

        public readonly double[] feedBackwardCoefficients;

        public readonly bool setMissingDataToNull;

        public LinearDoubleFilter(double[] feedForwardCoefficients, double[] feedBackwardCoefficients, bool setMissingDataToNull)
        {
            this.feedForwardCoefficients = feedForwardCoefficients;
            this.feedBackwardCoefficients = feedBackwardCoefficients;
            this.setMissingDataToNull = setMissingDataToNull;
        }

        public Expression<Func<bool>> SetMissingDataToNull() => () => setMissingDataToNull;

        public Expression<Func<int>> GetFeedForwardSize() => () => feedForwardCoefficients.Length;

        public Expression<Func<int>> GetFeedBackwardSize() => () => feedBackwardCoefficients.Length;

        public Expression<Func<BaseWindow<double>, BaseWindow<double>, double>> Compute()
            => (inputWindow, outputWindow) => Compute(inputWindow, outputWindow);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public double Compute(BaseWindow<double> input, BaseWindow<double> output)
        {
            double sum = 0.0;
            var range = input.CurrentRange();
            int j = 0;

            for (int i = range.First.Head; i < range.First.Tail; i++, j++)
            {
                sum += input.Items[i] * feedForwardCoefficients[j];
            }

            for (int i = range.Second.Head; i < range.Second.Tail; i++, j++)
            {
                sum += input.Items[i] * feedForwardCoefficients[j];
            }

            range = output.CurrentRange();
            j = 0;
            for (int i = range.First.Head; i < range.First.Tail; i++, j++)
            {
                sum += output.Items[i] * feedBackwardCoefficients[j];
            }

            for (int i = range.Second.Head; i < range.Second.Tail; i++, j++)
            {
                sum += output.Items[i] * feedBackwardCoefficients[j];
            }

            return sum;
        }
    }

    internal sealed class LinearComplexFilter : IDigitalFilter<Complex>
    {
        public readonly double[] feedForwardCoefficients;

        public readonly double[] feedBackwardCoefficients;

        public readonly bool setMissingDataToNull;

        public LinearComplexFilter(double[] feedForwardCoefficients, double[] feedBackwardCoefficients, bool setMissingDataToNull)
        {
            this.feedForwardCoefficients = feedForwardCoefficients;
            this.feedBackwardCoefficients = feedBackwardCoefficients;
            this.setMissingDataToNull = setMissingDataToNull;
        }

        public Expression<Func<bool>> SetMissingDataToNull() => () => setMissingDataToNull;

        public Expression<Func<int>> GetFeedForwardSize() => () => feedForwardCoefficients.Length;

        public Expression<Func<int>> GetFeedBackwardSize() => () => feedBackwardCoefficients.Length;

        public Expression<Func<BaseWindow<Complex>, BaseWindow<Complex>, Complex>> Compute()
            => (inputWindow, outputWindow) => ComputeUnsafe(inputWindow, outputWindow);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe Complex ComputeUnsafe(BaseWindow<Complex> input, BaseWindow<Complex> output)
        {
            Complex sum = 0.0;
            var range = input.CurrentRange();

            fixed (Complex* pa = input.Items)
            fixed (double* pb = feedForwardCoefficients)
            {
                double* ptrB = pb;
                Complex* ptrA = pa + range.First.Head;
                Complex* ptrEnd = pa + range.First.Tail;

                for (; ptrA != ptrEnd; ptrA++, ptrB++)
                {
                    sum += (*ptrA) * (*ptrB);
                }

                ptrA = pa + range.Second.Head;
                ptrEnd = pa + range.Second.Tail;
                for (; ptrA != ptrEnd; ptrA++, ptrB++)
                {
                    sum += (*ptrA) * (*ptrB);
                }
            }

            range = output.CurrentRange();

            fixed (Complex* pa = output.Items)
            fixed (double* pb = feedBackwardCoefficients)
            {

                double* ptrB = pb;
                Complex* ptrA = pa + range.First.Head;
                Complex* ptrEnd = pa + range.First.Tail;

                for (; ptrA != ptrEnd; ptrA++, ptrB++)
                {
                    sum += (*ptrA) * (*ptrB);
                }

                ptrA = pa + range.Second.Head;
                ptrEnd = pa + range.Second.Tail;
                for (; ptrA != ptrEnd; ptrA++, ptrB++)
                {
                    sum += (*ptrA) * (*ptrB);
                }
            }

            return sum;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Complex Compute(BaseWindow<Complex> input, BaseWindow<Complex> output)
        {
            Complex sum = 0.0;
            var range = input.CurrentRange();
            int j = 0;

            for (int i = range.First.Head; i < range.First.Tail; i++, j++)
            {
                sum += input.Items[i] * feedForwardCoefficients[j];
            }

            for (int i = range.Second.Head; i < range.Second.Tail; i++, j++)
            {
                sum += input.Items[i] * feedForwardCoefficients[j];
            }

            range = output.CurrentRange();
            j = 0;
            for (int i = range.First.Head; i < range.First.Tail; i++, j++)
            {
                sum += output.Items[i] * feedBackwardCoefficients[j];
            }

            for (int i = range.Second.Head; i < range.Second.Tail; i++, j++)
            {
                sum += output.Items[i] * feedBackwardCoefficients[j];
            }

            return sum;
        }
    }

}
