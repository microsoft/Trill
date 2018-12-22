// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Signal.UDO
{
    internal sealed class CorrelationDoubleOperator : ISignalWindowOperator<double, double>
    {
        private readonly double[] weights;
        private readonly double[] linearized;

        public CorrelationDoubleOperator(int length, double[] weights)
        {
            this.weights = weights;
            this.linearized = new double[length];
        }

        private BaseWindow<double> Correlate(BaseWindow<double> inputWindow, BaseWindow<double> outputWindow)
        {
            Linearize(inputWindow);
            this.linearized.CrossCorrelation(this.weights, outputWindow.Items);
            return outputWindow;
        }

        private unsafe void Linearize(BaseWindow<double> inputWindow)
        {
            var range = inputWindow.CurrentRange();
            fixed (double* pa = inputWindow.Items)
            fixed (double* pb = this.linearized)
            {
                double* ptrA = pa + range.First.Head;
                double* ptrB = pb;

                for (int i = 0; i < range.First.Length; i++)
                {
                    *ptrB = *ptrA;
                    ptrA++;
                    ptrB++;
                }

                ptrA = pa + range.Second.Head;
                for (int i = 0; i < range.Second.Length; i++)
                {
                    *ptrB = *ptrA;
                    ptrA++;
                    ptrB++;
                }
            }
        }

        public Expression<Func<int>> GetOutputWindowSize() => () => 2 * this.linearized.Length - 1;

        public Expression<Func<BaseWindow<double>, BaseWindow<double>, BaseWindow<double>>> Update()
            => (inputWindow, outputWindow) => Correlate(inputWindow, outputWindow);

        public Expression<Func<BaseWindow<double>, BaseWindow<double>, BaseWindow<double>>> Recompute()
            => (inputWindow, outputWindow) => Correlate(inputWindow, outputWindow);
    }
}
