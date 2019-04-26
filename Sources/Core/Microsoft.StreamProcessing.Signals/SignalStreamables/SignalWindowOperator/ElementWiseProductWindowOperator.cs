// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using System.Numerics;

namespace Microsoft.StreamProcessing.Signal.UDO
{
    internal sealed class ElementWiseProductDoubleOperator : ISignalWindowOperator<double, double>
    {
        private readonly double[] weights;

        public ElementWiseProductDoubleOperator(double[] weights) => this.weights = weights;

        public Expression<Func<int>> GetOutputWindowSize() => () => weights.Length;

        public Expression<Func<BaseWindow<double>, BaseWindow<double>, BaseWindow<double>>> Update()
            => (inputWindow, outputWindow) => Compute(inputWindow, outputWindow);

        public Expression<Func<BaseWindow<double>, BaseWindow<double>, BaseWindow<double>>> Recompute()
            => (inputWindow, outputWindow) => Compute(inputWindow, outputWindow);

        public BaseWindow<double> Compute(BaseWindow<double> inputWindow, BaseWindow<double> outputWindow)
        {
            var inputRange = inputWindow.CurrentRange();
            int j = 0;

            for (int i = inputRange.First.Head; i < inputRange.First.Tail; i++, j++)
            {
                outputWindow.Items[j] = inputWindow.Items[i] * weights[j];
            }

            for (int i = inputRange.Second.Head; i < inputRange.Second.Tail; i++, j++)
            {
                outputWindow.Items[j] = inputWindow.Items[i] * weights[j];
            }

            return outputWindow;
        }
    }

    internal sealed class ElementWiseProductComplexOperator : ISignalWindowOperator<Complex, Complex>
    {
        public readonly double[] weights;

        public readonly bool setMissingDataToNull;

        public ElementWiseProductComplexOperator(double[] weights, bool setMissingDataToNull)
        {
            this.weights = weights;
            this.setMissingDataToNull = setMissingDataToNull;
        }

        public Expression<Func<int>> GetOutputWindowSize() => () => weights.Length;

        public Expression<Func<BaseWindow<Complex>, BaseWindow<Complex>, BaseWindow<Complex>>> Update()
            => (inputWindow, outputWindow) => Compute(inputWindow, outputWindow);

        public Expression<Func<BaseWindow<Complex>, BaseWindow<Complex>, BaseWindow<Complex>>> Recompute()
            => (inputWindow, outputWindow) => Compute(inputWindow, outputWindow);

        public BaseWindow<Complex> Compute(BaseWindow<Complex> inputWindow, BaseWindow<Complex> outputWindow)
        {
            var inputRange = inputWindow.CurrentRange();
            int j = 0;

            for (int i = inputRange.First.Head; i < inputRange.First.Tail; i++, j++)
            {
                outputWindow.Items[j] = inputWindow.Items[i] * weights[j];
            }

            for (int i = inputRange.Second.Head; i < inputRange.Second.Tail; i++, j++)
            {
                outputWindow.Items[j] = inputWindow.Items[i] * weights[j];
            }

            return outputWindow;
        }
    }
}
