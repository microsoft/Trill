// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Signal.UDO
{
    internal sealed class AutoCorrelationDoubleOperator : ISignalWindowOperator<double, double>
    {
        private readonly double[] linearized;

        public AutoCorrelationDoubleOperator(int length)
            => this.linearized = new double[length];

        private BaseWindow<double> AutoCorrelate(BaseWindow<double> inputWindow, BaseWindow<double> outputWindow)
        {
            inputWindow.Linearize(this.linearized);
            this.linearized.AutoCorrelation(outputWindow.Items);
            return outputWindow;
        }

        public Expression<Func<int>> GetOutputWindowSize() => () => 2 * this.linearized.Length - 1;

        public Expression<Func<BaseWindow<double>, BaseWindow<double>, BaseWindow<double>>> Update()
            => (inputWindow, outputWindow) => AutoCorrelate(inputWindow, outputWindow);

        public Expression<Func<BaseWindow<double>, BaseWindow<double>, BaseWindow<double>>> Recompute()
            => (inputWindow, outputWindow) => AutoCorrelate(inputWindow, outputWindow);
    }
}
