// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using System.Numerics;


namespace Microsoft.StreamProcessing.Signal.UDO
{
    internal sealed class InverseDFTComplexOperator : ISignalWindowOperator<Complex, Complex>
    {
        private readonly int size;

        private readonly Complex[,] hermitW;

        private readonly Complex[] signal;

        public InverseDFTComplexOperator(int size)
        {
            this.size = size;
            this.hermitW = new Complex[size, size];
            this.signal = new Complex[size];

            var w = Complex.Exp(new Complex(0, Math.PI * 2 / size));

            for (int i = 0; i < size; i++)
            {
                for (int j = 0; j < size; j++)
                {
                    hermitW[i, j] = Complex.Pow(w, i * j);
                }
            }
        }

        private BaseWindow<Complex> Recompute(BaseWindow<Complex> inputWindow, BaseWindow<Complex> outputWindow)
        {
            var range = inputWindow.CurrentRange();

            for (int i = 0; i < size; i++)
            {
                signal[i] = 0.0;

                var j = 0;
                for (int k = range.First.Head; k < range.First.Tail; j++, k++)
                {
                    signal[i] += hermitW[i, j] * inputWindow.Items[k];
                }
                for (int k = range.Second.Head; k < range.Second.Tail; j++, k++)
                {
                    signal[i] += hermitW[i, j] * inputWindow.Items[k];
                }

                signal[i] /= size;
            }

            Array.Copy(signal, outputWindow.Items, size);

            return outputWindow;
        }

        public Expression<Func<int>> GetOutputWindowSize() => () => size;

        public Expression<Func<BaseWindow<Complex>, BaseWindow<Complex>, BaseWindow<Complex>>> Update()
            => (inputWindow, outputWindow) => Recompute(inputWindow, outputWindow);

        public Expression<Func<BaseWindow<Complex>, BaseWindow<Complex>, BaseWindow<Complex>>> Recompute()
            => (inputWindow, outputWindow) => Recompute(inputWindow, outputWindow);
    }
}
