// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using System.Numerics;

namespace Microsoft.StreamProcessing.Signal.UDO
{
    internal sealed class DFTDoubleOperator : ISignalWindowOperator<double, Complex>
    {
        private readonly int size;

        private readonly Complex[] spectrum;

        private readonly Complex[] powers;

        public DFTDoubleOperator(int size)
        {
            this.size = size;
            this.spectrum = new Complex[size];
            this.powers = new Complex[size];

            // Init powers
            var minusW = Complex.Exp(new Complex(0, Math.PI * 2 / powers.Length));
            powers[0] = 1;
            for (int i = 1; i < powers.Length; i++) { powers[i] = powers[i - 1] * minusW; }
        }

        private void Update(Complex[] spectrum, Complex oldValue, Complex newValue)
        {
            Complex diff = newValue - oldValue;
            for (int i = 0; i < spectrum.Length; i++)
            {
                spectrum[i] = (spectrum[i] + diff) * powers[i];
            }
        }

        private BaseWindow<Complex> UpdateSpectrum(BaseWindow<double> inputWindow, BaseWindow<Complex> outputWindow)
        {
            var oldRange = inputWindow.OldRange();
            var newRange = inputWindow.NewRange();

            int iterDel = oldRange.IteratorStart;
            int iterIns = newRange.IteratorStart;

            while (oldRange.Iterate(ref iterDel) && newRange.Iterate(ref iterIns))
            {
                Update(spectrum, inputWindow.Items[iterDel], inputWindow.Items[iterIns]);
            }

            Array.Copy(spectrum, 0, outputWindow.Items, 0, size);

            return outputWindow;
        }

        private BaseWindow<Complex> RecomputeSpectrum(BaseWindow<double> inputWindow, BaseWindow<Complex> outputWindow)
        {
            spectrum.Clear();
            var range = inputWindow.CurrentRange();

            for (int i = range.First.Head; i < range.First.Tail; i++)
            {
                Update(spectrum, 0.0, inputWindow.Items[i]);
            }

            for (int i = range.Second.Head; i < range.Second.Tail; i++)
            {
                Update(spectrum, 0.0, inputWindow.Items[i]);
            }

            Array.Copy(spectrum, 0, outputWindow.Items, 0, size);

            return outputWindow;
        }

        public Expression<Func<int>> GetOutputWindowSize()
            => () => size;

        public Expression<Func<BaseWindow<double>, BaseWindow<Complex>, BaseWindow<Complex>>> Update()
            => (inputWindow, outputWindow) => UpdateSpectrum(inputWindow, outputWindow);

        public Expression<Func<BaseWindow<double>, BaseWindow<Complex>, BaseWindow<Complex>>> Recompute()
            => (inputWindow, outputWindow) => RecomputeSpectrum(inputWindow, outputWindow);
    }

    internal sealed class DFTComplexOperator : ISignalWindowOperator<Complex, Complex>
    {
        private readonly int size;

        private readonly Complex[] spectrum;

        private readonly Complex[] powers;

        public DFTComplexOperator(int size)
        {
            this.size = size;
            this.spectrum = new Complex[size];
            this.powers = new Complex[size];

            // Init powers
            var minusW = Complex.Exp(new Complex(0, Math.PI * 2 / powers.Length));
            powers[0] = 1;
            for (int i = 1; i < powers.Length; i++) { powers[i] = powers[i - 1] * minusW; }
        }

        private void Update(Complex[] spectrum, Complex oldValue, Complex newValue)
        {
            Complex diff = newValue - oldValue;
            for (int i = 0; i < spectrum.Length; i++)
            {
                spectrum[i] = (spectrum[i] + diff) * powers[i];
            }
        }

        private BaseWindow<Complex> UpdateSpectrum(BaseWindow<Complex> inputWindow, BaseWindow<Complex> outputWindow)
        {
            var oldRange = inputWindow.OldRange();
            var newRange = inputWindow.NewRange();

            int iterDel = oldRange.IteratorStart;
            int iterIns = newRange.IteratorStart;

            while (oldRange.Iterate(ref iterDel) && newRange.Iterate(ref iterIns))
            {
                Update(spectrum, inputWindow.Items[iterDel], inputWindow.Items[iterIns]);
            }

            Array.Copy(spectrum, 0, outputWindow.Items, 0, size);

            return outputWindow;
        }

        private BaseWindow<Complex> RecomputeSpectrum(BaseWindow<Complex> inputWindow, BaseWindow<Complex> outputWindow)
        {
            spectrum.Clear();
            var range = inputWindow.CurrentRange();

            for (int i = range.First.Head; i < range.First.Tail; i++)
            {
                Update(spectrum, 0.0, inputWindow.Items[i]);
            }

            for (int i = range.Second.Head; i < range.Second.Tail; i++)
            {
                Update(spectrum, 0.0, inputWindow.Items[i]);
            }

            Array.Copy(spectrum, 0, outputWindow.Items, 0, size);

            return outputWindow;
        }

        public Expression<Func<int>> GetOutputWindowSize()
            => () => size;

        public Expression<Func<BaseWindow<Complex>, BaseWindow<Complex>, BaseWindow<Complex>>> Update()
            => (inputWindow, outputWindow) => UpdateSpectrum(inputWindow, outputWindow);

        public Expression<Func<BaseWindow<Complex>, BaseWindow<Complex>, BaseWindow<Complex>>> Recompute()
            => (inputWindow, outputWindow) => RecomputeSpectrum(inputWindow, outputWindow);
    }

}
