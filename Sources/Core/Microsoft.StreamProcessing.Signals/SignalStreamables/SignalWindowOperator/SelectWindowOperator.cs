// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Signal.UDO
{
    internal sealed class SelectWindowOperator<TSource, TResult> : ISignalWindowOperator<TSource, TResult>
    {
        private readonly Action<TSource[], TResult[]> windowAction;
        private readonly TSource[] linearized;
        private readonly int outputSize;

        public SelectWindowOperator(Action<TSource[], TResult[]> windowAction, int inputSize, int outputSize)
        {
            this.windowAction = windowAction;
            this.linearized = new TSource[inputSize];
            this.outputSize = outputSize;
        }

        public Expression<Func<int>> GetOutputWindowSize() => () => this.outputSize;

        public Expression<Func<BaseWindow<TSource>, BaseWindow<TResult>, BaseWindow<TResult>>> Update()
            => (inputWindow, outputWindow) => Compute(inputWindow, outputWindow);

        public Expression<Func<BaseWindow<TSource>, BaseWindow<TResult>, BaseWindow<TResult>>> Recompute()
            => (inputWindow, outputWindow) => Compute(inputWindow, outputWindow);

        public BaseWindow<TResult> Compute(BaseWindow<TSource> inputWindow, BaseWindow<TResult> outputWindow)
        {
            inputWindow.Linearize(this.linearized);
            windowAction(this.linearized, outputWindow.Items);
            return outputWindow;
        }
    }
}
