// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;

namespace Microsoft.StreamProcessing.Signal.UDO
{
    [DataContract]
    internal sealed class SignalWindowOperatorPipe<TSource, TResult> : ISignalWindowObserver<TSource>, ISignalWindowObservable<TResult>, IDisposable
    {
        [SchemaSerialization]
        private Expression<Func<BaseWindow<TSource>, BaseWindow<TResult>, BaseWindow<TResult>>> UpdateExpr { get; }
        private Func<BaseWindow<TSource>, BaseWindow<TResult>, BaseWindow<TResult>> UpdateFunc { get; }
        [SchemaSerialization]
        private Expression<Func<BaseWindow<TSource>, BaseWindow<TResult>, BaseWindow<TResult>>> RecomputeExpr { get; }
        private Func<BaseWindow<TSource>, BaseWindow<TResult>, BaseWindow<TResult>> RecomputeFunc { get; }

        [SchemaSerialization]
        private int OutputWindowSize { get; }
        [SchemaSerialization]
        private ISignalWindowObserver<TResult> Observer;
        [SchemaSerialization]
        private BaseWindow<TResult> OutputWindow;

        public SignalWindowOperatorPipe(ISignalWindowObservable<TSource> source, ISignalWindowOperator<TSource, TResult> signalOperator)
        {
            UpdateExpr = signalOperator.Update();
            UpdateFunc = UpdateExpr.Compile();
            RecomputeExpr = signalOperator.Recompute();
            RecomputeFunc = RecomputeExpr.Compile();
            OutputWindowSize = signalOperator.GetOutputWindowSize().Compile().Invoke();
            source.Subscribe(this);
        }

        private SignalWindowOperatorPipe(
            ISignalWindowObservable<TSource> source,
            Expression<Func<BaseWindow<TSource>, BaseWindow<TResult>, BaseWindow<TResult>>> updateExpr,
            Func<BaseWindow<TSource>, BaseWindow<TResult>, BaseWindow<TResult>> updateFunc,
            Expression<Func<BaseWindow<TSource>, BaseWindow<TResult>, BaseWindow<TResult>>> recomputeExpr,
            Func<BaseWindow<TSource>, BaseWindow<TResult>, BaseWindow<TResult>> recomputeFunc,
            int outputWindowSize)
        {
            UpdateExpr = updateExpr;
            UpdateFunc = updateFunc;
            RecomputeExpr = recomputeExpr;
            RecomputeFunc = recomputeFunc;
            OutputWindowSize = outputWindowSize;

            var capacity = Utility.Power2Ceil(outputWindowSize + 1);
            var array = new TResult[capacity];
            OutputWindow = new BaseWindow<TResult>(array, outputWindowSize);

            source.Subscribe(this);
        }

        public object Clone(ISignalWindowObservable<TSource> source)
        {
            var cloned = new SignalWindowOperatorPipe<TSource, TResult>(source, UpdateExpr, UpdateFunc, RecomputeExpr, RecomputeFunc, OutputWindowSize);
            return Observer == null ? cloned : Observer.Clone(cloned);
        }

        public void Subscribe(ISignalWindowObserver<TResult> observer)
        {
            Contract.Requires(observer != null);

            Observer = observer;
        }

        public int WindowSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => OutputWindowSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnInit(long time, BaseWindow<TSource> inputWindow)
        {
            // outputWindow.Clear();                        // Reset output window
            OutputWindow.tail = 0;
            RecomputeFunc(inputWindow, OutputWindow);       // Compute result
            OutputWindow.tail = OutputWindowSize;           // Set output index
            Observer.OnInit(time, OutputWindow);            // Notify observer
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnHop(long time, BaseWindow<TSource> inputWindow)
        {
            // outputWindow.Clear();                        // Reset output window
            OutputWindow.tail = 0;
            UpdateFunc(inputWindow, OutputWindow);          // Update result
            OutputWindow.tail = OutputWindowSize;           // Set output index
            Observer.OnInit(time, OutputWindow);            // Notify observer
        }

        public void Dispose() => OutputWindow.Dispose();
    }
}
