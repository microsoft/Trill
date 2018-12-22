// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing.Signal.UDO
{
    [DataContract]
    internal sealed class WindowPipeline<TSource, TResult>
    {
        public bool IsEmpty { get; } // checks if operatorPipeline is identity

        private ISignalWindowObserver<TSource> PipelineHead { get; }

        private BaseSignalWindowObservable<TSource> InputObservable { get; }

        public WindowPipeline(Func<ISignalWindowObservable<TSource>, ISignalWindowObservable<TResult>> operatorPipeline, int inputWindowSize)
        {
            this.InputObservable = new BaseSignalWindowObservable<TSource>(inputWindowSize);

            // Create a pipeline of operators. The first operator will
            // subscribe to inputObservable and set inputObservable.observer.
            var pipelineTail = operatorPipeline(InputObservable);
            PipelineHead = InputObservable.observer;
            IsEmpty = ReferenceEquals(InputObservable, pipelineTail);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CreateNewPipeline(out ISignalWindowObserver<TSource> firstObserver, out ISignalWindowObservable<TResult> lastObservable)
        {
            lastObservable = (ISignalWindowObservable<TResult>)PipelineHead.Clone(InputObservable);
            firstObserver = InputObservable.observer;
        }
    }

    [DataContract]
    internal sealed class WindowAggregatePipeline<TSource, TResult>
    {
        private ISignalWindowObserver<TSource> PipelineHead { get; }

        private BaseSignalWindowObservable<TSource> InputObservable { get; }

        public WindowAggregatePipeline(Func<ISignalWindowObservable<TSource>, ISignalObservable<TResult>> operatorPipeline, int inputWindowSize)
        {
            InputObservable = new BaseSignalWindowObservable<TSource>(inputWindowSize);

            // Create a pipeline of operators. The first operator will
            // subscribe to inputObservable and set inputObservable.observer.
            PipelineHead = InputObservable.observer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CreateNewPipeline(out ISignalWindowObserver<TSource> firstObserver, out ISignalObservable<TResult> lastObservable)
        {
            lastObservable = (ISignalObservable<TResult>)PipelineHead.Clone(InputObservable);
            firstObserver = InputObservable.observer;
        }
    }

}
