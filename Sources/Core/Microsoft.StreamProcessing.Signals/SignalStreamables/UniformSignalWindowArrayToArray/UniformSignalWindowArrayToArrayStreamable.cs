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
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing.Signal.UDO
{
    internal sealed class UniformSignalWindowArrayToArrayStreamable<TKey, TSource, TResult> : UnaryStreamable<TKey, TSource[], TResult[]>
    {
        internal readonly int WindowSize;
        internal readonly Func<ISignalWindowObservable<TSource>, ISignalWindowObservable<TResult>> OperatorPipeline;

        public UniformSignalWindowArrayToArrayStreamable(
            WindowedUniformSignal<TKey, TSource> source,
            int outputWindowSize, Func<ISignalWindowObservable<TSource>, ISignalWindowObservable<TResult>> operatorPipeline)
            : base(source.Stream, source.Properties.WindowedPipelineArrayToArray(operatorPipeline, outputWindowSize))
        {
            Contract.Requires(source != null);

            WindowSize = source.WindowSize;
            OperatorPipeline = operatorPipeline;

            Initialize();
        }

        internal override IStreamObserver<TKey, TSource[]> CreatePipe(IStreamObserver<TKey, TResult[]> observer)
            => new UniformSignalWindowArrayToArrayPipe<TKey, TSource, TResult>(this, observer);

        // TODO: CODEGEN
        protected override bool CanGenerateColumnar() => false;
    }
}