// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace Microsoft.StreamProcessing
{
    internal sealed class NullScheduler : IInternalScheduler
    {
        public NullScheduler() { }

        public IStreamObserver<TK, TP> RegisterStreamObserver<TK, TP>(IStreamObserver<TK, TP> o, Guid? classId = null) => o;

        public void Stop() { }

        public int MapArity => 1;

        public int ReduceArity => 1;
    }
}
