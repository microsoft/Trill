// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Class to set scheduling strategy for Trill (via Microsoft.StreamProcessing.Config.StreamScheduler)
    /// </summary>
    public class StreamScheduler
    {
        internal IInternalScheduler scheduler;

        /// <summary>
        /// Library mode (no threads owned by Trill).
        /// </summary>
        /// <returns></returns>
        public static StreamScheduler Null()
        {
            return new StreamScheduler(new NullScheduler());
        }

        /// <summary>
        /// Trill creates and owns the specified number of threads for scheduling the processing.
        /// </summary>
        /// <param name="numCores"></param>
        /// <param name="affinitizeThreads"></param>
        /// <returns></returns>
        public static StreamScheduler OwnedThreads(int numCores, bool affinitizeThreads = false)
        {
            return new StreamScheduler(new OwnedThreadsScheduler(numCores, affinitizeThreads));
        }

        private StreamScheduler(IInternalScheduler scheduler)
        {
            this.scheduler = scheduler;
        }

        internal IStreamObserver<TK, TP> RegisterStreamObserver<TK, TP>(IStreamObserver<TK, TP> o, Guid? classId = null)
        {
            return this.scheduler.RegisterStreamObserver(o, classId);
        }

        /// <summary>
        /// Stop the scheduler.
        /// </summary>
        public void Stop()
        {
            this.scheduler.Stop();
        }
    }

    internal interface IInternalScheduler
    {
        IStreamObserver<TK, TP> RegisterStreamObserver<TK, TP>(IStreamObserver<TK, TP> o, Guid? classId = null);
        void Stop();

        int MapArity { get; }
        int ReduceArity { get; }
    }
}
