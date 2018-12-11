// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Threading;

namespace Microsoft.StreamProcessing
{
    internal static class RxReplacements
    {
        public static void SynchronousForEach<T>(this IObservable<T> source, Action<T> action)
        {
            SynchronousForEachWorker<T>.DoIt(source, action);
        }

        private sealed class SynchronousForEachWorker<T> : IObserver<T>, IDisposable
        {
            private Action<T> action;
            private AutoResetEvent mutex;
            private Exception exception = null;

            private SynchronousForEachWorker() { }

            public static void DoIt(IObservable<T> observable, Action<T> action)
            {
                Contract.Requires(observable != null);
                Contract.Requires(action != null);

                var me = new SynchronousForEachWorker<T>
                {
                    action = action,
                    mutex = new AutoResetEvent(false)
                };

                IDisposable disp = observable.Subscribe(me);
                me.mutex.WaitOne();
                if (disp != null) disp.Dispose();
                if (me.exception != null) throw me.exception;
                return;
            }

            public void OnCompleted() => this.mutex.Set();

            public void OnError(Exception error)
            {
                this.exception = error;
                this.mutex.Set();
            }

            public void OnNext(T value)
            {
                if (this.exception != null)
                {
                    OnCompleted();
                    return;
                }
                try
                {
                    this.action(value);
                }
                catch (Exception e)
                {
                    OnError(e);
                }
            }

            public void Dispose() => this.mutex?.Dispose();
        }

    }
}