// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Threading.Tasks;

namespace Microsoft.StreamProcessing
{
    internal static class RxReplacements
    {
        public static void SynchronousForEach<T>(this IObservable<T> source, Action<T> action)
        {
            SynchronousForEachWorker<T>.DoIt(source, action);
        }

        private sealed class SynchronousForEachWorker<T>(Action<T> action) : IObserver<T>
        {
            private readonly TaskCompletionSource tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

            public static void DoIt(IObservable<T> observable, Action<T> action)
            {
                ArgumentNullException.ThrowIfNull(observable);
                ArgumentNullException.ThrowIfNull(action);

                SynchronousForEachWorker<T> worker = new(action);
                using (observable.Subscribe(worker))
                {
                    worker.tcs.Task.GetAwaiter().GetResult();
                }
            }

            public void OnCompleted() => this.tcs.TrySetResult();
            public void OnError(Exception error) => this.tcs.TrySetException(error);

            public void OnNext(T value)
            {
                if (this.tcs.Task.IsCompleted) return;
                try
                {
                    action(value);
                }
                catch (Exception e)
                {
                    this.OnError(e);
                }
            }
        }

    }
}