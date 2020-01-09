// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using BenchmarkDotNet.Attributes;
using Microsoft.StreamProcessing;

namespace Benchmark
{
    public class HoppingWindowMinAggregateBenchmark
    {
        private static Random random = new Random(Seed: (int)DateTime.UtcNow.Ticks);

        private readonly TimeSpan HopSize = TimeSpan.FromDays(1);
        private readonly TimeSpan WindowSize = TimeSpan.FromDays(4);

        private readonly StreamEvent<long> EndEvent = StreamEvent.CreatePunctuation<long>(StreamEvent.InfinitySyncTime);

        private IObservable<StreamEvent<long>> data;

        [GlobalSetup]
        public void HoppingWindowTopKAggregateRandomDistribution()
        {
            data = Observable.Create<StreamEvent<long>>(o =>
            {
                var startTime = DateTimeOffset.UtcNow.AddYears(-1);

                for (long counter = 0; counter < 100000000; counter++)
                {
                    long reading = random.Next(10000, 20000);
                    startTime += TimeSpan.FromMilliseconds(1);
                    var se = StreamEvent.CreateStart(startTime.Ticks, reading);
                    o.OnNext(se);
                }
                o.OnNext(EndEvent);
                o.OnCompleted();
                return Disposable.Empty;
            });
        }

        [Benchmark]
        public void TestHoppingWindowMinAggregate()
        {
            var output = data.ToStreamable().HoppingWindowLifetime(WindowSize.Ticks, HopSize.Ticks).Min();

            long totalEntries = 0;

            output.ToStreamEventObservable().ForEachAsync(se =>
            {
                ++totalEntries;
            }).Wait();
            Console.WriteLine("TotalEntries = {0}", totalEntries);
        }
    }
}
