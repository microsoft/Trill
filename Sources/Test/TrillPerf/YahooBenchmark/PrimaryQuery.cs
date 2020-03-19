// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using Microsoft.StreamProcessing;

namespace PerformanceTesting.YahooBenchmark
{
    public class PrimaryQuery
    {
        private const int ArraySize = 80000;

        public static IStreamable<Empty, Output> BenchmarkQuery(IStreamable<Empty, Event> streamable)
            => streamable
                .Where(e => e.event_type == Event_Type.View)
                .TumblingWindowLifetime(TimeSpan.FromSeconds(10).Ticks)
                .Select(e => new ProjectedEvent { ad_id = e.ad_id, event_time = e.event_time, campaign_id = DataGenerator.Campaigns[e.ad_id] })
                .GroupAggregate(e => e.campaign_id, o => o.Count(), o => o.Max(r => r.event_time), (key, count, max) => new Output { campaign_id = key.Key, count = count, lastUpdate = max })
            ;

        private static void PopulateArray(StreamEvent<Event>[] array)
        {
            for (int i = 0; i < ArraySize - 1; i++)
            {
                var ev = Event.CreateEvent(DataGenerator.Ads);
                array[i] = StreamEvent.CreatePoint(ev.event_time.Ticks, ev);
            }
        }

        [PerfTest("YahooBenchmark")]
        public static void YahooBenchmarkTest(IPerfTestState state)
        {
            var rowCount = 0UL;
            var batchCount = 0UL;

            var eventArray = new StreamEvent<Event>[ArraySize];
            var segment = new ArraySegment<StreamEvent<Event>>(eventArray);
            var events = new Subject<ArraySegment<StreamEvent<Event>>>();
            var inputStream = events.ToStreamable();
            var outputStream = BenchmarkQuery(inputStream);
            outputStream.ToStreamEventObservable().ForEachAsync(o => rowCount++);

            var stopwatch = new Stopwatch();
            stopwatch.Start();

            for (int i = 0; i < 1000; i++)
            {
                PopulateArray(eventArray);
                eventArray[ArraySize - 1] = StreamEvent.CreatePunctuation<Event>(DateTime.Now.Ticks);
                events.OnNext(segment);
                batchCount++;
                if (batchCount % 100 == 0) Console.WriteLine(new { rowCount, batchCount, time = stopwatch.ElapsedMilliseconds });
            }
        }
    }
}
