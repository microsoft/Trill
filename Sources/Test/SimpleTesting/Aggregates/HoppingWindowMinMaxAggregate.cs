// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class HoppingWindowMinMaxAggregate : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private const long HopSize = 10;
        private const long WindowSize = 4 * HopSize;

        private StreamEvent<int> EndEvent = StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime);
        private Random random = new Random(Seed: (int)DateTime.UtcNow.Ticks);

        [TestMethod, TestCategory("Gated")]
        public void TestHoppingWindowMinMaxAggregateSimple()
        {
            var input = new[]
            {
                StreamEvent.CreateStart(1, 10),
                StreamEvent.CreateStart(3, 20),
                StreamEvent.CreateStart(6, 10),
                StreamEvent.CreateStart(8, 30),
                StreamEvent.CreateStart(12, 20),
                StreamEvent.CreateStart(18, 10),
                EndEvent
            };

            var output = input.ToStreamable().HoppingWindowLifetime(10, 5).Max();

            var correctValues = new[] { 20, 30, 30, 20, 10 };
            var correctEvents = new List<StreamEvent<int>>();
            for (int i = 0; i < correctValues.Length; i++)
            {
                correctEvents.Add(StreamEvent.CreateStart(5 * (i + 1), correctValues[i]));
                correctEvents.Add(StreamEvent.CreateEnd(5 * (i + 2), 5 * (i + 1), correctValues[i]));
            }
            correctEvents.Add(EndEvent);

            CollectionAssert.AreEqual(correctEvents, output.ToStreamEventArray());
        }

        [TestMethod, TestCategory("Gated")]
        public void TestHoppingWindowMinAggregateSimple()
        {
            var input = new[]
            {
                StreamEvent.CreateStart(1, 30),
                StreamEvent.CreateStart(3, 20),
                StreamEvent.CreateStart(6, 30),
                StreamEvent.CreateStart(8, 10),
                StreamEvent.CreateStart(12, 20),
                StreamEvent.CreateStart(18, 30),
                EndEvent
            };

            var output = input.ToStreamable().HoppingWindowLifetime(10, 5).Min();

            var correctValues = new[] { 20, 10, 10, 20, 30 };
            var correctEvents = new List<StreamEvent<int>>();
            for (int i = 0; i < correctValues.Length; i++)
            {
                correctEvents.Add(StreamEvent.CreateStart(5 * (i + 1), correctValues[i]));
                correctEvents.Add(StreamEvent.CreateEnd(5 * (i + 2), 5 * (i + 1), correctValues[i]));
            }
            correctEvents.Add(EndEvent);

            CollectionAssert.AreEqual(correctEvents, output.ToStreamEventArray());
        }

        [TestMethod, TestCategory("Gated")]
        public void TestHoppingWindowMinMaxAggregateRandomDistribution()
        {
            // Distribution: [1,2,3,4,5] hops : 10% each, Closely-Spaced: 50%
            GenerateDataAndTestInput(
                numValues: 1000,
                valueGenerator: v => random.Next(100, 110),
                distanceGenerator: () =>
                {
                    var hopType = random.Next(1, 11);
                    return (hopType < 6) ? (hopType * HopSize) : (hopType - 5);
                });
        }

        [TestMethod, TestCategory("Gated")]
        public void TestHoppingWindowMinMaxAggregateUnaligned()
        {
            GenerateDataAndTestInput(
                numValues: 1000,
                valueGenerator: v => random.Next(100, 110),
                distanceGenerator: () => 10,
                windowSize: 27);

            GenerateDataAndTestInput(
                numValues: 1000,
                valueGenerator: v => random.Next(100, 110),
                distanceGenerator: () => 10,
                windowSize: 17);
        }

        [TestMethod, TestCategory("Gated")]
        public void TestHoppingWindowMinMaxAggregateAllIncreasing()
        {
            GenerateDataAndTestInput(
                numValues: 1000,
                valueGenerator: v => v + random.Next(1, 11),
                distanceGenerator: () => HopSize);
        }

        [TestMethod, TestCategory("Gated")]
        public void TestHoppingWindowMinMaxAggregateAllDecreasing()
        {
            GenerateDataAndTestInput(
                numValues: 1000,
                valueGenerator: v => (v == 0) ? 10000 : v - random.Next(1, 11),
                distanceGenerator: () => HopSize);
        }

        [TestMethod, TestCategory("Gated")]
        public void TestHoppingWindowMinMaxAggregateCyclingValues()
        {
            // Test values 1 -> 2 -> 3 -> 1, and run for 1/2, 1, 2, 4, 8 intervals of hops
            for (long distance = HopSize / 2; distance <= HopSize * 8; distance *= 2)
            {
                GenerateDataAndTestInput(
                    numValues: 10,
                    valueGenerator: v => (v % 3) + 1,
                    distanceGenerator: () => distance);
            }
        }

        private void GenerateDataAndTestInput(
            int numValues,
            Func<int, int> valueGenerator,
            Func<long> distanceGenerator,
            long windowSize = WindowSize)
        {
            var input = new List<StreamEvent<int>>();
            long maxStartTime = 0;

            long startTime = 0;
            int value = 100;
            for (int i = 0; i < numValues; i++)
            {
                startTime += distanceGenerator();
                value = valueGenerator(value);
                input.Add(StreamEvent.CreateStart(startTime, value));
                maxStartTime = Math.Max(maxStartTime, startTime);
            }
            input.Add(EndEvent);

            TestHoppingWindowMaxAggregateInternal(input, maxStartTime, windowSize);

            TestHoppingWindowMinAggregateInternal(input, maxStartTime, windowSize);
        }

        private void TestHoppingWindowMinAggregateInternal(IEnumerable<StreamEvent<int>> streamEvents, long maxStartTime, long windowSize)
        {
            TestHoppingWindowMinMaxAggregateInternal(streamEvents, isMax: true, maxStartTime, windowSize);
        }

        private void TestHoppingWindowMaxAggregateInternal(IEnumerable<StreamEvent<int>> streamEvents, long maxStartTime, long windowSize)
        {
            TestHoppingWindowMinMaxAggregateInternal(streamEvents, isMax: false, maxStartTime, windowSize);
        }

        private void TestHoppingWindowMinMaxAggregateInternal(IEnumerable<StreamEvent<int>> streamEvents, bool isMax, long maxStartTime, long windowSize)
        {
            var output = streamEvents.ToStreamable().HoppingWindowLifetime(windowSize, HopSize).Max();

            var correct = new List<StreamEvent<int>>();

            maxStartTime += windowSize;
            for (long startTime = 0; startTime < maxStartTime; startTime += HopSize)
            {
                var eventsInWindow = streamEvents.Where(x => x.StartTime > (startTime - windowSize) && x.StartTime <= startTime);

                if (!eventsInWindow.Any())
                    continue;

                var fun = isMax ? new Func<int, int, bool>((x, y) => x > y) : ((x, y) => x < y);
                var max = eventsInWindow.Aggregate((state, input) => state.Payload > input.Payload ? state : input);

                correct.Add(StreamEvent.CreateStart(startTime, max.Payload));
                correct.Add(StreamEvent.CreateEnd(startTime + HopSize, startTime, max.Payload));
            }

            var actual = NormalizeToInterval(output.ToStreamEventArray<int>());

            Assert.IsTrue(Enumerable.SequenceEqual(NormalizeToInterval(correct), actual));
        }

        private IEnumerable<StreamEvent<int>> NormalizeToInterval(IEnumerable<StreamEvent<int>> streamEvents)
        {
            var result = new List<StreamEvent<int>>();

            var endEvents = streamEvents.Where(se => se.Kind == StreamEventKind.End || se.Kind == StreamEventKind.Interval);

            if (!endEvents.Any())
                return result;

            var firstEvent = endEvents.First();
            StreamEvent<int> curInterval = StreamEvent.CreateInterval(firstEvent.StartTime, firstEvent.EndTime, firstEvent.Payload);

            foreach (var streamEvent in endEvents.Skip(1))
            {
                if ((streamEvent.StartTime == curInterval.EndTime || streamEvent.Kind == StreamEventKind.Interval) && // Merge into current interval if payload is same
                    streamEvent.Payload == curInterval.Payload)
                {
                    curInterval.OtherTime = streamEvent.EndTime;
                }
                else
                {
                    result.Add(curInterval);
                    curInterval = StreamEvent.CreateInterval(streamEvent.StartTime, streamEvent.EndTime, streamEvent.Payload); ;
                }
            }
            result.Add(curInterval);
            return result;
        }
    }
}
