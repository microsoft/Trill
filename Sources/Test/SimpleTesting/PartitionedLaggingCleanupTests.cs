// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading.Tasks;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting.PartitionedIngressAndEgress.LaggingCleanup
{
    public static class StreamableExtension
    {
        public static IStreamable<PartitionKey<int>, double> GroupAggregateAverageTestRecord(
            this IStreamable<PartitionKey<int>, LaggingPartitionCleanupTests.TestRecord> source)
            => source.GroupAggregate(
                        testRecord => testRecord.Key,
                        w => w.Average(testRecord => testRecord.Payload),
                        (key, testRecord) => testRecord);

        public static IStreamable<PartitionKey<int>, double> AggregateAverageLong(
            this IStreamable<PartitionKey<int>, long> source)
            => source.Aggregate(w => w.Average(payload => payload));
    }

    // Tests scenarios where some partitions lag behind the low watermark to ensure appropriate and correct cleanup
    [TestClass]
    public class LaggingPartitionCleanupTests : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private const int HopSize = 50;
        private const int WindowSize = 100;
        private const int IntervalLength = WindowSize;

        [TestMethod, TestCategory("Gated")]
        public void HoppingWindow() =>
            HoppingWindowWorker(
                ingress => ingress.HoppingWindowLifetime(WindowSize, HopSize).GroupAggregateAverageTestRecord(), this.testRecordCreator);

        [TestMethod, TestCategory("Gated")]
        public void HoppingWindowSimple() =>
            HoppingWindowWorker(
                ingress => ingress.HoppingWindowLifetime(WindowSize, HopSize).AggregateAverageLong(), this.longCreator);

        // Even though we only ingress constant duration intervals, the Streamable doesn't know that,
        // so this will use PriorityQueue under the covers
        [TestMethod, TestCategory("Gated")]
        public void PriorityQueueSimple() =>
            SlidingWorker(
                ingress => ingress.AggregateAverageLong(), this.longCreator);

        [TestMethod, TestCategory("Gated")]
        public void PriorityQueue() =>
            SlidingWorker(
                ingress => ingress.GroupAggregateAverageTestRecord(), this.testRecordCreator);

        // SlidingWindow requires a constant duration streamable, so insert an AlterEventDuration
        [TestMethod, TestCategory("Gated")]
        public void SlidingWindowSimple() =>
            SlidingWorker(
                ingress => ingress.AlterEventDuration(IntervalLength).AggregateAverageLong(), this.longCreator);

        [TestMethod, TestCategory("Gated")]
        public void SlidingWindow() =>
            SlidingWorker(
                ingress => ingress.AlterEventDuration(IntervalLength).GroupAggregateAverageTestRecord(), this.testRecordCreator);

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeSimple() =>
            StartEdgeWorker(
                ingress => ingress.AlterEventDuration(StreamEvent.InfinitySyncTime).AggregateAverageLong(), this.longCreator);

        [TestMethod, TestCategory("Gated")]
        public void StartEdge() =>
            StartEdgeWorker(
                ingress => ingress.AlterEventDuration(StreamEvent.InfinitySyncTime).GroupAggregateAverageTestRecord(), this.testRecordCreator);

        [TestMethod, TestCategory("Gated")]
        public void TumblingWindowSimple() =>
            TumblingWindowWorker(
                ingress => ingress.TumblingWindowLifetime(WindowSize).AggregateAverageLong(), this.longCreator);

        [TestMethod, TestCategory("Gated")]
        public void TumblingWindow() =>
            TumblingWindowWorker(
                ingress => ingress.TumblingWindowLifetime(WindowSize).GroupAggregateAverageTestRecord(), this.testRecordCreator);

        private void HoppingWindowWorker<TPayload>(
            Func<IPartitionedIngressStreamable<int, TPayload>, IStreamable<PartitionKey<int>, double>> createQuery,
            Func<int, long, TPayload> resultCreator)
        {
            SetupQuery(createQuery, out var input);

            // Establish two partitions with keys 1,2
            input.OnNext(CreateInputInterval(key: 1, startTime: 100, resultCreator));
            input.OnNext(CreateInputInterval(key: 2, startTime: 100, resultCreator));

            // Move partition 1 ahead, generating a low watermark at 150 (due to the lifetime alteration of hopping window).
            // Partition 2 should not be cleaned up, since it still contains data in its sliding window
            input.OnNext(CreateInputInterval(key: 1, startTime: 150, resultCreator));

            // Move partition 1 ahead again, generating a low watermark at 250-20=230. Partition 2 can now be cleaned up,
            // since it no longer contains data in its sliding window
            input.OnNext(CreateInputInterval(key: 1, startTime: 250, resultCreator));

            // Reactivate partition 2, which should start with a clean slate
            input.OnNext(CreateInputInterval(key: 2, startTime: 250, resultCreator));

            input.OnCompleted();

            var expected = new PartitionedStreamEvent<int, double>[]
            {
                // Two partitions established with new hop window starting at 100
                PartitionedStreamEvent.CreateLowWatermark<int, double>(100),
                CreateOutputStart(key: 1, startTime: 100, average: 100),
                CreateOutputStart(key: 2, startTime: 100, average: 100),

                // New event for partition 1's sliding window (150), establishes a new hop window starting at 150
                PartitionedStreamEvent.CreateLowWatermark<int, double>(150),
                CreateOutputEnd(key: 1, endTime: 150, originalStart: 100, average: 100),
                CreateOutputStart(key: 1, startTime: 150, average: 125),    // Average(100, 150)=125

                // New event for partition 1's sliding window (250) creates a low watermark, which  deaccumulates
                // both events at startTime 100. Partition 2 can be cleaned up (no more data), but partition 1 should
                // see its average updated to exclude the 100.
                CreateOutputEnd(key: 1, endTime: 200, originalStart: 150, average: 125),
                CreateOutputEnd(key: 2, endTime: 200, originalStart: 100, average: 100),

                CreateOutputStart(key: 1, startTime: 200, average: 150),    // Average(150)=150
                CreateOutputEnd(key: 1, endTime: 250, originalStart: 200, average: 150),
                PartitionedStreamEvent.CreateLowWatermark<int, double>(250),

                // OnCompletedPolicy.EndOfStream - Both partitions accumulate the 250
                CreateOutputStart(key: 1, startTime: 250, average: 250),    // Average(250)=250
                CreateOutputStart(key: 2, startTime: 250, average: 250),    // Average(250)=250
                CreateOutputEnd(key: 1, endTime: 350, originalStart: 250, average: 250),
                CreateOutputEnd(key: 2, endTime: 350, originalStart: 250, average: 250),
                PartitionedStreamEvent.CreateLowWatermark<int, double>(StreamEvent.InfinitySyncTime),
            };

            FinishQuery(input, expected);
        }

        private void SlidingWorker<TPayload>(
            Func<IPartitionedIngressStreamable<int, TPayload>, IStreamable<PartitionKey<int>, double>> createQuery,
            Func<int, long, TPayload> resultCreator)
        {
            SetupQuery(createQuery, out var input);

            // Establish two partitions with keys 1,2
            input.OnNext(CreateInputInterval(key: 1, startTime: 100, resultCreator));
            input.OnNext(CreateInputInterval(key: 2, startTime: 100, resultCreator));

            // Move partition 1 ahead, generating a low watermark at 150-20=130. Partition 2 should not be cleaned up,
            // since it still contains data in its sliding window
            input.OnNext(CreateInputInterval(key: 1, startTime: 150, resultCreator));

            // Move partition 1 ahead again, generating a low watermark at 250-20=230. Partition 2 can now be cleaned up,
            // since it no longer contains data in its sliding window
            input.OnNext(CreateInputInterval(key: 1, startTime: 250, resultCreator));

            // Reactivate partition 2, which should start with a clean slate
            input.OnNext(CreateInputInterval(key: 2, startTime: 250, resultCreator));

            input.OnCompleted();

            var expected = new PartitionedStreamEvent<int, double>[]
            {
                // Two partitions established
                PartitionedStreamEvent.CreateLowWatermark<int, double>(80),
                CreateOutputStart(key: 1, startTime: 100, average: 100),
                CreateOutputStart(key: 2, startTime: 100, average: 100),

                // New event for partition 1's sliding window (150)
                PartitionedStreamEvent.CreateLowWatermark<int, double>(130),
                CreateOutputEnd(key: 1, endTime: 150, originalStart: 100, average: 100),
                CreateOutputStart(key: 1, startTime: 150, average: 125),    // Average(100, 150)=125

                // New event for partition 1's sliding window (250) creates a low watermark, which  deaccumulates
                // both events at startTime 100. Partition 2 can be cleaned up (no more data), but partition 1 should
                // see its average updated to exclude the 100.
                CreateOutputEnd(key: 1, endTime: 200, originalStart: 150, average: 125),
                CreateOutputEnd(key: 2, endTime: 200, originalStart: 100, average: 100),

                CreateOutputStart(key: 1, startTime: 200, average: 150),    // Average(150)=150
                PartitionedStreamEvent.CreateLowWatermark<int, double>(230),
                CreateOutputEnd(key: 1, endTime: 250, originalStart: 200, average: 150),

                // OnCompletedPolicy.EndOfStream - Both partitions accumulate the 250
                CreateOutputStart(key: 1, startTime: 250, average: 250),    // Average(250)=250
                CreateOutputStart(key: 2, startTime: 250, average: 250),    // Average(250)=250
                CreateOutputEnd(key: 1, endTime: 350, originalStart: 250, average: 250),
                CreateOutputEnd(key: 2, endTime: 350, originalStart: 250, average: 250),
                PartitionedStreamEvent.CreateLowWatermark<int, double>(StreamEvent.InfinitySyncTime),
            };

            FinishQuery(input, expected);
        }

        // StartEdge-only streamables should never clean up partitions, since start edges are always valid state
        private void StartEdgeWorker<TPayload>(
            Func<IPartitionedIngressStreamable<int, TPayload>, IStreamable<PartitionKey<int>, double>> createQuery,
            Func<int, long, TPayload> resultCreator)
        {
            SetupQuery(createQuery, out var input);

            input.OnNext(CreateInputInterval(key: 1, startTime: 100, resultCreator));
            input.OnNext(CreateInputInterval(key: 2, startTime: 100, resultCreator));

            input.OnNext(CreateInputInterval(key: 1, startTime: 160, resultCreator));

            input.OnNext(CreateInputInterval(key: 1, startTime: 250, resultCreator));
            input.OnNext(CreateInputInterval(key: 2, startTime: 250, resultCreator));

            input.OnCompleted();

            var expected = new PartitionedStreamEvent<int, double>[]
            {
                PartitionedStreamEvent.CreateLowWatermark<int, double>(80),
                CreateOutputStart(key: 1, startTime: 100, average: 100),
                CreateOutputStart(key: 2, startTime: 100, average: 100),

                PartitionedStreamEvent.CreateLowWatermark<int, double>(140),
                CreateOutputEnd(key: 1, endTime: 160, originalStart: 100, average: 100),
                CreateOutputStart(key: 1, startTime: 160, average: 130),    // Average(100, 160)=130

                PartitionedStreamEvent.CreateLowWatermark<int, double>(230),
                CreateOutputEnd(key: 1, endTime: 250, originalStart: 160, average: 130),
                CreateOutputEnd(key: 2, endTime: 250, originalStart: 100, average: 100),
                CreateOutputStart(key: 1, startTime: 250, average: 170),    // Average(100, 160, 250)=170
                CreateOutputStart(key: 2, startTime: 250, average: 175),    // Average(100, 250)=175

                PartitionedStreamEvent.CreateLowWatermark<int, double>(StreamEvent.InfinitySyncTime),
            };

            FinishQuery(input, expected);
        }

        private void TumblingWindowWorker<TPayload>(
            Func<IPartitionedIngressStreamable<int, TPayload>, IStreamable<PartitionKey<int>, double>> createQuery,
            Func<int, long, TPayload> resultCreator)
        {
            SetupQuery(createQuery, out var input);

            input.OnNext(CreateInputInterval(key: 1, startTime: 120, resultCreator));
            input.OnNext(CreateInputInterval(key: 2, startTime: 120, resultCreator));

            input.OnNext(CreateInputInterval(key: 1, startTime: 150, resultCreator));

            input.OnNext(CreateInputInterval(key: 1, startTime: 250, resultCreator));
            input.OnNext(CreateInputInterval(key: 2, startTime: 250, resultCreator));

            input.OnCompleted();

            var expected = new PartitionedStreamEvent<int, double>[]
            {
                PartitionedStreamEvent.CreateLowWatermark<int, double>(100),
                PartitionedStreamEvent.CreateLowWatermark<int, double>(200),

                CreateOutputInterval(key: 1, startTime: 200, endTime: 300, average: 135),
                CreateOutputInterval(key: 2, startTime: 200, endTime: 300, average: 120),
                PartitionedStreamEvent.CreateLowWatermark<int, double>(300),

                CreateOutputInterval(key: 1, startTime: 300, endTime: 400, average: 250),
                CreateOutputInterval(key: 2, startTime: 300, endTime: 400, average: 250),
                PartitionedStreamEvent.CreateLowWatermark<int, double>(StreamEvent.InfinitySyncTime),
            };

            FinishQuery(input, expected);
        }

        private void SetupQuery<TInput>(
            Func<IPartitionedIngressStreamable<int, TInput>, IStreamable<PartitionKey<int>, double>> createQuery,
            out Subject<PartitionedStreamEvent<int, TInput>> input)
        {
            const uint generationPeriod = 10;
            const uint lowWatermarkTimestampLag = 20;

            var qc = new QueryContainer();
            input = new Subject<PartitionedStreamEvent<int, TInput>>();
            var ingress = qc.RegisterInput(input, null, PartitionedFlushPolicy.None, null, PeriodicLowWatermarkPolicy.Time(generationPeriod, lowWatermarkTimestampLag));

            var query = createQuery(ingress);

            this.output = new List<PartitionedStreamEvent<int, double>>();
            this.egress = qc.RegisterOutput(query).ForEachAsync(o => this.output.Add(o));
            this.process = qc.Restore();
        }

        private void FinishQuery<TInput>(
            Subject<PartitionedStreamEvent<int, TInput>> input,
            IEnumerable<PartitionedStreamEvent<int, double>> expected)
        {
            input.OnCompleted();
            this.process.Flush();
            this.egress.Wait();

            // Validate input per partition, including low watermarks in each partition for ordering
            var expectedKeys = expected.Select(e => e.PartitionKey).Distinct().OrderBy(e => e).ToList();
            var keys = this.output.Select(e => e.PartitionKey).Distinct().OrderBy(e => e).ToList();
            Assert.IsTrue(expectedKeys.SequenceEqual(keys));

            foreach (var key in keys)
            {
                var expectedKeyed = expected.Where(e => e.IsLowWatermark || e.PartitionKey == key).ToList();
                var outputKeyed = this.output.Where(e => e.IsLowWatermark || e.PartitionKey == key).ToList();
                Assert.IsTrue(expectedKeyed.SequenceEqual(outputKeyed));
            }
        }

        private readonly Func<int, long, TestRecord> testRecordCreator = (key, payload) => new TestRecord(key, payload);
        private readonly Func<int, long, long> longCreator = (key, payload) => payload;
        private List<PartitionedStreamEvent<int, double>> output;
        private Task egress;
        private Process process;

        private static PartitionedStreamEvent<int, TInput> CreateInputInterval<TInput>(int key, long startTime, Func<int, long, TInput> resultCreator) =>
            PartitionedStreamEvent.CreateInterval(key, startTime, startTime + IntervalLength, resultCreator(key, startTime));
        private static PartitionedStreamEvent<int, double> CreateOutputStart(int key, long startTime, double average) =>
            PartitionedStreamEvent.CreateStart(key, startTime, average);
        private static PartitionedStreamEvent<int, double> CreateOutputEnd(int key, long endTime, long originalStart, double average) =>
            PartitionedStreamEvent.CreateEnd(key, endTime, originalStart, average);
        private static PartitionedStreamEvent<int, double> CreateOutputInterval(int key, long startTime, long endTime, double average) =>
            PartitionedStreamEvent.CreateInterval(key, startTime, endTime, average);

        public class TestRecord
        {
            public TestRecord(int key, long payload)
            {
                this.Key = key;
                this.Payload = payload;
            }

            public int Key { get; }
            public long Payload { get; }
        }
    }
}
