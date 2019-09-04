// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting.PartitionedIngressAndEgress
{
    public class TriPartitionedOrderedTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        internal TriPartitionedOrderedTestsBase(
            ConfigModifier config,
            DisorderPolicy disorderPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            PeriodicLowWatermarkPolicy lowWatermarkPolicy) : base(config)
        {
            this.disorderPolicy = disorderPolicy;
            this.punctuationPolicy = punctuationPolicy;
            this.lowWatermarkPolicy = lowWatermarkPolicy;
        }

        [TestMethod, TestCategory("Gated")]
        public void PartitionedStartEdgeTest()
        {
            var input = Enumerable.Range(0, 1000).ToList();

            var passThrough = input.ToObservable().ToPartitionedStreamable(
                o => o % 3,
                o => o, this.disorderPolicy,
                PartitionedFlushPolicy.FlushOnLowWatermark, this.punctuationPolicy, this.lowWatermarkPolicy).ToStreamEventObservable().ToEnumerable().ToList();

            ValidateOutput(input, passThrough, false);
        }

        [TestMethod, TestCategory("Gated")]
        public void PartitionedIntervalIngressTest()
        {
            var input = Enumerable.Range(0, 1000).ToList();
            var passThrough = input.ToObservable().ToPartitionedStreamable(
                o => o % 3,
                o => o,
                o => o + 5, this.disorderPolicy,
                PartitionedFlushPolicy.FlushOnLowWatermark, this.punctuationPolicy, this.lowWatermarkPolicy).ToStreamEventObservable().ToEnumerable().ToList();

            ValidateOutput(input, passThrough, true);
        }

        private void ValidateOutput(IList<int> input, IList<PartitionedStreamEvent<int, int>> passThrough, bool intervals)
        {
            var output = passThrough.Where(o => o.IsData).ToList();
            var punctuations = passThrough.Where(o => o.IsPunctuation).ToList();
            var lowWatermarks = passThrough.Where(o => o.IsLowWatermark).ToList();

            // Expected low watermarks and their arrival times
            var expectedLowWatermarks = new List<LowWatermarkAndArrival>();
            if (this.lowWatermarkPolicy.type == PeriodicLowWatermarkPolicyType.Time)
            {
                long last = 0;
                var period = (int)this.lowWatermarkPolicy.generationPeriod;
                var lag = this.lowWatermarkPolicy.lowWatermarkTimestampLag;
                foreach (var t in input.Where(t => (t > lag) && (t - lag) >= (last + period)))
                {
                    var lowWatermarkPreSnap = t - lag;
                    var lowWatermarkArrivalTime = LowWatermarkArrivalTime(lowWatermarkPreSnap);
                    var lowWatermarkSnapped = lowWatermarkPreSnap.SnapToLeftBoundary(period);
                    var lowWatermarkQuantizedForPunctuation = lowWatermarkPreSnap.SnapToLeftBoundary((long)this.punctuationPolicy.generationPeriod);
                    var lowWatermark = PartitionedStreamEvent.CreateLowWatermark<int, int>(lowWatermarkSnapped);

                    expectedLowWatermarks.Add(new LowWatermarkAndArrival(lowWatermark, lowWatermarkQuantizedForPunctuation, lowWatermarkArrivalTime));
                    last = lowWatermarkSnapped;
                }

                expectedLowWatermarks.Add(
                    new LowWatermarkAndArrival(
                        PartitionedStreamEvent.CreateLowWatermark<int, int>(StreamEvent.InfinitySyncTime),
                        StreamEvent.InfinitySyncTime,
                        StreamEvent.InfinitySyncTime));

                var expectedLowWatermarkEvents = expectedLowWatermarks.Select(kvp => kvp.LowWatermark).ToList();
                Assert.IsTrue(expectedLowWatermarkEvents.SequenceEqual(lowWatermarks));
            }
            else
            {
                Assert.IsTrue(lowWatermarks.Count() == 1);
                Assert.IsTrue(lowWatermarks.First().StartTime == StreamEvent.InfinitySyncTime);
            }

            if (this.punctuationPolicy.type == PeriodicPunctuationPolicyType.Time)
            {
                var expectedPunctuations = new List<Tuple<int, int>>();
                var inputGroups = input.GroupBy(i => i % 3).ToList();
                foreach (var inputGroup in inputGroups)
                {
                    int last = 0;
                    foreach (var t in inputGroup.Where(t => t - last >= (uint)this.punctuationPolicy.generationPeriod))
                    {
                        var newPunctuationArrivalTime = PunctuationArrivalTime(t, inputGroup, expectedLowWatermarks,
                            out bool triggeredByLowWatermark);

                        // Check to see if a low watermark reset the punctuation period.
                        var interruptingWatermarks = expectedLowWatermarks
                            .Where(w => LowWatermarkResetsPunctuationPeriod(w.LowWatermark.StartTime, w.ArrivalTime,
                                last, t, newPunctuationArrivalTime, triggeredByLowWatermark))
                            .ToList();

                        if (interruptingWatermarks.Any())
                        {
                            last = (int)interruptingWatermarks.Last().QuantizedForPunctuationGeneration;
                            if (t - last < (uint)this.punctuationPolicy.generationPeriod)
                            {
                                continue;
                            }
                        }

                        var snappedPunctuation = (int)((long)t).SnapToLeftBoundary((long)this.punctuationPolicy.generationPeriod);
                        last = snappedPunctuation;
                        expectedPunctuations.Add(Tuple.Create(inputGroup.Key, snappedPunctuation));
                    }
                }

                foreach (var punctuationGroup in punctuations.GroupBy(t => t.PartitionKey))
                {
                    var t1 = punctuationGroup.ToList();
                    var t2 = expectedPunctuations.Where(t => t.Item1 % 3 == punctuationGroup.Key)
                            .Select(i => PartitionedStreamEvent.CreatePunctuation<int, int>(i.Item1, i.Item2)).ToList();
                    Assert.IsTrue(t1.SequenceEqual(t2));
                }
            }
            else
            {
                Assert.IsTrue(!punctuations.Any());
            }

            for (int key = 0; key < 3; key++)
            {
                var selector = intervals ?
                    (Func<int, PartitionedStreamEvent<int, int>>)(i => PartitionedStreamEvent.CreateInterval(key, i, i + 5, i)) :
                    (Func<int, PartitionedStreamEvent<int, int>>)(i => PartitionedStreamEvent.CreateStart(key, i, i));
                var keyOutput = output.Where(t => t.IsData && t.PartitionKey == key).ToList();
                var keyExpected = input.Where(i => i % 3 == key).Select(selector).ToList();
                Assert.IsTrue(keyExpected.SequenceEqual(keyOutput));
            }
        }

        private struct LowWatermarkAndArrival
        {
            public LowWatermarkAndArrival(PartitionedStreamEvent<int, int> lowWatermark, long quantizedForPunctuation, long arrivalTime)
            {
                this.LowWatermark = lowWatermark;
                this.ArrivalTime = arrivalTime;
                this.QuantizedForPunctuationGeneration = quantizedForPunctuation;
            }
            public readonly PartitionedStreamEvent<int, int> LowWatermark;
            public readonly long QuantizedForPunctuationGeneration;
            public readonly long ArrivalTime;
        }

        // Determines whether the given watermark should reset the punctuation generation period
        private static bool LowWatermarkResetsPunctuationPeriod(long lowWatermark, long lowWatermarkArrivalTime, int lastCTI,
            long newPunctuationPreSnap, long newPunctuationArrivalTime, bool newPunctuationTriggeredByLowWatermark)
        {
            // If the watermark timestamp is before the last CTI, it won't reset anything.
            if (lowWatermark < lastCTI)
            {
                return false;
            }

            // If the watermark arrives after the potential punctuation, it can't reset the generation period.
            if (lowWatermarkArrivalTime > newPunctuationArrivalTime)
            {
                return false;
            }

            // If the potential punctuation arrival is triggered by a low watermark itself, and this low watermark
            // arrives at the same time as the potential punctuation, then this watermark was the trigger
            if (newPunctuationTriggeredByLowWatermark && lowWatermarkArrivalTime == newPunctuationArrivalTime)
            {
                // If the punctuation would have arrived at the same time as the pre-snap trigger, but it's also triggered
                // by the low watermark, the low watermark takes precedence
                return newPunctuationArrivalTime == newPunctuationPreSnap;
            }

            // Otherwise, the low watermark will reset the punctuation period.
            return true;
        }

        private long PunctuationArrivalTime(
            int punctuationTime,
            IEnumerable<int> inputGroup,
            IEnumerable<LowWatermarkAndArrival> lowWatermarks,
            out bool triggeredByLowWatermark)
        {
            triggeredByLowWatermark = false;
            if (punctuationTime == 0) // not a real punctuation arrival
            {
                return 0;
            }

            // Punctuation arrival time can be delayed by the reorderLatency buffering
            if (this.disorderPolicy.reorderLatency <= 0)
            {
                return punctuationTime;
            }

            // The punctuation could arrive in response to the first event that's past the reorderLatency from
            // the punctuation time
            var eligibleEvents = inputGroup.Where(nextT =>
                nextT >= punctuationTime + this.disorderPolicy.reorderLatency).ToList();
            var punctuationTrigger = (eligibleEvents.Any() ? eligibleEvents.First() : StreamEvent.InfinitySyncTime);

            // The punctuation could arrive due to a low watermark triggering the processing of the reorder buffer
            var eligibleWatermarks = lowWatermarks.Where(w => w.LowWatermark.StartTime >= punctuationTime).ToList();
            var watermarkTrigger = (eligibleWatermarks.Any() ?
                eligibleWatermarks.First().ArrivalTime : StreamEvent.InfinitySyncTime);

            // The punctuation will arrive in response to the first trigger
            if (punctuationTrigger < watermarkTrigger)
            {
                return punctuationTrigger;
            }
            else
            {
                triggeredByLowWatermark = true;
                return watermarkTrigger;
            }
        }

        private long LowWatermarkArrivalTime(long lowWatermarkTimePreSnap)
        {
            if (lowWatermarkTimePreSnap == 0) // not a real low watermark arrival
            {
                return 0;
            }

            // LowWatermark arrival is delayed by the lowWatermarkTimestampLag
            return lowWatermarkTimePreSnap + this.lowWatermarkPolicy.lowWatermarkTimestampLag;
        }

        internal static readonly ConfigModifier baseConfigModifier = new ConfigModifier()
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(
                Config.CodegenOptions.MultiStringFlags.Wrappers |
                Config.CodegenOptions.MultiStringFlags.VectorOperations);

        private readonly DisorderPolicy disorderPolicy;
        private readonly PeriodicPunctuationPolicy punctuationPolicy;
        private readonly PeriodicLowWatermarkPolicy lowWatermarkPolicy;
    }

    public class PartitionedDisorderedTestsBase
    {
        protected void LocalDisorderingBase(DisorderPolicy disorderPolicy, PeriodicLowWatermarkPolicy lowWatermarkPolicy)
        {
            TestSetup(disorderPolicy, lowWatermarkPolicy);

            // Add events that are locally disordered

            // Partition 0
            AddEvent(0, 100);

            // Partition 1
            AddEvent(1, 100);
            AddEvent(1, 95);
            AddEvent(1, 89);

            // Partition 2
            AddEvent(2, 96);
            AddEvent(2, 94);
            AddEvent(2, 91);

            RunAndValidate();
        }

        protected void ReorderLatencyDisorderingBase(DisorderPolicy disorderPolicy, PeriodicLowWatermarkPolicy lowWatermarkPolicy)
        {
            TestSetup(disorderPolicy, lowWatermarkPolicy);

            // Partition 0 - locally disordered
            AddEvent(0, 100);
            AddEvent(0, 95);
            AddEvent(0, 89);

            // Partition 1 - globally disordered
            AddEvent(1, 80);
            AddEvent(1, 85);
            AddEvent(1, 90);
            AddEvent(1, 95);
            AddEvent(1, 100);

            // Partition 2 - locally and globally disordered
            AddEvent(2, 80);
            AddEvent(2, 85);
            AddEvent(2, 90);
            AddEvent(2, 95);
            AddEvent(2, 100);
            AddEvent(2, 99);
            AddEvent(2, 94);
            AddEvent(2, 89);
            AddEvent(2, 84);
            AddEvent(2, 79);

            RunAndValidate();
        }

        private void RunAndValidate()
        {
            // Flush
            AddEvent(0, 200);
            AddEvent(1, 200);
            AddEvent(2, 200);

            ProcessInput(out var diagnosticEvents, out var dataEvents);

            for (int key = 0; key < 3; key++)
            {
                var keyData = dataEvents.Where(@event => @event.IsData && @event.PartitionKey == key).ToList();
                var expectedData = this.expected[key];
                Assert.IsTrue(expectedData.SequenceEqual(keyData));

                // Don't bother validating order of diagnostics
                var keyDiagnostic = diagnosticEvents.Where(@event => @event.Event.PartitionKey == key)
                    .OrderBy(e => e.Event.SyncTime).ThenBy(e => e.Event.EndTime).ToList();
                var expectedDiagnostic = this.diagnostic[key].OrderBy(e => e.Event.SyncTime).ThenBy(e => e.Event.EndTime).ToList();
                Assert.IsTrue(expectedDiagnostic.SequenceEqual(keyDiagnostic));
            }
            var lowWatermarks = dataEvents.Where(@event => @event.IsLowWatermark).ToList();
            this.expectedLowWatermarks.Add(PartitionedStreamEvent.CreateLowWatermark<int, int>(StreamEvent.InfinitySyncTime));
            Assert.IsTrue(this.expectedLowWatermarks.SequenceEqual(lowWatermarks));
        }

        public void TestSetup(DisorderPolicy disorderPolicy, PeriodicLowWatermarkPolicy lowWatermarkPolicy)
        {
            this.disorderPolicy = disorderPolicy;
            this.lowWatermarkPolicy = lowWatermarkPolicy;

            // This establishes three partitions with keys 0,1,2
            PartitionedStreamEvent<int, int> point;
            for (int key = 0; key < 3; key++)
            {
                point = PartitionedStreamEvent.CreatePoint(key, 0, key);
                this.input.Add(point);
                this.expected[key].Add(point);
                this.batchMarker[key] = 1;
            }

            // Add a point at 100 for key 0 as a baseline, and a low watermark if necessary
            this.highWatermark = 100;
            point = PartitionedStreamEvent.CreatePoint(0, this.highWatermark, 0);
            this.input.Add(point);
            UpdateExpectedLowWatermark(this.highWatermark);
            this.expected[0].Insert(this.batchMarker[0]++, point);

            // If we have a nonzero reorder latency, the event at 100 will be buffered, so add another point at
            // 100+ReorderLatency to push the real highwatermark to 100
            if (this.ReorderLatency > 0)
            {
                AddEvent(0, this.highWatermark + this.ReorderLatency);
            }
        }

        [TestCleanup]
        public void Cleanup()
        {
            this.input.Clear();

            for (int key = 0; key < 3; key++)
            {
                this.partitionHighWatermark[key] = 0;
                this.batchMarker[key] = 0;
                this.expected[key].Clear();
                this.diagnostic[key].Clear();
            }
        }

        private void ProcessInput(
            out List<OutOfOrderPartitionedStreamEvent<int, int>> diagnosticEvents,
            out List<PartitionedStreamEvent<int, int>> dataEvents)
        {
            var qc = new QueryContainer();
            var ingress = qc.RegisterInput(this.input.ToObservable(), this.disorderPolicy,
                PartitionedFlushPolicy.None, PeriodicPunctuationPolicy.None(), this.lowWatermarkPolicy);

            var outOfOrderEvents = new List<OutOfOrderPartitionedStreamEvent<int, int>>();
            ingress.GetDroppedAdjustedEventsDiagnostic().Subscribe(o => outOfOrderEvents.Add(o));

            var output = new List<PartitionedStreamEvent<int, int>>();
            var egress = qc.RegisterOutput(ingress).ForEachAsync(o => output.Add(o));
            var process = qc.Restore();
            process.Flush();
            egress.Wait();

            diagnosticEvents = outOfOrderEvents;
            dataEvents = output;
        }

        private void InsertSorted(int key, PartitionedStreamEvent<int, int> dataEvent, bool batchImmediately)
        {
            if (batchImmediately)
            {
                this.expected[key].Insert(this.batchMarker[key]++, dataEvent);
            }
            else
            {
                int index = this.batchMarker[key];
                while (index < this.expected[key].Count && this.expected[key][index].SyncTime < dataEvent.SyncTime)
                {
                    index++;
                }

                this.expected[key].Insert(index, dataEvent);
            }
        }

        private void AddDiagnostic(int key, long time, long? delta = default)
            => this.diagnostic[key].Add(OutOfOrderPartitionedStreamEvent.Create(
                PartitionedStreamEvent.CreateInterval(key, time, time + IntervalLength, key), delta));

        private const int IntervalLength = 20;
        private void AddEvent(int key, long time)
        {
            var dataEvent = PartitionedStreamEvent.CreateInterval(key, time, time + IntervalLength, key);
            this.input.Add(dataEvent);

            UpdateExpectedLowWatermark(time);

            long localMin = Math.Max(this.partitionHighWatermark[key] - this.ReorderLatency, this.lowWatermark);
            bool locallyOrdered = time >= localMin;
            long batchBorder = localMin;

            if (locallyOrdered)
            {
                InsertSorted(key, dataEvent, (time == batchBorder) || this.ReorderLatency == 0);
            }
            else if (this.disorderPolicy.type == DisorderPolicyType.Drop || batchBorder >= dataEvent.EndTime)
            {
                AddDiagnostic(key, time); // Drop
            }
            else // Adjust
            {
                AddDiagnostic(key, time, batchBorder - dataEvent.SyncTime);
                var adjustedEvent = PartitionedStreamEvent.CreateInterval(key, batchBorder, dataEvent.EndTime, key);
                InsertSorted(key, adjustedEvent, true); // Since we adjust to batchBorder, batch immediately
            }

            this.highWatermark = Math.Max(this.highWatermark, time);
            this.partitionHighWatermark[key] = Math.Max(this.partitionHighWatermark[key], time);

            UpdateBatchMarker(key, batchBorder);
        }

        private void UpdateExpectedLowWatermark(long eventTime)
        {
            if (this.GenerateLowWatermarks && eventTime > this.Lag)
            {
                var newLowWatermarkTime = eventTime - this.Lag;
                if (newLowWatermarkTime >= this.lowWatermark + this.GenerationPeriod && newLowWatermarkTime > this.lowWatermark)
                {
                    // eventTime is sufficiently high to generate a new watermark, but first snap it to the nearest generationPeriod boundary
                    newLowWatermarkTime = newLowWatermarkTime.SnapToLeftBoundary((long)this.lowWatermarkPolicy.generationPeriod);
                    this.lowWatermark = newLowWatermarkTime;
                    for (int key = 0; key < 3; key++)
                    {
                        this.partitionHighWatermark[key] = Math.Max(this.partitionHighWatermark[key], newLowWatermarkTime);
                        UpdateBatchMarker(key, newLowWatermarkTime);
                    }

                    this.expectedLowWatermarks.Add(PartitionedStreamEvent.CreateLowWatermark<int, int>(newLowWatermarkTime));
                }
            }
        }

        private void UpdateBatchMarker(int key, long time)
        {
            while (this.batchMarker[key] < this.expected[key].Count && this.expected[key][this.batchMarker[key]].SyncTime <= time)
            {
                this.batchMarker[key]++;
            }
        }

        private long ReorderLatency => this.disorderPolicy.reorderLatency;
        private bool GenerateLowWatermarks => this.lowWatermarkPolicy.type == PeriodicLowWatermarkPolicyType.Time;
        private long GenerationPeriod => (long)this.lowWatermarkPolicy.generationPeriod;
        private long Lag => this.lowWatermarkPolicy.lowWatermarkTimestampLag;

        private DisorderPolicy disorderPolicy;
        private PeriodicLowWatermarkPolicy lowWatermarkPolicy;
        private readonly long[] partitionHighWatermark = new long[3];
        private readonly int[] batchMarker = new int[3]; // Marks the point between data events that should be batched vs. events that can be reordered
        private long highWatermark = 100;
        private long lowWatermark = 0;

        private readonly List<PartitionedStreamEvent<int, int>> input = new List<PartitionedStreamEvent<int, int>>();
        private readonly List<PartitionedStreamEvent<int, int>>[] expected = new List<PartitionedStreamEvent<int, int>>[]
        {
            new List<PartitionedStreamEvent<int, int>>(),
            new List<PartitionedStreamEvent<int, int>>(),
            new List<PartitionedStreamEvent<int, int>>(),
        };
        private readonly List<OutOfOrderPartitionedStreamEvent<int, int>>[] diagnostic = new List<OutOfOrderPartitionedStreamEvent<int, int>>[]
        {
            new List<OutOfOrderPartitionedStreamEvent<int, int>>(),
            new List<OutOfOrderPartitionedStreamEvent<int, int>>(),
            new List<OutOfOrderPartitionedStreamEvent<int, int>>(),
        };
        private readonly List<PartitionedStreamEvent<int, int>> expectedLowWatermarks = new List<PartitionedStreamEvent<int, int>>();
    }
}
