// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class EquiJoinStreamableTest : TestWithConfigSettingsAndMemoryLeakDetection
    {
        [TestMethod, TestCategory("Gated")]
        public void EquiJoinStreamable_Row() => EquiJoinStreamable(columnar: false, fixedInterval: false);

        [TestMethod, TestCategory("Gated")]
        public void EquiJoinStreamable_Row_FixedInterval() => EquiJoinStreamable(columnar: false, fixedInterval: true);

        [TestMethod, TestCategory("Gated")]
        public void EquiJoinStreamable_Columnar() => EquiJoinStreamable(columnar: true, fixedInterval: false);

        [TestMethod, TestCategory("Gated")]
        public void EquiJoinStreamable_Columnar_FixedInterval() => EquiJoinStreamable(columnar: true, fixedInterval: true);

        private static void EquiJoinStreamable(bool columnar, bool fixedInterval)
        {
            using (var modifier = new ConfigModifier()
                .ForceRowBasedExecution(!columnar)
                .DontFallBackToRowBasedExecution(columnar)
                .Modify())
            {
                JoinIntervalsTest(fixedInterval);
                JoinPointsTest(fixedInterval);
                if (!fixedInterval)
                {
                    JoinEdgeIntervalTest();
                    JoinEdgesTest();
                    PartitionedStartEdgeJoinTest();
                }
            }
        }

        private static void JoinIntervalsTest(bool fixedInterval = false)
        {
            const long duration = 10;
            var input1 = new StreamEvent<string>[]
            {
                StreamEvent.CreateInterval(100, 100 + duration, "A1"),
                StreamEvent.CreateInterval(101, 101 + duration, "B1"),
                StreamEvent.CreateInterval(102, 102 + duration, "C1"),

                StreamEvent.CreateInterval(120, 120 + duration, "A3"),
                StreamEvent.CreateInterval(121, 121 + duration, "B3"),
                StreamEvent.CreateInterval(122, 122 + duration, "C3"),
                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            };

            var input2 = new[]
            {
                StreamEvent.CreateInterval(105, 105 + duration, "A2"),
                StreamEvent.CreateInterval(106, 106 + duration, "B2"),
                StreamEvent.CreateInterval(107, 107 + duration, "D2"),

                StreamEvent.CreateInterval(125, 125 + duration, "A4"),
                StreamEvent.CreateInterval(126, 126 + duration, "B4"),
                StreamEvent.CreateInterval(127, 127 + duration, "C4"),
                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            };

            var inputStream1 = input1.ToStreamable();
            var inputStream2 = input2.ToStreamable();

            if (fixedInterval)
            {
                inputStream1 = inputStream1.AlterEventDuration(duration);
                inputStream2 = inputStream2.AlterEventDuration(duration);
            }

            var outputStream = inputStream1.Join(
                inputStream2,
                l => (l != null ? l[0].ToString() : null),
                r => (r != null ? r[0].ToString() : null),
                (l, r) => $"{l},{r}");

            var correct = new[]
            {
                StreamEvent.CreateInterval(105, 110, "A1,A2"),
                StreamEvent.CreateInterval(106, 111, "B1,B2"),

                StreamEvent.CreateInterval(125, 130, "A3,A4"),
                StreamEvent.CreateInterval(126, 131, "B3,B4"),
                StreamEvent.CreateInterval(127, 132, "C3,C4"),
                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(outputStream.IsEquivalentTo(correct));
        }

        private static void JoinPointsTest(bool fixedInterval = false)
        {
            var left = new Subject<StreamEvent<string>>();
            var right = new Subject<StreamEvent<string>>();


            var qc = new QueryContainer();
            IStreamable<Empty, string> leftInput = qc.RegisterInput(left);
            IStreamable<Empty, string> rightInput = qc.RegisterInput(right);
            if (fixedInterval)
            {
                leftInput = leftInput.AlterEventDuration(1);
                rightInput = rightInput.AlterEventDuration(1);
            }

            var query = leftInput.Join(
                rightInput,
                l => (l != null ? l[0].ToString() : null),
                r => (r != null ? r[0].ToString() : null),
                (l, r) => $"{l},{r}");

            var output = new List<StreamEvent<string>>();
            qc.RegisterOutput(query).ForEachAsync(o => output.Add(o));
            var process = qc.Restore();

            // Should match and egress immediately
            left.OnNext(StreamEvent.CreatePoint(100, "A1"));
            right.OnNext(StreamEvent.CreatePoint(100, "A2"));
            process.Flush();

            var expected = new StreamEvent<string>[]
            {
                StreamEvent.CreatePoint(100, "A1,A2"),
            };
            Assert.IsTrue(expected.SequenceEqual(output));
            output.Clear();

            left.OnCompleted();
            right.OnCompleted();
        }

        private static void JoinEdgeIntervalTest()
        {
            var input1 = new[]
            {
                StreamEvent.CreateInterval(100, 110, "A1"),

                StreamEvent.CreateStart(101, "B1"),
                StreamEvent.CreateEnd(111, 101, "B1"),

                StreamEvent.CreateStart(102, "C1"),
                StreamEvent.CreateEnd(112, 102, "C1"),

                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            };

            var input2 = new[]
            {
                StreamEvent.CreateStart(105, "A2"),
                StreamEvent.CreateEnd(115, 105, "A2"),

                StreamEvent.CreateInterval(106, 116, "B2"),

                StreamEvent.CreateInterval(107, 117, "D2"),

                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            };

            var inputStream1 = input1.ToCleanStreamable();
            var inputStream2 = input2.ToCleanStreamable();
            var outputStream = inputStream1.Join(
                inputStream2,
                l => (l != null ? l[0].ToString() : null),
                r => (r != null ? r[0].ToString() : null),
                (l, r) => $"{l},{r}");

            var correct = new[]
            {
                StreamEvent.CreateStart(105, "A1,A2"),
                StreamEvent.CreateEnd(110, 105, "A1,A2"),

                StreamEvent.CreateStart(106, "B1,B2"),
                StreamEvent.CreateEnd(111, 106, "B1,B2"),

                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(outputStream.IsEquivalentTo(correct));
        }

        private static void JoinEdgesTest()
        {
            var input1 = new[]
            {
                StreamEvent.CreateStart(100, "A1"),
                StreamEvent.CreateEnd(110, 100, "A1"),

                StreamEvent.CreateStart(101, "B1"),
                StreamEvent.CreateEnd(111, 101, "B1"),

                StreamEvent.CreateStart(102, "C1"),
                StreamEvent.CreateEnd(112, 102, "C1"),

                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            };

            var input2 = new[]
            {
                StreamEvent.CreateStart(105, "A2"),
                StreamEvent.CreateEnd(115, 105, "A2"),

                StreamEvent.CreateStart(106, "B2"),
                StreamEvent.CreateEnd(116, 106, "B2"),

                StreamEvent.CreateStart(107, "D2"),
                StreamEvent.CreateEnd(117, 107, "D2"),

                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            };

            var inputStream1 = input1.ToCleanStreamable();
            var inputStream2 = input2.ToCleanStreamable();
            var outputStream = inputStream1.Join(
                inputStream2,
                l => (l != null ? l[0].ToString() : null),
                r => (r != null ? r[0].ToString() : null),
                (l, r) => $"{l},{r}");

            var correct = new[]
            {
                StreamEvent.CreateStart(105, "A1,A2"),
                StreamEvent.CreateEnd(110, 105, "A1,A2"),

                StreamEvent.CreateStart(106, "B1,B2"),
                StreamEvent.CreateEnd(111, 106, "B1,B2"),

                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(outputStream.IsEquivalentTo(correct));
        }

        private static void PartitionedStartEdgeJoinTest()
        {
            var input1 = new PartitionedStreamEvent<string, string>[]
            {
                PartitionedStreamEvent.CreateStart<string, string>("Partition1", 100, "A1"),
                PartitionedStreamEvent.CreateStart<string, string>("Partition1", 101, "B1"),
                PartitionedStreamEvent.CreateStart<string, string>("Partition1", 102, "C1"),

                PartitionedStreamEvent.CreateStart<string, string>("Partition2", 100, "A1"),
                PartitionedStreamEvent.CreateStart<string, string>("Partition2", 101, "B1"),
                PartitionedStreamEvent.CreateStart<string, string>("Partition2", 102, "C1"),
            };

            var input2 = new PartitionedStreamEvent<string, string>[]
            {
                PartitionedStreamEvent.CreateStart<string, string>("Partition1", 105, "A2"),
                PartitionedStreamEvent.CreateStart<string, string>("Partition1", 106, "B2"),
                PartitionedStreamEvent.CreateStart<string, string>("Partition1", 107, "D2"),

                PartitionedStreamEvent.CreateStart<string, string>("Partition2", 108, "A2"),
                PartitionedStreamEvent.CreateStart<string, string>("Partition2", 109, "D2"),
                PartitionedStreamEvent.CreateStart<string, string>("Partition2", 110, "C2"),
            };

            // Set properties to start-edge only
            var inputStream1 = input1.ToObservable().ToStreamable();
            inputStream1.Properties.IsConstantDuration = true;
            inputStream1.Properties.ConstantDurationLength = StreamEvent.InfinitySyncTime;
            var inputStream2 = input2.ToObservable().ToStreamable();
            inputStream2.Properties.IsConstantDuration = true;
            inputStream2.Properties.ConstantDurationLength = StreamEvent.InfinitySyncTime;

            var output = new List<PartitionedStreamEvent<string, string>>();
            inputStream1
                .Join(
                    inputStream2,
                    l => (l != null ? l[0].ToString() : null),
                    r => (r != null ? r[0].ToString() : null),
                    (l, r) => $"{l},{r}")
                .ToStreamEventObservable()
                .ForEachAsync(e => output.Add(e))
                .Wait();

            var correct = new PartitionedStreamEvent<string, string>[]
            {
                PartitionedStreamEvent.CreateStart<string, string>("Partition1", 105, "A1,A2"),
                PartitionedStreamEvent.CreateStart<string, string>("Partition1", 106, "B1,B2"),

                PartitionedStreamEvent.CreateStart<string, string>("Partition2", 108, "A1,A2"),
                PartitionedStreamEvent.CreateStart<string, string>("Partition2", 110, "C1,C2"),

                PartitionedStreamEvent.CreateLowWatermark<string, string>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.SequenceEqual(correct));
        }
    }
}
