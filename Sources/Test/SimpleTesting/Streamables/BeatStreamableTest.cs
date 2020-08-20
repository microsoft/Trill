// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class BeatStreamableTest : TestWithConfigSettingsAndMemoryLeakDetection
    {
        [TestMethod, TestCategory("Gated")]
        public void BeatStreamable()
        {
            var input = new[]
            {
                StreamEvent.CreateInterval(100, 109, "P1"),
                StreamEvent.CreateInterval(100, 110, "P2"),
                StreamEvent.CreateInterval(100, 111, "P3"),
                StreamEvent.CreateInterval(100, 150, "P4"),

                StreamEvent.CreateStart(100, "P5"),
                StreamEvent.CreateEnd(109, 100, "P5"),

                StreamEvent.CreateStart(100, "P6"),
                StreamEvent.CreateEnd(110, 100, "P6"),

                StreamEvent.CreateStart(100, "P7"),
                StreamEvent.CreateEnd(111, 100, "P7"),

                StreamEvent.CreateStart(100, "P8"),
                StreamEvent.CreateEnd(150, 100, "P8"),

                StreamEvent.CreateInterval(101, 109, "P9"),
                StreamEvent.CreateInterval(101, 110, "P10"),
                StreamEvent.CreateInterval(101, 111, "P11"),
                StreamEvent.CreateInterval(101, 150, "P12"),

                StreamEvent.CreateStart(101, "P13"),
                StreamEvent.CreateEnd(109, 101, "P13"),

                StreamEvent.CreateStart(101, "P14"),
                StreamEvent.CreateEnd(110, 101, "P14"),

                StreamEvent.CreateStart(101, "P15"),
                StreamEvent.CreateEnd(111, 101, "P15"),

                StreamEvent.CreateStart(101, "P16"),
                StreamEvent.CreateEnd(150, 101, "P16"),

                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            };

            var inputStream = input.ToCleanStreamable();
            var outputStream = inputStream.Chop(0, 10);

            var correct = new[]
            {
                StreamEvent.CreateInterval(100, 109, "P1"),

                StreamEvent.CreateInterval(100, 110, "P2"),

                StreamEvent.CreateInterval(100, 110, "P3"),
                StreamEvent.CreateInterval(110, 111, "P3"),

                StreamEvent.CreateInterval(100, 110, "P4"),
                StreamEvent.CreateInterval(110, 120, "P4"),
                StreamEvent.CreateInterval(120, 130, "P4"),
                StreamEvent.CreateInterval(130, 140, "P4"),
                StreamEvent.CreateInterval(140, 150, "P4"),

                StreamEvent.CreateStart(100, "P5"),
                StreamEvent.CreateEnd(109, 100, "P5"),

                StreamEvent.CreateStart(100, "P6"),
                StreamEvent.CreateEnd(110, 100, "P6"),

                StreamEvent.CreateStart(100, "P7"),
                StreamEvent.CreateEnd(110, 100, "P7"),
                StreamEvent.CreateStart(110, "P7"),
                StreamEvent.CreateEnd(111, 110, "P7"),

                StreamEvent.CreateStart(100, "P8"),
                StreamEvent.CreateEnd(110, 100, "P8"),
                StreamEvent.CreateStart(110, "P8"),
                StreamEvent.CreateEnd(120, 110, "P8"),
                StreamEvent.CreateInterval(120, 130, "P8"),
                StreamEvent.CreateInterval(130, 140, "P8"),
                StreamEvent.CreateInterval(140, 150, "P8"),

                StreamEvent.CreateInterval(101, 109, "P9"),

                StreamEvent.CreateInterval(101, 110, "P10"),

                StreamEvent.CreateInterval(101, 110, "P11"),
                StreamEvent.CreateInterval(110, 111, "P11"),

                StreamEvent.CreateInterval(101, 110, "P12"),
                StreamEvent.CreateInterval(110, 120, "P12"),
                StreamEvent.CreateInterval(120, 130, "P12"),
                StreamEvent.CreateInterval(130, 140, "P12"),
                StreamEvent.CreateInterval(140, 150, "P12"),

                StreamEvent.CreateStart(101, "P13"),
                StreamEvent.CreateEnd(109, 101, "P13"),

                StreamEvent.CreateStart(101, "P14"),
                StreamEvent.CreateEnd(110, 101, "P14"),

                StreamEvent.CreateStart(101, "P15"),
                StreamEvent.CreateEnd(110, 101, "P15"),
                StreamEvent.CreateStart(110, "P15"),
                StreamEvent.CreateEnd(111, 110, "P15"),

                StreamEvent.CreateStart(101, "P16"),
                StreamEvent.CreateEnd(110, 101, "P16"),
                StreamEvent.CreateStart(110, "P16"),
                StreamEvent.CreateEnd(120, 110, "P16"),
                StreamEvent.CreateInterval(120, 130, "P16"),
                StreamEvent.CreateInterval(130, 140, "P16"),
                StreamEvent.CreateInterval(140, 150, "P16"),

                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(outputStream.IsEquivalentTo(correct));
        }

        /// <summary>
        /// Test that causes codegen to create a columnar Beat operator.
        /// Need to have a payload that is columnar and string ingress produces
        /// a stream that isn't considered to be columnar. The second component
        /// of the tuples is ignored, so the output of the test is the same
        /// as the test above (BeatStreamable).
        /// </summary>
        [TestMethod, TestCategory("Gated")]
        public void BeatStreamable2()
        {
            var input = new[]
            {
                StreamEvent.CreateInterval(100, 109, StructTuple.Create("P1", 2)),

                StreamEvent.CreateInterval(100, 110, StructTuple.Create("P2", 2)),

                StreamEvent.CreateInterval(100, 111, StructTuple.Create("P3", 2)),

                StreamEvent.CreateInterval(100, 150, StructTuple.Create("P4", 2)),

                StreamEvent.CreateStart(100, StructTuple.Create("P5", 2)),
                StreamEvent.CreateEnd(109, 100, StructTuple.Create("P5", 2)),

                StreamEvent.CreateStart(100, StructTuple.Create("P6", 2)),
                StreamEvent.CreateEnd(110, 100, StructTuple.Create("P6", 2)),

                StreamEvent.CreateStart(100, StructTuple.Create("P7", 2)),
                StreamEvent.CreateEnd(111, 100, StructTuple.Create("P7", 2)),

                StreamEvent.CreateStart(100, StructTuple.Create("P8", 2)),
                StreamEvent.CreateEnd(150, 100, StructTuple.Create("P8", 2)),

                StreamEvent.CreateInterval(101, 109, StructTuple.Create("P9", 2)),

                StreamEvent.CreateInterval(101, 110, StructTuple.Create("P10", 2)),

                StreamEvent.CreateInterval(101, 111, StructTuple.Create("P11", 2)),

                StreamEvent.CreateInterval(101, 150, StructTuple.Create("P12", 2)),

                StreamEvent.CreateStart(101, StructTuple.Create("P13", 2)),
                StreamEvent.CreateEnd(109, 101, StructTuple.Create("P13", 2)),

                StreamEvent.CreateStart(101, StructTuple.Create("P14", 2)),
                StreamEvent.CreateEnd(110, 101, StructTuple.Create("P14", 2)),

                StreamEvent.CreateStart(101, StructTuple.Create("P15", 2)),
                StreamEvent.CreateEnd(111, 101, StructTuple.Create("P15", 2)),

                StreamEvent.CreateStart(101, StructTuple.Create("P16", 2)),
                StreamEvent.CreateEnd(150, 101, StructTuple.Create("P16", 2)),

                StreamEvent.CreatePunctuation<StructTuple<string, int>>(StreamEvent.InfinitySyncTime)
            };

            var inputStream = input.ToCleanStreamable();
            var outputStream = inputStream.Chop(0, 10).Select(e => e.Item1);

            var correct = new[]
            {
                StreamEvent.CreateInterval(100, 109, "P1"),

                StreamEvent.CreateInterval(100, 110, "P2"),

                StreamEvent.CreateInterval(100, 110, "P3"),
                StreamEvent.CreateInterval(110, 111, "P3"),

                StreamEvent.CreateInterval(100, 110, "P4"),
                StreamEvent.CreateInterval(110, 120, "P4"),
                StreamEvent.CreateInterval(120, 130, "P4"),
                StreamEvent.CreateInterval(130, 140, "P4"),
                StreamEvent.CreateInterval(140, 150, "P4"),

                StreamEvent.CreateStart(100, "P5"),
                StreamEvent.CreateEnd(109, 100, "P5"),

                StreamEvent.CreateStart(100, "P6"),
                StreamEvent.CreateEnd(110, 100, "P6"),

                StreamEvent.CreateStart(100, "P7"),
                StreamEvent.CreateEnd(110, 100, "P7"),
                StreamEvent.CreateStart(110, "P7"),
                StreamEvent.CreateEnd(111, 110, "P7"),

                StreamEvent.CreateStart(100, "P8"),
                StreamEvent.CreateEnd(110, 100, "P8"),
                StreamEvent.CreateStart(110, "P8"),
                StreamEvent.CreateEnd(120, 110, "P8"),
                StreamEvent.CreateInterval(120, 130, "P8"),
                StreamEvent.CreateInterval(130, 140, "P8"),
                StreamEvent.CreateInterval(140, 150, "P8"),

                StreamEvent.CreateInterval(101, 109, "P9"),

                StreamEvent.CreateInterval(101, 110, "P10"),

                StreamEvent.CreateInterval(101, 110, "P11"),
                StreamEvent.CreateInterval(110, 111, "P11"),

                StreamEvent.CreateInterval(101, 110, "P12"),
                StreamEvent.CreateInterval(110, 120, "P12"),
                StreamEvent.CreateInterval(120, 130, "P12"),
                StreamEvent.CreateInterval(130, 140, "P12"),
                StreamEvent.CreateInterval(140, 150, "P12"),

                StreamEvent.CreateStart(101, "P13"),
                StreamEvent.CreateEnd(109, 101, "P13"),

                StreamEvent.CreateStart(101, "P14"),
                StreamEvent.CreateEnd(110, 101, "P14"),

                StreamEvent.CreateStart(101, "P15"),
                StreamEvent.CreateEnd(110, 101, "P15"),
                StreamEvent.CreateStart(110, "P15"),
                StreamEvent.CreateEnd(111, 110, "P15"),

                StreamEvent.CreateStart(101, "P16"),
                StreamEvent.CreateEnd(110, 101, "P16"),
                StreamEvent.CreateStart(110, "P16"),
                StreamEvent.CreateEnd(120, 110, "P16"),
                StreamEvent.CreateInterval(120, 130, "P16"),
                StreamEvent.CreateInterval(130, 140, "P16"),
                StreamEvent.CreateInterval(140, 150, "P16"),

                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(outputStream.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void PartitionedChop()
        {
            var input = new[]
            {
                PartitionedStreamEvent.CreateInterval(0, 100, 109, StructTuple.Create("P1", 2)),
                PartitionedStreamEvent.CreateInterval(0, 100, 110, StructTuple.Create("P2", 2)),
                PartitionedStreamEvent.CreateInterval(0, 100, 111, StructTuple.Create("P3", 2)),
                PartitionedStreamEvent.CreateInterval(0, 100, 150, StructTuple.Create("P4", 2)),

                PartitionedStreamEvent.CreateInterval(1, 100, 109, StructTuple.Create("P1", 2)),
                PartitionedStreamEvent.CreateInterval(1, 100, 110, StructTuple.Create("P2", 2)),
                PartitionedStreamEvent.CreateInterval(1, 100, 111, StructTuple.Create("P3", 2)),
                PartitionedStreamEvent.CreateInterval(1, 100, 150, StructTuple.Create("P4", 2)),

                PartitionedStreamEvent.CreateLowWatermark<int, StructTuple<string, int>>(100),

                PartitionedStreamEvent.CreateStart(0, 100, StructTuple.Create("P5", 2)),
                PartitionedStreamEvent.CreateEnd(0, 109, 100, StructTuple.Create("P5", 2)),
                PartitionedStreamEvent.CreateStart(0, 100, StructTuple.Create("P6", 2)),
                PartitionedStreamEvent.CreateEnd(0, 110, 100, StructTuple.Create("P6", 2)),

                PartitionedStreamEvent.CreateStart(1, 100, StructTuple.Create("P5", 2)),
                PartitionedStreamEvent.CreateEnd(1, 109, 100, StructTuple.Create("P5", 2)),
                PartitionedStreamEvent.CreateStart(1, 100, StructTuple.Create("P6", 2)),
                PartitionedStreamEvent.CreateEnd(1, 110, 100, StructTuple.Create("P6", 2)),

                PartitionedStreamEvent.CreateLowWatermark<int, StructTuple<string, int>>(StreamEvent.InfinitySyncTime)
            }
                .OrderBy(v => v.SyncTime)
                .ToArray();

            var outputStream = input
                .ToObservable()
                .ToStreamable()
                .Chop(0, 10)
                .Select(payload => payload.Item1);

            var correct = new PartitionedStreamEvent<int, string>[]
            {
                PartitionedStreamEvent.CreateInterval(0, 100, 109, "P1"),
                PartitionedStreamEvent.CreateInterval(0, 100, 110, "P2"),
                PartitionedStreamEvent.CreateInterval(0, 100, 110, "P3"),
                PartitionedStreamEvent.CreateInterval(0, 100, 110, "P4"),

                PartitionedStreamEvent.CreateInterval(1, 100, 109, "P1"),
                PartitionedStreamEvent.CreateInterval(1, 100, 110, "P2"),
                PartitionedStreamEvent.CreateInterval(1, 100, 110, "P3"),
                PartitionedStreamEvent.CreateInterval(1, 100, 110, "P4"),

                PartitionedStreamEvent.CreateLowWatermark<int, string>(100),

                PartitionedStreamEvent.CreateStart(0, 100, "P5"),
                PartitionedStreamEvent.CreateStart(0, 100, "P6"),
                PartitionedStreamEvent.CreateEnd(0, 109, 100, "P5"),
                PartitionedStreamEvent.CreateEnd(0, 110, 100, "P6"),

                PartitionedStreamEvent.CreateStart(1, 100, "P5"),
                PartitionedStreamEvent.CreateStart(1, 100, "P6"),
                PartitionedStreamEvent.CreateEnd(1, 109, 100, "P5"),
                PartitionedStreamEvent.CreateEnd(1, 110, 100, "P6"),

                PartitionedStreamEvent.CreateInterval(0, 110, 111, "P3"),
                PartitionedStreamEvent.CreateInterval(1, 110, 111, "P3"),

                PartitionedStreamEvent.CreateInterval(0, 110, 120, "P4"),
                PartitionedStreamEvent.CreateInterval(0, 120, 130, "P4"),
                PartitionedStreamEvent.CreateInterval(0, 130, 140, "P4"),
                PartitionedStreamEvent.CreateInterval(0, 140, 150, "P4"),

                PartitionedStreamEvent.CreateInterval(1, 110, 120, "P4"),
                PartitionedStreamEvent.CreateInterval(1, 120, 130, "P4"),
                PartitionedStreamEvent.CreateInterval(1, 130, 140, "P4"),
                PartitionedStreamEvent.CreateInterval(1, 140, 150, "P4"),

                PartitionedStreamEvent.CreateLowWatermark<int, string>(StreamEvent.InfinitySyncTime)
            };

            var events = outputStream.ToStreamEventObservable().ToEnumerable().ToArray();

            // Compare each key separately (this is solely for readability of the test)
            var expectedKey0 = correct.Where(e => !e.IsData || e.PartitionKey == 0).ToArray();
            var expectedKey1 = correct.Where(e => !e.IsData || e.PartitionKey == 1).ToArray();
            var actualKey0 = events.Where(e => !e.IsData || e.PartitionKey == 0).ToArray();
            var actualKey1 = events.Where(e => !e.IsData || e.PartitionKey == 1).ToArray();

            Assert.IsTrue(actualKey0.SequenceEqual(expectedKey0));
            Assert.IsTrue(actualKey1.SequenceEqual(expectedKey1));
        }
    }
}
