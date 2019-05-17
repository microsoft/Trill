// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class ClipStreamableTest : TestWithConfigSettingsAndMemoryLeakDetection
    {
        [TestMethod, TestCategory("Gated")]
        public void ClipStreamable01()
        {
            var input1 = new[]
            {
                StreamEvent.CreateInterval(100, 105, "A1"),

                StreamEvent.CreateInterval(101, 104, "A2"),

                StreamEvent.CreateInterval(105, 110, "A3"),

                StreamEvent.CreateStart(105, "A4"),
                StreamEvent.CreateEnd(110, 105, "A4"),

                StreamEvent.CreateInterval(115, 120, "A5"),

                StreamEvent.CreateStart(115, "A6"),
                StreamEvent.CreateEnd(120, 115, "A6"),

                StreamEvent.CreateInterval(100, 112, "B1"),
                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            };

            var input2 = new[]
            {
                StreamEvent.CreateInterval(105, 110, "A"),

                StreamEvent.CreateStart(105, "A"),
                StreamEvent.CreateEnd(106, 105, "A"),

                StreamEvent.CreateInterval(106, 108, "A"),

                StreamEvent.CreateStart(107, "A"),
                StreamEvent.CreateEnd(110, 107, "A"),

                StreamEvent.CreateInterval(114, 120, "A"),

                StreamEvent.CreateStart(116, "A"),
                StreamEvent.CreateEnd(120, 116, "A"),

                StreamEvent.CreateInterval(117, 118, "A"),

                StreamEvent.CreateInterval(99, 112, "C"),

                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            };

            var inputStream1 = input1.ToCleanStreamable();
            var inputStream2 = input2.ToCleanStreamable();
            var outputStream = inputStream1.ClipEventDuration(
                inputStream2,
                l => (l != null ? l[0].ToString() : null),
                r => (r != null ? r[0].ToString() : null));

            var correct = new[]
            {
                StreamEvent.CreateInterval(100, 105, "A1"),

                StreamEvent.CreateInterval(101, 104, "A2"),

                StreamEvent.CreateStart(105, "A3"),
                StreamEvent.CreateEnd(106, 105, "A3"),

                StreamEvent.CreateStart(105, "A4"),
                StreamEvent.CreateEnd(106, 105, "A4"),

                StreamEvent.CreateStart(115, "A5"),
                StreamEvent.CreateEnd(116, 115, "A5"),

                StreamEvent.CreateStart(115, "A6"),
                StreamEvent.CreateEnd(116, 115, "A6"),

                StreamEvent.CreateStart(100, "B1"),
                StreamEvent.CreateEnd(112, 100, "B1"),

                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(outputStream.IsEquivalentTo(correct));
        }
        [TestMethod, TestCategory("Gated")]
        public void ClipStreamable02()
        {
            var input1 = new[]
            {
                StreamEvent.CreateInterval(100, 105, 1),
                StreamEvent.CreateInterval(105, 110, 3),

                StreamEvent.CreateStart(105, 4),
                StreamEvent.CreateEnd(110, 105, 4),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };

            var input2 = new[]
            {
                StreamEvent.CreateInterval(105, 110, 0),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };

            var inputStream1 = input1.ToCleanStreamable();
            var inputStream2 = input2.ToCleanStreamable();
            var outputStream = inputStream1.ClipEventDuration(
                inputStream2,
                l => l % 10,
                r => r);

            var correct = new[]
            {
                StreamEvent.CreateInterval(100, 105, 1),
                StreamEvent.CreateStart(105, 3),
                StreamEvent.CreateStart(105, 4),
                StreamEvent.CreateEnd(110, 105, 3),
                StreamEvent.CreateEnd(110, 105, 4),

                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(outputStream.IsEquivalentTo(correct));
        }
        [TestMethod, TestCategory("Gated")]
        public void ClipStreamable03()
        {
            var input1 = new[]
            {
                StreamEvent.CreateInterval(100, 105, 1),

                StreamEvent.CreateInterval(101, 104, 2),

                StreamEvent.CreateInterval(105, 110, 3),

                StreamEvent.CreateStart(105, 4),
                StreamEvent.CreateEnd(110, 105, 4),

                StreamEvent.CreateInterval(115, 120, 5),

                StreamEvent.CreateStart(115, 6),
                StreamEvent.CreateEnd(120, 115, 6),

                StreamEvent.CreateInterval(100, 112, 10),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };

            var input2 = new[]
            {
                StreamEvent.CreateInterval(105, 110, 0),

                StreamEvent.CreateStart(105, 0),
                StreamEvent.CreateEnd(106, 105, 0),

                StreamEvent.CreateInterval(106, 108, 0),

                StreamEvent.CreateStart(107, 0),
                StreamEvent.CreateEnd(110, 107, 0),

                StreamEvent.CreateInterval(114, 120, 0),

                StreamEvent.CreateStart(116, 0),
                StreamEvent.CreateEnd(120, 116, 0),

                StreamEvent.CreateInterval(117, 118, 0),

                StreamEvent.CreateInterval(99, 112, 2),

                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };

            var inputStream1 = input1.ToCleanStreamable();
            var inputStream2 = input2.ToCleanStreamable();
            var outputStream = inputStream1.ClipEventDuration(
                inputStream2,
                l => l % 10,
                r => r);

            var correct = new[]
            {
                StreamEvent.CreateInterval(100, 105, 1),
                StreamEvent.CreateStart(100, 10),
                StreamEvent.CreateInterval(101, 104, 2),
                StreamEvent.CreateEnd(105, 100, 10),
                StreamEvent.CreateStart(105, 3),
                StreamEvent.CreateStart(105, 4),
                StreamEvent.CreateEnd(110, 105, 3),
                StreamEvent.CreateEnd(110, 105, 4),
                StreamEvent.CreateStart(115, 5),
                StreamEvent.CreateStart(115, 6),
                StreamEvent.CreateEnd(120, 115, 5),
                StreamEvent.CreateEnd(120, 115, 6),

                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(outputStream.IsEquivalentTo(correct));
        }
    }
}
