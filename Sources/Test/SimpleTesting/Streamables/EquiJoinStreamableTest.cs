// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class EquiJoinStreamableTest : TestWithConfigSettingsAndMemoryLeakDetection
    {
        [TestMethod, TestCategory("Gated")]
        public void EquiJoinStreamable()
        {
            JoinIntervalsTest();
            JoinEdgeIntervalTest();
            JoinEdgesTest();
        }

        private static void JoinIntervalsTest()
        {
            var input1 = new[]
            {
                StreamEvent.CreateInterval(100, 110, "A1"),
                StreamEvent.CreateInterval(101, 111, "B1"),
                StreamEvent.CreateInterval(102, 112, "C1"),
                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            };

            var input2 = new[]
            {
                StreamEvent.CreateInterval(105, 115, "A2"),
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
                (l, r) => l + "," + r);

            var correct = new[]
            {
                StreamEvent.CreateInterval(105, 110, "A1,A2"),
                StreamEvent.CreateInterval(106, 111, "B1,B2"),
                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(outputStream.IsEquivalentTo(correct));
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
                (l, r) => l + "," + r);

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
                (l, r) => l + "," + r);

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
    }
}
