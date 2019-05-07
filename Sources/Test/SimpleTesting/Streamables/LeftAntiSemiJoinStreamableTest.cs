// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class LeftAntiSemiJoinStreamableTest : TestWithConfigSettingsAndMemoryLeakDetection
    {
        [TestMethod, TestCategory("Gated")]
        public void LeftAntiSemiJoinStreamable()
        {
            LeftAntiSemiJoinIntervalsTest();
            LeftAntiSemiJoinEdgesTest();
        }

        private static void LeftAntiSemiJoinIntervalsTest()
        {
            var input1 = new[]
            {
                StreamEvent.CreateInterval(100, 101, "A3"),
                StreamEvent.CreateInterval(100, 110, "A1"),
                StreamEvent.CreateInterval(101, 105, "A2"),
                StreamEvent.CreateInterval(109, 112, "A4"),
                StreamEvent.CreateInterval(102, 112, "B1"),
                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            };

            var input2 = new[]
            {
                StreamEvent.CreateInterval(101, 103, "A"),
                StreamEvent.CreateInterval(106, 108, "A"),
                StreamEvent.CreateInterval(106, 109, "A"),
                StreamEvent.CreateInterval(100, 110, "C"),
                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            };

            var inputStream1 = input1.ToCleanStreamable();
            var inputStream2 = input2.ToCleanStreamable();
            var outputStream = inputStream1.WhereNotExists(
                inputStream2,
                l => (l != null ? l[0].ToString() : null),
                r => (r != null ? r[0].ToString() : null));

            var correct = new[]
            {
                StreamEvent.CreateStart(100, "A1"),
                StreamEvent.CreateEnd(101, 100, "A1"),

                StreamEvent.CreateStart(103, "A1"),
                StreamEvent.CreateEnd(106, 103, "A1"),

                StreamEvent.CreateInterval(109, 110, "A1"),

                StreamEvent.CreateInterval(103, 105, "A2"),

                StreamEvent.CreateInterval(100, 101, "A3"),

                StreamEvent.CreateInterval(109, 112, "A4"),

                StreamEvent.CreateStart(102, "B1"),
                StreamEvent.CreateEnd(112, 102, "B1"),

                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(outputStream.IsEquivalentTo(correct));
        }

        private static void LeftAntiSemiJoinEdgesTest()
        {
            var input1 = new[]
            {
                StreamEvent.CreateStart(100, "A1"),
                StreamEvent.CreateEnd(110, 100, "A1"),

                StreamEvent.CreateStart(101, "A2"),
                StreamEvent.CreateEnd(105, 101, "A2"),

                StreamEvent.CreateStart(102, "B1"),
                StreamEvent.CreateEnd(112, 102, "B1"),

                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            };

            var input2 = new[]
            {
                StreamEvent.CreateStart(101, "A"),
                StreamEvent.CreateEnd(103, 101, "A"),

                StreamEvent.CreateStart(106, "A"),
                StreamEvent.CreateEnd(108, 106, "A"),

                StreamEvent.CreateStart(106, "A"),
                StreamEvent.CreateEnd(109, 106, "A"),

                StreamEvent.CreateStart(100, "C"),
                StreamEvent.CreateEnd(110, 100, "C"),

                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            };

            var inputStream1 = input1.ToCleanStreamable();
            var inputStream2 = input2.ToCleanStreamable();
            var outputStream = inputStream1.WhereNotExists(
                inputStream2,
                l => (l != null ? l[0].ToString() : null),
                r => (r != null ? r[0].ToString() : null));

            var correct = new[]
            {
                StreamEvent.CreateStart(100, "A1"),
                StreamEvent.CreateEnd(101, 100, "A1"),

                StreamEvent.CreateStart(103, "A2"),
                StreamEvent.CreateEnd(105, 103, "A2"),

                StreamEvent.CreateStart(103, "A1"),
                StreamEvent.CreateEnd(106, 103, "A1"),

                StreamEvent.CreateStart(109, "A1"),
                StreamEvent.CreateEnd(110, 109, "A1"),

                StreamEvent.CreateStart(102, "B1"),
                StreamEvent.CreateEnd(112, 102, "B1"),

                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(outputStream.IsEquivalentTo(correct));
        }
    }
}
