// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class TopKAggregate : TestWithConfigSettingsAndMemoryLeakDetection
    {
        [TestMethod, TestCategory("Gated")]
        public void TestTopKAggregate()
        {
            /*
             * Time:     1  2  3  4  5
             * Input: 10 |-----------|
             *        20 |-----|
             *        30    |--|
             *        40    |-----|
             *        50       |--|
             * Output:   20 40 50 10
             *           10 30 40
             *              20 10
             */
            var input = new[]
            {
                StreamEvent.CreateInterval(1, 5, 10),
                StreamEvent.CreateInterval(1, 3, 20),
                StreamEvent.CreateInterval(2, 3, 30),
                StreamEvent.CreateInterval(2, 4, 40),
                StreamEvent.CreateInterval(3, 4, 50),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            }.ToStreamable();

            var output = input.TopK(v => v, 3).SelectMany(v => v).Select(v => v.Payload);

            var correct = new[]
            {
                StreamEvent.CreateStart(1, 20),
                StreamEvent.CreateEnd(2, 1, 20),
                StreamEvent.CreateStart(1, 10),
                StreamEvent.CreateEnd(2, 1, 10),

                StreamEvent.CreateStart(2, 40),
                StreamEvent.CreateEnd(3, 2, 40),
                StreamEvent.CreateStart(2, 30),
                StreamEvent.CreateEnd(3, 2, 30),
                StreamEvent.CreateStart(2, 20),
                StreamEvent.CreateEnd(3, 2, 20),

                StreamEvent.CreateStart(3, 50),
                StreamEvent.CreateEnd(4, 3, 50),
                StreamEvent.CreateStart(3, 40),
                StreamEvent.CreateEnd(4, 3, 40),
                StreamEvent.CreateStart(3, 10),
                StreamEvent.CreateEnd(4, 3, 10),

                StreamEvent.CreateStart(4, 10),
                StreamEvent.CreateEnd(5, 4, 10),

                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void TestTopKRobsBug()
        {
            /*
             * Time:     1  2  3  4  5
             * Input: 10 |-----------|
             *        20 |-----|
             *        30    |--|
             *        40    |-----|
             *        50       |--|
             * Output:   20 40 50 10
             *           10 30 40
             *              20 10
             */
            var input = new[]
            {
                StreamEvent.CreateInterval(1, 5, 10),
                StreamEvent.CreateInterval(1, 3, 20),
                StreamEvent.CreateInterval(2, 3, 30),
                StreamEvent.CreateInterval(2, 4, 40),
                StreamEvent.CreateInterval(3, 4, 50),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            }.ToStreamable();

            var output = input.Aggregate(a => a.TopK(v => v, 3), b => b.Count(), (a, b) => a.Select(v => v.Payload)).SelectMany(v => v);

            var correct = new[]
            {
                StreamEvent.CreateStart(1, 20),
                StreamEvent.CreateEnd(2, 1, 20),
                StreamEvent.CreateStart(1, 10),
                StreamEvent.CreateEnd(2, 1, 10),

                StreamEvent.CreateStart(2, 40),
                StreamEvent.CreateEnd(3, 2, 40),
                StreamEvent.CreateStart(2, 30),
                StreamEvent.CreateEnd(3, 2, 30),
                StreamEvent.CreateStart(2, 20),
                StreamEvent.CreateEnd(3, 2, 20),

                StreamEvent.CreateStart(3, 50),
                StreamEvent.CreateEnd(4, 3, 50),
                StreamEvent.CreateStart(3, 40),
                StreamEvent.CreateEnd(4, 3, 40),
                StreamEvent.CreateStart(3, 10),
                StreamEvent.CreateEnd(4, 3, 10),

                StreamEvent.CreateStart(4, 10),
                StreamEvent.CreateEnd(5, 4, 10),

                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }
    }
}
