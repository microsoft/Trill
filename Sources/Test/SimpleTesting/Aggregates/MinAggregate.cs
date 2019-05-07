// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class MinAggregate : TestWithConfigSettingsAndMemoryLeakDetection
    {
        [TestMethod, TestCategory("Gated")]
        public void TestMinAggregate()
        {
            /*
             * Time:     1  2  3  4  5
             * Input: 50 |-----------|
             *        40 |-----|
             *        30    |--|
             *        20    |-----|
             *        10       |--|
             * Output:   40 20 10 50 0
             */
            var input = new[]
            {
                StreamEvent.CreateInterval(1, 5, 50),
                StreamEvent.CreateInterval(1, 3, 40),
                StreamEvent.CreateInterval(2, 3, 30),
                StreamEvent.CreateInterval(2, 4, 20),
                StreamEvent.CreateInterval(3, 4, 10),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            }.ToStreamable();

            var output = input.Min(v => v);

            var correct = new[]
            {
                StreamEvent.CreateStart(1, 40),
                StreamEvent.CreateEnd(2, 1, 40),

                StreamEvent.CreateStart(2, 20),
                StreamEvent.CreateEnd(3, 2, 20),

                StreamEvent.CreateStart(3, 10),
                StreamEvent.CreateEnd(4, 3, 10),

                StreamEvent.CreateStart(4, 50),
                StreamEvent.CreateEnd(5, 4, 50),

                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }
    }
}
