// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class WhereAggregate : TestWithConfigSettingsAndMemoryLeakDetection
    {
        [TestMethod, TestCategory("Gated")]
        public void TestWhereAggregate()
        {
            /*
             * Time:     1 2 3 4 5
             * Input: 10 |-------|
             *        20 |---|
             *        30   |-|
             *        40   |---|
             *        50     |-|
             * Output:   2 4 3 1 0
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

            var output = input.Aggregate(
                a => a.Count(),
                b => b.Where(v => v >= 30).Count(),
                b => b.Where(v => v >= 30).Where(v => v <= 40).Count(),
                (a, b, c) => (a * 100) + (b * 10) + c);

            var correct = new[]
            {
                StreamEvent.CreateStart<ulong>(1, 200),
                StreamEvent.CreateEnd<ulong>(2, 1, 200),

                StreamEvent.CreateStart<ulong>(2, 422),
                StreamEvent.CreateEnd<ulong>(3, 2, 422),

                StreamEvent.CreateStart<ulong>(3, 321),
                StreamEvent.CreateEnd<ulong>(4, 3, 321),

                StreamEvent.CreateStart<ulong>(4, 100),
                StreamEvent.CreateEnd<ulong>(5, 4, 100),

                StreamEvent.CreatePunctuation<ulong>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }
    }
}
