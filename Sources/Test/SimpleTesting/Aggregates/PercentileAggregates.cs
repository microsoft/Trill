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
    public class PercentileAggregates : TestWithConfigSettingsAndMemoryLeakDetection
    {
        [TestMethod, TestCategory("Gated")]
        public void PercentileDiscAggregate()
        {
            /*
             * Time:     1  2  3  4  5
             * Input: 10 |-----------|
             *        20 |-----|
             *        30    |--|
             *        40    |-----|
             *        50       |--|
             * Output:   20 30 40 10
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

            var correct = new[]
            {
                StreamEvent.CreateStart(1, 20.0),
                StreamEvent.CreateEnd(2, 1, 20.0),

                StreamEvent.CreateStart(2, 30.0),
                StreamEvent.CreateEnd(3, 2, 30.0),

                StreamEvent.CreateStart(3, 40.0),
                StreamEvent.CreateEnd(4, 3, 40.0),

                StreamEvent.CreateStart(4, 10.0),
                StreamEvent.CreateEnd(5, 4, 10.0),

                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime)
            };

            var output = input.Aggregate(w => w.PercentileDiscrete(0.6, e => e));
            Assert.IsTrue(output.IsEquivalentTo(correct));

            // And with reverse order...
            output = input.Aggregate(w => w.PercentileDiscrete((x, y) => y.CompareTo(x), 0.4, e => e));
            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void PercentileContAggregate()
        {
            /*
             * Time:     1  2  3  4  5
             * Input: 10 |-----------|
             *        20 |-----|
             *        30    |--|
             *        40    |-----|
             *        50       |--|
             * Output:   16 28 42 10
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

            var correct = new[]
            {
                StreamEvent.CreateStart(1, 16.0),
                StreamEvent.CreateEnd(2, 1, 16.0),

                StreamEvent.CreateStart(2, 28.0),
                StreamEvent.CreateEnd(3, 2, 28.0),

                StreamEvent.CreateStart(3, 42.0),
                StreamEvent.CreateEnd(4, 3, 42.0),

                StreamEvent.CreateStart(4, 10.0),
                StreamEvent.CreateEnd(5, 4, 10.0),

                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime)
            };

            var output = input.Aggregate(w => w.PercentileContinuous(0.6, e => e));
            Assert.IsTrue(output.IsEquivalentTo(correct));

            // And with reverse order...
            output = input.Aggregate(w => w.PercentileContinuous((x, y) => y.CompareTo(x), 0.4, e => e));
            Assert.IsTrue(output.IsEquivalentTo(correct));
        }
    }
}
