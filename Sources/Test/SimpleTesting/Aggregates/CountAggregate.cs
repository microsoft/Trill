// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class CountAggregate : TestWithConfigSettingsAndMemoryLeakDetection
    {
        [TestMethod, TestCategory("Gated")]
        public void TestCountAggregate_NoNulls()
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

            var output = input.Count();

            var correct = new[]
            {
                StreamEvent.CreateStart<ulong>(1, 2),
                StreamEvent.CreateEnd<ulong>(2, 1, 2),

                StreamEvent.CreateStart<ulong>(2, 4),
                StreamEvent.CreateEnd<ulong>(3, 2, 4),

                StreamEvent.CreateStart<ulong>(3, 3),
                StreamEvent.CreateEnd<ulong>(4, 3, 3),

                StreamEvent.CreateStart<ulong>(4, 1),
                StreamEvent.CreateEnd<ulong>(5, 4, 1),

                StreamEvent.CreatePunctuation<ulong>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void TestCountAggregate_WithNulls()
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
                StreamEvent.CreateInterval(1, 5, "10"),
                StreamEvent.CreateInterval<string>(1, 3, null),
                StreamEvent.CreateInterval(2, 3, "30"),
                StreamEvent.CreateInterval(2, 4, "40"),
                StreamEvent.CreateInterval(3, 4, "50"),
                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            }.ToStreamable();

            var output = input.Count();

            var correct = new[]
            {
                StreamEvent.CreateStart<ulong>(1, 2),
                StreamEvent.CreateEnd<ulong>(2, 1, 2),

                StreamEvent.CreateStart<ulong>(2, 4),
                StreamEvent.CreateEnd<ulong>(3, 2, 4),

                StreamEvent.CreateStart<ulong>(3, 3),
                StreamEvent.CreateEnd<ulong>(4, 3, 3),

                StreamEvent.CreateStart<ulong>(4, 1),
                StreamEvent.CreateEnd<ulong>(5, 4, 1),

                StreamEvent.CreatePunctuation<ulong>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void TestCountNotNullAggregate_NoNulls()
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
                StreamEvent.CreateInterval(1, 5, "10"),
                StreamEvent.CreateInterval(1, 3, "20"),
                StreamEvent.CreateInterval(2, 3, "30"),
                StreamEvent.CreateInterval(2, 4, "40"),
                StreamEvent.CreateInterval(3, 4, "50"),
                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            }.ToStreamable();

            var output = input.CountNotNull(v => v);

            var correct = new[]
            {
                StreamEvent.CreateStart<ulong>(1, 2),
                StreamEvent.CreateEnd<ulong>(2, 1, 2),

                StreamEvent.CreateStart<ulong>(2, 4),
                StreamEvent.CreateEnd<ulong>(3, 2, 4),

                StreamEvent.CreateStart<ulong>(3, 3),
                StreamEvent.CreateEnd<ulong>(4, 3, 3),

                StreamEvent.CreateStart<ulong>(4, 1),
                StreamEvent.CreateEnd<ulong>(5, 4, 1),

                StreamEvent.CreatePunctuation<ulong>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void TestCountNotNullAggregate_WithNulls()
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
                StreamEvent.CreateInterval(1, 5, "10"),
                StreamEvent.CreateInterval<string>(1, 3, null),
                StreamEvent.CreateInterval(2, 3, "30"),
                StreamEvent.CreateInterval(2, 4, "40"),
                StreamEvent.CreateInterval(3, 4, "50"),
                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            }.ToStreamable();

            var output = input.CountNotNull(v => v);

            var correct = new[]
            {
                StreamEvent.CreateStart<ulong>(1, 1),
                StreamEvent.CreateEnd<ulong>(2, 1, 1),

                StreamEvent.CreateStart<ulong>(2, 3),
                StreamEvent.CreateEnd<ulong>(3, 2, 3),

                StreamEvent.CreateStart<ulong>(3, 3),
                StreamEvent.CreateEnd<ulong>(4, 3, 3),

                StreamEvent.CreateStart<ulong>(4, 1),
                StreamEvent.CreateEnd<ulong>(5, 4, 1),

                StreamEvent.CreatePunctuation<ulong>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void TestCountNotNullAggregate_AllNulls()
        {
            /*
             * Time:     1 2 3 4 5
             * Input: 10 |-------|
             *        20 |---|
             *        30   |-|
             *        40   |---|
             *        50     |-|
             * Output:   0 0 0 0 0
             */
            var input = new[]
            {
                StreamEvent.CreateInterval<string>(1, 5, null),
                StreamEvent.CreateInterval<string>(1, 3, null),
                StreamEvent.CreateInterval<string>(2, 3, null),
                StreamEvent.CreateInterval<string>(2, 4, null),
                StreamEvent.CreateInterval<string>(3, 4, null),
                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            }.ToStreamable();

            var output = input.CountNotNull(v => v);

            var correct = new[]
            {
                StreamEvent.CreateStart<ulong>(1, 0),
                StreamEvent.CreateEnd<ulong>(2, 1, 0),

                StreamEvent.CreateStart<ulong>(2, 0),
                StreamEvent.CreateEnd<ulong>(3, 2, 0),

                StreamEvent.CreateStart<ulong>(3, 0),
                StreamEvent.CreateEnd<ulong>(4, 3, 0),

                StreamEvent.CreateStart<ulong>(4, 0),
                StreamEvent.CreateEnd<ulong>(5, 4, 0),

                StreamEvent.CreatePunctuation<ulong>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }


    }
}
