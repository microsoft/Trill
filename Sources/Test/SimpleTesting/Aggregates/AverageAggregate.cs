// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class AverageAggregate : TestWithConfigSettingsAndMemoryLeakDetection
    {
        [TestMethod, TestCategory("Gated")]
        public void TestAverageAggregate_Int()
        {
            /*
             * Time:     1   2   3   4   5
             * Input: 1  |---------------|
             *        2  |-------|
             *        4      |---|
             *        8      |-------|
             *        16         |---|
             * Output:   3   15  25  1
             *           -   --  --  -
             *           2   4   3   1
             */
            var input = new[]
            {
                StreamEvent.CreateInterval(1, 5, 1),
                StreamEvent.CreateInterval(1, 3, 2),
                StreamEvent.CreateInterval(2, 3, 4),
                StreamEvent.CreateInterval(2, 4, 8),
                StreamEvent.CreateInterval(3, 4, 16),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            }.ToStreamable();

            var output = input.Average(v => v);

            var correct = new[]
            {
                StreamEvent.CreateStart(1, 3.0 / 2),
                StreamEvent.CreateEnd(2, 1, 3.0 / 2),

                StreamEvent.CreateStart(2, 15.0 / 4),
                StreamEvent.CreateEnd(3, 2, 15.0 / 4),

                StreamEvent.CreateStart(3, 25.0 / 3),
                StreamEvent.CreateEnd(4, 3, 25.0 / 3),

                StreamEvent.CreateStart(4, 1.0),
                StreamEvent.CreateEnd(5, 4, 1.0),

                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void TestAverageAggregate_Double()
        {
            /*
             * Time:     1   2   3   4   5
             * Input: 1  |---------------|
             *        2  |-------|
             *        4      |---|
             *        8      |-------|
             *        16         |---|
             * Output:   3   15  25  1
             *           -   --  --  -
             *           2   4   3   1
             */
            var input = new[]
            {
                StreamEvent.CreateInterval(1, 5, 1d),
                StreamEvent.CreateInterval(1, 3, 2d),
                StreamEvent.CreateInterval(2, 3, 4d),
                StreamEvent.CreateInterval(2, 4, 8d),
                StreamEvent.CreateInterval(3, 4, 16d),
                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime)
            }.ToStreamable();

            var output = input.Average(v => v);

            var correct = new[]
            {
                StreamEvent.CreateStart(1, 3.0 / 2),
                StreamEvent.CreateEnd(2, 1, 3.0 / 2),

                StreamEvent.CreateStart(2, 15.0 / 4),
                StreamEvent.CreateEnd(3, 2, 15.0 / 4),

                StreamEvent.CreateStart(3, 25.0 / 3),
                StreamEvent.CreateEnd(4, 3, 25.0 / 3),

                StreamEvent.CreateStart(4, 1.0),
                StreamEvent.CreateEnd(5, 4, 1.0),

                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void TestAverageAggregate_Long()
        {
            /*
             * Time:     1   2   3   4   5
             * Input: 1  |---------------|
             *        2  |-------|
             *        4      |---|
             *        8      |-------|
             *        16         |---|
             * Output:   3   15  25  1
             *           -   --  --  -
             *           2   4   3   1
             */
            var input = new[]
            {
                StreamEvent.CreateInterval<long>(1, 5, 1),
                StreamEvent.CreateInterval<long>(1, 3, 2),
                StreamEvent.CreateInterval<long>(2, 3, 4),
                StreamEvent.CreateInterval<long>(2, 4, 8),
                StreamEvent.CreateInterval<long>(3, 4, 16),
                StreamEvent.CreatePunctuation<long>(StreamEvent.InfinitySyncTime)
            }.ToStreamable();

            var output = input.Average(v => v);

            var correct = new[]
            {
                StreamEvent.CreateStart(1, 3.0 / 2),
                StreamEvent.CreateEnd(2, 1, 3.0 / 2),

                StreamEvent.CreateStart(2, 15.0 / 4),
                StreamEvent.CreateEnd(3, 2, 15.0 / 4),

                StreamEvent.CreateStart(3, 25.0 / 3),
                StreamEvent.CreateEnd(4, 3, 25.0 / 3),

                StreamEvent.CreateStart(4, 1.0),
                StreamEvent.CreateEnd(5, 4, 1.0),

                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void TestAverageAggregate_Short()
        {
            /*
             * Time:     1   2   3   4   5
             * Input: 1  |---------------|
             *        2  |-------|
             *        4      |---|
             *        8      |-------|
             *        16         |---|
             * Output:   3   15  25  1
             *           -   --  --  -
             *           2   4   3   1
             */
            var input = new[]
            {
                StreamEvent.CreateInterval<short>(1, 5, 1),
                StreamEvent.CreateInterval<short>(1, 3, 2),
                StreamEvent.CreateInterval<short>(2, 3, 4),
                StreamEvent.CreateInterval<short>(2, 4, 8),
                StreamEvent.CreateInterval<short>(3, 4, 16),
                StreamEvent.CreatePunctuation<short>(StreamEvent.InfinitySyncTime)
            }.ToStreamable();

            var output = input.Average(v => v);

            var correct = new[]
            {
                StreamEvent.CreateStart(1, 3.0 / 2),
                StreamEvent.CreateEnd(2, 1, 3.0 / 2),

                StreamEvent.CreateStart(2, 15.0 / 4),
                StreamEvent.CreateEnd(3, 2, 15.0 / 4),

                StreamEvent.CreateStart(3, 25.0 / 3),
                StreamEvent.CreateEnd(4, 3, 25.0 / 3),

                StreamEvent.CreateStart(4, 1.0),
                StreamEvent.CreateEnd(5, 4, 1.0),

                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void TestAverageAggregate_UInt()
        {
            /*
             * Time:     1   2   3   4   5
             * Input: 1  |---------------|
             *        2  |-------|
             *        4      |---|
             *        8      |-------|
             *        16         |---|
             * Output:   3   15  25  1
             *           -   --  --  -
             *           2   4   3   1
             */
            var input = new[]
            {
                StreamEvent.CreateInterval<uint>(1, 5, 1),
                StreamEvent.CreateInterval<uint>(1, 3, 2),
                StreamEvent.CreateInterval<uint>(2, 3, 4),
                StreamEvent.CreateInterval<uint>(2, 4, 8),
                StreamEvent.CreateInterval<uint>(3, 4, 16),
                StreamEvent.CreatePunctuation<uint>(StreamEvent.InfinitySyncTime)
            }.ToStreamable();

            var output = input.Average(v => v);

            var correct = new[]
            {
                StreamEvent.CreateStart(1, 3.0 / 2),
                StreamEvent.CreateEnd(2, 1, 3.0 / 2),

                StreamEvent.CreateStart(2, 15.0 / 4),
                StreamEvent.CreateEnd(3, 2, 15.0 / 4),

                StreamEvent.CreateStart(3, 25.0 / 3),
                StreamEvent.CreateEnd(4, 3, 25.0 / 3),

                StreamEvent.CreateStart(4, 1.0),
                StreamEvent.CreateEnd(5, 4, 1.0),

                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void TestAverageAggregate_ULong()
        {
            /*
             * Time:     1   2   3   4   5
             * Input: 1  |---------------|
             *        2  |-------|
             *        4      |---|
             *        8      |-------|
             *        16         |---|
             * Output:   3   15  25  1
             *           -   --  --  -
             *           2   4   3   1
             */
            var input = new[]
            {
                StreamEvent.CreateInterval<ulong>(1, 5, 1),
                StreamEvent.CreateInterval<ulong>(1, 3, 2),
                StreamEvent.CreateInterval<ulong>(2, 3, 4),
                StreamEvent.CreateInterval<ulong>(2, 4, 8),
                StreamEvent.CreateInterval<ulong>(3, 4, 16),
                StreamEvent.CreatePunctuation<ulong>(StreamEvent.InfinitySyncTime)
            }.ToStreamable();

            var output = input.Average(v => v);

            var correct = new[]
            {
                StreamEvent.CreateStart(1, 3.0 / 2),
                StreamEvent.CreateEnd(2, 1, 3.0 / 2),

                StreamEvent.CreateStart(2, 15.0 / 4),
                StreamEvent.CreateEnd(3, 2, 15.0 / 4),

                StreamEvent.CreateStart(3, 25.0 / 3),
                StreamEvent.CreateEnd(4, 3, 25.0 / 3),

                StreamEvent.CreateStart(4, 1.0),
                StreamEvent.CreateEnd(5, 4, 1.0),

                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void TestAverageAggregate_UShort()
        {
            /*
             * Time:     1   2   3   4   5
             * Input: 1  |---------------|
             *        2  |-------|
             *        4      |---|
             *        8      |-------|
             *        16         |---|
             * Output:   3   15  25  1
             *           -   --  --  -
             *           2   4   3   1
             */
            var input = new[]
            {
                StreamEvent.CreateInterval<ushort>(1, 5, 1),
                StreamEvent.CreateInterval<ushort>(1, 3, 2),
                StreamEvent.CreateInterval<ushort>(2, 3, 4),
                StreamEvent.CreateInterval<ushort>(2, 4, 8),
                StreamEvent.CreateInterval<ushort>(3, 4, 16),
                StreamEvent.CreatePunctuation<ushort>(StreamEvent.InfinitySyncTime)
            }.ToStreamable();

            var output = input.Average(v => v);

            var correct = new[]
            {
                StreamEvent.CreateStart(1, 3.0 / 2),
                StreamEvent.CreateEnd(2, 1, 3.0 / 2),

                StreamEvent.CreateStart(2, 15.0 / 4),
                StreamEvent.CreateEnd(3, 2, 15.0 / 4),

                StreamEvent.CreateStart(3, 25.0 / 3),
                StreamEvent.CreateEnd(4, 3, 25.0 / 3),

                StreamEvent.CreateStart(4, 1.0),
                StreamEvent.CreateEnd(5, 4, 1.0),

                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void TestAverageAggregate_Byte()
        {
            /*
             * Time:     1   2   3   4   5
             * Input: 1  |---------------|
             *        2  |-------|
             *        4      |---|
             *        8      |-------|
             *        16         |---|
             * Output:   3   15  25  1
             *           -   --  --  -
             *           2   4   3   1
             */
            var input = new[]
            {
                StreamEvent.CreateInterval<byte>(1, 5, 1),
                StreamEvent.CreateInterval<byte>(1, 3, 2),
                StreamEvent.CreateInterval<byte>(2, 3, 4),
                StreamEvent.CreateInterval<byte>(2, 4, 8),
                StreamEvent.CreateInterval<byte>(3, 4, 16),
                StreamEvent.CreatePunctuation<byte>(StreamEvent.InfinitySyncTime)
            }.ToStreamable();

            var output = input.Average(v => v);

            var correct = new[]
            {
                StreamEvent.CreateStart(1, 3.0 / 2),
                StreamEvent.CreateEnd(2, 1, 3.0 / 2),

                StreamEvent.CreateStart(2, 15.0 / 4),
                StreamEvent.CreateEnd(3, 2, 15.0 / 4),

                StreamEvent.CreateStart(3, 25.0 / 3),
                StreamEvent.CreateEnd(4, 3, 25.0 / 3),

                StreamEvent.CreateStart(4, 1.0),
                StreamEvent.CreateEnd(5, 4, 1.0),

                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void TestAverageAggregate_SByte()
        {
            /*
             * Time:     1   2   3   4   5
             * Input: 1  |---------------|
             *        2  |-------|
             *        4      |---|
             *        8      |-------|
             *        16         |---|
             * Output:   3   15  25  1
             *           -   --  --  -
             *           2   4   3   1
             */
            var input = new[]
            {
                StreamEvent.CreateInterval<sbyte>(1, 5, 1),
                StreamEvent.CreateInterval<sbyte>(1, 3, 2),
                StreamEvent.CreateInterval<sbyte>(2, 3, 4),
                StreamEvent.CreateInterval<sbyte>(2, 4, 8),
                StreamEvent.CreateInterval<sbyte>(3, 4, 16),
                StreamEvent.CreatePunctuation<sbyte>(StreamEvent.InfinitySyncTime)
            }.ToStreamable();

            var output = input.Average(v => v);

            var correct = new[]
            {
                StreamEvent.CreateStart(1, 3.0 / 2),
                StreamEvent.CreateEnd(2, 1, 3.0 / 2),

                StreamEvent.CreateStart(2, 15.0 / 4),
                StreamEvent.CreateEnd(3, 2, 15.0 / 4),

                StreamEvent.CreateStart(3, 25.0 / 3),
                StreamEvent.CreateEnd(4, 3, 25.0 / 3),

                StreamEvent.CreateStart(4, 1.0),
                StreamEvent.CreateEnd(5, 4, 1.0),

                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

    }
}
