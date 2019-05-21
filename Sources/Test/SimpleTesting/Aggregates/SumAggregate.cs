// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class SumAggregate : TestWithConfigSettingsAndMemoryLeakDetection
    {
        [TestMethod, TestCategory("Gated")]
        public void TestSumAggregate_INT()
        {
            /*
             * Time:     1   2   3   4   5
             * Input: 1  |---------------|
             *        2  |-------|
             *        4      |---|
             *        8      |-------|
             *        16         |---|
             * Output:   3   15  25  1   0
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

            var output = input.Sum(v => v);

            var correct = new[]
            {
                StreamEvent.CreateStart(1, 3),
                StreamEvent.CreateEnd(2, 1, 3),

                StreamEvent.CreateStart(2, 15),
                StreamEvent.CreateEnd(3, 2, 15),

                StreamEvent.CreateStart(3, 25),
                StreamEvent.CreateEnd(4, 3, 25),

                StreamEvent.CreateStart(4, 1),
                StreamEvent.CreateEnd(5, 4, 1),

                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void TestSumAggregate_Long()
        {
            /*
             * Time:     1   2   3   4   5
             * Input: 1  |---------------|
             *        2  |-------|
             *        4      |---|
             *        8      |-------|
             *        16         |---|
             * Output:   3   15  25  1   0
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

            var output = input.Sum(v => v);

            var correct = new[]
            {
                StreamEvent.CreateStart<long>(1, 3),
                StreamEvent.CreateEnd<long>(2, 1, 3),

                StreamEvent.CreateStart<long>(2, 15),
                StreamEvent.CreateEnd<long>(3, 2, 15),

                StreamEvent.CreateStart<long>(3, 25),
                StreamEvent.CreateEnd<long>(4, 3, 25),

                StreamEvent.CreateStart<long>(4, 1),
                StreamEvent.CreateEnd<long>(5, 4, 1),

                StreamEvent.CreatePunctuation<long>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void TestSumAggregate_Short()
        {
            /*
             * Time:     1   2   3   4   5
             * Input: 1  |---------------|
             *        2  |-------|
             *        4      |---|
             *        8      |-------|
             *        16         |---|
             * Output:   3   15  25  1   0
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

            var output = input.Sum(v => v);

            var correct = new[]
            {
                StreamEvent.CreateStart<short>(1, 3),
                StreamEvent.CreateEnd<short>(2, 1, 3),

                StreamEvent.CreateStart<short>(2, 15),
                StreamEvent.CreateEnd<short>(3, 2, 15),

                StreamEvent.CreateStart<short>(3, 25),
                StreamEvent.CreateEnd<short>(4, 3, 25),

                StreamEvent.CreateStart<short>(4, 1),
                StreamEvent.CreateEnd<short>(5, 4, 1),

                StreamEvent.CreatePunctuation<short>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void TestSumAggregate_UInt()
        {
            /*
             * Time:     1   2   3   4   5
             * Input: 1  |---------------|
             *        2  |-------|
             *        4      |---|
             *        8      |-------|
             *        16         |---|
             * Output:   3   15  25  1   0
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

            var output = input.Sum(v => v);

            var correct = new[]
            {
                StreamEvent.CreateStart<uint>(1, 3),
                StreamEvent.CreateEnd<uint>(2, 1, 3),

                StreamEvent.CreateStart<uint>(2, 15),
                StreamEvent.CreateEnd<uint>(3, 2, 15),

                StreamEvent.CreateStart<uint>(3, 25),
                StreamEvent.CreateEnd<uint>(4, 3, 25),

                StreamEvent.CreateStart<uint>(4, 1),
                StreamEvent.CreateEnd<uint>(5, 4, 1),

                StreamEvent.CreatePunctuation<uint>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void TestSumAggregate_ULong()
        {
            /*
             * Time:     1   2   3   4   5
             * Input: 1  |---------------|
             *        2  |-------|
             *        4      |---|
             *        8      |-------|
             *        16         |---|
             * Output:   3   15  25  1   0
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

            var output = input.Sum(v => v);

            var correct = new[]
            {
                StreamEvent.CreateStart<ulong>(1, 3),
                StreamEvent.CreateEnd<ulong>(2, 1, 3),

                StreamEvent.CreateStart<ulong>(2, 15),
                StreamEvent.CreateEnd<ulong>(3, 2, 15),

                StreamEvent.CreateStart<ulong>(3, 25),
                StreamEvent.CreateEnd<ulong>(4, 3, 25),

                StreamEvent.CreateStart<ulong>(4, 1),
                StreamEvent.CreateEnd<ulong>(5, 4, 1),

                StreamEvent.CreatePunctuation<ulong>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void TestSumAggregate_UShort()
        {
            /*
             * Time:     1   2   3   4   5
             * Input: 1  |---------------|
             *        2  |-------|
             *        4      |---|
             *        8      |-------|
             *        16         |---|
             * Output:   3   15  25  1   0
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

            var output = input.Sum(v => v);

            var correct = new[]
            {
                StreamEvent.CreateStart<ushort>(1, 3),
                StreamEvent.CreateEnd<ushort>(2, 1, 3),

                StreamEvent.CreateStart<ushort>(2, 15),
                StreamEvent.CreateEnd<ushort>(3, 2, 15),

                StreamEvent.CreateStart<ushort>(3, 25),
                StreamEvent.CreateEnd<ushort>(4, 3, 25),

                StreamEvent.CreateStart<ushort>(4, 1),
                StreamEvent.CreateEnd<ushort>(5, 4, 1),

                StreamEvent.CreatePunctuation<ushort>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void TestSumAggregate_Decimal()
        {
            /*
             * Time:     1   2   3   4   5
             * Input: 1  |---------------|
             *        2  |-------|
             *        4      |---|
             *        8      |-------|
             *        16         |---|
             * Output:   3   15  25  1   0
             */
            var input = new[]
            {
                StreamEvent.CreateInterval(1, 5, 1.5m),
                StreamEvent.CreateInterval(1, 3, 2m),
                StreamEvent.CreateInterval(2, 3, 3.6m),
                StreamEvent.CreateInterval(2, 4, 8.2m),
                StreamEvent.CreateInterval(3, 4, 16m),
                StreamEvent.CreatePunctuation<decimal>(StreamEvent.InfinitySyncTime)
            }.ToStreamable();

            var output = input.Sum(v => v);

            var correct = new[]
            {
                StreamEvent.CreateStart(1, 3.5m),
                StreamEvent.CreateEnd(2, 1, 3.5m),

                StreamEvent.CreateStart(2, 15.3m),
                StreamEvent.CreateEnd(3, 2, 15.3m),

                StreamEvent.CreateStart(3, 25.7m),
                StreamEvent.CreateEnd(4, 3, 25.7m),

                StreamEvent.CreateStart(4, 1.5m),
                StreamEvent.CreateEnd(5, 4, 1.5m),

                StreamEvent.CreatePunctuation<decimal>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void TestSumAggregate_Double()
        {
            /*
             * Time:     1   2   3   4   5
             * Input: 1  |---------------|
             *        2  |-------|
             *        4      |---|
             *        8      |-------|
             *        16         |---|
             * Output:   3   15  25  1   0
             */
            var input = new[]
            {
                StreamEvent.CreateInterval(1, 5, 1.0),
                StreamEvent.CreateInterval(1, 3, 2.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime)
            }.ToStreamable();

            var output = input.Sum(v => v);

            var correct = new[]
            {
                StreamEvent.CreateStart(1, 3.0),
                StreamEvent.CreateEnd(3, 1, 3.0),
                StreamEvent.CreateStart(3, 1.0),
                StreamEvent.CreateEnd(5, 3, 1.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void TestSumAggregate_Float()
        {
            var input = new[]
            {
                StreamEvent.CreateInterval(1, 5, 1.0f),
                StreamEvent.CreateInterval(1, 3, 2.0f),
                StreamEvent.CreatePunctuation<float>(StreamEvent.InfinitySyncTime)
            }.ToStreamable();

            var output = input.Sum(v => v);

            var correct = new[]
            {
                StreamEvent.CreateStart(1, 3.0f),
                StreamEvent.CreateEnd(3, 1, 3.0f),
                StreamEvent.CreateStart(3, 1.0f),
                StreamEvent.CreateEnd(5, 3, 1.0f),
                StreamEvent.CreatePunctuation<float>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void TestSumAggregate_Byte()
        {
            /*
             * Time:     1   2   3   4   5
             * Input: 1  |---------------|
             *        2  |-------|
             *        4      |---|
             *        8      |-------|
             *        16         |---|
             * Output:   3   15  25  1   0
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

            var output = input.Sum(v => v);

            var correct = new[]
            {
                StreamEvent.CreateStart<byte>(1, 3),
                StreamEvent.CreateEnd<byte>(2, 1, 3),

                StreamEvent.CreateStart<byte>(2, 15),
                StreamEvent.CreateEnd<byte>(3, 2, 15),

                StreamEvent.CreateStart<byte>(3, 25),
                StreamEvent.CreateEnd<byte>(4, 3, 25),

                StreamEvent.CreateStart<byte>(4, 1),
                StreamEvent.CreateEnd<byte>(5, 4, 1),

                StreamEvent.CreatePunctuation<byte>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void TestSumAggregate_SByte()
        {
            /*
             * Time:     1   2   3   4   5
             * Input: 1  |---------------|
             *        2  |-------|
             *        4      |---|
             *        8      |-------|
             *        16         |---|
             * Output:   3   15  25  1   0
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

            var output = input.Sum(v => v);

            var correct = new[]
            {
                StreamEvent.CreateStart<sbyte>(1, 3),
                StreamEvent.CreateEnd<sbyte>(2, 1, 3),

                StreamEvent.CreateStart<sbyte>(2, 15),
                StreamEvent.CreateEnd<sbyte>(3, 2, 15),

                StreamEvent.CreateStart<sbyte>(3, 25),
                StreamEvent.CreateEnd<sbyte>(4, 3, 25),

                StreamEvent.CreateStart<sbyte>(4, 1),
                StreamEvent.CreateEnd<sbyte>(5, 4, 1),

                StreamEvent.CreatePunctuation<sbyte>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

    }
}
