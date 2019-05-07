// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class CompoundAggregate : TestWithConfigSettingsAndMemoryLeakDetection
    {
        [TestMethod, TestCategory("Gated")]
        public void TestCompoundAggregate()
        {
            /*
             * Time:     1   2   3   4   5
             * Input: 1  |---------------|
             *        2  |-------|
             *        4      |---|
             *        8      |-------|
             *        16         |---|
             * Sum:      3   15  25  1   0
             * Count:    2   4   3   1   0
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

            var output = input.Aggregate(a => a.Sum(v => v), b => b.Count(), (a, b) => new SumCount { Sum = a, Count = b });

            // Also test max supported number of aggs (15).
            var output2 = input.Aggregate(
                a => a.Sum(v => v),
                b => b.Count(),
                b => b.Count(),
                b => b.Count(),
                b => b.Count(),
                b => b.Count(),
                b => b.Count(),
                b => b.Count(),
                b => b.Count(),
                b => b.Count(),
                b => b.Count(),
                b => b.Count(),
                b => b.Count(),
                b => b.Count(),
                b => b.Count(),
                (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o) =>
                    new SumCount { Sum = a, Count = (b + c + d + e + f + g + h + i + j + k + l + m + n + o) / 14 });

            var correct = new[]
            {
                StreamEvent.CreateStart(1, new SumCount { Sum = 3, Count = 2 }),
                StreamEvent.CreateEnd(2, 1, new SumCount { Sum = 3, Count = 2 }),

                StreamEvent.CreateStart(2, new SumCount { Sum = 15, Count = 4 }),
                StreamEvent.CreateEnd(3, 2, new SumCount { Sum = 15, Count = 4 }),

                StreamEvent.CreateStart(3, new SumCount { Sum = 25, Count = 3 }),
                StreamEvent.CreateEnd(4, 3, new SumCount { Sum = 25, Count = 3 }),

                StreamEvent.CreateStart(4, new SumCount { Sum = 1, Count = 1 }),
                StreamEvent.CreateEnd(5, 4, new SumCount { Sum = 1, Count = 1 }),

                StreamEvent.CreatePunctuation<SumCount>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
            Assert.IsTrue(output2.IsEquivalentTo(correct));
        }

        private struct SumCount : IComparable<SumCount>, IEquatable<SumCount>
        {
            public long Sum;

            public ulong Count;

            public int CompareTo(SumCount other)
            {
                if (this.Sum < other.Sum)
                {
                    return -1;
                }

                if (this.Sum > other.Sum)
                {
                    return 1;
                }

                if (this.Count < other.Count)
                {
                    return -1;
                }

                if (this.Count > other.Count)
                {
                    return 1;
                }

                return 0;
            }

            public bool Equals(SumCount other) => this.Sum == other.Sum && this.Count == other.Count;

            public override string ToString() => "[Sum=" + this.Sum + ", Count=" + this.Count + "]";
        }
    }
}
