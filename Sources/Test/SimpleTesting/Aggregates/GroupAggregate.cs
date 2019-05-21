// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class GroupAggregate : TestWithConfigSettingsAndMemoryLeakDetection
    {
        [TestMethod, TestCategory("Gated")]
        public void TestGroupAggregate()
        {
            var input = new[]
            {
                StreamEvent.CreateStart(0, 100),
                StreamEvent.CreateStart(0, 105),
                StreamEvent.CreateStart(0, 104),
                StreamEvent.CreateStart(0, 200),
                StreamEvent.CreateStart(0, 201),
                StreamEvent.CreateStart(0, 300),
                StreamEvent.CreateStart(0, 302),
                StreamEvent.CreateStart(0, 303),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            }.ToStreamable();

            var output = input.GroupAggregate(
                a => a / 100,
                b => b.Count(),
                c => c.Sum(v => v % 100),
                (key, count, sum) => key.Key * 100 + (int)count * 10 + sum);

            // Also test max supported number of aggs (15).
            var output2 = input.GroupAggregate(
                a => a / 100,
                b => b.Count(),
                c => c.Sum(v => v % 100),
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
                (key, c, sum, d, e, f, g, h, i, j, k, l, m, n, o, p) =>
                    key.Key * 100 + (int)((c + d + e + f + g + h + i + j + k + l + m + n + o) / 2) * 10 + sum);

            var correct = new[]
            {
                StreamEvent.CreateStart(0, 139),
                StreamEvent.CreateStart(0, 221),
                StreamEvent.CreateStart(0, 335),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }
    }
}
