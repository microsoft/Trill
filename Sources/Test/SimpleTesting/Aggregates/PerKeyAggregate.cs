// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [SuppressMessage("StyleCop.CSharp.SpacingRules", "SA1009", Justification = "Reviewed.")]
    [TestClass]
    public class AggregateByKey : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public AggregateByKey()
            : base(new ConfigModifier()
                        .ForceRowBasedExecution(false)
                        .DontFallBackToRowBasedExecution(true)
                        .UseMultiString(false)
                        .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.None))
       { }

        [TestMethod, TestCategory("Gated")]
        public void TestAggregateByKey()
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
            }.ToStreamable().SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime);

            var output = input
                .GroupAggregate(
                a => a / 100,
                b => b.Count(),
                (key, count) => key.Key * 100 + (int)count * 10);

            var correct = new[]
            {
                StreamEvent.CreateStart(0, 130),
                StreamEvent.CreateStart(0, 220),
                StreamEvent.CreateStart(0, 330),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void TestAggregateByKey2()
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
            }.ToStreamable().SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime);

            var output = input
                .Select(e => new StructTuple<long, long> { Item1 = 0, Item2 = e })
                .GroupAggregate(
                a => (int)a.Item2 / 100,
                b => b.Sum(s => (int)(s.Item2 & 0xffff)),
                (key, sum) => new StructTuple<int, ulong> { Item1 = key.Key, Item2 = (ulong)key.Key * 100 + (ulong)sum * 10 })
                .Select(e => (int)e.Item2);

            var correct = new[]
            {
                StreamEvent.CreateStart(0, 3190),
                StreamEvent.CreateStart(0, 4210),
                StreamEvent.CreateStart(0, 9350),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void TestAggregateByKey3()
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
            }.ToStreamable().SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime);

            var output = input
                .Select(e => new StructTuple<long, long> { Item1 = 0, Item2 = e })
                .GroupAggregate(
                a => (int)a.Item2 / 100,
                b => b.Sum(s => (int)(s.Item2 & 0xffff)),
                c => c.Count(),
                (key, sum, count) => new StructTuple<int, ulong, ulong> { Item1 = key.Key, Item2 = (ulong)key.Key * 100 + (ulong)sum * 10, Item3 = (ulong)key.Key * 100 + (ulong)count * 34 * 0 })
                .Select(e => (int)e.Item2);

            var correct = new[]
            {
                StreamEvent.CreateStart(0, 3190),
                StreamEvent.CreateStart(0, 4210),
                StreamEvent.CreateStart(0, 9350),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo(correct));
        }

        [TestMethod, TestCategory("Gated")]
        public void TestAggregateByKeyTupleDecomposition()
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

            var output = new List<StreamEvent<(bool, ulong)>>();
            input
                .GroupAggregate(s => true, w => w.Count(), (g, c) => ValueTuple.Create(g.Key, c))
                .ToStreamEventObservable()
                .ForEachAsync(e => output.Add(e))
                .Wait();

            var correct = new[]
            {
                StreamEvent.CreateStart(0, ValueTuple.Create(true, (ulong)8)),
                StreamEvent.CreatePunctuation<ValueTuple<bool, ulong>>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(correct.SequenceEqual(output));
        }
    }
}
