// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public sealed class ReduceWithoutArgumentsInput
    {
        public string Key;
        public int Value;
    }

    public abstract class ReduceWithoutArgumentsTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private static readonly string[] Unmapped = { "1-6:a:1", "3-8:b:2", "4-9:a:3" };

        internal ReduceWithoutArgumentsTestsBase(ConfigModifier modifier) : base(modifier) { }

        // Reduce() with no arguments passes the mapper's output through, with or without a key selector.
        [TestMethod, TestCategory("Gated")]
        public void ReduceAfterAMapWithoutAKeySelector()
            => CollectionAssert.AreEqual(Unmapped, Events(Input().Map((IStreamable<Empty, ReduceWithoutArgumentsInput> s) => s).Reduce()));

        [TestMethod, TestCategory("Gated")]
        public void ReduceAfterAMapWithAKeySelector()
            => CollectionAssert.AreEqual(Unmapped, Events(Input().Map(s => s, e => e.Key).Reduce()));

        [TestMethod, TestCategory("Gated")]
        public void ReduceAfterAGroupBy()
            => CollectionAssert.AreEqual(Unmapped, Events(Input().GroupBy(e => e.Key).Reduce()));

        // The events passed through are the mapper's output, not its input.
        [TestMethod, TestCategory("Gated")]
        public void ReduceAfterAKeyedMapThatTransforms()
        {
            var result = Input()
                .Map(s => s.Select(e => new ReduceWithoutArgumentsInput { Key = e.Key, Value = e.Value * 10 }), e => e.Key)
                .Reduce();

            CollectionAssert.AreEqual(new[] { "1-6:a:10", "3-8:b:20", "4-9:a:30" }, Events(result));
        }

        private static IStreamable<Empty, ReduceWithoutArgumentsInput> Input()
            => new[]
            {
                StreamEvent.CreateInterval(1, 6, new ReduceWithoutArgumentsInput { Key = "a", Value = 1 }),
                StreamEvent.CreateInterval(3, 8, new ReduceWithoutArgumentsInput { Key = "b", Value = 2 }),
                StreamEvent.CreateInterval(4, 9, new ReduceWithoutArgumentsInput { Key = "a", Value = 3 }),
            }.ToObservable().ToStreamable();

        // Each data event as start-end:key:value, in ordinal order, so the merge order of the groups does not matter.
        private static List<string> Events(IStreamable<Empty, ReduceWithoutArgumentsInput> result)
            => result
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.StartTime + "-" + e.EndTime + ":" + e.Payload.Key + ":" + e.Payload.Value)
                .ToEnumerable()
                .OrderBy(s => s, StringComparer.Ordinal)
                .ToList();
    }

    [TestClass]
    public class ReduceWithoutArgumentsTestsRow : ReduceWithoutArgumentsTestsBase
    {
        public ReduceWithoutArgumentsTestsRow()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }
    }

    [TestClass]
    public class ReduceWithoutArgumentsTestsColumnar : ReduceWithoutArgumentsTestsBase
    {
        public ReduceWithoutArgumentsTestsColumnar()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(true)) { }
    }
}
