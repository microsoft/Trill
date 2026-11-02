// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public sealed class PivotDifferenceInput
    {
        public string Key;
        public int Value;
    }

    public sealed class PivotDifferenceSums
    {
        public string Key;
        public int Odd;
        public int Even;
    }

    public sealed class PivotDifferenceAverages
    {
        public string Key;
        public double Odd;
    }

    public abstract class PivotDifferenceTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        internal PivotDifferenceTestsBase(ConfigModifier modifier) : base(modifier) { }

        // When an interval ends while another event of its group is alive, the aggregate subtracts
        // the ended event's state with Difference. At 6, a:1 ends and a:3 is still alive.
        [TestMethod, TestCategory("Gated")]
        public void PivotSumWhenAnIntervalEnds()
        {
            var result = Input(Interval(1, 6, 1), Interval(3, 8, 3))
                .Pivot(() => new PivotDifferenceSums(), e => e.Key, e => e.Value % 2 == 0 ? "Even" : "Odd", e => e.Value, w => w.Sum(v => v));

            CollectionAssert.AreEqual(new[] { "1:1/0", "3:4/0", "6:3/0" }, Starts(result, p => p.Odd + "/" + p.Even));
        }

        [TestMethod, TestCategory("Gated")]
        public void PivotAverageWhenAnIntervalEnds()
        {
            var result = Input(Interval(1, 6, 1), Interval(3, 8, 3))
                .Pivot(() => new PivotDifferenceAverages(), e => e.Key, e => "Odd", e => e.Value, w => w.Average(v => v));

            CollectionAssert.AreEqual(new[] { "1:1", "3:2", "6:3" }, Starts(result, p => p.Odd.ToString(System.Globalization.CultureInfo.InvariantCulture)));
        }

        // a:5 starts at the moment a:1 ends, so one output covers both changes.
        [TestMethod, TestCategory("Gated")]
        public void PivotSumWhenAnIntervalEndsAsAnotherStarts()
        {
            var result = Input(Interval(1, 6, 1), Interval(3, 8, 3), Interval(6, 8, 5))
                .Pivot(() => new PivotDifferenceSums(), e => e.Key, e => e.Value % 2 == 0 ? "Even" : "Odd", e => e.Value, w => w.Sum(v => v));

            CollectionAssert.AreEqual(new[] { "1:1/0", "3:4/0", "6:8/0" }, Starts(result, p => p.Odd + "/" + p.Even));
        }

        private static IStreamable<Empty, PivotDifferenceInput> Input(params StreamEvent<PivotDifferenceInput>[] events)
            => events.ToObservable().ToStreamable();

        private static StreamEvent<PivotDifferenceInput> Interval(long start, long end, int value)
            => StreamEvent.CreateInterval(start, end, new PivotDifferenceInput { Key = "a", Value = value });

        private static List<string> Starts<T>(IStreamable<Empty, T> result, System.Func<T, string> show)
            => result
                .ToStreamEventObservable()
                .Where(e => e.IsStart)
                .Select(e => e.StartTime + ":" + show(e.Payload))
                .ToEnumerable()
                .ToList();
    }

    [TestClass]
    public class PivotDifferenceTestsRow : PivotDifferenceTestsBase
    {
        public PivotDifferenceTestsRow()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }
    }

    [TestClass]
    public class PivotDifferenceTestsColumnar : PivotDifferenceTestsBase
    {
        public PivotDifferenceTestsColumnar()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(true)) { }
    }
}
