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

namespace SimpleTesting.DisorderedIngressAndEgress
{
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowDrop105 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowDrop105() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowDrop105()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i >= (latest - 5);
                latest = Math.Max(i, latest);
                return ret;
            }).Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowDrop105()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i.start >= (latest - 5);
                latest = Math.Max(i.start, latest);
                return ret;
            }).OrderBy(i => i.start).Select(i => i.start).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected)); // Not really checking for each interval but thats ok

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowDrop105Diagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowDrop105Diagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowDrop105Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i >= (latest - 5);
                latest = Math.Max(i, latest);
                return ret;
            }).Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 5);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowDrop105Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i.start >= (latest - 5);
                latest = Math.Max(i.start, latest);
                return ret;
            }).OrderBy(i => i.start).Select(i => i.start).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected)); // Not really checking for each interval but thats ok

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 5);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowDrop1020 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowDrop1020() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowDrop1020()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowDrop1020()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowDrop1020Diagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowDrop1020Diagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowDrop1020Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 20);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowDrop1020Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 20);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowDrop105Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowDrop105Time() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowDrop105Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i >= (latest - 5);
                latest = Math.Max(i, latest);
                return ret;
            }).Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowDrop105Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i.start >= (latest - 5);
                latest = Math.Max(i.start, latest);
                return ret;
            }).OrderBy(i => i.start).Select(i => i.start).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected)); // Not really checking for each interval but thats ok

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowDrop105TimeDiagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowDrop105TimeDiagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowDrop105TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i >= (latest - 5);
                latest = Math.Max(i, latest);
                return ret;
            }).Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 5);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowDrop105TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i.start >= (latest - 5);
                latest = Math.Max(i.start, latest);
                return ret;
            }).OrderBy(i => i.start).Select(i => i.start).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected)); // Not really checking for each interval but thats ok

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 5);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowDrop1020Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowDrop1020Time() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowDrop1020Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowDrop1020Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowDrop1020TimeDiagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowDrop1020TimeDiagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowDrop1020TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 20);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowDrop1020TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 20);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowThrow105 : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowThrow105() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowThrow105()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowThrow105()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowThrow105Diagnostic : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowThrow105Diagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowThrow105Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowThrow105Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowThrow1020 : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowThrow1020() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowThrow1020()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
                Assert.IsTrue(output.SequenceEqual(expected));
    
                Assert.IsTrue(!punctuations.Any());
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowThrow1020()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
                Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));
    
                Assert.IsTrue(!punctuations.Any());
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowThrow1020Diagnostic : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowThrow1020Diagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowThrow1020Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
                Assert.IsTrue(output.SequenceEqual(expected));
    
                {
                    var lt = 0;
                    var expectedOutOfOrder = disorderedInputData.Select(i =>
                        {
                            var outofwindow = i < (lt - 20);
                            lt = Math.Max(i, lt);
                            if (outofwindow)
                                return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), null);
                            return null;
                        }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                    Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
                }
    
                Assert.IsTrue(!punctuations.Any());
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowThrow1020Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
                Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));
    
                {
                    var lt = 0L;
                    var expectedOutOfOrder = disorderedInputData.Select(i =>
                        {
                            var outofwindow = i.start < (lt - 20);
                            lt = Math.Max(i.start, lt);
                            if (outofwindow)
                                return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), null);
                            return null;
                        }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                    Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
                }
    
                Assert.IsTrue(!punctuations.Any());
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowThrow105Time : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowThrow105Time() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowThrow105Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowThrow105Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowThrow105TimeDiagnostic : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowThrow105TimeDiagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowThrow105TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowThrow105TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowThrow1020Time : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowThrow1020Time() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowThrow1020Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
                Assert.IsTrue(output.SequenceEqual(expected));
    
                var highmark = 0;
                var lastPunc = 0;
                var current = 0;
                var queue = new PriorityQueue<int>();
                var expectedPunctuations = disorderedInputData.Select(i =>
                {
                    var outoforder = i < highmark - 20;
                    if (outoforder)
                    {
                        return null;
                    }
                    queue.Enqueue(i);
                    highmark = Math.Max(i, highmark);
                    while (queue.Peek() <= highmark - 20)
                    {
                        current = queue.Dequeue();
                        if (current - lastPunc >= 10)
                        {
                            lastPunc = current;
                            return Tuple.Create(i, current);
                        }
                    }
                    return null;
                }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
                while (queue.Count() > 0)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                    }
                }
                Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowThrow1020Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
                Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));
    
                long highmark = 0L;
                long lastPunc = 0L;
                long current = 0L;
                var queue = new PriorityQueue<long>();
                var expectedPunctuations = disorderedInputData.Select(i =>
                {
                    var outoforder = i.start < highmark - 20;
                    if (outoforder)
                    {
                        return null;
                    }
                    queue.Enqueue(i.start);
                    highmark = Math.Max(i.start, highmark);
                    while (queue.Peek() <= highmark - 20)
                    {
                        current = queue.Dequeue();
                        if (current - lastPunc >= 10)
                        {
                            lastPunc = current;
                            return Tuple.Create(i.start, current);
                        }
                    }
                    return null;
                }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
                while (queue.Count() > 0)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                    }
                }
                Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowThrow1020TimeDiagnostic : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowThrow1020TimeDiagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowThrow1020TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
                Assert.IsTrue(output.SequenceEqual(expected));
    
                {
                    var lt = 0;
                    var expectedOutOfOrder = disorderedInputData.Select(i =>
                        {
                            var outofwindow = i < (lt - 20);
                            lt = Math.Max(i, lt);
                            if (outofwindow)
                                return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), null);
                            return null;
                        }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                    Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
                }
    
                var highmark = 0;
                var lastPunc = 0;
                var current = 0;
                var queue = new PriorityQueue<int>();
                var expectedPunctuations = disorderedInputData.Select(i =>
                {
                    var outoforder = i < highmark - 20;
                    if (outoforder)
                    {
                        return null;
                    }
                    queue.Enqueue(i);
                    highmark = Math.Max(i, highmark);
                    while (queue.Peek() <= highmark - 20)
                    {
                        current = queue.Dequeue();
                        if (current - lastPunc >= 10)
                        {
                            lastPunc = current;
                            return Tuple.Create(i, current);
                        }
                    }
                    return null;
                }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
                while (queue.Count() > 0)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                    }
                }
                Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowThrow1020TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
                Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));
    
                {
                    var lt = 0L;
                    var expectedOutOfOrder = disorderedInputData.Select(i =>
                        {
                            var outofwindow = i.start < (lt - 20);
                            lt = Math.Max(i.start, lt);
                            if (outofwindow)
                                return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), null);
                            return null;
                        }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                    Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
                }
    
                long highmark = 0L;
                long lastPunc = 0L;
                long current = 0L;
                var queue = new PriorityQueue<long>();
                var expectedPunctuations = disorderedInputData.Select(i =>
                {
                    var outoforder = i.start < highmark - 20;
                    if (outoforder)
                    {
                        return null;
                    }
                    queue.Enqueue(i.start);
                    highmark = Math.Max(i.start, highmark);
                    while (queue.Peek() <= highmark - 20)
                    {
                        current = queue.Dequeue();
                        if (current - lastPunc >= 10)
                        {
                            lastPunc = current;
                            return Tuple.Create(i.start, current);
                        }
                    }
                    return null;
                }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
                while (queue.Count() > 0)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                    }
                }
                Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowAdjust105 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowAdjust105() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowAdjust105()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i < (latest - 5);
                latest = Math.Max(i, latest);
                if (outofwindow) return StreamEvent.CreateStart(latest - 5, i); // adjust
                return StreamEvent.CreateStart(i, i);
            }).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowAdjust105()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i.start < (latest - 5);
                var endoutofwindow = i.end < (latest - 5);

                latest = Math.Max(i.start, latest);
                if (endoutofwindow) return null;
                if (outofwindow) return Tuple.Create(latest - 5, i.end, i); // adjust
                return Tuple.Create(i.start, i.end, i);
            }).Where(t => t != null).Select(t => StreamEvent.CreateInterval(t.Item1, t.Item2, t.Item3)).OrderBy(i => i.StartTime).Select(i => i.StartTime).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowAdjust105Diagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowAdjust105Diagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowAdjust105Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i < (latest - 5);
                latest = Math.Max(i, latest);
                if (outofwindow) return StreamEvent.CreateStart(latest - 5, i); // adjust
                return StreamEvent.CreateStart(i, i);
            }).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 5);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), lt - 5 - i);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowAdjust105Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i.start < (latest - 5);
                var endoutofwindow = i.end < (latest - 5);

                latest = Math.Max(i.start, latest);
                if (endoutofwindow) return null;
                if (outofwindow) return Tuple.Create(latest - 5, i.end, i); // adjust
                return Tuple.Create(i.start, i.end, i);
            }).Where(t => t != null).Select(t => StreamEvent.CreateInterval(t.Item1, t.Item2, t.Item3)).OrderBy(i => i.StartTime).Select(i => i.StartTime).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 5);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), lt - 5 - i.start);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowAdjust1020 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowAdjust1020() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowAdjust1020()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowAdjust1020()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowAdjust1020Diagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowAdjust1020Diagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowAdjust1020Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 20);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), lt - 20 - i);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowAdjust1020Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 20);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), lt - 20 - i.start);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowAdjust105Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowAdjust105Time() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowAdjust105Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i < (latest - 5);
                latest = Math.Max(i, latest);
                if (outofwindow) return StreamEvent.CreateStart(latest - 5, i); // adjust
                return StreamEvent.CreateStart(i, i);
            }).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowAdjust105Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i.start < (latest - 5);
                var endoutofwindow = i.end < (latest - 5);

                latest = Math.Max(i.start, latest);
                if (endoutofwindow) return null;
                if (outofwindow) return Tuple.Create(latest - 5, i.end, i); // adjust
                return Tuple.Create(i.start, i.end, i);
            }).Where(t => t != null).Select(t => StreamEvent.CreateInterval(t.Item1, t.Item2, t.Item3)).OrderBy(i => i.StartTime).Select(i => i.StartTime).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowAdjust105TimeDiagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowAdjust105TimeDiagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowAdjust105TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i < (latest - 5);
                latest = Math.Max(i, latest);
                if (outofwindow) return StreamEvent.CreateStart(latest - 5, i); // adjust
                return StreamEvent.CreateStart(i, i);
            }).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 5);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), lt - 5 - i);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowAdjust105TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i.start < (latest - 5);
                var endoutofwindow = i.end < (latest - 5);

                latest = Math.Max(i.start, latest);
                if (endoutofwindow) return null;
                if (outofwindow) return Tuple.Create(latest - 5, i.end, i); // adjust
                return Tuple.Create(i.start, i.end, i);
            }).Where(t => t != null).Select(t => StreamEvent.CreateInterval(t.Item1, t.Item2, t.Item3)).OrderBy(i => i.StartTime).Select(i => i.StartTime).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 5);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), lt - 5 - i.start);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowAdjust1020Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowAdjust1020Time() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowAdjust1020Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowAdjust1020Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowAdjust1020TimeDiagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowAdjust1020TimeDiagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowAdjust1020TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 20);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), lt - 20 - i);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowAdjust1020TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 20);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), lt - 20 - i.start);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowSmallBatchDrop105 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowSmallBatchDrop105() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowSmallBatchDrop105()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i >= (latest - 5);
                latest = Math.Max(i, latest);
                return ret;
            }).Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowSmallBatchDrop105()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i.start >= (latest - 5);
                latest = Math.Max(i.start, latest);
                return ret;
            }).OrderBy(i => i.start).Select(i => i.start).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected)); // Not really checking for each interval but thats ok

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowSmallBatchDrop105Diagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowSmallBatchDrop105Diagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowSmallBatchDrop105Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i >= (latest - 5);
                latest = Math.Max(i, latest);
                return ret;
            }).Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 5);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowSmallBatchDrop105Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i.start >= (latest - 5);
                latest = Math.Max(i.start, latest);
                return ret;
            }).OrderBy(i => i.start).Select(i => i.start).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected)); // Not really checking for each interval but thats ok

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 5);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowSmallBatchDrop1020 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowSmallBatchDrop1020() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowSmallBatchDrop1020()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowSmallBatchDrop1020()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowSmallBatchDrop1020Diagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowSmallBatchDrop1020Diagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowSmallBatchDrop1020Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 20);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowSmallBatchDrop1020Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 20);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowSmallBatchDrop105Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowSmallBatchDrop105Time() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowSmallBatchDrop105Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i >= (latest - 5);
                latest = Math.Max(i, latest);
                return ret;
            }).Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowSmallBatchDrop105Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i.start >= (latest - 5);
                latest = Math.Max(i.start, latest);
                return ret;
            }).OrderBy(i => i.start).Select(i => i.start).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected)); // Not really checking for each interval but thats ok

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowSmallBatchDrop105TimeDiagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowSmallBatchDrop105TimeDiagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowSmallBatchDrop105TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i >= (latest - 5);
                latest = Math.Max(i, latest);
                return ret;
            }).Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 5);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowSmallBatchDrop105TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i.start >= (latest - 5);
                latest = Math.Max(i.start, latest);
                return ret;
            }).OrderBy(i => i.start).Select(i => i.start).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected)); // Not really checking for each interval but thats ok

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 5);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowSmallBatchDrop1020Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowSmallBatchDrop1020Time() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowSmallBatchDrop1020Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowSmallBatchDrop1020Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowSmallBatchDrop1020TimeDiagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowSmallBatchDrop1020TimeDiagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowSmallBatchDrop1020TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 20);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowSmallBatchDrop1020TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 20);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowSmallBatchThrow105 : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowSmallBatchThrow105() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowSmallBatchThrow105()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowSmallBatchThrow105()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowSmallBatchThrow105Diagnostic : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowSmallBatchThrow105Diagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowSmallBatchThrow105Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowSmallBatchThrow105Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowSmallBatchThrow1020 : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowSmallBatchThrow1020() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowSmallBatchThrow1020()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
                Assert.IsTrue(output.SequenceEqual(expected));
    
                Assert.IsTrue(!punctuations.Any());
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowSmallBatchThrow1020()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
                Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));
    
                Assert.IsTrue(!punctuations.Any());
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowSmallBatchThrow1020Diagnostic : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowSmallBatchThrow1020Diagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowSmallBatchThrow1020Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
                Assert.IsTrue(output.SequenceEqual(expected));
    
                {
                    var lt = 0;
                    var expectedOutOfOrder = disorderedInputData.Select(i =>
                        {
                            var outofwindow = i < (lt - 20);
                            lt = Math.Max(i, lt);
                            if (outofwindow)
                                return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), null);
                            return null;
                        }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                    Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
                }
    
                Assert.IsTrue(!punctuations.Any());
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowSmallBatchThrow1020Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
                Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));
    
                {
                    var lt = 0L;
                    var expectedOutOfOrder = disorderedInputData.Select(i =>
                        {
                            var outofwindow = i.start < (lt - 20);
                            lt = Math.Max(i.start, lt);
                            if (outofwindow)
                                return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), null);
                            return null;
                        }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                    Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
                }
    
                Assert.IsTrue(!punctuations.Any());
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowSmallBatchThrow105Time : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowSmallBatchThrow105Time() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowSmallBatchThrow105Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowSmallBatchThrow105Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowSmallBatchThrow105TimeDiagnostic : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowSmallBatchThrow105TimeDiagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowSmallBatchThrow105TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowSmallBatchThrow105TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowSmallBatchThrow1020Time : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowSmallBatchThrow1020Time() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowSmallBatchThrow1020Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
                Assert.IsTrue(output.SequenceEqual(expected));
    
                var highmark = 0;
                var lastPunc = 0;
                var current = 0;
                var queue = new PriorityQueue<int>();
                var expectedPunctuations = disorderedInputData.Select(i =>
                {
                    var outoforder = i < highmark - 20;
                    if (outoforder)
                    {
                        return null;
                    }
                    queue.Enqueue(i);
                    highmark = Math.Max(i, highmark);
                    while (queue.Peek() <= highmark - 20)
                    {
                        current = queue.Dequeue();
                        if (current - lastPunc >= 10)
                        {
                            lastPunc = current;
                            return Tuple.Create(i, current);
                        }
                    }
                    return null;
                }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
                while (queue.Count() > 0)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                    }
                }
                Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowSmallBatchThrow1020Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
                Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));
    
                long highmark = 0L;
                long lastPunc = 0L;
                long current = 0L;
                var queue = new PriorityQueue<long>();
                var expectedPunctuations = disorderedInputData.Select(i =>
                {
                    var outoforder = i.start < highmark - 20;
                    if (outoforder)
                    {
                        return null;
                    }
                    queue.Enqueue(i.start);
                    highmark = Math.Max(i.start, highmark);
                    while (queue.Peek() <= highmark - 20)
                    {
                        current = queue.Dequeue();
                        if (current - lastPunc >= 10)
                        {
                            lastPunc = current;
                            return Tuple.Create(i.start, current);
                        }
                    }
                    return null;
                }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
                while (queue.Count() > 0)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                    }
                }
                Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowSmallBatchThrow1020TimeDiagnostic : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowSmallBatchThrow1020TimeDiagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowSmallBatchThrow1020TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
                Assert.IsTrue(output.SequenceEqual(expected));
    
                {
                    var lt = 0;
                    var expectedOutOfOrder = disorderedInputData.Select(i =>
                        {
                            var outofwindow = i < (lt - 20);
                            lt = Math.Max(i, lt);
                            if (outofwindow)
                                return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), null);
                            return null;
                        }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                    Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
                }
    
                var highmark = 0;
                var lastPunc = 0;
                var current = 0;
                var queue = new PriorityQueue<int>();
                var expectedPunctuations = disorderedInputData.Select(i =>
                {
                    var outoforder = i < highmark - 20;
                    if (outoforder)
                    {
                        return null;
                    }
                    queue.Enqueue(i);
                    highmark = Math.Max(i, highmark);
                    while (queue.Peek() <= highmark - 20)
                    {
                        current = queue.Dequeue();
                        if (current - lastPunc >= 10)
                        {
                            lastPunc = current;
                            return Tuple.Create(i, current);
                        }
                    }
                    return null;
                }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
                while (queue.Count() > 0)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                    }
                }
                Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowSmallBatchThrow1020TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
                Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));
    
                {
                    var lt = 0L;
                    var expectedOutOfOrder = disorderedInputData.Select(i =>
                        {
                            var outofwindow = i.start < (lt - 20);
                            lt = Math.Max(i.start, lt);
                            if (outofwindow)
                                return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), null);
                            return null;
                        }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                    Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
                }
    
                long highmark = 0L;
                long lastPunc = 0L;
                long current = 0L;
                var queue = new PriorityQueue<long>();
                var expectedPunctuations = disorderedInputData.Select(i =>
                {
                    var outoforder = i.start < highmark - 20;
                    if (outoforder)
                    {
                        return null;
                    }
                    queue.Enqueue(i.start);
                    highmark = Math.Max(i.start, highmark);
                    while (queue.Peek() <= highmark - 20)
                    {
                        current = queue.Dequeue();
                        if (current - lastPunc >= 10)
                        {
                            lastPunc = current;
                            return Tuple.Create(i.start, current);
                        }
                    }
                    return null;
                }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
                while (queue.Count() > 0)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                    }
                }
                Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowSmallBatchAdjust105 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowSmallBatchAdjust105() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowSmallBatchAdjust105()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i < (latest - 5);
                latest = Math.Max(i, latest);
                if (outofwindow) return StreamEvent.CreateStart(latest - 5, i); // adjust
                return StreamEvent.CreateStart(i, i);
            }).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowSmallBatchAdjust105()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i.start < (latest - 5);
                var endoutofwindow = i.end < (latest - 5);

                latest = Math.Max(i.start, latest);
                if (endoutofwindow) return null;
                if (outofwindow) return Tuple.Create(latest - 5, i.end, i); // adjust
                return Tuple.Create(i.start, i.end, i);
            }).Where(t => t != null).Select(t => StreamEvent.CreateInterval(t.Item1, t.Item2, t.Item3)).OrderBy(i => i.StartTime).Select(i => i.StartTime).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowSmallBatchAdjust105Diagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowSmallBatchAdjust105Diagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowSmallBatchAdjust105Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i < (latest - 5);
                latest = Math.Max(i, latest);
                if (outofwindow) return StreamEvent.CreateStart(latest - 5, i); // adjust
                return StreamEvent.CreateStart(i, i);
            }).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 5);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), lt - 5 - i);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowSmallBatchAdjust105Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i.start < (latest - 5);
                var endoutofwindow = i.end < (latest - 5);

                latest = Math.Max(i.start, latest);
                if (endoutofwindow) return null;
                if (outofwindow) return Tuple.Create(latest - 5, i.end, i); // adjust
                return Tuple.Create(i.start, i.end, i);
            }).Where(t => t != null).Select(t => StreamEvent.CreateInterval(t.Item1, t.Item2, t.Item3)).OrderBy(i => i.StartTime).Select(i => i.StartTime).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 5);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), lt - 5 - i.start);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowSmallBatchAdjust1020 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowSmallBatchAdjust1020() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowSmallBatchAdjust1020()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowSmallBatchAdjust1020()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowSmallBatchAdjust1020Diagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowSmallBatchAdjust1020Diagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowSmallBatchAdjust1020Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 20);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), lt - 20 - i);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowSmallBatchAdjust1020Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 20);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), lt - 20 - i.start);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowSmallBatchAdjust105Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowSmallBatchAdjust105Time() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowSmallBatchAdjust105Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i < (latest - 5);
                latest = Math.Max(i, latest);
                if (outofwindow) return StreamEvent.CreateStart(latest - 5, i); // adjust
                return StreamEvent.CreateStart(i, i);
            }).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowSmallBatchAdjust105Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i.start < (latest - 5);
                var endoutofwindow = i.end < (latest - 5);

                latest = Math.Max(i.start, latest);
                if (endoutofwindow) return null;
                if (outofwindow) return Tuple.Create(latest - 5, i.end, i); // adjust
                return Tuple.Create(i.start, i.end, i);
            }).Where(t => t != null).Select(t => StreamEvent.CreateInterval(t.Item1, t.Item2, t.Item3)).OrderBy(i => i.StartTime).Select(i => i.StartTime).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowSmallBatchAdjust105TimeDiagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowSmallBatchAdjust105TimeDiagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowSmallBatchAdjust105TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i < (latest - 5);
                latest = Math.Max(i, latest);
                if (outofwindow) return StreamEvent.CreateStart(latest - 5, i); // adjust
                return StreamEvent.CreateStart(i, i);
            }).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 5);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), lt - 5 - i);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowSmallBatchAdjust105TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i.start < (latest - 5);
                var endoutofwindow = i.end < (latest - 5);

                latest = Math.Max(i.start, latest);
                if (endoutofwindow) return null;
                if (outofwindow) return Tuple.Create(latest - 5, i.end, i); // adjust
                return Tuple.Create(i.start, i.end, i);
            }).Where(t => t != null).Select(t => StreamEvent.CreateInterval(t.Item1, t.Item2, t.Item3)).OrderBy(i => i.StartTime).Select(i => i.StartTime).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 5);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), lt - 5 - i.start);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowSmallBatchAdjust1020Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowSmallBatchAdjust1020Time() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowSmallBatchAdjust1020Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowSmallBatchAdjust1020Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsRowSmallBatchAdjust1020TimeDiagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsRowSmallBatchAdjust1020TimeDiagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestRowSmallBatchAdjust1020TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 20);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), lt - 20 - i);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestRowSmallBatchAdjust1020TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 20);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), lt - 20 - i.start);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarDrop105 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarDrop105() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarDrop105()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i >= (latest - 5);
                latest = Math.Max(i, latest);
                return ret;
            }).Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarDrop105()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i.start >= (latest - 5);
                latest = Math.Max(i.start, latest);
                return ret;
            }).OrderBy(i => i.start).Select(i => i.start).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected)); // Not really checking for each interval but thats ok

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarDrop105Diagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarDrop105Diagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarDrop105Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i >= (latest - 5);
                latest = Math.Max(i, latest);
                return ret;
            }).Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 5);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarDrop105Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i.start >= (latest - 5);
                latest = Math.Max(i.start, latest);
                return ret;
            }).OrderBy(i => i.start).Select(i => i.start).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected)); // Not really checking for each interval but thats ok

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 5);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarDrop1020 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarDrop1020() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarDrop1020()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarDrop1020()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarDrop1020Diagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarDrop1020Diagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarDrop1020Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 20);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarDrop1020Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 20);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarDrop105Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarDrop105Time() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarDrop105Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i >= (latest - 5);
                latest = Math.Max(i, latest);
                return ret;
            }).Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarDrop105Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i.start >= (latest - 5);
                latest = Math.Max(i.start, latest);
                return ret;
            }).OrderBy(i => i.start).Select(i => i.start).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected)); // Not really checking for each interval but thats ok

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarDrop105TimeDiagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarDrop105TimeDiagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarDrop105TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i >= (latest - 5);
                latest = Math.Max(i, latest);
                return ret;
            }).Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 5);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarDrop105TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i.start >= (latest - 5);
                latest = Math.Max(i.start, latest);
                return ret;
            }).OrderBy(i => i.start).Select(i => i.start).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected)); // Not really checking for each interval but thats ok

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 5);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarDrop1020Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarDrop1020Time() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarDrop1020Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarDrop1020Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarDrop1020TimeDiagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarDrop1020TimeDiagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarDrop1020TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 20);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarDrop1020TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 20);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarThrow105 : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarThrow105() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarThrow105()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarThrow105()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarThrow105Diagnostic : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarThrow105Diagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarThrow105Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarThrow105Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarThrow1020 : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarThrow1020() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarThrow1020()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
                Assert.IsTrue(output.SequenceEqual(expected));
    
                Assert.IsTrue(!punctuations.Any());
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarThrow1020()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
                Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));
    
                Assert.IsTrue(!punctuations.Any());
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarThrow1020Diagnostic : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarThrow1020Diagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarThrow1020Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
                Assert.IsTrue(output.SequenceEqual(expected));
    
                {
                    var lt = 0;
                    var expectedOutOfOrder = disorderedInputData.Select(i =>
                        {
                            var outofwindow = i < (lt - 20);
                            lt = Math.Max(i, lt);
                            if (outofwindow)
                                return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), null);
                            return null;
                        }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                    Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
                }
    
                Assert.IsTrue(!punctuations.Any());
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarThrow1020Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
                Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));
    
                {
                    var lt = 0L;
                    var expectedOutOfOrder = disorderedInputData.Select(i =>
                        {
                            var outofwindow = i.start < (lt - 20);
                            lt = Math.Max(i.start, lt);
                            if (outofwindow)
                                return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), null);
                            return null;
                        }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                    Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
                }
    
                Assert.IsTrue(!punctuations.Any());
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarThrow105Time : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarThrow105Time() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarThrow105Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarThrow105Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarThrow105TimeDiagnostic : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarThrow105TimeDiagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarThrow105TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarThrow105TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarThrow1020Time : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarThrow1020Time() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarThrow1020Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
                Assert.IsTrue(output.SequenceEqual(expected));
    
                var highmark = 0;
                var lastPunc = 0;
                var current = 0;
                var queue = new PriorityQueue<int>();
                var expectedPunctuations = disorderedInputData.Select(i =>
                {
                    var outoforder = i < highmark - 20;
                    if (outoforder)
                    {
                        return null;
                    }
                    queue.Enqueue(i);
                    highmark = Math.Max(i, highmark);
                    while (queue.Peek() <= highmark - 20)
                    {
                        current = queue.Dequeue();
                        if (current - lastPunc >= 10)
                        {
                            lastPunc = current;
                            return Tuple.Create(i, current);
                        }
                    }
                    return null;
                }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
                while (queue.Count() > 0)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                    }
                }
                Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarThrow1020Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
                Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));
    
                long highmark = 0L;
                long lastPunc = 0L;
                long current = 0L;
                var queue = new PriorityQueue<long>();
                var expectedPunctuations = disorderedInputData.Select(i =>
                {
                    var outoforder = i.start < highmark - 20;
                    if (outoforder)
                    {
                        return null;
                    }
                    queue.Enqueue(i.start);
                    highmark = Math.Max(i.start, highmark);
                    while (queue.Peek() <= highmark - 20)
                    {
                        current = queue.Dequeue();
                        if (current - lastPunc >= 10)
                        {
                            lastPunc = current;
                            return Tuple.Create(i.start, current);
                        }
                    }
                    return null;
                }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
                while (queue.Count() > 0)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                    }
                }
                Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarThrow1020TimeDiagnostic : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarThrow1020TimeDiagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarThrow1020TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
                Assert.IsTrue(output.SequenceEqual(expected));
    
                {
                    var lt = 0;
                    var expectedOutOfOrder = disorderedInputData.Select(i =>
                        {
                            var outofwindow = i < (lt - 20);
                            lt = Math.Max(i, lt);
                            if (outofwindow)
                                return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), null);
                            return null;
                        }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                    Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
                }
    
                var highmark = 0;
                var lastPunc = 0;
                var current = 0;
                var queue = new PriorityQueue<int>();
                var expectedPunctuations = disorderedInputData.Select(i =>
                {
                    var outoforder = i < highmark - 20;
                    if (outoforder)
                    {
                        return null;
                    }
                    queue.Enqueue(i);
                    highmark = Math.Max(i, highmark);
                    while (queue.Peek() <= highmark - 20)
                    {
                        current = queue.Dequeue();
                        if (current - lastPunc >= 10)
                        {
                            lastPunc = current;
                            return Tuple.Create(i, current);
                        }
                    }
                    return null;
                }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
                while (queue.Count() > 0)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                    }
                }
                Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarThrow1020TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
                Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));
    
                {
                    var lt = 0L;
                    var expectedOutOfOrder = disorderedInputData.Select(i =>
                        {
                            var outofwindow = i.start < (lt - 20);
                            lt = Math.Max(i.start, lt);
                            if (outofwindow)
                                return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), null);
                            return null;
                        }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                    Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
                }
    
                long highmark = 0L;
                long lastPunc = 0L;
                long current = 0L;
                var queue = new PriorityQueue<long>();
                var expectedPunctuations = disorderedInputData.Select(i =>
                {
                    var outoforder = i.start < highmark - 20;
                    if (outoforder)
                    {
                        return null;
                    }
                    queue.Enqueue(i.start);
                    highmark = Math.Max(i.start, highmark);
                    while (queue.Peek() <= highmark - 20)
                    {
                        current = queue.Dequeue();
                        if (current - lastPunc >= 10)
                        {
                            lastPunc = current;
                            return Tuple.Create(i.start, current);
                        }
                    }
                    return null;
                }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
                while (queue.Count() > 0)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                    }
                }
                Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarAdjust105 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarAdjust105() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarAdjust105()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i < (latest - 5);
                latest = Math.Max(i, latest);
                if (outofwindow) return StreamEvent.CreateStart(latest - 5, i); // adjust
                return StreamEvent.CreateStart(i, i);
            }).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarAdjust105()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i.start < (latest - 5);
                var endoutofwindow = i.end < (latest - 5);

                latest = Math.Max(i.start, latest);
                if (endoutofwindow) return null;
                if (outofwindow) return Tuple.Create(latest - 5, i.end, i); // adjust
                return Tuple.Create(i.start, i.end, i);
            }).Where(t => t != null).Select(t => StreamEvent.CreateInterval(t.Item1, t.Item2, t.Item3)).OrderBy(i => i.StartTime).Select(i => i.StartTime).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarAdjust105Diagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarAdjust105Diagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarAdjust105Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i < (latest - 5);
                latest = Math.Max(i, latest);
                if (outofwindow) return StreamEvent.CreateStart(latest - 5, i); // adjust
                return StreamEvent.CreateStart(i, i);
            }).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 5);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), lt - 5 - i);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarAdjust105Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i.start < (latest - 5);
                var endoutofwindow = i.end < (latest - 5);

                latest = Math.Max(i.start, latest);
                if (endoutofwindow) return null;
                if (outofwindow) return Tuple.Create(latest - 5, i.end, i); // adjust
                return Tuple.Create(i.start, i.end, i);
            }).Where(t => t != null).Select(t => StreamEvent.CreateInterval(t.Item1, t.Item2, t.Item3)).OrderBy(i => i.StartTime).Select(i => i.StartTime).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 5);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), lt - 5 - i.start);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarAdjust1020 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarAdjust1020() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarAdjust1020()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarAdjust1020()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarAdjust1020Diagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarAdjust1020Diagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarAdjust1020Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 20);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), lt - 20 - i);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarAdjust1020Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 20);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), lt - 20 - i.start);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarAdjust105Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarAdjust105Time() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarAdjust105Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i < (latest - 5);
                latest = Math.Max(i, latest);
                if (outofwindow) return StreamEvent.CreateStart(latest - 5, i); // adjust
                return StreamEvent.CreateStart(i, i);
            }).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarAdjust105Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i.start < (latest - 5);
                var endoutofwindow = i.end < (latest - 5);

                latest = Math.Max(i.start, latest);
                if (endoutofwindow) return null;
                if (outofwindow) return Tuple.Create(latest - 5, i.end, i); // adjust
                return Tuple.Create(i.start, i.end, i);
            }).Where(t => t != null).Select(t => StreamEvent.CreateInterval(t.Item1, t.Item2, t.Item3)).OrderBy(i => i.StartTime).Select(i => i.StartTime).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarAdjust105TimeDiagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarAdjust105TimeDiagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarAdjust105TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i < (latest - 5);
                latest = Math.Max(i, latest);
                if (outofwindow) return StreamEvent.CreateStart(latest - 5, i); // adjust
                return StreamEvent.CreateStart(i, i);
            }).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 5);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), lt - 5 - i);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarAdjust105TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i.start < (latest - 5);
                var endoutofwindow = i.end < (latest - 5);

                latest = Math.Max(i.start, latest);
                if (endoutofwindow) return null;
                if (outofwindow) return Tuple.Create(latest - 5, i.end, i); // adjust
                return Tuple.Create(i.start, i.end, i);
            }).Where(t => t != null).Select(t => StreamEvent.CreateInterval(t.Item1, t.Item2, t.Item3)).OrderBy(i => i.StartTime).Select(i => i.StartTime).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 5);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), lt - 5 - i.start);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarAdjust1020Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarAdjust1020Time() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarAdjust1020Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarAdjust1020Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarAdjust1020TimeDiagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarAdjust1020TimeDiagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarAdjust1020TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 20);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), lt - 20 - i);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarAdjust1020TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 20);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), lt - 20 - i.start);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarSmallBatchDrop105 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarSmallBatchDrop105() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarSmallBatchDrop105()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i >= (latest - 5);
                latest = Math.Max(i, latest);
                return ret;
            }).Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarSmallBatchDrop105()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i.start >= (latest - 5);
                latest = Math.Max(i.start, latest);
                return ret;
            }).OrderBy(i => i.start).Select(i => i.start).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected)); // Not really checking for each interval but thats ok

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarSmallBatchDrop105Diagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarSmallBatchDrop105Diagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarSmallBatchDrop105Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i >= (latest - 5);
                latest = Math.Max(i, latest);
                return ret;
            }).Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 5);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarSmallBatchDrop105Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i.start >= (latest - 5);
                latest = Math.Max(i.start, latest);
                return ret;
            }).OrderBy(i => i.start).Select(i => i.start).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected)); // Not really checking for each interval but thats ok

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 5);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarSmallBatchDrop1020 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarSmallBatchDrop1020() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarSmallBatchDrop1020()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarSmallBatchDrop1020()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarSmallBatchDrop1020Diagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarSmallBatchDrop1020Diagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarSmallBatchDrop1020Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 20);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarSmallBatchDrop1020Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 20);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarSmallBatchDrop105Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarSmallBatchDrop105Time() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarSmallBatchDrop105Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i >= (latest - 5);
                latest = Math.Max(i, latest);
                return ret;
            }).Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarSmallBatchDrop105Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i.start >= (latest - 5);
                latest = Math.Max(i.start, latest);
                return ret;
            }).OrderBy(i => i.start).Select(i => i.start).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected)); // Not really checking for each interval but thats ok

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarSmallBatchDrop105TimeDiagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarSmallBatchDrop105TimeDiagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarSmallBatchDrop105TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i >= (latest - 5);
                latest = Math.Max(i, latest);
                return ret;
            }).Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 5);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarSmallBatchDrop105TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Where(i =>
            {
                var ret = i.start >= (latest - 5);
                latest = Math.Max(i.start, latest);
                return ret;
            }).OrderBy(i => i.start).Select(i => i.start).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected)); // Not really checking for each interval but thats ok

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 5);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarSmallBatchDrop1020Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarSmallBatchDrop1020Time() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarSmallBatchDrop1020Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarSmallBatchDrop1020Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarSmallBatchDrop1020TimeDiagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarSmallBatchDrop1020TimeDiagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarSmallBatchDrop1020TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 20);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarSmallBatchDrop1020TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Drop(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 20);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), null);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarSmallBatchThrow105 : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarSmallBatchThrow105() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarSmallBatchThrow105()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarSmallBatchThrow105()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarSmallBatchThrow105Diagnostic : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarSmallBatchThrow105Diagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarSmallBatchThrow105Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarSmallBatchThrow105Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarSmallBatchThrow1020 : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarSmallBatchThrow1020() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarSmallBatchThrow1020()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
                Assert.IsTrue(output.SequenceEqual(expected));
    
                Assert.IsTrue(!punctuations.Any());
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarSmallBatchThrow1020()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
                Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));
    
                Assert.IsTrue(!punctuations.Any());
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarSmallBatchThrow1020Diagnostic : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarSmallBatchThrow1020Diagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarSmallBatchThrow1020Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
                Assert.IsTrue(output.SequenceEqual(expected));
    
                {
                    var lt = 0;
                    var expectedOutOfOrder = disorderedInputData.Select(i =>
                        {
                            var outofwindow = i < (lt - 20);
                            lt = Math.Max(i, lt);
                            if (outofwindow)
                                return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), null);
                            return null;
                        }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                    Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
                }
    
                Assert.IsTrue(!punctuations.Any());
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarSmallBatchThrow1020Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    null);
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
                Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));
    
                {
                    var lt = 0L;
                    var expectedOutOfOrder = disorderedInputData.Select(i =>
                        {
                            var outofwindow = i.start < (lt - 20);
                            lt = Math.Max(i.start, lt);
                            if (outofwindow)
                                return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), null);
                            return null;
                        }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                    Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
                }
    
                Assert.IsTrue(!punctuations.Any());
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarSmallBatchThrow105Time : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarSmallBatchThrow105Time() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarSmallBatchThrow105Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarSmallBatchThrow105Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarSmallBatchThrow105TimeDiagnostic : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarSmallBatchThrow105TimeDiagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarSmallBatchThrow105TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarSmallBatchThrow105TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(5),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                Assert.Fail("Expecting OutofOrderException");
    
            }
            catch(Exception)
            {
                Assert.IsTrue(true); // Todo. Verify if the ingress/egress before the exception was correct.
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarSmallBatchThrow1020Time : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarSmallBatchThrow1020Time() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarSmallBatchThrow1020Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
                Assert.IsTrue(output.SequenceEqual(expected));
    
                var highmark = 0;
                var lastPunc = 0;
                var current = 0;
                var queue = new PriorityQueue<int>();
                var expectedPunctuations = disorderedInputData.Select(i =>
                {
                    var outoforder = i < highmark - 20;
                    if (outoforder)
                    {
                        return null;
                    }
                    queue.Enqueue(i);
                    highmark = Math.Max(i, highmark);
                    while (queue.Peek() <= highmark - 20)
                    {
                        current = queue.Dequeue();
                        if (current - lastPunc >= 10)
                        {
                            lastPunc = current;
                            return Tuple.Create(i, current);
                        }
                    }
                    return null;
                }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
                while (queue.Count() > 0)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                    }
                }
                Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarSmallBatchThrow1020Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
                Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));
    
                long highmark = 0L;
                long lastPunc = 0L;
                long current = 0L;
                var queue = new PriorityQueue<long>();
                var expectedPunctuations = disorderedInputData.Select(i =>
                {
                    var outoforder = i.start < highmark - 20;
                    if (outoforder)
                    {
                        return null;
                    }
                    queue.Enqueue(i.start);
                    highmark = Math.Max(i.start, highmark);
                    while (queue.Peek() <= highmark - 20)
                    {
                        current = queue.Dequeue();
                        if (current - lastPunc >= 10)
                        {
                            lastPunc = current;
                            return Tuple.Create(i.start, current);
                        }
                    }
                    return null;
                }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
                while (queue.Count() > 0)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                    }
                }
                Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarSmallBatchThrow1020TimeDiagnostic : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarSmallBatchThrow1020TimeDiagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarSmallBatchThrow1020TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                    o => o,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
                Assert.IsTrue(output.SequenceEqual(expected));
    
                {
                    var lt = 0;
                    var expectedOutOfOrder = disorderedInputData.Select(i =>
                        {
                            var outofwindow = i < (lt - 20);
                            lt = Math.Max(i, lt);
                            if (outofwindow)
                                return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), null);
                            return null;
                        }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                    Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
                }
    
                var highmark = 0;
                var lastPunc = 0;
                var current = 0;
                var queue = new PriorityQueue<int>();
                var expectedPunctuations = disorderedInputData.Select(i =>
                {
                    var outoforder = i < highmark - 20;
                    if (outoforder)
                    {
                        return null;
                    }
                    queue.Enqueue(i);
                    highmark = Math.Max(i, highmark);
                    while (queue.Peek() <= highmark - 20)
                    {
                        current = queue.Dequeue();
                        if (current - lastPunc >= 10)
                        {
                            lastPunc = current;
                            return Tuple.Create(i, current);
                        }
                    }
                    return null;
                }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
                while (queue.Count() > 0)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                    }
                }
                Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarSmallBatchThrow1020TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            try
            {
                var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                    DisorderPolicy.Throw(20),
                    FlushPolicy.FlushOnPunctuation,
                    PeriodicPunctuationPolicy.Time(10));
    
                var prog = ingress.ToStreamEventObservable();
    
                var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
                var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
                diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));
    
                var outevents = prog.ToEnumerable().ToList();
                Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));
    
                var output = outevents.Where(o => !o.IsPunctuation).ToList();
                var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
                var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
                Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));
    
                {
                    var lt = 0L;
                    var expectedOutOfOrder = disorderedInputData.Select(i =>
                        {
                            var outofwindow = i.start < (lt - 20);
                            lt = Math.Max(i.start, lt);
                            if (outofwindow)
                                return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), null);
                            return null;
                        }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                    Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
                }
    
                long highmark = 0L;
                long lastPunc = 0L;
                long current = 0L;
                var queue = new PriorityQueue<long>();
                var expectedPunctuations = disorderedInputData.Select(i =>
                {
                    var outoforder = i.start < highmark - 20;
                    if (outoforder)
                    {
                        return null;
                    }
                    queue.Enqueue(i.start);
                    highmark = Math.Max(i.start, highmark);
                    while (queue.Peek() <= highmark - 20)
                    {
                        current = queue.Dequeue();
                        if (current - lastPunc >= 10)
                        {
                            lastPunc = current;
                            return Tuple.Create(i.start, current);
                        }
                    }
                    return null;
                }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
                while (queue.Count() > 0)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                    }
                }
                Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
            }
            catch(Exception)
            {
                Assert.Fail("no exception should be thrown since disorder: 10 is always less than lag: 20.");
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarSmallBatchAdjust105 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarSmallBatchAdjust105() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarSmallBatchAdjust105()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i < (latest - 5);
                latest = Math.Max(i, latest);
                if (outofwindow) return StreamEvent.CreateStart(latest - 5, i); // adjust
                return StreamEvent.CreateStart(i, i);
            }).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarSmallBatchAdjust105()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i.start < (latest - 5);
                var endoutofwindow = i.end < (latest - 5);

                latest = Math.Max(i.start, latest);
                if (endoutofwindow) return null;
                if (outofwindow) return Tuple.Create(latest - 5, i.end, i); // adjust
                return Tuple.Create(i.start, i.end, i);
            }).Where(t => t != null).Select(t => StreamEvent.CreateInterval(t.Item1, t.Item2, t.Item3)).OrderBy(i => i.StartTime).Select(i => i.StartTime).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarSmallBatchAdjust105Diagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarSmallBatchAdjust105Diagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarSmallBatchAdjust105Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i < (latest - 5);
                latest = Math.Max(i, latest);
                if (outofwindow) return StreamEvent.CreateStart(latest - 5, i); // adjust
                return StreamEvent.CreateStart(i, i);
            }).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 5);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), lt - 5 - i);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarSmallBatchAdjust105Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i.start < (latest - 5);
                var endoutofwindow = i.end < (latest - 5);

                latest = Math.Max(i.start, latest);
                if (endoutofwindow) return null;
                if (outofwindow) return Tuple.Create(latest - 5, i.end, i); // adjust
                return Tuple.Create(i.start, i.end, i);
            }).Where(t => t != null).Select(t => StreamEvent.CreateInterval(t.Item1, t.Item2, t.Item3)).OrderBy(i => i.StartTime).Select(i => i.StartTime).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 5);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), lt - 5 - i.start);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarSmallBatchAdjust1020 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarSmallBatchAdjust1020() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarSmallBatchAdjust1020()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarSmallBatchAdjust1020()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarSmallBatchAdjust1020Diagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarSmallBatchAdjust1020Diagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarSmallBatchAdjust1020Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 20);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), lt - 20 - i);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarSmallBatchAdjust1020Diagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                null);

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 20);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), lt - 20 - i.start);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            Assert.IsTrue(!punctuations.Any());
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarSmallBatchAdjust105Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarSmallBatchAdjust105Time() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarSmallBatchAdjust105Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i < (latest - 5);
                latest = Math.Max(i, latest);
                if (outofwindow) return StreamEvent.CreateStart(latest - 5, i); // adjust
                return StreamEvent.CreateStart(i, i);
            }).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarSmallBatchAdjust105Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i.start < (latest - 5);
                var endoutofwindow = i.end < (latest - 5);

                latest = Math.Max(i.start, latest);
                if (endoutofwindow) return null;
                if (outofwindow) return Tuple.Create(latest - 5, i.end, i); // adjust
                return Tuple.Create(i.start, i.end, i);
            }).Where(t => t != null).Select(t => StreamEvent.CreateInterval(t.Item1, t.Item2, t.Item3)).OrderBy(i => i.StartTime).Select(i => i.StartTime).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarSmallBatchAdjust105TimeDiagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarSmallBatchAdjust105TimeDiagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarSmallBatchAdjust105TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i < (latest - 5);
                latest = Math.Max(i, latest);
                if (outofwindow) return StreamEvent.CreateStart(latest - 5, i); // adjust
                return StreamEvent.CreateStart(i, i);
            }).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 5);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), lt - 5 - i);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarSmallBatchAdjust105TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(5),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var latest = 0L;
            var expected = disorderedInputData.Select(i =>
            {
                var outofwindow = i.start < (latest - 5);
                var endoutofwindow = i.end < (latest - 5);

                latest = Math.Max(i.start, latest);
                if (endoutofwindow) return null;
                if (outofwindow) return Tuple.Create(latest - 5, i.end, i); // adjust
                return Tuple.Create(i.start, i.end, i);
            }).Where(t => t != null).Select(t => StreamEvent.CreateInterval(t.Item1, t.Item2, t.Item3)).OrderBy(i => i.StartTime).Select(i => i.StartTime).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 5);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), lt - 5 - i.start);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 5;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 5)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarSmallBatchAdjust1020Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarSmallBatchAdjust1020Time() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarSmallBatchAdjust1020Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarSmallBatchAdjust1020Time()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1028:CodeMustNotContainTrailingWhitespace",
        Justification = "T4 generation with PushIndent violates Trailing Whitespace rule")]
    [TestClass]
    public class DisorderedIngressAndEgressTestsColumnarSmallBatchAdjust1020TimeDiagnostic : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public DisorderedIngressAndEgressTestsColumnarSmallBatchAdjust1020TimeDiagnostic() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedStartEdgeTestColumnarSmallBatchAdjust1020TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<int>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => StreamEvent.CreateStart(i, i)).OrderBy(i => i.StartTime).ToList();
            Assert.IsTrue(output.SequenceEqual(expected));

            {
                var lt = 0;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i < (lt - 20);
                        lt = Math.Max(i, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<int>, long?>(StreamEvent.CreateStart(i, i), lt - 20 - i);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<int>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            var highmark = 0;
            var lastPunc = 0;
            var current = 0;
            var queue = new PriorityQueue<int>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i);
                highmark = Math.Max(i, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<int>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<int>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }

        public struct TestStruct
        {
            public long start;
            public long end;
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedIntervalTestColumnarSmallBatchAdjust1020TimeDiagnostic()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            int maxInterval = 10;
            const int inputsize = 30;
            var rand = new Random(10);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize).ToList()
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .Select(t => new TestStruct { start = t, end = t + rand.Next(1, maxInterval) })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(o => o.start, o => o.end,
                DisorderPolicy.Adjust(20),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10));

            var prog = ingress.ToStreamEventObservable();

            var diagnosticStream = ingress.GetDroppedAdjustedEventsDiagnostic();
            var outOfOrderEvents = new List<OutOfOrderStreamEvent<TestStruct>>();
            diagnosticStream.Subscribe(o => outOfOrderEvents.Add(o));

            var outevents = prog.ToEnumerable().ToList();
            Assert.IsTrue(outevents.IsOrdered(t => t.SyncTime));

            var output = outevents.Where(o => !o.IsPunctuation).ToList();
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime).ToList();
            var expected = disorderedInputData.Select(i => i.start).OrderBy(i => i).ToList();
            Assert.IsTrue(output.Select(i => i.StartTime).SequenceEqual(expected));

            {
                var lt = 0L;
                var expectedOutOfOrder = disorderedInputData.Select(i =>
                    {
                        var outofwindow = i.start < (lt - 20);
                        lt = Math.Max(i.start, lt);
                        if (outofwindow)
                            return Tuple.Create<StreamEvent<TestStruct>, long?>(StreamEvent.CreateInterval(i.start, i.end, i), lt - 20 - i.start);
                        return null;
                    }).Where(t => t != null).Select(t => new OutOfOrderStreamEvent<TestStruct>() { Event = t.Item1, TimeAdjustment = t.Item2 });
                Assert.IsTrue(outOfOrderEvents.SequenceEqual(expectedOutOfOrder));
            }

            long highmark = 0L;
            long lastPunc = 0L;
            long current = 0L;
            var queue = new PriorityQueue<long>();
            var expectedPunctuations = disorderedInputData.Select(i =>
            {
                var outoforder = i.start < highmark - 20;
                if (outoforder)
                {
                    return null;
                }
                queue.Enqueue(i.start);
                highmark = Math.Max(i.start, highmark);
                while (queue.Peek() <= highmark - 20)
                {
                    current = queue.Dequeue();
                    if (current - lastPunc >= 10)
                    {
                        lastPunc = current;
                        return Tuple.Create(i.start, current);
                    }
                }
                return null;
            }).Where(t => t != null).Select(t => StreamEvent.CreatePunctuation<TestStruct>(((long)(t.Item2)).SnapToLeftBoundary(10))).ToList();
            while (queue.Count() > 0)
            {
                current = queue.Dequeue();
                if (current - lastPunc >= 10)
                {
                    lastPunc = current;
                    expectedPunctuations.Add(StreamEvent.CreatePunctuation<TestStruct>(((long)current).SnapToLeftBoundary(10)));
                }
            }
            Assert.IsTrue(punctuations.SequenceEqual(expectedPunctuations));
        }
    }
}
