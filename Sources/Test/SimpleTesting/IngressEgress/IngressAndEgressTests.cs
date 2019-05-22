// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{

    [TestClass]
    public class IngressAndEgressTestsRow : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRow() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void Level1InLevel1OutTrivialRow()
        {
            var input = Enumerable.Range(0, 1000).ToList();
            var subject = new Subject<int>();
            var prog = subject.ToAtemporalStreamable(TimelinePolicy.Sequence(100)).ToEnumerable();
            input.ForEach(o => subject.OnNext(o));
            subject.OnCompleted();
            while (!prog.Completed) { }
            var output = prog.OrderBy(o => o);

            Assert.IsTrue(output.SequenceEqual(input));
        }

        [TestMethod, TestCategory("Gated")]
        public void Level1InLevel3OutTrivialRow()
        {
            var input = Enumerable.Range(0, 1000).ToList();
            var subject = new Subject<int>();
            var output = new List<StreamEvent<int>>();

            var outputAwait = subject.ToAtemporalStreamable(TimelinePolicy.Sequence(100)).ToStreamEventObservable().Where(o => o.IsData).ForEachAsync(o => output.Add(o));
            input.ForEach(o => subject.OnNext(o));
            subject.OnCompleted();
            outputAwait.Wait();

            Assert.IsTrue(output.Select(o => o.Payload).SequenceEqual(input));
            Assert.IsTrue(output.All(o => o.EndTime == StreamEvent.InfinitySyncTime));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRow()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                null,
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRow()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                null,
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowTime : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowTime() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                null,
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                null,
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowDrop : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowDrop() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowDrop()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Drop(0),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowDrop()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Drop(0),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowDrop10 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowDrop10() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowDrop10()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Drop(10),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowDrop10()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Drop(10),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowDrop1000 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowDrop1000() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowDrop1000()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Drop(1000),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowDrop1000()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Drop(1000),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowDropTime : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowDropTime() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowDropTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Drop(0),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowDropTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Drop(0),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowDrop10Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowDrop10Time() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowDrop10Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Drop(10),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowDrop10Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Drop(10),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowDrop1000Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowDrop1000Time() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowDrop1000Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Drop(1000),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowDrop1000Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Drop(1000),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowThrow : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowThrow() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowThrow()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Throw(0),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowThrow()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Throw(0),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowThrow10 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowThrow10() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowThrow10()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Throw(10),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowThrow10()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Throw(10),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowThrow1000 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowThrow1000() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowThrow1000()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Throw(1000),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowThrow1000()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Throw(1000),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowThrowTime : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowThrowTime() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowThrowTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Throw(0),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowThrowTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Throw(0),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowThrow10Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowThrow10Time() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowThrow10Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Throw(10),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowThrow10Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Throw(10),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowThrow1000Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowThrow1000Time() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowThrow1000Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Throw(1000),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowThrow1000Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Throw(1000),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowAdjust : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowAdjust() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowAdjust()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Adjust(0),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowAdjust()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Adjust(0),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowAdjust10 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowAdjust10() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowAdjust10()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Adjust(10),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowAdjust10()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Adjust(10),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowAdjust1000 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowAdjust1000() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowAdjust1000()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Adjust(1000),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowAdjust1000()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Adjust(1000),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowAdjustTime : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowAdjustTime() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowAdjustTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Adjust(0),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowAdjustTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Adjust(0),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowAdjust10Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowAdjust10Time() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowAdjust10Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Adjust(10),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowAdjust10Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Adjust(10),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowAdjust1000Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowAdjust1000Time() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowAdjust1000Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Adjust(1000),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowAdjust1000Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Adjust(1000),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowSmallBatch : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowSmallBatch() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void Level1InLevel1OutTrivialRowSmallBatch()
        {
            var input = Enumerable.Range(0, 1000).ToList();
            var subject = new Subject<int>();
            var prog = subject.ToAtemporalStreamable(TimelinePolicy.Sequence(100)).ToEnumerable();
            input.ForEach(o => subject.OnNext(o));
            subject.OnCompleted();
            while (!prog.Completed) { }
            var output = prog.OrderBy(o => o);

            Assert.IsTrue(output.SequenceEqual(input));
        }

        [TestMethod, TestCategory("Gated")]
        public void Level1InLevel3OutTrivialRowSmallBatch()
        {
            var input = Enumerable.Range(0, 1000).ToList();
            var subject = new Subject<int>();
            var output = new List<StreamEvent<int>>();

            var outputAwait = subject.ToAtemporalStreamable(TimelinePolicy.Sequence(100)).ToStreamEventObservable().Where(o => o.IsData).ForEachAsync(o => output.Add(o));
            input.ForEach(o => subject.OnNext(o));
            subject.OnCompleted();
            outputAwait.Wait();

            Assert.IsTrue(output.Select(o => o.Payload).SequenceEqual(input));
            Assert.IsTrue(output.All(o => o.EndTime == StreamEvent.InfinitySyncTime));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowSmallBatch()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                null,
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowSmallBatch()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                null,
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowSmallBatchTime : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowSmallBatchTime() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowSmallBatchTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                null,
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowSmallBatchTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                null,
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowSmallBatchDrop : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowSmallBatchDrop() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowSmallBatchDrop()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Drop(0),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowSmallBatchDrop()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Drop(0),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowSmallBatchDrop10 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowSmallBatchDrop10() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowSmallBatchDrop10()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Drop(10),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowSmallBatchDrop10()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Drop(10),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowSmallBatchDrop1000 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowSmallBatchDrop1000() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowSmallBatchDrop1000()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Drop(1000),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowSmallBatchDrop1000()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Drop(1000),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowSmallBatchDropTime : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowSmallBatchDropTime() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowSmallBatchDropTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Drop(0),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowSmallBatchDropTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Drop(0),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowSmallBatchDrop10Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowSmallBatchDrop10Time() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowSmallBatchDrop10Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Drop(10),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowSmallBatchDrop10Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Drop(10),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowSmallBatchDrop1000Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowSmallBatchDrop1000Time() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowSmallBatchDrop1000Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Drop(1000),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowSmallBatchDrop1000Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Drop(1000),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowSmallBatchThrow : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowSmallBatchThrow() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowSmallBatchThrow()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Throw(0),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowSmallBatchThrow()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Throw(0),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowSmallBatchThrow10 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowSmallBatchThrow10() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowSmallBatchThrow10()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Throw(10),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowSmallBatchThrow10()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Throw(10),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowSmallBatchThrow1000 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowSmallBatchThrow1000() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowSmallBatchThrow1000()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Throw(1000),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowSmallBatchThrow1000()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Throw(1000),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowSmallBatchThrowTime : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowSmallBatchThrowTime() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowSmallBatchThrowTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Throw(0),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowSmallBatchThrowTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Throw(0),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowSmallBatchThrow10Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowSmallBatchThrow10Time() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowSmallBatchThrow10Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Throw(10),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowSmallBatchThrow10Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Throw(10),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowSmallBatchThrow1000Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowSmallBatchThrow1000Time() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowSmallBatchThrow1000Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Throw(1000),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowSmallBatchThrow1000Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Throw(1000),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowSmallBatchAdjust : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowSmallBatchAdjust() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowSmallBatchAdjust()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Adjust(0),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowSmallBatchAdjust()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Adjust(0),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowSmallBatchAdjust10 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowSmallBatchAdjust10() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowSmallBatchAdjust10()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Adjust(10),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowSmallBatchAdjust10()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Adjust(10),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowSmallBatchAdjust1000 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowSmallBatchAdjust1000() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowSmallBatchAdjust1000()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Adjust(1000),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowSmallBatchAdjust1000()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Adjust(1000),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowSmallBatchAdjustTime : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowSmallBatchAdjustTime() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowSmallBatchAdjustTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Adjust(0),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowSmallBatchAdjustTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Adjust(0),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowSmallBatchAdjust10Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowSmallBatchAdjust10Time() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowSmallBatchAdjust10Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Adjust(10),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowSmallBatchAdjust10Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Adjust(10),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsRowSmallBatchAdjust1000Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsRowSmallBatchAdjust1000Time() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestRowSmallBatchAdjust1000Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Adjust(1000),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestRowSmallBatchAdjust1000Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Adjust(1000),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnar : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnar() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void Level1InLevel1OutTrivialColumnar()
        {
            var input = Enumerable.Range(0, 1000).ToList();
            var subject = new Subject<int>();
            var prog = subject.ToAtemporalStreamable(TimelinePolicy.Sequence(100)).ToEnumerable();
            input.ForEach(o => subject.OnNext(o));
            subject.OnCompleted();
            while (!prog.Completed) { }
            var output = prog.OrderBy(o => o);

            Assert.IsTrue(output.SequenceEqual(input));
        }

        [TestMethod, TestCategory("Gated")]
        public void Level1InLevel3OutTrivialColumnar()
        {
            var input = Enumerable.Range(0, 1000).ToList();
            var subject = new Subject<int>();
            var output = new List<StreamEvent<int>>();

            var outputAwait = subject.ToAtemporalStreamable(TimelinePolicy.Sequence(100)).ToStreamEventObservable().Where(o => o.IsData).ForEachAsync(o => output.Add(o));
            input.ForEach(o => subject.OnNext(o));
            subject.OnCompleted();
            outputAwait.Wait();

            Assert.IsTrue(output.Select(o => o.Payload).SequenceEqual(input));
            Assert.IsTrue(output.All(o => o.EndTime == StreamEvent.InfinitySyncTime));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnar()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                null,
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnar()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                null,
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarTime : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarTime() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                null,
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                null,
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarDrop : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarDrop() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarDrop()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Drop(0),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarDrop()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Drop(0),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarDrop10 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarDrop10() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarDrop10()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Drop(10),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarDrop10()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Drop(10),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarDrop1000 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarDrop1000() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarDrop1000()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Drop(1000),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarDrop1000()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Drop(1000),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarDropTime : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarDropTime() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarDropTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Drop(0),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarDropTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Drop(0),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarDrop10Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarDrop10Time() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarDrop10Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Drop(10),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarDrop10Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Drop(10),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarDrop1000Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarDrop1000Time() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarDrop1000Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Drop(1000),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarDrop1000Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Drop(1000),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarThrow : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarThrow() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarThrow()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Throw(0),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarThrow()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Throw(0),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarThrow10 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarThrow10() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarThrow10()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Throw(10),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarThrow10()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Throw(10),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarThrow1000 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarThrow1000() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarThrow1000()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Throw(1000),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarThrow1000()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Throw(1000),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarThrowTime : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarThrowTime() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarThrowTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Throw(0),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarThrowTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Throw(0),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarThrow10Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarThrow10Time() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarThrow10Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Throw(10),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarThrow10Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Throw(10),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarThrow1000Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarThrow1000Time() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarThrow1000Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Throw(1000),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarThrow1000Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Throw(1000),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarAdjust : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarAdjust() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarAdjust()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Adjust(0),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarAdjust()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Adjust(0),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarAdjust10 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarAdjust10() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarAdjust10()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Adjust(10),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarAdjust10()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Adjust(10),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarAdjust1000 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarAdjust1000() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarAdjust1000()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Adjust(1000),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarAdjust1000()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Adjust(1000),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarAdjustTime : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarAdjustTime() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarAdjustTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Adjust(0),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarAdjustTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Adjust(0),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarAdjust10Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarAdjust10Time() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarAdjust10Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Adjust(10),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarAdjust10Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Adjust(10),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarAdjust1000Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarAdjust1000Time() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarAdjust1000Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Adjust(1000),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarAdjust1000Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Adjust(1000),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarSmallBatch : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarSmallBatch() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void Level1InLevel1OutTrivialColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, 1000).ToList();
            var subject = new Subject<int>();
            var prog = subject.ToAtemporalStreamable(TimelinePolicy.Sequence(100)).ToEnumerable();
            input.ForEach(o => subject.OnNext(o));
            subject.OnCompleted();
            while (!prog.Completed) { }
            var output = prog.OrderBy(o => o);

            Assert.IsTrue(output.SequenceEqual(input));
        }

        [TestMethod, TestCategory("Gated")]
        public void Level1InLevel3OutTrivialColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, 1000).ToList();
            var subject = new Subject<int>();
            var output = new List<StreamEvent<int>>();

            var outputAwait = subject.ToAtemporalStreamable(TimelinePolicy.Sequence(100)).ToStreamEventObservable().Where(o => o.IsData).ForEachAsync(o => output.Add(o));
            input.ForEach(o => subject.OnNext(o));
            subject.OnCompleted();
            outputAwait.Wait();

            Assert.IsTrue(output.Select(o => o.Payload).SequenceEqual(input));
            Assert.IsTrue(output.All(o => o.EndTime == StreamEvent.InfinitySyncTime));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarSmallBatch()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                null,
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarSmallBatch()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                null,
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarSmallBatchTime : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarSmallBatchTime() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarSmallBatchTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                null,
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarSmallBatchTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                null,
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarSmallBatchDrop : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarSmallBatchDrop() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarSmallBatchDrop()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Drop(0),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarSmallBatchDrop()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Drop(0),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarSmallBatchDrop10 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarSmallBatchDrop10() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarSmallBatchDrop10()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Drop(10),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarSmallBatchDrop10()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Drop(10),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarSmallBatchDrop1000 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarSmallBatchDrop1000() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarSmallBatchDrop1000()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Drop(1000),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarSmallBatchDrop1000()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Drop(1000),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarSmallBatchDropTime : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarSmallBatchDropTime() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarSmallBatchDropTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Drop(0),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarSmallBatchDropTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Drop(0),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarSmallBatchDrop10Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarSmallBatchDrop10Time() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarSmallBatchDrop10Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Drop(10),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarSmallBatchDrop10Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Drop(10),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarSmallBatchDrop1000Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarSmallBatchDrop1000Time() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarSmallBatchDrop1000Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Drop(1000),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarSmallBatchDrop1000Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Drop(1000),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarSmallBatchThrow : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarSmallBatchThrow() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarSmallBatchThrow()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Throw(0),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarSmallBatchThrow()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Throw(0),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarSmallBatchThrow10 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarSmallBatchThrow10() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarSmallBatchThrow10()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Throw(10),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarSmallBatchThrow10()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Throw(10),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarSmallBatchThrow1000 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarSmallBatchThrow1000() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarSmallBatchThrow1000()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Throw(1000),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarSmallBatchThrow1000()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Throw(1000),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarSmallBatchThrowTime : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarSmallBatchThrowTime() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarSmallBatchThrowTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Throw(0),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarSmallBatchThrowTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Throw(0),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarSmallBatchThrow10Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarSmallBatchThrow10Time() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarSmallBatchThrow10Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Throw(10),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarSmallBatchThrow10Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Throw(10),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarSmallBatchThrow1000Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarSmallBatchThrow1000Time() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarSmallBatchThrow1000Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Throw(1000),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarSmallBatchThrow1000Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Throw(1000),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarSmallBatchAdjust : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarSmallBatchAdjust() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarSmallBatchAdjust()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Adjust(0),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarSmallBatchAdjust()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Adjust(0),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarSmallBatchAdjust10 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarSmallBatchAdjust10() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarSmallBatchAdjust10()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Adjust(10),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarSmallBatchAdjust10()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Adjust(10),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarSmallBatchAdjust1000 : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarSmallBatchAdjust1000() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarSmallBatchAdjust1000()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Adjust(1000),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarSmallBatchAdjust1000()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Adjust(1000),
                FlushPolicy.FlushOnPunctuation,
                null).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarSmallBatchAdjustTime : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarSmallBatchAdjustTime() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarSmallBatchAdjustTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Adjust(0),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarSmallBatchAdjustTime()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Adjust(0),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarSmallBatchAdjust10Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarSmallBatchAdjust10Time() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarSmallBatchAdjust10Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Adjust(10),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarSmallBatchAdjust10Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Adjust(10),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }

    [TestClass]
    public class IngressAndEgressTestsColumnarSmallBatchAdjust1000Time : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public IngressAndEgressTestsColumnarSmallBatchAdjust1000Time() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeTestColumnarSmallBatchAdjust1000Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                DisorderPolicy.Adjust(1000),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateStart(i, i))));
        }

        [TestMethod, TestCategory("Gated")]
        public void IntervalIngressTestColumnarSmallBatchAdjust1000Time()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var prog = input.ToObservable().ToTemporalStreamable(
                o => (long)o,
                o => (long)o + 5,
                DisorderPolicy.Adjust(1000),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(10)).ToStreamEventObservable();

            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(punctuations.SequenceEqual(input.Where(i => i % 10 == 0).Select(i => StreamEvent.CreatePunctuation<int>((long)i))));
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }
    }
}
