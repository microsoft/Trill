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

    [TestClass]
    public class QuantizeLifetimeTestsRow : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public QuantizeLifetimeTestsRow() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .MapArity(1)
            .ReduceArity(1))
        { }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingPassthroughRow()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10, o * 10 + 10, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(10, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(input));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingPointsRow()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o * 10, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10, o * 10 + 10, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(10, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingPointsStatelessRow()
        {
            var input = Enumerable.Range(0, 50000).Select(o => (long)o);

            var expected = Enumerable.Range(0, 50000).Select(o => (long)o)
                .Select(o => StreamEvent.CreateInterval(o * 10, o * 10 + 10, o));

            var inputStream = input.ToObservable().ToTemporalStreamable(o => o * 10, o => o * 10 + 1);
            var outputStream = inputStream.QuantizeLifetime(10, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingConsecutiveRow()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - (o % 10), o - (o % 10) + 10, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(10, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingProgressiveRow()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - (o % 5), o - (o % 10) + 10, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.ProgressiveQuantizeLifetime(10, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingPassthroughRow()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10, o * 10 + 100, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(100, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(input));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingPointsRow()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o * 10, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10, o * 10 + 100, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(100, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingPointsStatelessRow()
        {
            var input = Enumerable.Range(0, 50000).Select(o => (long)o);

            var expected = Enumerable.Range(0, 50000).Select(o => (long)o)
                .Select(o => StreamEvent.CreateInterval(o * 10, o * 10 + 100, o));

            var inputStream = input.ToObservable().ToTemporalStreamable(o => o * 10, o => o * 10 + 1);
            var outputStream = inputStream.QuantizeLifetime(100, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingConsecutiveRow()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - (o % 10), o - (o % 10) + 100, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(100, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingProgressiveRow()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - (o % 5), o - (o % 10) + 100, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.ProgressiveQuantizeLifetime(100, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingOffsetPassthroughRow()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10 + 5, o * 10 + 15, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(10, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(input));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingOffsetPointsRow()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o * 10 + 5, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10 + 5, o * 10 + 15, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(10, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingOffsetPointsStatelessRow()
        {
            var input = Enumerable.Range(0, 50000).Select(o => (long)o);

            var expected = Enumerable.Range(0, 50000).Select(o => (long)o)
                .Select(o => StreamEvent.CreateInterval(o * 10 + 5, o * 10 + 15, o));

            var inputStream = input.ToObservable().ToTemporalStreamable(o => o * 10 + 5, o => (o * 10 + 5) + 1);
            var outputStream = inputStream.QuantizeLifetime(10, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingOffsetConsecutiveRow()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - ((o + 5) % 10), o - ((o + 5) % 10) + 10, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(10, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingOffsetProgressiveRow()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - ((o + 5) % 5), o - ((o + 5) % 10) + 10, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.ProgressiveQuantizeLifetime(10, 10, 5, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingOffsetPassthroughRow()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10 + 5, o * 10 + 105, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(100, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(input));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingOffsetPointsRow()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o * 10 + 5, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10 + 5, o * 10 + 105, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(100, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingOffsetPointsStatelessRow()
        {
            var input = Enumerable.Range(0, 50000).Select(o => (long)o);

            var expected = Enumerable.Range(0, 50000).Select(o => (long)o)
                .Select(o => StreamEvent.CreateInterval(o * 10 + 5, o * 10 + 105, o));

            var inputStream = input.ToObservable().ToTemporalStreamable(o => o * 10 + 5, o => (o * 10 + 5) + 1);
            var outputStream = inputStream.QuantizeLifetime(100, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingOffsetConsecutiveRow()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - ((o + 5) % 10), o - ((o + 5) % 10) + 100, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(100, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingOffsetProgressiveRow()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - ((o + 5) % 5), o - ((o + 5) % 10) + 100, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.ProgressiveQuantizeLifetime(100, 10, 5, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }
    }

    [TestClass]
    public class QuantizeLifetimeTestsRowSmallBatch : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public QuantizeLifetimeTestsRowSmallBatch() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .MapArity(1)
            .ReduceArity(1))
        { }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingPassthroughRowSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10, o * 10 + 10, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(10, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(input));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingPointsRowSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o * 10, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10, o * 10 + 10, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(10, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingPointsStatelessRowSmallBatch()
        {
            var input = Enumerable.Range(0, 50000).Select(o => (long)o);

            var expected = Enumerable.Range(0, 50000).Select(o => (long)o)
                .Select(o => StreamEvent.CreateInterval(o * 10, o * 10 + 10, o));

            var inputStream = input.ToObservable().ToTemporalStreamable(o => o * 10, o => o * 10 + 1);
            var outputStream = inputStream.QuantizeLifetime(10, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingConsecutiveRowSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - (o % 10), o - (o % 10) + 10, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(10, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingProgressiveRowSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - (o % 5), o - (o % 10) + 10, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.ProgressiveQuantizeLifetime(10, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingPassthroughRowSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10, o * 10 + 100, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(100, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(input));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingPointsRowSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o * 10, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10, o * 10 + 100, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(100, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingPointsStatelessRowSmallBatch()
        {
            var input = Enumerable.Range(0, 50000).Select(o => (long)o);

            var expected = Enumerable.Range(0, 50000).Select(o => (long)o)
                .Select(o => StreamEvent.CreateInterval(o * 10, o * 10 + 100, o));

            var inputStream = input.ToObservable().ToTemporalStreamable(o => o * 10, o => o * 10 + 1);
            var outputStream = inputStream.QuantizeLifetime(100, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingConsecutiveRowSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - (o % 10), o - (o % 10) + 100, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(100, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingProgressiveRowSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - (o % 5), o - (o % 10) + 100, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.ProgressiveQuantizeLifetime(100, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingOffsetPassthroughRowSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10 + 5, o * 10 + 15, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(10, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(input));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingOffsetPointsRowSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o * 10 + 5, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10 + 5, o * 10 + 15, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(10, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingOffsetPointsStatelessRowSmallBatch()
        {
            var input = Enumerable.Range(0, 50000).Select(o => (long)o);

            var expected = Enumerable.Range(0, 50000).Select(o => (long)o)
                .Select(o => StreamEvent.CreateInterval(o * 10 + 5, o * 10 + 15, o));

            var inputStream = input.ToObservable().ToTemporalStreamable(o => o * 10 + 5, o => (o * 10 + 5) + 1);
            var outputStream = inputStream.QuantizeLifetime(10, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingOffsetConsecutiveRowSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - ((o + 5) % 10), o - ((o + 5) % 10) + 10, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(10, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingOffsetProgressiveRowSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - ((o + 5) % 5), o - ((o + 5) % 10) + 10, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.ProgressiveQuantizeLifetime(10, 10, 5, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingOffsetPassthroughRowSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10 + 5, o * 10 + 105, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(100, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(input));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingOffsetPointsRowSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o * 10 + 5, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10 + 5, o * 10 + 105, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(100, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingOffsetPointsStatelessRowSmallBatch()
        {
            var input = Enumerable.Range(0, 50000).Select(o => (long)o);

            var expected = Enumerable.Range(0, 50000).Select(o => (long)o)
                .Select(o => StreamEvent.CreateInterval(o * 10 + 5, o * 10 + 105, o));

            var inputStream = input.ToObservable().ToTemporalStreamable(o => o * 10 + 5, o => (o * 10 + 5) + 1);
            var outputStream = inputStream.QuantizeLifetime(100, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingOffsetConsecutiveRowSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - ((o + 5) % 10), o - ((o + 5) % 10) + 100, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(100, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingOffsetProgressiveRowSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - ((o + 5) % 5), o - ((o + 5) % 10) + 100, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.ProgressiveQuantizeLifetime(100, 10, 5, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }
    }

    [TestClass]
    public class QuantizeLifetimeTestsColumnar : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public QuantizeLifetimeTestsColumnar() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .MapArity(1)
            .ReduceArity(1))
        { }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingPassthroughColumnar()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10, o * 10 + 10, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(10, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(input));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingPointsColumnar()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o * 10, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10, o * 10 + 10, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(10, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingPointsStatelessColumnar()
        {
            var input = Enumerable.Range(0, 50000).Select(o => (long)o);

            var expected = Enumerable.Range(0, 50000).Select(o => (long)o)
                .Select(o => StreamEvent.CreateInterval(o * 10, o * 10 + 10, o));

            var inputStream = input.ToObservable().ToTemporalStreamable(o => o * 10, o => o * 10 + 1);
            var outputStream = inputStream.QuantizeLifetime(10, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingConsecutiveColumnar()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - (o % 10), o - (o % 10) + 10, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(10, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingProgressiveColumnar()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - (o % 5), o - (o % 10) + 10, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.ProgressiveQuantizeLifetime(10, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingPassthroughColumnar()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10, o * 10 + 100, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(100, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(input));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingPointsColumnar()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o * 10, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10, o * 10 + 100, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(100, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingPointsStatelessColumnar()
        {
            var input = Enumerable.Range(0, 50000).Select(o => (long)o);

            var expected = Enumerable.Range(0, 50000).Select(o => (long)o)
                .Select(o => StreamEvent.CreateInterval(o * 10, o * 10 + 100, o));

            var inputStream = input.ToObservable().ToTemporalStreamable(o => o * 10, o => o * 10 + 1);
            var outputStream = inputStream.QuantizeLifetime(100, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingConsecutiveColumnar()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - (o % 10), o - (o % 10) + 100, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(100, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingProgressiveColumnar()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - (o % 5), o - (o % 10) + 100, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.ProgressiveQuantizeLifetime(100, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingOffsetPassthroughColumnar()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10 + 5, o * 10 + 15, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(10, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(input));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingOffsetPointsColumnar()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o * 10 + 5, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10 + 5, o * 10 + 15, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(10, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingOffsetPointsStatelessColumnar()
        {
            var input = Enumerable.Range(0, 50000).Select(o => (long)o);

            var expected = Enumerable.Range(0, 50000).Select(o => (long)o)
                .Select(o => StreamEvent.CreateInterval(o * 10 + 5, o * 10 + 15, o));

            var inputStream = input.ToObservable().ToTemporalStreamable(o => o * 10 + 5, o => (o * 10 + 5) + 1);
            var outputStream = inputStream.QuantizeLifetime(10, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingOffsetConsecutiveColumnar()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - ((o + 5) % 10), o - ((o + 5) % 10) + 10, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(10, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingOffsetProgressiveColumnar()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - ((o + 5) % 5), o - ((o + 5) % 10) + 10, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.ProgressiveQuantizeLifetime(10, 10, 5, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingOffsetPassthroughColumnar()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10 + 5, o * 10 + 105, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(100, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(input));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingOffsetPointsColumnar()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o * 10 + 5, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10 + 5, o * 10 + 105, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(100, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingOffsetPointsStatelessColumnar()
        {
            var input = Enumerable.Range(0, 50000).Select(o => (long)o);

            var expected = Enumerable.Range(0, 50000).Select(o => (long)o)
                .Select(o => StreamEvent.CreateInterval(o * 10 + 5, o * 10 + 105, o));

            var inputStream = input.ToObservable().ToTemporalStreamable(o => o * 10 + 5, o => (o * 10 + 5) + 1);
            var outputStream = inputStream.QuantizeLifetime(100, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingOffsetConsecutiveColumnar()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - ((o + 5) % 10), o - ((o + 5) % 10) + 100, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(100, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingOffsetProgressiveColumnar()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - ((o + 5) % 5), o - ((o + 5) % 10) + 100, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.ProgressiveQuantizeLifetime(100, 10, 5, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }
    }

    [TestClass]
    public class QuantizeLifetimeTestsColumnarSmallBatch : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public QuantizeLifetimeTestsColumnarSmallBatch() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .MapArity(1)
            .ReduceArity(1))
        { }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingPassthroughColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10, o * 10 + 10, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(10, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(input));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingPointsColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o * 10, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10, o * 10 + 10, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(10, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingPointsStatelessColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, 50000).Select(o => (long)o);

            var expected = Enumerable.Range(0, 50000).Select(o => (long)o)
                .Select(o => StreamEvent.CreateInterval(o * 10, o * 10 + 10, o));

            var inputStream = input.ToObservable().ToTemporalStreamable(o => o * 10, o => o * 10 + 1);
            var outputStream = inputStream.QuantizeLifetime(10, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingConsecutiveColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - (o % 10), o - (o % 10) + 10, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(10, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingProgressiveColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - (o % 5), o - (o % 10) + 10, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.ProgressiveQuantizeLifetime(10, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingPassthroughColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10, o * 10 + 100, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(100, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(input));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingPointsColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o * 10, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10, o * 10 + 100, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(100, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingPointsStatelessColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, 50000).Select(o => (long)o);

            var expected = Enumerable.Range(0, 50000).Select(o => (long)o)
                .Select(o => StreamEvent.CreateInterval(o * 10, o * 10 + 100, o));

            var inputStream = input.ToObservable().ToTemporalStreamable(o => o * 10, o => o * 10 + 1);
            var outputStream = inputStream.QuantizeLifetime(100, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingConsecutiveColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - (o % 10), o - (o % 10) + 100, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(100, 10);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingProgressiveColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - (o % 5), o - (o % 10) + 100, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.ProgressiveQuantizeLifetime(100, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingOffsetPassthroughColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10 + 5, o * 10 + 15, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(10, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(input));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingOffsetPointsColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o * 10 + 5, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10 + 5, o * 10 + 15, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(10, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingOffsetPointsStatelessColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, 50000).Select(o => (long)o);

            var expected = Enumerable.Range(0, 50000).Select(o => (long)o)
                .Select(o => StreamEvent.CreateInterval(o * 10 + 5, o * 10 + 15, o));

            var inputStream = input.ToObservable().ToTemporalStreamable(o => o * 10 + 5, o => (o * 10 + 5) + 1);
            var outputStream = inputStream.QuantizeLifetime(10, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingOffsetConsecutiveColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - ((o + 5) % 10), o - ((o + 5) % 10) + 10, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(10, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeTumblingOffsetProgressiveColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - ((o + 5) % 5), o - ((o + 5) % 10) + 10, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.ProgressiveQuantizeLifetime(10, 10, 5, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingOffsetPassthroughColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10 + 5, o * 10 + 105, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(100, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(input));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingOffsetPointsColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o * 10 + 5, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o * 10 + 5, o * 10 + 105, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(100, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingOffsetPointsStatelessColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, 50000).Select(o => (long)o);

            var expected = Enumerable.Range(0, 50000).Select(o => (long)o)
                .Select(o => StreamEvent.CreateInterval(o * 10 + 5, o * 10 + 105, o));

            var inputStream = input.ToObservable().ToTemporalStreamable(o => o * 10 + 5, o => (o * 10 + 5) + 1);
            var outputStream = inputStream.QuantizeLifetime(100, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingOffsetConsecutiveColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - ((o + 5) % 10), o - ((o + 5) % 10) + 100, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.QuantizeLifetime(100, 10, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void QuantizeLifetimeHoppingOffsetProgressiveColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreatePoint(o, o));

            var expected = Enumerable.Range(0, 50000)
                .Select(o => StreamEvent.CreateInterval(o - ((o + 5) % 5), o - ((o + 5) % 10) + 100, o));

            var inputStream = input.ToObservable().ToStreamable();
            var outputStream = inputStream.ProgressiveQuantizeLifetime(100, 10, 5, 5);
            var output = outputStream.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(expected));
        }
    }
}