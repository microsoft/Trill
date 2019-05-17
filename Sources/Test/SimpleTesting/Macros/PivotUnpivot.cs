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
    internal struct UnpivotStruct1
    {
        public string Key1;
        public string Attribute;
        public int Value;
    }

    internal struct UnpivotStruct1N
    {
        public string Key1;
        public string Attribute;
        public int? Value;
    }

    internal struct UnpivotStruct2
    {
        public string Key1;
        public string Key2;
        public string Attribute;
        public int Value;
    }

    internal struct UnpivotStruct2N
    {
        public string Key1;
        public string Key2;
        public string Attribute;
        public int? Value;
    }

    [TestClass]
    public class UnpivotTestsRow : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public UnpivotTestsRow() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(false)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.None))
        { }

        [TestMethod, TestCategory("Gated")]
        public void Unpivot1AllRow()
        {
            var input = WideStruct1.GetTestData()
                .ToStreamable()
                .Unpivot(
                    () => new UnpivotStruct1(),
                    e => e.Key1,
                    e => e.Attribute,
                    e => e.Value)
                .ToAtemporalObservable()
                .ToEnumerable()
                .ToList();

            var expected = new List<UnpivotStruct1>
            {
                new UnpivotStruct1 { Key1 = "a", Attribute = "i5", Value = 5 },
                new UnpivotStruct1 { Key1 = "a", Attribute = "i9", Value = 9 },
                new UnpivotStruct1 { Key1 = "b", Attribute = "i1", Value = 11 },
                new UnpivotStruct1 { Key1 = "b", Attribute = "i14", Value = 100 },
                new UnpivotStruct1 { Key1 = "b", Attribute = "i2", Value = 9 },
                new UnpivotStruct1 { Key1 = "c", Attribute = "i15", Value = 0 },
                new UnpivotStruct1 { Key1 = "c", Attribute = "o", Value = 9 },
            };

            Assert.IsTrue(input.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void Unpivot2AllRow()
        {
            var input = WideStruct2.GetTestData()
                .ToStreamable()
                .Unpivot(
                    () => new UnpivotStruct2(),
                    e => new { e.Key1, e.Key2 },
                    e => e.Attribute,
                    e => e.Value)
                .ToAtemporalObservable()
                .ToEnumerable()
                .ToList();

            var expected = new List<UnpivotStruct2>
            {
                new UnpivotStruct2 { Key1 = "a", Key2 = "d", Attribute = "i5", Value = 5 },
                new UnpivotStruct2 { Key1 = "a", Key2 = "d", Attribute = "i9", Value = 9 },
                new UnpivotStruct2 { Key1 = "b", Key2 = "d", Attribute = "i1", Value = 11 },
                new UnpivotStruct2 { Key1 = "b", Key2 = "d", Attribute = "i14", Value = 100 },
                new UnpivotStruct2 { Key1 = "b", Key2 = "d", Attribute = "i2", Value = 9 },
                new UnpivotStruct2 { Key1 = "c", Key2 = "e", Attribute = "i15", Value = 0 },
                new UnpivotStruct2 { Key1 = "c", Key2 = "e", Attribute = "o", Value = 9 },
            };

            Assert.IsTrue(input.SequenceEqual(expected));
        }
    }

    [TestClass]
    public class PivotTestsRow : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public PivotTestsRow() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(false)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.None))
        { }

        [TestMethod, TestCategory("Gated")]
        public void Pivot1AllRow()
        {
            var input = new List<UnpivotStruct1N>
            {
                new UnpivotStruct1N { Key1 = "a", Attribute = "i5", Value = 5 },
                new UnpivotStruct1N { Key1 = "a", Attribute = "i9", Value = 9 },
                new UnpivotStruct1N { Key1 = "b", Attribute = "i1", Value = 11 },
                new UnpivotStruct1N { Key1 = "b", Attribute = "i2", Value = 9 },
                new UnpivotStruct1N { Key1 = "b", Attribute = "i14", Value = 100 },
                new UnpivotStruct1N { Key1 = "c", Attribute = "i15", Value = 0 },
                new UnpivotStruct1N { Key1 = "c", Attribute = "o", Value = 9 },
            }.ToStreamable()
            .Pivot(
                () => new WideStruct1(),
                o => o.Key1,
                o => o.Attribute,
                o => o.Value,
                o => o.SingleOrDefault())
            .ToAtemporalObservable()
            .ToEnumerable()
            .ToList();

            var expected = WideStruct1.GetTestData();

            Assert.IsTrue(input.OrderBy(o => o.Key1).SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void Pivot2AllRow()
        {
            var input = new List<UnpivotStruct2N>
            {
                new UnpivotStruct2N { Key1 = "a", Key2 = "d", Attribute = "i5", Value = 5 },
                new UnpivotStruct2N { Key1 = "a", Key2 = "d", Attribute = "i9", Value = 9 },
                new UnpivotStruct2N { Key1 = "b", Key2 = "d", Attribute = "i1", Value = 11 },
                new UnpivotStruct2N { Key1 = "b", Key2 = "d", Attribute = "i14", Value = 100 },
                new UnpivotStruct2N { Key1 = "b", Key2 = "d", Attribute = "i2", Value = 9 },
                new UnpivotStruct2N { Key1 = "c", Key2 = "e", Attribute = "i15", Value = 0 },
                new UnpivotStruct2N { Key1 = "c", Key2 = "e", Attribute = "o", Value = 9 },
            }.ToStreamable()
            .Pivot(
                () => new WideStruct2(),
                o => new { o.Key1, o.Key2 },
                o => o.Attribute,
                o => o.Value,
                o => o.SingleOrDefault())
            .ToAtemporalObservable()
            .ToEnumerable()
            .ToList();

            var expected = WideStruct2.GetTestData();

            Assert.IsTrue(input.OrderBy(o => o.Key1).SequenceEqual(expected));
        }
    }

    [TestClass]
    public class UnpivotTestsRowSmallBatch : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public UnpivotTestsRowSmallBatch() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(false)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.None))
        { }

        [TestMethod, TestCategory("Gated")]
        public void Unpivot1AllRowSmallBatch()
        {
            var input = WideStruct1.GetTestData()
                .ToStreamable()
                .Unpivot(
                    () => new UnpivotStruct1(),
                    e => e.Key1,
                    e => e.Attribute,
                    e => e.Value)
                .ToAtemporalObservable()
                .ToEnumerable()
                .ToList();

            var expected = new List<UnpivotStruct1>
            {
                new UnpivotStruct1 { Key1 = "a", Attribute = "i5", Value = 5 },
                new UnpivotStruct1 { Key1 = "a", Attribute = "i9", Value = 9 },
                new UnpivotStruct1 { Key1 = "b", Attribute = "i1", Value = 11 },
                new UnpivotStruct1 { Key1 = "b", Attribute = "i14", Value = 100 },
                new UnpivotStruct1 { Key1 = "b", Attribute = "i2", Value = 9 },
                new UnpivotStruct1 { Key1 = "c", Attribute = "i15", Value = 0 },
                new UnpivotStruct1 { Key1 = "c", Attribute = "o", Value = 9 },
            };

            Assert.IsTrue(input.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void Unpivot2AllRowSmallBatch()
        {
            var input = WideStruct2.GetTestData()
                .ToStreamable()
                .Unpivot(
                    () => new UnpivotStruct2(),
                    e => new { e.Key1, e.Key2 },
                    e => e.Attribute,
                    e => e.Value)
                .ToAtemporalObservable()
                .ToEnumerable()
                .ToList();

            var expected = new List<UnpivotStruct2>
            {
                new UnpivotStruct2 { Key1 = "a", Key2 = "d", Attribute = "i5", Value = 5 },
                new UnpivotStruct2 { Key1 = "a", Key2 = "d", Attribute = "i9", Value = 9 },
                new UnpivotStruct2 { Key1 = "b", Key2 = "d", Attribute = "i1", Value = 11 },
                new UnpivotStruct2 { Key1 = "b", Key2 = "d", Attribute = "i14", Value = 100 },
                new UnpivotStruct2 { Key1 = "b", Key2 = "d", Attribute = "i2", Value = 9 },
                new UnpivotStruct2 { Key1 = "c", Key2 = "e", Attribute = "i15", Value = 0 },
                new UnpivotStruct2 { Key1 = "c", Key2 = "e", Attribute = "o", Value = 9 },
            };

            Assert.IsTrue(input.SequenceEqual(expected));
        }
    }

    [TestClass]
    public class PivotTestsRowSmallBatch : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public PivotTestsRowSmallBatch() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(false)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.None))
        { }

        [TestMethod, TestCategory("Gated")]
        public void Pivot1AllRowSmallBatch()
        {
            var input = new List<UnpivotStruct1N>
            {
                new UnpivotStruct1N { Key1 = "a", Attribute = "i5", Value = 5 },
                new UnpivotStruct1N { Key1 = "a", Attribute = "i9", Value = 9 },
                new UnpivotStruct1N { Key1 = "b", Attribute = "i1", Value = 11 },
                new UnpivotStruct1N { Key1 = "b", Attribute = "i2", Value = 9 },
                new UnpivotStruct1N { Key1 = "b", Attribute = "i14", Value = 100 },
                new UnpivotStruct1N { Key1 = "c", Attribute = "i15", Value = 0 },
                new UnpivotStruct1N { Key1 = "c", Attribute = "o", Value = 9 },
            }.ToStreamable()
            .Pivot(
                () => new WideStruct1(),
                o => o.Key1,
                o => o.Attribute,
                o => o.Value,
                o => o.SingleOrDefault())
            .ToAtemporalObservable()
            .ToEnumerable()
            .ToList();

            var expected = WideStruct1.GetTestData();

            Assert.IsTrue(input.OrderBy(o => o.Key1).SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void Pivot2AllRowSmallBatch()
        {
            var input = new List<UnpivotStruct2N>
            {
                new UnpivotStruct2N { Key1 = "a", Key2 = "d", Attribute = "i5", Value = 5 },
                new UnpivotStruct2N { Key1 = "a", Key2 = "d", Attribute = "i9", Value = 9 },
                new UnpivotStruct2N { Key1 = "b", Key2 = "d", Attribute = "i1", Value = 11 },
                new UnpivotStruct2N { Key1 = "b", Key2 = "d", Attribute = "i14", Value = 100 },
                new UnpivotStruct2N { Key1 = "b", Key2 = "d", Attribute = "i2", Value = 9 },
                new UnpivotStruct2N { Key1 = "c", Key2 = "e", Attribute = "i15", Value = 0 },
                new UnpivotStruct2N { Key1 = "c", Key2 = "e", Attribute = "o", Value = 9 },
            }.ToStreamable()
            .Pivot(
                () => new WideStruct2(),
                o => new { o.Key1, o.Key2 },
                o => o.Attribute,
                o => o.Value,
                o => o.SingleOrDefault())
            .ToAtemporalObservable()
            .ToEnumerable()
            .ToList();

            var expected = WideStruct2.GetTestData();

            Assert.IsTrue(input.OrderBy(o => o.Key1).SequenceEqual(expected));
        }
    }

    [TestClass]
    public class UnpivotTestsColumnar : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public UnpivotTestsColumnar() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(false)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.None))
        { }

        [TestMethod, TestCategory("Gated")]
        public void Unpivot1AllColumnar()
        {
            var input = WideStruct1.GetTestData()
                .ToStreamable()
                .Unpivot(
                    () => new UnpivotStruct1(),
                    e => e.Key1,
                    e => e.Attribute,
                    e => e.Value)
                .ToAtemporalObservable()
                .ToEnumerable()
                .ToList();

            var expected = new List<UnpivotStruct1>
            {
                new UnpivotStruct1 { Key1 = "a", Attribute = "i5", Value = 5 },
                new UnpivotStruct1 { Key1 = "a", Attribute = "i9", Value = 9 },
                new UnpivotStruct1 { Key1 = "b", Attribute = "i1", Value = 11 },
                new UnpivotStruct1 { Key1 = "b", Attribute = "i14", Value = 100 },
                new UnpivotStruct1 { Key1 = "b", Attribute = "i2", Value = 9 },
                new UnpivotStruct1 { Key1 = "c", Attribute = "i15", Value = 0 },
                new UnpivotStruct1 { Key1 = "c", Attribute = "o", Value = 9 },
            };

            Assert.IsTrue(input.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void Unpivot2AllColumnar()
        {
            var input = WideStruct2.GetTestData()
                .ToStreamable()
                .Unpivot(
                    () => new UnpivotStruct2(),
                    e => new { e.Key1, e.Key2 },
                    e => e.Attribute,
                    e => e.Value)
                .ToAtemporalObservable()
                .ToEnumerable()
                .ToList();

            var expected = new List<UnpivotStruct2>
            {
                new UnpivotStruct2 { Key1 = "a", Key2 = "d", Attribute = "i5", Value = 5 },
                new UnpivotStruct2 { Key1 = "a", Key2 = "d", Attribute = "i9", Value = 9 },
                new UnpivotStruct2 { Key1 = "b", Key2 = "d", Attribute = "i1", Value = 11 },
                new UnpivotStruct2 { Key1 = "b", Key2 = "d", Attribute = "i14", Value = 100 },
                new UnpivotStruct2 { Key1 = "b", Key2 = "d", Attribute = "i2", Value = 9 },
                new UnpivotStruct2 { Key1 = "c", Key2 = "e", Attribute = "i15", Value = 0 },
                new UnpivotStruct2 { Key1 = "c", Key2 = "e", Attribute = "o", Value = 9 },
            };

            Assert.IsTrue(input.SequenceEqual(expected));
        }
    }

    [TestClass]
    public class PivotTestsColumnar : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public PivotTestsColumnar() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(false)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.None))
        { }

        [TestMethod, TestCategory("Gated")]
        public void Pivot1AllColumnar()
        {
            var input = new List<UnpivotStruct1N>
            {
                new UnpivotStruct1N { Key1 = "a", Attribute = "i5", Value = 5 },
                new UnpivotStruct1N { Key1 = "a", Attribute = "i9", Value = 9 },
                new UnpivotStruct1N { Key1 = "b", Attribute = "i1", Value = 11 },
                new UnpivotStruct1N { Key1 = "b", Attribute = "i2", Value = 9 },
                new UnpivotStruct1N { Key1 = "b", Attribute = "i14", Value = 100 },
                new UnpivotStruct1N { Key1 = "c", Attribute = "i15", Value = 0 },
                new UnpivotStruct1N { Key1 = "c", Attribute = "o", Value = 9 },
            }.ToStreamable()
            .Pivot(
                () => new WideStruct1(),
                o => o.Key1,
                o => o.Attribute,
                o => o.Value,
                o => o.SingleOrDefault())
            .ToAtemporalObservable()
            .ToEnumerable()
            .ToList();

            var expected = WideStruct1.GetTestData();

            Assert.IsTrue(input.OrderBy(o => o.Key1).SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void Pivot2AllColumnar()
        {
            var input = new List<UnpivotStruct2N>
            {
                new UnpivotStruct2N { Key1 = "a", Key2 = "d", Attribute = "i5", Value = 5 },
                new UnpivotStruct2N { Key1 = "a", Key2 = "d", Attribute = "i9", Value = 9 },
                new UnpivotStruct2N { Key1 = "b", Key2 = "d", Attribute = "i1", Value = 11 },
                new UnpivotStruct2N { Key1 = "b", Key2 = "d", Attribute = "i14", Value = 100 },
                new UnpivotStruct2N { Key1 = "b", Key2 = "d", Attribute = "i2", Value = 9 },
                new UnpivotStruct2N { Key1 = "c", Key2 = "e", Attribute = "i15", Value = 0 },
                new UnpivotStruct2N { Key1 = "c", Key2 = "e", Attribute = "o", Value = 9 },
            }.ToStreamable()
            .Pivot(
                () => new WideStruct2(),
                o => new { o.Key1, o.Key2 },
                o => o.Attribute,
                o => o.Value,
                o => o.SingleOrDefault())
            .ToAtemporalObservable()
            .ToEnumerable()
            .ToList();

            var expected = WideStruct2.GetTestData();

            Assert.IsTrue(input.OrderBy(o => o.Key1).SequenceEqual(expected));
        }
    }

    [TestClass]
    public class UnpivotTestsColumnarSmallBatch : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public UnpivotTestsColumnarSmallBatch() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(false)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.None))
        { }

        [TestMethod, TestCategory("Gated")]
        public void Unpivot1AllColumnarSmallBatch()
        {
            var input = WideStruct1.GetTestData()
                .ToStreamable()
                .Unpivot(
                    () => new UnpivotStruct1(),
                    e => e.Key1,
                    e => e.Attribute,
                    e => e.Value)
                .ToAtemporalObservable()
                .ToEnumerable()
                .ToList();

            var expected = new List<UnpivotStruct1>
            {
                new UnpivotStruct1 { Key1 = "a", Attribute = "i5", Value = 5 },
                new UnpivotStruct1 { Key1 = "a", Attribute = "i9", Value = 9 },
                new UnpivotStruct1 { Key1 = "b", Attribute = "i1", Value = 11 },
                new UnpivotStruct1 { Key1 = "b", Attribute = "i14", Value = 100 },
                new UnpivotStruct1 { Key1 = "b", Attribute = "i2", Value = 9 },
                new UnpivotStruct1 { Key1 = "c", Attribute = "i15", Value = 0 },
                new UnpivotStruct1 { Key1 = "c", Attribute = "o", Value = 9 },
            };

            Assert.IsTrue(input.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void Unpivot2AllColumnarSmallBatch()
        {
            var input = WideStruct2.GetTestData()
                .ToStreamable()
                .Unpivot(
                    () => new UnpivotStruct2(),
                    e => new { e.Key1, e.Key2 },
                    e => e.Attribute,
                    e => e.Value)
                .ToAtemporalObservable()
                .ToEnumerable()
                .ToList();

            var expected = new List<UnpivotStruct2>
            {
                new UnpivotStruct2 { Key1 = "a", Key2 = "d", Attribute = "i5", Value = 5 },
                new UnpivotStruct2 { Key1 = "a", Key2 = "d", Attribute = "i9", Value = 9 },
                new UnpivotStruct2 { Key1 = "b", Key2 = "d", Attribute = "i1", Value = 11 },
                new UnpivotStruct2 { Key1 = "b", Key2 = "d", Attribute = "i14", Value = 100 },
                new UnpivotStruct2 { Key1 = "b", Key2 = "d", Attribute = "i2", Value = 9 },
                new UnpivotStruct2 { Key1 = "c", Key2 = "e", Attribute = "i15", Value = 0 },
                new UnpivotStruct2 { Key1 = "c", Key2 = "e", Attribute = "o", Value = 9 },
            };

            Assert.IsTrue(input.SequenceEqual(expected));
        }
    }

    [TestClass]
    public class PivotTestsColumnarSmallBatch : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public PivotTestsColumnarSmallBatch() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(false)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.None))
        { }

        [TestMethod, TestCategory("Gated")]
        public void Pivot1AllColumnarSmallBatch()
        {
            var input = new List<UnpivotStruct1N>
            {
                new UnpivotStruct1N { Key1 = "a", Attribute = "i5", Value = 5 },
                new UnpivotStruct1N { Key1 = "a", Attribute = "i9", Value = 9 },
                new UnpivotStruct1N { Key1 = "b", Attribute = "i1", Value = 11 },
                new UnpivotStruct1N { Key1 = "b", Attribute = "i2", Value = 9 },
                new UnpivotStruct1N { Key1 = "b", Attribute = "i14", Value = 100 },
                new UnpivotStruct1N { Key1 = "c", Attribute = "i15", Value = 0 },
                new UnpivotStruct1N { Key1 = "c", Attribute = "o", Value = 9 },
            }.ToStreamable()
            .Pivot(
                () => new WideStruct1(),
                o => o.Key1,
                o => o.Attribute,
                o => o.Value,
                o => o.SingleOrDefault())
            .ToAtemporalObservable()
            .ToEnumerable()
            .ToList();

            var expected = WideStruct1.GetTestData();

            Assert.IsTrue(input.OrderBy(o => o.Key1).SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void Pivot2AllColumnarSmallBatch()
        {
            var input = new List<UnpivotStruct2N>
            {
                new UnpivotStruct2N { Key1 = "a", Key2 = "d", Attribute = "i5", Value = 5 },
                new UnpivotStruct2N { Key1 = "a", Key2 = "d", Attribute = "i9", Value = 9 },
                new UnpivotStruct2N { Key1 = "b", Key2 = "d", Attribute = "i1", Value = 11 },
                new UnpivotStruct2N { Key1 = "b", Key2 = "d", Attribute = "i14", Value = 100 },
                new UnpivotStruct2N { Key1 = "b", Key2 = "d", Attribute = "i2", Value = 9 },
                new UnpivotStruct2N { Key1 = "c", Key2 = "e", Attribute = "i15", Value = 0 },
                new UnpivotStruct2N { Key1 = "c", Key2 = "e", Attribute = "o", Value = 9 },
            }.ToStreamable()
            .Pivot(
                () => new WideStruct2(),
                o => new { o.Key1, o.Key2 },
                o => o.Attribute,
                o => o.Value,
                o => o.SingleOrDefault())
            .ToAtemporalObservable()
            .ToEnumerable()
            .ToList();

            var expected = WideStruct2.GetTestData();

            Assert.IsTrue(input.OrderBy(o => o.Key1).SequenceEqual(expected));
        }
    }
}
