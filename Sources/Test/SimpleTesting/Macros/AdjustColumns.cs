// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class AdjustColumnsTestsRow : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public AdjustColumnsTestsRow() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(false)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.None))
        { }

        [TestMethod, TestCategory("Gated")]
        public void AdjustColumnsTrivialRow()
        {
            var input = WideStruct1.GetTestData()
                .ToStreamable()
                .Select(() => new WideStruct1())
                .ToAtemporalObservable()
                .ToEnumerable()
                .ToList();

            Assert.IsTrue(input.SequenceEqual(WideStruct1.GetTestData()));
        }

        [TestMethod, TestCategory("Gated")]
        public void AdjustColumnsDropRow()
        {
            var input = WideStruct2.GetTestData()
                .ToStreamable()
                .Select(() => new WideStruct1())
                .ToAtemporalObservable()
                .ToEnumerable()
                .ToList();

            Assert.IsTrue(input.SequenceEqual(WideStruct1.GetTestData()));
        }

        [TestMethod, TestCategory("Gated")]
        public void AdjustColumnsAddViaFormulaRow()
        {
            var input = WideStruct1.GetTestData()
                .ToStreamable()
                .Select(
                    () => new WideStruct2(),
                    o => o.Key2,
                    o => o.Key1 == "c" ? "e" : "d")
                .ToAtemporalObservable()
                .ToEnumerable()
                .ToList();

            Assert.IsTrue(input.SequenceEqual(WideStruct2.GetTestData()));
        }

        [TestMethod, TestCategory("Gated")]
        public void AdjustColumnsAddViaDictionaryRow()
        {
            var input = WideStruct1.GetTestData()
                .ToStreamable()
                .Select(() => new WideStruct2(), new Dictionary<string, Expression<Func<WideStruct1, object>>>
                    {
                        { "Key2", o => (o.Key1 == "c" ? "e" : "d") }
                    })
                .ToAtemporalObservable()
                .ToEnumerable()
                .ToList();

            Assert.IsTrue(input.SequenceEqual(WideStruct2.GetTestData()));
        }
    }

    [TestClass]
    public class AdjustColumnsTestsRowSmallBatch : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public AdjustColumnsTestsRowSmallBatch() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(false)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.None))
        { }

        [TestMethod, TestCategory("Gated")]
        public void AdjustColumnsTrivialRowSmallBatch()
        {
            var input = WideStruct1.GetTestData()
                .ToStreamable()
                .Select(() => new WideStruct1())
                .ToAtemporalObservable()
                .ToEnumerable()
                .ToList();

            Assert.IsTrue(input.SequenceEqual(WideStruct1.GetTestData()));
        }

        [TestMethod, TestCategory("Gated")]
        public void AdjustColumnsDropRowSmallBatch()
        {
            var input = WideStruct2.GetTestData()
                .ToStreamable()
                .Select(() => new WideStruct1())
                .ToAtemporalObservable()
                .ToEnumerable()
                .ToList();

            Assert.IsTrue(input.SequenceEqual(WideStruct1.GetTestData()));
        }

        [TestMethod, TestCategory("Gated")]
        public void AdjustColumnsAddViaFormulaRowSmallBatch()
        {
            var input = WideStruct1.GetTestData()
                .ToStreamable()
                .Select(
                    () => new WideStruct2(),
                    o => o.Key2,
                    o => o.Key1 == "c" ? "e" : "d")
                .ToAtemporalObservable()
                .ToEnumerable()
                .ToList();

            Assert.IsTrue(input.SequenceEqual(WideStruct2.GetTestData()));
        }

        [TestMethod, TestCategory("Gated")]
        public void AdjustColumnsAddViaDictionaryRowSmallBatch()
        {
            var input = WideStruct1.GetTestData()
                .ToStreamable()
                .Select(() => new WideStruct2(), new Dictionary<string, Expression<Func<WideStruct1, object>>>
                    {
                        { "Key2", o => (o.Key1 == "c" ? "e" : "d") }
                    })
                .ToAtemporalObservable()
                .ToEnumerable()
                .ToList();

            Assert.IsTrue(input.SequenceEqual(WideStruct2.GetTestData()));
        }
    }

    [TestClass]
    public class AdjustColumnsTestsColumnar : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public AdjustColumnsTestsColumnar() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(false)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.None))
        { }

        [TestMethod, TestCategory("Gated")]
        public void AdjustColumnsTrivialColumnar()
        {
            var input = WideStruct1.GetTestData()
                .ToStreamable()
                .Select(() => new WideStruct1())
                .ToAtemporalObservable()
                .ToEnumerable()
                .ToList();

            Assert.IsTrue(input.SequenceEqual(WideStruct1.GetTestData()));
        }

        [TestMethod, TestCategory("Gated")]
        public void AdjustColumnsDropColumnar()
        {
            var input = WideStruct2.GetTestData()
                .ToStreamable()
                .Select(() => new WideStruct1())
                .ToAtemporalObservable()
                .ToEnumerable()
                .ToList();

            Assert.IsTrue(input.SequenceEqual(WideStruct1.GetTestData()));
        }

        [TestMethod, TestCategory("Gated")]
        public void AdjustColumnsAddViaFormulaColumnar()
        {
            var input = WideStruct1.GetTestData()
                .ToStreamable()
                .Select(
                    () => new WideStruct2(),
                    o => o.Key2,
                    o => o.Key1 == "c" ? "e" : "d")
                .ToAtemporalObservable()
                .ToEnumerable()
                .ToList();

            Assert.IsTrue(input.SequenceEqual(WideStruct2.GetTestData()));
        }

        [TestMethod, TestCategory("Gated")]
        public void AdjustColumnsAddViaDictionaryColumnar()
        {
            var input = WideStruct1.GetTestData()
                .ToStreamable()
                .Select(() => new WideStruct2(), new Dictionary<string, Expression<Func<WideStruct1, object>>>
                    {
                        { "Key2", o => (o.Key1 == "c" ? "e" : "d") }
                    })
                .ToAtemporalObservable()
                .ToEnumerable()
                .ToList();

            Assert.IsTrue(input.SequenceEqual(WideStruct2.GetTestData()));
        }
    }

    [TestClass]
    public class AdjustColumnsTestsColumnarSmallBatch : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public AdjustColumnsTestsColumnarSmallBatch() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(false)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.None))
        { }

        [TestMethod, TestCategory("Gated")]
        public void AdjustColumnsTrivialColumnarSmallBatch()
        {
            var input = WideStruct1.GetTestData()
                .ToStreamable()
                .Select(() => new WideStruct1())
                .ToAtemporalObservable()
                .ToEnumerable()
                .ToList();

            Assert.IsTrue(input.SequenceEqual(WideStruct1.GetTestData()));
        }

        [TestMethod, TestCategory("Gated")]
        public void AdjustColumnsDropColumnarSmallBatch()
        {
            var input = WideStruct2.GetTestData()
                .ToStreamable()
                .Select(() => new WideStruct1())
                .ToAtemporalObservable()
                .ToEnumerable()
                .ToList();

            Assert.IsTrue(input.SequenceEqual(WideStruct1.GetTestData()));
        }

        [TestMethod, TestCategory("Gated")]
        public void AdjustColumnsAddViaFormulaColumnarSmallBatch()
        {
            var input = WideStruct1.GetTestData()
                .ToStreamable()
                .Select(
                    () => new WideStruct2(),
                    o => o.Key2,
                    o => o.Key1 == "c" ? "e" : "d")
                .ToAtemporalObservable()
                .ToEnumerable()
                .ToList();

            Assert.IsTrue(input.SequenceEqual(WideStruct2.GetTestData()));
        }

        [TestMethod, TestCategory("Gated")]
        public void AdjustColumnsAddViaDictionaryColumnarSmallBatch()
        {
            var input = WideStruct1.GetTestData()
                .ToStreamable()
                .Select(() => new WideStruct2(), new Dictionary<string, Expression<Func<WideStruct1, object>>>
                    {
                        { "Key2", o => (o.Key1 == "c" ? "e" : "d") }
                    })
                .ToAtemporalObservable()
                .ToEnumerable()
                .ToList();

            Assert.IsTrue(input.SequenceEqual(WideStruct2.GetTestData()));
        }
    }

}
