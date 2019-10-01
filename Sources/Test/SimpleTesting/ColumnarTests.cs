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
using Microsoft.VisualStudio.TestTools.UnitTesting;

// Tests covering columnar specific consideratations
namespace SimpleTesting.ColumnarTests
{
    public class ColumnarTestBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public ColumnarTestBase() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .BreakIntoCodeGen(Config.CodegenOptions.DebugFlags.Operators)) // TODO: remove
        {
        }
    }

    /// <summary>
    /// Tests ensuring the "degenerate" projection case where we must use the generated batch's indexer to assign the payload,
    /// rather than optimizing/decomposing the projection expression.
    /// See SelectTransformer.Transform and SelectTransformer.ProjectionReturningResultInstance.
    /// This class covers all consumers of SelectTransformer.Transform
    /// </summary>
    [SuppressMessage("StyleCop.CSharp.SpacingRules", "SA1009", Justification = "Reviewed.")]
    [TestClass]
    public class GeneralProjectionFallbackTests : ColumnarTestBase
    {
        // Use a member function to construct the value tuple so that Trill doesn't know how to optimize/decompose it
        public static ValueTuple<T1, T2> CreateValueTuple<T1, T2>(T1 item1, T2 item2) => ValueTuple.Create(item1, item2);

        [TestMethod, TestCategory("Gated")]
        public void SelectTemplate()
        {
            var input = new[]
            {
                StreamEvent.CreateStart(0, 100L),
                StreamEvent.CreateStart(0, 105L),
                StreamEvent.CreateStart(0, 104L),
                StreamEvent.CreateStart(0, 200L),
                StreamEvent.CreateStart(0, 201L),
                StreamEvent.CreateStart(0, 300L),
                StreamEvent.CreateStart(0, 302L),
                StreamEvent.CreateStart(0, 303L),
                StreamEvent.CreatePunctuation<long>(StreamEvent.InfinitySyncTime),
            }.ToStreamable();

            var output = new List<StreamEvent<(long, long)>>();
            input
                .Select((time, payload) => CreateValueTuple(time, payload))
                .ToStreamEventObservable()
                .ForEachAsync(e => output.Add(e))
                .Wait();

            var correct = new[]
            {
                StreamEvent.CreateStart(0, ValueTuple.Create(0L, 100L)),
                StreamEvent.CreateStart(0, ValueTuple.Create(0L, 105L)),
                StreamEvent.CreateStart(0, ValueTuple.Create(0L, 104L)),
                StreamEvent.CreateStart(0, ValueTuple.Create(0L, 200L)),
                StreamEvent.CreateStart(0, ValueTuple.Create(0L, 201L)),
                StreamEvent.CreateStart(0, ValueTuple.Create(0L, 300L)),
                StreamEvent.CreateStart(0, ValueTuple.Create(0L, 302L)),
                StreamEvent.CreateStart(0, ValueTuple.Create(0L, 303L)),
                StreamEvent.CreatePunctuation<(long, long)>(StreamEvent.InfinitySyncTime),
            };

            Assert.IsTrue(correct.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void UngroupTemplate()
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
                .GroupAggregate(s => true, w => w.Count(), (g, c) => CreateValueTuple(g.Key, c))
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