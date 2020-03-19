// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
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
            .DontFallBackToRowBasedExecution(true))
        {
        }
    }

    /// <summary>
    /// Tests ensuring the "degenerate" projection case where we must use the generated batch's indexer to assign the payload,
    /// rather than optimizing/decomposing the projection expression.
    /// See SelectTransformer.Transform and SelectTransformer.ProjectionReturningResultInstance.
    /// This class covers all consumers of SelectTransformer.Transform
    /// </summary>
    [SuppressMessage("StyleCop.CSharp.SpacingRules", "SA1008", Justification = "Reviewed.")]
    [SuppressMessage("StyleCop.CSharp.SpacingRules", "SA1009", Justification = "Reviewed.")]
    [TestClass]
    public class GeneralProjectionFallbackTests : ColumnarTestBase
    {
        // Use a member function to construct the value tuple so that Trill doesn't know how to optimize/decompose it
        public static ValueTuple<T1, T2> CreateValueTuple<T1, T2>(T1 item1, T2 item2) => ValueTuple.Create(item1, item2);

        [TestMethod, TestCategory("Gated")]
        public void SelectTemplate() => SelectWorker(selectMany: false, fuse: false);

        [TestMethod, TestCategory("Gated")]
        public void SelectManyTemplate() => SelectWorker(selectMany: true, fuse: false);

        [TestMethod, TestCategory("Gated")]
        public void FusedIngress() => SelectWorker(selectMany: false, fuse: true);

        [TestMethod, TestCategory("Gated")]
        public void UngroupTemplate() => GroupWorker(group: false);

        [TestMethod, TestCategory("Gated")]
        public void GroupedWindowTemplate() => GroupWorker(group: false);

        [TestMethod, TestCategory("Gated")]
        public void EquiJoinTemplate() => EquiJoinWorker();

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeEquiJoinTemplate() => EquiJoinWorker(constantDuration: StreamEvent.InfinitySyncTime);

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoinTemplate() => EquiJoinWorker(constantDuration: 10);

        private void EquiJoinWorker(long? constantDuration = null)
        {
            var left = new[]
            {
                StreamEvent.CreateStart(0, 100),
                StreamEvent.CreateStart(0, 110),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            }.ToStreamable();

            var right = new[]
            {
                StreamEvent.CreateStart(0, 200),
                StreamEvent.CreateStart(0, 210),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            }.ToStreamable();

            if (constantDuration.HasValue)
            {
                left = left.AlterEventDuration(constantDuration.Value);
                right = right.AlterEventDuration(constantDuration.Value);
            }

            var output = new List<StreamEvent<(int, int)>>();
            left.Join(right, (l, r) => CreateValueTuple(l, r))
                .ToStreamEventObservable()
                .ForEachAsync(e => output.Add(e))
                .Wait();

            var correct = new[]
            {
                StreamEvent.CreateStart(0, ValueTuple.Create(110, 200)),
                StreamEvent.CreateStart(0, ValueTuple.Create(100, 200)),
                StreamEvent.CreateStart(0, ValueTuple.Create(110, 210)),
                StreamEvent.CreateStart(0, ValueTuple.Create(100, 210)),
                StreamEvent.CreatePunctuation<ValueTuple<int, int>>(StreamEvent.InfinitySyncTime)
            };

            if (constantDuration.HasValue && constantDuration.Value != StreamEvent.InfinitySyncTime)
            {
                correct = correct
                    .Select(e => e.IsPunctuation ? e : StreamEvent.CreateInterval(e.StartTime, e.StartTime + constantDuration.Value, e.Payload))
                    .ToArray();
            }

            Assert.IsTrue(correct.SequenceEqual(output));
        }

        private void GroupWorker(bool group)
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

            if (group)
            {
                input = input.SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime);
            }

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

        private void SelectWorker(bool selectMany, bool fuse)
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

            var streamable = input;
            if (!fuse)
            {
                // Insert a fake AlterEventLifetime to prevent a fuse with ingress
                streamable = streamable.AlterEventLifetime(time => time, StreamEvent.InfinitySyncTime);
            }

            // SelectMany optimizes Enumerable.Repeat into a SelectTransformer.Transform
            var selectStreamable = selectMany
                ? streamable.SelectMany((time, payload) => Enumerable.Repeat(CreateValueTuple(time, payload), 1))
                : streamable.Select((time, payload) => CreateValueTuple(time, payload));

            selectStreamable
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

        private class EmptyKeyComparer : IEqualityComparerExpression<Empty>
        {
            public Expression<Func<Empty, Empty, bool>> GetEqualsExpr() => (l, r) => l == r;
            public Expression<Func<Empty, int>> GetGetHashCodeExpr() => (value) => value.GetHashCode();
        }
    }
}