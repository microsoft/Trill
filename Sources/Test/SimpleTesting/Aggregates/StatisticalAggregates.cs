// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Aggregates;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class StatisticalAggregates : TestWithConfigSettingsAndMemoryLeakDetection
    {
        [TestMethod, TestCategory("Gated")]
        public void VarianceAndStandardDeviationCoverScalarAndSimdPaths()
        {
            AssertVarianceAndStandardDeviation(StatisticalAggregate.VarianceScratchThreshold);
            AssertVarianceAndStandardDeviation(StatisticalAggregate.VarianceScratchThreshold + 1);
        }

        private static void AssertVarianceAndStandardDeviation(int count)
        {
            var events = Enumerable.Range(1, count)
                .Select(i => StreamEvent.CreateStart(0, (double)i))
                .Concat([StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime)])
                .ToArray();

            var input = events
                .ToStreamable()
                .SetProperty()
                .IsConstantDuration(true, StreamEvent.InfinitySyncTime);

            double expectedSampleVariance = count * (count + 1.0) / 12.0;
            double expectedPopulationVariance = ((count * count) - 1.0) / 12.0;

            AssertAggregate(input.Aggregate(w => w.Variance(e => e)), expectedSampleVariance);
            AssertAggregate(input.Aggregate(w => w.PopulationVariance(e => e)), expectedPopulationVariance);
            AssertAggregate(input.Aggregate(w => w.StandardDeviation(e => e)), Math.Sqrt(expectedSampleVariance));
            AssertAggregate(input.Aggregate(w => w.PopulationStandardDeviation(e => e)), Math.Sqrt(expectedPopulationVariance));
        }

        private static void AssertAggregate(IStreamable<Empty, double?> stream, double expected)
        {
            var output = new List<StreamEvent<double?>>();
            stream.ToStreamEventObservable().ForEachAsync(output.Add).Wait();

            var data = output.Single(e => e.IsData);
            Assert.IsTrue(data.Payload.HasValue);
            Assert.AreEqual(expected, data.Payload.Value, 1e-9);
        }
    }
}
