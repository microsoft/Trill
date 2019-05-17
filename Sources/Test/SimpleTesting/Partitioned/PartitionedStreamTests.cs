// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public static class Ex
    {
        public static IObserver<T> NotificationListObserver<T>(this List<Notification<T>> bucket)
            => Observer.Create<T>(
                e => bucket.Add(Notification.CreateOnNext(e)),
                e => bucket.Add(Notification.CreateOnError<T>(e)),
                () => bucket.Add(Notification.CreateOnCompleted<T>()));
    }

    [TestClass]
    public class PartitionedStreamTests : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public PartitionedStreamTests() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true))
        { }

        public static IEnumerable<PartitionedStreamEvent<T, U>> Passthrough<T, U>(
            IEnumerable<PartitionedStreamEvent<T, U>> input)
            => input
                .ToObservable()
                .ToStreamable()
                .ToStreamEventObservable()
                .ToEnumerable();

        public static IEnumerable<PartitionedStreamEvent<T, double>> AverageGeneral<T>(
            IEnumerable<PartitionedStreamEvent<T, double>> input)
            => input
                .ToObservable()
                .ToStreamable()
                .Average(o => o)
                .ToStreamEventObservable()
                .ToEnumerable();

        public static IEnumerable<PartitionedStreamEvent<T, double>> AverageConstantDuration<T>(
            IEnumerable<PartitionedStreamEvent<T, double>> input)
            => input
                .ToObservable()
                .ToStreamable()
                .AlterEventDuration(10)
                .Average(o => o)
                .ToStreamEventObservable()
                .ToEnumerable();

        public static IEnumerable<PartitionedStreamEvent<T, double>> AverageStartEdgeOnly<T>(
            IEnumerable<PartitionedStreamEvent<T, double>> input)
        {
            var foo = input
                .ToObservable()
                .ToStreamable();
            foo.Properties.IsConstantDuration = true;
            foo.Properties.ConstantDurationLength = StreamEvent.InfinitySyncTime;
            return foo
                .Average(o => o)
                .ToStreamEventObservable()
                .ToEnumerable();
        }

        public static IEnumerable<PartitionedStreamEvent<T, double>> TumblingWindow<T>(
            IEnumerable<PartitionedStreamEvent<T, double>> input)
            => input
                .ToObservable()
                .ToStreamable()
                .TumblingWindowLifetime(1)
                .Average(o => o)
                .ToStreamEventObservable()
                .ToEnumerable();

        public static IEnumerable<PartitionedStreamEvent<T, double>> SessionWindowAverage<T>(
            IEnumerable<PartitionedStreamEvent<T, double>> input, long timeout, long maxDuration)
            => input
                .ToObservable()
                .ToStreamable()
                .SessionTimeoutWindow(timeout, maxDuration)
                .Average(o => o)
                .ToStreamEventObservable()
                .ToEnumerable();

        public static IEnumerable<PartitionedStreamEvent<T, double>> SessionWindowSum<T>(
            IEnumerable<PartitionedStreamEvent<T, double>> input, long timeout, long maxDuration)
            => input
                .ToObservable()
                .ToStreamable()
                .SessionTimeoutWindow(timeout, maxDuration)
                .Sum(o => o)
                .ToStreamEventObservable()
                .ToEnumerable();

        public static IEnumerable<PartitionedStreamEvent<T, U>> Union<T, U>(
            IEnumerable<PartitionedStreamEvent<T, U>> input1,
            IEnumerable<PartitionedStreamEvent<T, U>> input2)
        {
            var s1 = input1
                .ToObservable()
                .ToStreamable();

            var s2 = input2
                .ToObservable()
                .ToStreamable();

            return s1.Union(s2)
                .ToStreamEventObservable()
                .ToEnumerable();
        }

        [TestMethod, TestCategory("Gated")]
        public void PassThroughEmpty()
        {
            var input = Array.Empty<PartitionedStreamEvent<int, int>>();

            var expected = new PartitionedStreamEvent<int, int>[]
            {
                PartitionedStreamEvent.CreateLowWatermark<int, int>(StreamEvent.InfinitySyncTime),
            };
            var output = Passthrough(input).ToList();
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void PassThroughSimple()
        {
            var input = new PartitionedStreamEvent<int, int>[]
            {
                PartitionedStreamEvent.CreateStart(0, 0, 0),
                PartitionedStreamEvent.CreateStart(1, 1, 1),
                PartitionedStreamEvent.CreateStart(2, 2, 2),
                PartitionedStreamEvent.CreateStart(3, 3, 3),
            };

            var expected = new PartitionedStreamEvent<int, int>[]
            {
                PartitionedStreamEvent.CreateStart(0, 0, 0),
                PartitionedStreamEvent.CreateStart(1, 1, 1),
                PartitionedStreamEvent.CreateStart(2, 2, 2),
                PartitionedStreamEvent.CreateStart(3, 3, 3),
                PartitionedStreamEvent.CreateLowWatermark<int, int>(StreamEvent.InfinitySyncTime),
            };
            var output = Passthrough(input).ToList();
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void PassThroughOutOfOrder1()
        {
            var input = new PartitionedStreamEvent<int, int>[]
            {
                PartitionedStreamEvent.CreateStart(0, 0, 0),
                PartitionedStreamEvent.CreateStart(2, 2, 2),
                PartitionedStreamEvent.CreateStart(1, 1, 1),
                PartitionedStreamEvent.CreateStart(3, 3, 3),
            };
            var expected = new PartitionedStreamEvent<int, int>[]
            {
                PartitionedStreamEvent.CreateStart(0, 0, 0),
                PartitionedStreamEvent.CreateStart(2, 2, 2),
                PartitionedStreamEvent.CreateStart(1, 1, 1),
                PartitionedStreamEvent.CreateStart(3, 3, 3),
                PartitionedStreamEvent.CreateLowWatermark<int, int>(StreamEvent.InfinitySyncTime),
            };

            var output = Passthrough(input).ToList();
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void PassThroughOutOfOrder2()
        {
            var input = new PartitionedStreamEvent<int, int>[]
            {
                PartitionedStreamEvent.CreateStart(3, 3, 3),
                PartitionedStreamEvent.CreateStart(2, 2, 2),
                PartitionedStreamEvent.CreateStart(1, 1, 1),
                PartitionedStreamEvent.CreateStart(0, 0, 0),
            };
            var expected = new PartitionedStreamEvent<int, int>[]
            {
                PartitionedStreamEvent.CreateStart(3, 3, 3),
                PartitionedStreamEvent.CreateStart(2, 2, 2),
                PartitionedStreamEvent.CreateStart(1, 1, 1),
                PartitionedStreamEvent.CreateStart(0, 0, 0),
                PartitionedStreamEvent.CreateLowWatermark<int, int>(StreamEvent.InfinitySyncTime),
            };

            var output = Passthrough(input).ToList();
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void PassThroughSimpleWithPunctuation1()
        {
            var input = new PartitionedStreamEvent<int, int>[]
            {
                PartitionedStreamEvent.CreateStart(0, 0, 0),
                PartitionedStreamEvent.CreatePunctuation<int, int>(0, 1),
                PartitionedStreamEvent.CreateStart(1, 1, 1),
                PartitionedStreamEvent.CreatePunctuation<int, int>(1, 2),
                PartitionedStreamEvent.CreateStart(2, 2, 2),
                PartitionedStreamEvent.CreatePunctuation<int, int>(2, 3),
                PartitionedStreamEvent.CreateStart(3, 3, 3),
                PartitionedStreamEvent.CreatePunctuation<int, int>(3, 4),
            };
            var expected = new PartitionedStreamEvent<int, int>[]
            {
                PartitionedStreamEvent.CreateStart(0, 0, 0),
                PartitionedStreamEvent.CreatePunctuation<int, int>(0, 1),
                PartitionedStreamEvent.CreateStart(1, 1, 1),
                PartitionedStreamEvent.CreatePunctuation<int, int>(1, 2),
                PartitionedStreamEvent.CreateStart(2, 2, 2),
                PartitionedStreamEvent.CreatePunctuation<int, int>(2, 3),
                PartitionedStreamEvent.CreateStart(3, 3, 3),
                PartitionedStreamEvent.CreatePunctuation<int, int>(3, 4),
                PartitionedStreamEvent.CreateLowWatermark<int, int>(StreamEvent.InfinitySyncTime),
            };

            var output = Passthrough(input).ToList();
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void PassThroughSimpleWithPunctuation2()
        {
            var input = new PartitionedStreamEvent<int, int>[]
            {
                PartitionedStreamEvent.CreateStart(0, 0, 0),
                PartitionedStreamEvent.CreatePunctuation<int, int>(0, 10),
                PartitionedStreamEvent.CreateStart(1, 1, 1),
                PartitionedStreamEvent.CreatePunctuation<int, int>(1, 10),
                PartitionedStreamEvent.CreateStart(2, 2, 2),
                PartitionedStreamEvent.CreatePunctuation<int, int>(2, 10),
                PartitionedStreamEvent.CreateStart(3, 3, 3),
                PartitionedStreamEvent.CreatePunctuation<int, int>(3, 10),
            };
            var expected = new PartitionedStreamEvent<int, int>[]
            {
                PartitionedStreamEvent.CreateStart(0, 0, 0),
                PartitionedStreamEvent.CreatePunctuation<int, int>(0, 10),
                PartitionedStreamEvent.CreateStart(1, 1, 1),
                PartitionedStreamEvent.CreatePunctuation<int, int>(1, 10),
                PartitionedStreamEvent.CreateStart(2, 2, 2),
                PartitionedStreamEvent.CreatePunctuation<int, int>(2, 10),
                PartitionedStreamEvent.CreateStart(3, 3, 3),
                PartitionedStreamEvent.CreatePunctuation<int, int>(3, 10),
                PartitionedStreamEvent.CreateLowWatermark<int, int>(StreamEvent.InfinitySyncTime),
            };

            var output = Passthrough(input).ToList();
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void PassThroughOutOfOrderWithPunctuation1()
        {
            var input = new PartitionedStreamEvent<int, int>[]
            {
                PartitionedStreamEvent.CreateStart(0, 0, 0),
                PartitionedStreamEvent.CreateStart(2, 2, 2),
                PartitionedStreamEvent.CreateStart(1, 1, 1),
                PartitionedStreamEvent.CreateStart(3, 3, 3),
                PartitionedStreamEvent.CreatePunctuation<int, int>(0, 1),
                PartitionedStreamEvent.CreatePunctuation<int, int>(1, 2),
                PartitionedStreamEvent.CreatePunctuation<int, int>(2, 3),
                PartitionedStreamEvent.CreatePunctuation<int, int>(3, 4),
            };
            var expected = new PartitionedStreamEvent<int, int>[]
            {
                PartitionedStreamEvent.CreateStart(0, 0, 0),
                PartitionedStreamEvent.CreateStart(2, 2, 2),
                PartitionedStreamEvent.CreateStart(1, 1, 1),
                PartitionedStreamEvent.CreateStart(3, 3, 3),
                PartitionedStreamEvent.CreatePunctuation<int, int>(0, 1),
                PartitionedStreamEvent.CreatePunctuation<int, int>(1, 2),
                PartitionedStreamEvent.CreatePunctuation<int, int>(2, 3),
                PartitionedStreamEvent.CreatePunctuation<int, int>(3, 4),
                PartitionedStreamEvent.CreateLowWatermark<int, int>(StreamEvent.InfinitySyncTime),
            };

            var output = Passthrough(input).ToList();
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void PassThroughOutOfOrderWithPunctuation2()
        {
            var input = new PartitionedStreamEvent<int, int>[]
            {
                PartitionedStreamEvent.CreateStart(3, 3, 3),
                PartitionedStreamEvent.CreatePunctuation<int, int>(3, 10),
                PartitionedStreamEvent.CreateStart(2, 2, 2),
                PartitionedStreamEvent.CreatePunctuation<int, int>(2, 10),
                PartitionedStreamEvent.CreateStart(1, 1, 1),
                PartitionedStreamEvent.CreatePunctuation<int, int>(1, 10),
                PartitionedStreamEvent.CreateStart(0, 0, 0),
                PartitionedStreamEvent.CreatePunctuation<int, int>(0, 10),
            };
            var expected = new PartitionedStreamEvent<int, int>[]
            {
                PartitionedStreamEvent.CreateStart(3, 3, 3),
                PartitionedStreamEvent.CreatePunctuation<int, int>(3, 10),
                PartitionedStreamEvent.CreateStart(2, 2, 2),
                PartitionedStreamEvent.CreatePunctuation<int, int>(2, 10),
                PartitionedStreamEvent.CreateStart(1, 1, 1),
                PartitionedStreamEvent.CreatePunctuation<int, int>(1, 10),
                PartitionedStreamEvent.CreateStart(0, 0, 0),
                PartitionedStreamEvent.CreatePunctuation<int, int>(0, 10),
                PartitionedStreamEvent.CreateLowWatermark<int, int>(StreamEvent.InfinitySyncTime),
            };

            var output = Passthrough(input).ToList();
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageStartEdgeOnly()
        {
            var input = new PartitionedStreamEvent<int, double>[]
            {
                PartitionedStreamEvent.CreateStart(0, 0, 0.0),
                PartitionedStreamEvent.CreateStart(0, 1, 1.0),
                PartitionedStreamEvent.CreateStart(0, 2, 2.0),
                PartitionedStreamEvent.CreateStart(1, 1, 1.0),
                PartitionedStreamEvent.CreateStart(1, 2, 2.0),
                PartitionedStreamEvent.CreateStart(1, 3, 3.0),
                PartitionedStreamEvent.CreateStart(2, 2, 2.0),
                PartitionedStreamEvent.CreateStart(2, 3, 3.0),
                PartitionedStreamEvent.CreateStart(2, 4, 4.0),
                PartitionedStreamEvent.CreateStart(3, 3, 3.0),
                PartitionedStreamEvent.CreateStart(3, 4, 4.0),
                PartitionedStreamEvent.CreateStart(3, 5, 5.0),
            };

            var expected = new PartitionedStreamEvent<int, double>[]
            {
                PartitionedStreamEvent.CreateStart(0, 0, 0.0),
                PartitionedStreamEvent.CreateEnd(0, 1, 0, 0.0),
                PartitionedStreamEvent.CreateStart(0, 1, 0.5),
                PartitionedStreamEvent.CreateEnd(0, 2, 1, 0.5),
                PartitionedStreamEvent.CreateStart(1, 1, 1.0),
                PartitionedStreamEvent.CreateEnd(1, 2, 1, 1.0),
                PartitionedStreamEvent.CreateStart(1, 2, 1.5),
                PartitionedStreamEvent.CreateEnd(1, 3, 2, 1.5),
                PartitionedStreamEvent.CreateStart(2, 2, 2.0),
                PartitionedStreamEvent.CreateEnd(2, 3, 2, 2.0),
                PartitionedStreamEvent.CreateStart(2, 3, 2.5),
                PartitionedStreamEvent.CreateEnd(2, 4, 3, 2.5),
                PartitionedStreamEvent.CreateStart(3, 3, 3.0),
                PartitionedStreamEvent.CreateEnd(3, 4, 3, 3.0),
                PartitionedStreamEvent.CreateStart(3, 4, 3.5),
                PartitionedStreamEvent.CreateEnd(3, 5, 4, 3.5),
                PartitionedStreamEvent.CreateStart(0, 2, 1.0),
                PartitionedStreamEvent.CreateStart(1, 3, 2.0),
                PartitionedStreamEvent.CreateStart(2, 4, 3.0),
                PartitionedStreamEvent.CreateStart(3, 5, 4.0),
                PartitionedStreamEvent.CreateLowWatermark<int, double>(StreamEvent.InfinitySyncTime),
            };

            var output = AverageStartEdgeOnly(input);
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageConstantDuration()
        {
            var input = new PartitionedStreamEvent<int, double>[]
            {
                PartitionedStreamEvent.CreateStart(0, 0, 0.0),
                PartitionedStreamEvent.CreateStart(0, 1, 1.0),
                PartitionedStreamEvent.CreateStart(0, 2, 2.0),
                PartitionedStreamEvent.CreateStart(1, 1, 1.0),
                PartitionedStreamEvent.CreateStart(1, 2, 2.0),
                PartitionedStreamEvent.CreateStart(1, 3, 3.0),
                PartitionedStreamEvent.CreateStart(2, 2, 2.0),
                PartitionedStreamEvent.CreateStart(2, 3, 3.0),
                PartitionedStreamEvent.CreateStart(2, 4, 4.0),
                PartitionedStreamEvent.CreateStart(3, 3, 3.0),
                PartitionedStreamEvent.CreateStart(3, 4, 4.0),
                PartitionedStreamEvent.CreateStart(3, 5, 5.0),
            };

            var expected = new PartitionedStreamEvent<int, double>[]
            {
                PartitionedStreamEvent.CreateStart(0, 0, 0.0),
                PartitionedStreamEvent.CreateEnd(0, 1, 0, 0.0),
                PartitionedStreamEvent.CreateStart(0, 1, 0.5),
                PartitionedStreamEvent.CreateEnd(0, 2, 1, 0.5),
                PartitionedStreamEvent.CreateStart(1, 1, 1.0),
                PartitionedStreamEvent.CreateEnd(1, 2, 1, 1.0),
                PartitionedStreamEvent.CreateStart(1, 2, 1.5),
                PartitionedStreamEvent.CreateEnd(1, 3, 2, 1.5),
                PartitionedStreamEvent.CreateStart(2, 2, 2.0),
                PartitionedStreamEvent.CreateEnd(2, 3, 2, 2.0),
                PartitionedStreamEvent.CreateStart(2, 3, 2.5),
                PartitionedStreamEvent.CreateEnd(2, 4, 3, 2.5),
                PartitionedStreamEvent.CreateStart(3, 3, 3.0),
                PartitionedStreamEvent.CreateEnd(3, 4, 3, 3.0),
                PartitionedStreamEvent.CreateStart(3, 4, 3.5),
                PartitionedStreamEvent.CreateEnd(3, 5, 4, 3.5),
                PartitionedStreamEvent.CreateStart(0, 2, 1.0),
                PartitionedStreamEvent.CreateEnd(0, 10, 2, 1.0),
                PartitionedStreamEvent.CreateStart(0, 10, 1.5),
                PartitionedStreamEvent.CreateEnd(0, 11, 10, 1.5),
                PartitionedStreamEvent.CreateStart(0, 11, 2.0),
                PartitionedStreamEvent.CreateEnd(0, 12, 11, 2.0),
                PartitionedStreamEvent.CreateStart(1, 3, 2.0),
                PartitionedStreamEvent.CreateEnd(1, 11, 3, 2.0),
                PartitionedStreamEvent.CreateStart(1, 11, 2.5),
                PartitionedStreamEvent.CreateEnd(1, 12, 11, 2.5),
                PartitionedStreamEvent.CreateStart(1, 12, 3.0),
                PartitionedStreamEvent.CreateEnd(1, 13, 12, 3.0),
                PartitionedStreamEvent.CreateStart(2, 4, 3.0),
                PartitionedStreamEvent.CreateEnd(2, 12, 4, 3.0),
                PartitionedStreamEvent.CreateStart(2, 12, 3.5),
                PartitionedStreamEvent.CreateEnd(2, 13, 12, 3.5),
                PartitionedStreamEvent.CreateStart(2, 13, 4.0),
                PartitionedStreamEvent.CreateEnd(2, 14, 13, 4.0),
                PartitionedStreamEvent.CreateStart(3, 5, 4.0),
                PartitionedStreamEvent.CreateEnd(3, 13, 5, 4.0),
                PartitionedStreamEvent.CreateStart(3, 13, 4.5),
                PartitionedStreamEvent.CreateEnd(3, 14, 13, 4.5),
                PartitionedStreamEvent.CreateStart(3, 14, 5.0),
                PartitionedStreamEvent.CreateEnd(3, 15, 14, 5.0),
                PartitionedStreamEvent.CreateLowWatermark<int, double>(StreamEvent.InfinitySyncTime),
            };

            var output = AverageConstantDuration(input).ToList();
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageTumblingWindow()
        {
            var input = new PartitionedStreamEvent<int, double>[]
            {
                PartitionedStreamEvent.CreateStart(0, 0, 0.0),
                PartitionedStreamEvent.CreateStart(0, 1, 1.0),
                PartitionedStreamEvent.CreateStart(0, 2, 2.0),
                PartitionedStreamEvent.CreateStart(1, 1, 1.0),
                PartitionedStreamEvent.CreateStart(1, 2, 2.0),
                PartitionedStreamEvent.CreateStart(1, 3, 3.0),
                PartitionedStreamEvent.CreateStart(2, 2, 2.0),
                PartitionedStreamEvent.CreateStart(2, 3, 3.0),
                PartitionedStreamEvent.CreateStart(2, 4, 4.0),
                PartitionedStreamEvent.CreateStart(3, 3, 3.0),
                PartitionedStreamEvent.CreateStart(3, 4, 4.0),
                PartitionedStreamEvent.CreateStart(3, 5, 5.0),
            };

            var expected = new PartitionedStreamEvent<int, double>[]
            {
                PartitionedStreamEvent.CreateInterval(0, 0, 1, 0.0),
                PartitionedStreamEvent.CreateInterval(0, 1, 2, 1.0),
                PartitionedStreamEvent.CreateInterval(1, 1, 2, 1.0),
                PartitionedStreamEvent.CreateInterval(1, 2, 3, 2.0),
                PartitionedStreamEvent.CreateInterval(2, 2, 3, 2.0),
                PartitionedStreamEvent.CreateInterval(2, 3, 4, 3.0),
                PartitionedStreamEvent.CreateInterval(3, 3, 4, 3.0),
                PartitionedStreamEvent.CreateInterval(3, 4, 5, 4.0),
                PartitionedStreamEvent.CreateInterval(0, 2, 3, 2.0),
                PartitionedStreamEvent.CreateInterval(1, 3, 4, 3.0),
                PartitionedStreamEvent.CreateInterval(2, 4, 5, 4.0),
                PartitionedStreamEvent.CreateInterval(3, 5, 6, 5.0),
                PartitionedStreamEvent.CreateLowWatermark<int, double>(StreamEvent.InfinitySyncTime),
            };

            var output = TumblingWindow(input);
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageSessionWindow()
        {
            var input = new PartitionedStreamEvent<int, double>[]
            {
                PartitionedStreamEvent.CreateStart(0, 0, 0.0),
                PartitionedStreamEvent.CreateStart(0, 1, 1.0),
                PartitionedStreamEvent.CreateStart(0, 2, 2.0),
                PartitionedStreamEvent.CreateStart(0, 8, 4.0),
                PartitionedStreamEvent.CreateStart(0, 9, 7.0),
                PartitionedStreamEvent.CreateStart(1, 1, 1.0),
                PartitionedStreamEvent.CreateStart(1, 2, 2.0),
                PartitionedStreamEvent.CreateStart(1, 3, 3.0),
                PartitionedStreamEvent.CreateStart(2, 8, 2.0),
                PartitionedStreamEvent.CreateStart(2, 20, 3.0),
                PartitionedStreamEvent.CreateStart(2, 22, 4.0),
                PartitionedStreamEvent.CreateStart(3, 3, 3.0),
                PartitionedStreamEvent.CreateStart(3, 4, 4.0),
                PartitionedStreamEvent.CreateStart(3, 5, 5.0),
            };

            var expected = new PartitionedStreamEvent<int, double>[]
            {
                PartitionedStreamEvent.CreateStart(0, 0, 0.0),
                PartitionedStreamEvent.CreateEnd(0, 1, 0, 0.0),
                PartitionedStreamEvent.CreateStart(0, 1, 0.5),
                PartitionedStreamEvent.CreateEnd(0, 2, 1, 0.5),
                PartitionedStreamEvent.CreateStart(0, 2, 1.0),
                PartitionedStreamEvent.CreateEnd(0, 7, 2, 1.0),
                PartitionedStreamEvent.CreateStart(0, 8, 4.0),
                PartitionedStreamEvent.CreateEnd(0, 9, 8, 4.0),
                PartitionedStreamEvent.CreateStart(1, 1, 1.0),
                PartitionedStreamEvent.CreateEnd(1, 2, 1, 1.0),
                PartitionedStreamEvent.CreateStart(1, 2, 1.5),
                PartitionedStreamEvent.CreateEnd(1, 3, 2, 1.5),
                PartitionedStreamEvent.CreateStart(2, 8, 2.0),
                PartitionedStreamEvent.CreateEnd(2, 13, 8, 2.0),
                PartitionedStreamEvent.CreateStart(2, 20, 3.0),
                PartitionedStreamEvent.CreateEnd(2, 22, 20, 3.0),
                PartitionedStreamEvent.CreateStart(3, 3, 3.0),
                PartitionedStreamEvent.CreateEnd(3, 4, 3, 3.0),
                PartitionedStreamEvent.CreateStart(3, 4, 3.5),
                PartitionedStreamEvent.CreateEnd(3, 5, 4, 3.5),
                PartitionedStreamEvent.CreateStart(0, 9, 5.5),
                PartitionedStreamEvent.CreateEnd(0, 14, 9, 5.5),
                PartitionedStreamEvent.CreateStart(1, 3, 2.0),
                PartitionedStreamEvent.CreateEnd(1, 8, 3, 2.0),
                PartitionedStreamEvent.CreateStart(2, 22, 3.5),
                PartitionedStreamEvent.CreateEnd(2, 27, 22, 3.5),
                PartitionedStreamEvent.CreateStart(3, 5, 4.0),
                PartitionedStreamEvent.CreateEnd(3, 10, 5, 4.0),
                PartitionedStreamEvent.CreateLowWatermark<int, double>(StreamEvent.InfinitySyncTime),
            };

            var output = SessionWindowAverage(input, 5, 10);
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SumSessionWindow()
        {
            var input = new PartitionedStreamEvent<int, double>[]
            {
                PartitionedStreamEvent.CreatePoint(0, 0, 1.0),
                PartitionedStreamEvent.CreatePoint(0, 1, 2.0),
                PartitionedStreamEvent.CreatePoint(0, 2, 3.0),
                PartitionedStreamEvent.CreatePoint(0, 5, 4.0),
                PartitionedStreamEvent.CreatePoint(0, 6, 5.0),
                PartitionedStreamEvent.CreatePoint(0, 9, 6.0),
            };

            var expected = new PartitionedStreamEvent<int, double>[]
            {
                PartitionedStreamEvent.CreateStart(0, 0, 1.0),
                PartitionedStreamEvent.CreateEnd(0, 1, 0, 1.0),
                PartitionedStreamEvent.CreateStart(0, 1, 3.0),
                PartitionedStreamEvent.CreateEnd(0, 2, 1, 3.0),
                PartitionedStreamEvent.CreateStart(0, 2, 6.0),
                PartitionedStreamEvent.CreateEnd(0, 4, 2, 6.0),
                PartitionedStreamEvent.CreateStart(0, 5, 4.0),
                PartitionedStreamEvent.CreateEnd(0, 6, 5, 4.0),
                PartitionedStreamEvent.CreateStart(0, 6, 9.0),
                PartitionedStreamEvent.CreateEnd(0, 8, 6, 9.0),
                PartitionedStreamEvent.CreateStart(0, 9, 6.0),
                PartitionedStreamEvent.CreateEnd(0, 11, 9, 6.0),
                PartitionedStreamEvent.CreateLowWatermark<int, double>(StreamEvent.InfinitySyncTime),
            };

            var output = SessionWindowSum(input, 2, 5).ToArray();
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageGeneral()
        {
            var input = new PartitionedStreamEvent<int, double>[]
            {
                PartitionedStreamEvent.CreateStart(0, 0, 0.0),
                PartitionedStreamEvent.CreateStart(0, 1, 1.0),
                PartitionedStreamEvent.CreateStart(0, 2, 2.0),
                PartitionedStreamEvent.CreateStart(1, 1, 1.0),
                PartitionedStreamEvent.CreateStart(1, 2, 2.0),
                PartitionedStreamEvent.CreateStart(1, 3, 3.0),
                PartitionedStreamEvent.CreateStart(2, 2, 2.0),
                PartitionedStreamEvent.CreateStart(2, 3, 3.0),
                PartitionedStreamEvent.CreateStart(2, 4, 4.0),
                PartitionedStreamEvent.CreateStart(3, 3, 3.0),
                PartitionedStreamEvent.CreateStart(3, 4, 4.0),
                PartitionedStreamEvent.CreateStart(3, 5, 5.0),
            };

            var expected = new PartitionedStreamEvent<int, double>[]
            {
                PartitionedStreamEvent.CreateStart(0, 0, 0.0),
                PartitionedStreamEvent.CreateEnd(0, 1, 0, 0.0),
                PartitionedStreamEvent.CreateStart(0, 1, 0.5),
                PartitionedStreamEvent.CreateEnd(0, 2, 1, 0.5),
                PartitionedStreamEvent.CreateStart(1, 1, 1.0),
                PartitionedStreamEvent.CreateEnd(1, 2, 1, 1.0),
                PartitionedStreamEvent.CreateStart(1, 2, 1.5),
                PartitionedStreamEvent.CreateEnd(1, 3, 2, 1.5),
                PartitionedStreamEvent.CreateStart(2, 2, 2.0),
                PartitionedStreamEvent.CreateEnd(2, 3, 2, 2.0),
                PartitionedStreamEvent.CreateStart(2, 3, 2.5),
                PartitionedStreamEvent.CreateEnd(2, 4, 3, 2.5),
                PartitionedStreamEvent.CreateStart(3, 3, 3.0),
                PartitionedStreamEvent.CreateEnd(3, 4, 3, 3.0),
                PartitionedStreamEvent.CreateStart(3, 4, 3.5),
                PartitionedStreamEvent.CreateEnd(3, 5, 4, 3.5),
                PartitionedStreamEvent.CreateStart(0, 2, 1.0),
                PartitionedStreamEvent.CreateStart(1, 3, 2.0),
                PartitionedStreamEvent.CreateStart(2, 4, 3.0),
                PartitionedStreamEvent.CreateStart(3, 5, 4.0),
                PartitionedStreamEvent.CreateLowWatermark<int, double>(StreamEvent.InfinitySyncTime),
            };

            var output = AverageGeneral(input).ToList();
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void Union0()
        {
            var input1 = new PartitionedStreamEvent<int, double>[]
            {
                PartitionedStreamEvent.CreateStart(0, 0, 0.0),
                PartitionedStreamEvent.CreateStart(0, 1, 1.0),
                PartitionedStreamEvent.CreateStart(0, 2, 2.0),
                PartitionedStreamEvent.CreateStart(1, 1, 1.0),
                PartitionedStreamEvent.CreateStart(1, 2, 2.0),
                PartitionedStreamEvent.CreateStart(1, 3, 3.0),
                PartitionedStreamEvent.CreateStart(2, 2, 2.0),
                PartitionedStreamEvent.CreateStart(2, 3, 3.0),
                PartitionedStreamEvent.CreateStart(2, 4, 4.0),
            };

            var input2 = new PartitionedStreamEvent<int, double>[]
            {
                PartitionedStreamEvent.CreateStart(2, 0, 0.0),
                PartitionedStreamEvent.CreateStart(2, 1, 1.0),
                PartitionedStreamEvent.CreateStart(2, 2, 2.0),
                PartitionedStreamEvent.CreateStart(1, 1, 1.0),
                PartitionedStreamEvent.CreateStart(1, 2, 2.0),
                PartitionedStreamEvent.CreateStart(1, 3, 3.0),
                PartitionedStreamEvent.CreateStart(0, 2, 2.0),
                PartitionedStreamEvent.CreateStart(0, 3, 3.0),
                PartitionedStreamEvent.CreateStart(0, 4, 4.0),
            };

            var output = Union(input1, input2).ToArray();

            var expected = new PartitionedStreamEvent<int, double>[]
            {
                PartitionedStreamEvent.CreateStart(0, 0, 0.0),
                PartitionedStreamEvent.CreateStart(2, 0, 0.0),
                PartitionedStreamEvent.CreateStart(2, 1, 1.0),
                PartitionedStreamEvent.CreateStart(2, 2, 2.0),
                PartitionedStreamEvent.CreateStart(2, 2, 2.0),
                PartitionedStreamEvent.CreateStart(2, 3, 3.0),
                PartitionedStreamEvent.CreateStart(2, 4, 4.0),
                PartitionedStreamEvent.CreateStart(1, 1, 1.0),
                PartitionedStreamEvent.CreateStart(1, 1, 1.0),
                PartitionedStreamEvent.CreateStart(1, 2, 2.0),
                PartitionedStreamEvent.CreateStart(1, 2, 2.0),
                PartitionedStreamEvent.CreateStart(1, 3, 3.0),
                PartitionedStreamEvent.CreateStart(1, 3, 3.0),
                PartitionedStreamEvent.CreateStart(0, 1, 1.0),
                PartitionedStreamEvent.CreateStart(0, 2, 2.0),
                PartitionedStreamEvent.CreateStart(0, 2, 2.0),
                PartitionedStreamEvent.CreateStart(0, 3, 3.0),
                PartitionedStreamEvent.CreateStart(0, 4, 4.0),
                PartitionedStreamEvent.CreateLowWatermark<int, double>(StreamEvent.InfinitySyncTime),
            };
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyWithFlush()
        {
            var data = new PartitionedStreamEvent<int, double>[]
            {
                PartitionedStreamEvent.CreatePoint(0, 0, 0.0),
                PartitionedStreamEvent.CreatePoint(0, 1, 1.0),
                PartitionedStreamEvent.CreatePoint(0, 2, 2.0),
                PartitionedStreamEvent.CreatePoint(1, 1, 1.0),
                PartitionedStreamEvent.CreatePoint(1, 2, 2.0),
                PartitionedStreamEvent.CreatePoint(1, 3, 3.0),
                PartitionedStreamEvent.CreatePoint(2, 2, 2.0),
                PartitionedStreamEvent.CreatePoint(2, 3, 3.0),
                PartitionedStreamEvent.CreatePoint(2, 4, 4.0),
            };

            var j = new Subject<PartitionedStreamEvent<int, double>>();
            var res = new List<Notification<PartitionedStreamEvent<int, double>>>();

            var qc = new QueryContainer();
            var input = qc.RegisterInput(j);
            var output = qc.RegisterOutput(input.SelectMany(e => new[] { e, 100.0 * e }));
            using (j)
            using (output.Subscribe(res.NotificationListObserver()))
            {
                Process p = qc.Restore(null);
                foreach (var x in data)
                    j.OnNext(x);

                p.Flush();
                Assert.IsTrue(res.Count > data.Length, "Flush should push all events out.");

                j.OnCompleted();
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void AlterLifeTimeWithFlush()
        {
            var startDate = new DateTime(100);
            var data = new[]
            {
                new { Time = startDate, DeviceId = 4, Value = 4 },
                new { Time = startDate, DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(1),  DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(2),  DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(3),  DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(11), DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(5),  DeviceId = 4, Value = 4 },
                new { Time = startDate.AddMinutes(12), DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(13), DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(14), DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(15), DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(16), DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(17), DeviceId = 4, Value = 4 },
                new { Time = startDate.AddMinutes(21), DeviceId = 6, Value = 6 },
            }
            .Select(e => PartitionedStreamEvent.CreateInterval(e.DeviceId, e.Time.Ticks, StreamEvent.MaxSyncTime, e.Value));

            var res = new List<Notification<PartitionedStreamEvent<int, int>>>();

            using (data.ToObservable()
                .ToStreamable(
                    DisorderPolicy.Adjust(0),
                    PartitionedFlushPolicy.FlushOnLowWatermark,
                    PeriodicPunctuationPolicy.Time(36000))
                .AlterEventDuration(1)
                .ToStreamEventObservable()
                .Subscribe(res.NotificationListObserver()))
            {
            }

            Assert.IsTrue(res.Count > 0, "There should be some results.");
        }

        [TestMethod, TestCategory("Gated")]
        public void AlterLifeTimeWithFlushDoubles()
        {
            var startDate = new DateTime(100);
            var data = new[]
            {
                new { Time = startDate,                DeviceId = 4, Value = 4 },
                new { Time = startDate,                DeviceId = 4, Value = 4 },
                new { Time = startDate,                DeviceId = 6, Value = 6 },
                new { Time = startDate,                DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(1),  DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(1),  DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(2),  DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(2),  DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(3),  DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(3),  DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(11), DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(11), DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(5),  DeviceId = 4, Value = 4 },
                new { Time = startDate.AddMinutes(5),  DeviceId = 4, Value = 4 },
                new { Time = startDate.AddMinutes(12), DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(12), DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(13), DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(13), DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(14), DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(14), DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(15), DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(15), DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(16), DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(16), DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(17), DeviceId = 4, Value = 4 },
                new { Time = startDate.AddMinutes(17), DeviceId = 4, Value = 4 },
                new { Time = startDate.AddMinutes(21), DeviceId = 6, Value = 6 },
                new { Time = startDate.AddMinutes(21), DeviceId = 6, Value = 6 },
            }
            .Select(e => PartitionedStreamEvent.CreateInterval(e.DeviceId, e.Time.Ticks, StreamEvent.MaxSyncTime, e.Value));

            var res = new List<Notification<PartitionedStreamEvent<int, int>>>();

            using (data.ToObservable()
                .ToStreamable(
                    DisorderPolicy.Adjust(0),
                    PartitionedFlushPolicy.FlushOnLowWatermark,
                    PeriodicPunctuationPolicy.Time(36000))
                .AlterEventDuration(1)
                .ToStreamEventObservable()
                .Subscribe(res.NotificationListObserver()))
            {
            }

            Assert.IsTrue(res.Count > 0, "There should be some results.");
        }

        [TestMethod, TestCategory("Gated")]
        public void PartitionedStitch()
        {
            var subject = new Subject<PartitionedStreamEvent<string, string>>();

            var qc = new QueryContainer();
            var input = qc.RegisterInput(subject);

            var output = new List<PartitionedStreamEvent<string, string>>();
            var egress = qc.RegisterOutput(input.Stitch()).ForEachAsync(o => output.Add(o));
            var process = qc.Restore();

            var payload = new[] { "c1payload", "c2payload" };

            // c1 - [1,7),[7,10),[11,12) => [1,10),[11,12)
            subject.OnNext(PartitionedStreamEvent.CreateStart("c1", 1, payload: payload[0]));
            subject.OnNext(PartitionedStreamEvent.CreateEnd("c1", 7, 1, payload: payload[0]));
            subject.OnNext(PartitionedStreamEvent.CreateStart("c1", 7, payload: payload[0]));
            subject.OnNext(PartitionedStreamEvent.CreateEnd("c1", 10, 7, payload: payload[0]));
            subject.OnNext(PartitionedStreamEvent.CreateStart("c1", 11, payload: payload[0]));
            subject.OnNext(PartitionedStreamEvent.CreateEnd("c1", 12, 11, payload: payload[0]));

            // c2 - [2,3),[5,10),[10,12) => [2,3),[5,12)
            subject.OnNext(PartitionedStreamEvent.CreateStart("c2", 2, payload: payload[1]));
            subject.OnNext(PartitionedStreamEvent.CreateEnd("c2", 3, 2, payload: payload[1]));
            subject.OnNext(PartitionedStreamEvent.CreateStart("c2", 5, payload: payload[1]));
            subject.OnNext(PartitionedStreamEvent.CreateEnd("c2", 10, 5, payload: payload[1]));
            subject.OnNext(PartitionedStreamEvent.CreateStart("c2", 10, payload: payload[1]));
            subject.OnNext(PartitionedStreamEvent.CreateEnd("c2", 12, 10, payload: payload[1]));

            subject.OnCompleted();

            process.Flush();

            var expected = new[]
            {
                new List<PartitionedStreamEvent<string, string>>
                {
                    PartitionedStreamEvent.CreateStart("c1", 1, payload: payload[0]),
                    PartitionedStreamEvent.CreateEnd("c1", 10, 1, payload: payload[0]),
                    PartitionedStreamEvent.CreateStart("c1", 11, payload: payload[0]),
                    PartitionedStreamEvent.CreateEnd("c1", 12, 11, payload: payload[0]),
                },
                new List<PartitionedStreamEvent<string, string>>
                {
                    PartitionedStreamEvent.CreateStart("c2", 2, payload: payload[1]),
                    PartitionedStreamEvent.CreateEnd("c2", 3, 2, payload: payload[1]),
                    PartitionedStreamEvent.CreateStart("c2", 5, payload: payload[1]),
                    PartitionedStreamEvent.CreateEnd("c2", 12, 5, payload: payload[1]),
                },
            };

            var outputData = new[]
            {
                output.Where(o => o.IsData && o.PartitionKey == "c1").ToList(),
                output.Where(o => o.IsData && o.PartitionKey == "c2").ToList(),
            };

            Assert.IsTrue(expected[0].SequenceEqual(outputData[0]));
            Assert.IsTrue(expected[1].SequenceEqual(outputData[1]));
        }
    }
}
