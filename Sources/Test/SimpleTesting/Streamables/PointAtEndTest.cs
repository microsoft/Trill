// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class PointAtEndTest : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private static readonly StreamEvent<string> END = StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime);
        private static readonly StreamEvent<StructTuple<string, int>> END2 = StreamEvent.CreatePunctuation<StructTuple<string, int>>(StreamEvent.InfinitySyncTime);

        [TestMethod, TestCategory("Gated")]
        public void PointAtEndTest1()
        {
            var inputList = new[]
            {
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateEnd(2, 1, "A"),
                StreamEvent.CreateStart(2, "A"),
                StreamEvent.CreateEnd(3, 2, "A"),
                StreamEvent.CreateStart(3, "A"),
                StreamEvent.CreateEnd(4, 3, "A"),
                StreamEvent.CreateStart(4, "A"),
                StreamEvent.CreateEnd(5, 4, "A")
            };

            var compareTo = new[]
            {
                StreamEvent.CreatePoint(2, "A"),
                StreamEvent.CreatePoint(3, "A"),
                StreamEvent.CreatePoint(4, "A"),
                StreamEvent.CreatePoint(5, "A"),
                END
            };

            var input = inputList.ToList().ToObservable().ToStreamable().SetProperty().IsIntervalFree(true);
            var outputStream = input.PointAtEnd();

            Assert.IsTrue(outputStream.IsEquivalentTo(compareTo));
        }

        [TestMethod, TestCategory("Gated")]
        public void PointAtEndTest2()
        {
            // nothing interesting happens here
            var inputList = new[]
            {
                StreamEvent.CreateInterval(1, 5, "A"),
                StreamEvent.CreateInterval(2, 10, "A"),
                StreamEvent.CreateInterval(3, 8, "A"),
                StreamEvent.CreateInterval(4, 6, "A"),
                StreamEvent.CreateInterval(8, 9, "A"),
            };

            var compareTo = new[]
            {
                StreamEvent.CreatePoint(5, "A"),
                StreamEvent.CreatePoint(6, "A"),
                StreamEvent.CreatePoint(8, "A"),
                StreamEvent.CreatePoint(9, "A"),
                StreamEvent.CreatePoint(10, "A"),
                END
            };

            var inputObservable = inputList.ToList().ToObservable();

            var container = new QueryContainer();
            var input = container.RegisterInput(inputObservable);

            var outputStream = input.PointAtEnd();

            var output = container.RegisterOutput(outputStream);
            var result = new List<StreamEvent<string>>();
            output.Subscribe(t => result.Add(t));
            container.Restore(null);

            Assert.IsTrue(result.SequenceEqual(compareTo));
        }

        [TestMethod, TestCategory("Gated")]
        public void PointAtEndTest3()
        {
            // nothing interesting happens here
            var inputList = new[]
            {
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateEnd(2, 1, "A"),
                StreamEvent.CreateStart(2, "A"),
                StreamEvent.CreateEnd(3, 2, "A"),
                StreamEvent.CreateStart(3, "A"),
                StreamEvent.CreateEnd(4, 3, "A"),
                StreamEvent.CreateStart(4, "A"),
                StreamEvent.CreateEnd(5, 4, "A")
            };

            var compareTo = new[]
            {
                StreamEvent.CreatePoint(101, "A"),
                StreamEvent.CreatePoint(102, "A"),
                StreamEvent.CreatePoint(103, "A"),
                StreamEvent.CreatePoint(104, "A"),
                END
            };

            var input = inputList.ToList().ToObservable().ToStreamable()
                .AlterEventDuration(100);
            var outputStream = input.PointAtEnd();

            Assert.IsTrue(outputStream.IsEquivalentTo(compareTo));
        }

        /// <summary>
        /// Same as PointAtEndTest1, but with a columnar payload
        /// </summary>
        [TestMethod, TestCategory("Gated")]
        public void PointAtEndTest4()
        {
            var payload = StructTuple.Create("A", 3);
            var inputList = new[]
            {
                StreamEvent.CreateStart(1, payload),
                StreamEvent.CreateEnd(2, 1, payload),
                StreamEvent.CreateStart(2, payload),
                StreamEvent.CreateEnd(3, 2, payload),
                StreamEvent.CreateStart(3, payload),
                StreamEvent.CreateEnd(4, 3, payload),
                StreamEvent.CreateStart(4, payload),
                StreamEvent.CreateEnd(5, 4, payload)
            };

            var compareTo = new StreamEvent<StructTuple<string, int>>[]
            {
                StreamEvent.CreatePoint(2, payload),
                StreamEvent.CreatePoint(3, payload),
                StreamEvent.CreatePoint(4, payload),
                StreamEvent.CreatePoint(5, payload),
                END2
            };

            var input = inputList.ToList().ToObservable().ToStreamable().SetProperty().IsIntervalFree(true);
            var outputStream = input.PointAtEnd();

            Assert.IsTrue(outputStream.Select(r => r.Item1).IsEquivalentTo(compareTo.Select(r => r.IsData ? StreamEvent.CreatePoint(r.SyncTime, r.Payload.Item1) : END).ToArray()));
        }

        /// <summary>
        /// Same as PointAtEndTest2, but with a columnar payload
        /// </summary>
        [TestMethod, TestCategory("Gated")]
        public void PointAtEndTest5()
        {
            var payload = StructTuple.Create("A", 3);
            var inputList = new[]
            {
                StreamEvent.CreateInterval(1, 5, payload),
                StreamEvent.CreateInterval(2, 10, payload),
                StreamEvent.CreateInterval(3, 8, payload),
                StreamEvent.CreateInterval(4, 6, payload),
                StreamEvent.CreateInterval(8, 9, payload),
            };

            var compareTo = new StreamEvent<StructTuple<string, int>>[]
            {
                StreamEvent.CreatePoint(5, payload),
                StreamEvent.CreatePoint(6, payload),
                StreamEvent.CreatePoint(8, payload),
                StreamEvent.CreatePoint(9, payload),
                StreamEvent.CreatePoint(10, payload),
                END2
            };

            var input = inputList.ToList().ToObservable().ToStreamable();
            var outputStream = input.PointAtEnd();

            Assert.IsTrue(outputStream.Select(r => r.Item1).IsEquivalentTo(compareTo.Select(r => r.IsData ? StreamEvent.CreatePoint(r.SyncTime, r.Payload.Item1) : END).ToArray()));
        }

        /// <summary>
        /// Same as PointAtEndTest3, but with a columnar payload
        /// </summary>
        [TestMethod, TestCategory("Gated")]
        public void PointAtEndTest6()
        {
            var payload = StructTuple.Create("A", 3);
            var inputList = new[]
            {
                StreamEvent.CreateStart(1, payload),
                StreamEvent.CreateEnd(2, 1, payload),
                StreamEvent.CreateStart(2, payload),
                StreamEvent.CreateEnd(3, 2, payload),
                StreamEvent.CreateStart(3, payload),
                StreamEvent.CreateEnd(4, 3, payload),
                StreamEvent.CreateStart(4, payload),
                StreamEvent.CreateEnd(5, 4, payload)
            };

            var compareTo = new StreamEvent<StructTuple<string, int>>[]
            {
                StreamEvent.CreatePoint(101, payload),
                StreamEvent.CreatePoint(102, payload),
                StreamEvent.CreatePoint(103, payload),
                StreamEvent.CreatePoint(104, payload),
                END2
            };

            var input = inputList.ToList().ToObservable().ToStreamable()
                .AlterEventDuration(100);
            var outputStream = input.PointAtEnd();

            Assert.IsTrue(outputStream.Select(r => r.Item1).IsEquivalentTo(compareTo.Select(r => r.IsData ? StreamEvent.CreatePoint(r.SyncTime, r.Payload.Item1) : END).ToArray()));
        }
    }

}
