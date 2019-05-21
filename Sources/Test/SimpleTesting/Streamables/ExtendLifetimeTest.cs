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
    public class ExtendLifetimeTest : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private static readonly StreamEvent<string> END = StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime);
        private static readonly StreamEvent<StructTuple<string, int>> END2 = StreamEvent.CreatePunctuation<StructTuple<string, int>>(StreamEvent.InfinitySyncTime);

        [TestMethod, TestCategory("Gated")]
        public void ExtendTest1()
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
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateStart(2, "A"),
                StreamEvent.CreateStart(3, "A"),
                StreamEvent.CreateStart(4, "A"),
                StreamEvent.CreateEnd(5, 1, "A"),
                StreamEvent.CreateEnd(6, 2, "A"),
                StreamEvent.CreateEnd(7, 3, "A"),
                StreamEvent.CreateEnd(8, 4, "A"),
                END
            };

            var inputObservable = inputList.ToList().ToObservable();
            var container = new QueryContainer();
            var input = container.RegisterInput(inputObservable);

            input.SetProperty().IsIntervalFree(true);

            var outputStream = input.ExtendLifetime(3);

            var output = container.RegisterOutput(outputStream);
            var result = new List<StreamEvent<string>>();
            output.Subscribe(t => result.Add(t));

            container.Restore(null);

            Assert.IsTrue(result.SequenceEqual(compareTo));
        }

        [TestMethod, TestCategory("Gated")]
        public void ExtendTest2()
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
                StreamEvent.CreateInterval(1, 15, "A"),
                StreamEvent.CreateInterval(2, 20, "A"),
                StreamEvent.CreateInterval(3, 18, "A"),
                StreamEvent.CreateInterval(4, 16, "A"),
                StreamEvent.CreateInterval(8, 19, "A"),
                END
            };

            var input = inputList.ToList().ToObservable().ToStreamable();
            var outputStream = input.ExtendLifetime(10);

            Assert.IsTrue(outputStream.IsEquivalentTo(compareTo));
        }

        /// <summary>
        /// Same as ExtendTest2, but with columnar payload
        /// </summary>
        [TestMethod, TestCategory("Gated")]
        public void ExtendTest3()
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
                StreamEvent.CreateInterval(1, 15, payload),
                StreamEvent.CreateInterval(2, 20, payload),
                StreamEvent.CreateInterval(3, 18, payload),
                StreamEvent.CreateInterval(4, 16, payload),
                StreamEvent.CreateInterval(8, 19, payload),
                END2
            };

            var input = inputList.ToList().ToObservable().ToStreamable();
            var outputStream = input.ExtendLifetime(10);

            Assert.IsTrue(outputStream.Select(r => r.Item1).IsEquivalentTo(compareTo.Select(r => r.IsData ? StreamEvent.CreateInterval(r.SyncTime, r.OtherTime, r.Payload.Item1) : END).ToArray()));
        }

        [TestMethod, TestCategory("Gated")]
        public void ExtendTestNegative1()
        {
            // nothing interesting happens here
            var inputList = new[]
            {
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateStart(2, "A"),
                StreamEvent.CreateStart(3, "A"),
                StreamEvent.CreateStart(4, "A"),
                StreamEvent.CreateEnd(5, 1, "A"),
                StreamEvent.CreateEnd(6, 2, "A"),
                StreamEvent.CreateEnd(7, 3, "A"),
                StreamEvent.CreateEnd(8, 4, "A"),
            };

            var compareTo = new[]
            {
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateEnd(2, 1, "A"),
                StreamEvent.CreateStart(2, "A"),
                StreamEvent.CreateEnd(3, 2, "A"),
                StreamEvent.CreateStart(3, "A"),
                StreamEvent.CreateEnd(4, 3, "A"),
                StreamEvent.CreateStart(4, "A"),
                StreamEvent.CreateEnd(5, 4, "A"),
                END
            };

            var input = inputList.ToList().ToObservable().ToStreamable().SetProperty().IsIntervalFree(true);
            var outputStream = input.ExtendLifetime(-3);

            Assert.IsTrue(outputStream.IsEquivalentTo(compareTo));
        }

        [TestMethod, TestCategory("Gated")]
        public void ExtendTestNegative2()
        {
            // nothing interesting happens here
            var inputList = new[]
            {
                StreamEvent.CreateInterval(1, 15, "A"),
                StreamEvent.CreateInterval(2, 20, "A"),
                StreamEvent.CreateInterval(3, 18, "A"),
                StreamEvent.CreateInterval(4, 16, "A"),
                StreamEvent.CreateInterval(8, 19, "A"),
            };

            var compareTo = new[]
            {
                StreamEvent.CreateInterval(1, 5, "A"),
                StreamEvent.CreateInterval(2, 10, "A"),
                StreamEvent.CreateInterval(3, 8, "A"),
                StreamEvent.CreateInterval(4, 6, "A"),
                StreamEvent.CreateInterval(8, 9, "A"),
                END
            };

            var input = inputList.ToList().ToObservable().ToStreamable();
            var outputStream = input.ExtendLifetime(-10);

            Assert.IsTrue(outputStream.IsEquivalentTo(compareTo));
        }

        [TestMethod, TestCategory("Gated")]
        public void ExtendTestNegative3()
        {
            // nothing interesting happens here
            var inputList = new[]
            {
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateStart(2, "A"),
                StreamEvent.CreateStart(3, "A"),
                StreamEvent.CreateStart(4, "A"),
                StreamEvent.CreateEnd(5, 1, "A"),
                StreamEvent.CreateEnd(6, 2, "A"),
                StreamEvent.CreateEnd(17, 3, "A"),
                StreamEvent.CreateEnd(18, 4, "A"),
            };

            var compareTo = new[]
            {
                StreamEvent.CreateStart(3, "A"),
                StreamEvent.CreateStart(4, "A"),
                StreamEvent.CreateEnd(7, 3, "A"),
                StreamEvent.CreateEnd(8, 4, "A"),
                END
            };

            var input = inputList.ToList().ToObservable().ToStreamable().SetProperty().IsIntervalFree(true);
            var outputStream = input.ExtendLifetime(-10);

            Assert.IsTrue(outputStream.IsEquivalentTo(compareTo));
        }

        [TestMethod, TestCategory("Gated")]
        public void ExtendTestNegative4()
        {
            // nothing interesting happens here
            var inputList = new[]
            {
                StreamEvent.CreateInterval(1, 15, "A"),
                StreamEvent.CreateInterval(2, 20, "A"),
                StreamEvent.CreateInterval(3, 18, "A"),
                StreamEvent.CreateInterval(4, 16, "A"),
                StreamEvent.CreateInterval(8, 19, "A"),
            };

            var compareTo = new[]
            {
                StreamEvent.CreateInterval(2, 5, "A"),
                END
            };

            var input = inputList.ToList().ToObservable().ToStreamable();
            var outputStream = input.ExtendLifetime(-15);

            Assert.IsTrue(outputStream.IsEquivalentTo(compareTo));
        }

        /// <summary>
        /// Same as ExtendTestNegative4, but with a columnar payload
        /// </summary>
        [TestMethod, TestCategory("Gated")]
        public void ExtendTestNegative5()
        {
            var payload = StructTuple.Create("A", 3);
            var inputList = new[]
            {
                StreamEvent.CreateInterval(1, 15, payload),
                StreamEvent.CreateInterval(2, 20, payload),
                StreamEvent.CreateInterval(3, 18, payload),
                StreamEvent.CreateInterval(4, 16, payload),
                StreamEvent.CreateInterval(8, 19, payload),
            };

            var compareTo = new[]
            {
                StreamEvent.CreateInterval(2, 5, payload),
                END2
            };

            var input = inputList.ToList().ToObservable().ToStreamable();
            var outputStream = input.ExtendLifetime(-15);

            Assert.IsTrue(outputStream.Select(r => r.Item1).IsEquivalentTo(compareTo.Select(r => r.IsData ? StreamEvent.CreateInterval(r.SyncTime, r.OtherTime, r.Payload.Item1) : END).ToArray()));
        }
    }
}
