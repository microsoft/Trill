// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************

using Microsoft.StreamProcessing.Internal.Collections;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class EndPointQueueTest : TestWithConfigSettingsAndMemoryLeakDetection
    {
        [TestMethod, TestCategory("Gated")]
        public void TestEndPointQueue()
        {
            SimpleEndPointQueueTest();
            SequentialEndPointQueueTest(100);
        }

        private static void SimpleEndPointQueueTest()
        {
            var queue = new EndPointQueue();

            Assert.IsTrue(queue.IsEmpty);
            Assert.AreEqual(0, queue.Count);
            Assert.IsFalse(queue.TryPeekNext(out _, out _));
            Assert.IsFalse(queue.TryGetNext(out _, out _));
            Assert.IsFalse(queue.TryGetNextExclusive(100, out _, out _));
            Assert.IsFalse(queue.TryGetNextInclusive(100, out _, out _));

            queue.Insert(5, 1000);

            Assert.IsFalse(queue.IsEmpty);
            Assert.AreEqual(1, queue.Count);
            Assert.IsFalse(queue.TryGetNextExclusive(1, out _, out _));
            Assert.IsFalse(queue.TryGetNextInclusive(1, out _, out _));
            Assert.IsTrue(queue.TryPeekNext(out var time, out var value));
            Assert.AreEqual(5, time);
            Assert.AreEqual(1000, value);

            Assert.IsTrue(queue.TryGetNext(out time, out value));
            Assert.AreEqual(5, time);
            Assert.AreEqual(1000, value);

            Assert.IsTrue(queue.IsEmpty);

            queue.Insert(3, 1001);
            queue.Insert(4, 1003);
            queue.Insert(5, 1002);

            Assert.IsFalse(queue.TryGetNextInclusive(2, out _, out _));
            Assert.IsTrue(queue.TryGetNextInclusive(3, out time, out value));
            Assert.AreEqual(3, time);
            Assert.AreEqual(1001, value);

            Assert.IsFalse(queue.TryGetNextExclusive(4, out _, out _));
            Assert.IsTrue(queue.TryGetNextExclusive(5, out time, out value));
            Assert.AreEqual(4, time);
            Assert.AreEqual(1003, value);

            Assert.IsTrue(queue.TryGetNextInclusive(100, out time, out value));
            Assert.AreEqual(5, time);
            Assert.AreEqual(1002, value);

            Assert.IsTrue(queue.IsEmpty);
        }

        private static void SequentialEndPointQueueTest(int size)
        {
            var heap = new EndPointQueue();

            for (int j = 0; j < size; j++)
            {
                heap.Insert(j, j);
            }

            for (int j = 0; j < size; j++)
            {
                Assert.IsTrue(heap.TryGetNext(out long time, out int value));
                Assert.AreEqual(j, time);
                Assert.AreEqual(j, value);
            }

            Assert.IsTrue(heap.IsEmpty);
        }
    }
}
