// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using Microsoft.StreamProcessing.Internal.Collections;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class EndPointHeapTest : TestWithConfigSettingsAndMemoryLeakDetection
    {
        [TestMethod, TestCategory("Gated")]
        public void TestEndPointHeap()
        {
            SimpleEndPointHeapTest();
            RandomEndPointHeapTest(1000, 10, 100);
            RandomEndPointHeapTest(100, 10, 1000000);
        }

        private static void SimpleEndPointHeapTest()
        {
            var heap = new EndPointHeap();

            Assert.IsTrue(heap.IsEmpty);
            Assert.AreEqual(0, heap.Count);
            Assert.IsFalse(heap.TryPeekNext(out _, out _));
            Assert.IsFalse(heap.TryGetNext(out _, out _));
            Assert.IsFalse(heap.TryGetNextExclusive(100, out _, out _));
            Assert.IsFalse(heap.TryGetNextInclusive(100, out _, out _));

            heap.Insert(5, 1000);

            Assert.IsFalse(heap.IsEmpty);
            Assert.AreEqual(1, heap.Count);
            Assert.IsFalse(heap.TryGetNextExclusive(1, out _, out _));
            Assert.IsFalse(heap.TryGetNextInclusive(1, out _, out _));
            Assert.IsTrue(heap.TryPeekNext(out var time, out var value));
            Assert.AreEqual(5, time);
            Assert.AreEqual(1000, value);

            Assert.IsTrue(heap.TryGetNext(out time, out value));
            Assert.AreEqual(5, time);
            Assert.AreEqual(1000, value);

            Assert.IsTrue(heap.IsEmpty);

            heap.Insert(3, 1001);
            heap.Insert(5, 1002);
            heap.Insert(4, 1003);

            Assert.IsFalse(heap.TryGetNextInclusive(2, out _, out _));
            Assert.IsTrue(heap.TryGetNextInclusive(3, out time, out value));
            Assert.AreEqual(3, time);
            Assert.AreEqual(1001, value);

            Assert.IsFalse(heap.TryGetNextExclusive(4, out _, out _));
            Assert.IsTrue(heap.TryGetNextExclusive(5, out time, out value));
            Assert.AreEqual(4, time);
            Assert.AreEqual(1003, value);

            Assert.IsTrue(heap.TryGetNextInclusive(100, out time, out value));
            Assert.AreEqual(5, time);
            Assert.AreEqual(1002, value);

            Assert.IsTrue(heap.IsEmpty);
        }

        private static void RandomEndPointHeapTest(int size, int minValue, int maxValue)
        {
            var heap = new EndPointHeap();

            var rand = new Random();
            var input = new int[size];
            for (int j = 0; j < size; j++)
            {
                int value = rand.Next(minValue, maxValue);
                heap.Insert(value, value);
                input[j] = value;
            }

            Assert.AreEqual(size, heap.Count);

            Array.Sort(input);

            for (int j = 0; j < size; j++)
            {
                Assert.IsTrue(heap.TryGetNext(out long time, out int value));
                Assert.AreEqual(input[j], time);
                Assert.AreEqual(input[j], value);
            }

            Assert.IsTrue(heap.IsEmpty);
        }
    }
}
