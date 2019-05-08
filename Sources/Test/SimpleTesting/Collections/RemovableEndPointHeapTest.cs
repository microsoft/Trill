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
    public class RemovableEndPointHeapTest : TestWithConfigSettingsAndMemoryLeakDetection
    {
        [TestMethod, TestCategory("Gated")]
        public void TestRemovableEndPointHeap()
        {
            SimpleRemovableEndPointHeapTest();
            RandomRemovableEndPointHeapTest(1000, 10, 100);
            RandomRemovableEndPointHeapTest(100, 10, 1000000);
            BugReproTest();
        }

        private static void BugReproTest()
        {
            var heap = new RemovableEndPointHeap();
            int index1 = heap.Insert(1, 1);
            int index2 = heap.Insert(2, 2);
            heap.Remove(index1);
            heap.Insert(3, 3);
            heap.Remove(index2);
            Assert.IsTrue(heap.TryGetNext(out long time, out int value));
            Assert.AreEqual(3L, time);
            Assert.AreEqual(3, value);
            Assert.IsTrue(heap.IsEmpty);
        }

        private static void SimpleRemovableEndPointHeapTest()
        {
            var heap = new RemovableEndPointHeap();

            int index1 = heap.Insert(5, 1000);
            heap.Remove(index1);
            Assert.IsTrue(heap.IsEmpty);
            Assert.AreEqual(0, heap.Count);
            Assert.IsFalse(heap.TryPeekNext(out _, out _));
            Assert.IsFalse(heap.TryGetNext(out _, out _));
            Assert.IsFalse(heap.TryGetNextExclusive(100, out _, out _));
            Assert.IsFalse(heap.TryGetNextInclusive(100, out _, out _));

            index1 = heap.Insert(6, 100);
            int index2 = heap.Insert(7, 200);
            heap.Remove(index2);
            Assert.IsFalse(heap.IsEmpty);
            Assert.AreEqual(1, heap.Count);
            Assert.IsFalse(heap.TryGetNextExclusive(1, out _, out _));
            Assert.IsFalse(heap.TryGetNextInclusive(1, out _, out _));
            Assert.IsTrue(heap.TryPeekNext(out var time, out var value));
            Assert.AreEqual(6, time);
            Assert.AreEqual(100, value);
            heap.Remove(index1);
            Assert.IsTrue(heap.IsEmpty);

            index1 = heap.Insert(8, 100);
            index2 = heap.Insert(9, 200);
            heap.Remove(index1);
            Assert.IsFalse(heap.IsEmpty);
            Assert.AreEqual(1, heap.Count);
            Assert.IsFalse(heap.TryGetNextExclusive(1, out _, out _));
            Assert.IsFalse(heap.TryGetNextInclusive(1, out _, out _));
            Assert.IsTrue(heap.TryPeekNext(out time, out value));
            Assert.AreEqual(9, time);
            Assert.AreEqual(200, value);
            heap.Remove(index2);
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

        private static void RandomRemovableEndPointHeapTest(int size, int minValue, int maxValue)
        {
            var heap = new RemovableEndPointHeap();

            var rand = new Random();
            var input = new int[size];
            var indexes = new int[size];
            for (int j = 0; j < size; j++)
            {
                int value = rand.Next(minValue, maxValue);
                indexes[j] = heap.Insert(value, value);
                input[j] = value;
            }

            Assert.AreEqual(size, heap.Count);

            for (int j = size / 2; j < size; j++)
            {
                heap.Remove(indexes[j]);
            }

            Assert.AreEqual(size / 2, heap.Count);

            Array.Sort(input, 0, size / 2);

            for (int j = 0; j < size / 2; j++)
            {
                Assert.IsTrue(heap.TryGetNext(out long time, out int value));
                Assert.AreEqual(input[j], time);
                Assert.AreEqual(input[j], value);
            }

            Assert.IsTrue(heap.IsEmpty);
        }
    }
}
