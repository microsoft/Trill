// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using Microsoft.StreamProcessing.Internal.Collections;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ComponentTesting.Collections
{
    [TestClass]
    public static class FastLinkedListTest
    {
        public static void TestFastLinkedList()
        {
            SimpleListTest();
            BetterListTest();
        }

        private static void SimpleListTest()
        {
            var list = new FastLinkedList<string>(1);

            Assert.IsTrue(list.IsEmpty);
            Assert.AreEqual(0, list.Count);

            var iter = new FastLinkedList<string>.ListTraverser(list);
            Assert.IsFalse(iter.Next(out int index));

            int indexA = list.Insert("a");
            Assert.IsFalse(list.IsEmpty);
            Assert.AreEqual(1, list.Count);
            iter.Reset();
            Assert.IsTrue(iter.Next(out index));
            Assert.AreEqual(indexA, index);
            Assert.IsFalse(iter.Next(out index));


            int indexB = list.Insert("b");
            int indexA2 = list.Insert("a");
            int indexB2 = list.Insert("b");

            Assert.IsTrue(indexA != indexA2);
            Assert.IsTrue(indexA != indexB);
            Assert.IsTrue(indexA2 != indexB2);

            Assert.IsFalse(list.IsEmpty);
            Assert.AreEqual(4, list.Count);

            var iter2 = new FastLinkedList<string>.ListTraverser(list);
            Assert.IsTrue(iter2.Next(out index));
            Assert.AreEqual(indexB2, index);
            Assert.IsTrue(iter2.Next(out index));
            Assert.AreEqual(indexA2, index);
            Assert.IsTrue(iter2.Next(out index));
            Assert.AreEqual(indexB, index);
            Assert.IsTrue(iter2.Next(out index));
            Assert.AreEqual(indexA, index);
            Assert.IsFalse(iter2.Next(out index));

            list.Remove(indexA2);
            Assert.IsFalse(list.IsEmpty);
            Assert.AreEqual(3, list.Count);

            iter2 = new FastLinkedList<string>.ListTraverser(list);
            Assert.IsTrue(iter2.Next(out index));
            Assert.AreEqual(indexB2, index);
            Assert.IsTrue(iter2.Next(out index));
            Assert.AreEqual(indexB, index);
            Assert.IsTrue(iter2.Next(out index));
            Assert.AreEqual(indexA, index);
            Assert.IsFalse(iter2.Next(out index));

            int indexC = list.Insert("C");
            Assert.IsFalse(list.IsEmpty);
            Assert.AreEqual(4, list.Count);

            iter2 = new FastLinkedList<string>.ListTraverser(list);
            Assert.IsTrue(iter2.Next(out index));
            Assert.AreEqual(indexC, index);
            Assert.IsTrue(iter2.Next(out index));
            Assert.AreEqual(indexB2, index);
            Assert.IsTrue(iter2.Next(out index));
            Assert.AreEqual(indexB, index);
            iter2.Remove();

            Assert.IsTrue(iter2.Next(out index));
            Assert.AreEqual(indexA, index);
            Assert.IsFalse(iter2.Next(out index));

            list.Remove(indexA);
            list.Remove(indexC);
            list.Remove(indexB2);
            // list.Remove(indexB);

            Assert.IsTrue(list.IsEmpty);
            Assert.AreEqual(0, list.Count);

        }

        private static void BetterListTest()
        {
            FastLinkedList<int> list = new FastLinkedList<int>(1);

            long sum = 0;
            for (int i = 1; i <= 10000; i++)
            {
                sum += i;
                list.Insert(i);
            }

            /*for (int i = 1; i <= 10000; i+=10)
            {
                list.Remove(i);
                sum -= i;
            }*/

            Random r = new Random(0);
            int cnt = 1;
            while (cnt < 10000)
            {
                cnt += r.Next(1, 5);
                list.Remove(cnt);
                list.Remove(cnt);
                sum -= cnt;
            }

            FastLinkedList<int>.ListTraverser iter = new FastLinkedList<int>.ListTraverser(list);
            iter.Reset();

            long check = 0;
            while (iter.Next(out int index))
            {
                check += list.Values[index];
            }
            Assert.IsTrue(sum == check);
        }
    }
}
