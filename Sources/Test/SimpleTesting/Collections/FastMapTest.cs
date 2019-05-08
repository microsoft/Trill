// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using Microsoft.StreamProcessing.Internal.Collections;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class FastMapTest : TestWithConfigSettingsAndMemoryLeakDetection
    {
        [TestMethod, TestCategory("Gated")]
        public void TestFastMap()
        {
            SimpleMapTest();
            ReuseTest();
            LargerTest();
        }

        private static void SimpleMapTest()
        {
            var map = new FastMap<string>();

            Assert.IsTrue(map.IsEmpty);
            Assert.AreEqual(0, map.Count);
            Assert.IsFalse(map.Find(5).Next(out _));
            Assert.IsFalse(map.Find(6).Next(out _));
            Assert.IsFalse(map.Find(7).Next(out _));

            int indexA5 = map.Insert(5, "a");
            Assert.IsFalse(map.IsEmpty);
            Assert.AreEqual(1, map.Count);
            Assert.IsTrue(map.Find(5).Next(out var index));
            Assert.AreEqual(indexA5, index);
            Assert.IsFalse(map.Find(6).Next(out _));
            Assert.IsFalse(map.Find(7).Next(out _));

            int indexB6 = map.Insert(6, "b");
            int indexA5Two = map.Insert(5, "a");
            int indexB5 = map.Insert(5, "b");

            Assert.IsTrue(indexA5 != indexA5Two);
            Assert.IsTrue(indexA5 != indexB5);
            Assert.IsTrue(indexA5Two != indexB5);

            Assert.IsFalse(map.IsEmpty);
            Assert.AreEqual(4, map.Count);

            var traverser = map.Find(5);
            Assert.IsTrue(traverser.Next(out index));
            Assert.AreEqual(indexB5, index);
            Assert.IsTrue(traverser.Next(out index));
            Assert.AreEqual(indexA5Two, index);
            Assert.IsTrue(traverser.Next(out index));
            Assert.AreEqual(indexA5, index);
            Assert.IsFalse(traverser.Next(out _));

            traverser = map.Find(6);
            Assert.IsTrue(traverser.Next(out index));
            Assert.AreEqual(indexB6, index);
            Assert.IsFalse(traverser.Next(out _));

            Assert.IsFalse(map.Find(7).Next(out _));

            map.Remove(indexA5);

            Assert.IsFalse(map.IsEmpty);
            Assert.AreEqual(3, map.Count);

            traverser = map.Find(5);
            Assert.IsTrue(traverser.Next(out index));
            Assert.AreEqual(indexB5, index);
            Assert.IsTrue(traverser.Next(out index));
            Assert.AreEqual(indexA5Two, index);
            Assert.IsFalse(traverser.Next(out _));

            traverser = map.Find(6);
            Assert.IsTrue(traverser.Next(out index));
            Assert.AreEqual(indexB6, index);
            Assert.IsFalse(traverser.Next(out _));

            Assert.IsFalse(map.Find(7).Next(out _));

            map.Remove(indexA5Two);
            map.Remove(indexB6);

            Assert.IsFalse(map.IsEmpty);
            Assert.AreEqual(1, map.Count);

            traverser = map.Find(5);
            Assert.IsTrue(traverser.Next(out index));
            Assert.AreEqual(indexB5, index);
            Assert.IsFalse(traverser.Next(out _));
            Assert.IsFalse(map.Find(6).Next(out _));
            Assert.IsFalse(map.Find(7).Next(out _));

            map.Remove(indexB5);

            Assert.IsTrue(map.IsEmpty);
            Assert.AreEqual(0, map.Count);
            Assert.IsFalse(map.Find(5).Next(out _));
            Assert.IsFalse(map.Find(6).Next(out _));
            Assert.IsFalse(map.Find(7).Next(out _));
        }

        private static void ReuseTest()
        {
            var map = new FastMap<string>(5);

            Assert.AreEqual(1, map.Insert(5, "0"));
            Assert.AreEqual(2, map.Insert(5, "1"));
            Assert.AreEqual(3, map.Insert(5, "2"));
            Assert.AreEqual(4, map.Insert(5, "3"));
            Assert.AreEqual(5, map.Insert(5, "4"));

            map.Remove(3);
            Assert.AreEqual(3, map.Insert(5, "5"));

            map.Remove(2);
            map.Remove(1);
            map.Remove(5);
            map.Remove(3);
            map.Remove(4);
            Assert.AreEqual(4, map.Insert(5, "6"));
            Assert.AreEqual(3, map.Insert(5, "7"));
            Assert.AreEqual(5, map.Insert(5, "8"));
            Assert.AreEqual(1, map.Insert(5, "9"));
            Assert.AreEqual(2, map.Insert(5, "10"));

            Assert.AreEqual(6, map.Insert(5, "11"));
        }

        private static void LargerTest()
        {
            var map = new FastMap<string>(1);

            const int Size = 100;
            for (int i = 0; i < Size; i++)
            {
                map.Insert(i, "one" + i);
                map.Insert(i, "two" + i);
            }

            for (int i = 0; i < Size; i++)
            {
                map.Insert(i, "three" + i);
            }

            Assert.AreEqual(Size * 3, map.Count);

            for (int i = 0; i < Size; i++)
            {
                var traverse = map.Find(i);

                bool hasOne = false;
                bool hasTwo = false;
                bool hasThree = false;
                for (int j = 0; j < 3; j++)
                {
                    Assert.IsTrue(traverse.Next(out var index));
                    if (map.Values[index] == "one" + i)
                    {
                        hasOne = true;
                    }
                    else if (map.Values[index] == "two" + i)
                    {
                        hasTwo = true;
                    }
                    else if (map.Values[index] == "three" + i)
                    {
                        hasThree = true;
                    }
                    else
                    {
                        Assert.Fail();
                    }
                }

                Assert.IsFalse(traverse.Next(out _));
                Assert.IsTrue(hasOne);
                Assert.IsTrue(hasTwo);
                Assert.IsTrue(hasThree);
            }

            for (int i = 0; i < Size; i++)
            {
                var traverse = map.Find(i);
                while (traverse.Next(out int index))
                {
                    if (map.Values[index] == "two" + i)
                    {
                        map.Remove(index);
                        break;
                    }
                }
            }

            Assert.AreEqual(Size * 2, map.Count);

            for (int i = 0; i < Size; i++)
            {
                var traverse = map.Find(i);

                bool hasOne = false;
                bool hasThree = false;
                for (int j = 0; j < 2; j++)
                {
                    Assert.IsTrue(traverse.Next(out var index));
                    if (map.Values[index] == "one" + i)
                    {
                        hasOne = true;
                    }
                    else if (map.Values[index] == "three" + i)
                    {
                        hasThree = true;
                    }
                    else
                    {
                        Assert.Fail();
                    }
                }

                Assert.IsFalse(traverse.Next(out _));
                Assert.IsTrue(hasOne);
                Assert.IsTrue(hasThree);
            }

            for (int i = 0; i < Size; i++)
            {
                var traverse = map.Find(i);
                while (traverse.Next(out int index))
                {
                    if (map.Values[index] == "one" + i)
                    {
                        map.Remove(index);
                        break;
                    }
                }
            }

            Assert.AreEqual(Size, map.Count);

            for (int i = 0; i < Size; i++)
            {
                var traverse = map.Find(i);

                Assert.IsTrue(traverse.Next(out int index));
                Assert.AreEqual("three" + i, map.Values[index]);
                Assert.IsFalse(traverse.Next(out _));
            }

            for (int i = 0; i < Size; i++)
            {
                var traverse = map.Find(i);
                while (traverse.Next(out int index))
                {
                    if (map.Values[index] == "three" + i)
                    {
                        map.Remove(index);
                        break;
                    }
                }
            }

            Assert.IsTrue(map.IsEmpty);
        }
    }
}
