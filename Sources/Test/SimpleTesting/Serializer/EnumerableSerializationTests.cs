// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing.Serializer;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public class TestEnumerable<T> : IEnumerable<T>
    {
        protected readonly List<T> data;

        public TestEnumerable() => this.data = new List<T>();

        public TestEnumerable(IEnumerable<T> data) => this.data = new List<T>(data);

        public void Add(T value) => this.data.Add(value);

        public IEnumerator<T> GetEnumerator() => this.data.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    public class TestCollection<T> : TestEnumerable<T>, ICollection<T>
    {
        public TestCollection()
        {
        }

        public TestCollection(IEnumerable<T> data) : base(data)
        {
        }

        public int Count => this.data.Count;

        public bool IsReadOnly => false;

        public void Clear() => this.data.Clear();
        public bool Contains(T item) => this.data.Contains(item);
        public void CopyTo(T[] array, int arrayIndex) => this.data.CopyTo(array, arrayIndex);
        public bool Remove(T item) => this.data.Remove(item);
    }

    public class TestList<T> : TestCollection<T>, IList<T>
    {
        public TestList()
        {
        }

        public TestList(IEnumerable<T> data) : base(data)
        {
        }

        public T this[int index] { get => this.data[index]; set => this.data[index] = value; }

        public int IndexOf(T item) => this.data.IndexOf(item);
        public void Insert(int index, T item) => this.data.Insert(index, item);
        public void RemoveAt(int index) => this.data.RemoveAt(index);
    }

    [TestClass]
    public class EnumerableSerializationTests : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        private const int SerializationCount = 10000;

        [TestMethod, TestCategory("Gated")]
        public void EnumerableSerialization() => TestSerialization(new TestEnumerable<int>(GetTestData()));

        [TestMethod, TestCategory("Gated")]
        public void CollectionSerialization() => TestSerialization(new TestCollection<int>(GetTestData()));

        [TestMethod, TestCategory("Gated")]
        public void ListSerialization() => TestSerialization(new TestList<int>(GetTestData()));

        private static void TestSerialization<T>(T enumerable)
            where T : IEnumerable<int>
        {
            var serializer = StreamableSerializer.Create<T>();
            using (var stream = new MemoryStream())
            {
                serializer.Serialize(stream, enumerable);
                stream.Flush();
                stream.Seek(0, SeekOrigin.Begin);

                var copy = serializer.Deserialize(stream);

                Assert.IsTrue(enumerable.SequenceEqual(copy));
            }
        }

        private List<int> GetTestData() => Enumerable.Range(0, SerializationCount).ToList();
    }
}