// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;

using Microsoft.StreamProcessing.Serializer;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting.Serializer
{
    public interface ITestInterface
    {
    }

    [DataContract]
    public class TestInterfaceImpl : ITestInterface
    {
        [DataMember]
        public int Number { get; set; }

        public override bool Equals(object obj)
        {
            return Equals(obj as TestInterfaceImpl);
        }

        public bool Equals(TestInterfaceImpl other)
        {
            return other != null && this.Number == other.Number;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = 41;
                hashCode = hashCode * 31 + EqualityComparer<int>.Default.GetHashCode(this.Number);
                return hashCode;
            }
        }
    }

    [DataContract]
    [KnownType(typeof(TestInterfaceImpl))]
    public class TestClass
    {
        [DataMember]
        public ITestInterface TestMember { get; set; }

        public override bool Equals(object obj)
        {
            return Equals(obj as TestClass);
        }

        public bool Equals(TestClass other)
        {
            return other != null && this.TestMember.Equals(other.TestMember);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = 41;
                hashCode = hashCode * 31 + this.TestMember.GetHashCode();
                return hashCode;
            }
        }
    }

    [TestClass]
    public class NestedTypeSerializationTest : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        private delegate bool CheckEquals<T>(T expected, T actual);

        [TestMethod, TestCategory("Gated")]
        public void EnumerableSerialization() => TestSerialization(new List<TestClass> { GetTestData() }, Enumerable.SequenceEqual);

        [TestMethod, TestCategory("Gated")]
        public void ArraySerialization() => TestSerialization(new TestClass[] { GetTestData() }, (expected, actual) => expected[0].Equals(actual[0]));

        [TestMethod, TestCategory("Gated")]
        public void DictionarySerialization()
        {
            var dict = new Dictionary<string, TestClass>();
            dict.Add("foo", GetTestData());
            TestSerialization(dict, (expected, actual) => expected["foo"].Equals(actual["foo"]));
        }

        private static void TestSerialization<T>(T value, CheckEquals<T> equals)
        {
            var serializer = StreamSerializer.Create<T>();
            using (var stream = new MemoryStream())
            {
                serializer.Serialize(stream, value);
                stream.Flush();
                stream.Seek(0, SeekOrigin.Begin);

                var copy = serializer.Deserialize(stream);

                Assert.IsTrue(equals(copy, value));
            }
        }

        private TestClass GetTestData() => new TestClass { TestMember = new TestInterfaceImpl { Number = 1 } };
    }
}
