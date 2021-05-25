// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************

using System.Collections.Generic;
using System.IO;
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
    }

    [DataContract]
    [KnownType(typeof(TestInterfaceImpl))]
    public class TestClass
    {
        [DataMember]
        public ITestInterface TestMember { get; set; }
    }

    [TestClass]
    public class NestedTypeSerializationTest : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        [TestMethod, TestCategory("Gated")]
        public void EnumerableSerialization() => TestSerialization(new List<TestClass> { GetTestData() });

        [TestMethod, TestCategory("Gated")]
        public void ArraySerialization() => TestSerialization(new TestClass[] { GetTestData() });

        [TestMethod, TestCategory("Gated")]
        public void DictionarySerialization()
        {
            var dict = new Dictionary<string, TestClass>();
            dict.Add("foo", GetTestData());
            TestSerialization(dict);
        }

        private static void TestSerialization<T>(T value)
        {
            var serializer = StreamSerializer.Create<T>();
            using (var stream = new MemoryStream())
            {
                serializer.Serialize(stream, value);
                stream.Flush();
                stream.Seek(0, SeekOrigin.Begin);

                var copy = serializer.Deserialize(stream);

                Assert.IsTrue(value.ToString() == copy.ToString());
            }
        }

        private TestClass GetTestData() => new TestClass { TestMember = new TestInterfaceImpl { Number = 1 } };
    }
}
