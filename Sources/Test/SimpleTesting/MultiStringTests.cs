// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Internal.Collections;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class MultiStringTests : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public MultiStringTests() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
       { }

        public struct StructContainingString
        {
            public int field1;
            public string field2;
            public override string ToString()
            {
                var s = string.Format("StructContainingString() {{ field1 = {0}, field2 = \"{1}\" }}", this.field1, this.field2);
                return s;
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString_Contains_01() // tests an op that is implemented by MultiString
        {
            var input = Enumerable.Range(0, 20).Select(i => new StructContainingString() { field1 = i, field2 = i.ToString(), });
            Expression<Func<StructContainingString, bool>> query = r => r.field2.Contains("1");

            var streamResult = input
                .ToStreamable()
                .Where(query)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var expected = input.Where(query.Compile());
            Assert.IsTrue(expected.SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString02() // tests an op that is not implemented by MultiString
        {
            var input = Enumerable.Range(0, 20).Select(i => new StructContainingString() { field1 = i, field2 = i.ToString(), });
            Expression<Func<StructContainingString, bool>> query = r => r.field2.Length > 1 && r.field2.Substring(0, 2) == "11";

            var streamResult = input
                .ToStreamable()
                .Where(query)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var expected = input.Where(query.Compile());
            Assert.IsTrue(expected.SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString03() // ingress through ObservableCache
        {
            var input = Enumerable.Range(0, 20).Select(i => new StructContainingString() { field1 = i, field2 = i.ToString(), });
            Expression<Func<StructContainingString, bool>> query = r => r.field2.Contains("1");

            var cached = input
                .ToObservable();
            var cachedStream = cached
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var streamResult = cachedStream
                .Where(query)
                .ToPayloadEnumerable();
            var a = streamResult.ToArray();
            var expected = input.Where(query.Compile());
            Assert.IsTrue(expected.SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString_Direct_Contains_01()
        {
            for (int j = 0; j < 11; j++)
            {
                var targetString = j < 10 ? j.ToString() : "x"; // something not found for the last iteration
                var cap = new CharArrayPool();
                var intPool = new ColumnPool<int>();
                var shortPool = new ColumnPool<short>();
                var bitvectorPool = new ColumnPool<long>(1 + (Config.DataBatchSize >> 6));

                var ms = new MultiString(cap, intPool, shortPool, bitvectorPool);
                var input = new string[20];
                for (int i = 0; i < 20; i++)
                {
                    var s = i.ToString();
                    input[i] = s;
                    ms.AddString(s);
                }
                ms.Seal();
                bitvectorPool.Get(out var inBV);
                var result = ms.Contains(targetString, inBV, false);
                var output = new List<string>();
                for (int i = 0; i < 20; i++)
                {
                    if ((result.col[i >> 6] & (1L << (i & 0x3f))) == 0)
                    {
                        output.Add(ms[i]);
                    }
                }
                var expected = input.Where(e => e.Contains(targetString));
                Assert.IsTrue(expected.SequenceEqual(output));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString_Direct_Equals_01()
        {
            for (int j = 0; j < 21; j++)
            {
                var targetString = j < 20 ? j.ToString() : "x"; // something not found for the last iteration
                var cap = new CharArrayPool();
                var intPool = new ColumnPool<int>();
                var shortPool = new ColumnPool<short>();
                var bitvectorPool = new ColumnPool<long>(1 + (Config.DataBatchSize >> 6));
                var ms = new MultiString(cap, intPool, shortPool, bitvectorPool);
                var input = new string[20];
                for (int i = 0; i < 20; i++)
                {
                    var s = i.ToString();
                    input[i] = s;
                    ms.AddString(s);
                }
                ms.Seal();
                bitvectorPool.Get(out var inBV);
                var result = ms.Equals(targetString, inBV, false);
                var output = new List<string>();
                for (int i = 0; i < 20; i++)
                {
                    if ((result.col[i >> 6] & (1L << (i & 0x3f))) == 0)
                    {
                        output.Add(ms[i]);
                    }
                }
                var expected = input.Where(e => e.Equals(targetString));
                Assert.IsTrue(expected.SequenceEqual(output));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString05() // tests apply boolean on a single method call
        {
            var input = Enumerable.Range(0, 20).Select(i => new StructContainingString() { field1 = i, field2 = i.ToString(), });
            Expression<Func<StructContainingString, bool>> query = r => r.field2.StartsWith("a");

            var streamResult = input
                .ToStreamable()
                .Where(query)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var expected = input.Where(query.Compile());
            Assert.IsTrue(expected.SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString06() // tests apply boolean on a boolean expression containing 2 method calls
        {
            var input = Enumerable.Range(0, 20).Select(i => new StructContainingString() { field1 = i, field2 = i.ToString(), });
            Expression<Func<StructContainingString, bool>> query = r => r.field2.StartsWith("a") && r.field2.EndsWith("z");

            var streamResult = input
                .ToStreamable()
                .Where(query)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var expected = input.Where(query.Compile());
            Assert.IsTrue(expected.SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString07() // tests apply boolean on a boolean expression containing 3 method calls
        {
            var input = Enumerable.Range(0, 20).Select(i => new StructContainingString() { field1 = i, field2 = i.ToString(), });
            Expression<Func<StructContainingString, bool>> query = r => r.field2.StartsWith("1") && (r.field2.EndsWith("3") || r.field2.EndsWith("5"));

            var streamResult = input
                .ToStreamable()
                .Where(query)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var expected = input.Where(query.Compile());
            Assert.IsTrue(expected.SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString08() // tests vectorization of Regex.IsMatch
        {
            var input = Enumerable.Range(0, 20).Select(i => new StructContainingString() { field1 = i, field2 = i.ToString(), });
            Expression<Func<StructContainingString, bool>> query = e => System.Text.RegularExpressions.Regex.IsMatch(e.field2, @"\d{2}");

            var streamResult = input
                .ToStreamable()
                .Where(query)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var expected = input.Where(query.Compile());
            Assert.IsTrue(expected.SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString_IndexOf_01() // tests IndexOf(char)
        {
            var input = Enumerable.Range(0, 20).Select(i => new StructContainingString() { field1 = i, field2 = i.ToString(), });
            Expression<Func<StructContainingString, bool>> query = r => r.field2.IndexOf('7') > 0;

            var streamResult = input
                .ToStreamable()
                .Where(query)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var expected = input.Where(query.Compile());
            Assert.IsTrue(expected.SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString_IndexOf_02() // tests IndexOf(string)
        {
            var input = Enumerable.Range(0, 20).Select(i => new StructContainingString() { field1 = i, field2 = i.ToString(), });
            Expression<Func<StructContainingString, bool>> query = r => r.field2.IndexOf("7") > 0;

            var streamResult = input
                .ToStreamable()
                .Where(query)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var expected = input.Where(query.Compile());
            Assert.IsTrue(expected.SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString_IndexOf_03() // tests IndexOf(char, int32)
        {
            var input = Enumerable.Range(0, 20).Select(i => new StructContainingString() { field1 = i, field2 = i.ToString(), });
            Expression<Func<StructContainingString, bool>> query = r => r.field2.Length > 1 && r.field2.IndexOf('7', 1) > 0;

            var streamResult = input
                .ToStreamable()
                .Where(query)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var expected = input.Where(query.Compile());
            Assert.IsTrue(expected.SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString_IndexOf_04() // tests IndexOf(string, int32)
        {
            var input = Enumerable.Range(0, 200).Select(i => new StructContainingString() { field1 = i, field2 = i.ToString(), });
            Expression<Func<StructContainingString, bool>> query = r => r.field2.Length > 1 && r.field2.IndexOf("7", 1) > 0;

            var streamResult = input
                .ToStreamable()
                .Where(query)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var expected = input.Where(query.Compile());
            Assert.IsTrue(expected.SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString_IndexOf_05() // tests IndexOf(string, StringComparison)
        {
            var input1 = Enumerable.Range(0, 26).Select(i => ((char)('a' + i)).ToString());
            var input2 = Enumerable.Range(0, 26).Select(i => ((char)('A' + i)).ToString());
            var input = input1.Concat(input2).Select(s => new StructContainingString() { field1 = 3, field2 = s, });
            Expression<Func<StructContainingString, bool>> query = r => r.field2.IndexOf("a", StringComparison.OrdinalIgnoreCase) >= 0;

            var streamResult = input
                .ToStreamable()
                .Where(query)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var expected = input.Where(query.Compile());
            Assert.IsTrue(expected.SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString_IndexOf_06() // tests IndexOf(string, int, int)
        {
            var input = Enumerable.Range(0, 200).Select(i => new StructContainingString() { field1 = i, field2 = i.ToString(), });
            Expression<Func<StructContainingString, bool>> query = r => r.field2.Length > 1 && r.field2.IndexOf("7", 1, 1) > 0;

            var streamResult = input
                .ToStreamable()
                .Where(query)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var expected = input.Where(query.Compile());
            Assert.IsTrue(expected.SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString_LastIndexOf_01() // tests LastIndexOf(char)
        {
            var input = Enumerable.Range(0, 20).Select(i => new StructContainingString() { field1 = i, field2 = i.ToString(), });
            Expression<Func<StructContainingString, bool>> query = r => r.field2.LastIndexOf('7') > 0;

            var streamResult = input
                .ToStreamable()
                .Where(query)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var expected = input.Where(query.Compile());
            Assert.IsTrue(expected.SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString_LastIndexOf_02() // tests LastIndexOf(string)
        {
            var input = Enumerable.Range(0, 20).Select(i => new StructContainingString() { field1 = i, field2 = i.ToString(), });
            Expression<Func<StructContainingString, bool>> query = r => r.field2.LastIndexOf("7") > 0;

            var streamResult = input
                .ToStreamable()
                .Where(query)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var expected = input.Where(query.Compile());
            Assert.IsTrue(expected.SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString_LastIndexOf_03() // tests LastIndexOf(char, int32)
        {
            var input = Enumerable.Range(0, 20).Select(i => new StructContainingString() { field1 = i, field2 = i.ToString(), });
            Expression<Func<StructContainingString, bool>> query = r => r.field2.LastIndexOf('7', r.field2.Length - 1) > 0;

            var streamResult = input
                .ToStreamable()
                .Where(query)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var expected = input.Where(query.Compile());
            var b = expected.ToArray();
            Assert.IsTrue(expected.SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString_LastIndexOf_04() // tests LastIndexOf(string, int32)
        {
            var input = Enumerable.Range(0, 200).Select(i => new StructContainingString() { field1 = i, field2 = i.ToString(), });
            Expression<Func<StructContainingString, bool>> query = r => r.field2.Length > 1 && r.field2.LastIndexOf("7", 1) > 0;

            var streamResult = input
                .ToStreamable()
                .Where(query)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var expected = input.Where(query.Compile());
            Assert.IsTrue(expected.SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString_LastIndexOf_05() // tests LastIndexOf(string, StringComparison)
        {
            var input1 = Enumerable.Range(0, 26).Select(i => ((char)('a' + i)).ToString());
            var input2 = Enumerable.Range(0, 26).Select(i => ((char)('A' + i)).ToString());
            var input = input1.Concat(input2).Select(s => new StructContainingString() { field1 = 3, field2 = s, });
            Expression<Func<StructContainingString, bool>> query = r => r.field2.LastIndexOf("a", StringComparison.OrdinalIgnoreCase) > 0;

            var streamResult = input
                .ToStreamable()
                .Where(query)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var expected = input.Where(query.Compile());
            Assert.IsTrue(expected.SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString_LastIndexOf_06() // tests LastIndexOf(string, int, int)
        {
            var input = Enumerable.Range(0, 200).Select(i => new StructContainingString() { field1 = i, field2 = i.ToString(), });
            Expression<Func<StructContainingString, bool>> query = r => r.field2.Length > 1 && r.field2.LastIndexOf("7", 1, 1) > 0;

            var streamResult = input
                .ToStreamable()
                .Where(query)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var expected = input.Where(query.Compile());
            Assert.IsTrue(expected.SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString_Length_01() // tests Length
        {
            var input = Enumerable.Range(0, 20).Select(i => new StructContainingString() { field1 = i, field2 = i.ToString(), });
            Expression<Func<StructContainingString, bool>> query = r => r.field2.Length > 1;

            var streamResult = input
                .ToStreamable()
                .Where(query)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var expected = input.Where(query.Compile());
            Assert.IsTrue(expected.SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString_WrapperAndIndexer_01() // tests a method for which no wrapper exists
        {
            var input = Enumerable.Range(0, 20).Select(i => new StructContainingString() { field1 = i, field2 = i.ToString(), });
            Expression<Func<StructContainingString, bool>> query = r => r.field2.IndexOf("7") > 1 && r.field2.IndexOfAny(new char[] { '2', '3' }) > 0;

            var streamResult = input
                .ToStreamable()
                .Where(query)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var expected = input.Where(query.Compile());
            Assert.IsTrue(expected.SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString_EqualsAndContains_01() // tests calling two vectorizable operations
        {
            var input = Enumerable.Range(0, 20).Select(i => new StructContainingString() { field1 = i, field2 = i.ToString(), });

            Expression<Func<StructContainingString, bool>> query = r => r.field2.Equals("7") || r.field2.Contains("2");

            var streamResult = input
                .ToStreamable()
                .Where(query)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var expected = input.Where(query.Compile());
            Assert.IsTrue(expected.SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString_EqualsAndContains_02() // tests calling two vectorizable operations
        {
            var input = Enumerable.Range(0, 20).Select(i => new StructContainingString() { field1 = i, field2 = i.ToString(), });

            Expression<Func<StructContainingString, bool>> query = r => r.field2.Contains("2") && r.field2.Contains("1");

            var streamResult = input
                .ToStreamable()
                .Where(query)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var expected = input.Where(query.Compile());
            Assert.IsTrue(expected.SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString_EqualsAndContains_03() // tests calling two vectorizable operations
        {
            var input = Enumerable.Range(0, 20).Select(i => new StructContainingString() { field1 = i, field2 = i.ToString(), });

            Expression<Func<StructContainingString, bool>> query = r => r.field2.Contains("2") || r.field2.Contains("1");

            var streamResult = input
                .ToStreamable()
                .Where(query)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var expected = input.Where(query.Compile());
            Assert.IsTrue(expected.SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString_EqualsAndContains_04() // tests calling two vectorizable operations
        {
            var input = Enumerable.Range(0, 20).Select(i => new StructContainingString() { field1 = i, field2 = i.ToString(), });

            Expression<Func<StructContainingString, bool>> query = r => r.field2.Equals("7") || (r.field2.Contains("2") && r.field2.Contains("1"));

            var streamResult = input
                .ToStreamable()
                .Where(query)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var expected = input.Where(query.Compile());
            Assert.IsTrue(expected.SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString_EqualsAndContains_05() // tests calling two vectorizable operations
        {
            var input = Enumerable.Range(0, 20).Select(i => new StructContainingString() { field1 = i, field2 = i.ToString(), });

            Expression<Func<StructContainingString, bool>> query = r => (r.field2.Contains("7") || r.field2.Contains("2")) && r.field2.Contains("1");

            var streamResult = input
                .ToStreamable()
                .Where(query)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var expected = input.Where(query.Compile());
            Assert.IsTrue(expected.SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString_Select_AnonymousType_Result_01()
        {
            var input = Enumerable.Range(0, 20).Select(i => new StructContainingString() { field1 = i, field2 = i.ToString(), });

            Assert.IsTrue(input.TestSelect(r => new { A = r.field2.IndexOf("7"), B = r.field1 / 2, }));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString_Select_AtomicType_Result_01()
        {
            var input = Enumerable.Range(0, 20).Select(i => new StructContainingString() { field1 = i, field2 = i.ToString(), });

            Assert.IsTrue(input.TestSelect(r => r.field2.IndexOf("7")));
        }

        public class ContainerForThisOneTest
        {
            public int A;

            public override bool Equals(object obj) => obj is ContainerForThisOneTest other && other.A == this.A;

            public override int GetHashCode() => this.A.GetHashCode();

        }
        [TestMethod, TestCategory("Gated")]
        public void MultiString_Select_NamedType_Result_01()
        {
            var input = Enumerable.Range(0, 20).Select(i => new StructContainingString() { field1 = i, field2 = i.ToString(), });

            Assert.IsTrue(input.TestSelect(r => new ContainerForThisOneTest { A = r.field2.IndexOf("7"), }));
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiStringRegex1()
        {
            var savedBatchSize = Config.DataBatchSize;
            Config.DataBatchSize = 80000;

            try
            {
                var cap = new CharArrayPool();
                var intPool = new ColumnPool<int>();
                var shortPool = new ColumnPool<short>();
                var bitvectorPool = new ColumnPool<long>(1 + (Config.DataBatchSize >> 6));

                string[] patterns = new string[] { "bb*?abb", "bb*abb", "ab*?a", "aba", "ab+a" };

                for (int r = 0; r < 10; r++)
                {
                    int numStrings = 10000 + r * 500;
                    int maxStringLen = 10 + r * 10;

                    var rand = new Random(300 + r);

                    var ms = new MultiString(cap, intPool, shortPool, bitvectorPool);
                    var input = new string[numStrings];
                    for (int i = 0; i < numStrings; i++)
                    {
                        string s = string.Empty;
                        for (int j = 0; j <= rand.Next(maxStringLen); j++)
                        {
                            s += (rand.NextDouble() < 0.75 ? "a" : "b");
                        }
                        input[i] = s;
                        ms.AddString(s);
                    }
                    ms.Seal();

                    foreach (var pattern in patterns)
                    {
                        // contains test
                        bitvectorPool.Get(out var inBV);
                        var result = ms.Contains(pattern, inBV, false);
                        for (int i = 0; i < numStrings; i++)
                        {
                            if ((result.col[i >> 6] & (1L << (i & 0x3f))) == 0)
                            {
                                Assert.IsTrue(input[i].Contains(pattern));
                            }
                            else
                            {
                                Assert.IsFalse(input[i].Contains(pattern));
                            }
                        }
                        inBV.ReturnClear();
                        result.ReturnClear();

                        // regex test
                        var reg = new System.Text.RegularExpressions.Regex(pattern);
                        bitvectorPool.Get(out inBV);
                        var result2 = ms.IsMatch(reg, 0, inBV, false);
                        for (int i = 0; i < numStrings; i++)
                        {
                            if ((result2.col[i >> 6] & (1L << (i & 0x3f))) == 0)
                            {
                                Assert.IsTrue(reg.IsMatch(input[i]));
                            }
                            else
                            {
                                Assert.IsFalse(reg.IsMatch(input[i]));
                            }
                        }
                        inBV.ReturnClear();
                        result2.ReturnClear();
                    }
                }
            }
            finally
            {
                Config.DataBatchSize = savedBatchSize;
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void MultiString_Direct_GetHashCode_01()
        {
            for (int j = 0; j < 11; j++)
            {
                var cap = new CharArrayPool();
                var intPool = new ColumnPool<int>();
                var shortPool = new ColumnPool<short>();
                var bitvectorPool = new ColumnPool<long>(1 + (Config.DataBatchSize >> 6));
                var ms = new MultiString(cap, intPool, shortPool, bitvectorPool);
                var input = Enumerable.Range(0, 20).Select(i => i.ToString());
                foreach (var s in input)
                {
                    ms.AddString(s);
                }
                ms.Seal();
                bitvectorPool.Get(out var inBV);
                var result = ms.GetHashCode(inBV);
                var output = new List<int>();
                for (int i = 0; i < input.Count(); i++)
                    output.Add(result.col[i]);
                var expected = input.Select(s => s.StableHash());
                Assert.IsTrue(expected.SequenceEqual(output));
            }
        }

    }
}
