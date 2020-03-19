// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;
using Microsoft.StreamProcessing.Serializer;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public static class ClassWithNestedType1<T>
    {
        public class NestedType<U>
        {
            public T x;
            public U y;
        }
    }

    public static class ClassWithNestedType2<T, U>
    {
        public class NestedType
        {
            public T x;
            public U y;
        }
    }

    public class ClassWithAutoProps : IEqualityComparer<ClassWithAutoProps>
    {
        public int IntField;
        public string StringField;
        public string StringAutoProp { get; set; }
        public int IntAutoProp { get; set; }
        public bool Equals(ClassWithAutoProps a, ClassWithAutoProps b)
            => a.IntAutoProp == b.IntAutoProp &&
                a.IntField == b.IntField &&
                (a.StringAutoProp != null || b.StringAutoProp == null) &&
                (a.StringAutoProp == null || a.StringAutoProp.Equals(b.StringAutoProp)) &&
                (a.StringField != null || b.StringField == null) &&
                (a.StringField == null || a.StringField.Equals(b.StringField));

        public int GetHashCode(ClassWithAutoProps obj) => obj.IntField.GetHashCode();
        public override bool Equals(object obj) => obj is ClassWithAutoProps other ? Equals(this, other) : false;
        public override int GetHashCode() => GetHashCode(this);
    }

    public class ClassWithOnlyAutoProps : IEqualityComparer<ClassWithOnlyAutoProps>
    {
        public string StringAutoProp { get; set; }
        public int IntAutoProp { get; set; }
        public bool Equals(ClassWithOnlyAutoProps a, ClassWithOnlyAutoProps b)
            => a.IntAutoProp == b.IntAutoProp &&
                (a.StringAutoProp != null || b.StringAutoProp == null) &&
                (a.StringAutoProp == null || a.StringAutoProp.Equals(b.StringAutoProp));

        public int GetHashCode(ClassWithOnlyAutoProps obj) => obj.IntAutoProp.GetHashCode();
        public override bool Equals(object obj) => obj is ClassWithOnlyAutoProps other ? Equals(this, other) : false;
        public override int GetHashCode() => GetHashCode(this);
    }

    internal class ClassImplementingIComparable : IComparable<ClassImplementingIComparable>
    {
        public int CompareTo(ClassImplementingIComparable other) => 3;
    }

    internal class ClassImplementingGenericIComparer : IComparer<ClassImplementingGenericIComparer>
    {
        public int Compare(ClassImplementingGenericIComparer x, ClassImplementingGenericIComparer y) => 3;
    }

    internal class ClassImplementingNonGenericIComparer : System.Collections.IComparer
    {
        public int Compare(object x, object y) => 3;
    }

    public class SERListEqualityComparer : IEqualityComparer<StreamEvent<List<RankedEvent<char>>>>
    {
        public bool Equals(StreamEvent<List<RankedEvent<char>>> a, StreamEvent<List<RankedEvent<char>>> b)
        {
            if (a.SyncTime != b.SyncTime) return false;
            if (a.OtherTime != b.OtherTime) return false;

            if (a.Kind != b.Kind) return false;

            if (a.Kind != StreamEventKind.Punctuation)
            {
                if (a.Payload.Count != b.Payload.Count) return false;

                for (int j = 0; j < a.Payload.Count; j++)
                {
                    if (!a.Payload[j].Equals(b.Payload[j])) return false;
                }
            }

            return true;
        }

        public int GetHashCode(StreamEvent<List<RankedEvent<char>>> obj)
        {
            int ret = (int)(obj.SyncTime ^ obj.OtherTime);
            foreach (var l in obj.Payload)
            {
                ret ^= l.GetHashCode();
            }
            return ret;
        }
    }

    public struct Payload
    {
        public long field1;
        public long field2;
    }

    public class Event
    {
        public DateTime Vs;
        public long V;
        public int Key;
    }

    public struct ResultEvent
    {
        public int Key;
        public ulong Cnt;
    }

    public class PayloadWithFieldOfNestedType1
    {
        public ClassWithNestedType1<int>.NestedType<string> nt;

        public PayloadWithFieldOfNestedType1()
            => this.nt = new ClassWithNestedType1<int>.NestedType<string>();

        public PayloadWithFieldOfNestedType1(int i)
            => this.nt = new ClassWithNestedType1<int>.NestedType<string> { x = i };
    }

    public class PayloadWithFieldOfNestedType2
    {
        public ClassWithNestedType2<int, string>.NestedType nt;

        public PayloadWithFieldOfNestedType2()
            => this.nt = new ClassWithNestedType2<int, string>.NestedType();

        public PayloadWithFieldOfNestedType2(int i)
            => this.nt = new ClassWithNestedType2<int, string>.NestedType { x = i };
    }

    [TestClass]
    public class SimpleTestsFusedRow : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public SimpleTestsFusedRow() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        // Total number of input events
        private const int NumEvents = 1000;
        private readonly IEnumerable<MyStruct2> enumerable = Enumerable.Range(0, NumEvents)
            .Select(e =>
                new MyStruct2 { field1 = e, field2 = new MyString(Convert.ToString("string" + e)), field3 = new NestedStruct { nestedField = e } });

        public static bool MyIsEqual(List<RankedEvent<char>> a, List<RankedEvent<char>> b)
        {
            if (a.Count != b.Count) return false;

            for (int j = 0; j < a.Count; j++)
            {
                if (!a[j].Equals(b[j])) return false;
            }

            return true;
        }

        public static int MyGetHashCode(List<RankedEvent<char>> a)
        {
            int ret = 0;
            foreach (var l in a)
            {
                ret ^= l.GetHashCode();
            }
            return ret;
        }

        private void TestWhere(Expression<Func<MyStruct2, bool>> predicate, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhere(predicate, disableFusion));

        private void TestSelect<TResult>(Expression<Func<MyStruct2, TResult>> function, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestSelect(function, disableFusion));

        public void TestWhereSelect<U>(Expression<Func<MyStruct2, bool>> wherePredicate, Expression<Func<MyStruct2, U>> selectFunction, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhereSelect(wherePredicate, selectFunction, disableFusion));

        [TestMethod, TestCategory("Gated")]
        public void Where1FusedRow()
            => TestWhere(e => e.field2.mystring.Contains("string"), false);

        [TestMethod, TestCategory("Gated")]
        public void Where2FusedRow()
            => TestWhere(e => e.field2.mystring.Equals("string0"), false);

        [TestMethod, TestCategory("Gated")]
        public void Where3FusedRow()
            => TestWhere(e => e.field2.mystring.Equals("string0"), false);

        [TestMethod, TestCategory("Gated")]
        public void Where4FusedRow()
            => TestWhere(e => e.field3.nestedField == 0, false);

        [TestMethod, TestCategory("Gated")]
        public void Where5FusedRow()
            => TestWhere(e => true, false);

        [TestMethod, TestCategory("Gated")]
        public void Where6FusedRow()
            => TestWhere(e => false, false);

        [TestMethod, TestCategory("Gated")]
        public void Where7FusedRow()
            => TestWhere(e => e.field3.nestedField == 0 || e.field3.nestedField == 1, false);

        [TestMethod, TestCategory("Gated")]
        public void Where8FusedRow()
            => TestWhere(e => string.Compare(e.field2.mystring, "string0") == 0, false);

        [TestMethod, TestCategory("Gated")]
        public void Where9FusedRow()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Where(v => v % 2 == 0)
                .Where(v => v % 3 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            Assert.IsTrue(input.Where(v => v % 6 == 0).SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereWithFailedTransformationFusedRow() // stays columnar, but causes payload to be reconstituted
            => Enumerable.Range(0, 100)
                .Select(i => new StructTuple<int, char> { Item1 = i, Item2 = i.ToString()[0], })
                .TestWhere(r => r.ToString().Length > 3, false);

        [TestMethod, TestCategory("Gated")]
        public void Select1FusedRow()
            => TestSelect(e => e.field1, false);

        [TestMethod, TestCategory("Gated")]
        public void Select2FusedRow()
            => TestSelect(e => e.field2, false);

        [TestMethod, TestCategory("Gated")]
        public void Select3FusedRow()
            => TestSelect(e => e.field3, false);

        [TestMethod, TestCategory("Gated")]
        public void Select4FusedRow()
            => TestSelect(e => new NestedStruct { nestedField = e.field1 }, false);

        [TestMethod, TestCategory("Gated")]
        public void Select5FusedRow()
            => TestSelect(e => e.doubleField, false);

        [TestMethod, TestCategory("Gated")]
        public void Select6FusedRow()
            => TestSelect(e => ((ulong)e.field1), false);

        // Tests to make sure that we ref count correctly by doing two pointer swings to the same column.
        [TestMethod, TestCategory("Gated")]
        public void Select7FusedRow()
            => TestSelect(e => new { A = e.field1, B = e.field1, }, false);

        // Tests whether an expression that cannot be columnerized causes code gen to fall back to row-oriented.
        // This is just a degenerate case where the expression is *always* null.
        [TestMethod, TestCategory("Gated")]
        public void Select8FusedRow()
            => TestSelect<ClassWithOnlyAutoProps>(e => null, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectFromUlongToLongFusedRow()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Select(v => (long)v)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, longInteger) => (long)integer == longInteger).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeFusedRow()
            => TestSelect(e => new { anonfield1 = e.field1, x = 3 }, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedFusedRow()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Select(v => new { X = v })
                .Select(e => new { Y = e.X })
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.Y).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedNoOpFusedRow()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Select(v => new { X = v })
                .Select(e => e)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.X).All(p => p));
        }

        // The point of this test is to have an anonymous type with a "field" in it
        // whose type does not have an overload for it in MemoryPool<TKey, TPayload>.Get(out ColumnBatch<>).
        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeWithFloatFieldFusedRow()
            => TestSelect(e => new { floatField = e.field1 * 3.0, }, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectWhereAnonymousTypeFusedRow()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithAutoPropsFusedRow()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithAutoPropsFusedRow()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithOnlyAutoPropsFusedRow()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithOnlyAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithOnlyAutoPropsFusedRow()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_SelectWhereFusedRow()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_GroupApplyFusedRow()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .GroupApply(e => e.field1, str => str.Count(), (g, cnt) => new MyStruct { field1 = (int)cnt, })
                .Where(e => e.field2 >= 0.0)
                .ToPayloadEnumerable();
            Assert.IsTrue(streamResult.Count() == this.enumerable.Count());
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectWithCtorCallFusedRow()
            => TestSelect(e => new StructWithCtor(e.field1), false);

        [TestMethod, TestCategory("Gated")]
        public void SelectMany01FusedRow()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany02FusedRow()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany03FusedRow()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany04FusedRow()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart01FusedRow()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .Select((l, e) => e + ((int)l))
                ;
            var expected = input
                .Select(e => e + e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart02FusedRow()
        {
            // tests simple parameter selection with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .Select((l, e) => l)
                ;
            var expected = input
                .Select(e => (long)e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey01FusedRow()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectByKey((k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey02FusedRow()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectByKey((k, e) => k)
                ;
            var expected = input
                .Select(e => Empty.Default)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey03FusedRow()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectByKey((s, k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e + (int)s, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e + e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey04FusedRow()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .Select(e => (int?)(e + 1))
                .SelectByKey((k, e) => new object[] { e, })
                ;
            var expected = input
                .Select(e => new object[] { (int?)(e + 1), })
                .ToArray()
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.Length == output.Length);
            for (int i = 0; i < expected.Length; i++)
            {
                var e = (int?)(expected[i][0]);
                var o = (int?)(output[i][0]);
                Assert.IsTrue(e.HasValue && o.HasValue && (e.Value == o.Value));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStart01FusedRow()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectMany((l, e) => Enumerable.Repeat(33, e + ((int)l)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + e))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyKey01FusedRow()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectManyByKey((k, e) => Enumerable.Repeat(33, e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStartKey01FusedRow()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectManyByKey((ts, k, e) => Enumerable.Repeat(33 + ((int)ts), e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33 + e, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereSelect1FusedRow()
            => TestWhereSelect(e => true, e => e.field1, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectStructFusedRow()
        {
            using (var modifier = new ConfigModifier().ForceRowBasedExecution(true).Modify())
            {
                var savedForceRowBasedExecution = Config.ForceRowBasedExecution;
                Config.ForceRowBasedExecution = true;

                var input = Enumerable.Range(0, 100);
                Expression<Func<int, MyStruct>> lambda = e => new MyStruct { field1 = e, field2 = e + 0.5, field3 = default };

                var streamResult = input
                    .ToStreamable()
                    .Select(lambda)
                    .ToPayloadEnumerable();
                var linqResult = input
                    .Select(lambda.Compile());

                var query = input.Where(e => true);
                Assert.IsTrue(linqResult.SequenceEqual(streamResult));
            }
        }

    }

    [TestClass]
    public class SimpleTestsFusedRowFloating : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public SimpleTestsFusedRowFloating() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .AllowFloatingReorderPolicy(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        // Total number of input events
        private const int NumEvents = 1000;
        private readonly IEnumerable<MyStruct2> enumerable = Enumerable.Range(0, NumEvents)
            .Select(e =>
                new MyStruct2 { field1 = e, field2 = new MyString(Convert.ToString("string" + e)), field3 = new NestedStruct { nestedField = e } });

        public static bool MyIsEqual(List<RankedEvent<char>> a, List<RankedEvent<char>> b)
        {
            if (a.Count != b.Count) return false;

            for (int j = 0; j < a.Count; j++)
            {
                if (!a[j].Equals(b[j])) return false;
            }

            return true;
        }

        public static int MyGetHashCode(List<RankedEvent<char>> a)
        {
            int ret = 0;
            foreach (var l in a)
            {
                ret ^= l.GetHashCode();
            }
            return ret;
        }

        private void TestWhere(Expression<Func<MyStruct2, bool>> predicate, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhere(predicate, disableFusion));

        private void TestSelect<TResult>(Expression<Func<MyStruct2, TResult>> function, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestSelect(function, disableFusion));

        public void TestWhereSelect<U>(Expression<Func<MyStruct2, bool>> wherePredicate, Expression<Func<MyStruct2, U>> selectFunction, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhereSelect(wherePredicate, selectFunction, disableFusion));

        [TestMethod, TestCategory("Gated")]
        public void Where1FusedRowFloating()
            => TestWhere(e => e.field2.mystring.Contains("string"), false);

        [TestMethod, TestCategory("Gated")]
        public void Where2FusedRowFloating()
            => TestWhere(e => e.field2.mystring.Equals("string0"), false);

        [TestMethod, TestCategory("Gated")]
        public void Where3FusedRowFloating()
            => TestWhere(e => e.field2.mystring.Equals("string0"), false);

        [TestMethod, TestCategory("Gated")]
        public void Where4FusedRowFloating()
            => TestWhere(e => e.field3.nestedField == 0, false);

        [TestMethod, TestCategory("Gated")]
        public void Where5FusedRowFloating()
            => TestWhere(e => true, false);

        [TestMethod, TestCategory("Gated")]
        public void Where6FusedRowFloating()
            => TestWhere(e => false, false);

        [TestMethod, TestCategory("Gated")]
        public void Where7FusedRowFloating()
            => TestWhere(e => e.field3.nestedField == 0 || e.field3.nestedField == 1, false);

        [TestMethod, TestCategory("Gated")]
        public void Where8FusedRowFloating()
            => TestWhere(e => string.Compare(e.field2.mystring, "string0") == 0, false);

        [TestMethod, TestCategory("Gated")]
        public void Where9FusedRowFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Where(v => v % 2 == 0)
                .Where(v => v % 3 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            Assert.IsTrue(input.Where(v => v % 6 == 0).SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereWithFailedTransformationFusedRowFloating() // stays columnar, but causes payload to be reconstituted
            => Enumerable.Range(0, 100)
                .Select(i => new StructTuple<int, char> { Item1 = i, Item2 = i.ToString()[0], })
                .TestWhere(r => r.ToString().Length > 3, false);

        [TestMethod, TestCategory("Gated")]
        public void Select1FusedRowFloating()
            => TestSelect(e => e.field1, false);

        [TestMethod, TestCategory("Gated")]
        public void Select2FusedRowFloating()
            => TestSelect(e => e.field2, false);

        [TestMethod, TestCategory("Gated")]
        public void Select3FusedRowFloating()
            => TestSelect(e => e.field3, false);

        [TestMethod, TestCategory("Gated")]
        public void Select4FusedRowFloating()
            => TestSelect(e => new NestedStruct { nestedField = e.field1 }, false);

        [TestMethod, TestCategory("Gated")]
        public void Select5FusedRowFloating()
            => TestSelect(e => e.doubleField, false);

        [TestMethod, TestCategory("Gated")]
        public void Select6FusedRowFloating()
            => TestSelect(e => ((ulong)e.field1), false);

        // Tests to make sure that we ref count correctly by doing two pointer swings to the same column.
        [TestMethod, TestCategory("Gated")]
        public void Select7FusedRowFloating()
            => TestSelect(e => new { A = e.field1, B = e.field1, }, false);

        // Tests whether an expression that cannot be columnerized causes code gen to fall back to row-oriented.
        // This is just a degenerate case where the expression is *always* null.
        [TestMethod, TestCategory("Gated")]
        public void Select8FusedRowFloating()
            => TestSelect<ClassWithOnlyAutoProps>(e => null, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectFromUlongToLongFusedRowFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Select(v => (long)v)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, longInteger) => (long)integer == longInteger).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeFusedRowFloating()
            => TestSelect(e => new { anonfield1 = e.field1, x = 3 }, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedFusedRowFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Select(v => new { X = v })
                .Select(e => new { Y = e.X })
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.Y).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedNoOpFusedRowFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Select(v => new { X = v })
                .Select(e => e)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.X).All(p => p));
        }

        // The point of this test is to have an anonymous type with a "field" in it
        // whose type does not have an overload for it in MemoryPool<TKey, TPayload>.Get(out ColumnBatch<>).
        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeWithFloatFieldFusedRowFloating()
            => TestSelect(e => new { floatField = e.field1 * 3.0, }, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectWhereAnonymousTypeFusedRowFloating()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithAutoPropsFusedRowFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithAutoPropsFusedRowFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithOnlyAutoPropsFusedRowFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithOnlyAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithOnlyAutoPropsFusedRowFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_SelectWhereFusedRowFloating()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_GroupApplyFusedRowFloating()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .GroupApply(e => e.field1, str => str.Count(), (g, cnt) => new MyStruct { field1 = (int)cnt, })
                .Where(e => e.field2 >= 0.0)
                .ToPayloadEnumerable();
            Assert.IsTrue(streamResult.Count() == this.enumerable.Count());
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectWithCtorCallFusedRowFloating()
            => TestSelect(e => new StructWithCtor(e.field1), false);

        [TestMethod, TestCategory("Gated")]
        public void SelectMany01FusedRowFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany02FusedRowFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany03FusedRowFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany04FusedRowFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart01FusedRowFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .Select((l, e) => e + ((int)l))
                ;
            var expected = input
                .Select(e => e + e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart02FusedRowFloating()
        {
            // tests simple parameter selection with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .Select((l, e) => l)
                ;
            var expected = input
                .Select(e => (long)e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey01FusedRowFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectByKey((k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey02FusedRowFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectByKey((k, e) => k)
                ;
            var expected = input
                .Select(e => Empty.Default)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey03FusedRowFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectByKey((s, k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e + (int)s, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e + e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey04FusedRowFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .Select(e => (int?)(e + 1))
                .SelectByKey((k, e) => new object[] { e, })
                ;
            var expected = input
                .Select(e => new object[] { (int?)(e + 1), })
                .ToArray()
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.Length == output.Length);
            for (int i = 0; i < expected.Length; i++)
            {
                var e = (int?)(expected[i][0]);
                var o = (int?)(output[i][0]);
                Assert.IsTrue(e.HasValue && o.HasValue && (e.Value == o.Value));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStart01FusedRowFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectMany((l, e) => Enumerable.Repeat(33, e + ((int)l)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + e))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyKey01FusedRowFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectManyByKey((k, e) => Enumerable.Repeat(33, e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStartKey01FusedRowFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectManyByKey((ts, k, e) => Enumerable.Repeat(33 + ((int)ts), e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33 + e, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereSelect1FusedRowFloating()
            => TestWhereSelect(e => true, e => e.field1, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectStructFusedRowFloating()
        {
            using (var modifier = new ConfigModifier().ForceRowBasedExecution(true).Modify())
            {
                var savedForceRowBasedExecution = Config.ForceRowBasedExecution;
                Config.ForceRowBasedExecution = true;

                var input = Enumerable.Range(0, 100);
                Expression<Func<int, MyStruct>> lambda = e => new MyStruct { field1 = e, field2 = e + 0.5, field3 = default };

                var streamResult = input
                    .ToStreamable()
                    .Select(lambda)
                    .ToPayloadEnumerable();
                var linqResult = input
                    .Select(lambda.Compile());

                var query = input.Where(e => true);
                Assert.IsTrue(linqResult.SequenceEqual(streamResult));
            }
        }

    }

    [TestClass]
    public class SimpleTestsFusedRowSmallBatch : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public SimpleTestsFusedRowSmallBatch() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        // Total number of input events
        private const int NumEvents = 1000;
        private readonly IEnumerable<MyStruct2> enumerable = Enumerable.Range(0, NumEvents)
            .Select(e =>
                new MyStruct2 { field1 = e, field2 = new MyString(Convert.ToString("string" + e)), field3 = new NestedStruct { nestedField = e } });

        public static bool MyIsEqual(List<RankedEvent<char>> a, List<RankedEvent<char>> b)
        {
            if (a.Count != b.Count) return false;

            for (int j = 0; j < a.Count; j++)
            {
                if (!a[j].Equals(b[j])) return false;
            }

            return true;
        }

        public static int MyGetHashCode(List<RankedEvent<char>> a)
        {
            int ret = 0;
            foreach (var l in a)
            {
                ret ^= l.GetHashCode();
            }
            return ret;
        }

        private void TestWhere(Expression<Func<MyStruct2, bool>> predicate, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhere(predicate, disableFusion));

        private void TestSelect<TResult>(Expression<Func<MyStruct2, TResult>> function, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestSelect(function, disableFusion));

        public void TestWhereSelect<U>(Expression<Func<MyStruct2, bool>> wherePredicate, Expression<Func<MyStruct2, U>> selectFunction, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhereSelect(wherePredicate, selectFunction, disableFusion));

        [TestMethod, TestCategory("Gated")]
        public void Where1FusedRowSmallBatch()
            => TestWhere(e => e.field2.mystring.Contains("string"), false);

        [TestMethod, TestCategory("Gated")]
        public void Where2FusedRowSmallBatch()
            => TestWhere(e => e.field2.mystring.Equals("string0"), false);

        [TestMethod, TestCategory("Gated")]
        public void Where3FusedRowSmallBatch()
            => TestWhere(e => e.field2.mystring.Equals("string0"), false);

        [TestMethod, TestCategory("Gated")]
        public void Where4FusedRowSmallBatch()
            => TestWhere(e => e.field3.nestedField == 0, false);

        [TestMethod, TestCategory("Gated")]
        public void Where5FusedRowSmallBatch()
            => TestWhere(e => true, false);

        [TestMethod, TestCategory("Gated")]
        public void Where6FusedRowSmallBatch()
            => TestWhere(e => false, false);

        [TestMethod, TestCategory("Gated")]
        public void Where7FusedRowSmallBatch()
            => TestWhere(e => e.field3.nestedField == 0 || e.field3.nestedField == 1, false);

        [TestMethod, TestCategory("Gated")]
        public void Where8FusedRowSmallBatch()
            => TestWhere(e => string.Compare(e.field2.mystring, "string0") == 0, false);

        [TestMethod, TestCategory("Gated")]
        public void Where9FusedRowSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Where(v => v % 2 == 0)
                .Where(v => v % 3 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            Assert.IsTrue(input.Where(v => v % 6 == 0).SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereWithFailedTransformationFusedRowSmallBatch() // stays columnar, but causes payload to be reconstituted
            => Enumerable.Range(0, 100)
                .Select(i => new StructTuple<int, char> { Item1 = i, Item2 = i.ToString()[0], })
                .TestWhere(r => r.ToString().Length > 3, false);

        [TestMethod, TestCategory("Gated")]
        public void Select1FusedRowSmallBatch()
            => TestSelect(e => e.field1, false);

        [TestMethod, TestCategory("Gated")]
        public void Select2FusedRowSmallBatch()
            => TestSelect(e => e.field2, false);

        [TestMethod, TestCategory("Gated")]
        public void Select3FusedRowSmallBatch()
            => TestSelect(e => e.field3, false);

        [TestMethod, TestCategory("Gated")]
        public void Select4FusedRowSmallBatch()
            => TestSelect(e => new NestedStruct { nestedField = e.field1 }, false);

        [TestMethod, TestCategory("Gated")]
        public void Select5FusedRowSmallBatch()
            => TestSelect(e => e.doubleField, false);

        [TestMethod, TestCategory("Gated")]
        public void Select6FusedRowSmallBatch()
            => TestSelect(e => ((ulong)e.field1), false);

        // Tests to make sure that we ref count correctly by doing two pointer swings to the same column.
        [TestMethod, TestCategory("Gated")]
        public void Select7FusedRowSmallBatch()
            => TestSelect(e => new { A = e.field1, B = e.field1, }, false);

        // Tests whether an expression that cannot be columnerized causes code gen to fall back to row-oriented.
        // This is just a degenerate case where the expression is *always* null.
        [TestMethod, TestCategory("Gated")]
        public void Select8FusedRowSmallBatch()
            => TestSelect<ClassWithOnlyAutoProps>(e => null, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectFromUlongToLongFusedRowSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Select(v => (long)v)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, longInteger) => (long)integer == longInteger).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeFusedRowSmallBatch()
            => TestSelect(e => new { anonfield1 = e.field1, x = 3 }, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedFusedRowSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Select(v => new { X = v })
                .Select(e => new { Y = e.X })
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.Y).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedNoOpFusedRowSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Select(v => new { X = v })
                .Select(e => e)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.X).All(p => p));
        }

        // The point of this test is to have an anonymous type with a "field" in it
        // whose type does not have an overload for it in MemoryPool<TKey, TPayload>.Get(out ColumnBatch<>).
        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeWithFloatFieldFusedRowSmallBatch()
            => TestSelect(e => new { floatField = e.field1 * 3.0, }, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectWhereAnonymousTypeFusedRowSmallBatch()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithAutoPropsFusedRowSmallBatch()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithAutoPropsFusedRowSmallBatch()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithOnlyAutoPropsFusedRowSmallBatch()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithOnlyAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithOnlyAutoPropsFusedRowSmallBatch()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_SelectWhereFusedRowSmallBatch()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_GroupApplyFusedRowSmallBatch()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .GroupApply(e => e.field1, str => str.Count(), (g, cnt) => new MyStruct { field1 = (int)cnt, })
                .Where(e => e.field2 >= 0.0)
                .ToPayloadEnumerable();
            Assert.IsTrue(streamResult.Count() == this.enumerable.Count());
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectWithCtorCallFusedRowSmallBatch()
            => TestSelect(e => new StructWithCtor(e.field1), false);

        [TestMethod, TestCategory("Gated")]
        public void SelectMany01FusedRowSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany02FusedRowSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany03FusedRowSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany04FusedRowSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart01FusedRowSmallBatch()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .Select((l, e) => e + ((int)l))
                ;
            var expected = input
                .Select(e => e + e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart02FusedRowSmallBatch()
        {
            // tests simple parameter selection with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .Select((l, e) => l)
                ;
            var expected = input
                .Select(e => (long)e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey01FusedRowSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectByKey((k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey02FusedRowSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectByKey((k, e) => k)
                ;
            var expected = input
                .Select(e => Empty.Default)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey03FusedRowSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectByKey((s, k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e + (int)s, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e + e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey04FusedRowSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .Select(e => (int?)(e + 1))
                .SelectByKey((k, e) => new object[] { e, })
                ;
            var expected = input
                .Select(e => new object[] { (int?)(e + 1), })
                .ToArray()
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.Length == output.Length);
            for (int i = 0; i < expected.Length; i++)
            {
                var e = (int?)(expected[i][0]);
                var o = (int?)(output[i][0]);
                Assert.IsTrue(e.HasValue && o.HasValue && (e.Value == o.Value));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStart01FusedRowSmallBatch()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectMany((l, e) => Enumerable.Repeat(33, e + ((int)l)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + e))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyKey01FusedRowSmallBatch()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectManyByKey((k, e) => Enumerable.Repeat(33, e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStartKey01FusedRowSmallBatch()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectManyByKey((ts, k, e) => Enumerable.Repeat(33 + ((int)ts), e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33 + e, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereSelect1FusedRowSmallBatch()
            => TestWhereSelect(e => true, e => e.field1, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectStructFusedRowSmallBatch()
        {
            using (var modifier = new ConfigModifier().ForceRowBasedExecution(true).Modify())
            {
                var savedForceRowBasedExecution = Config.ForceRowBasedExecution;
                Config.ForceRowBasedExecution = true;

                var input = Enumerable.Range(0, 100);
                Expression<Func<int, MyStruct>> lambda = e => new MyStruct { field1 = e, field2 = e + 0.5, field3 = default };

                var streamResult = input
                    .ToStreamable()
                    .Select(lambda)
                    .ToPayloadEnumerable();
                var linqResult = input
                    .Select(lambda.Compile());

                var query = input.Where(e => true);
                Assert.IsTrue(linqResult.SequenceEqual(streamResult));
            }
        }

    }

    [TestClass]
    public class SimpleTestsFusedRowSmallBatchFloating : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public SimpleTestsFusedRowSmallBatchFloating() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .AllowFloatingReorderPolicy(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        // Total number of input events
        private const int NumEvents = 1000;
        private readonly IEnumerable<MyStruct2> enumerable = Enumerable.Range(0, NumEvents)
            .Select(e =>
                new MyStruct2 { field1 = e, field2 = new MyString(Convert.ToString("string" + e)), field3 = new NestedStruct { nestedField = e } });

        public static bool MyIsEqual(List<RankedEvent<char>> a, List<RankedEvent<char>> b)
        {
            if (a.Count != b.Count) return false;

            for (int j = 0; j < a.Count; j++)
            {
                if (!a[j].Equals(b[j])) return false;
            }

            return true;
        }

        public static int MyGetHashCode(List<RankedEvent<char>> a)
        {
            int ret = 0;
            foreach (var l in a)
            {
                ret ^= l.GetHashCode();
            }
            return ret;
        }

        private void TestWhere(Expression<Func<MyStruct2, bool>> predicate, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhere(predicate, disableFusion));

        private void TestSelect<TResult>(Expression<Func<MyStruct2, TResult>> function, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestSelect(function, disableFusion));

        public void TestWhereSelect<U>(Expression<Func<MyStruct2, bool>> wherePredicate, Expression<Func<MyStruct2, U>> selectFunction, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhereSelect(wherePredicate, selectFunction, disableFusion));

        [TestMethod, TestCategory("Gated")]
        public void Where1FusedRowSmallBatchFloating()
            => TestWhere(e => e.field2.mystring.Contains("string"), false);

        [TestMethod, TestCategory("Gated")]
        public void Where2FusedRowSmallBatchFloating()
            => TestWhere(e => e.field2.mystring.Equals("string0"), false);

        [TestMethod, TestCategory("Gated")]
        public void Where3FusedRowSmallBatchFloating()
            => TestWhere(e => e.field2.mystring.Equals("string0"), false);

        [TestMethod, TestCategory("Gated")]
        public void Where4FusedRowSmallBatchFloating()
            => TestWhere(e => e.field3.nestedField == 0, false);

        [TestMethod, TestCategory("Gated")]
        public void Where5FusedRowSmallBatchFloating()
            => TestWhere(e => true, false);

        [TestMethod, TestCategory("Gated")]
        public void Where6FusedRowSmallBatchFloating()
            => TestWhere(e => false, false);

        [TestMethod, TestCategory("Gated")]
        public void Where7FusedRowSmallBatchFloating()
            => TestWhere(e => e.field3.nestedField == 0 || e.field3.nestedField == 1, false);

        [TestMethod, TestCategory("Gated")]
        public void Where8FusedRowSmallBatchFloating()
            => TestWhere(e => string.Compare(e.field2.mystring, "string0") == 0, false);

        [TestMethod, TestCategory("Gated")]
        public void Where9FusedRowSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Where(v => v % 2 == 0)
                .Where(v => v % 3 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            Assert.IsTrue(input.Where(v => v % 6 == 0).SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereWithFailedTransformationFusedRowSmallBatchFloating() // stays columnar, but causes payload to be reconstituted
            => Enumerable.Range(0, 100)
                .Select(i => new StructTuple<int, char> { Item1 = i, Item2 = i.ToString()[0], })
                .TestWhere(r => r.ToString().Length > 3, false);

        [TestMethod, TestCategory("Gated")]
        public void Select1FusedRowSmallBatchFloating()
            => TestSelect(e => e.field1, false);

        [TestMethod, TestCategory("Gated")]
        public void Select2FusedRowSmallBatchFloating()
            => TestSelect(e => e.field2, false);

        [TestMethod, TestCategory("Gated")]
        public void Select3FusedRowSmallBatchFloating()
            => TestSelect(e => e.field3, false);

        [TestMethod, TestCategory("Gated")]
        public void Select4FusedRowSmallBatchFloating()
            => TestSelect(e => new NestedStruct { nestedField = e.field1 }, false);

        [TestMethod, TestCategory("Gated")]
        public void Select5FusedRowSmallBatchFloating()
            => TestSelect(e => e.doubleField, false);

        [TestMethod, TestCategory("Gated")]
        public void Select6FusedRowSmallBatchFloating()
            => TestSelect(e => ((ulong)e.field1), false);

        // Tests to make sure that we ref count correctly by doing two pointer swings to the same column.
        [TestMethod, TestCategory("Gated")]
        public void Select7FusedRowSmallBatchFloating()
            => TestSelect(e => new { A = e.field1, B = e.field1, }, false);

        // Tests whether an expression that cannot be columnerized causes code gen to fall back to row-oriented.
        // This is just a degenerate case where the expression is *always* null.
        [TestMethod, TestCategory("Gated")]
        public void Select8FusedRowSmallBatchFloating()
            => TestSelect<ClassWithOnlyAutoProps>(e => null, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectFromUlongToLongFusedRowSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Select(v => (long)v)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, longInteger) => (long)integer == longInteger).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeFusedRowSmallBatchFloating()
            => TestSelect(e => new { anonfield1 = e.field1, x = 3 }, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedFusedRowSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Select(v => new { X = v })
                .Select(e => new { Y = e.X })
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.Y).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedNoOpFusedRowSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Select(v => new { X = v })
                .Select(e => e)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.X).All(p => p));
        }

        // The point of this test is to have an anonymous type with a "field" in it
        // whose type does not have an overload for it in MemoryPool<TKey, TPayload>.Get(out ColumnBatch<>).
        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeWithFloatFieldFusedRowSmallBatchFloating()
            => TestSelect(e => new { floatField = e.field1 * 3.0, }, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectWhereAnonymousTypeFusedRowSmallBatchFloating()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithAutoPropsFusedRowSmallBatchFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithAutoPropsFusedRowSmallBatchFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithOnlyAutoPropsFusedRowSmallBatchFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithOnlyAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithOnlyAutoPropsFusedRowSmallBatchFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_SelectWhereFusedRowSmallBatchFloating()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_GroupApplyFusedRowSmallBatchFloating()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .GroupApply(e => e.field1, str => str.Count(), (g, cnt) => new MyStruct { field1 = (int)cnt, })
                .Where(e => e.field2 >= 0.0)
                .ToPayloadEnumerable();
            Assert.IsTrue(streamResult.Count() == this.enumerable.Count());
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectWithCtorCallFusedRowSmallBatchFloating()
            => TestSelect(e => new StructWithCtor(e.field1), false);

        [TestMethod, TestCategory("Gated")]
        public void SelectMany01FusedRowSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany02FusedRowSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany03FusedRowSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany04FusedRowSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart01FusedRowSmallBatchFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .Select((l, e) => e + ((int)l))
                ;
            var expected = input
                .Select(e => e + e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart02FusedRowSmallBatchFloating()
        {
            // tests simple parameter selection with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .Select((l, e) => l)
                ;
            var expected = input
                .Select(e => (long)e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey01FusedRowSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectByKey((k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey02FusedRowSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectByKey((k, e) => k)
                ;
            var expected = input
                .Select(e => Empty.Default)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey03FusedRowSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectByKey((s, k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e + (int)s, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e + e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey04FusedRowSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .Select(e => (int?)(e + 1))
                .SelectByKey((k, e) => new object[] { e, })
                ;
            var expected = input
                .Select(e => new object[] { (int?)(e + 1), })
                .ToArray()
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.Length == output.Length);
            for (int i = 0; i < expected.Length; i++)
            {
                var e = (int?)(expected[i][0]);
                var o = (int?)(output[i][0]);
                Assert.IsTrue(e.HasValue && o.HasValue && (e.Value == o.Value));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStart01FusedRowSmallBatchFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectMany((l, e) => Enumerable.Repeat(33, e + ((int)l)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + e))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyKey01FusedRowSmallBatchFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectManyByKey((k, e) => Enumerable.Repeat(33, e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStartKey01FusedRowSmallBatchFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectManyByKey((ts, k, e) => Enumerable.Repeat(33 + ((int)ts), e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33 + e, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereSelect1FusedRowSmallBatchFloating()
            => TestWhereSelect(e => true, e => e.field1, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectStructFusedRowSmallBatchFloating()
        {
            using (var modifier = new ConfigModifier().ForceRowBasedExecution(true).Modify())
            {
                var savedForceRowBasedExecution = Config.ForceRowBasedExecution;
                Config.ForceRowBasedExecution = true;

                var input = Enumerable.Range(0, 100);
                Expression<Func<int, MyStruct>> lambda = e => new MyStruct { field1 = e, field2 = e + 0.5, field3 = default };

                var streamResult = input
                    .ToStreamable()
                    .Select(lambda)
                    .ToPayloadEnumerable();
                var linqResult = input
                    .Select(lambda.Compile());

                var query = input.Where(e => true);
                Assert.IsTrue(linqResult.SequenceEqual(streamResult));
            }
        }

    }

    [TestClass]
    public class SimpleTestsFusedColumnar : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public SimpleTestsFusedColumnar() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        // Total number of input events
        private const int NumEvents = 1000;
        private readonly IEnumerable<MyStruct2> enumerable = Enumerable.Range(0, NumEvents)
            .Select(e =>
                new MyStruct2 { field1 = e, field2 = new MyString(Convert.ToString("string" + e)), field3 = new NestedStruct { nestedField = e } });

        public static bool MyIsEqual(List<RankedEvent<char>> a, List<RankedEvent<char>> b)
        {
            if (a.Count != b.Count) return false;

            for (int j = 0; j < a.Count; j++)
            {
                if (!a[j].Equals(b[j])) return false;
            }

            return true;
        }

        public static int MyGetHashCode(List<RankedEvent<char>> a)
        {
            int ret = 0;
            foreach (var l in a)
            {
                ret ^= l.GetHashCode();
            }
            return ret;
        }

        private void TestWhere(Expression<Func<MyStruct2, bool>> predicate, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhere(predicate, disableFusion));

        private void TestSelect<TResult>(Expression<Func<MyStruct2, TResult>> function, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestSelect(function, disableFusion));

        public void TestWhereSelect<U>(Expression<Func<MyStruct2, bool>> wherePredicate, Expression<Func<MyStruct2, U>> selectFunction, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhereSelect(wherePredicate, selectFunction, disableFusion));

        [TestMethod, TestCategory("Gated")]
        public void Where1FusedColumnar()
            => TestWhere(e => e.field2.mystring.Contains("string"), false);

        [TestMethod, TestCategory("Gated")]
        public void Where2FusedColumnar()
            => TestWhere(e => e.field2.mystring.Equals("string0"), false);

        [TestMethod, TestCategory("Gated")]
        public void Where3FusedColumnar()
            => TestWhere(e => e.field2.mystring.Equals("string0"), false);

        [TestMethod, TestCategory("Gated")]
        public void Where4FusedColumnar()
            => TestWhere(e => e.field3.nestedField == 0, false);

        [TestMethod, TestCategory("Gated")]
        public void Where5FusedColumnar()
            => TestWhere(e => true, false);

        [TestMethod, TestCategory("Gated")]
        public void Where6FusedColumnar()
            => TestWhere(e => false, false);

        [TestMethod, TestCategory("Gated")]
        public void Where7FusedColumnar()
            => TestWhere(e => e.field3.nestedField == 0 || e.field3.nestedField == 1, false);

        [TestMethod, TestCategory("Gated")]
        public void Where8FusedColumnar()
            => TestWhere(e => string.Compare(e.field2.mystring, "string0") == 0, false);

        [TestMethod, TestCategory("Gated")]
        public void Where9FusedColumnar()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Where(v => v % 2 == 0)
                .Where(v => v % 3 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            Assert.IsTrue(input.Where(v => v % 6 == 0).SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereWithFailedTransformationFusedColumnar() // stays columnar, but causes payload to be reconstituted
            => Enumerable.Range(0, 100)
                .Select(i => new StructTuple<int, char> { Item1 = i, Item2 = i.ToString()[0], })
                .TestWhere(r => r.ToString().Length > 3, false);

        [TestMethod, TestCategory("Gated")]
        public void Select1FusedColumnar()
            => TestSelect(e => e.field1, false);

        [TestMethod, TestCategory("Gated")]
        public void Select2FusedColumnar()
            => TestSelect(e => e.field2, false);

        [TestMethod, TestCategory("Gated")]
        public void Select3FusedColumnar()
            => TestSelect(e => e.field3, false);

        [TestMethod, TestCategory("Gated")]
        public void Select4FusedColumnar()
            => TestSelect(e => new NestedStruct { nestedField = e.field1 }, false);

        [TestMethod, TestCategory("Gated")]
        public void Select5FusedColumnar()
            => TestSelect(e => e.doubleField, false);

        [TestMethod, TestCategory("Gated")]
        public void Select6FusedColumnar()
            => TestSelect(e => ((ulong)e.field1), false);

        // Tests to make sure that we ref count correctly by doing two pointer swings to the same column.
        [TestMethod, TestCategory("Gated")]
        public void Select7FusedColumnar()
            => TestSelect(e => new { A = e.field1, B = e.field1, }, false);

        // Tests whether an expression that cannot be columnerized causes code gen to fall back to row-oriented.
        // This is just a degenerate case where the expression is *always* null.
        [TestMethod, TestCategory("Gated")]
        public void Select8FusedColumnar()
            => TestSelect<ClassWithOnlyAutoProps>(e => null, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectFromUlongToLongFusedColumnar()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Select(v => (long)v)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, longInteger) => (long)integer == longInteger).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeFusedColumnar()
            => TestSelect(e => new { anonfield1 = e.field1, x = 3 }, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedFusedColumnar()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Select(v => new { X = v })
                .Select(e => new { Y = e.X })
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.Y).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedNoOpFusedColumnar()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Select(v => new { X = v })
                .Select(e => e)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.X).All(p => p));
        }

        // The point of this test is to have an anonymous type with a "field" in it
        // whose type does not have an overload for it in MemoryPool<TKey, TPayload>.Get(out ColumnBatch<>).
        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeWithFloatFieldFusedColumnar()
            => TestSelect(e => new { floatField = e.field1 * 3.0, }, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectWhereAnonymousTypeFusedColumnar()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithAutoPropsFusedColumnar()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithAutoPropsFusedColumnar()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithOnlyAutoPropsFusedColumnar()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithOnlyAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithOnlyAutoPropsFusedColumnar()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_SelectWhereFusedColumnar()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_GroupApplyFusedColumnar()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .GroupApply(e => e.field1, str => str.Count(), (g, cnt) => new MyStruct { field1 = (int)cnt, })
                .Where(e => e.field2 >= 0.0)
                .ToPayloadEnumerable();
            Assert.IsTrue(streamResult.Count() == this.enumerable.Count());
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectWithCtorCallFusedColumnar()
            => TestSelect(e => new StructWithCtor(e.field1), false);

        [TestMethod, TestCategory("Gated")]
        public void SelectMany01FusedColumnar()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany02FusedColumnar()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany03FusedColumnar()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany04FusedColumnar()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart01FusedColumnar()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .Select((l, e) => e + ((int)l))
                ;
            var expected = input
                .Select(e => e + e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart02FusedColumnar()
        {
            // tests simple parameter selection with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .Select((l, e) => l)
                ;
            var expected = input
                .Select(e => (long)e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey01FusedColumnar()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectByKey((k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey02FusedColumnar()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectByKey((k, e) => k)
                ;
            var expected = input
                .Select(e => Empty.Default)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey03FusedColumnar()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectByKey((s, k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e + (int)s, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e + e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey04FusedColumnar()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .Select(e => (int?)(e + 1))
                .SelectByKey((k, e) => new object[] { e, })
                ;
            var expected = input
                .Select(e => new object[] { (int?)(e + 1), })
                .ToArray()
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.Length == output.Length);
            for (int i = 0; i < expected.Length; i++)
            {
                var e = (int?)(expected[i][0]);
                var o = (int?)(output[i][0]);
                Assert.IsTrue(e.HasValue && o.HasValue && (e.Value == o.Value));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStart01FusedColumnar()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectMany((l, e) => Enumerable.Repeat(33, e + ((int)l)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + e))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyKey01FusedColumnar()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectManyByKey((k, e) => Enumerable.Repeat(33, e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStartKey01FusedColumnar()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectManyByKey((ts, k, e) => Enumerable.Repeat(33 + ((int)ts), e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33 + e, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereSelect1FusedColumnar()
            => TestWhereSelect(e => true, e => e.field1, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectStructFusedColumnar()
        {
            using (var modifier = new ConfigModifier().ForceRowBasedExecution(true).Modify())
            {
                var savedForceRowBasedExecution = Config.ForceRowBasedExecution;
                Config.ForceRowBasedExecution = true;

                var input = Enumerable.Range(0, 100);
                Expression<Func<int, MyStruct>> lambda = e => new MyStruct { field1 = e, field2 = e + 0.5, field3 = default };

                var streamResult = input
                    .ToStreamable()
                    .Select(lambda)
                    .ToPayloadEnumerable();
                var linqResult = input
                    .Select(lambda.Compile());

                var query = input.Where(e => true);
                Assert.IsTrue(linqResult.SequenceEqual(streamResult));
            }
        }

    }

    [TestClass]
    public class SimpleTestsFusedColumnarFloating : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public SimpleTestsFusedColumnarFloating() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .AllowFloatingReorderPolicy(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        // Total number of input events
        private const int NumEvents = 1000;
        private readonly IEnumerable<MyStruct2> enumerable = Enumerable.Range(0, NumEvents)
            .Select(e =>
                new MyStruct2 { field1 = e, field2 = new MyString(Convert.ToString("string" + e)), field3 = new NestedStruct { nestedField = e } });

        public static bool MyIsEqual(List<RankedEvent<char>> a, List<RankedEvent<char>> b)
        {
            if (a.Count != b.Count) return false;

            for (int j = 0; j < a.Count; j++)
            {
                if (!a[j].Equals(b[j])) return false;
            }

            return true;
        }

        public static int MyGetHashCode(List<RankedEvent<char>> a)
        {
            int ret = 0;
            foreach (var l in a)
            {
                ret ^= l.GetHashCode();
            }
            return ret;
        }

        private void TestWhere(Expression<Func<MyStruct2, bool>> predicate, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhere(predicate, disableFusion));

        private void TestSelect<TResult>(Expression<Func<MyStruct2, TResult>> function, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestSelect(function, disableFusion));

        public void TestWhereSelect<U>(Expression<Func<MyStruct2, bool>> wherePredicate, Expression<Func<MyStruct2, U>> selectFunction, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhereSelect(wherePredicate, selectFunction, disableFusion));

        [TestMethod, TestCategory("Gated")]
        public void Where1FusedColumnarFloating()
            => TestWhere(e => e.field2.mystring.Contains("string"), false);

        [TestMethod, TestCategory("Gated")]
        public void Where2FusedColumnarFloating()
            => TestWhere(e => e.field2.mystring.Equals("string0"), false);

        [TestMethod, TestCategory("Gated")]
        public void Where3FusedColumnarFloating()
            => TestWhere(e => e.field2.mystring.Equals("string0"), false);

        [TestMethod, TestCategory("Gated")]
        public void Where4FusedColumnarFloating()
            => TestWhere(e => e.field3.nestedField == 0, false);

        [TestMethod, TestCategory("Gated")]
        public void Where5FusedColumnarFloating()
            => TestWhere(e => true, false);

        [TestMethod, TestCategory("Gated")]
        public void Where6FusedColumnarFloating()
            => TestWhere(e => false, false);

        [TestMethod, TestCategory("Gated")]
        public void Where7FusedColumnarFloating()
            => TestWhere(e => e.field3.nestedField == 0 || e.field3.nestedField == 1, false);

        [TestMethod, TestCategory("Gated")]
        public void Where8FusedColumnarFloating()
            => TestWhere(e => string.Compare(e.field2.mystring, "string0") == 0, false);

        [TestMethod, TestCategory("Gated")]
        public void Where9FusedColumnarFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Where(v => v % 2 == 0)
                .Where(v => v % 3 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            Assert.IsTrue(input.Where(v => v % 6 == 0).SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereWithFailedTransformationFusedColumnarFloating() // stays columnar, but causes payload to be reconstituted
            => Enumerable.Range(0, 100)
                .Select(i => new StructTuple<int, char> { Item1 = i, Item2 = i.ToString()[0], })
                .TestWhere(r => r.ToString().Length > 3, false);

        [TestMethod, TestCategory("Gated")]
        public void Select1FusedColumnarFloating()
            => TestSelect(e => e.field1, false);

        [TestMethod, TestCategory("Gated")]
        public void Select2FusedColumnarFloating()
            => TestSelect(e => e.field2, false);

        [TestMethod, TestCategory("Gated")]
        public void Select3FusedColumnarFloating()
            => TestSelect(e => e.field3, false);

        [TestMethod, TestCategory("Gated")]
        public void Select4FusedColumnarFloating()
            => TestSelect(e => new NestedStruct { nestedField = e.field1 }, false);

        [TestMethod, TestCategory("Gated")]
        public void Select5FusedColumnarFloating()
            => TestSelect(e => e.doubleField, false);

        [TestMethod, TestCategory("Gated")]
        public void Select6FusedColumnarFloating()
            => TestSelect(e => ((ulong)e.field1), false);

        // Tests to make sure that we ref count correctly by doing two pointer swings to the same column.
        [TestMethod, TestCategory("Gated")]
        public void Select7FusedColumnarFloating()
            => TestSelect(e => new { A = e.field1, B = e.field1, }, false);

        // Tests whether an expression that cannot be columnerized causes code gen to fall back to row-oriented.
        // This is just a degenerate case where the expression is *always* null.
        [TestMethod, TestCategory("Gated")]
        public void Select8FusedColumnarFloating()
            => TestSelect<ClassWithOnlyAutoProps>(e => null, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectFromUlongToLongFusedColumnarFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Select(v => (long)v)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, longInteger) => (long)integer == longInteger).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeFusedColumnarFloating()
            => TestSelect(e => new { anonfield1 = e.field1, x = 3 }, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedFusedColumnarFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Select(v => new { X = v })
                .Select(e => new { Y = e.X })
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.Y).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedNoOpFusedColumnarFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Select(v => new { X = v })
                .Select(e => e)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.X).All(p => p));
        }

        // The point of this test is to have an anonymous type with a "field" in it
        // whose type does not have an overload for it in MemoryPool<TKey, TPayload>.Get(out ColumnBatch<>).
        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeWithFloatFieldFusedColumnarFloating()
            => TestSelect(e => new { floatField = e.field1 * 3.0, }, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectWhereAnonymousTypeFusedColumnarFloating()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithAutoPropsFusedColumnarFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithAutoPropsFusedColumnarFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithOnlyAutoPropsFusedColumnarFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithOnlyAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithOnlyAutoPropsFusedColumnarFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_SelectWhereFusedColumnarFloating()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_GroupApplyFusedColumnarFloating()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .GroupApply(e => e.field1, str => str.Count(), (g, cnt) => new MyStruct { field1 = (int)cnt, })
                .Where(e => e.field2 >= 0.0)
                .ToPayloadEnumerable();
            Assert.IsTrue(streamResult.Count() == this.enumerable.Count());
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectWithCtorCallFusedColumnarFloating()
            => TestSelect(e => new StructWithCtor(e.field1), false);

        [TestMethod, TestCategory("Gated")]
        public void SelectMany01FusedColumnarFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany02FusedColumnarFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany03FusedColumnarFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany04FusedColumnarFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart01FusedColumnarFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .Select((l, e) => e + ((int)l))
                ;
            var expected = input
                .Select(e => e + e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart02FusedColumnarFloating()
        {
            // tests simple parameter selection with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .Select((l, e) => l)
                ;
            var expected = input
                .Select(e => (long)e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey01FusedColumnarFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectByKey((k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey02FusedColumnarFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectByKey((k, e) => k)
                ;
            var expected = input
                .Select(e => Empty.Default)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey03FusedColumnarFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectByKey((s, k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e + (int)s, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e + e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey04FusedColumnarFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .Select(e => (int?)(e + 1))
                .SelectByKey((k, e) => new object[] { e, })
                ;
            var expected = input
                .Select(e => new object[] { (int?)(e + 1), })
                .ToArray()
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.Length == output.Length);
            for (int i = 0; i < expected.Length; i++)
            {
                var e = (int?)(expected[i][0]);
                var o = (int?)(output[i][0]);
                Assert.IsTrue(e.HasValue && o.HasValue && (e.Value == o.Value));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStart01FusedColumnarFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectMany((l, e) => Enumerable.Repeat(33, e + ((int)l)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + e))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyKey01FusedColumnarFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectManyByKey((k, e) => Enumerable.Repeat(33, e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStartKey01FusedColumnarFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectManyByKey((ts, k, e) => Enumerable.Repeat(33 + ((int)ts), e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33 + e, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereSelect1FusedColumnarFloating()
            => TestWhereSelect(e => true, e => e.field1, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectStructFusedColumnarFloating()
        {
            using (var modifier = new ConfigModifier().ForceRowBasedExecution(true).Modify())
            {
                var savedForceRowBasedExecution = Config.ForceRowBasedExecution;
                Config.ForceRowBasedExecution = true;

                var input = Enumerable.Range(0, 100);
                Expression<Func<int, MyStruct>> lambda = e => new MyStruct { field1 = e, field2 = e + 0.5, field3 = default };

                var streamResult = input
                    .ToStreamable()
                    .Select(lambda)
                    .ToPayloadEnumerable();
                var linqResult = input
                    .Select(lambda.Compile());

                var query = input.Where(e => true);
                Assert.IsTrue(linqResult.SequenceEqual(streamResult));
            }
        }

    }

    [TestClass]
    public class SimpleTestsFusedColumnarSmallBatch : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public SimpleTestsFusedColumnarSmallBatch() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        // Total number of input events
        private const int NumEvents = 1000;
        private readonly IEnumerable<MyStruct2> enumerable = Enumerable.Range(0, NumEvents)
            .Select(e =>
                new MyStruct2 { field1 = e, field2 = new MyString(Convert.ToString("string" + e)), field3 = new NestedStruct { nestedField = e } });

        public static bool MyIsEqual(List<RankedEvent<char>> a, List<RankedEvent<char>> b)
        {
            if (a.Count != b.Count) return false;

            for (int j = 0; j < a.Count; j++)
            {
                if (!a[j].Equals(b[j])) return false;
            }

            return true;
        }

        public static int MyGetHashCode(List<RankedEvent<char>> a)
        {
            int ret = 0;
            foreach (var l in a)
            {
                ret ^= l.GetHashCode();
            }
            return ret;
        }

        private void TestWhere(Expression<Func<MyStruct2, bool>> predicate, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhere(predicate, disableFusion));

        private void TestSelect<TResult>(Expression<Func<MyStruct2, TResult>> function, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestSelect(function, disableFusion));

        public void TestWhereSelect<U>(Expression<Func<MyStruct2, bool>> wherePredicate, Expression<Func<MyStruct2, U>> selectFunction, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhereSelect(wherePredicate, selectFunction, disableFusion));

        [TestMethod, TestCategory("Gated")]
        public void Where1FusedColumnarSmallBatch()
            => TestWhere(e => e.field2.mystring.Contains("string"), false);

        [TestMethod, TestCategory("Gated")]
        public void Where2FusedColumnarSmallBatch()
            => TestWhere(e => e.field2.mystring.Equals("string0"), false);

        [TestMethod, TestCategory("Gated")]
        public void Where3FusedColumnarSmallBatch()
            => TestWhere(e => e.field2.mystring.Equals("string0"), false);

        [TestMethod, TestCategory("Gated")]
        public void Where4FusedColumnarSmallBatch()
            => TestWhere(e => e.field3.nestedField == 0, false);

        [TestMethod, TestCategory("Gated")]
        public void Where5FusedColumnarSmallBatch()
            => TestWhere(e => true, false);

        [TestMethod, TestCategory("Gated")]
        public void Where6FusedColumnarSmallBatch()
            => TestWhere(e => false, false);

        [TestMethod, TestCategory("Gated")]
        public void Where7FusedColumnarSmallBatch()
            => TestWhere(e => e.field3.nestedField == 0 || e.field3.nestedField == 1, false);

        [TestMethod, TestCategory("Gated")]
        public void Where8FusedColumnarSmallBatch()
            => TestWhere(e => string.Compare(e.field2.mystring, "string0") == 0, false);

        [TestMethod, TestCategory("Gated")]
        public void Where9FusedColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Where(v => v % 2 == 0)
                .Where(v => v % 3 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            Assert.IsTrue(input.Where(v => v % 6 == 0).SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereWithFailedTransformationFusedColumnarSmallBatch() // stays columnar, but causes payload to be reconstituted
            => Enumerable.Range(0, 100)
                .Select(i => new StructTuple<int, char> { Item1 = i, Item2 = i.ToString()[0], })
                .TestWhere(r => r.ToString().Length > 3, false);

        [TestMethod, TestCategory("Gated")]
        public void Select1FusedColumnarSmallBatch()
            => TestSelect(e => e.field1, false);

        [TestMethod, TestCategory("Gated")]
        public void Select2FusedColumnarSmallBatch()
            => TestSelect(e => e.field2, false);

        [TestMethod, TestCategory("Gated")]
        public void Select3FusedColumnarSmallBatch()
            => TestSelect(e => e.field3, false);

        [TestMethod, TestCategory("Gated")]
        public void Select4FusedColumnarSmallBatch()
            => TestSelect(e => new NestedStruct { nestedField = e.field1 }, false);

        [TestMethod, TestCategory("Gated")]
        public void Select5FusedColumnarSmallBatch()
            => TestSelect(e => e.doubleField, false);

        [TestMethod, TestCategory("Gated")]
        public void Select6FusedColumnarSmallBatch()
            => TestSelect(e => ((ulong)e.field1), false);

        // Tests to make sure that we ref count correctly by doing two pointer swings to the same column.
        [TestMethod, TestCategory("Gated")]
        public void Select7FusedColumnarSmallBatch()
            => TestSelect(e => new { A = e.field1, B = e.field1, }, false);

        // Tests whether an expression that cannot be columnerized causes code gen to fall back to row-oriented.
        // This is just a degenerate case where the expression is *always* null.
        [TestMethod, TestCategory("Gated")]
        public void Select8FusedColumnarSmallBatch()
            => TestSelect<ClassWithOnlyAutoProps>(e => null, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectFromUlongToLongFusedColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Select(v => (long)v)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, longInteger) => (long)integer == longInteger).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeFusedColumnarSmallBatch()
            => TestSelect(e => new { anonfield1 = e.field1, x = 3 }, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedFusedColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Select(v => new { X = v })
                .Select(e => new { Y = e.X })
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.Y).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedNoOpFusedColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Select(v => new { X = v })
                .Select(e => e)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.X).All(p => p));
        }

        // The point of this test is to have an anonymous type with a "field" in it
        // whose type does not have an overload for it in MemoryPool<TKey, TPayload>.Get(out ColumnBatch<>).
        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeWithFloatFieldFusedColumnarSmallBatch()
            => TestSelect(e => new { floatField = e.field1 * 3.0, }, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectWhereAnonymousTypeFusedColumnarSmallBatch()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithAutoPropsFusedColumnarSmallBatch()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithAutoPropsFusedColumnarSmallBatch()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithOnlyAutoPropsFusedColumnarSmallBatch()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithOnlyAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithOnlyAutoPropsFusedColumnarSmallBatch()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_SelectWhereFusedColumnarSmallBatch()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_GroupApplyFusedColumnarSmallBatch()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .GroupApply(e => e.field1, str => str.Count(), (g, cnt) => new MyStruct { field1 = (int)cnt, })
                .Where(e => e.field2 >= 0.0)
                .ToPayloadEnumerable();
            Assert.IsTrue(streamResult.Count() == this.enumerable.Count());
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectWithCtorCallFusedColumnarSmallBatch()
            => TestSelect(e => new StructWithCtor(e.field1), false);

        [TestMethod, TestCategory("Gated")]
        public void SelectMany01FusedColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany02FusedColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany03FusedColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany04FusedColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart01FusedColumnarSmallBatch()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .Select((l, e) => e + ((int)l))
                ;
            var expected = input
                .Select(e => e + e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart02FusedColumnarSmallBatch()
        {
            // tests simple parameter selection with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .Select((l, e) => l)
                ;
            var expected = input
                .Select(e => (long)e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey01FusedColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectByKey((k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey02FusedColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectByKey((k, e) => k)
                ;
            var expected = input
                .Select(e => Empty.Default)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey03FusedColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectByKey((s, k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e + (int)s, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e + e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey04FusedColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .Select(e => (int?)(e + 1))
                .SelectByKey((k, e) => new object[] { e, })
                ;
            var expected = input
                .Select(e => new object[] { (int?)(e + 1), })
                .ToArray()
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.Length == output.Length);
            for (int i = 0; i < expected.Length; i++)
            {
                var e = (int?)(expected[i][0]);
                var o = (int?)(output[i][0]);
                Assert.IsTrue(e.HasValue && o.HasValue && (e.Value == o.Value));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStart01FusedColumnarSmallBatch()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectMany((l, e) => Enumerable.Repeat(33, e + ((int)l)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + e))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyKey01FusedColumnarSmallBatch()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectManyByKey((k, e) => Enumerable.Repeat(33, e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStartKey01FusedColumnarSmallBatch()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectManyByKey((ts, k, e) => Enumerable.Repeat(33 + ((int)ts), e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33 + e, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereSelect1FusedColumnarSmallBatch()
            => TestWhereSelect(e => true, e => e.field1, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectStructFusedColumnarSmallBatch()
        {
            using (var modifier = new ConfigModifier().ForceRowBasedExecution(true).Modify())
            {
                var savedForceRowBasedExecution = Config.ForceRowBasedExecution;
                Config.ForceRowBasedExecution = true;

                var input = Enumerable.Range(0, 100);
                Expression<Func<int, MyStruct>> lambda = e => new MyStruct { field1 = e, field2 = e + 0.5, field3 = default };

                var streamResult = input
                    .ToStreamable()
                    .Select(lambda)
                    .ToPayloadEnumerable();
                var linqResult = input
                    .Select(lambda.Compile());

                var query = input.Where(e => true);
                Assert.IsTrue(linqResult.SequenceEqual(streamResult));
            }
        }

    }

    [TestClass]
    public class SimpleTestsFusedColumnarSmallBatchFloating : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public SimpleTestsFusedColumnarSmallBatchFloating() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .AllowFloatingReorderPolicy(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        // Total number of input events
        private const int NumEvents = 1000;
        private readonly IEnumerable<MyStruct2> enumerable = Enumerable.Range(0, NumEvents)
            .Select(e =>
                new MyStruct2 { field1 = e, field2 = new MyString(Convert.ToString("string" + e)), field3 = new NestedStruct { nestedField = e } });

        public static bool MyIsEqual(List<RankedEvent<char>> a, List<RankedEvent<char>> b)
        {
            if (a.Count != b.Count) return false;

            for (int j = 0; j < a.Count; j++)
            {
                if (!a[j].Equals(b[j])) return false;
            }

            return true;
        }

        public static int MyGetHashCode(List<RankedEvent<char>> a)
        {
            int ret = 0;
            foreach (var l in a)
            {
                ret ^= l.GetHashCode();
            }
            return ret;
        }

        private void TestWhere(Expression<Func<MyStruct2, bool>> predicate, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhere(predicate, disableFusion));

        private void TestSelect<TResult>(Expression<Func<MyStruct2, TResult>> function, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestSelect(function, disableFusion));

        public void TestWhereSelect<U>(Expression<Func<MyStruct2, bool>> wherePredicate, Expression<Func<MyStruct2, U>> selectFunction, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhereSelect(wherePredicate, selectFunction, disableFusion));

        [TestMethod, TestCategory("Gated")]
        public void Where1FusedColumnarSmallBatchFloating()
            => TestWhere(e => e.field2.mystring.Contains("string"), false);

        [TestMethod, TestCategory("Gated")]
        public void Where2FusedColumnarSmallBatchFloating()
            => TestWhere(e => e.field2.mystring.Equals("string0"), false);

        [TestMethod, TestCategory("Gated")]
        public void Where3FusedColumnarSmallBatchFloating()
            => TestWhere(e => e.field2.mystring.Equals("string0"), false);

        [TestMethod, TestCategory("Gated")]
        public void Where4FusedColumnarSmallBatchFloating()
            => TestWhere(e => e.field3.nestedField == 0, false);

        [TestMethod, TestCategory("Gated")]
        public void Where5FusedColumnarSmallBatchFloating()
            => TestWhere(e => true, false);

        [TestMethod, TestCategory("Gated")]
        public void Where6FusedColumnarSmallBatchFloating()
            => TestWhere(e => false, false);

        [TestMethod, TestCategory("Gated")]
        public void Where7FusedColumnarSmallBatchFloating()
            => TestWhere(e => e.field3.nestedField == 0 || e.field3.nestedField == 1, false);

        [TestMethod, TestCategory("Gated")]
        public void Where8FusedColumnarSmallBatchFloating()
            => TestWhere(e => string.Compare(e.field2.mystring, "string0") == 0, false);

        [TestMethod, TestCategory("Gated")]
        public void Where9FusedColumnarSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Where(v => v % 2 == 0)
                .Where(v => v % 3 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            Assert.IsTrue(input.Where(v => v % 6 == 0).SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereWithFailedTransformationFusedColumnarSmallBatchFloating() // stays columnar, but causes payload to be reconstituted
            => Enumerable.Range(0, 100)
                .Select(i => new StructTuple<int, char> { Item1 = i, Item2 = i.ToString()[0], })
                .TestWhere(r => r.ToString().Length > 3, false);

        [TestMethod, TestCategory("Gated")]
        public void Select1FusedColumnarSmallBatchFloating()
            => TestSelect(e => e.field1, false);

        [TestMethod, TestCategory("Gated")]
        public void Select2FusedColumnarSmallBatchFloating()
            => TestSelect(e => e.field2, false);

        [TestMethod, TestCategory("Gated")]
        public void Select3FusedColumnarSmallBatchFloating()
            => TestSelect(e => e.field3, false);

        [TestMethod, TestCategory("Gated")]
        public void Select4FusedColumnarSmallBatchFloating()
            => TestSelect(e => new NestedStruct { nestedField = e.field1 }, false);

        [TestMethod, TestCategory("Gated")]
        public void Select5FusedColumnarSmallBatchFloating()
            => TestSelect(e => e.doubleField, false);

        [TestMethod, TestCategory("Gated")]
        public void Select6FusedColumnarSmallBatchFloating()
            => TestSelect(e => ((ulong)e.field1), false);

        // Tests to make sure that we ref count correctly by doing two pointer swings to the same column.
        [TestMethod, TestCategory("Gated")]
        public void Select7FusedColumnarSmallBatchFloating()
            => TestSelect(e => new { A = e.field1, B = e.field1, }, false);

        // Tests whether an expression that cannot be columnerized causes code gen to fall back to row-oriented.
        // This is just a degenerate case where the expression is *always* null.
        [TestMethod, TestCategory("Gated")]
        public void Select8FusedColumnarSmallBatchFloating()
            => TestSelect<ClassWithOnlyAutoProps>(e => null, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectFromUlongToLongFusedColumnarSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Select(v => (long)v)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, longInteger) => (long)integer == longInteger).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeFusedColumnarSmallBatchFloating()
            => TestSelect(e => new { anonfield1 = e.field1, x = 3 }, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedFusedColumnarSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Select(v => new { X = v })
                .Select(e => new { Y = e.X })
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.Y).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedNoOpFusedColumnarSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .Select(v => new { X = v })
                .Select(e => e)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.X).All(p => p));
        }

        // The point of this test is to have an anonymous type with a "field" in it
        // whose type does not have an overload for it in MemoryPool<TKey, TPayload>.Get(out ColumnBatch<>).
        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeWithFloatFieldFusedColumnarSmallBatchFloating()
            => TestSelect(e => new { floatField = e.field1 * 3.0, }, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectWhereAnonymousTypeFusedColumnarSmallBatchFloating()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithAutoPropsFusedColumnarSmallBatchFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithAutoPropsFusedColumnarSmallBatchFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithOnlyAutoPropsFusedColumnarSmallBatchFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithOnlyAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithOnlyAutoPropsFusedColumnarSmallBatchFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_SelectWhereFusedColumnarSmallBatchFloating()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_GroupApplyFusedColumnarSmallBatchFloating()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .GroupApply(e => e.field1, str => str.Count(), (g, cnt) => new MyStruct { field1 = (int)cnt, })
                .Where(e => e.field2 >= 0.0)
                .ToPayloadEnumerable();
            Assert.IsTrue(streamResult.Count() == this.enumerable.Count());
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectWithCtorCallFusedColumnarSmallBatchFloating()
            => TestSelect(e => new StructWithCtor(e.field1), false);

        [TestMethod, TestCategory("Gated")]
        public void SelectMany01FusedColumnarSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany02FusedColumnarSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany03FusedColumnarSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany04FusedColumnarSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart01FusedColumnarSmallBatchFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .Select((l, e) => e + ((int)l))
                ;
            var expected = input
                .Select(e => e + e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart02FusedColumnarSmallBatchFloating()
        {
            // tests simple parameter selection with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .Select((l, e) => l)
                ;
            var expected = input
                .Select(e => (long)e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey01FusedColumnarSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectByKey((k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey02FusedColumnarSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectByKey((k, e) => k)
                ;
            var expected = input
                .Select(e => Empty.Default)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey03FusedColumnarSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectByKey((s, k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e + (int)s, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e + e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey04FusedColumnarSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .Select(e => (int?)(e + 1))
                .SelectByKey((k, e) => new object[] { e, })
                ;
            var expected = input
                .Select(e => new object[] { (int?)(e + 1), })
                .ToArray()
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.Length == output.Length);
            for (int i = 0; i < expected.Length; i++)
            {
                var e = (int?)(expected[i][0]);
                var o = (int?)(output[i][0]);
                Assert.IsTrue(e.HasValue && o.HasValue && (e.Value == o.Value));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStart01FusedColumnarSmallBatchFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectMany((l, e) => Enumerable.Repeat(33, e + ((int)l)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + e))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyKey01FusedColumnarSmallBatchFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectManyByKey((k, e) => Enumerable.Repeat(33, e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStartKey01FusedColumnarSmallBatchFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .SelectManyByKey((ts, k, e) => Enumerable.Repeat(33 + ((int)ts), e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33 + e, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereSelect1FusedColumnarSmallBatchFloating()
            => TestWhereSelect(e => true, e => e.field1, false);

        [TestMethod, TestCategory("Gated")]
        public void SelectStructFusedColumnarSmallBatchFloating()
        {
            using (var modifier = new ConfigModifier().ForceRowBasedExecution(true).Modify())
            {
                var savedForceRowBasedExecution = Config.ForceRowBasedExecution;
                Config.ForceRowBasedExecution = true;

                var input = Enumerable.Range(0, 100);
                Expression<Func<int, MyStruct>> lambda = e => new MyStruct { field1 = e, field2 = e + 0.5, field3 = default };

                var streamResult = input
                    .ToStreamable()
                    .Select(lambda)
                    .ToPayloadEnumerable();
                var linqResult = input
                    .Select(lambda.Compile());

                var query = input.Where(e => true);
                Assert.IsTrue(linqResult.SequenceEqual(streamResult));
            }
        }

    }

    [TestClass]
    public class SimpleTestsRow : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public SimpleTestsRow() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        // Total number of input events
        private const int NumEvents = 1000;
        private readonly IEnumerable<MyStruct2> enumerable = Enumerable.Range(0, NumEvents)
            .Select(e =>
                new MyStruct2 { field1 = e, field2 = new MyString(Convert.ToString("string" + e)), field3 = new NestedStruct { nestedField = e } });

        private static IComparerExpression<T> GetDefaultComparerExpression<T>(T t) => ComparerExpression<T>.Default;

        private static IEqualityComparerExpression<T> GetDefaultEqualityComparerExpression<T>(T t) => EqualityComparerExpression<T>.Default;

        public static bool MyIsEqual(List<RankedEvent<char>> a, List<RankedEvent<char>> b)
        {
            if (a.Count != b.Count) return false;

            for (int j = 0; j < a.Count; j++)
            {
                if (!a[j].Equals(b[j])) return false;
            }

            return true;
        }

        public static int MyGetHashCode(List<RankedEvent<char>> a)
        {
            int ret = 0;
            foreach (var l in a)
            {
                ret ^= l.GetHashCode();
            }
            return ret;
        }

        private void TestWhere(Expression<Func<MyStruct2, bool>> predicate, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhere(predicate, disableFusion));

        private void TestSelect<TResult>(Expression<Func<MyStruct2, TResult>> function, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestSelect(function, disableFusion));

        public void TestWhereSelect<U>(Expression<Func<MyStruct2, bool>> wherePredicate, Expression<Func<MyStruct2, U>> selectFunction, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhereSelect(wherePredicate, selectFunction, disableFusion));

        [TestMethod, TestCategory("Gated")]
        public void Where1Row()
            => TestWhere(e => e.field2.mystring.Contains("string"));

        [TestMethod, TestCategory("Gated")]
        public void Where2Row()
            => TestWhere(e => e.field2.mystring.Equals("string0"));

        [TestMethod, TestCategory("Gated")]
        public void Where3Row()
            => TestWhere(e => e.field2.mystring.Equals("string0"));

        [TestMethod, TestCategory("Gated")]
        public void Where4Row()
            => TestWhere(e => e.field3.nestedField == 0);

        [TestMethod, TestCategory("Gated")]
        public void Where5Row()
            => TestWhere(e => true);

        [TestMethod, TestCategory("Gated")]
        public void Where6Row()
            => TestWhere(e => false);

        [TestMethod, TestCategory("Gated")]
        public void Where7Row()
            => TestWhere(e => e.field3.nestedField == 0 || e.field3.nestedField == 1);

        [TestMethod, TestCategory("Gated")]
        public void Where8Row()
            => TestWhere(e => string.Compare(e.field2.mystring, "string0") == 0);

        [TestMethod, TestCategory("Gated")]
        public void Where9Row()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Where(v => v % 2 == 0)
                .Where(v => v % 3 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            Assert.IsTrue(input.Where(v => v % 6 == 0).SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereWithFailedTransformationRow() // stays columnar, but causes payload to be reconstituted
            => Enumerable.Range(0, 100)
                .Select(i => new StructTuple<int, char> { Item1 = i, Item2 = i.ToString()[0], })
                .TestWhere(r => r.ToString().Length > 3);

        [TestMethod, TestCategory("Gated")]
        public void Select1Row()
            => TestSelect(e => e.field1);

        [TestMethod, TestCategory("Gated")]
        public void Select2Row()
            => TestSelect(e => e.field2);

        [TestMethod, TestCategory("Gated")]
        public void Select3Row()
            => TestSelect(e => e.field3);

        [TestMethod, TestCategory("Gated")]
        public void Select4Row()
            => TestSelect(e => new NestedStruct { nestedField = e.field1 });

        [TestMethod, TestCategory("Gated")]
        public void Select5Row()
            => TestSelect(e => e.doubleField);

        [TestMethod, TestCategory("Gated")]
        public void Select6Row()
            => TestSelect(e => ((ulong)e.field1));

        // Tests to make sure that we ref count correctly by doing two pointer swings to the same column.
        [TestMethod, TestCategory("Gated")]
        public void Select7Row()
            => TestSelect(e => new { A = e.field1, B = e.field1, });

        // Tests whether an expression that cannot be columnerized causes code gen to fall back to row-oriented.
        // This is just a degenerate case where the expression is *always* null.
        [TestMethod, TestCategory("Gated")]
        public void Select8Row()
            => TestSelect<ClassWithOnlyAutoProps>(e => null);

        [TestMethod, TestCategory("Gated")]
        public void SelectFromUlongToLongRow()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(v => (long)v)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, longInteger) => (long)integer == longInteger).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeRow()
            => TestSelect(e => new { anonfield1 = e.field1, x = 3 });

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedRow()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(v => new { X = v })
                .Select(e => new { Y = e.X })
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.Y).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedNoOpRow()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(v => new { X = v })
                .Select(e => e)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.X).All(p => p));
        }

        // The point of this test is to have an anonymous type with a "field" in it
        // whose type does not have an overload for it in MemoryPool<TKey, TPayload>.Get(out ColumnBatch<>).
        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeWithFloatFieldRow()
            => TestSelect(e => new { floatField = e.field1 * 3.0, });

        [TestMethod, TestCategory("Gated")]
        public void SelectWhereAnonymousTypeRow()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithAutoPropsRow()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithAutoPropsRow()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithOnlyAutoPropsRow()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithOnlyAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithOnlyAutoPropsRow()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_SelectWhereRow()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_GroupApplyRow()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .GroupApply(e => e.field1, str => str.Count(), (g, cnt) => new MyStruct { field1 = (int)cnt, })
                .Where(e => e.field2 >= 0.0)
                .ToPayloadEnumerable();
            Assert.IsTrue(streamResult.Count() == this.enumerable.Count());
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectWithCtorCallRow()
            => TestSelect(e => new StructWithCtor(e.field1));

        [TestMethod, TestCategory("Gated")]
        public void SelectMany01Row()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany02Row()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany03Row()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany04Row()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart01Row()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select((l, e) => e + ((int)l))
                ;
            var expected = input
                .Select(e => e + e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart02Row()
        {
            // tests simple parameter selection with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select((l, e) => l)
                ;
            var expected = input
                .Select(e => (long)e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey01Row()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectByKey((k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey02Row()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectByKey((k, e) => k)
                ;
            var expected = input
                .Select(e => Empty.Default)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey03Row()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectByKey((s, k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e + (int)s, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e + e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey04Row()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => (int?)(e + 1))
                .SelectByKey((k, e) => new object[] { e, })
                ;
            var expected = input
                .Select(e => new object[] { (int?)(e + 1), })
                .ToArray()
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.Length == output.Length);
            for (int i = 0; i < expected.Length; i++)
            {
                var e = (int?)(expected[i][0]);
                var o = (int?)(output[i][0]);
                Assert.IsTrue(e.HasValue && o.HasValue && (e.Value == o.Value));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStart01Row()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany((l, e) => Enumerable.Repeat(33, e + ((int)l)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + e))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyKey01Row()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectManyByKey((k, e) => Enumerable.Repeat(33, e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStartKey01Row()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectManyByKey((ts, k, e) => Enumerable.Repeat(33 + ((int)ts), e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33 + e, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereSelect1Row()
            => TestWhereSelect(e => true, e => e.field1);

        [TestMethod, TestCategory("Gated")]
        public void SelectStructRow()
        {
            using (var modifier = new ConfigModifier().ForceRowBasedExecution(true).Modify())
            {
                var savedForceRowBasedExecution = Config.ForceRowBasedExecution;
                Config.ForceRowBasedExecution = true;

                var input = Enumerable.Range(0, 100);
                Expression<Func<int, MyStruct>> lambda = e => new MyStruct { field1 = e, field2 = e + 0.5, field3 = default };

                var streamResult = input
                    .ToStreamable()
                    .ShiftEventLifetime(0)
                    .Select(lambda)
                    .ToPayloadEnumerable();
                var linqResult = input
                    .Select(lambda.Compile());

                var query = input.Where(e => true);
                Assert.IsTrue(linqResult.SequenceEqual(streamResult));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantNoOpIntervalsRow()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 5, 'a'),
                StreamEvent.CreateInterval(2, 6, 'b'),
                StreamEvent.CreateInterval(3, 7, 'c'),
                StreamEvent.CreateInterval(4, 8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 5, 'a'),
                StreamEvent.CreateInterval(2, 6, 'b'),
                StreamEvent.CreateInterval(3, 7, 'c'),
                StreamEvent.CreateInterval(4, 8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(10)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantNoOpEdgesRow()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateEnd(6, 2, 'b'),
                StreamEvent.CreateEnd(7, 3, 'c'),
                StreamEvent.CreateEnd(8, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateEnd(6, 2, 'b'),
                StreamEvent.CreateEnd(7, 3, 'c'),
                StreamEvent.CreateEnd(8, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(10)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantClippedIntervalsRow()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 15, 'a'),
                StreamEvent.CreateInterval(2, 16, 'b'),
                StreamEvent.CreateInterval(3, 17, 'c'),
                StreamEvent.CreateInterval(4, 18, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 11, 'a'),
                StreamEvent.CreateInterval(2, 12, 'b'),
                StreamEvent.CreateInterval(3, 13, 'c'),
                StreamEvent.CreateInterval(4, 14, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(10)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantClippedEdgesRow()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateEnd(6, 2, 'b'),
                StreamEvent.CreateEnd(7, 3, 'c'),
                StreamEvent.CreateEnd(8, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateEnd(3, 1, 'a'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateEnd(4, 2, 'b'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 3, 'c'),
                StreamEvent.CreateEnd(6, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(2)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderPolicy1Row()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateEnd(20, 5, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(9, 'x'),
                StreamEvent.CreateEnd(20, 9, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderPolicy2Row()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreateEnd(24, 5, 'x'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(9, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreateEnd(24, 9, 'x'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderPolicy3Row()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreateEnd(20, 5, 'x'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(9, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreateEnd(22, 9, 'x'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var observInput = input.ToObservable();
            var str = observInput.ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderPolicy4Row()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateStart(11, 'z'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateEnd(20, 5, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreateEnd(24, 5, 'x'),
                StreamEvent.CreateEnd(26, 5, 'x'),
                StreamEvent.CreateEnd(28, 11, 'z'),
                StreamEvent.CreateEnd(30, 5, 'x'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(9, 'x'),
                StreamEvent.CreateStart(9, 'x'),
                StreamEvent.CreateStart(11, 'z'),
                StreamEvent.CreateStart(11, 'x'),
                StreamEvent.CreateStart(11, 'x'),
                StreamEvent.CreateEnd(20, 9, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreateEnd(24, 9, 'x'),
                StreamEvent.CreateEnd(26, 11, 'x'),
                StreamEvent.CreateEnd(28, 11, 'z'),
                StreamEvent.CreateEnd(30, 11, 'x'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void PunctuationPolicy2Row()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(4, 'c'),
                StreamEvent.CreateStart(7, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(34, 4, 'c'),
                StreamEvent.CreateEnd(35, 7, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };

            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreatePunctuation<char>(2),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreatePunctuation<char>(4),
                StreamEvent.CreateStart(4, 'c'),
                StreamEvent.CreatePunctuation<char>(6),
                StreamEvent.CreateStart(7, 'd'),
                StreamEvent.CreatePunctuation<char>(20),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreatePunctuation<char>(22),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreatePunctuation<char>(34),
                StreamEvent.CreateEnd(34, 4, 'c'),
                StreamEvent.CreateEnd(35, 7, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(2), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void AlterDurationTest1Row()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(3, 'x'),
                StreamEvent.CreateEnd(5, 3, 'x'),
                StreamEvent.CreateStart(7, 'y'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(3, 8, 'x'),
                StreamEvent.CreateInterval(7, 12, 'y'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var result = str.AlterEventDuration(5).ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void PunctuationPolicy3Row()
        { // Simulates the way Stat does ingress (but with 80K batches, not 3...)
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void StreamCache1Row()
        {
            var input = Enumerable.Range(0, 1000000); // make sure it is enough to have more than one data batch
            ulong limit = 10000;
            var cachedStream = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e))
                .ToStreamable(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .Cache(limit);
            var streamResult = cachedStream
                .ToPayloadEnumerable();

            var c = (ulong)streamResult.Count();
            Assert.IsTrue(c == limit);
            cachedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void StreamCache2Row()
        {
            var limit = 10; // 00000;
            var input = Enumerable.Range(0, limit); // make sure it is enough to have more than one data batch
            var cachedStream = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(StreamEvent.MinSyncTime, e))
                .ToStreamable(DisorderPolicy.Throw())
                .Cache();
            var streamResult = cachedStream
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.Payload)
                .ToEnumerable();
            var foo = streamResult.Count();

            Assert.IsTrue(input.SequenceEqual(streamResult));
            cachedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals1Row()
        {
            var x = ComparerExpression<int>.Default.GetCompareExpr()
                .ExpressionEquals(ComparerExpression<int>.Default.GetCompareExpr());
            Assert.IsTrue(x);
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals2Row()
        {
            var x = new CompoundGroupKeyEqualityComparer<Empty, MyStruct2>(EqualityComparerExpression<Empty>.Default, EqualityComparerExpression<MyStruct2>.Default);
            var y = x.GetEqualsExpr().ExpressionEquals(x.GetEqualsExpr());
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals3Row()
        {
            var x1 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var x2 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var y = x1.GetCompareExpr().ExpressionEquals(x2.GetCompareExpr());
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals4Row()
        {
            var x1 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var x2 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);

            var xx1 = new CompoundGroupKeyComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x1, ComparerExpression<MyStruct2>.Default);
            var xx2 = new CompoundGroupKeyComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x2, ComparerExpression<MyStruct2>.Default);

            var y = xx1.GetCompareExpr().ExpressionEquals(xx2.GetCompareExpr());
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void CompoundGroupKeyEqualityComparerDefaultRow()
        {
            var xx1 = EqualityComparerExpression<CompoundGroupKey<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>>.Default;

            var x2 = new CompoundGroupKeyEqualityComparer<Empty, MyStruct2>(EqualityComparerExpression<Empty>.Default, EqualityComparerExpression<MyStruct2>.Default);
            var xx2 = new CompoundGroupKeyEqualityComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x2, EqualityComparerExpression<MyStruct2>.Default);

            var y = xx1.ExpressionEquals(xx2);
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void CompoundGroupKeyComparerDefaultRow()
        {
            var xx1 = ComparerExpression<CompoundGroupKey<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>>.Default;

            var x2 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var xx2 = new CompoundGroupKeyComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x2, ComparerExpression<MyStruct2>.Default);

            var y = xx1.ExpressionEquals(xx2);
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForIComparableRow()
        {
            var defaultComparerExpressionProvider = ComparerExpression<ClassImplementingIComparable>.Default;
            var defaultCompareExpression = defaultComparerExpressionProvider.GetCompareExpr();
            var compiledDefaultCompareExpression = defaultCompareExpression.Compile();
            var o = new ClassImplementingIComparable();
            var result = compiledDefaultCompareExpression(o, o);
            Assert.IsTrue(result == 3);

        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForGenericIComparerRow()
        {
            var defaultComparerExpressionProvider = ComparerExpression<ClassImplementingGenericIComparer>.Default;
            var defaultCompareExpression = defaultComparerExpressionProvider.GetCompareExpr();
            var compiledDefaultCompareExpression = defaultCompareExpression.Compile();
            var o = new ClassImplementingGenericIComparer();
            var result = compiledDefaultCompareExpression(o, o);
            Assert.IsTrue(result == 3);

        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForNonGenericIComparerRow()
        {
            var defaultComparerExpressionProvider = ComparerExpression<ClassImplementingNonGenericIComparer>.Default;
            var defaultCompareExpression = defaultComparerExpressionProvider.GetCompareExpr();
            var compiledDefaultCompareExpression = defaultCompareExpression.Compile();
            var o = new ClassImplementingNonGenericIComparer();
            var result = compiledDefaultCompareExpression(o, o);
            Assert.IsTrue(result == 3);

        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForAnonymousType1Row()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var f = GetDefaultComparerExpression(a);
            var g = f.GetCompareExpr();
            var h = g.Compile();
            var k = h(a, a);
            Assert.IsTrue(k == 0);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType1Row()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, a);
            Assert.IsTrue(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType2Row()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var b = new { Z = i + 1, A = (char)('a' + i), W = "abc" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, b);
            Assert.IsFalse(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType3Row()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var b = new { Z = i, A = (char)('b' + i), W = "abc" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, b);
            Assert.IsFalse(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType4Row()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var b = new { Z = i, A = (char)('a' + i), W = "abcd" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, b);
            Assert.IsFalse(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void NaryMulticast1Row()
        {
            var enumerable = Enumerable.Range(0, 10000).ToList();

            var stream = enumerable.ToStreamable();
            var multiStream = stream.Multicast(3);

            var output1 = multiStream[0].Where(e => e % 3 == 0);
            var output2 = multiStream[1].Where(e => e % 3 == 1);
            var output3 = multiStream[2].Where(e => e % 3 == 2);
            var union = output1.Union(output2).Union(output3);

            var output = union.ToPayloadEnumerable().ToList();
            output.Sort();
            Assert.IsTrue(enumerable.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void NaryMulticastContainer1Row()
        {
            var outputList = new List<int>();

            // Inputs
            var container = new QueryContainer();
            Stream state = null;

            // Input data
            var enumerable = Enumerable.Range(0, 10000).ToList();
            var inputObservable = enumerable.ToObservable().Select(e => StreamEvent.CreateStart(0, e));

            // Query start
            var stream = container.RegisterInput(inputObservable);
            var multiStream = stream.Multicast(3);

            var output1 = multiStream[0].Where(e => e % 3 == 0);
            var output2 = multiStream[1].Where(e => e % 3 == 1);
            var output3 = multiStream[2].Where(e => e % 3 == 2);
            var union = output1.Union(output2).Union(output3);

            var outputObservable = container.RegisterOutput(union);

            // Test the output
            var outputAsync = outputObservable.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
            container.Restore(state);
            outputAsync.Wait();

            outputList.Sort();
            Assert.IsTrue(enumerable.SequenceEqual(outputList));
        }

        [TestMethod, TestCategory("Gated")]
        public void NaryMulticastContainerErrorRow()
        {
            // Inputs
            var container = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var enumerable = Enumerable.Range(0, 10000).ToList();
            var inputObservable = enumerable.ToObservable().Select(e => StreamEvent.CreateStart(0, e));

            // Query start
            var stream = container.RegisterInput(inputObservable);
            var multiStream = stream.Multicast(3);

            var output1 = multiStream[0].Where(e => e % 3 == 0);
            var output2 = multiStream[1].Where(e => e % 3 == 1);
            var output3 = multiStream[2].Where(e => e % 3 == 2);
            var union = output1.Union(output2).Union(output3);

            var outputObservable = container.RegisterOutput(union);

            // Validate that attempting to start the query before subscriptions will result in an error.
            bool foundException = false;
            try
            {
                container.Restore(state);
            }
            catch (Exception)
            {
                foundException = true;
            }

            Assert.IsTrue(foundException, "Expected an exception.");
        }

        [TestMethod, TestCategory("Gated")]
        public void StreamSortRow()
        {
            var input = Enumerable.Range(0, 20).Select(i => (char)('a' + i));
            var reversedInput = input.Reverse();
            var str = reversedInput.ToStatStreamable();
            var sortedStream = str.Sort(c => c);
            var result = sortedStream.ToStreamEventObservable().Where(se => se.IsData).Select(se => se.Payload).ToEnumerable();
            Assert.IsTrue(input.SequenceEqual(result));
            sortedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void SortModifiedStreamRow()
        {
            // The Sort method on a stream uses an observable cache. The cache
            // had been putting all rows from the messages into the list to be sorted,
            // even if it was an invalid row.
            var input = Enumerable.Range(0, 20).Select(i => (char)('a' + i));
            var str = input.ToStatStreamable();
            var str2 = str.Where(r => r != 'b');
            var sortedStream = str2.Sort(c => c);
            var result = sortedStream.ToStreamEventObservable().Where(se => se.IsData).Select(se => se.Payload).ToEnumerable();
            Assert.IsTrue(input.Count() == result.Count() + 1); // just make sure deleted element isn't counted anymore
            sortedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void GroupStreamableCodeGenTest1Row()
        {
            var input1 = new[]
            {
                StreamEvent.CreateInterval(100, 101, 13),
                StreamEvent.CreateInterval(100, 110, 11),
                StreamEvent.CreateInterval(101, 105, 12),
                StreamEvent.CreateInterval(102, 112, 21),
                StreamEvent.CreateInterval(109, 112, 14),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };

            var input2 = new[]
            {
                StreamEvent.CreateInterval(100, 110, 'd'),
                StreamEvent.CreateInterval(101, 103, 'b'),
                StreamEvent.CreateInterval(106, 108, 'b'),
                StreamEvent.CreateInterval(106, 109, 'b'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime)
            };

            var inputStream1 = input1.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var inputStream2 = input2.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var outputStream = inputStream1.WhereNotExists(
                inputStream2,
                l => l / 10,
                r => r - 'a');

            var expected = new[]
            {
                StreamEvent.CreateStart(100, 11),
                StreamEvent.CreateEnd(101, 100, 11),
                StreamEvent.CreateStart(103, 11),
                StreamEvent.CreateEnd(106, 103, 11),
                StreamEvent.CreateInterval(109, 110, 11),
                StreamEvent.CreateInterval(103, 105, 12),
                StreamEvent.CreateInterval(100, 101, 13),
                StreamEvent.CreateInterval(109, 112, 14),
                StreamEvent.CreateStart(102, 21),
                StreamEvent.CreateEnd(112, 102, 21),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };
            var outputEnumerable = outputStream.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void CountColumnar1Row()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<ulong>[]
            {
                StreamEvent.CreateStart<ulong>(StreamEvent.MinSyncTime, 3),
                StreamEvent.CreatePunctuation<ulong>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateEnd<ulong>(StreamEvent.MinSyncTime + 1, StreamEvent.MinSyncTime, 3),
                StreamEvent.CreateStart<ulong>(StreamEvent.MinSyncTime + 1, 6),
                StreamEvent.CreatePunctuation<ulong>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateEnd<ulong>(StreamEvent.MinSyncTime + 2, StreamEvent.MinSyncTime + 1, 6),
                StreamEvent.CreateStart<ulong>(StreamEvent.MinSyncTime + 2, 9),
                StreamEvent.CreatePunctuation<ulong>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None);
            var count = str.Count();
            var outputEnumerable = count.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageColumnar1Row() // tests codegen for Snapshot_noecq
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),      // 97
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),      // 98
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),      // 99  running average = 98

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),  // 100
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),  // 101
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),  // 102 running average = 99.5

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),  // 103
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),  // 104
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),  // 105 running average = 101
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<double>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 1, StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 2, StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 101.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None).SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime);
            var avg = str.Average(c => (double)c);
            var outputEnumerable = avg.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageColumnar2Row() // tests codegen for Snapshot_q
        {
            var zero = StreamEvent.MinSyncTime;
            var one = StreamEvent.MinSyncTime + 1;
            var two = StreamEvent.MinSyncTime + 2;
            var three = StreamEvent.MinSyncTime + 3;
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(zero, one, 'a'),
                StreamEvent.CreateInterval(zero, one, 'b'),
                StreamEvent.CreateInterval(zero, one, 'c'),
                StreamEvent.CreateInterval(one, two, 'd'),
                StreamEvent.CreateInterval(one, two, 'e'),
                StreamEvent.CreateInterval(one, two, 'f'),
                StreamEvent.CreateInterval(two, three, 'g'),
                StreamEvent.CreateInterval(two, three, 'h'),
                StreamEvent.CreateInterval(two, three, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<double>[]
            {
                StreamEvent.CreateInterval(zero, one, 98.0),
                StreamEvent.CreatePunctuation<double>(one),
                StreamEvent.CreateInterval(one, two, 101.0),
                StreamEvent.CreatePunctuation<double>(two),
                StreamEvent.CreateInterval(two, three, 104.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None).SetProperty().IsConstantDuration(true, 1);
            var avg = str.Average(c => (double)c);
            var outputEnumerable = avg.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageColumnar3Row() // tests codegen for Snapshot_pq
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),      // 97
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),      // 98
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),      // 99  running average = 98

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),  // 100
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),  // 101
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),  // 102 running average = 99.5

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),  // 103
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),  // 104
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),  // 105 running average = 101
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<double>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 1, StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 2, StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 101.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None);
            var avg = str.Average(c => (double)c);
            var outputEnumerable = avg.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SumColumnar1Row() // make sure expression is properly parenthesized in codegen
        {
            var input = Enumerable.Range(0, 100);
            Expression<Func<int, int>> query = e => e & 0x7;

            var streamResult = input
                .ToStreamable()
                .Sum(query)
                .ToPayloadEnumerable()
                .Last();

            var expected = input.Sum(query.Compile());
            Assert.IsTrue(expected == streamResult);

        }

        [TestMethod, TestCategory("Gated")]
        public void MultiAggregate1Row() // test codegen for multi-aggregates
        {
            var input = Enumerable.Range(0, 100).Select(i => new MyStruct { field1 = i, field2 = 0.0, field3 = Guid.Empty, });

            var streamResult = input
                .ToStreamable()
                .Aggregate(a => a.Sum(x => x.field1), a => a.Count(), (s, c) => new MyStruct { field1 = s + ((int)c) })
                .ToPayloadEnumerable()
                ;

            Assert.IsTrue(streamResult.Count() == 1);

            Assert.IsTrue(streamResult.First().field1 == 5050);

        }

        [TestMethod, TestCategory("Gated")]
        public void MultiAggregate2Row() // test codegen for multi-aggregates with anonymous type
        {
            var input = Enumerable.Range(0, 100).Select(i => new MyStruct { field1 = i, field2 = 0.0, field3 = Guid.Empty, });

            var streamResult = input
                .ToStreamable()
                .Aggregate(a => a.Sum(x => x.field1), a => a.Count(), (s, c) => new { field1 = s, field2 = c })
                .ToPayloadEnumerable()
                ;

            Assert.IsTrue(streamResult.Count() == 1);

            var f = streamResult.First();
            Assert.IsTrue(f.field1 == 4950 && f.field2 == 100);

        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest1Row()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(24, 3, 'c'),
                StreamEvent.CreateEnd(26, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateInterval(2, 22, 'b'),
                StreamEvent.CreateInterval(3, 24, 'c'),
                StreamEvent.CreateInterval(4, 26, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Cache(0, false, true);
            var result = str.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest2Row()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(24, 3, 'c'),
                StreamEvent.CreateEnd(26, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateInterval(3, 24, 'c'),
                StreamEvent.CreateInterval(4, 26, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Cache(0, false, true);
            var result = str.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest3Row()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(24, 4, 'd'),
                StreamEvent.CreateEnd(26, 3, 'c'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateInterval(2, 22, 'b'),
                StreamEvent.CreateInterval(3, 26, 'c'),
                StreamEvent.CreateInterval(4, 24, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Cache(0, false, true);
            var result = str.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void BinaryUnionTest1Row()
        {
            var input1 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(2, 20, 'a'),
                StreamEvent.CreateStart(4, 'b'),
                StreamEvent.CreateStart(6, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var input2 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(3, 20, 'a'),
                StreamEvent.CreateStart(5, 'b'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(9, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };

            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(2, 20, 'a'),
                StreamEvent.CreateInterval(3, 20, 'a'),
                StreamEvent.CreateStart(4, 'b'),
                StreamEvent.CreateStart(5, 'b'),
                StreamEvent.CreateStart(6, 'c'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreateStart(9, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str1 = input1.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null).Cache();
            var str2 = input2.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null).Cache();
            var resultEnum = str1.Union(str2).ToStreamEventObservable().ToEnumerable();
            var result = resultEnum.ToList();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str1.Dispose();
            str2.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void BinaryUnionTest2Row()
        {
            var input1 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var input2 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(5, 20, 'a'),
                StreamEvent.CreateStart(6, 'b'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };

            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateInterval(5, 20, 'a'),
                StreamEvent.CreateStart(6, 'b'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str1 = input1.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation).Cache();
            var str2 = input2.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation).Cache();
            var result = str1.Union(str2).ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str1.Dispose();
            str2.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void DeterministicUnionTestRow()
        {
            int count = 100;
            DateTime now = DateTime.UtcNow;

            var input = new List<StreamEvent<int>> { StreamEvent.CreatePunctuation<int>(now.Ticks) };

            // Add right events.
            for (int i = 0; i < count; ++i)
                input.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i + 1));

            // Cti forces right batch to go through before reaching Config.DataBatchSize.
            input.Add(StreamEvent.CreatePunctuation<int>(now.Ticks));

            // Add left events.
            for (int i = 0; i < count; ++i)
                input.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i));
            input.Add(StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime));

            // Expect left before right even though right went through first.
            var expectedOutput = new List<StreamEvent<int>> { StreamEvent.CreatePunctuation<int>(now.Ticks) };
            for (int i = 0; i < count; i++)
                expectedOutput.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i));
            for (int i = 0; i < count; i++)
                expectedOutput.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i + 1));
            expectedOutput.Add(StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime));

            var inputStreams = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Multicast(2);

            var left = inputStreams[0].Where(e => e % 2 == 0);
            var right = inputStreams[1].Where(e => e % 2 == 1);

            var result = left
                .Union(right)
                .ToStreamEventObservable()
                .ToEnumerable()
                .ToList();

            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest4Row()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateStart(6, 'b'),
                StreamEvent.CreateEnd(10, 6, 'b'),
                StreamEvent.CreateStart(11, 'c'),
                StreamEvent.CreateEnd(15, 11, 'c'),
                StreamEvent.CreateStart(16, 'd'),
                StreamEvent.CreateEnd(20, 16, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 5, 'a'),
                StreamEvent.CreateInterval(6, 10, 'b'),
                StreamEvent.CreateInterval(11, 15, 'c'),
                StreamEvent.CreateInterval(16, 20, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation);
            var result = str.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void AggregateTest1Row()
        {
            long threeSecondTicks = TimeSpan.FromSeconds(3).Ticks;

            var stream = Events().ToEvents(e => e.Vs.Ticks, e => e.Vs.Ticks + threeSecondTicks)
                                 .ToObservable()
                                 .ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None());
            var expectedResult = new StreamEvent<ResultEvent>[]
            {
                StreamEvent.CreateStart(633979008010000000, new ResultEvent { Key = 1, Cnt = 1 }),
                StreamEvent.CreateEnd(633979008020000000, 633979008010000000, new ResultEvent { Key = 1, Cnt = 1 }),
                StreamEvent.CreateStart(633979008020000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateEnd(633979008030000000, 633979008020000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateStart(633979008030000000, new ResultEvent { Key = 1, Cnt = 3 }),
                StreamEvent.CreateEnd(633979008040000000, 633979008030000000, new ResultEvent { Key = 1, Cnt = 3 }),
                StreamEvent.CreateStart(633979008040000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateEnd(633979008050000000, 633979008040000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateStart(633979008050000000, new ResultEvent { Key = 1, Cnt = 1 }),
                StreamEvent.CreateEnd(633979008060000000, 633979008050000000, new ResultEvent { Key = 1, Cnt = 1 }),
            };
            var counts = stream.GroupApply(
                                    g => g.Key,
                                    e => e.Count(),
                                    (g, e) => new ResultEvent { Key = g.Key, Cnt = e })
                               .ToStreamEventObservable()
                               .ToEnumerable()
                               .Where(e => e.IsData && e.Payload.Key == 1);
            foreach (var x in counts)
            {
                Console.WriteLine("{0}, {1}", x.SyncTime, x.OtherTime);
            }
            Assert.IsTrue(expectedResult.SequenceEqual(counts));
        }

        public IEnumerable<Event> Events()
        {
            var vs = new DateTime(2010, 1, 1, 0, 00, 00, DateTimeKind.Utc);
            var rand = new Random(0);
            int eventCnt = 5;
            for (int i = 0; i < eventCnt; i++)
            {
                yield return new Event { Vs = vs, V = rand.Next(10), Key = rand.Next(3) };
                vs = vs.AddSeconds(1);
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void CodeGenWithFieldOfGenericTypeRow()
        {
            // Taken from the PerformanceTest sample.
            // The GroupApply creates batches where the key is a generic type and codegen was doing a type cast without making sure the type's name was valid C# syntax
            var numEventsPerTumble = 2000;
            var tumblingWindowDataset =
                Observable.Range(0, 100)
                    .Select(e => StreamEvent.CreateInterval(((long)e / numEventsPerTumble) * numEventsPerTumble, ((long)e / numEventsPerTumble) * numEventsPerTumble + numEventsPerTumble, new Payload { field1 = 0, field2 = 0 }))
                    .ToStreamable()
                    .SetProperty().IsConstantDuration(true, numEventsPerTumble)
                    .SetProperty().IsSnapshotSorted(true, e => e.field1);
            var result = tumblingWindowDataset.GroupApply(e => e.field2, str => str.Count(), (g, c) => new { Key = g, Count = c });
            var a = result.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(a.Length == 3); // just make sure it doesn't crash
        }

        [TestMethod, TestCategory("Gated")]
        public void StringSerializationRow()
        {
            var rand = new Random(0);
            var pool = MemoryManager.GetColumnPool<string>();

            for (int x = 0; x < 100; x++)
            {
                pool.Get(out ColumnBatch<string> inputStr);

                var toss1 = rand.NextDouble();
                inputStr.UsedLength = toss1 < 0.1 ? 0 : rand.Next(Config.DataBatchSize);

                for (int i = 0; i < inputStr.UsedLength; i++)
                {
                    var toss = rand.NextDouble();
                    inputStr.col[i] = toss < 0.2 ? string.Empty : (toss < 0.4 ? null : Guid.NewGuid().ToString());
                    if (x == 0) inputStr.col[i] = null;
                    if (x == 1) inputStr.col[i] = string.Empty;
                }

                StateSerializer<ColumnBatch<string>> s = StreamableSerializer.Create<ColumnBatch<string>>(new SerializerSettings { });
                var ms = new MemoryStream { Position = 0 };

                s.Serialize(ms, inputStr);
                ms.Position = 0;
                ColumnBatch<string> resultStr = s.Deserialize(ms);

                Assert.IsTrue(resultStr.UsedLength == inputStr.UsedLength);

                for (int j = 0; j < inputStr.UsedLength; j++)
                {
                    Assert.IsTrue(inputStr.col[j] == resultStr.col[j]);
                }
                resultStr.ReturnClear();
                inputStr.ReturnClear();
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void CharArraySerializationRow()
        {
            using (var modifier = new ConfigModifier().SerializationCompressionLevel(SerializationCompressionLevel.CharArrayToUTF8).Modify())
            {
                var rand = new Random(0);

                for (int x = 0; x < 5; x++)
                {
                    var inputStr = new MultiString();

                    var toss1 = rand.NextDouble();
                    var usedLength = 1 + rand.Next(Config.DataBatchSize - 1);

                    for (int i = 0; i < usedLength; i++)
                    {
                        var toss = rand.NextDouble();
                        string str = toss < 0.2 ? string.Empty : Guid.NewGuid().ToString();
                        if (x == 0) str = string.Empty;
                        inputStr.AddString(str);
                    }

                    inputStr.Seal();

                    StateSerializer<MultiString> s = StreamableSerializer.Create<MultiString>(new SerializerSettings { });
                    var ms = new MemoryStream { Position = 0 };

                    s.Serialize(ms, inputStr);
                    ms.Position = 0;
                    var resultStr = s.Deserialize(ms);

                    Assert.IsTrue(resultStr.Count == inputStr.Count);

                    for (int j = 0; j < inputStr.Count; j++)
                    {
                        Assert.IsTrue(inputStr[j] == resultStr[j]);
                    }
                    resultStr.Dispose();
                    inputStr.Dispose();
                }
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void MultipleAggregatesWithCompiledDictionariesRow()
        {
            IEnumerable<StreamEvent<StreamScope_InputEvent0>> bacon = Array.Empty<StreamEvent<StreamScope_InputEvent0>>();
            IObservableIngressStreamable<StreamScope_InputEvent0> input = bacon.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var result =
                input
                .GroupAggregate(
                    s => new { s.OutputTs, s.DelayInMilliseconds, s.UserId },
                    s => s.Count(),
                    s => s.Min(r => r.Duration),
                    s => s.Max(r => r.Duration),
                    s => s.Average(r => r.Duration),
                    (key, cnt, minDuration, maxDuration, avgDuration) => new StreamScope_OutputEvent0()
                    {
                        OutputTs = key.Key.OutputTs,
                        DelayInMilliseconds = key.Key.DelayInMilliseconds,
                        UserId = key.Key.UserId,
                        Cnt = (long)cnt,
                        MinDuration = minDuration,
                        MaxDuration = maxDuration,
                        AvgDuration = avgDuration,
                    }).ToStreamEventObservable();

            var b = result;
        }

        [TestMethod, TestCategory("Gated")]
        public void PayloadWithFieldOfNestedType01Row()
        {
            var input = Enumerable
                .Range(0, NumEvents)
                .Select(i => new PayloadWithFieldOfNestedType1(i))
                ;
            var streamResult = input
                .ToStreamable()
                .Where(p => p.nt.x % 2 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var linqResult = input.Where(p => p.nt.x % 2 == 0);
            Assert.IsTrue(Enumerable.Zip(linqResult, a, (p1, p2) => p1.nt.x == p2.nt.x).All(p => p));
        }
        [TestMethod, TestCategory("Gated")]
        public void PayloadWithFieldOfNestedType02Row()
        {
            var input = Enumerable
                .Range(0, NumEvents)
                .Select(i => new PayloadWithFieldOfNestedType2(i))
                ;
            var streamResult = input
                .ToStreamable()
                .Where(p => p.nt.x % 2 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var linqResult = input.Where(p => p.nt.x % 2 == 0);
            Assert.IsTrue(Enumerable.Zip(linqResult, a, (p1, p2) => p1.nt.x == p2.nt.x).All(p => p));
        }
    }

    [TestClass]
    public class SimpleTestsRowFloating : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public SimpleTestsRowFloating() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .AllowFloatingReorderPolicy(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        // Total number of input events
        private const int NumEvents = 1000;
        private readonly IEnumerable<MyStruct2> enumerable = Enumerable.Range(0, NumEvents)
            .Select(e =>
                new MyStruct2 { field1 = e, field2 = new MyString(Convert.ToString("string" + e)), field3 = new NestedStruct { nestedField = e } });

        private static IComparerExpression<T> GetDefaultComparerExpression<T>(T t) => ComparerExpression<T>.Default;

        private static IEqualityComparerExpression<T> GetDefaultEqualityComparerExpression<T>(T t) => EqualityComparerExpression<T>.Default;

        public static bool MyIsEqual(List<RankedEvent<char>> a, List<RankedEvent<char>> b)
        {
            if (a.Count != b.Count) return false;

            for (int j = 0; j < a.Count; j++)
            {
                if (!a[j].Equals(b[j])) return false;
            }

            return true;
        }

        public static int MyGetHashCode(List<RankedEvent<char>> a)
        {
            int ret = 0;
            foreach (var l in a)
            {
                ret ^= l.GetHashCode();
            }
            return ret;
        }

        private void TestWhere(Expression<Func<MyStruct2, bool>> predicate, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhere(predicate, disableFusion));

        private void TestSelect<TResult>(Expression<Func<MyStruct2, TResult>> function, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestSelect(function, disableFusion));

        public void TestWhereSelect<U>(Expression<Func<MyStruct2, bool>> wherePredicate, Expression<Func<MyStruct2, U>> selectFunction, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhereSelect(wherePredicate, selectFunction, disableFusion));

        [TestMethod, TestCategory("Gated")]
        public void Where1RowFloating()
            => TestWhere(e => e.field2.mystring.Contains("string"));

        [TestMethod, TestCategory("Gated")]
        public void Where2RowFloating()
            => TestWhere(e => e.field2.mystring.Equals("string0"));

        [TestMethod, TestCategory("Gated")]
        public void Where3RowFloating()
            => TestWhere(e => e.field2.mystring.Equals("string0"));

        [TestMethod, TestCategory("Gated")]
        public void Where4RowFloating()
            => TestWhere(e => e.field3.nestedField == 0);

        [TestMethod, TestCategory("Gated")]
        public void Where5RowFloating()
            => TestWhere(e => true);

        [TestMethod, TestCategory("Gated")]
        public void Where6RowFloating()
            => TestWhere(e => false);

        [TestMethod, TestCategory("Gated")]
        public void Where7RowFloating()
            => TestWhere(e => e.field3.nestedField == 0 || e.field3.nestedField == 1);

        [TestMethod, TestCategory("Gated")]
        public void Where8RowFloating()
            => TestWhere(e => string.Compare(e.field2.mystring, "string0") == 0);

        [TestMethod, TestCategory("Gated")]
        public void Where9RowFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Where(v => v % 2 == 0)
                .Where(v => v % 3 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            Assert.IsTrue(input.Where(v => v % 6 == 0).SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereWithFailedTransformationRowFloating() // stays columnar, but causes payload to be reconstituted
            => Enumerable.Range(0, 100)
                .Select(i => new StructTuple<int, char> { Item1 = i, Item2 = i.ToString()[0], })
                .TestWhere(r => r.ToString().Length > 3);

        [TestMethod, TestCategory("Gated")]
        public void Select1RowFloating()
            => TestSelect(e => e.field1);

        [TestMethod, TestCategory("Gated")]
        public void Select2RowFloating()
            => TestSelect(e => e.field2);

        [TestMethod, TestCategory("Gated")]
        public void Select3RowFloating()
            => TestSelect(e => e.field3);

        [TestMethod, TestCategory("Gated")]
        public void Select4RowFloating()
            => TestSelect(e => new NestedStruct { nestedField = e.field1 });

        [TestMethod, TestCategory("Gated")]
        public void Select5RowFloating()
            => TestSelect(e => e.doubleField);

        [TestMethod, TestCategory("Gated")]
        public void Select6RowFloating()
            => TestSelect(e => ((ulong)e.field1));

        // Tests to make sure that we ref count correctly by doing two pointer swings to the same column.
        [TestMethod, TestCategory("Gated")]
        public void Select7RowFloating()
            => TestSelect(e => new { A = e.field1, B = e.field1, });

        // Tests whether an expression that cannot be columnerized causes code gen to fall back to row-oriented.
        // This is just a degenerate case where the expression is *always* null.
        [TestMethod, TestCategory("Gated")]
        public void Select8RowFloating()
            => TestSelect<ClassWithOnlyAutoProps>(e => null);

        [TestMethod, TestCategory("Gated")]
        public void SelectFromUlongToLongRowFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(v => (long)v)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, longInteger) => (long)integer == longInteger).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeRowFloating()
            => TestSelect(e => new { anonfield1 = e.field1, x = 3 });

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedRowFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(v => new { X = v })
                .Select(e => new { Y = e.X })
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.Y).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedNoOpRowFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(v => new { X = v })
                .Select(e => e)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.X).All(p => p));
        }

        // The point of this test is to have an anonymous type with a "field" in it
        // whose type does not have an overload for it in MemoryPool<TKey, TPayload>.Get(out ColumnBatch<>).
        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeWithFloatFieldRowFloating()
            => TestSelect(e => new { floatField = e.field1 * 3.0, });

        [TestMethod, TestCategory("Gated")]
        public void SelectWhereAnonymousTypeRowFloating()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithAutoPropsRowFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithAutoPropsRowFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithOnlyAutoPropsRowFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithOnlyAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithOnlyAutoPropsRowFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_SelectWhereRowFloating()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_GroupApplyRowFloating()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .GroupApply(e => e.field1, str => str.Count(), (g, cnt) => new MyStruct { field1 = (int)cnt, })
                .Where(e => e.field2 >= 0.0)
                .ToPayloadEnumerable();
            Assert.IsTrue(streamResult.Count() == this.enumerable.Count());
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectWithCtorCallRowFloating()
            => TestSelect(e => new StructWithCtor(e.field1));

        [TestMethod, TestCategory("Gated")]
        public void SelectMany01RowFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany02RowFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany03RowFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany04RowFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart01RowFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select((l, e) => e + ((int)l))
                ;
            var expected = input
                .Select(e => e + e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart02RowFloating()
        {
            // tests simple parameter selection with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select((l, e) => l)
                ;
            var expected = input
                .Select(e => (long)e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey01RowFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectByKey((k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey02RowFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectByKey((k, e) => k)
                ;
            var expected = input
                .Select(e => Empty.Default)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey03RowFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectByKey((s, k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e + (int)s, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e + e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey04RowFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => (int?)(e + 1))
                .SelectByKey((k, e) => new object[] { e, })
                ;
            var expected = input
                .Select(e => new object[] { (int?)(e + 1), })
                .ToArray()
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.Length == output.Length);
            for (int i = 0; i < expected.Length; i++)
            {
                var e = (int?)(expected[i][0]);
                var o = (int?)(output[i][0]);
                Assert.IsTrue(e.HasValue && o.HasValue && (e.Value == o.Value));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStart01RowFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany((l, e) => Enumerable.Repeat(33, e + ((int)l)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + e))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyKey01RowFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectManyByKey((k, e) => Enumerable.Repeat(33, e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStartKey01RowFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectManyByKey((ts, k, e) => Enumerable.Repeat(33 + ((int)ts), e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33 + e, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereSelect1RowFloating()
            => TestWhereSelect(e => true, e => e.field1);

        [TestMethod, TestCategory("Gated")]
        public void SelectStructRowFloating()
        {
            using (var modifier = new ConfigModifier().ForceRowBasedExecution(true).Modify())
            {
                var savedForceRowBasedExecution = Config.ForceRowBasedExecution;
                Config.ForceRowBasedExecution = true;

                var input = Enumerable.Range(0, 100);
                Expression<Func<int, MyStruct>> lambda = e => new MyStruct { field1 = e, field2 = e + 0.5, field3 = default };

                var streamResult = input
                    .ToStreamable()
                    .ShiftEventLifetime(0)
                    .Select(lambda)
                    .ToPayloadEnumerable();
                var linqResult = input
                    .Select(lambda.Compile());

                var query = input.Where(e => true);
                Assert.IsTrue(linqResult.SequenceEqual(streamResult));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantNoOpIntervalsRowFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 5, 'a'),
                StreamEvent.CreateInterval(2, 6, 'b'),
                StreamEvent.CreateInterval(3, 7, 'c'),
                StreamEvent.CreateInterval(4, 8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 5, 'a'),
                StreamEvent.CreateInterval(2, 6, 'b'),
                StreamEvent.CreateInterval(3, 7, 'c'),
                StreamEvent.CreateInterval(4, 8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(10)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantNoOpEdgesRowFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateEnd(6, 2, 'b'),
                StreamEvent.CreateEnd(7, 3, 'c'),
                StreamEvent.CreateEnd(8, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateEnd(6, 2, 'b'),
                StreamEvent.CreateEnd(7, 3, 'c'),
                StreamEvent.CreateEnd(8, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(10)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantClippedIntervalsRowFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 15, 'a'),
                StreamEvent.CreateInterval(2, 16, 'b'),
                StreamEvent.CreateInterval(3, 17, 'c'),
                StreamEvent.CreateInterval(4, 18, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 11, 'a'),
                StreamEvent.CreateInterval(2, 12, 'b'),
                StreamEvent.CreateInterval(3, 13, 'c'),
                StreamEvent.CreateInterval(4, 14, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(10)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantClippedEdgesRowFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateEnd(6, 2, 'b'),
                StreamEvent.CreateEnd(7, 3, 'c'),
                StreamEvent.CreateEnd(8, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateEnd(3, 1, 'a'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateEnd(4, 2, 'b'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 3, 'c'),
                StreamEvent.CreateEnd(6, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(2)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void PunctuationPolicy2RowFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(4, 'c'),
                StreamEvent.CreateStart(7, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(34, 4, 'c'),
                StreamEvent.CreateEnd(35, 7, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };

            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(4, 'c'),
                StreamEvent.CreateStart(7, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(34, 4, 'c'),
                StreamEvent.CreateEnd(35, 7, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(2), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void AlterDurationTest1RowFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(3, 'x'),
                StreamEvent.CreateEnd(5, 3, 'x'),
                StreamEvent.CreateStart(7, 'y'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(3, 8, 'x'),
                StreamEvent.CreateInterval(7, 12, 'y'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var result = str.AlterEventDuration(5).ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void PunctuationPolicy3RowFloating()
        { // Simulates the way Stat does ingress (but with 80K batches, not 3...)
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void StreamCache1RowFloating()
        {
            var input = Enumerable.Range(0, 1000000); // make sure it is enough to have more than one data batch
            ulong limit = 10000;
            var cachedStream = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e))
                .ToStreamable(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .Cache(limit);
            var streamResult = cachedStream
                .ToPayloadEnumerable();

            var c = (ulong)streamResult.Count();
            Assert.IsTrue(c == limit);
            cachedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void StreamCache2RowFloating()
        {
            var limit = 10; // 00000;
            var input = Enumerable.Range(0, limit); // make sure it is enough to have more than one data batch
            var cachedStream = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(StreamEvent.MinSyncTime, e))
                .ToStreamable(DisorderPolicy.Throw())
                .Cache();
            var streamResult = cachedStream
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.Payload)
                .ToEnumerable();
            var foo = streamResult.Count();

            Assert.IsTrue(input.SequenceEqual(streamResult));
            cachedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals1RowFloating()
        {
            var x = ComparerExpression<int>.Default.GetCompareExpr()
                .ExpressionEquals(ComparerExpression<int>.Default.GetCompareExpr());
            Assert.IsTrue(x);
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals2RowFloating()
        {
            var x = new CompoundGroupKeyEqualityComparer<Empty, MyStruct2>(EqualityComparerExpression<Empty>.Default, EqualityComparerExpression<MyStruct2>.Default);
            var y = x.GetEqualsExpr().ExpressionEquals(x.GetEqualsExpr());
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals3RowFloating()
        {
            var x1 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var x2 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var y = x1.GetCompareExpr().ExpressionEquals(x2.GetCompareExpr());
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals4RowFloating()
        {
            var x1 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var x2 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);

            var xx1 = new CompoundGroupKeyComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x1, ComparerExpression<MyStruct2>.Default);
            var xx2 = new CompoundGroupKeyComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x2, ComparerExpression<MyStruct2>.Default);

            var y = xx1.GetCompareExpr().ExpressionEquals(xx2.GetCompareExpr());
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void CompoundGroupKeyEqualityComparerDefaultRowFloating()
        {
            var xx1 = EqualityComparerExpression<CompoundGroupKey<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>>.Default;

            var x2 = new CompoundGroupKeyEqualityComparer<Empty, MyStruct2>(EqualityComparerExpression<Empty>.Default, EqualityComparerExpression<MyStruct2>.Default);
            var xx2 = new CompoundGroupKeyEqualityComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x2, EqualityComparerExpression<MyStruct2>.Default);

            var y = xx1.ExpressionEquals(xx2);
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void CompoundGroupKeyComparerDefaultRowFloating()
        {
            var xx1 = ComparerExpression<CompoundGroupKey<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>>.Default;

            var x2 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var xx2 = new CompoundGroupKeyComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x2, ComparerExpression<MyStruct2>.Default);

            var y = xx1.ExpressionEquals(xx2);
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForIComparableRowFloating()
        {
            var defaultComparerExpressionProvider = ComparerExpression<ClassImplementingIComparable>.Default;
            var defaultCompareExpression = defaultComparerExpressionProvider.GetCompareExpr();
            var compiledDefaultCompareExpression = defaultCompareExpression.Compile();
            var o = new ClassImplementingIComparable();
            var result = compiledDefaultCompareExpression(o, o);
            Assert.IsTrue(result == 3);

        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForGenericIComparerRowFloating()
        {
            var defaultComparerExpressionProvider = ComparerExpression<ClassImplementingGenericIComparer>.Default;
            var defaultCompareExpression = defaultComparerExpressionProvider.GetCompareExpr();
            var compiledDefaultCompareExpression = defaultCompareExpression.Compile();
            var o = new ClassImplementingGenericIComparer();
            var result = compiledDefaultCompareExpression(o, o);
            Assert.IsTrue(result == 3);

        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForNonGenericIComparerRowFloating()
        {
            var defaultComparerExpressionProvider = ComparerExpression<ClassImplementingNonGenericIComparer>.Default;
            var defaultCompareExpression = defaultComparerExpressionProvider.GetCompareExpr();
            var compiledDefaultCompareExpression = defaultCompareExpression.Compile();
            var o = new ClassImplementingNonGenericIComparer();
            var result = compiledDefaultCompareExpression(o, o);
            Assert.IsTrue(result == 3);

        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForAnonymousType1RowFloating()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var f = GetDefaultComparerExpression(a);
            var g = f.GetCompareExpr();
            var h = g.Compile();
            var k = h(a, a);
            Assert.IsTrue(k == 0);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType1RowFloating()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, a);
            Assert.IsTrue(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType2RowFloating()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var b = new { Z = i + 1, A = (char)('a' + i), W = "abc" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, b);
            Assert.IsFalse(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType3RowFloating()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var b = new { Z = i, A = (char)('b' + i), W = "abc" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, b);
            Assert.IsFalse(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType4RowFloating()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var b = new { Z = i, A = (char)('a' + i), W = "abcd" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, b);
            Assert.IsFalse(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void NaryMulticast1RowFloating()
        {
            var enumerable = Enumerable.Range(0, 10000).ToList();

            var stream = enumerable.ToStreamable();
            var multiStream = stream.Multicast(3);

            var output1 = multiStream[0].Where(e => e % 3 == 0);
            var output2 = multiStream[1].Where(e => e % 3 == 1);
            var output3 = multiStream[2].Where(e => e % 3 == 2);
            var union = output1.Union(output2).Union(output3);

            var output = union.ToPayloadEnumerable().ToList();
            output.Sort();
            Assert.IsTrue(enumerable.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void NaryMulticastContainer1RowFloating()
        {
            var outputList = new List<int>();

            // Inputs
            var container = new QueryContainer();
            Stream state = null;

            // Input data
            var enumerable = Enumerable.Range(0, 10000).ToList();
            var inputObservable = enumerable.ToObservable().Select(e => StreamEvent.CreateStart(0, e));

            // Query start
            var stream = container.RegisterInput(inputObservable);
            var multiStream = stream.Multicast(3);

            var output1 = multiStream[0].Where(e => e % 3 == 0);
            var output2 = multiStream[1].Where(e => e % 3 == 1);
            var output3 = multiStream[2].Where(e => e % 3 == 2);
            var union = output1.Union(output2).Union(output3);

            var outputObservable = container.RegisterOutput(union);

            // Test the output
            var outputAsync = outputObservable.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
            container.Restore(state);
            outputAsync.Wait();

            outputList.Sort();
            Assert.IsTrue(enumerable.SequenceEqual(outputList));
        }

        [TestMethod, TestCategory("Gated")]
        public void NaryMulticastContainerErrorRowFloating()
        {
            // Inputs
            var container = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var enumerable = Enumerable.Range(0, 10000).ToList();
            var inputObservable = enumerable.ToObservable().Select(e => StreamEvent.CreateStart(0, e));

            // Query start
            var stream = container.RegisterInput(inputObservable);
            var multiStream = stream.Multicast(3);

            var output1 = multiStream[0].Where(e => e % 3 == 0);
            var output2 = multiStream[1].Where(e => e % 3 == 1);
            var output3 = multiStream[2].Where(e => e % 3 == 2);
            var union = output1.Union(output2).Union(output3);

            var outputObservable = container.RegisterOutput(union);

            // Validate that attempting to start the query before subscriptions will result in an error.
            bool foundException = false;
            try
            {
                container.Restore(state);
            }
            catch (Exception)
            {
                foundException = true;
            }

            Assert.IsTrue(foundException, "Expected an exception.");
        }

        [TestMethod, TestCategory("Gated")]
        public void StreamSortRowFloating()
        {
            var input = Enumerable.Range(0, 20).Select(i => (char)('a' + i));
            var reversedInput = input.Reverse();
            var str = reversedInput.ToStatStreamable();
            var sortedStream = str.Sort(c => c);
            var result = sortedStream.ToStreamEventObservable().Where(se => se.IsData).Select(se => se.Payload).ToEnumerable();
            Assert.IsTrue(input.SequenceEqual(result));
            sortedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void SortModifiedStreamRowFloating()
        {
            // The Sort method on a stream uses an observable cache. The cache
            // had been putting all rows from the messages into the list to be sorted,
            // even if it was an invalid row.
            var input = Enumerable.Range(0, 20).Select(i => (char)('a' + i));
            var str = input.ToStatStreamable();
            var str2 = str.Where(r => r != 'b');
            var sortedStream = str2.Sort(c => c);
            var result = sortedStream.ToStreamEventObservable().Where(se => se.IsData).Select(se => se.Payload).ToEnumerable();
            Assert.IsTrue(input.Count() == result.Count() + 1); // just make sure deleted element isn't counted anymore
            sortedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void GroupStreamableCodeGenTest1RowFloating()
        {
            var input1 = new[]
            {
                StreamEvent.CreateInterval(100, 101, 13),
                StreamEvent.CreateInterval(100, 110, 11),
                StreamEvent.CreateInterval(101, 105, 12),
                StreamEvent.CreateInterval(102, 112, 21),
                StreamEvent.CreateInterval(109, 112, 14),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };

            var input2 = new[]
            {
                StreamEvent.CreateInterval(100, 110, 'd'),
                StreamEvent.CreateInterval(101, 103, 'b'),
                StreamEvent.CreateInterval(106, 108, 'b'),
                StreamEvent.CreateInterval(106, 109, 'b'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime)
            };

            var inputStream1 = input1.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var inputStream2 = input2.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var outputStream = inputStream1.WhereNotExists(
                inputStream2,
                l => l / 10,
                r => r - 'a');

            var expected = new[]
            {
                StreamEvent.CreateStart(100, 11),
                StreamEvent.CreateEnd(101, 100, 11),
                StreamEvent.CreateStart(103, 11),
                StreamEvent.CreateEnd(106, 103, 11),
                StreamEvent.CreateInterval(109, 110, 11),
                StreamEvent.CreateInterval(103, 105, 12),
                StreamEvent.CreateInterval(100, 101, 13),
                StreamEvent.CreateInterval(109, 112, 14),
                StreamEvent.CreateStart(102, 21),
                StreamEvent.CreateEnd(112, 102, 21),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };
            var outputEnumerable = outputStream.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void CountColumnar1RowFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<ulong>[]
            {
                StreamEvent.CreateStart<ulong>(StreamEvent.MinSyncTime, 3),
                StreamEvent.CreatePunctuation<ulong>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateEnd<ulong>(StreamEvent.MinSyncTime + 1, StreamEvent.MinSyncTime, 3),
                StreamEvent.CreateStart<ulong>(StreamEvent.MinSyncTime + 1, 6),
                StreamEvent.CreatePunctuation<ulong>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateEnd<ulong>(StreamEvent.MinSyncTime + 2, StreamEvent.MinSyncTime + 1, 6),
                StreamEvent.CreateStart<ulong>(StreamEvent.MinSyncTime + 2, 9),
                StreamEvent.CreatePunctuation<ulong>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None);
            var count = str.Count();
            var outputEnumerable = count.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageColumnar1RowFloating() // tests codegen for Snapshot_noecq
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),      // 97
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),      // 98
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),      // 99  running average = 98

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),  // 100
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),  // 101
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),  // 102 running average = 99.5

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),  // 103
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),  // 104
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),  // 105 running average = 101
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<double>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 1, StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 2, StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 101.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None).SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime);
            var avg = str.Average(c => (double)c);
            var outputEnumerable = avg.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageColumnar2RowFloating() // tests codegen for Snapshot_q
        {
            var zero = StreamEvent.MinSyncTime;
            var one = StreamEvent.MinSyncTime + 1;
            var two = StreamEvent.MinSyncTime + 2;
            var three = StreamEvent.MinSyncTime + 3;
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(zero, one, 'a'),
                StreamEvent.CreateInterval(zero, one, 'b'),
                StreamEvent.CreateInterval(zero, one, 'c'),
                StreamEvent.CreateInterval(one, two, 'd'),
                StreamEvent.CreateInterval(one, two, 'e'),
                StreamEvent.CreateInterval(one, two, 'f'),
                StreamEvent.CreateInterval(two, three, 'g'),
                StreamEvent.CreateInterval(two, three, 'h'),
                StreamEvent.CreateInterval(two, three, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<double>[]
            {
                StreamEvent.CreateInterval(zero, one, 98.0),
                StreamEvent.CreatePunctuation<double>(one),
                StreamEvent.CreateInterval(one, two, 101.0),
                StreamEvent.CreatePunctuation<double>(two),
                StreamEvent.CreateInterval(two, three, 104.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None).SetProperty().IsConstantDuration(true, 1);
            var avg = str.Average(c => (double)c);
            var outputEnumerable = avg.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageColumnar3RowFloating() // tests codegen for Snapshot_pq
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),      // 97
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),      // 98
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),      // 99  running average = 98

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),  // 100
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),  // 101
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),  // 102 running average = 99.5

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),  // 103
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),  // 104
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),  // 105 running average = 101
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<double>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 1, StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 2, StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 101.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None);
            var avg = str.Average(c => (double)c);
            var outputEnumerable = avg.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SumColumnar1RowFloating() // make sure expression is properly parenthesized in codegen
        {
            var input = Enumerable.Range(0, 100);
            Expression<Func<int, int>> query = e => e & 0x7;

            var streamResult = input
                .ToStreamable()
                .Sum(query)
                .ToPayloadEnumerable()
                .Last();

            var expected = input.Sum(query.Compile());
            Assert.IsTrue(expected == streamResult);

        }

        [TestMethod, TestCategory("Gated")]
        public void MultiAggregate1RowFloating() // test codegen for multi-aggregates
        {
            var input = Enumerable.Range(0, 100).Select(i => new MyStruct { field1 = i, field2 = 0.0, field3 = Guid.Empty, });

            var streamResult = input
                .ToStreamable()
                .Aggregate(a => a.Sum(x => x.field1), a => a.Count(), (s, c) => new MyStruct { field1 = s + ((int)c) })
                .ToPayloadEnumerable()
                ;

            Assert.IsTrue(streamResult.Count() == 1);

            Assert.IsTrue(streamResult.First().field1 == 5050);

        }

        [TestMethod, TestCategory("Gated")]
        public void MultiAggregate2RowFloating() // test codegen for multi-aggregates with anonymous type
        {
            var input = Enumerable.Range(0, 100).Select(i => new MyStruct { field1 = i, field2 = 0.0, field3 = Guid.Empty, });

            var streamResult = input
                .ToStreamable()
                .Aggregate(a => a.Sum(x => x.field1), a => a.Count(), (s, c) => new { field1 = s, field2 = c })
                .ToPayloadEnumerable()
                ;

            Assert.IsTrue(streamResult.Count() == 1);

            var f = streamResult.First();
            Assert.IsTrue(f.field1 == 4950 && f.field2 == 100);

        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest1RowFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(24, 3, 'c'),
                StreamEvent.CreateEnd(26, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateInterval(2, 22, 'b'),
                StreamEvent.CreateInterval(3, 24, 'c'),
                StreamEvent.CreateInterval(4, 26, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Cache(0, false, true);
            var result = str.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest2RowFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(24, 3, 'c'),
                StreamEvent.CreateEnd(26, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateInterval(3, 24, 'c'),
                StreamEvent.CreateInterval(4, 26, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Cache(0, false, true);
            var result = str.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest3RowFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(24, 4, 'd'),
                StreamEvent.CreateEnd(26, 3, 'c'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateInterval(2, 22, 'b'),
                StreamEvent.CreateInterval(3, 26, 'c'),
                StreamEvent.CreateInterval(4, 24, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Cache(0, false, true);
            var result = str.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void BinaryUnionTest1RowFloating()
        {
            var input1 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(2, 20, 'a'),
                StreamEvent.CreateStart(4, 'b'),
                StreamEvent.CreateStart(6, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var input2 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(3, 20, 'a'),
                StreamEvent.CreateStart(5, 'b'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(9, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };

            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(2, 20, 'a'),
                StreamEvent.CreateInterval(3, 20, 'a'),
                StreamEvent.CreateStart(4, 'b'),
                StreamEvent.CreateStart(5, 'b'),
                StreamEvent.CreateStart(6, 'c'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreateStart(9, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str1 = input1.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null).Cache();
            var str2 = input2.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null).Cache();
            var resultEnum = str1.Union(str2).ToStreamEventObservable().ToEnumerable();
            var result = resultEnum.ToList();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str1.Dispose();
            str2.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void BinaryUnionTest2RowFloating()
        {
            var input1 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var input2 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(5, 20, 'a'),
                StreamEvent.CreateStart(6, 'b'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };

            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateInterval(5, 20, 'a'),
                StreamEvent.CreateStart(6, 'b'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str1 = input1.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation).Cache();
            var str2 = input2.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation).Cache();
            var result = str1.Union(str2).ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str1.Dispose();
            str2.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void DeterministicUnionTestRowFloating()
        {
            int count = 100;
            DateTime now = DateTime.UtcNow;

            var input = new List<StreamEvent<int>> { StreamEvent.CreatePunctuation<int>(now.Ticks) };

            // Add right events.
            for (int i = 0; i < count; ++i)
                input.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i + 1));

            // Cti forces right batch to go through before reaching Config.DataBatchSize.
            input.Add(StreamEvent.CreatePunctuation<int>(now.Ticks));

            // Add left events.
            for (int i = 0; i < count; ++i)
                input.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i));
            input.Add(StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime));

            // Expect left before right even though right went through first.
            var expectedOutput = new List<StreamEvent<int>> { StreamEvent.CreatePunctuation<int>(now.Ticks) };
            for (int i = 0; i < count; i++)
                expectedOutput.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i));
            for (int i = 0; i < count; i++)
                expectedOutput.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i + 1));
            expectedOutput.Add(StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime));

            var inputStreams = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Multicast(2);

            var left = inputStreams[0].Where(e => e % 2 == 0);
            var right = inputStreams[1].Where(e => e % 2 == 1);

            var result = left
                .Union(right)
                .ToStreamEventObservable()
                .ToEnumerable()
                .ToList();

            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest4RowFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateStart(6, 'b'),
                StreamEvent.CreateEnd(10, 6, 'b'),
                StreamEvent.CreateStart(11, 'c'),
                StreamEvent.CreateEnd(15, 11, 'c'),
                StreamEvent.CreateStart(16, 'd'),
                StreamEvent.CreateEnd(20, 16, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 5, 'a'),
                StreamEvent.CreateInterval(6, 10, 'b'),
                StreamEvent.CreateInterval(11, 15, 'c'),
                StreamEvent.CreateInterval(16, 20, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation);
            var result = str.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void AggregateTest1RowFloating()
        {
            long threeSecondTicks = TimeSpan.FromSeconds(3).Ticks;

            var stream = Events().ToEvents(e => e.Vs.Ticks, e => e.Vs.Ticks + threeSecondTicks)
                                 .ToObservable()
                                 .ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None());
            var expectedResult = new StreamEvent<ResultEvent>[]
            {
                StreamEvent.CreateStart(633979008010000000, new ResultEvent { Key = 1, Cnt = 1 }),
                StreamEvent.CreateEnd(633979008020000000, 633979008010000000, new ResultEvent { Key = 1, Cnt = 1 }),
                StreamEvent.CreateStart(633979008020000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateEnd(633979008030000000, 633979008020000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateStart(633979008030000000, new ResultEvent { Key = 1, Cnt = 3 }),
                StreamEvent.CreateEnd(633979008040000000, 633979008030000000, new ResultEvent { Key = 1, Cnt = 3 }),
                StreamEvent.CreateStart(633979008040000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateEnd(633979008050000000, 633979008040000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateStart(633979008050000000, new ResultEvent { Key = 1, Cnt = 1 }),
                StreamEvent.CreateEnd(633979008060000000, 633979008050000000, new ResultEvent { Key = 1, Cnt = 1 }),
            };
            var counts = stream.GroupApply(
                                    g => g.Key,
                                    e => e.Count(),
                                    (g, e) => new ResultEvent { Key = g.Key, Cnt = e })
                               .ToStreamEventObservable()
                               .ToEnumerable()
                               .Where(e => e.IsData && e.Payload.Key == 1);
            foreach (var x in counts)
            {
                Console.WriteLine("{0}, {1}", x.SyncTime, x.OtherTime);
            }
            Assert.IsTrue(expectedResult.SequenceEqual(counts));
        }

        public IEnumerable<Event> Events()
        {
            var vs = new DateTime(2010, 1, 1, 0, 00, 00, DateTimeKind.Utc);
            var rand = new Random(0);
            int eventCnt = 5;
            for (int i = 0; i < eventCnt; i++)
            {
                yield return new Event { Vs = vs, V = rand.Next(10), Key = rand.Next(3) };
                vs = vs.AddSeconds(1);
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void CodeGenWithFieldOfGenericTypeRowFloating()
        {
            // Taken from the PerformanceTest sample.
            // The GroupApply creates batches where the key is a generic type and codegen was doing a type cast without making sure the type's name was valid C# syntax
            var numEventsPerTumble = 2000;
            var tumblingWindowDataset =
                Observable.Range(0, 100)
                    .Select(e => StreamEvent.CreateInterval(((long)e / numEventsPerTumble) * numEventsPerTumble, ((long)e / numEventsPerTumble) * numEventsPerTumble + numEventsPerTumble, new Payload { field1 = 0, field2 = 0 }))
                    .ToStreamable()
                    .SetProperty().IsConstantDuration(true, numEventsPerTumble)
                    .SetProperty().IsSnapshotSorted(true, e => e.field1);
            var result = tumblingWindowDataset.GroupApply(e => e.field2, str => str.Count(), (g, c) => new { Key = g, Count = c });
            var a = result.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(a.Length == 3); // just make sure it doesn't crash
        }

        [TestMethod, TestCategory("Gated")]
        public void StringSerializationRowFloating()
        {
            var rand = new Random(0);
            var pool = MemoryManager.GetColumnPool<string>();

            for (int x = 0; x < 100; x++)
            {
                pool.Get(out ColumnBatch<string> inputStr);

                var toss1 = rand.NextDouble();
                inputStr.UsedLength = toss1 < 0.1 ? 0 : rand.Next(Config.DataBatchSize);

                for (int i = 0; i < inputStr.UsedLength; i++)
                {
                    var toss = rand.NextDouble();
                    inputStr.col[i] = toss < 0.2 ? string.Empty : (toss < 0.4 ? null : Guid.NewGuid().ToString());
                    if (x == 0) inputStr.col[i] = null;
                    if (x == 1) inputStr.col[i] = string.Empty;
                }

                StateSerializer<ColumnBatch<string>> s = StreamableSerializer.Create<ColumnBatch<string>>(new SerializerSettings { });
                var ms = new MemoryStream { Position = 0 };

                s.Serialize(ms, inputStr);
                ms.Position = 0;
                ColumnBatch<string> resultStr = s.Deserialize(ms);

                Assert.IsTrue(resultStr.UsedLength == inputStr.UsedLength);

                for (int j = 0; j < inputStr.UsedLength; j++)
                {
                    Assert.IsTrue(inputStr.col[j] == resultStr.col[j]);
                }
                resultStr.ReturnClear();
                inputStr.ReturnClear();
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void CharArraySerializationRowFloating()
        {
            using (var modifier = new ConfigModifier().SerializationCompressionLevel(SerializationCompressionLevel.CharArrayToUTF8).Modify())
            {
                var rand = new Random(0);

                for (int x = 0; x < 5; x++)
                {
                    var inputStr = new MultiString();

                    var toss1 = rand.NextDouble();
                    var usedLength = 1 + rand.Next(Config.DataBatchSize - 1);

                    for (int i = 0; i < usedLength; i++)
                    {
                        var toss = rand.NextDouble();
                        string str = toss < 0.2 ? string.Empty : Guid.NewGuid().ToString();
                        if (x == 0) str = string.Empty;
                        inputStr.AddString(str);
                    }

                    inputStr.Seal();

                    StateSerializer<MultiString> s = StreamableSerializer.Create<MultiString>(new SerializerSettings { });
                    var ms = new MemoryStream { Position = 0 };

                    s.Serialize(ms, inputStr);
                    ms.Position = 0;
                    var resultStr = s.Deserialize(ms);

                    Assert.IsTrue(resultStr.Count == inputStr.Count);

                    for (int j = 0; j < inputStr.Count; j++)
                    {
                        Assert.IsTrue(inputStr[j] == resultStr[j]);
                    }
                    resultStr.Dispose();
                    inputStr.Dispose();
                }
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void MultipleAggregatesWithCompiledDictionariesRowFloating()
        {
            IEnumerable<StreamEvent<StreamScope_InputEvent0>> bacon = Array.Empty<StreamEvent<StreamScope_InputEvent0>>();
            IObservableIngressStreamable<StreamScope_InputEvent0> input = bacon.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var result =
                input
                .GroupAggregate(
                    s => new { s.OutputTs, s.DelayInMilliseconds, s.UserId },
                    s => s.Count(),
                    s => s.Min(r => r.Duration),
                    s => s.Max(r => r.Duration),
                    s => s.Average(r => r.Duration),
                    (key, cnt, minDuration, maxDuration, avgDuration) => new StreamScope_OutputEvent0()
                    {
                        OutputTs = key.Key.OutputTs,
                        DelayInMilliseconds = key.Key.DelayInMilliseconds,
                        UserId = key.Key.UserId,
                        Cnt = (long)cnt,
                        MinDuration = minDuration,
                        MaxDuration = maxDuration,
                        AvgDuration = avgDuration,
                    }).ToStreamEventObservable();

            var b = result;
        }

        [TestMethod, TestCategory("Gated")]
        public void PayloadWithFieldOfNestedType01RowFloating()
        {
            var input = Enumerable
                .Range(0, NumEvents)
                .Select(i => new PayloadWithFieldOfNestedType1(i))
                ;
            var streamResult = input
                .ToStreamable()
                .Where(p => p.nt.x % 2 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var linqResult = input.Where(p => p.nt.x % 2 == 0);
            Assert.IsTrue(Enumerable.Zip(linqResult, a, (p1, p2) => p1.nt.x == p2.nt.x).All(p => p));
        }
        [TestMethod, TestCategory("Gated")]
        public void PayloadWithFieldOfNestedType02RowFloating()
        {
            var input = Enumerable
                .Range(0, NumEvents)
                .Select(i => new PayloadWithFieldOfNestedType2(i))
                ;
            var streamResult = input
                .ToStreamable()
                .Where(p => p.nt.x % 2 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var linqResult = input.Where(p => p.nt.x % 2 == 0);
            Assert.IsTrue(Enumerable.Zip(linqResult, a, (p1, p2) => p1.nt.x == p2.nt.x).All(p => p));
        }
    }

    [TestClass]
    public class SimpleTestsRowSmallBatch : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public SimpleTestsRowSmallBatch() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        // Total number of input events
        private const int NumEvents = 1000;
        private readonly IEnumerable<MyStruct2> enumerable = Enumerable.Range(0, NumEvents)
            .Select(e =>
                new MyStruct2 { field1 = e, field2 = new MyString(Convert.ToString("string" + e)), field3 = new NestedStruct { nestedField = e } });

        private static IComparerExpression<T> GetDefaultComparerExpression<T>(T t) => ComparerExpression<T>.Default;

        private static IEqualityComparerExpression<T> GetDefaultEqualityComparerExpression<T>(T t) => EqualityComparerExpression<T>.Default;

        public static bool MyIsEqual(List<RankedEvent<char>> a, List<RankedEvent<char>> b)
        {
            if (a.Count != b.Count) return false;

            for (int j = 0; j < a.Count; j++)
            {
                if (!a[j].Equals(b[j])) return false;
            }

            return true;
        }

        public static int MyGetHashCode(List<RankedEvent<char>> a)
        {
            int ret = 0;
            foreach (var l in a)
            {
                ret ^= l.GetHashCode();
            }
            return ret;
        }

        private void TestWhere(Expression<Func<MyStruct2, bool>> predicate, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhere(predicate, disableFusion));

        private void TestSelect<TResult>(Expression<Func<MyStruct2, TResult>> function, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestSelect(function, disableFusion));

        public void TestWhereSelect<U>(Expression<Func<MyStruct2, bool>> wherePredicate, Expression<Func<MyStruct2, U>> selectFunction, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhereSelect(wherePredicate, selectFunction, disableFusion));

        [TestMethod, TestCategory("Gated")]
        public void Where1RowSmallBatch()
            => TestWhere(e => e.field2.mystring.Contains("string"));

        [TestMethod, TestCategory("Gated")]
        public void Where2RowSmallBatch()
            => TestWhere(e => e.field2.mystring.Equals("string0"));

        [TestMethod, TestCategory("Gated")]
        public void Where3RowSmallBatch()
            => TestWhere(e => e.field2.mystring.Equals("string0"));

        [TestMethod, TestCategory("Gated")]
        public void Where4RowSmallBatch()
            => TestWhere(e => e.field3.nestedField == 0);

        [TestMethod, TestCategory("Gated")]
        public void Where5RowSmallBatch()
            => TestWhere(e => true);

        [TestMethod, TestCategory("Gated")]
        public void Where6RowSmallBatch()
            => TestWhere(e => false);

        [TestMethod, TestCategory("Gated")]
        public void Where7RowSmallBatch()
            => TestWhere(e => e.field3.nestedField == 0 || e.field3.nestedField == 1);

        [TestMethod, TestCategory("Gated")]
        public void Where8RowSmallBatch()
            => TestWhere(e => string.Compare(e.field2.mystring, "string0") == 0);

        [TestMethod, TestCategory("Gated")]
        public void Where9RowSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Where(v => v % 2 == 0)
                .Where(v => v % 3 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            Assert.IsTrue(input.Where(v => v % 6 == 0).SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereWithFailedTransformationRowSmallBatch() // stays columnar, but causes payload to be reconstituted
            => Enumerable.Range(0, 100)
                .Select(i => new StructTuple<int, char> { Item1 = i, Item2 = i.ToString()[0], })
                .TestWhere(r => r.ToString().Length > 3);

        [TestMethod, TestCategory("Gated")]
        public void Select1RowSmallBatch()
            => TestSelect(e => e.field1);

        [TestMethod, TestCategory("Gated")]
        public void Select2RowSmallBatch()
            => TestSelect(e => e.field2);

        [TestMethod, TestCategory("Gated")]
        public void Select3RowSmallBatch()
            => TestSelect(e => e.field3);

        [TestMethod, TestCategory("Gated")]
        public void Select4RowSmallBatch()
            => TestSelect(e => new NestedStruct { nestedField = e.field1 });

        [TestMethod, TestCategory("Gated")]
        public void Select5RowSmallBatch()
            => TestSelect(e => e.doubleField);

        [TestMethod, TestCategory("Gated")]
        public void Select6RowSmallBatch()
            => TestSelect(e => ((ulong)e.field1));

        // Tests to make sure that we ref count correctly by doing two pointer swings to the same column.
        [TestMethod, TestCategory("Gated")]
        public void Select7RowSmallBatch()
            => TestSelect(e => new { A = e.field1, B = e.field1, });

        // Tests whether an expression that cannot be columnerized causes code gen to fall back to row-oriented.
        // This is just a degenerate case where the expression is *always* null.
        [TestMethod, TestCategory("Gated")]
        public void Select8RowSmallBatch()
            => TestSelect<ClassWithOnlyAutoProps>(e => null);

        [TestMethod, TestCategory("Gated")]
        public void SelectFromUlongToLongRowSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(v => (long)v)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, longInteger) => (long)integer == longInteger).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeRowSmallBatch()
            => TestSelect(e => new { anonfield1 = e.field1, x = 3 });

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedRowSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(v => new { X = v })
                .Select(e => new { Y = e.X })
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.Y).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedNoOpRowSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(v => new { X = v })
                .Select(e => e)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.X).All(p => p));
        }

        // The point of this test is to have an anonymous type with a "field" in it
        // whose type does not have an overload for it in MemoryPool<TKey, TPayload>.Get(out ColumnBatch<>).
        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeWithFloatFieldRowSmallBatch()
            => TestSelect(e => new { floatField = e.field1 * 3.0, });

        [TestMethod, TestCategory("Gated")]
        public void SelectWhereAnonymousTypeRowSmallBatch()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithAutoPropsRowSmallBatch()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithAutoPropsRowSmallBatch()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithOnlyAutoPropsRowSmallBatch()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithOnlyAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithOnlyAutoPropsRowSmallBatch()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_SelectWhereRowSmallBatch()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_GroupApplyRowSmallBatch()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .GroupApply(e => e.field1, str => str.Count(), (g, cnt) => new MyStruct { field1 = (int)cnt, })
                .Where(e => e.field2 >= 0.0)
                .ToPayloadEnumerable();
            Assert.IsTrue(streamResult.Count() == this.enumerable.Count());
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectWithCtorCallRowSmallBatch()
            => TestSelect(e => new StructWithCtor(e.field1));

        [TestMethod, TestCategory("Gated")]
        public void SelectMany01RowSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany02RowSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany03RowSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany04RowSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart01RowSmallBatch()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select((l, e) => e + ((int)l))
                ;
            var expected = input
                .Select(e => e + e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart02RowSmallBatch()
        {
            // tests simple parameter selection with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select((l, e) => l)
                ;
            var expected = input
                .Select(e => (long)e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey01RowSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectByKey((k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey02RowSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectByKey((k, e) => k)
                ;
            var expected = input
                .Select(e => Empty.Default)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey03RowSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectByKey((s, k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e + (int)s, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e + e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey04RowSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => (int?)(e + 1))
                .SelectByKey((k, e) => new object[] { e, })
                ;
            var expected = input
                .Select(e => new object[] { (int?)(e + 1), })
                .ToArray()
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.Length == output.Length);
            for (int i = 0; i < expected.Length; i++)
            {
                var e = (int?)(expected[i][0]);
                var o = (int?)(output[i][0]);
                Assert.IsTrue(e.HasValue && o.HasValue && (e.Value == o.Value));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStart01RowSmallBatch()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany((l, e) => Enumerable.Repeat(33, e + ((int)l)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + e))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyKey01RowSmallBatch()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectManyByKey((k, e) => Enumerable.Repeat(33, e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStartKey01RowSmallBatch()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectManyByKey((ts, k, e) => Enumerable.Repeat(33 + ((int)ts), e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33 + e, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereSelect1RowSmallBatch()
            => TestWhereSelect(e => true, e => e.field1);

        [TestMethod, TestCategory("Gated")]
        public void SelectStructRowSmallBatch()
        {
            using (var modifier = new ConfigModifier().ForceRowBasedExecution(true).Modify())
            {
                var savedForceRowBasedExecution = Config.ForceRowBasedExecution;
                Config.ForceRowBasedExecution = true;

                var input = Enumerable.Range(0, 100);
                Expression<Func<int, MyStruct>> lambda = e => new MyStruct { field1 = e, field2 = e + 0.5, field3 = default };

                var streamResult = input
                    .ToStreamable()
                    .ShiftEventLifetime(0)
                    .Select(lambda)
                    .ToPayloadEnumerable();
                var linqResult = input
                    .Select(lambda.Compile());

                var query = input.Where(e => true);
                Assert.IsTrue(linqResult.SequenceEqual(streamResult));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantNoOpIntervalsRowSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 5, 'a'),
                StreamEvent.CreateInterval(2, 6, 'b'),
                StreamEvent.CreateInterval(3, 7, 'c'),
                StreamEvent.CreateInterval(4, 8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 5, 'a'),
                StreamEvent.CreateInterval(2, 6, 'b'),
                StreamEvent.CreateInterval(3, 7, 'c'),
                StreamEvent.CreateInterval(4, 8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(10)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantNoOpEdgesRowSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateEnd(6, 2, 'b'),
                StreamEvent.CreateEnd(7, 3, 'c'),
                StreamEvent.CreateEnd(8, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateEnd(6, 2, 'b'),
                StreamEvent.CreateEnd(7, 3, 'c'),
                StreamEvent.CreateEnd(8, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(10)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantClippedIntervalsRowSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 15, 'a'),
                StreamEvent.CreateInterval(2, 16, 'b'),
                StreamEvent.CreateInterval(3, 17, 'c'),
                StreamEvent.CreateInterval(4, 18, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 11, 'a'),
                StreamEvent.CreateInterval(2, 12, 'b'),
                StreamEvent.CreateInterval(3, 13, 'c'),
                StreamEvent.CreateInterval(4, 14, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(10)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantClippedEdgesRowSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateEnd(6, 2, 'b'),
                StreamEvent.CreateEnd(7, 3, 'c'),
                StreamEvent.CreateEnd(8, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateEnd(3, 1, 'a'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateEnd(4, 2, 'b'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 3, 'c'),
                StreamEvent.CreateEnd(6, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(2)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderPolicy1RowSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateEnd(20, 5, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(9, 'x'),
                StreamEvent.CreateEnd(20, 9, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderPolicy2RowSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreateEnd(24, 5, 'x'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(9, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreateEnd(24, 9, 'x'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderPolicy3RowSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreateEnd(20, 5, 'x'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(9, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreateEnd(22, 9, 'x'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var observInput = input.ToObservable();
            var str = observInput.ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderPolicy4RowSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateStart(11, 'z'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateEnd(20, 5, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreateEnd(24, 5, 'x'),
                StreamEvent.CreateEnd(26, 5, 'x'),
                StreamEvent.CreateEnd(28, 11, 'z'),
                StreamEvent.CreateEnd(30, 5, 'x'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(9, 'x'),
                StreamEvent.CreateStart(9, 'x'),
                StreamEvent.CreateStart(11, 'z'),
                StreamEvent.CreateStart(11, 'x'),
                StreamEvent.CreateStart(11, 'x'),
                StreamEvent.CreateEnd(20, 9, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreateEnd(24, 9, 'x'),
                StreamEvent.CreateEnd(26, 11, 'x'),
                StreamEvent.CreateEnd(28, 11, 'z'),
                StreamEvent.CreateEnd(30, 11, 'x'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void PunctuationPolicy2RowSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(4, 'c'),
                StreamEvent.CreateStart(7, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(34, 4, 'c'),
                StreamEvent.CreateEnd(35, 7, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };

            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreatePunctuation<char>(2),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreatePunctuation<char>(4),
                StreamEvent.CreateStart(4, 'c'),
                StreamEvent.CreatePunctuation<char>(6),
                StreamEvent.CreateStart(7, 'd'),
                StreamEvent.CreatePunctuation<char>(20),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreatePunctuation<char>(22),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreatePunctuation<char>(34),
                StreamEvent.CreateEnd(34, 4, 'c'),
                StreamEvent.CreateEnd(35, 7, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(2), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void AlterDurationTest1RowSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(3, 'x'),
                StreamEvent.CreateEnd(5, 3, 'x'),
                StreamEvent.CreateStart(7, 'y'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(3, 8, 'x'),
                StreamEvent.CreateInterval(7, 12, 'y'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var result = str.AlterEventDuration(5).ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void PunctuationPolicy3RowSmallBatch()
        { // Simulates the way Stat does ingress (but with 80K batches, not 3...)
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void StreamCache1RowSmallBatch()
        {
            var input = Enumerable.Range(0, 1000000); // make sure it is enough to have more than one data batch
            ulong limit = 10000;
            var cachedStream = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e))
                .ToStreamable(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .Cache(limit);
            var streamResult = cachedStream
                .ToPayloadEnumerable();

            var c = (ulong)streamResult.Count();
            Assert.IsTrue(c == limit);
            cachedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void StreamCache2RowSmallBatch()
        {
            var limit = 10; // 00000;
            var input = Enumerable.Range(0, limit); // make sure it is enough to have more than one data batch
            var cachedStream = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(StreamEvent.MinSyncTime, e))
                .ToStreamable(DisorderPolicy.Throw())
                .Cache();
            var streamResult = cachedStream
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.Payload)
                .ToEnumerable();
            var foo = streamResult.Count();

            Assert.IsTrue(input.SequenceEqual(streamResult));
            cachedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals1RowSmallBatch()
        {
            var x = ComparerExpression<int>.Default.GetCompareExpr()
                .ExpressionEquals(ComparerExpression<int>.Default.GetCompareExpr());
            Assert.IsTrue(x);
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals2RowSmallBatch()
        {
            var x = new CompoundGroupKeyEqualityComparer<Empty, MyStruct2>(EqualityComparerExpression<Empty>.Default, EqualityComparerExpression<MyStruct2>.Default);
            var y = x.GetEqualsExpr().ExpressionEquals(x.GetEqualsExpr());
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals3RowSmallBatch()
        {
            var x1 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var x2 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var y = x1.GetCompareExpr().ExpressionEquals(x2.GetCompareExpr());
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals4RowSmallBatch()
        {
            var x1 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var x2 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);

            var xx1 = new CompoundGroupKeyComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x1, ComparerExpression<MyStruct2>.Default);
            var xx2 = new CompoundGroupKeyComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x2, ComparerExpression<MyStruct2>.Default);

            var y = xx1.GetCompareExpr().ExpressionEquals(xx2.GetCompareExpr());
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void CompoundGroupKeyEqualityComparerDefaultRowSmallBatch()
        {
            var xx1 = EqualityComparerExpression<CompoundGroupKey<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>>.Default;

            var x2 = new CompoundGroupKeyEqualityComparer<Empty, MyStruct2>(EqualityComparerExpression<Empty>.Default, EqualityComparerExpression<MyStruct2>.Default);
            var xx2 = new CompoundGroupKeyEqualityComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x2, EqualityComparerExpression<MyStruct2>.Default);

            var y = xx1.ExpressionEquals(xx2);
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void CompoundGroupKeyComparerDefaultRowSmallBatch()
        {
            var xx1 = ComparerExpression<CompoundGroupKey<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>>.Default;

            var x2 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var xx2 = new CompoundGroupKeyComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x2, ComparerExpression<MyStruct2>.Default);

            var y = xx1.ExpressionEquals(xx2);
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForIComparableRowSmallBatch()
        {
            var defaultComparerExpressionProvider = ComparerExpression<ClassImplementingIComparable>.Default;
            var defaultCompareExpression = defaultComparerExpressionProvider.GetCompareExpr();
            var compiledDefaultCompareExpression = defaultCompareExpression.Compile();
            var o = new ClassImplementingIComparable();
            var result = compiledDefaultCompareExpression(o, o);
            Assert.IsTrue(result == 3);

        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForGenericIComparerRowSmallBatch()
        {
            var defaultComparerExpressionProvider = ComparerExpression<ClassImplementingGenericIComparer>.Default;
            var defaultCompareExpression = defaultComparerExpressionProvider.GetCompareExpr();
            var compiledDefaultCompareExpression = defaultCompareExpression.Compile();
            var o = new ClassImplementingGenericIComparer();
            var result = compiledDefaultCompareExpression(o, o);
            Assert.IsTrue(result == 3);

        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForNonGenericIComparerRowSmallBatch()
        {
            var defaultComparerExpressionProvider = ComparerExpression<ClassImplementingNonGenericIComparer>.Default;
            var defaultCompareExpression = defaultComparerExpressionProvider.GetCompareExpr();
            var compiledDefaultCompareExpression = defaultCompareExpression.Compile();
            var o = new ClassImplementingNonGenericIComparer();
            var result = compiledDefaultCompareExpression(o, o);
            Assert.IsTrue(result == 3);

        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForAnonymousType1RowSmallBatch()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var f = GetDefaultComparerExpression(a);
            var g = f.GetCompareExpr();
            var h = g.Compile();
            var k = h(a, a);
            Assert.IsTrue(k == 0);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType1RowSmallBatch()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, a);
            Assert.IsTrue(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType2RowSmallBatch()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var b = new { Z = i + 1, A = (char)('a' + i), W = "abc" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, b);
            Assert.IsFalse(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType3RowSmallBatch()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var b = new { Z = i, A = (char)('b' + i), W = "abc" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, b);
            Assert.IsFalse(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType4RowSmallBatch()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var b = new { Z = i, A = (char)('a' + i), W = "abcd" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, b);
            Assert.IsFalse(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void NaryMulticast1RowSmallBatch()
        {
            var enumerable = Enumerable.Range(0, 10000).ToList();

            var stream = enumerable.ToStreamable();
            var multiStream = stream.Multicast(3);

            var output1 = multiStream[0].Where(e => e % 3 == 0);
            var output2 = multiStream[1].Where(e => e % 3 == 1);
            var output3 = multiStream[2].Where(e => e % 3 == 2);
            var union = output1.Union(output2).Union(output3);

            var output = union.ToPayloadEnumerable().ToList();
            output.Sort();
            Assert.IsTrue(enumerable.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void NaryMulticastContainer1RowSmallBatch()
        {
            var outputList = new List<int>();

            // Inputs
            var container = new QueryContainer();
            Stream state = null;

            // Input data
            var enumerable = Enumerable.Range(0, 10000).ToList();
            var inputObservable = enumerable.ToObservable().Select(e => StreamEvent.CreateStart(0, e));

            // Query start
            var stream = container.RegisterInput(inputObservable);
            var multiStream = stream.Multicast(3);

            var output1 = multiStream[0].Where(e => e % 3 == 0);
            var output2 = multiStream[1].Where(e => e % 3 == 1);
            var output3 = multiStream[2].Where(e => e % 3 == 2);
            var union = output1.Union(output2).Union(output3);

            var outputObservable = container.RegisterOutput(union);

            // Test the output
            var outputAsync = outputObservable.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
            container.Restore(state);
            outputAsync.Wait();

            outputList.Sort();
            Assert.IsTrue(enumerable.SequenceEqual(outputList));
        }

        [TestMethod, TestCategory("Gated")]
        public void NaryMulticastContainerErrorRowSmallBatch()
        {
            // Inputs
            var container = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var enumerable = Enumerable.Range(0, 10000).ToList();
            var inputObservable = enumerable.ToObservable().Select(e => StreamEvent.CreateStart(0, e));

            // Query start
            var stream = container.RegisterInput(inputObservable);
            var multiStream = stream.Multicast(3);

            var output1 = multiStream[0].Where(e => e % 3 == 0);
            var output2 = multiStream[1].Where(e => e % 3 == 1);
            var output3 = multiStream[2].Where(e => e % 3 == 2);
            var union = output1.Union(output2).Union(output3);

            var outputObservable = container.RegisterOutput(union);

            // Validate that attempting to start the query before subscriptions will result in an error.
            bool foundException = false;
            try
            {
                container.Restore(state);
            }
            catch (Exception)
            {
                foundException = true;
            }

            Assert.IsTrue(foundException, "Expected an exception.");
        }

        [TestMethod, TestCategory("Gated")]
        public void StreamSortRowSmallBatch()
        {
            var input = Enumerable.Range(0, 20).Select(i => (char)('a' + i));
            var reversedInput = input.Reverse();
            var str = reversedInput.ToStatStreamable();
            var sortedStream = str.Sort(c => c);
            var result = sortedStream.ToStreamEventObservable().Where(se => se.IsData).Select(se => se.Payload).ToEnumerable();
            Assert.IsTrue(input.SequenceEqual(result));
            sortedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void SortModifiedStreamRowSmallBatch()
        {
            // The Sort method on a stream uses an observable cache. The cache
            // had been putting all rows from the messages into the list to be sorted,
            // even if it was an invalid row.
            var input = Enumerable.Range(0, 20).Select(i => (char)('a' + i));
            var str = input.ToStatStreamable();
            var str2 = str.Where(r => r != 'b');
            var sortedStream = str2.Sort(c => c);
            var result = sortedStream.ToStreamEventObservable().Where(se => se.IsData).Select(se => se.Payload).ToEnumerable();
            Assert.IsTrue(input.Count() == result.Count() + 1); // just make sure deleted element isn't counted anymore
            sortedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void GroupStreamableCodeGenTest1RowSmallBatch()
        {
            var input1 = new[]
            {
                StreamEvent.CreateInterval(100, 101, 13),
                StreamEvent.CreateInterval(100, 110, 11),
                StreamEvent.CreateInterval(101, 105, 12),
                StreamEvent.CreateInterval(102, 112, 21),
                StreamEvent.CreateInterval(109, 112, 14),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };

            var input2 = new[]
            {
                StreamEvent.CreateInterval(100, 110, 'd'),
                StreamEvent.CreateInterval(101, 103, 'b'),
                StreamEvent.CreateInterval(106, 108, 'b'),
                StreamEvent.CreateInterval(106, 109, 'b'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime)
            };

            var inputStream1 = input1.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var inputStream2 = input2.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var outputStream = inputStream1.WhereNotExists(
                inputStream2,
                l => l / 10,
                r => r - 'a');

            var expected = new[]
            {
                StreamEvent.CreateStart(100, 11),
                StreamEvent.CreateEnd(101, 100, 11),
                StreamEvent.CreateStart(103, 11),
                StreamEvent.CreateEnd(106, 103, 11),
                StreamEvent.CreateInterval(109, 110, 11),
                StreamEvent.CreateInterval(103, 105, 12),
                StreamEvent.CreateInterval(100, 101, 13),
                StreamEvent.CreateInterval(109, 112, 14),
                StreamEvent.CreateStart(102, 21),
                StreamEvent.CreateEnd(112, 102, 21),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };
            var outputEnumerable = outputStream.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void CountColumnar1RowSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<ulong>[]
            {
                StreamEvent.CreateStart<ulong>(StreamEvent.MinSyncTime, 3),
                StreamEvent.CreatePunctuation<ulong>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateEnd<ulong>(StreamEvent.MinSyncTime + 1, StreamEvent.MinSyncTime, 3),
                StreamEvent.CreateStart<ulong>(StreamEvent.MinSyncTime + 1, 6),
                StreamEvent.CreatePunctuation<ulong>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateEnd<ulong>(StreamEvent.MinSyncTime + 2, StreamEvent.MinSyncTime + 1, 6),
                StreamEvent.CreateStart<ulong>(StreamEvent.MinSyncTime + 2, 9),
                StreamEvent.CreatePunctuation<ulong>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None);
            var count = str.Count();
            var outputEnumerable = count.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageColumnar1RowSmallBatch() // tests codegen for Snapshot_noecq
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),      // 97
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),      // 98
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),      // 99  running average = 98

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),  // 100
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),  // 101
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),  // 102 running average = 99.5

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),  // 103
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),  // 104
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),  // 105 running average = 101
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<double>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 1, StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 2, StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 101.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None).SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime);
            var avg = str.Average(c => (double)c);
            var outputEnumerable = avg.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageColumnar2RowSmallBatch() // tests codegen for Snapshot_q
        {
            var zero = StreamEvent.MinSyncTime;
            var one = StreamEvent.MinSyncTime + 1;
            var two = StreamEvent.MinSyncTime + 2;
            var three = StreamEvent.MinSyncTime + 3;
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(zero, one, 'a'),
                StreamEvent.CreateInterval(zero, one, 'b'),
                StreamEvent.CreateInterval(zero, one, 'c'),
                StreamEvent.CreateInterval(one, two, 'd'),
                StreamEvent.CreateInterval(one, two, 'e'),
                StreamEvent.CreateInterval(one, two, 'f'),
                StreamEvent.CreateInterval(two, three, 'g'),
                StreamEvent.CreateInterval(two, three, 'h'),
                StreamEvent.CreateInterval(two, three, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<double>[]
            {
                StreamEvent.CreateInterval(zero, one, 98.0),
                StreamEvent.CreatePunctuation<double>(one),
                StreamEvent.CreateInterval(one, two, 101.0),
                StreamEvent.CreatePunctuation<double>(two),
                StreamEvent.CreateInterval(two, three, 104.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None).SetProperty().IsConstantDuration(true, 1);
            var avg = str.Average(c => (double)c);
            var outputEnumerable = avg.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageColumnar3RowSmallBatch() // tests codegen for Snapshot_pq
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),      // 97
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),      // 98
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),      // 99  running average = 98

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),  // 100
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),  // 101
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),  // 102 running average = 99.5

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),  // 103
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),  // 104
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),  // 105 running average = 101
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<double>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 1, StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 2, StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 101.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None);
            var avg = str.Average(c => (double)c);
            var outputEnumerable = avg.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SumColumnar1RowSmallBatch() // make sure expression is properly parenthesized in codegen
        {
            var input = Enumerable.Range(0, 100);
            Expression<Func<int, int>> query = e => e & 0x7;

            var streamResult = input
                .ToStreamable()
                .Sum(query)
                .ToPayloadEnumerable()
                .Last();

            var expected = input.Sum(query.Compile());
            Assert.IsTrue(expected == streamResult);

        }

        [TestMethod, TestCategory("Gated")]
        public void MultiAggregate1RowSmallBatch() // test codegen for multi-aggregates
        {
            var input = Enumerable.Range(0, 100).Select(i => new MyStruct { field1 = i, field2 = 0.0, field3 = Guid.Empty, });

            var streamResult = input
                .ToStreamable()
                .Aggregate(a => a.Sum(x => x.field1), a => a.Count(), (s, c) => new MyStruct { field1 = s + ((int)c) })
                .ToPayloadEnumerable()
                ;

            Assert.IsTrue(streamResult.Count() == 1);

            Assert.IsTrue(streamResult.First().field1 == 5050);

        }

        [TestMethod, TestCategory("Gated")]
        public void MultiAggregate2RowSmallBatch() // test codegen for multi-aggregates with anonymous type
        {
            var input = Enumerable.Range(0, 100).Select(i => new MyStruct { field1 = i, field2 = 0.0, field3 = Guid.Empty, });

            var streamResult = input
                .ToStreamable()
                .Aggregate(a => a.Sum(x => x.field1), a => a.Count(), (s, c) => new { field1 = s, field2 = c })
                .ToPayloadEnumerable()
                ;

            Assert.IsTrue(streamResult.Count() == 1);

            var f = streamResult.First();
            Assert.IsTrue(f.field1 == 4950 && f.field2 == 100);

        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest1RowSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(24, 3, 'c'),
                StreamEvent.CreateEnd(26, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateInterval(2, 22, 'b'),
                StreamEvent.CreateInterval(3, 24, 'c'),
                StreamEvent.CreateInterval(4, 26, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Cache(0, false, true);
            var result = str.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest2RowSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(24, 3, 'c'),
                StreamEvent.CreateEnd(26, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateInterval(3, 24, 'c'),
                StreamEvent.CreateInterval(4, 26, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Cache(0, false, true);
            var result = str.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest3RowSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(24, 4, 'd'),
                StreamEvent.CreateEnd(26, 3, 'c'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateInterval(2, 22, 'b'),
                StreamEvent.CreateInterval(3, 26, 'c'),
                StreamEvent.CreateInterval(4, 24, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Cache(0, false, true);
            var result = str.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void BinaryUnionTest1RowSmallBatch()
        {
            var input1 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(2, 20, 'a'),
                StreamEvent.CreateStart(4, 'b'),
                StreamEvent.CreateStart(6, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var input2 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(3, 20, 'a'),
                StreamEvent.CreateStart(5, 'b'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(9, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };

            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(2, 20, 'a'),
                StreamEvent.CreateInterval(3, 20, 'a'),
                StreamEvent.CreateStart(4, 'b'),
                StreamEvent.CreateStart(5, 'b'),
                StreamEvent.CreateStart(6, 'c'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreateStart(9, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str1 = input1.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null).Cache();
            var str2 = input2.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null).Cache();
            var resultEnum = str1.Union(str2).ToStreamEventObservable().ToEnumerable();
            var result = resultEnum.ToList();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str1.Dispose();
            str2.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void BinaryUnionTest2RowSmallBatch()
        {
            var input1 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var input2 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(5, 20, 'a'),
                StreamEvent.CreateStart(6, 'b'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };

            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateInterval(5, 20, 'a'),
                StreamEvent.CreateStart(6, 'b'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str1 = input1.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation).Cache();
            var str2 = input2.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation).Cache();
            var result = str1.Union(str2).ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str1.Dispose();
            str2.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void DeterministicUnionTestRowSmallBatch()
        {
            int count = 100;
            DateTime now = DateTime.UtcNow;

            var input = new List<StreamEvent<int>> { StreamEvent.CreatePunctuation<int>(now.Ticks) };

            // Add right events.
            for (int i = 0; i < count; ++i)
                input.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i + 1));

            // Cti forces right batch to go through before reaching Config.DataBatchSize.
            input.Add(StreamEvent.CreatePunctuation<int>(now.Ticks));

            // Add left events.
            for (int i = 0; i < count; ++i)
                input.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i));
            input.Add(StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime));

            // Expect left before right even though right went through first.
            var expectedOutput = new List<StreamEvent<int>> { StreamEvent.CreatePunctuation<int>(now.Ticks) };
            for (int i = 0; i < count; i++)
                expectedOutput.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i));
            for (int i = 0; i < count; i++)
                expectedOutput.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i + 1));
            expectedOutput.Add(StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime));

            var inputStreams = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Multicast(2);

            var left = inputStreams[0].Where(e => e % 2 == 0);
            var right = inputStreams[1].Where(e => e % 2 == 1);

            var result = left
                .Union(right)
                .ToStreamEventObservable()
                .ToEnumerable()
                .ToList();

            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest4RowSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateStart(6, 'b'),
                StreamEvent.CreateEnd(10, 6, 'b'),
                StreamEvent.CreateStart(11, 'c'),
                StreamEvent.CreateEnd(15, 11, 'c'),
                StreamEvent.CreateStart(16, 'd'),
                StreamEvent.CreateEnd(20, 16, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 5, 'a'),
                StreamEvent.CreateInterval(6, 10, 'b'),
                StreamEvent.CreateInterval(11, 15, 'c'),
                StreamEvent.CreateInterval(16, 20, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation);
            var result = str.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void AggregateTest1RowSmallBatch()
        {
            long threeSecondTicks = TimeSpan.FromSeconds(3).Ticks;

            var stream = Events().ToEvents(e => e.Vs.Ticks, e => e.Vs.Ticks + threeSecondTicks)
                                 .ToObservable()
                                 .ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None());
            var expectedResult = new StreamEvent<ResultEvent>[]
            {
                StreamEvent.CreateStart(633979008010000000, new ResultEvent { Key = 1, Cnt = 1 }),
                StreamEvent.CreateEnd(633979008020000000, 633979008010000000, new ResultEvent { Key = 1, Cnt = 1 }),
                StreamEvent.CreateStart(633979008020000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateEnd(633979008030000000, 633979008020000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateStart(633979008030000000, new ResultEvent { Key = 1, Cnt = 3 }),
                StreamEvent.CreateEnd(633979008040000000, 633979008030000000, new ResultEvent { Key = 1, Cnt = 3 }),
                StreamEvent.CreateStart(633979008040000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateEnd(633979008050000000, 633979008040000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateStart(633979008050000000, new ResultEvent { Key = 1, Cnt = 1 }),
                StreamEvent.CreateEnd(633979008060000000, 633979008050000000, new ResultEvent { Key = 1, Cnt = 1 }),
            };
            var counts = stream.GroupApply(
                                    g => g.Key,
                                    e => e.Count(),
                                    (g, e) => new ResultEvent { Key = g.Key, Cnt = e })
                               .ToStreamEventObservable()
                               .ToEnumerable()
                               .Where(e => e.IsData && e.Payload.Key == 1);
            foreach (var x in counts)
            {
                Console.WriteLine("{0}, {1}", x.SyncTime, x.OtherTime);
            }
            Assert.IsTrue(expectedResult.SequenceEqual(counts));
        }

        public IEnumerable<Event> Events()
        {
            var vs = new DateTime(2010, 1, 1, 0, 00, 00, DateTimeKind.Utc);
            var rand = new Random(0);
            int eventCnt = 5;
            for (int i = 0; i < eventCnt; i++)
            {
                yield return new Event { Vs = vs, V = rand.Next(10), Key = rand.Next(3) };
                vs = vs.AddSeconds(1);
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void CodeGenWithFieldOfGenericTypeRowSmallBatch()
        {
            // Taken from the PerformanceTest sample.
            // The GroupApply creates batches where the key is a generic type and codegen was doing a type cast without making sure the type's name was valid C# syntax
            var numEventsPerTumble = 2000;
            var tumblingWindowDataset =
                Observable.Range(0, 100)
                    .Select(e => StreamEvent.CreateInterval(((long)e / numEventsPerTumble) * numEventsPerTumble, ((long)e / numEventsPerTumble) * numEventsPerTumble + numEventsPerTumble, new Payload { field1 = 0, field2 = 0 }))
                    .ToStreamable()
                    .SetProperty().IsConstantDuration(true, numEventsPerTumble)
                    .SetProperty().IsSnapshotSorted(true, e => e.field1);
            var result = tumblingWindowDataset.GroupApply(e => e.field2, str => str.Count(), (g, c) => new { Key = g, Count = c });
            var a = result.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(a.Length == 3); // just make sure it doesn't crash
        }

        [TestMethod, TestCategory("Gated")]
        public void StringSerializationRowSmallBatch()
        {
            var rand = new Random(0);
            var pool = MemoryManager.GetColumnPool<string>();

            for (int x = 0; x < 100; x++)
            {
                pool.Get(out ColumnBatch<string> inputStr);

                var toss1 = rand.NextDouble();
                inputStr.UsedLength = toss1 < 0.1 ? 0 : rand.Next(Config.DataBatchSize);

                for (int i = 0; i < inputStr.UsedLength; i++)
                {
                    var toss = rand.NextDouble();
                    inputStr.col[i] = toss < 0.2 ? string.Empty : (toss < 0.4 ? null : Guid.NewGuid().ToString());
                    if (x == 0) inputStr.col[i] = null;
                    if (x == 1) inputStr.col[i] = string.Empty;
                }

                StateSerializer<ColumnBatch<string>> s = StreamableSerializer.Create<ColumnBatch<string>>(new SerializerSettings { });
                var ms = new MemoryStream { Position = 0 };

                s.Serialize(ms, inputStr);
                ms.Position = 0;
                ColumnBatch<string> resultStr = s.Deserialize(ms);

                Assert.IsTrue(resultStr.UsedLength == inputStr.UsedLength);

                for (int j = 0; j < inputStr.UsedLength; j++)
                {
                    Assert.IsTrue(inputStr.col[j] == resultStr.col[j]);
                }
                resultStr.ReturnClear();
                inputStr.ReturnClear();
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void CharArraySerializationRowSmallBatch()
        {
            using (var modifier = new ConfigModifier().SerializationCompressionLevel(SerializationCompressionLevel.CharArrayToUTF8).Modify())
            {
                var rand = new Random(0);

                for (int x = 0; x < 5; x++)
                {
                    var inputStr = new MultiString();

                    var toss1 = rand.NextDouble();
                    var usedLength = 1 + rand.Next(Config.DataBatchSize - 1);

                    for (int i = 0; i < usedLength; i++)
                    {
                        var toss = rand.NextDouble();
                        string str = toss < 0.2 ? string.Empty : Guid.NewGuid().ToString();
                        if (x == 0) str = string.Empty;
                        inputStr.AddString(str);
                    }

                    inputStr.Seal();

                    StateSerializer<MultiString> s = StreamableSerializer.Create<MultiString>(new SerializerSettings { });
                    var ms = new MemoryStream { Position = 0 };

                    s.Serialize(ms, inputStr);
                    ms.Position = 0;
                    var resultStr = s.Deserialize(ms);

                    Assert.IsTrue(resultStr.Count == inputStr.Count);

                    for (int j = 0; j < inputStr.Count; j++)
                    {
                        Assert.IsTrue(inputStr[j] == resultStr[j]);
                    }
                    resultStr.Dispose();
                    inputStr.Dispose();
                }
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void MultipleAggregatesWithCompiledDictionariesRowSmallBatch()
        {
            IEnumerable<StreamEvent<StreamScope_InputEvent0>> bacon = Array.Empty<StreamEvent<StreamScope_InputEvent0>>();
            IObservableIngressStreamable<StreamScope_InputEvent0> input = bacon.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var result =
                input
                .GroupAggregate(
                    s => new { s.OutputTs, s.DelayInMilliseconds, s.UserId },
                    s => s.Count(),
                    s => s.Min(r => r.Duration),
                    s => s.Max(r => r.Duration),
                    s => s.Average(r => r.Duration),
                    (key, cnt, minDuration, maxDuration, avgDuration) => new StreamScope_OutputEvent0()
                    {
                        OutputTs = key.Key.OutputTs,
                        DelayInMilliseconds = key.Key.DelayInMilliseconds,
                        UserId = key.Key.UserId,
                        Cnt = (long)cnt,
                        MinDuration = minDuration,
                        MaxDuration = maxDuration,
                        AvgDuration = avgDuration,
                    }).ToStreamEventObservable();

            var b = result;
        }

        [TestMethod, TestCategory("Gated")]
        public void PayloadWithFieldOfNestedType01RowSmallBatch()
        {
            var input = Enumerable
                .Range(0, NumEvents)
                .Select(i => new PayloadWithFieldOfNestedType1(i))
                ;
            var streamResult = input
                .ToStreamable()
                .Where(p => p.nt.x % 2 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var linqResult = input.Where(p => p.nt.x % 2 == 0);
            Assert.IsTrue(Enumerable.Zip(linqResult, a, (p1, p2) => p1.nt.x == p2.nt.x).All(p => p));
        }
        [TestMethod, TestCategory("Gated")]
        public void PayloadWithFieldOfNestedType02RowSmallBatch()
        {
            var input = Enumerable
                .Range(0, NumEvents)
                .Select(i => new PayloadWithFieldOfNestedType2(i))
                ;
            var streamResult = input
                .ToStreamable()
                .Where(p => p.nt.x % 2 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var linqResult = input.Where(p => p.nt.x % 2 == 0);
            Assert.IsTrue(Enumerable.Zip(linqResult, a, (p1, p2) => p1.nt.x == p2.nt.x).All(p => p));
        }
    }

    [TestClass]
    public class SimpleTestsRowSmallBatchFloating : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public SimpleTestsRowSmallBatchFloating() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .AllowFloatingReorderPolicy(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        // Total number of input events
        private const int NumEvents = 1000;
        private readonly IEnumerable<MyStruct2> enumerable = Enumerable.Range(0, NumEvents)
            .Select(e =>
                new MyStruct2 { field1 = e, field2 = new MyString(Convert.ToString("string" + e)), field3 = new NestedStruct { nestedField = e } });

        private static IComparerExpression<T> GetDefaultComparerExpression<T>(T t) => ComparerExpression<T>.Default;

        private static IEqualityComparerExpression<T> GetDefaultEqualityComparerExpression<T>(T t) => EqualityComparerExpression<T>.Default;

        public static bool MyIsEqual(List<RankedEvent<char>> a, List<RankedEvent<char>> b)
        {
            if (a.Count != b.Count) return false;

            for (int j = 0; j < a.Count; j++)
            {
                if (!a[j].Equals(b[j])) return false;
            }

            return true;
        }

        public static int MyGetHashCode(List<RankedEvent<char>> a)
        {
            int ret = 0;
            foreach (var l in a)
            {
                ret ^= l.GetHashCode();
            }
            return ret;
        }

        private void TestWhere(Expression<Func<MyStruct2, bool>> predicate, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhere(predicate, disableFusion));

        private void TestSelect<TResult>(Expression<Func<MyStruct2, TResult>> function, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestSelect(function, disableFusion));

        public void TestWhereSelect<U>(Expression<Func<MyStruct2, bool>> wherePredicate, Expression<Func<MyStruct2, U>> selectFunction, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhereSelect(wherePredicate, selectFunction, disableFusion));

        [TestMethod, TestCategory("Gated")]
        public void Where1RowSmallBatchFloating()
            => TestWhere(e => e.field2.mystring.Contains("string"));

        [TestMethod, TestCategory("Gated")]
        public void Where2RowSmallBatchFloating()
            => TestWhere(e => e.field2.mystring.Equals("string0"));

        [TestMethod, TestCategory("Gated")]
        public void Where3RowSmallBatchFloating()
            => TestWhere(e => e.field2.mystring.Equals("string0"));

        [TestMethod, TestCategory("Gated")]
        public void Where4RowSmallBatchFloating()
            => TestWhere(e => e.field3.nestedField == 0);

        [TestMethod, TestCategory("Gated")]
        public void Where5RowSmallBatchFloating()
            => TestWhere(e => true);

        [TestMethod, TestCategory("Gated")]
        public void Where6RowSmallBatchFloating()
            => TestWhere(e => false);

        [TestMethod, TestCategory("Gated")]
        public void Where7RowSmallBatchFloating()
            => TestWhere(e => e.field3.nestedField == 0 || e.field3.nestedField == 1);

        [TestMethod, TestCategory("Gated")]
        public void Where8RowSmallBatchFloating()
            => TestWhere(e => string.Compare(e.field2.mystring, "string0") == 0);

        [TestMethod, TestCategory("Gated")]
        public void Where9RowSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Where(v => v % 2 == 0)
                .Where(v => v % 3 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            Assert.IsTrue(input.Where(v => v % 6 == 0).SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereWithFailedTransformationRowSmallBatchFloating() // stays columnar, but causes payload to be reconstituted
            => Enumerable.Range(0, 100)
                .Select(i => new StructTuple<int, char> { Item1 = i, Item2 = i.ToString()[0], })
                .TestWhere(r => r.ToString().Length > 3);

        [TestMethod, TestCategory("Gated")]
        public void Select1RowSmallBatchFloating()
            => TestSelect(e => e.field1);

        [TestMethod, TestCategory("Gated")]
        public void Select2RowSmallBatchFloating()
            => TestSelect(e => e.field2);

        [TestMethod, TestCategory("Gated")]
        public void Select3RowSmallBatchFloating()
            => TestSelect(e => e.field3);

        [TestMethod, TestCategory("Gated")]
        public void Select4RowSmallBatchFloating()
            => TestSelect(e => new NestedStruct { nestedField = e.field1 });

        [TestMethod, TestCategory("Gated")]
        public void Select5RowSmallBatchFloating()
            => TestSelect(e => e.doubleField);

        [TestMethod, TestCategory("Gated")]
        public void Select6RowSmallBatchFloating()
            => TestSelect(e => ((ulong)e.field1));

        // Tests to make sure that we ref count correctly by doing two pointer swings to the same column.
        [TestMethod, TestCategory("Gated")]
        public void Select7RowSmallBatchFloating()
            => TestSelect(e => new { A = e.field1, B = e.field1, });

        // Tests whether an expression that cannot be columnerized causes code gen to fall back to row-oriented.
        // This is just a degenerate case where the expression is *always* null.
        [TestMethod, TestCategory("Gated")]
        public void Select8RowSmallBatchFloating()
            => TestSelect<ClassWithOnlyAutoProps>(e => null);

        [TestMethod, TestCategory("Gated")]
        public void SelectFromUlongToLongRowSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(v => (long)v)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, longInteger) => (long)integer == longInteger).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeRowSmallBatchFloating()
            => TestSelect(e => new { anonfield1 = e.field1, x = 3 });

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedRowSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(v => new { X = v })
                .Select(e => new { Y = e.X })
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.Y).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedNoOpRowSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(v => new { X = v })
                .Select(e => e)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.X).All(p => p));
        }

        // The point of this test is to have an anonymous type with a "field" in it
        // whose type does not have an overload for it in MemoryPool<TKey, TPayload>.Get(out ColumnBatch<>).
        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeWithFloatFieldRowSmallBatchFloating()
            => TestSelect(e => new { floatField = e.field1 * 3.0, });

        [TestMethod, TestCategory("Gated")]
        public void SelectWhereAnonymousTypeRowSmallBatchFloating()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithAutoPropsRowSmallBatchFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithAutoPropsRowSmallBatchFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithOnlyAutoPropsRowSmallBatchFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithOnlyAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithOnlyAutoPropsRowSmallBatchFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_SelectWhereRowSmallBatchFloating()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_GroupApplyRowSmallBatchFloating()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .GroupApply(e => e.field1, str => str.Count(), (g, cnt) => new MyStruct { field1 = (int)cnt, })
                .Where(e => e.field2 >= 0.0)
                .ToPayloadEnumerable();
            Assert.IsTrue(streamResult.Count() == this.enumerable.Count());
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectWithCtorCallRowSmallBatchFloating()
            => TestSelect(e => new StructWithCtor(e.field1));

        [TestMethod, TestCategory("Gated")]
        public void SelectMany01RowSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany02RowSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany03RowSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany04RowSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart01RowSmallBatchFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select((l, e) => e + ((int)l))
                ;
            var expected = input
                .Select(e => e + e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart02RowSmallBatchFloating()
        {
            // tests simple parameter selection with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select((l, e) => l)
                ;
            var expected = input
                .Select(e => (long)e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey01RowSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectByKey((k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey02RowSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectByKey((k, e) => k)
                ;
            var expected = input
                .Select(e => Empty.Default)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey03RowSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectByKey((s, k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e + (int)s, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e + e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey04RowSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => (int?)(e + 1))
                .SelectByKey((k, e) => new object[] { e, })
                ;
            var expected = input
                .Select(e => new object[] { (int?)(e + 1), })
                .ToArray()
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.Length == output.Length);
            for (int i = 0; i < expected.Length; i++)
            {
                var e = (int?)(expected[i][0]);
                var o = (int?)(output[i][0]);
                Assert.IsTrue(e.HasValue && o.HasValue && (e.Value == o.Value));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStart01RowSmallBatchFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany((l, e) => Enumerable.Repeat(33, e + ((int)l)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + e))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyKey01RowSmallBatchFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectManyByKey((k, e) => Enumerable.Repeat(33, e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStartKey01RowSmallBatchFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectManyByKey((ts, k, e) => Enumerable.Repeat(33 + ((int)ts), e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33 + e, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereSelect1RowSmallBatchFloating()
            => TestWhereSelect(e => true, e => e.field1);

        [TestMethod, TestCategory("Gated")]
        public void SelectStructRowSmallBatchFloating()
        {
            using (var modifier = new ConfigModifier().ForceRowBasedExecution(true).Modify())
            {
                var savedForceRowBasedExecution = Config.ForceRowBasedExecution;
                Config.ForceRowBasedExecution = true;

                var input = Enumerable.Range(0, 100);
                Expression<Func<int, MyStruct>> lambda = e => new MyStruct { field1 = e, field2 = e + 0.5, field3 = default };

                var streamResult = input
                    .ToStreamable()
                    .ShiftEventLifetime(0)
                    .Select(lambda)
                    .ToPayloadEnumerable();
                var linqResult = input
                    .Select(lambda.Compile());

                var query = input.Where(e => true);
                Assert.IsTrue(linqResult.SequenceEqual(streamResult));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantNoOpIntervalsRowSmallBatchFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 5, 'a'),
                StreamEvent.CreateInterval(2, 6, 'b'),
                StreamEvent.CreateInterval(3, 7, 'c'),
                StreamEvent.CreateInterval(4, 8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 5, 'a'),
                StreamEvent.CreateInterval(2, 6, 'b'),
                StreamEvent.CreateInterval(3, 7, 'c'),
                StreamEvent.CreateInterval(4, 8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(10)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantNoOpEdgesRowSmallBatchFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateEnd(6, 2, 'b'),
                StreamEvent.CreateEnd(7, 3, 'c'),
                StreamEvent.CreateEnd(8, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateEnd(6, 2, 'b'),
                StreamEvent.CreateEnd(7, 3, 'c'),
                StreamEvent.CreateEnd(8, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(10)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantClippedIntervalsRowSmallBatchFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 15, 'a'),
                StreamEvent.CreateInterval(2, 16, 'b'),
                StreamEvent.CreateInterval(3, 17, 'c'),
                StreamEvent.CreateInterval(4, 18, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 11, 'a'),
                StreamEvent.CreateInterval(2, 12, 'b'),
                StreamEvent.CreateInterval(3, 13, 'c'),
                StreamEvent.CreateInterval(4, 14, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(10)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantClippedEdgesRowSmallBatchFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateEnd(6, 2, 'b'),
                StreamEvent.CreateEnd(7, 3, 'c'),
                StreamEvent.CreateEnd(8, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateEnd(3, 1, 'a'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateEnd(4, 2, 'b'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 3, 'c'),
                StreamEvent.CreateEnd(6, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(2)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void PunctuationPolicy2RowSmallBatchFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(4, 'c'),
                StreamEvent.CreateStart(7, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(34, 4, 'c'),
                StreamEvent.CreateEnd(35, 7, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };

            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(4, 'c'),
                StreamEvent.CreateStart(7, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(34, 4, 'c'),
                StreamEvent.CreateEnd(35, 7, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(2), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void AlterDurationTest1RowSmallBatchFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(3, 'x'),
                StreamEvent.CreateEnd(5, 3, 'x'),
                StreamEvent.CreateStart(7, 'y'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(3, 8, 'x'),
                StreamEvent.CreateInterval(7, 12, 'y'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var result = str.AlterEventDuration(5).ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void PunctuationPolicy3RowSmallBatchFloating()
        { // Simulates the way Stat does ingress (but with 80K batches, not 3...)
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void StreamCache1RowSmallBatchFloating()
        {
            var input = Enumerable.Range(0, 1000000); // make sure it is enough to have more than one data batch
            ulong limit = 10000;
            var cachedStream = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e))
                .ToStreamable(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .Cache(limit);
            var streamResult = cachedStream
                .ToPayloadEnumerable();

            var c = (ulong)streamResult.Count();
            Assert.IsTrue(c == limit);
            cachedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void StreamCache2RowSmallBatchFloating()
        {
            var limit = 10; // 00000;
            var input = Enumerable.Range(0, limit); // make sure it is enough to have more than one data batch
            var cachedStream = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(StreamEvent.MinSyncTime, e))
                .ToStreamable(DisorderPolicy.Throw())
                .Cache();
            var streamResult = cachedStream
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.Payload)
                .ToEnumerable();
            var foo = streamResult.Count();

            Assert.IsTrue(input.SequenceEqual(streamResult));
            cachedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals1RowSmallBatchFloating()
        {
            var x = ComparerExpression<int>.Default.GetCompareExpr()
                .ExpressionEquals(ComparerExpression<int>.Default.GetCompareExpr());
            Assert.IsTrue(x);
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals2RowSmallBatchFloating()
        {
            var x = new CompoundGroupKeyEqualityComparer<Empty, MyStruct2>(EqualityComparerExpression<Empty>.Default, EqualityComparerExpression<MyStruct2>.Default);
            var y = x.GetEqualsExpr().ExpressionEquals(x.GetEqualsExpr());
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals3RowSmallBatchFloating()
        {
            var x1 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var x2 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var y = x1.GetCompareExpr().ExpressionEquals(x2.GetCompareExpr());
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals4RowSmallBatchFloating()
        {
            var x1 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var x2 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);

            var xx1 = new CompoundGroupKeyComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x1, ComparerExpression<MyStruct2>.Default);
            var xx2 = new CompoundGroupKeyComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x2, ComparerExpression<MyStruct2>.Default);

            var y = xx1.GetCompareExpr().ExpressionEquals(xx2.GetCompareExpr());
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void CompoundGroupKeyEqualityComparerDefaultRowSmallBatchFloating()
        {
            var xx1 = EqualityComparerExpression<CompoundGroupKey<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>>.Default;

            var x2 = new CompoundGroupKeyEqualityComparer<Empty, MyStruct2>(EqualityComparerExpression<Empty>.Default, EqualityComparerExpression<MyStruct2>.Default);
            var xx2 = new CompoundGroupKeyEqualityComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x2, EqualityComparerExpression<MyStruct2>.Default);

            var y = xx1.ExpressionEquals(xx2);
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void CompoundGroupKeyComparerDefaultRowSmallBatchFloating()
        {
            var xx1 = ComparerExpression<CompoundGroupKey<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>>.Default;

            var x2 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var xx2 = new CompoundGroupKeyComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x2, ComparerExpression<MyStruct2>.Default);

            var y = xx1.ExpressionEquals(xx2);
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForIComparableRowSmallBatchFloating()
        {
            var defaultComparerExpressionProvider = ComparerExpression<ClassImplementingIComparable>.Default;
            var defaultCompareExpression = defaultComparerExpressionProvider.GetCompareExpr();
            var compiledDefaultCompareExpression = defaultCompareExpression.Compile();
            var o = new ClassImplementingIComparable();
            var result = compiledDefaultCompareExpression(o, o);
            Assert.IsTrue(result == 3);

        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForGenericIComparerRowSmallBatchFloating()
        {
            var defaultComparerExpressionProvider = ComparerExpression<ClassImplementingGenericIComparer>.Default;
            var defaultCompareExpression = defaultComparerExpressionProvider.GetCompareExpr();
            var compiledDefaultCompareExpression = defaultCompareExpression.Compile();
            var o = new ClassImplementingGenericIComparer();
            var result = compiledDefaultCompareExpression(o, o);
            Assert.IsTrue(result == 3);

        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForNonGenericIComparerRowSmallBatchFloating()
        {
            var defaultComparerExpressionProvider = ComparerExpression<ClassImplementingNonGenericIComparer>.Default;
            var defaultCompareExpression = defaultComparerExpressionProvider.GetCompareExpr();
            var compiledDefaultCompareExpression = defaultCompareExpression.Compile();
            var o = new ClassImplementingNonGenericIComparer();
            var result = compiledDefaultCompareExpression(o, o);
            Assert.IsTrue(result == 3);

        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForAnonymousType1RowSmallBatchFloating()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var f = GetDefaultComparerExpression(a);
            var g = f.GetCompareExpr();
            var h = g.Compile();
            var k = h(a, a);
            Assert.IsTrue(k == 0);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType1RowSmallBatchFloating()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, a);
            Assert.IsTrue(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType2RowSmallBatchFloating()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var b = new { Z = i + 1, A = (char)('a' + i), W = "abc" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, b);
            Assert.IsFalse(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType3RowSmallBatchFloating()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var b = new { Z = i, A = (char)('b' + i), W = "abc" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, b);
            Assert.IsFalse(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType4RowSmallBatchFloating()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var b = new { Z = i, A = (char)('a' + i), W = "abcd" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, b);
            Assert.IsFalse(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void NaryMulticast1RowSmallBatchFloating()
        {
            var enumerable = Enumerable.Range(0, 10000).ToList();

            var stream = enumerable.ToStreamable();
            var multiStream = stream.Multicast(3);

            var output1 = multiStream[0].Where(e => e % 3 == 0);
            var output2 = multiStream[1].Where(e => e % 3 == 1);
            var output3 = multiStream[2].Where(e => e % 3 == 2);
            var union = output1.Union(output2).Union(output3);

            var output = union.ToPayloadEnumerable().ToList();
            output.Sort();
            Assert.IsTrue(enumerable.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void NaryMulticastContainer1RowSmallBatchFloating()
        {
            var outputList = new List<int>();

            // Inputs
            var container = new QueryContainer();
            Stream state = null;

            // Input data
            var enumerable = Enumerable.Range(0, 10000).ToList();
            var inputObservable = enumerable.ToObservable().Select(e => StreamEvent.CreateStart(0, e));

            // Query start
            var stream = container.RegisterInput(inputObservable);
            var multiStream = stream.Multicast(3);

            var output1 = multiStream[0].Where(e => e % 3 == 0);
            var output2 = multiStream[1].Where(e => e % 3 == 1);
            var output3 = multiStream[2].Where(e => e % 3 == 2);
            var union = output1.Union(output2).Union(output3);

            var outputObservable = container.RegisterOutput(union);

            // Test the output
            var outputAsync = outputObservable.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
            container.Restore(state);
            outputAsync.Wait();

            outputList.Sort();
            Assert.IsTrue(enumerable.SequenceEqual(outputList));
        }

        [TestMethod, TestCategory("Gated")]
        public void NaryMulticastContainerErrorRowSmallBatchFloating()
        {
            // Inputs
            var container = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var enumerable = Enumerable.Range(0, 10000).ToList();
            var inputObservable = enumerable.ToObservable().Select(e => StreamEvent.CreateStart(0, e));

            // Query start
            var stream = container.RegisterInput(inputObservable);
            var multiStream = stream.Multicast(3);

            var output1 = multiStream[0].Where(e => e % 3 == 0);
            var output2 = multiStream[1].Where(e => e % 3 == 1);
            var output3 = multiStream[2].Where(e => e % 3 == 2);
            var union = output1.Union(output2).Union(output3);

            var outputObservable = container.RegisterOutput(union);

            // Validate that attempting to start the query before subscriptions will result in an error.
            bool foundException = false;
            try
            {
                container.Restore(state);
            }
            catch (Exception)
            {
                foundException = true;
            }

            Assert.IsTrue(foundException, "Expected an exception.");
        }

        [TestMethod, TestCategory("Gated")]
        public void StreamSortRowSmallBatchFloating()
        {
            var input = Enumerable.Range(0, 20).Select(i => (char)('a' + i));
            var reversedInput = input.Reverse();
            var str = reversedInput.ToStatStreamable();
            var sortedStream = str.Sort(c => c);
            var result = sortedStream.ToStreamEventObservable().Where(se => se.IsData).Select(se => se.Payload).ToEnumerable();
            Assert.IsTrue(input.SequenceEqual(result));
            sortedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void SortModifiedStreamRowSmallBatchFloating()
        {
            // The Sort method on a stream uses an observable cache. The cache
            // had been putting all rows from the messages into the list to be sorted,
            // even if it was an invalid row.
            var input = Enumerable.Range(0, 20).Select(i => (char)('a' + i));
            var str = input.ToStatStreamable();
            var str2 = str.Where(r => r != 'b');
            var sortedStream = str2.Sort(c => c);
            var result = sortedStream.ToStreamEventObservable().Where(se => se.IsData).Select(se => se.Payload).ToEnumerable();
            Assert.IsTrue(input.Count() == result.Count() + 1); // just make sure deleted element isn't counted anymore
            sortedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void GroupStreamableCodeGenTest1RowSmallBatchFloating()
        {
            var input1 = new[]
            {
                StreamEvent.CreateInterval(100, 101, 13),
                StreamEvent.CreateInterval(100, 110, 11),
                StreamEvent.CreateInterval(101, 105, 12),
                StreamEvent.CreateInterval(102, 112, 21),
                StreamEvent.CreateInterval(109, 112, 14),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };

            var input2 = new[]
            {
                StreamEvent.CreateInterval(100, 110, 'd'),
                StreamEvent.CreateInterval(101, 103, 'b'),
                StreamEvent.CreateInterval(106, 108, 'b'),
                StreamEvent.CreateInterval(106, 109, 'b'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime)
            };

            var inputStream1 = input1.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var inputStream2 = input2.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var outputStream = inputStream1.WhereNotExists(
                inputStream2,
                l => l / 10,
                r => r - 'a');

            var expected = new[]
            {
                StreamEvent.CreateStart(100, 11),
                StreamEvent.CreateEnd(101, 100, 11),
                StreamEvent.CreateStart(103, 11),
                StreamEvent.CreateEnd(106, 103, 11),
                StreamEvent.CreateInterval(109, 110, 11),
                StreamEvent.CreateInterval(103, 105, 12),
                StreamEvent.CreateInterval(100, 101, 13),
                StreamEvent.CreateInterval(109, 112, 14),
                StreamEvent.CreateStart(102, 21),
                StreamEvent.CreateEnd(112, 102, 21),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };
            var outputEnumerable = outputStream.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void CountColumnar1RowSmallBatchFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<ulong>[]
            {
                StreamEvent.CreateStart<ulong>(StreamEvent.MinSyncTime, 3),
                StreamEvent.CreatePunctuation<ulong>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateEnd<ulong>(StreamEvent.MinSyncTime + 1, StreamEvent.MinSyncTime, 3),
                StreamEvent.CreateStart<ulong>(StreamEvent.MinSyncTime + 1, 6),
                StreamEvent.CreatePunctuation<ulong>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateEnd<ulong>(StreamEvent.MinSyncTime + 2, StreamEvent.MinSyncTime + 1, 6),
                StreamEvent.CreateStart<ulong>(StreamEvent.MinSyncTime + 2, 9),
                StreamEvent.CreatePunctuation<ulong>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None);
            var count = str.Count();
            var outputEnumerable = count.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageColumnar1RowSmallBatchFloating() // tests codegen for Snapshot_noecq
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),      // 97
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),      // 98
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),      // 99  running average = 98

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),  // 100
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),  // 101
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),  // 102 running average = 99.5

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),  // 103
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),  // 104
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),  // 105 running average = 101
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<double>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 1, StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 2, StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 101.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None).SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime);
            var avg = str.Average(c => (double)c);
            var outputEnumerable = avg.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageColumnar2RowSmallBatchFloating() // tests codegen for Snapshot_q
        {
            var zero = StreamEvent.MinSyncTime;
            var one = StreamEvent.MinSyncTime + 1;
            var two = StreamEvent.MinSyncTime + 2;
            var three = StreamEvent.MinSyncTime + 3;
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(zero, one, 'a'),
                StreamEvent.CreateInterval(zero, one, 'b'),
                StreamEvent.CreateInterval(zero, one, 'c'),
                StreamEvent.CreateInterval(one, two, 'd'),
                StreamEvent.CreateInterval(one, two, 'e'),
                StreamEvent.CreateInterval(one, two, 'f'),
                StreamEvent.CreateInterval(two, three, 'g'),
                StreamEvent.CreateInterval(two, three, 'h'),
                StreamEvent.CreateInterval(two, three, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<double>[]
            {
                StreamEvent.CreateInterval(zero, one, 98.0),
                StreamEvent.CreatePunctuation<double>(one),
                StreamEvent.CreateInterval(one, two, 101.0),
                StreamEvent.CreatePunctuation<double>(two),
                StreamEvent.CreateInterval(two, three, 104.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None).SetProperty().IsConstantDuration(true, 1);
            var avg = str.Average(c => (double)c);
            var outputEnumerable = avg.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageColumnar3RowSmallBatchFloating() // tests codegen for Snapshot_pq
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),      // 97
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),      // 98
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),      // 99  running average = 98

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),  // 100
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),  // 101
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),  // 102 running average = 99.5

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),  // 103
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),  // 104
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),  // 105 running average = 101
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<double>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 1, StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 2, StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 101.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None);
            var avg = str.Average(c => (double)c);
            var outputEnumerable = avg.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SumColumnar1RowSmallBatchFloating() // make sure expression is properly parenthesized in codegen
        {
            var input = Enumerable.Range(0, 100);
            Expression<Func<int, int>> query = e => e & 0x7;

            var streamResult = input
                .ToStreamable()
                .Sum(query)
                .ToPayloadEnumerable()
                .Last();

            var expected = input.Sum(query.Compile());
            Assert.IsTrue(expected == streamResult);

        }

        [TestMethod, TestCategory("Gated")]
        public void MultiAggregate1RowSmallBatchFloating() // test codegen for multi-aggregates
        {
            var input = Enumerable.Range(0, 100).Select(i => new MyStruct { field1 = i, field2 = 0.0, field3 = Guid.Empty, });

            var streamResult = input
                .ToStreamable()
                .Aggregate(a => a.Sum(x => x.field1), a => a.Count(), (s, c) => new MyStruct { field1 = s + ((int)c) })
                .ToPayloadEnumerable()
                ;

            Assert.IsTrue(streamResult.Count() == 1);

            Assert.IsTrue(streamResult.First().field1 == 5050);

        }

        [TestMethod, TestCategory("Gated")]
        public void MultiAggregate2RowSmallBatchFloating() // test codegen for multi-aggregates with anonymous type
        {
            var input = Enumerable.Range(0, 100).Select(i => new MyStruct { field1 = i, field2 = 0.0, field3 = Guid.Empty, });

            var streamResult = input
                .ToStreamable()
                .Aggregate(a => a.Sum(x => x.field1), a => a.Count(), (s, c) => new { field1 = s, field2 = c })
                .ToPayloadEnumerable()
                ;

            Assert.IsTrue(streamResult.Count() == 1);

            var f = streamResult.First();
            Assert.IsTrue(f.field1 == 4950 && f.field2 == 100);

        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest1RowSmallBatchFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(24, 3, 'c'),
                StreamEvent.CreateEnd(26, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateInterval(2, 22, 'b'),
                StreamEvent.CreateInterval(3, 24, 'c'),
                StreamEvent.CreateInterval(4, 26, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Cache(0, false, true);
            var result = str.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest2RowSmallBatchFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(24, 3, 'c'),
                StreamEvent.CreateEnd(26, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateInterval(3, 24, 'c'),
                StreamEvent.CreateInterval(4, 26, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Cache(0, false, true);
            var result = str.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest3RowSmallBatchFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(24, 4, 'd'),
                StreamEvent.CreateEnd(26, 3, 'c'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateInterval(2, 22, 'b'),
                StreamEvent.CreateInterval(3, 26, 'c'),
                StreamEvent.CreateInterval(4, 24, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Cache(0, false, true);
            var result = str.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void BinaryUnionTest1RowSmallBatchFloating()
        {
            var input1 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(2, 20, 'a'),
                StreamEvent.CreateStart(4, 'b'),
                StreamEvent.CreateStart(6, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var input2 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(3, 20, 'a'),
                StreamEvent.CreateStart(5, 'b'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(9, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };

            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(2, 20, 'a'),
                StreamEvent.CreateInterval(3, 20, 'a'),
                StreamEvent.CreateStart(4, 'b'),
                StreamEvent.CreateStart(5, 'b'),
                StreamEvent.CreateStart(6, 'c'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreateStart(9, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str1 = input1.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null).Cache();
            var str2 = input2.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null).Cache();
            var resultEnum = str1.Union(str2).ToStreamEventObservable().ToEnumerable();
            var result = resultEnum.ToList();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str1.Dispose();
            str2.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void BinaryUnionTest2RowSmallBatchFloating()
        {
            var input1 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var input2 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(5, 20, 'a'),
                StreamEvent.CreateStart(6, 'b'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };

            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateInterval(5, 20, 'a'),
                StreamEvent.CreateStart(6, 'b'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str1 = input1.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation).Cache();
            var str2 = input2.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation).Cache();
            var result = str1.Union(str2).ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str1.Dispose();
            str2.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void DeterministicUnionTestRowSmallBatchFloating()
        {
            int count = 100;
            DateTime now = DateTime.UtcNow;

            var input = new List<StreamEvent<int>> { StreamEvent.CreatePunctuation<int>(now.Ticks) };

            // Add right events.
            for (int i = 0; i < count; ++i)
                input.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i + 1));

            // Cti forces right batch to go through before reaching Config.DataBatchSize.
            input.Add(StreamEvent.CreatePunctuation<int>(now.Ticks));

            // Add left events.
            for (int i = 0; i < count; ++i)
                input.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i));
            input.Add(StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime));

            // Expect left before right even though right went through first.
            var expectedOutput = new List<StreamEvent<int>> { StreamEvent.CreatePunctuation<int>(now.Ticks) };
            for (int i = 0; i < count; i++)
                expectedOutput.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i));
            for (int i = 0; i < count; i++)
                expectedOutput.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i + 1));
            expectedOutput.Add(StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime));

            var inputStreams = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Multicast(2);

            var left = inputStreams[0].Where(e => e % 2 == 0);
            var right = inputStreams[1].Where(e => e % 2 == 1);

            var result = left
                .Union(right)
                .ToStreamEventObservable()
                .ToEnumerable()
                .ToList();

            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest4RowSmallBatchFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateStart(6, 'b'),
                StreamEvent.CreateEnd(10, 6, 'b'),
                StreamEvent.CreateStart(11, 'c'),
                StreamEvent.CreateEnd(15, 11, 'c'),
                StreamEvent.CreateStart(16, 'd'),
                StreamEvent.CreateEnd(20, 16, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 5, 'a'),
                StreamEvent.CreateInterval(6, 10, 'b'),
                StreamEvent.CreateInterval(11, 15, 'c'),
                StreamEvent.CreateInterval(16, 20, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation);
            var result = str.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void AggregateTest1RowSmallBatchFloating()
        {
            long threeSecondTicks = TimeSpan.FromSeconds(3).Ticks;

            var stream = Events().ToEvents(e => e.Vs.Ticks, e => e.Vs.Ticks + threeSecondTicks)
                                 .ToObservable()
                                 .ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None());
            var expectedResult = new StreamEvent<ResultEvent>[]
            {
                StreamEvent.CreateStart(633979008010000000, new ResultEvent { Key = 1, Cnt = 1 }),
                StreamEvent.CreateEnd(633979008020000000, 633979008010000000, new ResultEvent { Key = 1, Cnt = 1 }),
                StreamEvent.CreateStart(633979008020000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateEnd(633979008030000000, 633979008020000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateStart(633979008030000000, new ResultEvent { Key = 1, Cnt = 3 }),
                StreamEvent.CreateEnd(633979008040000000, 633979008030000000, new ResultEvent { Key = 1, Cnt = 3 }),
                StreamEvent.CreateStart(633979008040000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateEnd(633979008050000000, 633979008040000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateStart(633979008050000000, new ResultEvent { Key = 1, Cnt = 1 }),
                StreamEvent.CreateEnd(633979008060000000, 633979008050000000, new ResultEvent { Key = 1, Cnt = 1 }),
            };
            var counts = stream.GroupApply(
                                    g => g.Key,
                                    e => e.Count(),
                                    (g, e) => new ResultEvent { Key = g.Key, Cnt = e })
                               .ToStreamEventObservable()
                               .ToEnumerable()
                               .Where(e => e.IsData && e.Payload.Key == 1);
            foreach (var x in counts)
            {
                Console.WriteLine("{0}, {1}", x.SyncTime, x.OtherTime);
            }
            Assert.IsTrue(expectedResult.SequenceEqual(counts));
        }

        public IEnumerable<Event> Events()
        {
            var vs = new DateTime(2010, 1, 1, 0, 00, 00, DateTimeKind.Utc);
            var rand = new Random(0);
            int eventCnt = 5;
            for (int i = 0; i < eventCnt; i++)
            {
                yield return new Event { Vs = vs, V = rand.Next(10), Key = rand.Next(3) };
                vs = vs.AddSeconds(1);
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void CodeGenWithFieldOfGenericTypeRowSmallBatchFloating()
        {
            // Taken from the PerformanceTest sample.
            // The GroupApply creates batches where the key is a generic type and codegen was doing a type cast without making sure the type's name was valid C# syntax
            var numEventsPerTumble = 2000;
            var tumblingWindowDataset =
                Observable.Range(0, 100)
                    .Select(e => StreamEvent.CreateInterval(((long)e / numEventsPerTumble) * numEventsPerTumble, ((long)e / numEventsPerTumble) * numEventsPerTumble + numEventsPerTumble, new Payload { field1 = 0, field2 = 0 }))
                    .ToStreamable()
                    .SetProperty().IsConstantDuration(true, numEventsPerTumble)
                    .SetProperty().IsSnapshotSorted(true, e => e.field1);
            var result = tumblingWindowDataset.GroupApply(e => e.field2, str => str.Count(), (g, c) => new { Key = g, Count = c });
            var a = result.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(a.Length == 3); // just make sure it doesn't crash
        }

        [TestMethod, TestCategory("Gated")]
        public void StringSerializationRowSmallBatchFloating()
        {
            var rand = new Random(0);
            var pool = MemoryManager.GetColumnPool<string>();

            for (int x = 0; x < 100; x++)
            {
                pool.Get(out ColumnBatch<string> inputStr);

                var toss1 = rand.NextDouble();
                inputStr.UsedLength = toss1 < 0.1 ? 0 : rand.Next(Config.DataBatchSize);

                for (int i = 0; i < inputStr.UsedLength; i++)
                {
                    var toss = rand.NextDouble();
                    inputStr.col[i] = toss < 0.2 ? string.Empty : (toss < 0.4 ? null : Guid.NewGuid().ToString());
                    if (x == 0) inputStr.col[i] = null;
                    if (x == 1) inputStr.col[i] = string.Empty;
                }

                StateSerializer<ColumnBatch<string>> s = StreamableSerializer.Create<ColumnBatch<string>>(new SerializerSettings { });
                var ms = new MemoryStream { Position = 0 };

                s.Serialize(ms, inputStr);
                ms.Position = 0;
                ColumnBatch<string> resultStr = s.Deserialize(ms);

                Assert.IsTrue(resultStr.UsedLength == inputStr.UsedLength);

                for (int j = 0; j < inputStr.UsedLength; j++)
                {
                    Assert.IsTrue(inputStr.col[j] == resultStr.col[j]);
                }
                resultStr.ReturnClear();
                inputStr.ReturnClear();
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void CharArraySerializationRowSmallBatchFloating()
        {
            using (var modifier = new ConfigModifier().SerializationCompressionLevel(SerializationCompressionLevel.CharArrayToUTF8).Modify())
            {
                var rand = new Random(0);

                for (int x = 0; x < 5; x++)
                {
                    var inputStr = new MultiString();

                    var toss1 = rand.NextDouble();
                    var usedLength = 1 + rand.Next(Config.DataBatchSize - 1);

                    for (int i = 0; i < usedLength; i++)
                    {
                        var toss = rand.NextDouble();
                        string str = toss < 0.2 ? string.Empty : Guid.NewGuid().ToString();
                        if (x == 0) str = string.Empty;
                        inputStr.AddString(str);
                    }

                    inputStr.Seal();

                    StateSerializer<MultiString> s = StreamableSerializer.Create<MultiString>(new SerializerSettings { });
                    var ms = new MemoryStream { Position = 0 };

                    s.Serialize(ms, inputStr);
                    ms.Position = 0;
                    var resultStr = s.Deserialize(ms);

                    Assert.IsTrue(resultStr.Count == inputStr.Count);

                    for (int j = 0; j < inputStr.Count; j++)
                    {
                        Assert.IsTrue(inputStr[j] == resultStr[j]);
                    }
                    resultStr.Dispose();
                    inputStr.Dispose();
                }
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void MultipleAggregatesWithCompiledDictionariesRowSmallBatchFloating()
        {
            IEnumerable<StreamEvent<StreamScope_InputEvent0>> bacon = Array.Empty<StreamEvent<StreamScope_InputEvent0>>();
            IObservableIngressStreamable<StreamScope_InputEvent0> input = bacon.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var result =
                input
                .GroupAggregate(
                    s => new { s.OutputTs, s.DelayInMilliseconds, s.UserId },
                    s => s.Count(),
                    s => s.Min(r => r.Duration),
                    s => s.Max(r => r.Duration),
                    s => s.Average(r => r.Duration),
                    (key, cnt, minDuration, maxDuration, avgDuration) => new StreamScope_OutputEvent0()
                    {
                        OutputTs = key.Key.OutputTs,
                        DelayInMilliseconds = key.Key.DelayInMilliseconds,
                        UserId = key.Key.UserId,
                        Cnt = (long)cnt,
                        MinDuration = minDuration,
                        MaxDuration = maxDuration,
                        AvgDuration = avgDuration,
                    }).ToStreamEventObservable();

            var b = result;
        }

        [TestMethod, TestCategory("Gated")]
        public void PayloadWithFieldOfNestedType01RowSmallBatchFloating()
        {
            var input = Enumerable
                .Range(0, NumEvents)
                .Select(i => new PayloadWithFieldOfNestedType1(i))
                ;
            var streamResult = input
                .ToStreamable()
                .Where(p => p.nt.x % 2 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var linqResult = input.Where(p => p.nt.x % 2 == 0);
            Assert.IsTrue(Enumerable.Zip(linqResult, a, (p1, p2) => p1.nt.x == p2.nt.x).All(p => p));
        }
        [TestMethod, TestCategory("Gated")]
        public void PayloadWithFieldOfNestedType02RowSmallBatchFloating()
        {
            var input = Enumerable
                .Range(0, NumEvents)
                .Select(i => new PayloadWithFieldOfNestedType2(i))
                ;
            var streamResult = input
                .ToStreamable()
                .Where(p => p.nt.x % 2 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var linqResult = input.Where(p => p.nt.x % 2 == 0);
            Assert.IsTrue(Enumerable.Zip(linqResult, a, (p1, p2) => p1.nt.x == p2.nt.x).All(p => p));
        }
    }

    [TestClass]
    public class SimpleTestsColumnar : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public SimpleTestsColumnar() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        // Total number of input events
        private const int NumEvents = 1000;
        private readonly IEnumerable<MyStruct2> enumerable = Enumerable.Range(0, NumEvents)
            .Select(e =>
                new MyStruct2 { field1 = e, field2 = new MyString(Convert.ToString("string" + e)), field3 = new NestedStruct { nestedField = e } });

        private static IComparerExpression<T> GetDefaultComparerExpression<T>(T t) => ComparerExpression<T>.Default;

        private static IEqualityComparerExpression<T> GetDefaultEqualityComparerExpression<T>(T t) => EqualityComparerExpression<T>.Default;

        public static bool MyIsEqual(List<RankedEvent<char>> a, List<RankedEvent<char>> b)
        {
            if (a.Count != b.Count) return false;

            for (int j = 0; j < a.Count; j++)
            {
                if (!a[j].Equals(b[j])) return false;
            }

            return true;
        }

        public static int MyGetHashCode(List<RankedEvent<char>> a)
        {
            int ret = 0;
            foreach (var l in a)
            {
                ret ^= l.GetHashCode();
            }
            return ret;
        }

        private void TestWhere(Expression<Func<MyStruct2, bool>> predicate, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhere(predicate, disableFusion));

        private void TestSelect<TResult>(Expression<Func<MyStruct2, TResult>> function, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestSelect(function, disableFusion));

        public void TestWhereSelect<U>(Expression<Func<MyStruct2, bool>> wherePredicate, Expression<Func<MyStruct2, U>> selectFunction, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhereSelect(wherePredicate, selectFunction, disableFusion));

        [TestMethod, TestCategory("Gated")]
        public void Where1Columnar()
            => TestWhere(e => e.field2.mystring.Contains("string"));

        [TestMethod, TestCategory("Gated")]
        public void Where2Columnar()
            => TestWhere(e => e.field2.mystring.Equals("string0"));

        [TestMethod, TestCategory("Gated")]
        public void Where3Columnar()
            => TestWhere(e => e.field2.mystring.Equals("string0"));

        [TestMethod, TestCategory("Gated")]
        public void Where4Columnar()
            => TestWhere(e => e.field3.nestedField == 0);

        [TestMethod, TestCategory("Gated")]
        public void Where5Columnar()
            => TestWhere(e => true);

        [TestMethod, TestCategory("Gated")]
        public void Where6Columnar()
            => TestWhere(e => false);

        [TestMethod, TestCategory("Gated")]
        public void Where7Columnar()
            => TestWhere(e => e.field3.nestedField == 0 || e.field3.nestedField == 1);

        [TestMethod, TestCategory("Gated")]
        public void Where8Columnar()
            => TestWhere(e => string.Compare(e.field2.mystring, "string0") == 0);

        [TestMethod, TestCategory("Gated")]
        public void Where9Columnar()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Where(v => v % 2 == 0)
                .Where(v => v % 3 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            Assert.IsTrue(input.Where(v => v % 6 == 0).SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereWithFailedTransformationColumnar() // stays columnar, but causes payload to be reconstituted
            => Enumerable.Range(0, 100)
                .Select(i => new StructTuple<int, char> { Item1 = i, Item2 = i.ToString()[0], })
                .TestWhere(r => r.ToString().Length > 3);

        [TestMethod, TestCategory("Gated")]
        public void Select1Columnar()
            => TestSelect(e => e.field1);

        [TestMethod, TestCategory("Gated")]
        public void Select2Columnar()
            => TestSelect(e => e.field2);

        [TestMethod, TestCategory("Gated")]
        public void Select3Columnar()
            => TestSelect(e => e.field3);

        [TestMethod, TestCategory("Gated")]
        public void Select4Columnar()
            => TestSelect(e => new NestedStruct { nestedField = e.field1 });

        [TestMethod, TestCategory("Gated")]
        public void Select5Columnar()
            => TestSelect(e => e.doubleField);

        [TestMethod, TestCategory("Gated")]
        public void Select6Columnar()
            => TestSelect(e => ((ulong)e.field1));

        // Tests to make sure that we ref count correctly by doing two pointer swings to the same column.
        [TestMethod, TestCategory("Gated")]
        public void Select7Columnar()
            => TestSelect(e => new { A = e.field1, B = e.field1, });

        // Tests whether an expression that cannot be columnerized causes code gen to fall back to row-oriented.
        // This is just a degenerate case where the expression is *always* null.
        [TestMethod, TestCategory("Gated")]
        public void Select8Columnar()
            => TestSelect<ClassWithOnlyAutoProps>(e => null);

        [TestMethod, TestCategory("Gated")]
        public void SelectFromUlongToLongColumnar()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(v => (long)v)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, longInteger) => (long)integer == longInteger).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeColumnar()
            => TestSelect(e => new { anonfield1 = e.field1, x = 3 });

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedColumnar()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(v => new { X = v })
                .Select(e => new { Y = e.X })
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.Y).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedNoOpColumnar()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(v => new { X = v })
                .Select(e => e)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.X).All(p => p));
        }

        // The point of this test is to have an anonymous type with a "field" in it
        // whose type does not have an overload for it in MemoryPool<TKey, TPayload>.Get(out ColumnBatch<>).
        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeWithFloatFieldColumnar()
            => TestSelect(e => new { floatField = e.field1 * 3.0, });

        [TestMethod, TestCategory("Gated")]
        public void SelectWhereAnonymousTypeColumnar()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithAutoPropsColumnar()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithAutoPropsColumnar()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithOnlyAutoPropsColumnar()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithOnlyAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithOnlyAutoPropsColumnar()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_SelectWhereColumnar()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_GroupApplyColumnar()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .GroupApply(e => e.field1, str => str.Count(), (g, cnt) => new MyStruct { field1 = (int)cnt, })
                .Where(e => e.field2 >= 0.0)
                .ToPayloadEnumerable();
            Assert.IsTrue(streamResult.Count() == this.enumerable.Count());
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectWithCtorCallColumnar()
            => TestSelect(e => new StructWithCtor(e.field1));

        [TestMethod, TestCategory("Gated")]
        public void SelectMany01Columnar()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany02Columnar()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany03Columnar()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany04Columnar()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart01Columnar()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select((l, e) => e + ((int)l))
                ;
            var expected = input
                .Select(e => e + e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart02Columnar()
        {
            // tests simple parameter selection with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select((l, e) => l)
                ;
            var expected = input
                .Select(e => (long)e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey01Columnar()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectByKey((k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey02Columnar()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectByKey((k, e) => k)
                ;
            var expected = input
                .Select(e => Empty.Default)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey03Columnar()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectByKey((s, k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e + (int)s, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e + e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey04Columnar()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => (int?)(e + 1))
                .SelectByKey((k, e) => new object[] { e, })
                ;
            var expected = input
                .Select(e => new object[] { (int?)(e + 1), })
                .ToArray()
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.Length == output.Length);
            for (int i = 0; i < expected.Length; i++)
            {
                var e = (int?)(expected[i][0]);
                var o = (int?)(output[i][0]);
                Assert.IsTrue(e.HasValue && o.HasValue && (e.Value == o.Value));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStart01Columnar()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany((l, e) => Enumerable.Repeat(33, e + ((int)l)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + e))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyKey01Columnar()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectManyByKey((k, e) => Enumerable.Repeat(33, e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStartKey01Columnar()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectManyByKey((ts, k, e) => Enumerable.Repeat(33 + ((int)ts), e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33 + e, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereSelect1Columnar()
            => TestWhereSelect(e => true, e => e.field1);

        [TestMethod, TestCategory("Gated")]
        public void SelectStructColumnar()
        {
            using (var modifier = new ConfigModifier().ForceRowBasedExecution(true).Modify())
            {
                var savedForceRowBasedExecution = Config.ForceRowBasedExecution;
                Config.ForceRowBasedExecution = true;

                var input = Enumerable.Range(0, 100);
                Expression<Func<int, MyStruct>> lambda = e => new MyStruct { field1 = e, field2 = e + 0.5, field3 = default };

                var streamResult = input
                    .ToStreamable()
                    .ShiftEventLifetime(0)
                    .Select(lambda)
                    .ToPayloadEnumerable();
                var linqResult = input
                    .Select(lambda.Compile());

                var query = input.Where(e => true);
                Assert.IsTrue(linqResult.SequenceEqual(streamResult));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantNoOpIntervalsColumnar()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 5, 'a'),
                StreamEvent.CreateInterval(2, 6, 'b'),
                StreamEvent.CreateInterval(3, 7, 'c'),
                StreamEvent.CreateInterval(4, 8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 5, 'a'),
                StreamEvent.CreateInterval(2, 6, 'b'),
                StreamEvent.CreateInterval(3, 7, 'c'),
                StreamEvent.CreateInterval(4, 8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(10)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantNoOpEdgesColumnar()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateEnd(6, 2, 'b'),
                StreamEvent.CreateEnd(7, 3, 'c'),
                StreamEvent.CreateEnd(8, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateEnd(6, 2, 'b'),
                StreamEvent.CreateEnd(7, 3, 'c'),
                StreamEvent.CreateEnd(8, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(10)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantClippedIntervalsColumnar()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 15, 'a'),
                StreamEvent.CreateInterval(2, 16, 'b'),
                StreamEvent.CreateInterval(3, 17, 'c'),
                StreamEvent.CreateInterval(4, 18, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 11, 'a'),
                StreamEvent.CreateInterval(2, 12, 'b'),
                StreamEvent.CreateInterval(3, 13, 'c'),
                StreamEvent.CreateInterval(4, 14, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(10)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantClippedEdgesColumnar()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateEnd(6, 2, 'b'),
                StreamEvent.CreateEnd(7, 3, 'c'),
                StreamEvent.CreateEnd(8, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateEnd(3, 1, 'a'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateEnd(4, 2, 'b'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 3, 'c'),
                StreamEvent.CreateEnd(6, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(2)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderPolicy1Columnar()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateEnd(20, 5, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(9, 'x'),
                StreamEvent.CreateEnd(20, 9, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderPolicy2Columnar()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreateEnd(24, 5, 'x'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(9, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreateEnd(24, 9, 'x'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderPolicy3Columnar()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreateEnd(20, 5, 'x'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(9, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreateEnd(22, 9, 'x'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var observInput = input.ToObservable();
            var str = observInput.ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderPolicy4Columnar()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateStart(11, 'z'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateEnd(20, 5, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreateEnd(24, 5, 'x'),
                StreamEvent.CreateEnd(26, 5, 'x'),
                StreamEvent.CreateEnd(28, 11, 'z'),
                StreamEvent.CreateEnd(30, 5, 'x'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(9, 'x'),
                StreamEvent.CreateStart(9, 'x'),
                StreamEvent.CreateStart(11, 'z'),
                StreamEvent.CreateStart(11, 'x'),
                StreamEvent.CreateStart(11, 'x'),
                StreamEvent.CreateEnd(20, 9, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreateEnd(24, 9, 'x'),
                StreamEvent.CreateEnd(26, 11, 'x'),
                StreamEvent.CreateEnd(28, 11, 'z'),
                StreamEvent.CreateEnd(30, 11, 'x'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void PunctuationPolicy2Columnar()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(4, 'c'),
                StreamEvent.CreateStart(7, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(34, 4, 'c'),
                StreamEvent.CreateEnd(35, 7, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };

            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreatePunctuation<char>(2),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreatePunctuation<char>(4),
                StreamEvent.CreateStart(4, 'c'),
                StreamEvent.CreatePunctuation<char>(6),
                StreamEvent.CreateStart(7, 'd'),
                StreamEvent.CreatePunctuation<char>(20),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreatePunctuation<char>(22),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreatePunctuation<char>(34),
                StreamEvent.CreateEnd(34, 4, 'c'),
                StreamEvent.CreateEnd(35, 7, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(2), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void AlterDurationTest1Columnar()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(3, 'x'),
                StreamEvent.CreateEnd(5, 3, 'x'),
                StreamEvent.CreateStart(7, 'y'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(3, 8, 'x'),
                StreamEvent.CreateInterval(7, 12, 'y'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var result = str.AlterEventDuration(5).ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void PunctuationPolicy3Columnar()
        { // Simulates the way Stat does ingress (but with 80K batches, not 3...)
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void StreamCache1Columnar()
        {
            var input = Enumerable.Range(0, 1000000); // make sure it is enough to have more than one data batch
            ulong limit = 10000;
            var cachedStream = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e))
                .ToStreamable(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .Cache(limit);
            var streamResult = cachedStream
                .ToPayloadEnumerable();

            var c = (ulong)streamResult.Count();
            Assert.IsTrue(c == limit);
            cachedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void StreamCache2Columnar()
        {
            var limit = 10; // 00000;
            var input = Enumerable.Range(0, limit); // make sure it is enough to have more than one data batch
            var cachedStream = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(StreamEvent.MinSyncTime, e))
                .ToStreamable(DisorderPolicy.Throw())
                .Cache();
            var streamResult = cachedStream
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.Payload)
                .ToEnumerable();
            var foo = streamResult.Count();

            Assert.IsTrue(input.SequenceEqual(streamResult));
            cachedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals1Columnar()
        {
            var x = ComparerExpression<int>.Default.GetCompareExpr()
                .ExpressionEquals(ComparerExpression<int>.Default.GetCompareExpr());
            Assert.IsTrue(x);
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals2Columnar()
        {
            var x = new CompoundGroupKeyEqualityComparer<Empty, MyStruct2>(EqualityComparerExpression<Empty>.Default, EqualityComparerExpression<MyStruct2>.Default);
            var y = x.GetEqualsExpr().ExpressionEquals(x.GetEqualsExpr());
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals3Columnar()
        {
            var x1 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var x2 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var y = x1.GetCompareExpr().ExpressionEquals(x2.GetCompareExpr());
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals4Columnar()
        {
            var x1 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var x2 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);

            var xx1 = new CompoundGroupKeyComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x1, ComparerExpression<MyStruct2>.Default);
            var xx2 = new CompoundGroupKeyComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x2, ComparerExpression<MyStruct2>.Default);

            var y = xx1.GetCompareExpr().ExpressionEquals(xx2.GetCompareExpr());
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void CompoundGroupKeyEqualityComparerDefaultColumnar()
        {
            var xx1 = EqualityComparerExpression<CompoundGroupKey<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>>.Default;

            var x2 = new CompoundGroupKeyEqualityComparer<Empty, MyStruct2>(EqualityComparerExpression<Empty>.Default, EqualityComparerExpression<MyStruct2>.Default);
            var xx2 = new CompoundGroupKeyEqualityComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x2, EqualityComparerExpression<MyStruct2>.Default);

            var y = xx1.ExpressionEquals(xx2);
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void CompoundGroupKeyComparerDefaultColumnar()
        {
            var xx1 = ComparerExpression<CompoundGroupKey<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>>.Default;

            var x2 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var xx2 = new CompoundGroupKeyComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x2, ComparerExpression<MyStruct2>.Default);

            var y = xx1.ExpressionEquals(xx2);
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForIComparableColumnar()
        {
            var defaultComparerExpressionProvider = ComparerExpression<ClassImplementingIComparable>.Default;
            var defaultCompareExpression = defaultComparerExpressionProvider.GetCompareExpr();
            var compiledDefaultCompareExpression = defaultCompareExpression.Compile();
            var o = new ClassImplementingIComparable();
            var result = compiledDefaultCompareExpression(o, o);
            Assert.IsTrue(result == 3);

        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForGenericIComparerColumnar()
        {
            var defaultComparerExpressionProvider = ComparerExpression<ClassImplementingGenericIComparer>.Default;
            var defaultCompareExpression = defaultComparerExpressionProvider.GetCompareExpr();
            var compiledDefaultCompareExpression = defaultCompareExpression.Compile();
            var o = new ClassImplementingGenericIComparer();
            var result = compiledDefaultCompareExpression(o, o);
            Assert.IsTrue(result == 3);

        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForNonGenericIComparerColumnar()
        {
            var defaultComparerExpressionProvider = ComparerExpression<ClassImplementingNonGenericIComparer>.Default;
            var defaultCompareExpression = defaultComparerExpressionProvider.GetCompareExpr();
            var compiledDefaultCompareExpression = defaultCompareExpression.Compile();
            var o = new ClassImplementingNonGenericIComparer();
            var result = compiledDefaultCompareExpression(o, o);
            Assert.IsTrue(result == 3);

        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForAnonymousType1Columnar()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var f = GetDefaultComparerExpression(a);
            var g = f.GetCompareExpr();
            var h = g.Compile();
            var k = h(a, a);
            Assert.IsTrue(k == 0);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType1Columnar()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, a);
            Assert.IsTrue(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType2Columnar()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var b = new { Z = i + 1, A = (char)('a' + i), W = "abc" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, b);
            Assert.IsFalse(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType3Columnar()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var b = new { Z = i, A = (char)('b' + i), W = "abc" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, b);
            Assert.IsFalse(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType4Columnar()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var b = new { Z = i, A = (char)('a' + i), W = "abcd" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, b);
            Assert.IsFalse(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void NaryMulticast1Columnar()
        {
            var enumerable = Enumerable.Range(0, 10000).ToList();

            var stream = enumerable.ToStreamable();
            var multiStream = stream.Multicast(3);

            var output1 = multiStream[0].Where(e => e % 3 == 0);
            var output2 = multiStream[1].Where(e => e % 3 == 1);
            var output3 = multiStream[2].Where(e => e % 3 == 2);
            var union = output1.Union(output2).Union(output3);

            var output = union.ToPayloadEnumerable().ToList();
            output.Sort();
            Assert.IsTrue(enumerable.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void NaryMulticastContainer1Columnar()
        {
            var outputList = new List<int>();

            // Inputs
            var container = new QueryContainer();
            Stream state = null;

            // Input data
            var enumerable = Enumerable.Range(0, 10000).ToList();
            var inputObservable = enumerable.ToObservable().Select(e => StreamEvent.CreateStart(0, e));

            // Query start
            var stream = container.RegisterInput(inputObservable);
            var multiStream = stream.Multicast(3);

            var output1 = multiStream[0].Where(e => e % 3 == 0);
            var output2 = multiStream[1].Where(e => e % 3 == 1);
            var output3 = multiStream[2].Where(e => e % 3 == 2);
            var union = output1.Union(output2).Union(output3);

            var outputObservable = container.RegisterOutput(union);

            // Test the output
            var outputAsync = outputObservable.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
            container.Restore(state);
            outputAsync.Wait();

            outputList.Sort();
            Assert.IsTrue(enumerable.SequenceEqual(outputList));
        }

        [TestMethod, TestCategory("Gated")]
        public void NaryMulticastContainerErrorColumnar()
        {
            // Inputs
            var container = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var enumerable = Enumerable.Range(0, 10000).ToList();
            var inputObservable = enumerable.ToObservable().Select(e => StreamEvent.CreateStart(0, e));

            // Query start
            var stream = container.RegisterInput(inputObservable);
            var multiStream = stream.Multicast(3);

            var output1 = multiStream[0].Where(e => e % 3 == 0);
            var output2 = multiStream[1].Where(e => e % 3 == 1);
            var output3 = multiStream[2].Where(e => e % 3 == 2);
            var union = output1.Union(output2).Union(output3);

            var outputObservable = container.RegisterOutput(union);

            // Validate that attempting to start the query before subscriptions will result in an error.
            bool foundException = false;
            try
            {
                container.Restore(state);
            }
            catch (Exception)
            {
                foundException = true;
            }

            Assert.IsTrue(foundException, "Expected an exception.");
        }

        [TestMethod, TestCategory("Gated")]
        public void StreamSortColumnar()
        {
            var input = Enumerable.Range(0, 20).Select(i => (char)('a' + i));
            var reversedInput = input.Reverse();
            var str = reversedInput.ToStatStreamable();
            var sortedStream = str.Sort(c => c);
            var result = sortedStream.ToStreamEventObservable().Where(se => se.IsData).Select(se => se.Payload).ToEnumerable();
            Assert.IsTrue(input.SequenceEqual(result));
            sortedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void SortModifiedStreamColumnar()
        {
            // The Sort method on a stream uses an observable cache. The cache
            // had been putting all rows from the messages into the list to be sorted,
            // even if it was an invalid row.
            var input = Enumerable.Range(0, 20).Select(i => (char)('a' + i));
            var str = input.ToStatStreamable();
            var str2 = str.Where(r => r != 'b');
            var sortedStream = str2.Sort(c => c);
            var result = sortedStream.ToStreamEventObservable().Where(se => se.IsData).Select(se => se.Payload).ToEnumerable();
            Assert.IsTrue(input.Count() == result.Count() + 1); // just make sure deleted element isn't counted anymore
            sortedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void GroupStreamableCodeGenTest1Columnar()
        {
            var input1 = new[]
            {
                StreamEvent.CreateInterval(100, 101, 13),
                StreamEvent.CreateInterval(100, 110, 11),
                StreamEvent.CreateInterval(101, 105, 12),
                StreamEvent.CreateInterval(102, 112, 21),
                StreamEvent.CreateInterval(109, 112, 14),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };

            var input2 = new[]
            {
                StreamEvent.CreateInterval(100, 110, 'd'),
                StreamEvent.CreateInterval(101, 103, 'b'),
                StreamEvent.CreateInterval(106, 108, 'b'),
                StreamEvent.CreateInterval(106, 109, 'b'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime)
            };

            var inputStream1 = input1.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var inputStream2 = input2.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var outputStream = inputStream1.WhereNotExists(
                inputStream2,
                l => l / 10,
                r => r - 'a');

            var expected = new[]
            {
                StreamEvent.CreateStart(100, 11),
                StreamEvent.CreateEnd(101, 100, 11),
                StreamEvent.CreateStart(103, 11),
                StreamEvent.CreateEnd(106, 103, 11),
                StreamEvent.CreateInterval(109, 110, 11),
                StreamEvent.CreateInterval(103, 105, 12),
                StreamEvent.CreateInterval(100, 101, 13),
                StreamEvent.CreateInterval(109, 112, 14),
                StreamEvent.CreateStart(102, 21),
                StreamEvent.CreateEnd(112, 102, 21),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };
            var outputEnumerable = outputStream.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void CountColumnar1Columnar()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<ulong>[]
            {
                StreamEvent.CreateStart<ulong>(StreamEvent.MinSyncTime, 3),
                StreamEvent.CreatePunctuation<ulong>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateEnd<ulong>(StreamEvent.MinSyncTime + 1, StreamEvent.MinSyncTime, 3),
                StreamEvent.CreateStart<ulong>(StreamEvent.MinSyncTime + 1, 6),
                StreamEvent.CreatePunctuation<ulong>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateEnd<ulong>(StreamEvent.MinSyncTime + 2, StreamEvent.MinSyncTime + 1, 6),
                StreamEvent.CreateStart<ulong>(StreamEvent.MinSyncTime + 2, 9),
                StreamEvent.CreatePunctuation<ulong>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None);
            var count = str.Count();
            var outputEnumerable = count.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageColumnar1Columnar() // tests codegen for Snapshot_noecq
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),      // 97
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),      // 98
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),      // 99  running average = 98

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),  // 100
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),  // 101
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),  // 102 running average = 99.5

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),  // 103
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),  // 104
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),  // 105 running average = 101
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<double>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 1, StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 2, StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 101.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None).SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime);
            var avg = str.Average(c => (double)c);
            var outputEnumerable = avg.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageColumnar2Columnar() // tests codegen for Snapshot_q
        {
            var zero = StreamEvent.MinSyncTime;
            var one = StreamEvent.MinSyncTime + 1;
            var two = StreamEvent.MinSyncTime + 2;
            var three = StreamEvent.MinSyncTime + 3;
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(zero, one, 'a'),
                StreamEvent.CreateInterval(zero, one, 'b'),
                StreamEvent.CreateInterval(zero, one, 'c'),
                StreamEvent.CreateInterval(one, two, 'd'),
                StreamEvent.CreateInterval(one, two, 'e'),
                StreamEvent.CreateInterval(one, two, 'f'),
                StreamEvent.CreateInterval(two, three, 'g'),
                StreamEvent.CreateInterval(two, three, 'h'),
                StreamEvent.CreateInterval(two, three, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<double>[]
            {
                StreamEvent.CreateInterval(zero, one, 98.0),
                StreamEvent.CreatePunctuation<double>(one),
                StreamEvent.CreateInterval(one, two, 101.0),
                StreamEvent.CreatePunctuation<double>(two),
                StreamEvent.CreateInterval(two, three, 104.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None).SetProperty().IsConstantDuration(true, 1);
            var avg = str.Average(c => (double)c);
            var outputEnumerable = avg.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageColumnar3Columnar() // tests codegen for Snapshot_pq
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),      // 97
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),      // 98
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),      // 99  running average = 98

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),  // 100
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),  // 101
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),  // 102 running average = 99.5

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),  // 103
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),  // 104
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),  // 105 running average = 101
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<double>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 1, StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 2, StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 101.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None);
            var avg = str.Average(c => (double)c);
            var outputEnumerable = avg.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SumColumnar1Columnar() // make sure expression is properly parenthesized in codegen
        {
            var input = Enumerable.Range(0, 100);
            Expression<Func<int, int>> query = e => e & 0x7;

            var streamResult = input
                .ToStreamable()
                .Sum(query)
                .ToPayloadEnumerable()
                .Last();

            var expected = input.Sum(query.Compile());
            Assert.IsTrue(expected == streamResult);

        }

        [TestMethod, TestCategory("Gated")]
        public void MultiAggregate1Columnar() // test codegen for multi-aggregates
        {
            var input = Enumerable.Range(0, 100).Select(i => new MyStruct { field1 = i, field2 = 0.0, field3 = Guid.Empty, });

            var streamResult = input
                .ToStreamable()
                .Aggregate(a => a.Sum(x => x.field1), a => a.Count(), (s, c) => new MyStruct { field1 = s + ((int)c) })
                .ToPayloadEnumerable()
                ;

            Assert.IsTrue(streamResult.Count() == 1);

            Assert.IsTrue(streamResult.First().field1 == 5050);

        }

        [TestMethod, TestCategory("Gated")]
        public void MultiAggregate2Columnar() // test codegen for multi-aggregates with anonymous type
        {
            var input = Enumerable.Range(0, 100).Select(i => new MyStruct { field1 = i, field2 = 0.0, field3 = Guid.Empty, });

            var streamResult = input
                .ToStreamable()
                .Aggregate(a => a.Sum(x => x.field1), a => a.Count(), (s, c) => new { field1 = s, field2 = c })
                .ToPayloadEnumerable()
                ;

            Assert.IsTrue(streamResult.Count() == 1);

            var f = streamResult.First();
            Assert.IsTrue(f.field1 == 4950 && f.field2 == 100);

        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest1Columnar()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(24, 3, 'c'),
                StreamEvent.CreateEnd(26, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateInterval(2, 22, 'b'),
                StreamEvent.CreateInterval(3, 24, 'c'),
                StreamEvent.CreateInterval(4, 26, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Cache(0, false, true);
            var result = str.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest2Columnar()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(24, 3, 'c'),
                StreamEvent.CreateEnd(26, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateInterval(3, 24, 'c'),
                StreamEvent.CreateInterval(4, 26, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Cache(0, false, true);
            var result = str.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest3Columnar()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(24, 4, 'd'),
                StreamEvent.CreateEnd(26, 3, 'c'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateInterval(2, 22, 'b'),
                StreamEvent.CreateInterval(3, 26, 'c'),
                StreamEvent.CreateInterval(4, 24, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Cache(0, false, true);
            var result = str.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void BinaryUnionTest1Columnar()
        {
            var input1 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(2, 20, 'a'),
                StreamEvent.CreateStart(4, 'b'),
                StreamEvent.CreateStart(6, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var input2 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(3, 20, 'a'),
                StreamEvent.CreateStart(5, 'b'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(9, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };

            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(2, 20, 'a'),
                StreamEvent.CreateInterval(3, 20, 'a'),
                StreamEvent.CreateStart(4, 'b'),
                StreamEvent.CreateStart(5, 'b'),
                StreamEvent.CreateStart(6, 'c'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreateStart(9, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str1 = input1.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null).Cache();
            var str2 = input2.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null).Cache();
            var resultEnum = str1.Union(str2).ToStreamEventObservable().ToEnumerable();
            var result = resultEnum.ToList();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str1.Dispose();
            str2.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void BinaryUnionTest2Columnar()
        {
            var input1 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var input2 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(5, 20, 'a'),
                StreamEvent.CreateStart(6, 'b'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };

            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateInterval(5, 20, 'a'),
                StreamEvent.CreateStart(6, 'b'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str1 = input1.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation).Cache();
            var str2 = input2.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation).Cache();
            var result = str1.Union(str2).ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str1.Dispose();
            str2.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void DeterministicUnionTestColumnar()
        {
            int count = 100;
            DateTime now = DateTime.UtcNow;

            var input = new List<StreamEvent<int>> { StreamEvent.CreatePunctuation<int>(now.Ticks) };

            // Add right events.
            for (int i = 0; i < count; ++i)
                input.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i + 1));

            // Cti forces right batch to go through before reaching Config.DataBatchSize.
            input.Add(StreamEvent.CreatePunctuation<int>(now.Ticks));

            // Add left events.
            for (int i = 0; i < count; ++i)
                input.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i));
            input.Add(StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime));

            // Expect left before right even though right went through first.
            var expectedOutput = new List<StreamEvent<int>> { StreamEvent.CreatePunctuation<int>(now.Ticks) };
            for (int i = 0; i < count; i++)
                expectedOutput.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i));
            for (int i = 0; i < count; i++)
                expectedOutput.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i + 1));
            expectedOutput.Add(StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime));

            var inputStreams = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Multicast(2);

            var left = inputStreams[0].Where(e => e % 2 == 0);
            var right = inputStreams[1].Where(e => e % 2 == 1);

            var result = left
                .Union(right)
                .ToStreamEventObservable()
                .ToEnumerable()
                .ToList();

            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest4Columnar()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateStart(6, 'b'),
                StreamEvent.CreateEnd(10, 6, 'b'),
                StreamEvent.CreateStart(11, 'c'),
                StreamEvent.CreateEnd(15, 11, 'c'),
                StreamEvent.CreateStart(16, 'd'),
                StreamEvent.CreateEnd(20, 16, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 5, 'a'),
                StreamEvent.CreateInterval(6, 10, 'b'),
                StreamEvent.CreateInterval(11, 15, 'c'),
                StreamEvent.CreateInterval(16, 20, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation);
            var result = str.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void AggregateTest1Columnar()
        {
            long threeSecondTicks = TimeSpan.FromSeconds(3).Ticks;

            var stream = Events().ToEvents(e => e.Vs.Ticks, e => e.Vs.Ticks + threeSecondTicks)
                                 .ToObservable()
                                 .ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None());
            var expectedResult = new StreamEvent<ResultEvent>[]
            {
                StreamEvent.CreateStart(633979008010000000, new ResultEvent { Key = 1, Cnt = 1 }),
                StreamEvent.CreateEnd(633979008020000000, 633979008010000000, new ResultEvent { Key = 1, Cnt = 1 }),
                StreamEvent.CreateStart(633979008020000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateEnd(633979008030000000, 633979008020000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateStart(633979008030000000, new ResultEvent { Key = 1, Cnt = 3 }),
                StreamEvent.CreateEnd(633979008040000000, 633979008030000000, new ResultEvent { Key = 1, Cnt = 3 }),
                StreamEvent.CreateStart(633979008040000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateEnd(633979008050000000, 633979008040000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateStart(633979008050000000, new ResultEvent { Key = 1, Cnt = 1 }),
                StreamEvent.CreateEnd(633979008060000000, 633979008050000000, new ResultEvent { Key = 1, Cnt = 1 }),
            };
            var counts = stream.GroupApply(
                                    g => g.Key,
                                    e => e.Count(),
                                    (g, e) => new ResultEvent { Key = g.Key, Cnt = e })
                               .ToStreamEventObservable()
                               .ToEnumerable()
                               .Where(e => e.IsData && e.Payload.Key == 1);
            foreach (var x in counts)
            {
                Console.WriteLine("{0}, {1}", x.SyncTime, x.OtherTime);
            }
            Assert.IsTrue(expectedResult.SequenceEqual(counts));
        }

        public IEnumerable<Event> Events()
        {
            var vs = new DateTime(2010, 1, 1, 0, 00, 00, DateTimeKind.Utc);
            var rand = new Random(0);
            int eventCnt = 5;
            for (int i = 0; i < eventCnt; i++)
            {
                yield return new Event { Vs = vs, V = rand.Next(10), Key = rand.Next(3) };
                vs = vs.AddSeconds(1);
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void CodeGenWithFieldOfGenericTypeColumnar()
        {
            // Taken from the PerformanceTest sample.
            // The GroupApply creates batches where the key is a generic type and codegen was doing a type cast without making sure the type's name was valid C# syntax
            var numEventsPerTumble = 2000;
            var tumblingWindowDataset =
                Observable.Range(0, 100)
                    .Select(e => StreamEvent.CreateInterval(((long)e / numEventsPerTumble) * numEventsPerTumble, ((long)e / numEventsPerTumble) * numEventsPerTumble + numEventsPerTumble, new Payload { field1 = 0, field2 = 0 }))
                    .ToStreamable()
                    .SetProperty().IsConstantDuration(true, numEventsPerTumble)
                    .SetProperty().IsSnapshotSorted(true, e => e.field1);
            var result = tumblingWindowDataset.GroupApply(e => e.field2, str => str.Count(), (g, c) => new { Key = g, Count = c });
            var a = result.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(a.Length == 3); // just make sure it doesn't crash
        }

        [TestMethod, TestCategory("Gated")]
        public void StringSerializationColumnar()
        {
            var rand = new Random(0);
            var pool = MemoryManager.GetColumnPool<string>();

            for (int x = 0; x < 100; x++)
            {
                pool.Get(out ColumnBatch<string> inputStr);

                var toss1 = rand.NextDouble();
                inputStr.UsedLength = toss1 < 0.1 ? 0 : rand.Next(Config.DataBatchSize);

                for (int i = 0; i < inputStr.UsedLength; i++)
                {
                    var toss = rand.NextDouble();
                    inputStr.col[i] = toss < 0.2 ? string.Empty : (toss < 0.4 ? null : Guid.NewGuid().ToString());
                    if (x == 0) inputStr.col[i] = null;
                    if (x == 1) inputStr.col[i] = string.Empty;
                }

                StateSerializer<ColumnBatch<string>> s = StreamableSerializer.Create<ColumnBatch<string>>(new SerializerSettings { });
                var ms = new MemoryStream { Position = 0 };

                s.Serialize(ms, inputStr);
                ms.Position = 0;
                ColumnBatch<string> resultStr = s.Deserialize(ms);

                Assert.IsTrue(resultStr.UsedLength == inputStr.UsedLength);

                for (int j = 0; j < inputStr.UsedLength; j++)
                {
                    Assert.IsTrue(inputStr.col[j] == resultStr.col[j]);
                }
                resultStr.ReturnClear();
                inputStr.ReturnClear();
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void CharArraySerializationColumnar()
        {
            using (var modifier = new ConfigModifier().SerializationCompressionLevel(SerializationCompressionLevel.CharArrayToUTF8).Modify())
            {
                var rand = new Random(0);

                for (int x = 0; x < 5; x++)
                {
                    var inputStr = new MultiString();

                    var toss1 = rand.NextDouble();
                    var usedLength = 1 + rand.Next(Config.DataBatchSize - 1);

                    for (int i = 0; i < usedLength; i++)
                    {
                        var toss = rand.NextDouble();
                        string str = toss < 0.2 ? string.Empty : Guid.NewGuid().ToString();
                        if (x == 0) str = string.Empty;
                        inputStr.AddString(str);
                    }

                    inputStr.Seal();

                    StateSerializer<MultiString> s = StreamableSerializer.Create<MultiString>(new SerializerSettings { });
                    var ms = new MemoryStream { Position = 0 };

                    s.Serialize(ms, inputStr);
                    ms.Position = 0;
                    var resultStr = s.Deserialize(ms);

                    Assert.IsTrue(resultStr.Count == inputStr.Count);

                    for (int j = 0; j < inputStr.Count; j++)
                    {
                        Assert.IsTrue(inputStr[j] == resultStr[j]);
                    }
                    resultStr.Dispose();
                    inputStr.Dispose();
                }
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void MultipleAggregatesWithCompiledDictionariesColumnar()
        {
            IEnumerable<StreamEvent<StreamScope_InputEvent0>> bacon = Array.Empty<StreamEvent<StreamScope_InputEvent0>>();
            IObservableIngressStreamable<StreamScope_InputEvent0> input = bacon.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var result =
                input
                .GroupAggregate(
                    s => new { s.OutputTs, s.DelayInMilliseconds, s.UserId },
                    s => s.Count(),
                    s => s.Min(r => r.Duration),
                    s => s.Max(r => r.Duration),
                    s => s.Average(r => r.Duration),
                    (key, cnt, minDuration, maxDuration, avgDuration) => new StreamScope_OutputEvent0()
                    {
                        OutputTs = key.Key.OutputTs,
                        DelayInMilliseconds = key.Key.DelayInMilliseconds,
                        UserId = key.Key.UserId,
                        Cnt = (long)cnt,
                        MinDuration = minDuration,
                        MaxDuration = maxDuration,
                        AvgDuration = avgDuration,
                    }).ToStreamEventObservable();

            var b = result;
        }

        [TestMethod, TestCategory("Gated")]
        public void PayloadWithFieldOfNestedType01Columnar()
        {
            var input = Enumerable
                .Range(0, NumEvents)
                .Select(i => new PayloadWithFieldOfNestedType1(i))
                ;
            var streamResult = input
                .ToStreamable()
                .Where(p => p.nt.x % 2 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var linqResult = input.Where(p => p.nt.x % 2 == 0);
            Assert.IsTrue(Enumerable.Zip(linqResult, a, (p1, p2) => p1.nt.x == p2.nt.x).All(p => p));
        }
        [TestMethod, TestCategory("Gated")]
        public void PayloadWithFieldOfNestedType02Columnar()
        {
            var input = Enumerable
                .Range(0, NumEvents)
                .Select(i => new PayloadWithFieldOfNestedType2(i))
                ;
            var streamResult = input
                .ToStreamable()
                .Where(p => p.nt.x % 2 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var linqResult = input.Where(p => p.nt.x % 2 == 0);
            Assert.IsTrue(Enumerable.Zip(linqResult, a, (p1, p2) => p1.nt.x == p2.nt.x).All(p => p));
        }
    }

    [TestClass]
    public class SimpleTestsColumnarFloating : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public SimpleTestsColumnarFloating() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .AllowFloatingReorderPolicy(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        // Total number of input events
        private const int NumEvents = 1000;
        private readonly IEnumerable<MyStruct2> enumerable = Enumerable.Range(0, NumEvents)
            .Select(e =>
                new MyStruct2 { field1 = e, field2 = new MyString(Convert.ToString("string" + e)), field3 = new NestedStruct { nestedField = e } });

        private static IComparerExpression<T> GetDefaultComparerExpression<T>(T t) => ComparerExpression<T>.Default;

        private static IEqualityComparerExpression<T> GetDefaultEqualityComparerExpression<T>(T t) => EqualityComparerExpression<T>.Default;

        public static bool MyIsEqual(List<RankedEvent<char>> a, List<RankedEvent<char>> b)
        {
            if (a.Count != b.Count) return false;

            for (int j = 0; j < a.Count; j++)
            {
                if (!a[j].Equals(b[j])) return false;
            }

            return true;
        }

        public static int MyGetHashCode(List<RankedEvent<char>> a)
        {
            int ret = 0;
            foreach (var l in a)
            {
                ret ^= l.GetHashCode();
            }
            return ret;
        }

        private void TestWhere(Expression<Func<MyStruct2, bool>> predicate, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhere(predicate, disableFusion));

        private void TestSelect<TResult>(Expression<Func<MyStruct2, TResult>> function, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestSelect(function, disableFusion));

        public void TestWhereSelect<U>(Expression<Func<MyStruct2, bool>> wherePredicate, Expression<Func<MyStruct2, U>> selectFunction, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhereSelect(wherePredicate, selectFunction, disableFusion));

        [TestMethod, TestCategory("Gated")]
        public void Where1ColumnarFloating()
            => TestWhere(e => e.field2.mystring.Contains("string"));

        [TestMethod, TestCategory("Gated")]
        public void Where2ColumnarFloating()
            => TestWhere(e => e.field2.mystring.Equals("string0"));

        [TestMethod, TestCategory("Gated")]
        public void Where3ColumnarFloating()
            => TestWhere(e => e.field2.mystring.Equals("string0"));

        [TestMethod, TestCategory("Gated")]
        public void Where4ColumnarFloating()
            => TestWhere(e => e.field3.nestedField == 0);

        [TestMethod, TestCategory("Gated")]
        public void Where5ColumnarFloating()
            => TestWhere(e => true);

        [TestMethod, TestCategory("Gated")]
        public void Where6ColumnarFloating()
            => TestWhere(e => false);

        [TestMethod, TestCategory("Gated")]
        public void Where7ColumnarFloating()
            => TestWhere(e => e.field3.nestedField == 0 || e.field3.nestedField == 1);

        [TestMethod, TestCategory("Gated")]
        public void Where8ColumnarFloating()
            => TestWhere(e => string.Compare(e.field2.mystring, "string0") == 0);

        [TestMethod, TestCategory("Gated")]
        public void Where9ColumnarFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Where(v => v % 2 == 0)
                .Where(v => v % 3 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            Assert.IsTrue(input.Where(v => v % 6 == 0).SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereWithFailedTransformationColumnarFloating() // stays columnar, but causes payload to be reconstituted
            => Enumerable.Range(0, 100)
                .Select(i => new StructTuple<int, char> { Item1 = i, Item2 = i.ToString()[0], })
                .TestWhere(r => r.ToString().Length > 3);

        [TestMethod, TestCategory("Gated")]
        public void Select1ColumnarFloating()
            => TestSelect(e => e.field1);

        [TestMethod, TestCategory("Gated")]
        public void Select2ColumnarFloating()
            => TestSelect(e => e.field2);

        [TestMethod, TestCategory("Gated")]
        public void Select3ColumnarFloating()
            => TestSelect(e => e.field3);

        [TestMethod, TestCategory("Gated")]
        public void Select4ColumnarFloating()
            => TestSelect(e => new NestedStruct { nestedField = e.field1 });

        [TestMethod, TestCategory("Gated")]
        public void Select5ColumnarFloating()
            => TestSelect(e => e.doubleField);

        [TestMethod, TestCategory("Gated")]
        public void Select6ColumnarFloating()
            => TestSelect(e => ((ulong)e.field1));

        // Tests to make sure that we ref count correctly by doing two pointer swings to the same column.
        [TestMethod, TestCategory("Gated")]
        public void Select7ColumnarFloating()
            => TestSelect(e => new { A = e.field1, B = e.field1, });

        // Tests whether an expression that cannot be columnerized causes code gen to fall back to row-oriented.
        // This is just a degenerate case where the expression is *always* null.
        [TestMethod, TestCategory("Gated")]
        public void Select8ColumnarFloating()
            => TestSelect<ClassWithOnlyAutoProps>(e => null);

        [TestMethod, TestCategory("Gated")]
        public void SelectFromUlongToLongColumnarFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(v => (long)v)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, longInteger) => (long)integer == longInteger).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeColumnarFloating()
            => TestSelect(e => new { anonfield1 = e.field1, x = 3 });

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedColumnarFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(v => new { X = v })
                .Select(e => new { Y = e.X })
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.Y).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedNoOpColumnarFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(v => new { X = v })
                .Select(e => e)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.X).All(p => p));
        }

        // The point of this test is to have an anonymous type with a "field" in it
        // whose type does not have an overload for it in MemoryPool<TKey, TPayload>.Get(out ColumnBatch<>).
        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeWithFloatFieldColumnarFloating()
            => TestSelect(e => new { floatField = e.field1 * 3.0, });

        [TestMethod, TestCategory("Gated")]
        public void SelectWhereAnonymousTypeColumnarFloating()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithAutoPropsColumnarFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithAutoPropsColumnarFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithOnlyAutoPropsColumnarFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithOnlyAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithOnlyAutoPropsColumnarFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_SelectWhereColumnarFloating()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_GroupApplyColumnarFloating()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .GroupApply(e => e.field1, str => str.Count(), (g, cnt) => new MyStruct { field1 = (int)cnt, })
                .Where(e => e.field2 >= 0.0)
                .ToPayloadEnumerable();
            Assert.IsTrue(streamResult.Count() == this.enumerable.Count());
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectWithCtorCallColumnarFloating()
            => TestSelect(e => new StructWithCtor(e.field1));

        [TestMethod, TestCategory("Gated")]
        public void SelectMany01ColumnarFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany02ColumnarFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany03ColumnarFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany04ColumnarFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart01ColumnarFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select((l, e) => e + ((int)l))
                ;
            var expected = input
                .Select(e => e + e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart02ColumnarFloating()
        {
            // tests simple parameter selection with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select((l, e) => l)
                ;
            var expected = input
                .Select(e => (long)e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey01ColumnarFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectByKey((k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey02ColumnarFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectByKey((k, e) => k)
                ;
            var expected = input
                .Select(e => Empty.Default)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey03ColumnarFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectByKey((s, k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e + (int)s, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e + e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey04ColumnarFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => (int?)(e + 1))
                .SelectByKey((k, e) => new object[] { e, })
                ;
            var expected = input
                .Select(e => new object[] { (int?)(e + 1), })
                .ToArray()
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.Length == output.Length);
            for (int i = 0; i < expected.Length; i++)
            {
                var e = (int?)(expected[i][0]);
                var o = (int?)(output[i][0]);
                Assert.IsTrue(e.HasValue && o.HasValue && (e.Value == o.Value));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStart01ColumnarFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany((l, e) => Enumerable.Repeat(33, e + ((int)l)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + e))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyKey01ColumnarFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectManyByKey((k, e) => Enumerable.Repeat(33, e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStartKey01ColumnarFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectManyByKey((ts, k, e) => Enumerable.Repeat(33 + ((int)ts), e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33 + e, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereSelect1ColumnarFloating()
            => TestWhereSelect(e => true, e => e.field1);

        [TestMethod, TestCategory("Gated")]
        public void SelectStructColumnarFloating()
        {
            using (var modifier = new ConfigModifier().ForceRowBasedExecution(true).Modify())
            {
                var savedForceRowBasedExecution = Config.ForceRowBasedExecution;
                Config.ForceRowBasedExecution = true;

                var input = Enumerable.Range(0, 100);
                Expression<Func<int, MyStruct>> lambda = e => new MyStruct { field1 = e, field2 = e + 0.5, field3 = default };

                var streamResult = input
                    .ToStreamable()
                    .ShiftEventLifetime(0)
                    .Select(lambda)
                    .ToPayloadEnumerable();
                var linqResult = input
                    .Select(lambda.Compile());

                var query = input.Where(e => true);
                Assert.IsTrue(linqResult.SequenceEqual(streamResult));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantNoOpIntervalsColumnarFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 5, 'a'),
                StreamEvent.CreateInterval(2, 6, 'b'),
                StreamEvent.CreateInterval(3, 7, 'c'),
                StreamEvent.CreateInterval(4, 8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 5, 'a'),
                StreamEvent.CreateInterval(2, 6, 'b'),
                StreamEvent.CreateInterval(3, 7, 'c'),
                StreamEvent.CreateInterval(4, 8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(10)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantNoOpEdgesColumnarFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateEnd(6, 2, 'b'),
                StreamEvent.CreateEnd(7, 3, 'c'),
                StreamEvent.CreateEnd(8, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateEnd(6, 2, 'b'),
                StreamEvent.CreateEnd(7, 3, 'c'),
                StreamEvent.CreateEnd(8, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(10)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantClippedIntervalsColumnarFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 15, 'a'),
                StreamEvent.CreateInterval(2, 16, 'b'),
                StreamEvent.CreateInterval(3, 17, 'c'),
                StreamEvent.CreateInterval(4, 18, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 11, 'a'),
                StreamEvent.CreateInterval(2, 12, 'b'),
                StreamEvent.CreateInterval(3, 13, 'c'),
                StreamEvent.CreateInterval(4, 14, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(10)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantClippedEdgesColumnarFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateEnd(6, 2, 'b'),
                StreamEvent.CreateEnd(7, 3, 'c'),
                StreamEvent.CreateEnd(8, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateEnd(3, 1, 'a'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateEnd(4, 2, 'b'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 3, 'c'),
                StreamEvent.CreateEnd(6, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(2)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void PunctuationPolicy2ColumnarFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(4, 'c'),
                StreamEvent.CreateStart(7, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(34, 4, 'c'),
                StreamEvent.CreateEnd(35, 7, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };

            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(4, 'c'),
                StreamEvent.CreateStart(7, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(34, 4, 'c'),
                StreamEvent.CreateEnd(35, 7, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(2), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void AlterDurationTest1ColumnarFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(3, 'x'),
                StreamEvent.CreateEnd(5, 3, 'x'),
                StreamEvent.CreateStart(7, 'y'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(3, 8, 'x'),
                StreamEvent.CreateInterval(7, 12, 'y'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var result = str.AlterEventDuration(5).ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void PunctuationPolicy3ColumnarFloating()
        { // Simulates the way Stat does ingress (but with 80K batches, not 3...)
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void StreamCache1ColumnarFloating()
        {
            var input = Enumerable.Range(0, 1000000); // make sure it is enough to have more than one data batch
            ulong limit = 10000;
            var cachedStream = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e))
                .ToStreamable(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .Cache(limit);
            var streamResult = cachedStream
                .ToPayloadEnumerable();

            var c = (ulong)streamResult.Count();
            Assert.IsTrue(c == limit);
            cachedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void StreamCache2ColumnarFloating()
        {
            var limit = 10; // 00000;
            var input = Enumerable.Range(0, limit); // make sure it is enough to have more than one data batch
            var cachedStream = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(StreamEvent.MinSyncTime, e))
                .ToStreamable(DisorderPolicy.Throw())
                .Cache();
            var streamResult = cachedStream
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.Payload)
                .ToEnumerable();
            var foo = streamResult.Count();

            Assert.IsTrue(input.SequenceEqual(streamResult));
            cachedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals1ColumnarFloating()
        {
            var x = ComparerExpression<int>.Default.GetCompareExpr()
                .ExpressionEquals(ComparerExpression<int>.Default.GetCompareExpr());
            Assert.IsTrue(x);
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals2ColumnarFloating()
        {
            var x = new CompoundGroupKeyEqualityComparer<Empty, MyStruct2>(EqualityComparerExpression<Empty>.Default, EqualityComparerExpression<MyStruct2>.Default);
            var y = x.GetEqualsExpr().ExpressionEquals(x.GetEqualsExpr());
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals3ColumnarFloating()
        {
            var x1 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var x2 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var y = x1.GetCompareExpr().ExpressionEquals(x2.GetCompareExpr());
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals4ColumnarFloating()
        {
            var x1 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var x2 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);

            var xx1 = new CompoundGroupKeyComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x1, ComparerExpression<MyStruct2>.Default);
            var xx2 = new CompoundGroupKeyComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x2, ComparerExpression<MyStruct2>.Default);

            var y = xx1.GetCompareExpr().ExpressionEquals(xx2.GetCompareExpr());
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void CompoundGroupKeyEqualityComparerDefaultColumnarFloating()
        {
            var xx1 = EqualityComparerExpression<CompoundGroupKey<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>>.Default;

            var x2 = new CompoundGroupKeyEqualityComparer<Empty, MyStruct2>(EqualityComparerExpression<Empty>.Default, EqualityComparerExpression<MyStruct2>.Default);
            var xx2 = new CompoundGroupKeyEqualityComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x2, EqualityComparerExpression<MyStruct2>.Default);

            var y = xx1.ExpressionEquals(xx2);
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void CompoundGroupKeyComparerDefaultColumnarFloating()
        {
            var xx1 = ComparerExpression<CompoundGroupKey<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>>.Default;

            var x2 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var xx2 = new CompoundGroupKeyComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x2, ComparerExpression<MyStruct2>.Default);

            var y = xx1.ExpressionEquals(xx2);
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForIComparableColumnarFloating()
        {
            var defaultComparerExpressionProvider = ComparerExpression<ClassImplementingIComparable>.Default;
            var defaultCompareExpression = defaultComparerExpressionProvider.GetCompareExpr();
            var compiledDefaultCompareExpression = defaultCompareExpression.Compile();
            var o = new ClassImplementingIComparable();
            var result = compiledDefaultCompareExpression(o, o);
            Assert.IsTrue(result == 3);

        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForGenericIComparerColumnarFloating()
        {
            var defaultComparerExpressionProvider = ComparerExpression<ClassImplementingGenericIComparer>.Default;
            var defaultCompareExpression = defaultComparerExpressionProvider.GetCompareExpr();
            var compiledDefaultCompareExpression = defaultCompareExpression.Compile();
            var o = new ClassImplementingGenericIComparer();
            var result = compiledDefaultCompareExpression(o, o);
            Assert.IsTrue(result == 3);

        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForNonGenericIComparerColumnarFloating()
        {
            var defaultComparerExpressionProvider = ComparerExpression<ClassImplementingNonGenericIComparer>.Default;
            var defaultCompareExpression = defaultComparerExpressionProvider.GetCompareExpr();
            var compiledDefaultCompareExpression = defaultCompareExpression.Compile();
            var o = new ClassImplementingNonGenericIComparer();
            var result = compiledDefaultCompareExpression(o, o);
            Assert.IsTrue(result == 3);

        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForAnonymousType1ColumnarFloating()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var f = GetDefaultComparerExpression(a);
            var g = f.GetCompareExpr();
            var h = g.Compile();
            var k = h(a, a);
            Assert.IsTrue(k == 0);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType1ColumnarFloating()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, a);
            Assert.IsTrue(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType2ColumnarFloating()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var b = new { Z = i + 1, A = (char)('a' + i), W = "abc" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, b);
            Assert.IsFalse(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType3ColumnarFloating()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var b = new { Z = i, A = (char)('b' + i), W = "abc" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, b);
            Assert.IsFalse(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType4ColumnarFloating()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var b = new { Z = i, A = (char)('a' + i), W = "abcd" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, b);
            Assert.IsFalse(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void NaryMulticast1ColumnarFloating()
        {
            var enumerable = Enumerable.Range(0, 10000).ToList();

            var stream = enumerable.ToStreamable();
            var multiStream = stream.Multicast(3);

            var output1 = multiStream[0].Where(e => e % 3 == 0);
            var output2 = multiStream[1].Where(e => e % 3 == 1);
            var output3 = multiStream[2].Where(e => e % 3 == 2);
            var union = output1.Union(output2).Union(output3);

            var output = union.ToPayloadEnumerable().ToList();
            output.Sort();
            Assert.IsTrue(enumerable.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void NaryMulticastContainer1ColumnarFloating()
        {
            var outputList = new List<int>();

            // Inputs
            var container = new QueryContainer();
            Stream state = null;

            // Input data
            var enumerable = Enumerable.Range(0, 10000).ToList();
            var inputObservable = enumerable.ToObservable().Select(e => StreamEvent.CreateStart(0, e));

            // Query start
            var stream = container.RegisterInput(inputObservable);
            var multiStream = stream.Multicast(3);

            var output1 = multiStream[0].Where(e => e % 3 == 0);
            var output2 = multiStream[1].Where(e => e % 3 == 1);
            var output3 = multiStream[2].Where(e => e % 3 == 2);
            var union = output1.Union(output2).Union(output3);

            var outputObservable = container.RegisterOutput(union);

            // Test the output
            var outputAsync = outputObservable.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
            container.Restore(state);
            outputAsync.Wait();

            outputList.Sort();
            Assert.IsTrue(enumerable.SequenceEqual(outputList));
        }

        [TestMethod, TestCategory("Gated")]
        public void NaryMulticastContainerErrorColumnarFloating()
        {
            // Inputs
            var container = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var enumerable = Enumerable.Range(0, 10000).ToList();
            var inputObservable = enumerable.ToObservable().Select(e => StreamEvent.CreateStart(0, e));

            // Query start
            var stream = container.RegisterInput(inputObservable);
            var multiStream = stream.Multicast(3);

            var output1 = multiStream[0].Where(e => e % 3 == 0);
            var output2 = multiStream[1].Where(e => e % 3 == 1);
            var output3 = multiStream[2].Where(e => e % 3 == 2);
            var union = output1.Union(output2).Union(output3);

            var outputObservable = container.RegisterOutput(union);

            // Validate that attempting to start the query before subscriptions will result in an error.
            bool foundException = false;
            try
            {
                container.Restore(state);
            }
            catch (Exception)
            {
                foundException = true;
            }

            Assert.IsTrue(foundException, "Expected an exception.");
        }

        [TestMethod, TestCategory("Gated")]
        public void StreamSortColumnarFloating()
        {
            var input = Enumerable.Range(0, 20).Select(i => (char)('a' + i));
            var reversedInput = input.Reverse();
            var str = reversedInput.ToStatStreamable();
            var sortedStream = str.Sort(c => c);
            var result = sortedStream.ToStreamEventObservable().Where(se => se.IsData).Select(se => se.Payload).ToEnumerable();
            Assert.IsTrue(input.SequenceEqual(result));
            sortedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void SortModifiedStreamColumnarFloating()
        {
            // The Sort method on a stream uses an observable cache. The cache
            // had been putting all rows from the messages into the list to be sorted,
            // even if it was an invalid row.
            var input = Enumerable.Range(0, 20).Select(i => (char)('a' + i));
            var str = input.ToStatStreamable();
            var str2 = str.Where(r => r != 'b');
            var sortedStream = str2.Sort(c => c);
            var result = sortedStream.ToStreamEventObservable().Where(se => se.IsData).Select(se => se.Payload).ToEnumerable();
            Assert.IsTrue(input.Count() == result.Count() + 1); // just make sure deleted element isn't counted anymore
            sortedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void GroupStreamableCodeGenTest1ColumnarFloating()
        {
            var input1 = new[]
            {
                StreamEvent.CreateInterval(100, 101, 13),
                StreamEvent.CreateInterval(100, 110, 11),
                StreamEvent.CreateInterval(101, 105, 12),
                StreamEvent.CreateInterval(102, 112, 21),
                StreamEvent.CreateInterval(109, 112, 14),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };

            var input2 = new[]
            {
                StreamEvent.CreateInterval(100, 110, 'd'),
                StreamEvent.CreateInterval(101, 103, 'b'),
                StreamEvent.CreateInterval(106, 108, 'b'),
                StreamEvent.CreateInterval(106, 109, 'b'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime)
            };

            var inputStream1 = input1.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var inputStream2 = input2.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var outputStream = inputStream1.WhereNotExists(
                inputStream2,
                l => l / 10,
                r => r - 'a');

            var expected = new[]
            {
                StreamEvent.CreateStart(100, 11),
                StreamEvent.CreateEnd(101, 100, 11),
                StreamEvent.CreateStart(103, 11),
                StreamEvent.CreateEnd(106, 103, 11),
                StreamEvent.CreateInterval(109, 110, 11),
                StreamEvent.CreateInterval(103, 105, 12),
                StreamEvent.CreateInterval(100, 101, 13),
                StreamEvent.CreateInterval(109, 112, 14),
                StreamEvent.CreateStart(102, 21),
                StreamEvent.CreateEnd(112, 102, 21),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };
            var outputEnumerable = outputStream.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void CountColumnar1ColumnarFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<ulong>[]
            {
                StreamEvent.CreateStart<ulong>(StreamEvent.MinSyncTime, 3),
                StreamEvent.CreatePunctuation<ulong>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateEnd<ulong>(StreamEvent.MinSyncTime + 1, StreamEvent.MinSyncTime, 3),
                StreamEvent.CreateStart<ulong>(StreamEvent.MinSyncTime + 1, 6),
                StreamEvent.CreatePunctuation<ulong>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateEnd<ulong>(StreamEvent.MinSyncTime + 2, StreamEvent.MinSyncTime + 1, 6),
                StreamEvent.CreateStart<ulong>(StreamEvent.MinSyncTime + 2, 9),
                StreamEvent.CreatePunctuation<ulong>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None);
            var count = str.Count();
            var outputEnumerable = count.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageColumnar1ColumnarFloating() // tests codegen for Snapshot_noecq
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),      // 97
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),      // 98
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),      // 99  running average = 98

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),  // 100
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),  // 101
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),  // 102 running average = 99.5

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),  // 103
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),  // 104
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),  // 105 running average = 101
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<double>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 1, StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 2, StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 101.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None).SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime);
            var avg = str.Average(c => (double)c);
            var outputEnumerable = avg.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageColumnar2ColumnarFloating() // tests codegen for Snapshot_q
        {
            var zero = StreamEvent.MinSyncTime;
            var one = StreamEvent.MinSyncTime + 1;
            var two = StreamEvent.MinSyncTime + 2;
            var three = StreamEvent.MinSyncTime + 3;
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(zero, one, 'a'),
                StreamEvent.CreateInterval(zero, one, 'b'),
                StreamEvent.CreateInterval(zero, one, 'c'),
                StreamEvent.CreateInterval(one, two, 'd'),
                StreamEvent.CreateInterval(one, two, 'e'),
                StreamEvent.CreateInterval(one, two, 'f'),
                StreamEvent.CreateInterval(two, three, 'g'),
                StreamEvent.CreateInterval(two, three, 'h'),
                StreamEvent.CreateInterval(two, three, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<double>[]
            {
                StreamEvent.CreateInterval(zero, one, 98.0),
                StreamEvent.CreatePunctuation<double>(one),
                StreamEvent.CreateInterval(one, two, 101.0),
                StreamEvent.CreatePunctuation<double>(two),
                StreamEvent.CreateInterval(two, three, 104.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None).SetProperty().IsConstantDuration(true, 1);
            var avg = str.Average(c => (double)c);
            var outputEnumerable = avg.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageColumnar3ColumnarFloating() // tests codegen for Snapshot_pq
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),      // 97
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),      // 98
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),      // 99  running average = 98

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),  // 100
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),  // 101
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),  // 102 running average = 99.5

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),  // 103
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),  // 104
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),  // 105 running average = 101
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<double>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 1, StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 2, StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 101.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None);
            var avg = str.Average(c => (double)c);
            var outputEnumerable = avg.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SumColumnar1ColumnarFloating() // make sure expression is properly parenthesized in codegen
        {
            var input = Enumerable.Range(0, 100);
            Expression<Func<int, int>> query = e => e & 0x7;

            var streamResult = input
                .ToStreamable()
                .Sum(query)
                .ToPayloadEnumerable()
                .Last();

            var expected = input.Sum(query.Compile());
            Assert.IsTrue(expected == streamResult);

        }

        [TestMethod, TestCategory("Gated")]
        public void MultiAggregate1ColumnarFloating() // test codegen for multi-aggregates
        {
            var input = Enumerable.Range(0, 100).Select(i => new MyStruct { field1 = i, field2 = 0.0, field3 = Guid.Empty, });

            var streamResult = input
                .ToStreamable()
                .Aggregate(a => a.Sum(x => x.field1), a => a.Count(), (s, c) => new MyStruct { field1 = s + ((int)c) })
                .ToPayloadEnumerable()
                ;

            Assert.IsTrue(streamResult.Count() == 1);

            Assert.IsTrue(streamResult.First().field1 == 5050);

        }

        [TestMethod, TestCategory("Gated")]
        public void MultiAggregate2ColumnarFloating() // test codegen for multi-aggregates with anonymous type
        {
            var input = Enumerable.Range(0, 100).Select(i => new MyStruct { field1 = i, field2 = 0.0, field3 = Guid.Empty, });

            var streamResult = input
                .ToStreamable()
                .Aggregate(a => a.Sum(x => x.field1), a => a.Count(), (s, c) => new { field1 = s, field2 = c })
                .ToPayloadEnumerable()
                ;

            Assert.IsTrue(streamResult.Count() == 1);

            var f = streamResult.First();
            Assert.IsTrue(f.field1 == 4950 && f.field2 == 100);

        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest1ColumnarFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(24, 3, 'c'),
                StreamEvent.CreateEnd(26, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateInterval(2, 22, 'b'),
                StreamEvent.CreateInterval(3, 24, 'c'),
                StreamEvent.CreateInterval(4, 26, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Cache(0, false, true);
            var result = str.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest2ColumnarFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(24, 3, 'c'),
                StreamEvent.CreateEnd(26, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateInterval(3, 24, 'c'),
                StreamEvent.CreateInterval(4, 26, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Cache(0, false, true);
            var result = str.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest3ColumnarFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(24, 4, 'd'),
                StreamEvent.CreateEnd(26, 3, 'c'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateInterval(2, 22, 'b'),
                StreamEvent.CreateInterval(3, 26, 'c'),
                StreamEvent.CreateInterval(4, 24, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Cache(0, false, true);
            var result = str.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void BinaryUnionTest1ColumnarFloating()
        {
            var input1 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(2, 20, 'a'),
                StreamEvent.CreateStart(4, 'b'),
                StreamEvent.CreateStart(6, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var input2 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(3, 20, 'a'),
                StreamEvent.CreateStart(5, 'b'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(9, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };

            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(2, 20, 'a'),
                StreamEvent.CreateInterval(3, 20, 'a'),
                StreamEvent.CreateStart(4, 'b'),
                StreamEvent.CreateStart(5, 'b'),
                StreamEvent.CreateStart(6, 'c'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreateStart(9, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str1 = input1.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null).Cache();
            var str2 = input2.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null).Cache();
            var resultEnum = str1.Union(str2).ToStreamEventObservable().ToEnumerable();
            var result = resultEnum.ToList();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str1.Dispose();
            str2.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void BinaryUnionTest2ColumnarFloating()
        {
            var input1 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var input2 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(5, 20, 'a'),
                StreamEvent.CreateStart(6, 'b'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };

            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateInterval(5, 20, 'a'),
                StreamEvent.CreateStart(6, 'b'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str1 = input1.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation).Cache();
            var str2 = input2.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation).Cache();
            var result = str1.Union(str2).ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str1.Dispose();
            str2.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void DeterministicUnionTestColumnarFloating()
        {
            int count = 100;
            DateTime now = DateTime.UtcNow;

            var input = new List<StreamEvent<int>> { StreamEvent.CreatePunctuation<int>(now.Ticks) };

            // Add right events.
            for (int i = 0; i < count; ++i)
                input.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i + 1));

            // Cti forces right batch to go through before reaching Config.DataBatchSize.
            input.Add(StreamEvent.CreatePunctuation<int>(now.Ticks));

            // Add left events.
            for (int i = 0; i < count; ++i)
                input.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i));
            input.Add(StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime));

            // Expect left before right even though right went through first.
            var expectedOutput = new List<StreamEvent<int>> { StreamEvent.CreatePunctuation<int>(now.Ticks) };
            for (int i = 0; i < count; i++)
                expectedOutput.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i));
            for (int i = 0; i < count; i++)
                expectedOutput.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i + 1));
            expectedOutput.Add(StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime));

            var inputStreams = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Multicast(2);

            var left = inputStreams[0].Where(e => e % 2 == 0);
            var right = inputStreams[1].Where(e => e % 2 == 1);

            var result = left
                .Union(right)
                .ToStreamEventObservable()
                .ToEnumerable()
                .ToList();

            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest4ColumnarFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateStart(6, 'b'),
                StreamEvent.CreateEnd(10, 6, 'b'),
                StreamEvent.CreateStart(11, 'c'),
                StreamEvent.CreateEnd(15, 11, 'c'),
                StreamEvent.CreateStart(16, 'd'),
                StreamEvent.CreateEnd(20, 16, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 5, 'a'),
                StreamEvent.CreateInterval(6, 10, 'b'),
                StreamEvent.CreateInterval(11, 15, 'c'),
                StreamEvent.CreateInterval(16, 20, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation);
            var result = str.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void AggregateTest1ColumnarFloating()
        {
            long threeSecondTicks = TimeSpan.FromSeconds(3).Ticks;

            var stream = Events().ToEvents(e => e.Vs.Ticks, e => e.Vs.Ticks + threeSecondTicks)
                                 .ToObservable()
                                 .ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None());
            var expectedResult = new StreamEvent<ResultEvent>[]
            {
                StreamEvent.CreateStart(633979008010000000, new ResultEvent { Key = 1, Cnt = 1 }),
                StreamEvent.CreateEnd(633979008020000000, 633979008010000000, new ResultEvent { Key = 1, Cnt = 1 }),
                StreamEvent.CreateStart(633979008020000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateEnd(633979008030000000, 633979008020000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateStart(633979008030000000, new ResultEvent { Key = 1, Cnt = 3 }),
                StreamEvent.CreateEnd(633979008040000000, 633979008030000000, new ResultEvent { Key = 1, Cnt = 3 }),
                StreamEvent.CreateStart(633979008040000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateEnd(633979008050000000, 633979008040000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateStart(633979008050000000, new ResultEvent { Key = 1, Cnt = 1 }),
                StreamEvent.CreateEnd(633979008060000000, 633979008050000000, new ResultEvent { Key = 1, Cnt = 1 }),
            };
            var counts = stream.GroupApply(
                                    g => g.Key,
                                    e => e.Count(),
                                    (g, e) => new ResultEvent { Key = g.Key, Cnt = e })
                               .ToStreamEventObservable()
                               .ToEnumerable()
                               .Where(e => e.IsData && e.Payload.Key == 1);
            foreach (var x in counts)
            {
                Console.WriteLine("{0}, {1}", x.SyncTime, x.OtherTime);
            }
            Assert.IsTrue(expectedResult.SequenceEqual(counts));
        }

        public IEnumerable<Event> Events()
        {
            var vs = new DateTime(2010, 1, 1, 0, 00, 00, DateTimeKind.Utc);
            var rand = new Random(0);
            int eventCnt = 5;
            for (int i = 0; i < eventCnt; i++)
            {
                yield return new Event { Vs = vs, V = rand.Next(10), Key = rand.Next(3) };
                vs = vs.AddSeconds(1);
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void CodeGenWithFieldOfGenericTypeColumnarFloating()
        {
            // Taken from the PerformanceTest sample.
            // The GroupApply creates batches where the key is a generic type and codegen was doing a type cast without making sure the type's name was valid C# syntax
            var numEventsPerTumble = 2000;
            var tumblingWindowDataset =
                Observable.Range(0, 100)
                    .Select(e => StreamEvent.CreateInterval(((long)e / numEventsPerTumble) * numEventsPerTumble, ((long)e / numEventsPerTumble) * numEventsPerTumble + numEventsPerTumble, new Payload { field1 = 0, field2 = 0 }))
                    .ToStreamable()
                    .SetProperty().IsConstantDuration(true, numEventsPerTumble)
                    .SetProperty().IsSnapshotSorted(true, e => e.field1);
            var result = tumblingWindowDataset.GroupApply(e => e.field2, str => str.Count(), (g, c) => new { Key = g, Count = c });
            var a = result.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(a.Length == 3); // just make sure it doesn't crash
        }

        [TestMethod, TestCategory("Gated")]
        public void StringSerializationColumnarFloating()
        {
            var rand = new Random(0);
            var pool = MemoryManager.GetColumnPool<string>();

            for (int x = 0; x < 100; x++)
            {
                pool.Get(out ColumnBatch<string> inputStr);

                var toss1 = rand.NextDouble();
                inputStr.UsedLength = toss1 < 0.1 ? 0 : rand.Next(Config.DataBatchSize);

                for (int i = 0; i < inputStr.UsedLength; i++)
                {
                    var toss = rand.NextDouble();
                    inputStr.col[i] = toss < 0.2 ? string.Empty : (toss < 0.4 ? null : Guid.NewGuid().ToString());
                    if (x == 0) inputStr.col[i] = null;
                    if (x == 1) inputStr.col[i] = string.Empty;
                }

                StateSerializer<ColumnBatch<string>> s = StreamableSerializer.Create<ColumnBatch<string>>(new SerializerSettings { });
                var ms = new MemoryStream { Position = 0 };

                s.Serialize(ms, inputStr);
                ms.Position = 0;
                ColumnBatch<string> resultStr = s.Deserialize(ms);

                Assert.IsTrue(resultStr.UsedLength == inputStr.UsedLength);

                for (int j = 0; j < inputStr.UsedLength; j++)
                {
                    Assert.IsTrue(inputStr.col[j] == resultStr.col[j]);
                }
                resultStr.ReturnClear();
                inputStr.ReturnClear();
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void CharArraySerializationColumnarFloating()
        {
            using (var modifier = new ConfigModifier().SerializationCompressionLevel(SerializationCompressionLevel.CharArrayToUTF8).Modify())
            {
                var rand = new Random(0);

                for (int x = 0; x < 5; x++)
                {
                    var inputStr = new MultiString();

                    var toss1 = rand.NextDouble();
                    var usedLength = 1 + rand.Next(Config.DataBatchSize - 1);

                    for (int i = 0; i < usedLength; i++)
                    {
                        var toss = rand.NextDouble();
                        string str = toss < 0.2 ? string.Empty : Guid.NewGuid().ToString();
                        if (x == 0) str = string.Empty;
                        inputStr.AddString(str);
                    }

                    inputStr.Seal();

                    StateSerializer<MultiString> s = StreamableSerializer.Create<MultiString>(new SerializerSettings { });
                    var ms = new MemoryStream { Position = 0 };

                    s.Serialize(ms, inputStr);
                    ms.Position = 0;
                    var resultStr = s.Deserialize(ms);

                    Assert.IsTrue(resultStr.Count == inputStr.Count);

                    for (int j = 0; j < inputStr.Count; j++)
                    {
                        Assert.IsTrue(inputStr[j] == resultStr[j]);
                    }
                    resultStr.Dispose();
                    inputStr.Dispose();
                }
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void MultipleAggregatesWithCompiledDictionariesColumnarFloating()
        {
            IEnumerable<StreamEvent<StreamScope_InputEvent0>> bacon = Array.Empty<StreamEvent<StreamScope_InputEvent0>>();
            IObservableIngressStreamable<StreamScope_InputEvent0> input = bacon.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var result =
                input
                .GroupAggregate(
                    s => new { s.OutputTs, s.DelayInMilliseconds, s.UserId },
                    s => s.Count(),
                    s => s.Min(r => r.Duration),
                    s => s.Max(r => r.Duration),
                    s => s.Average(r => r.Duration),
                    (key, cnt, minDuration, maxDuration, avgDuration) => new StreamScope_OutputEvent0()
                    {
                        OutputTs = key.Key.OutputTs,
                        DelayInMilliseconds = key.Key.DelayInMilliseconds,
                        UserId = key.Key.UserId,
                        Cnt = (long)cnt,
                        MinDuration = minDuration,
                        MaxDuration = maxDuration,
                        AvgDuration = avgDuration,
                    }).ToStreamEventObservable();

            var b = result;
        }

        [TestMethod, TestCategory("Gated")]
        public void PayloadWithFieldOfNestedType01ColumnarFloating()
        {
            var input = Enumerable
                .Range(0, NumEvents)
                .Select(i => new PayloadWithFieldOfNestedType1(i))
                ;
            var streamResult = input
                .ToStreamable()
                .Where(p => p.nt.x % 2 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var linqResult = input.Where(p => p.nt.x % 2 == 0);
            Assert.IsTrue(Enumerable.Zip(linqResult, a, (p1, p2) => p1.nt.x == p2.nt.x).All(p => p));
        }
        [TestMethod, TestCategory("Gated")]
        public void PayloadWithFieldOfNestedType02ColumnarFloating()
        {
            var input = Enumerable
                .Range(0, NumEvents)
                .Select(i => new PayloadWithFieldOfNestedType2(i))
                ;
            var streamResult = input
                .ToStreamable()
                .Where(p => p.nt.x % 2 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var linqResult = input.Where(p => p.nt.x % 2 == 0);
            Assert.IsTrue(Enumerable.Zip(linqResult, a, (p1, p2) => p1.nt.x == p2.nt.x).All(p => p));
        }
    }

    [TestClass]
    public class SimpleTestsColumnarSmallBatch : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public SimpleTestsColumnarSmallBatch() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        // Total number of input events
        private const int NumEvents = 1000;
        private readonly IEnumerable<MyStruct2> enumerable = Enumerable.Range(0, NumEvents)
            .Select(e =>
                new MyStruct2 { field1 = e, field2 = new MyString(Convert.ToString("string" + e)), field3 = new NestedStruct { nestedField = e } });

        private static IComparerExpression<T> GetDefaultComparerExpression<T>(T t) => ComparerExpression<T>.Default;

        private static IEqualityComparerExpression<T> GetDefaultEqualityComparerExpression<T>(T t) => EqualityComparerExpression<T>.Default;

        public static bool MyIsEqual(List<RankedEvent<char>> a, List<RankedEvent<char>> b)
        {
            if (a.Count != b.Count) return false;

            for (int j = 0; j < a.Count; j++)
            {
                if (!a[j].Equals(b[j])) return false;
            }

            return true;
        }

        public static int MyGetHashCode(List<RankedEvent<char>> a)
        {
            int ret = 0;
            foreach (var l in a)
            {
                ret ^= l.GetHashCode();
            }
            return ret;
        }

        private void TestWhere(Expression<Func<MyStruct2, bool>> predicate, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhere(predicate, disableFusion));

        private void TestSelect<TResult>(Expression<Func<MyStruct2, TResult>> function, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestSelect(function, disableFusion));

        public void TestWhereSelect<U>(Expression<Func<MyStruct2, bool>> wherePredicate, Expression<Func<MyStruct2, U>> selectFunction, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhereSelect(wherePredicate, selectFunction, disableFusion));

        [TestMethod, TestCategory("Gated")]
        public void Where1ColumnarSmallBatch()
            => TestWhere(e => e.field2.mystring.Contains("string"));

        [TestMethod, TestCategory("Gated")]
        public void Where2ColumnarSmallBatch()
            => TestWhere(e => e.field2.mystring.Equals("string0"));

        [TestMethod, TestCategory("Gated")]
        public void Where3ColumnarSmallBatch()
            => TestWhere(e => e.field2.mystring.Equals("string0"));

        [TestMethod, TestCategory("Gated")]
        public void Where4ColumnarSmallBatch()
            => TestWhere(e => e.field3.nestedField == 0);

        [TestMethod, TestCategory("Gated")]
        public void Where5ColumnarSmallBatch()
            => TestWhere(e => true);

        [TestMethod, TestCategory("Gated")]
        public void Where6ColumnarSmallBatch()
            => TestWhere(e => false);

        [TestMethod, TestCategory("Gated")]
        public void Where7ColumnarSmallBatch()
            => TestWhere(e => e.field3.nestedField == 0 || e.field3.nestedField == 1);

        [TestMethod, TestCategory("Gated")]
        public void Where8ColumnarSmallBatch()
            => TestWhere(e => string.Compare(e.field2.mystring, "string0") == 0);

        [TestMethod, TestCategory("Gated")]
        public void Where9ColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Where(v => v % 2 == 0)
                .Where(v => v % 3 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            Assert.IsTrue(input.Where(v => v % 6 == 0).SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereWithFailedTransformationColumnarSmallBatch() // stays columnar, but causes payload to be reconstituted
            => Enumerable.Range(0, 100)
                .Select(i => new StructTuple<int, char> { Item1 = i, Item2 = i.ToString()[0], })
                .TestWhere(r => r.ToString().Length > 3);

        [TestMethod, TestCategory("Gated")]
        public void Select1ColumnarSmallBatch()
            => TestSelect(e => e.field1);

        [TestMethod, TestCategory("Gated")]
        public void Select2ColumnarSmallBatch()
            => TestSelect(e => e.field2);

        [TestMethod, TestCategory("Gated")]
        public void Select3ColumnarSmallBatch()
            => TestSelect(e => e.field3);

        [TestMethod, TestCategory("Gated")]
        public void Select4ColumnarSmallBatch()
            => TestSelect(e => new NestedStruct { nestedField = e.field1 });

        [TestMethod, TestCategory("Gated")]
        public void Select5ColumnarSmallBatch()
            => TestSelect(e => e.doubleField);

        [TestMethod, TestCategory("Gated")]
        public void Select6ColumnarSmallBatch()
            => TestSelect(e => ((ulong)e.field1));

        // Tests to make sure that we ref count correctly by doing two pointer swings to the same column.
        [TestMethod, TestCategory("Gated")]
        public void Select7ColumnarSmallBatch()
            => TestSelect(e => new { A = e.field1, B = e.field1, });

        // Tests whether an expression that cannot be columnerized causes code gen to fall back to row-oriented.
        // This is just a degenerate case where the expression is *always* null.
        [TestMethod, TestCategory("Gated")]
        public void Select8ColumnarSmallBatch()
            => TestSelect<ClassWithOnlyAutoProps>(e => null);

        [TestMethod, TestCategory("Gated")]
        public void SelectFromUlongToLongColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(v => (long)v)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, longInteger) => (long)integer == longInteger).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeColumnarSmallBatch()
            => TestSelect(e => new { anonfield1 = e.field1, x = 3 });

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(v => new { X = v })
                .Select(e => new { Y = e.X })
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.Y).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedNoOpColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(v => new { X = v })
                .Select(e => e)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.X).All(p => p));
        }

        // The point of this test is to have an anonymous type with a "field" in it
        // whose type does not have an overload for it in MemoryPool<TKey, TPayload>.Get(out ColumnBatch<>).
        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeWithFloatFieldColumnarSmallBatch()
            => TestSelect(e => new { floatField = e.field1 * 3.0, });

        [TestMethod, TestCategory("Gated")]
        public void SelectWhereAnonymousTypeColumnarSmallBatch()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithAutoPropsColumnarSmallBatch()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithAutoPropsColumnarSmallBatch()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithOnlyAutoPropsColumnarSmallBatch()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithOnlyAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithOnlyAutoPropsColumnarSmallBatch()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_SelectWhereColumnarSmallBatch()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_GroupApplyColumnarSmallBatch()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .GroupApply(e => e.field1, str => str.Count(), (g, cnt) => new MyStruct { field1 = (int)cnt, })
                .Where(e => e.field2 >= 0.0)
                .ToPayloadEnumerable();
            Assert.IsTrue(streamResult.Count() == this.enumerable.Count());
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectWithCtorCallColumnarSmallBatch()
            => TestSelect(e => new StructWithCtor(e.field1));

        [TestMethod, TestCategory("Gated")]
        public void SelectMany01ColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany02ColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany03ColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany04ColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart01ColumnarSmallBatch()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select((l, e) => e + ((int)l))
                ;
            var expected = input
                .Select(e => e + e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart02ColumnarSmallBatch()
        {
            // tests simple parameter selection with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select((l, e) => l)
                ;
            var expected = input
                .Select(e => (long)e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey01ColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectByKey((k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey02ColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectByKey((k, e) => k)
                ;
            var expected = input
                .Select(e => Empty.Default)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey03ColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectByKey((s, k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e + (int)s, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e + e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey04ColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => (int?)(e + 1))
                .SelectByKey((k, e) => new object[] { e, })
                ;
            var expected = input
                .Select(e => new object[] { (int?)(e + 1), })
                .ToArray()
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.Length == output.Length);
            for (int i = 0; i < expected.Length; i++)
            {
                var e = (int?)(expected[i][0]);
                var o = (int?)(output[i][0]);
                Assert.IsTrue(e.HasValue && o.HasValue && (e.Value == o.Value));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStart01ColumnarSmallBatch()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany((l, e) => Enumerable.Repeat(33, e + ((int)l)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + e))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyKey01ColumnarSmallBatch()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectManyByKey((k, e) => Enumerable.Repeat(33, e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStartKey01ColumnarSmallBatch()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectManyByKey((ts, k, e) => Enumerable.Repeat(33 + ((int)ts), e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33 + e, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereSelect1ColumnarSmallBatch()
            => TestWhereSelect(e => true, e => e.field1);

        [TestMethod, TestCategory("Gated")]
        public void SelectStructColumnarSmallBatch()
        {
            using (var modifier = new ConfigModifier().ForceRowBasedExecution(true).Modify())
            {
                var savedForceRowBasedExecution = Config.ForceRowBasedExecution;
                Config.ForceRowBasedExecution = true;

                var input = Enumerable.Range(0, 100);
                Expression<Func<int, MyStruct>> lambda = e => new MyStruct { field1 = e, field2 = e + 0.5, field3 = default };

                var streamResult = input
                    .ToStreamable()
                    .ShiftEventLifetime(0)
                    .Select(lambda)
                    .ToPayloadEnumerable();
                var linqResult = input
                    .Select(lambda.Compile());

                var query = input.Where(e => true);
                Assert.IsTrue(linqResult.SequenceEqual(streamResult));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantNoOpIntervalsColumnarSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 5, 'a'),
                StreamEvent.CreateInterval(2, 6, 'b'),
                StreamEvent.CreateInterval(3, 7, 'c'),
                StreamEvent.CreateInterval(4, 8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 5, 'a'),
                StreamEvent.CreateInterval(2, 6, 'b'),
                StreamEvent.CreateInterval(3, 7, 'c'),
                StreamEvent.CreateInterval(4, 8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(10)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantNoOpEdgesColumnarSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateEnd(6, 2, 'b'),
                StreamEvent.CreateEnd(7, 3, 'c'),
                StreamEvent.CreateEnd(8, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateEnd(6, 2, 'b'),
                StreamEvent.CreateEnd(7, 3, 'c'),
                StreamEvent.CreateEnd(8, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(10)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantClippedIntervalsColumnarSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 15, 'a'),
                StreamEvent.CreateInterval(2, 16, 'b'),
                StreamEvent.CreateInterval(3, 17, 'c'),
                StreamEvent.CreateInterval(4, 18, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 11, 'a'),
                StreamEvent.CreateInterval(2, 12, 'b'),
                StreamEvent.CreateInterval(3, 13, 'c'),
                StreamEvent.CreateInterval(4, 14, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(10)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantClippedEdgesColumnarSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateEnd(6, 2, 'b'),
                StreamEvent.CreateEnd(7, 3, 'c'),
                StreamEvent.CreateEnd(8, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateEnd(3, 1, 'a'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateEnd(4, 2, 'b'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 3, 'c'),
                StreamEvent.CreateEnd(6, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(2)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderPolicy1ColumnarSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateEnd(20, 5, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(9, 'x'),
                StreamEvent.CreateEnd(20, 9, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderPolicy2ColumnarSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreateEnd(24, 5, 'x'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(9, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreateEnd(24, 9, 'x'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderPolicy3ColumnarSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreateEnd(20, 5, 'x'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(9, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreateEnd(22, 9, 'x'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var observInput = input.ToObservable();
            var str = observInput.ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderPolicy4ColumnarSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateStart(11, 'z'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateEnd(20, 5, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreateEnd(24, 5, 'x'),
                StreamEvent.CreateEnd(26, 5, 'x'),
                StreamEvent.CreateEnd(28, 11, 'z'),
                StreamEvent.CreateEnd(30, 5, 'x'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(5, 'x'),
                StreamEvent.CreateStart(9, 'y'),
                StreamEvent.CreateStart(9, 'x'),
                StreamEvent.CreateStart(9, 'x'),
                StreamEvent.CreateStart(11, 'z'),
                StreamEvent.CreateStart(11, 'x'),
                StreamEvent.CreateStart(11, 'x'),
                StreamEvent.CreateEnd(20, 9, 'x'),
                StreamEvent.CreateEnd(22, 9, 'y'),
                StreamEvent.CreateEnd(24, 9, 'x'),
                StreamEvent.CreateEnd(26, 11, 'x'),
                StreamEvent.CreateEnd(28, 11, 'z'),
                StreamEvent.CreateEnd(30, 11, 'x'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void PunctuationPolicy2ColumnarSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(4, 'c'),
                StreamEvent.CreateStart(7, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(34, 4, 'c'),
                StreamEvent.CreateEnd(35, 7, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };

            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreatePunctuation<char>(2),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreatePunctuation<char>(4),
                StreamEvent.CreateStart(4, 'c'),
                StreamEvent.CreatePunctuation<char>(6),
                StreamEvent.CreateStart(7, 'd'),
                StreamEvent.CreatePunctuation<char>(20),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreatePunctuation<char>(22),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreatePunctuation<char>(34),
                StreamEvent.CreateEnd(34, 4, 'c'),
                StreamEvent.CreateEnd(35, 7, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(2), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void AlterDurationTest1ColumnarSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(3, 'x'),
                StreamEvent.CreateEnd(5, 3, 'x'),
                StreamEvent.CreateStart(7, 'y'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(3, 8, 'x'),
                StreamEvent.CreateInterval(7, 12, 'y'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var result = str.AlterEventDuration(5).ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void PunctuationPolicy3ColumnarSmallBatch()
        { // Simulates the way Stat does ingress (but with 80K batches, not 3...)
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void StreamCache1ColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, 1000000); // make sure it is enough to have more than one data batch
            ulong limit = 10000;
            var cachedStream = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e))
                .ToStreamable(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .Cache(limit);
            var streamResult = cachedStream
                .ToPayloadEnumerable();

            var c = (ulong)streamResult.Count();
            Assert.IsTrue(c == limit);
            cachedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void StreamCache2ColumnarSmallBatch()
        {
            var limit = 10; // 00000;
            var input = Enumerable.Range(0, limit); // make sure it is enough to have more than one data batch
            var cachedStream = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(StreamEvent.MinSyncTime, e))
                .ToStreamable(DisorderPolicy.Throw())
                .Cache();
            var streamResult = cachedStream
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.Payload)
                .ToEnumerable();
            var foo = streamResult.Count();

            Assert.IsTrue(input.SequenceEqual(streamResult));
            cachedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals1ColumnarSmallBatch()
        {
            var x = ComparerExpression<int>.Default.GetCompareExpr()
                .ExpressionEquals(ComparerExpression<int>.Default.GetCompareExpr());
            Assert.IsTrue(x);
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals2ColumnarSmallBatch()
        {
            var x = new CompoundGroupKeyEqualityComparer<Empty, MyStruct2>(EqualityComparerExpression<Empty>.Default, EqualityComparerExpression<MyStruct2>.Default);
            var y = x.GetEqualsExpr().ExpressionEquals(x.GetEqualsExpr());
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals3ColumnarSmallBatch()
        {
            var x1 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var x2 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var y = x1.GetCompareExpr().ExpressionEquals(x2.GetCompareExpr());
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals4ColumnarSmallBatch()
        {
            var x1 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var x2 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);

            var xx1 = new CompoundGroupKeyComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x1, ComparerExpression<MyStruct2>.Default);
            var xx2 = new CompoundGroupKeyComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x2, ComparerExpression<MyStruct2>.Default);

            var y = xx1.GetCompareExpr().ExpressionEquals(xx2.GetCompareExpr());
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void CompoundGroupKeyEqualityComparerDefaultColumnarSmallBatch()
        {
            var xx1 = EqualityComparerExpression<CompoundGroupKey<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>>.Default;

            var x2 = new CompoundGroupKeyEqualityComparer<Empty, MyStruct2>(EqualityComparerExpression<Empty>.Default, EqualityComparerExpression<MyStruct2>.Default);
            var xx2 = new CompoundGroupKeyEqualityComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x2, EqualityComparerExpression<MyStruct2>.Default);

            var y = xx1.ExpressionEquals(xx2);
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void CompoundGroupKeyComparerDefaultColumnarSmallBatch()
        {
            var xx1 = ComparerExpression<CompoundGroupKey<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>>.Default;

            var x2 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var xx2 = new CompoundGroupKeyComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x2, ComparerExpression<MyStruct2>.Default);

            var y = xx1.ExpressionEquals(xx2);
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForIComparableColumnarSmallBatch()
        {
            var defaultComparerExpressionProvider = ComparerExpression<ClassImplementingIComparable>.Default;
            var defaultCompareExpression = defaultComparerExpressionProvider.GetCompareExpr();
            var compiledDefaultCompareExpression = defaultCompareExpression.Compile();
            var o = new ClassImplementingIComparable();
            var result = compiledDefaultCompareExpression(o, o);
            Assert.IsTrue(result == 3);

        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForGenericIComparerColumnarSmallBatch()
        {
            var defaultComparerExpressionProvider = ComparerExpression<ClassImplementingGenericIComparer>.Default;
            var defaultCompareExpression = defaultComparerExpressionProvider.GetCompareExpr();
            var compiledDefaultCompareExpression = defaultCompareExpression.Compile();
            var o = new ClassImplementingGenericIComparer();
            var result = compiledDefaultCompareExpression(o, o);
            Assert.IsTrue(result == 3);

        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForNonGenericIComparerColumnarSmallBatch()
        {
            var defaultComparerExpressionProvider = ComparerExpression<ClassImplementingNonGenericIComparer>.Default;
            var defaultCompareExpression = defaultComparerExpressionProvider.GetCompareExpr();
            var compiledDefaultCompareExpression = defaultCompareExpression.Compile();
            var o = new ClassImplementingNonGenericIComparer();
            var result = compiledDefaultCompareExpression(o, o);
            Assert.IsTrue(result == 3);

        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForAnonymousType1ColumnarSmallBatch()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var f = GetDefaultComparerExpression(a);
            var g = f.GetCompareExpr();
            var h = g.Compile();
            var k = h(a, a);
            Assert.IsTrue(k == 0);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType1ColumnarSmallBatch()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, a);
            Assert.IsTrue(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType2ColumnarSmallBatch()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var b = new { Z = i + 1, A = (char)('a' + i), W = "abc" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, b);
            Assert.IsFalse(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType3ColumnarSmallBatch()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var b = new { Z = i, A = (char)('b' + i), W = "abc" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, b);
            Assert.IsFalse(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType4ColumnarSmallBatch()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var b = new { Z = i, A = (char)('a' + i), W = "abcd" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, b);
            Assert.IsFalse(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void NaryMulticast1ColumnarSmallBatch()
        {
            var enumerable = Enumerable.Range(0, 10000).ToList();

            var stream = enumerable.ToStreamable();
            var multiStream = stream.Multicast(3);

            var output1 = multiStream[0].Where(e => e % 3 == 0);
            var output2 = multiStream[1].Where(e => e % 3 == 1);
            var output3 = multiStream[2].Where(e => e % 3 == 2);
            var union = output1.Union(output2).Union(output3);

            var output = union.ToPayloadEnumerable().ToList();
            output.Sort();
            Assert.IsTrue(enumerable.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void NaryMulticastContainer1ColumnarSmallBatch()
        {
            var outputList = new List<int>();

            // Inputs
            var container = new QueryContainer();
            Stream state = null;

            // Input data
            var enumerable = Enumerable.Range(0, 10000).ToList();
            var inputObservable = enumerable.ToObservable().Select(e => StreamEvent.CreateStart(0, e));

            // Query start
            var stream = container.RegisterInput(inputObservable);
            var multiStream = stream.Multicast(3);

            var output1 = multiStream[0].Where(e => e % 3 == 0);
            var output2 = multiStream[1].Where(e => e % 3 == 1);
            var output3 = multiStream[2].Where(e => e % 3 == 2);
            var union = output1.Union(output2).Union(output3);

            var outputObservable = container.RegisterOutput(union);

            // Test the output
            var outputAsync = outputObservable.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
            container.Restore(state);
            outputAsync.Wait();

            outputList.Sort();
            Assert.IsTrue(enumerable.SequenceEqual(outputList));
        }

        [TestMethod, TestCategory("Gated")]
        public void NaryMulticastContainerErrorColumnarSmallBatch()
        {
            // Inputs
            var container = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var enumerable = Enumerable.Range(0, 10000).ToList();
            var inputObservable = enumerable.ToObservable().Select(e => StreamEvent.CreateStart(0, e));

            // Query start
            var stream = container.RegisterInput(inputObservable);
            var multiStream = stream.Multicast(3);

            var output1 = multiStream[0].Where(e => e % 3 == 0);
            var output2 = multiStream[1].Where(e => e % 3 == 1);
            var output3 = multiStream[2].Where(e => e % 3 == 2);
            var union = output1.Union(output2).Union(output3);

            var outputObservable = container.RegisterOutput(union);

            // Validate that attempting to start the query before subscriptions will result in an error.
            bool foundException = false;
            try
            {
                container.Restore(state);
            }
            catch (Exception)
            {
                foundException = true;
            }

            Assert.IsTrue(foundException, "Expected an exception.");
        }

        [TestMethod, TestCategory("Gated")]
        public void StreamSortColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, 20).Select(i => (char)('a' + i));
            var reversedInput = input.Reverse();
            var str = reversedInput.ToStatStreamable();
            var sortedStream = str.Sort(c => c);
            var result = sortedStream.ToStreamEventObservable().Where(se => se.IsData).Select(se => se.Payload).ToEnumerable();
            Assert.IsTrue(input.SequenceEqual(result));
            sortedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void SortModifiedStreamColumnarSmallBatch()
        {
            // The Sort method on a stream uses an observable cache. The cache
            // had been putting all rows from the messages into the list to be sorted,
            // even if it was an invalid row.
            var input = Enumerable.Range(0, 20).Select(i => (char)('a' + i));
            var str = input.ToStatStreamable();
            var str2 = str.Where(r => r != 'b');
            var sortedStream = str2.Sort(c => c);
            var result = sortedStream.ToStreamEventObservable().Where(se => se.IsData).Select(se => se.Payload).ToEnumerable();
            Assert.IsTrue(input.Count() == result.Count() + 1); // just make sure deleted element isn't counted anymore
            sortedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void GroupStreamableCodeGenTest1ColumnarSmallBatch()
        {
            var input1 = new[]
            {
                StreamEvent.CreateInterval(100, 101, 13),
                StreamEvent.CreateInterval(100, 110, 11),
                StreamEvent.CreateInterval(101, 105, 12),
                StreamEvent.CreateInterval(102, 112, 21),
                StreamEvent.CreateInterval(109, 112, 14),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };

            var input2 = new[]
            {
                StreamEvent.CreateInterval(100, 110, 'd'),
                StreamEvent.CreateInterval(101, 103, 'b'),
                StreamEvent.CreateInterval(106, 108, 'b'),
                StreamEvent.CreateInterval(106, 109, 'b'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime)
            };

            var inputStream1 = input1.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var inputStream2 = input2.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var outputStream = inputStream1.WhereNotExists(
                inputStream2,
                l => l / 10,
                r => r - 'a');

            var expected = new[]
            {
                StreamEvent.CreateStart(100, 11),
                StreamEvent.CreateEnd(101, 100, 11),
                StreamEvent.CreateStart(103, 11),
                StreamEvent.CreateEnd(106, 103, 11),
                StreamEvent.CreateInterval(109, 110, 11),
                StreamEvent.CreateInterval(103, 105, 12),
                StreamEvent.CreateInterval(100, 101, 13),
                StreamEvent.CreateInterval(109, 112, 14),
                StreamEvent.CreateStart(102, 21),
                StreamEvent.CreateEnd(112, 102, 21),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };
            var outputEnumerable = outputStream.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void CountColumnar1ColumnarSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<ulong>[]
            {
                StreamEvent.CreateStart<ulong>(StreamEvent.MinSyncTime, 3),
                StreamEvent.CreatePunctuation<ulong>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateEnd<ulong>(StreamEvent.MinSyncTime + 1, StreamEvent.MinSyncTime, 3),
                StreamEvent.CreateStart<ulong>(StreamEvent.MinSyncTime + 1, 6),
                StreamEvent.CreatePunctuation<ulong>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateEnd<ulong>(StreamEvent.MinSyncTime + 2, StreamEvent.MinSyncTime + 1, 6),
                StreamEvent.CreateStart<ulong>(StreamEvent.MinSyncTime + 2, 9),
                StreamEvent.CreatePunctuation<ulong>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None);
            var count = str.Count();
            var outputEnumerable = count.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageColumnar1ColumnarSmallBatch() // tests codegen for Snapshot_noecq
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),      // 97
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),      // 98
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),      // 99  running average = 98

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),  // 100
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),  // 101
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),  // 102 running average = 99.5

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),  // 103
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),  // 104
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),  // 105 running average = 101
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<double>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 1, StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 2, StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 101.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None).SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime);
            var avg = str.Average(c => (double)c);
            var outputEnumerable = avg.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageColumnar2ColumnarSmallBatch() // tests codegen for Snapshot_q
        {
            var zero = StreamEvent.MinSyncTime;
            var one = StreamEvent.MinSyncTime + 1;
            var two = StreamEvent.MinSyncTime + 2;
            var three = StreamEvent.MinSyncTime + 3;
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(zero, one, 'a'),
                StreamEvent.CreateInterval(zero, one, 'b'),
                StreamEvent.CreateInterval(zero, one, 'c'),
                StreamEvent.CreateInterval(one, two, 'd'),
                StreamEvent.CreateInterval(one, two, 'e'),
                StreamEvent.CreateInterval(one, two, 'f'),
                StreamEvent.CreateInterval(two, three, 'g'),
                StreamEvent.CreateInterval(two, three, 'h'),
                StreamEvent.CreateInterval(two, three, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<double>[]
            {
                StreamEvent.CreateInterval(zero, one, 98.0),
                StreamEvent.CreatePunctuation<double>(one),
                StreamEvent.CreateInterval(one, two, 101.0),
                StreamEvent.CreatePunctuation<double>(two),
                StreamEvent.CreateInterval(two, three, 104.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None).SetProperty().IsConstantDuration(true, 1);
            var avg = str.Average(c => (double)c);
            var outputEnumerable = avg.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageColumnar3ColumnarSmallBatch() // tests codegen for Snapshot_pq
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),      // 97
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),      // 98
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),      // 99  running average = 98

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),  // 100
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),  // 101
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),  // 102 running average = 99.5

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),  // 103
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),  // 104
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),  // 105 running average = 101
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<double>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 1, StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 2, StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 101.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None);
            var avg = str.Average(c => (double)c);
            var outputEnumerable = avg.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SumColumnar1ColumnarSmallBatch() // make sure expression is properly parenthesized in codegen
        {
            var input = Enumerable.Range(0, 100);
            Expression<Func<int, int>> query = e => e & 0x7;

            var streamResult = input
                .ToStreamable()
                .Sum(query)
                .ToPayloadEnumerable()
                .Last();

            var expected = input.Sum(query.Compile());
            Assert.IsTrue(expected == streamResult);

        }

        [TestMethod, TestCategory("Gated")]
        public void MultiAggregate1ColumnarSmallBatch() // test codegen for multi-aggregates
        {
            var input = Enumerable.Range(0, 100).Select(i => new MyStruct { field1 = i, field2 = 0.0, field3 = Guid.Empty, });

            var streamResult = input
                .ToStreamable()
                .Aggregate(a => a.Sum(x => x.field1), a => a.Count(), (s, c) => new MyStruct { field1 = s + ((int)c) })
                .ToPayloadEnumerable()
                ;

            Assert.IsTrue(streamResult.Count() == 1);

            Assert.IsTrue(streamResult.First().field1 == 5050);

        }

        [TestMethod, TestCategory("Gated")]
        public void MultiAggregate2ColumnarSmallBatch() // test codegen for multi-aggregates with anonymous type
        {
            var input = Enumerable.Range(0, 100).Select(i => new MyStruct { field1 = i, field2 = 0.0, field3 = Guid.Empty, });

            var streamResult = input
                .ToStreamable()
                .Aggregate(a => a.Sum(x => x.field1), a => a.Count(), (s, c) => new { field1 = s, field2 = c })
                .ToPayloadEnumerable()
                ;

            Assert.IsTrue(streamResult.Count() == 1);

            var f = streamResult.First();
            Assert.IsTrue(f.field1 == 4950 && f.field2 == 100);

        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest1ColumnarSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(24, 3, 'c'),
                StreamEvent.CreateEnd(26, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateInterval(2, 22, 'b'),
                StreamEvent.CreateInterval(3, 24, 'c'),
                StreamEvent.CreateInterval(4, 26, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Cache(0, false, true);
            var result = str.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest2ColumnarSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(24, 3, 'c'),
                StreamEvent.CreateEnd(26, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateInterval(3, 24, 'c'),
                StreamEvent.CreateInterval(4, 26, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Cache(0, false, true);
            var result = str.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest3ColumnarSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(24, 4, 'd'),
                StreamEvent.CreateEnd(26, 3, 'c'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateInterval(2, 22, 'b'),
                StreamEvent.CreateInterval(3, 26, 'c'),
                StreamEvent.CreateInterval(4, 24, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Cache(0, false, true);
            var result = str.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void BinaryUnionTest1ColumnarSmallBatch()
        {
            var input1 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(2, 20, 'a'),
                StreamEvent.CreateStart(4, 'b'),
                StreamEvent.CreateStart(6, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var input2 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(3, 20, 'a'),
                StreamEvent.CreateStart(5, 'b'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(9, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };

            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(2, 20, 'a'),
                StreamEvent.CreateInterval(3, 20, 'a'),
                StreamEvent.CreateStart(4, 'b'),
                StreamEvent.CreateStart(5, 'b'),
                StreamEvent.CreateStart(6, 'c'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreateStart(9, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str1 = input1.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null).Cache();
            var str2 = input2.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null).Cache();
            var resultEnum = str1.Union(str2).ToStreamEventObservable().ToEnumerable();
            var result = resultEnum.ToList();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str1.Dispose();
            str2.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void BinaryUnionTest2ColumnarSmallBatch()
        {
            var input1 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var input2 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(5, 20, 'a'),
                StreamEvent.CreateStart(6, 'b'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };

            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateInterval(5, 20, 'a'),
                StreamEvent.CreateStart(6, 'b'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str1 = input1.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation).Cache();
            var str2 = input2.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation).Cache();
            var result = str1.Union(str2).ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str1.Dispose();
            str2.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void DeterministicUnionTestColumnarSmallBatch()
        {
            int count = 100;
            DateTime now = DateTime.UtcNow;

            var input = new List<StreamEvent<int>> { StreamEvent.CreatePunctuation<int>(now.Ticks) };

            // Add right events.
            for (int i = 0; i < count; ++i)
                input.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i + 1));

            // Cti forces right batch to go through before reaching Config.DataBatchSize.
            input.Add(StreamEvent.CreatePunctuation<int>(now.Ticks));

            // Add left events.
            for (int i = 0; i < count; ++i)
                input.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i));
            input.Add(StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime));

            // Expect left before right even though right went through first.
            var expectedOutput = new List<StreamEvent<int>> { StreamEvent.CreatePunctuation<int>(now.Ticks) };
            for (int i = 0; i < count; i++)
                expectedOutput.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i));
            for (int i = 0; i < count; i++)
                expectedOutput.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i + 1));
            expectedOutput.Add(StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime));

            var inputStreams = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Multicast(2);

            var left = inputStreams[0].Where(e => e % 2 == 0);
            var right = inputStreams[1].Where(e => e % 2 == 1);

            var result = left
                .Union(right)
                .ToStreamEventObservable()
                .ToEnumerable()
                .ToList();

            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest4ColumnarSmallBatch()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateStart(6, 'b'),
                StreamEvent.CreateEnd(10, 6, 'b'),
                StreamEvent.CreateStart(11, 'c'),
                StreamEvent.CreateEnd(15, 11, 'c'),
                StreamEvent.CreateStart(16, 'd'),
                StreamEvent.CreateEnd(20, 16, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 5, 'a'),
                StreamEvent.CreateInterval(6, 10, 'b'),
                StreamEvent.CreateInterval(11, 15, 'c'),
                StreamEvent.CreateInterval(16, 20, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation);
            var result = str.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void AggregateTest1ColumnarSmallBatch()
        {
            long threeSecondTicks = TimeSpan.FromSeconds(3).Ticks;

            var stream = Events().ToEvents(e => e.Vs.Ticks, e => e.Vs.Ticks + threeSecondTicks)
                                 .ToObservable()
                                 .ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None());
            var expectedResult = new StreamEvent<ResultEvent>[]
            {
                StreamEvent.CreateStart(633979008010000000, new ResultEvent { Key = 1, Cnt = 1 }),
                StreamEvent.CreateEnd(633979008020000000, 633979008010000000, new ResultEvent { Key = 1, Cnt = 1 }),
                StreamEvent.CreateStart(633979008020000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateEnd(633979008030000000, 633979008020000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateStart(633979008030000000, new ResultEvent { Key = 1, Cnt = 3 }),
                StreamEvent.CreateEnd(633979008040000000, 633979008030000000, new ResultEvent { Key = 1, Cnt = 3 }),
                StreamEvent.CreateStart(633979008040000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateEnd(633979008050000000, 633979008040000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateStart(633979008050000000, new ResultEvent { Key = 1, Cnt = 1 }),
                StreamEvent.CreateEnd(633979008060000000, 633979008050000000, new ResultEvent { Key = 1, Cnt = 1 }),
            };
            var counts = stream.GroupApply(
                                    g => g.Key,
                                    e => e.Count(),
                                    (g, e) => new ResultEvent { Key = g.Key, Cnt = e })
                               .ToStreamEventObservable()
                               .ToEnumerable()
                               .Where(e => e.IsData && e.Payload.Key == 1);
            foreach (var x in counts)
            {
                Console.WriteLine("{0}, {1}", x.SyncTime, x.OtherTime);
            }
            Assert.IsTrue(expectedResult.SequenceEqual(counts));
        }

        public IEnumerable<Event> Events()
        {
            var vs = new DateTime(2010, 1, 1, 0, 00, 00, DateTimeKind.Utc);
            var rand = new Random(0);
            int eventCnt = 5;
            for (int i = 0; i < eventCnt; i++)
            {
                yield return new Event { Vs = vs, V = rand.Next(10), Key = rand.Next(3) };
                vs = vs.AddSeconds(1);
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void CodeGenWithFieldOfGenericTypeColumnarSmallBatch()
        {
            // Taken from the PerformanceTest sample.
            // The GroupApply creates batches where the key is a generic type and codegen was doing a type cast without making sure the type's name was valid C# syntax
            var numEventsPerTumble = 2000;
            var tumblingWindowDataset =
                Observable.Range(0, 100)
                    .Select(e => StreamEvent.CreateInterval(((long)e / numEventsPerTumble) * numEventsPerTumble, ((long)e / numEventsPerTumble) * numEventsPerTumble + numEventsPerTumble, new Payload { field1 = 0, field2 = 0 }))
                    .ToStreamable()
                    .SetProperty().IsConstantDuration(true, numEventsPerTumble)
                    .SetProperty().IsSnapshotSorted(true, e => e.field1);
            var result = tumblingWindowDataset.GroupApply(e => e.field2, str => str.Count(), (g, c) => new { Key = g, Count = c });
            var a = result.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(a.Length == 3); // just make sure it doesn't crash
        }

        [TestMethod, TestCategory("Gated")]
        public void StringSerializationColumnarSmallBatch()
        {
            var rand = new Random(0);
            var pool = MemoryManager.GetColumnPool<string>();

            for (int x = 0; x < 100; x++)
            {
                pool.Get(out ColumnBatch<string> inputStr);

                var toss1 = rand.NextDouble();
                inputStr.UsedLength = toss1 < 0.1 ? 0 : rand.Next(Config.DataBatchSize);

                for (int i = 0; i < inputStr.UsedLength; i++)
                {
                    var toss = rand.NextDouble();
                    inputStr.col[i] = toss < 0.2 ? string.Empty : (toss < 0.4 ? null : Guid.NewGuid().ToString());
                    if (x == 0) inputStr.col[i] = null;
                    if (x == 1) inputStr.col[i] = string.Empty;
                }

                StateSerializer<ColumnBatch<string>> s = StreamableSerializer.Create<ColumnBatch<string>>(new SerializerSettings { });
                var ms = new MemoryStream { Position = 0 };

                s.Serialize(ms, inputStr);
                ms.Position = 0;
                ColumnBatch<string> resultStr = s.Deserialize(ms);

                Assert.IsTrue(resultStr.UsedLength == inputStr.UsedLength);

                for (int j = 0; j < inputStr.UsedLength; j++)
                {
                    Assert.IsTrue(inputStr.col[j] == resultStr.col[j]);
                }
                resultStr.ReturnClear();
                inputStr.ReturnClear();
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void CharArraySerializationColumnarSmallBatch()
        {
            using (var modifier = new ConfigModifier().SerializationCompressionLevel(SerializationCompressionLevel.CharArrayToUTF8).Modify())
            {
                var rand = new Random(0);

                for (int x = 0; x < 5; x++)
                {
                    var inputStr = new MultiString();

                    var toss1 = rand.NextDouble();
                    var usedLength = 1 + rand.Next(Config.DataBatchSize - 1);

                    for (int i = 0; i < usedLength; i++)
                    {
                        var toss = rand.NextDouble();
                        string str = toss < 0.2 ? string.Empty : Guid.NewGuid().ToString();
                        if (x == 0) str = string.Empty;
                        inputStr.AddString(str);
                    }

                    inputStr.Seal();

                    StateSerializer<MultiString> s = StreamableSerializer.Create<MultiString>(new SerializerSettings { });
                    var ms = new MemoryStream { Position = 0 };

                    s.Serialize(ms, inputStr);
                    ms.Position = 0;
                    var resultStr = s.Deserialize(ms);

                    Assert.IsTrue(resultStr.Count == inputStr.Count);

                    for (int j = 0; j < inputStr.Count; j++)
                    {
                        Assert.IsTrue(inputStr[j] == resultStr[j]);
                    }
                    resultStr.Dispose();
                    inputStr.Dispose();
                }
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void MultipleAggregatesWithCompiledDictionariesColumnarSmallBatch()
        {
            IEnumerable<StreamEvent<StreamScope_InputEvent0>> bacon = Array.Empty<StreamEvent<StreamScope_InputEvent0>>();
            IObservableIngressStreamable<StreamScope_InputEvent0> input = bacon.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var result =
                input
                .GroupAggregate(
                    s => new { s.OutputTs, s.DelayInMilliseconds, s.UserId },
                    s => s.Count(),
                    s => s.Min(r => r.Duration),
                    s => s.Max(r => r.Duration),
                    s => s.Average(r => r.Duration),
                    (key, cnt, minDuration, maxDuration, avgDuration) => new StreamScope_OutputEvent0()
                    {
                        OutputTs = key.Key.OutputTs,
                        DelayInMilliseconds = key.Key.DelayInMilliseconds,
                        UserId = key.Key.UserId,
                        Cnt = (long)cnt,
                        MinDuration = minDuration,
                        MaxDuration = maxDuration,
                        AvgDuration = avgDuration,
                    }).ToStreamEventObservable();

            var b = result;
        }

        [TestMethod, TestCategory("Gated")]
        public void PayloadWithFieldOfNestedType01ColumnarSmallBatch()
        {
            var input = Enumerable
                .Range(0, NumEvents)
                .Select(i => new PayloadWithFieldOfNestedType1(i))
                ;
            var streamResult = input
                .ToStreamable()
                .Where(p => p.nt.x % 2 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var linqResult = input.Where(p => p.nt.x % 2 == 0);
            Assert.IsTrue(Enumerable.Zip(linqResult, a, (p1, p2) => p1.nt.x == p2.nt.x).All(p => p));
        }
        [TestMethod, TestCategory("Gated")]
        public void PayloadWithFieldOfNestedType02ColumnarSmallBatch()
        {
            var input = Enumerable
                .Range(0, NumEvents)
                .Select(i => new PayloadWithFieldOfNestedType2(i))
                ;
            var streamResult = input
                .ToStreamable()
                .Where(p => p.nt.x % 2 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var linqResult = input.Where(p => p.nt.x % 2 == 0);
            Assert.IsTrue(Enumerable.Zip(linqResult, a, (p1, p2) => p1.nt.x == p2.nt.x).All(p => p));
        }
    }

    [TestClass]
    public class SimpleTestsColumnarSmallBatchFloating : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public SimpleTestsColumnarSmallBatchFloating() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .AllowFloatingReorderPolicy(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        // Total number of input events
        private const int NumEvents = 1000;
        private readonly IEnumerable<MyStruct2> enumerable = Enumerable.Range(0, NumEvents)
            .Select(e =>
                new MyStruct2 { field1 = e, field2 = new MyString(Convert.ToString("string" + e)), field3 = new NestedStruct { nestedField = e } });

        private static IComparerExpression<T> GetDefaultComparerExpression<T>(T t) => ComparerExpression<T>.Default;

        private static IEqualityComparerExpression<T> GetDefaultEqualityComparerExpression<T>(T t) => EqualityComparerExpression<T>.Default;

        public static bool MyIsEqual(List<RankedEvent<char>> a, List<RankedEvent<char>> b)
        {
            if (a.Count != b.Count) return false;

            for (int j = 0; j < a.Count; j++)
            {
                if (!a[j].Equals(b[j])) return false;
            }

            return true;
        }

        public static int MyGetHashCode(List<RankedEvent<char>> a)
        {
            int ret = 0;
            foreach (var l in a)
            {
                ret ^= l.GetHashCode();
            }
            return ret;
        }

        private void TestWhere(Expression<Func<MyStruct2, bool>> predicate, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhere(predicate, disableFusion));

        private void TestSelect<TResult>(Expression<Func<MyStruct2, TResult>> function, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestSelect(function, disableFusion));

        public void TestWhereSelect<U>(Expression<Func<MyStruct2, bool>> wherePredicate, Expression<Func<MyStruct2, U>> selectFunction, bool disableFusion = true)
            => Assert.IsTrue(this.enumerable.TestWhereSelect(wherePredicate, selectFunction, disableFusion));

        [TestMethod, TestCategory("Gated")]
        public void Where1ColumnarSmallBatchFloating()
            => TestWhere(e => e.field2.mystring.Contains("string"));

        [TestMethod, TestCategory("Gated")]
        public void Where2ColumnarSmallBatchFloating()
            => TestWhere(e => e.field2.mystring.Equals("string0"));

        [TestMethod, TestCategory("Gated")]
        public void Where3ColumnarSmallBatchFloating()
            => TestWhere(e => e.field2.mystring.Equals("string0"));

        [TestMethod, TestCategory("Gated")]
        public void Where4ColumnarSmallBatchFloating()
            => TestWhere(e => e.field3.nestedField == 0);

        [TestMethod, TestCategory("Gated")]
        public void Where5ColumnarSmallBatchFloating()
            => TestWhere(e => true);

        [TestMethod, TestCategory("Gated")]
        public void Where6ColumnarSmallBatchFloating()
            => TestWhere(e => false);

        [TestMethod, TestCategory("Gated")]
        public void Where7ColumnarSmallBatchFloating()
            => TestWhere(e => e.field3.nestedField == 0 || e.field3.nestedField == 1);

        [TestMethod, TestCategory("Gated")]
        public void Where8ColumnarSmallBatchFloating()
            => TestWhere(e => string.Compare(e.field2.mystring, "string0") == 0);

        [TestMethod, TestCategory("Gated")]
        public void Where9ColumnarSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Where(v => v % 2 == 0)
                .Where(v => v % 3 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            Assert.IsTrue(input.Where(v => v % 6 == 0).SequenceEqual(a));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereWithFailedTransformationColumnarSmallBatchFloating() // stays columnar, but causes payload to be reconstituted
            => Enumerable.Range(0, 100)
                .Select(i => new StructTuple<int, char> { Item1 = i, Item2 = i.ToString()[0], })
                .TestWhere(r => r.ToString().Length > 3);

        [TestMethod, TestCategory("Gated")]
        public void Select1ColumnarSmallBatchFloating()
            => TestSelect(e => e.field1);

        [TestMethod, TestCategory("Gated")]
        public void Select2ColumnarSmallBatchFloating()
            => TestSelect(e => e.field2);

        [TestMethod, TestCategory("Gated")]
        public void Select3ColumnarSmallBatchFloating()
            => TestSelect(e => e.field3);

        [TestMethod, TestCategory("Gated")]
        public void Select4ColumnarSmallBatchFloating()
            => TestSelect(e => new NestedStruct { nestedField = e.field1 });

        [TestMethod, TestCategory("Gated")]
        public void Select5ColumnarSmallBatchFloating()
            => TestSelect(e => e.doubleField);

        [TestMethod, TestCategory("Gated")]
        public void Select6ColumnarSmallBatchFloating()
            => TestSelect(e => ((ulong)e.field1));

        // Tests to make sure that we ref count correctly by doing two pointer swings to the same column.
        [TestMethod, TestCategory("Gated")]
        public void Select7ColumnarSmallBatchFloating()
            => TestSelect(e => new { A = e.field1, B = e.field1, });

        // Tests whether an expression that cannot be columnerized causes code gen to fall back to row-oriented.
        // This is just a degenerate case where the expression is *always* null.
        [TestMethod, TestCategory("Gated")]
        public void Select8ColumnarSmallBatchFloating()
            => TestSelect<ClassWithOnlyAutoProps>(e => null);

        [TestMethod, TestCategory("Gated")]
        public void SelectFromUlongToLongColumnarSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(v => (long)v)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, longInteger) => (long)integer == longInteger).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeColumnarSmallBatchFloating()
            => TestSelect(e => new { anonfield1 = e.field1, x = 3 });

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedColumnarSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(v => new { X = v })
                .Select(e => new { Y = e.X })
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.Y).All(p => p));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeChainedNoOpColumnarSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents);
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(v => new { X = v })
                .Select(e => e)
                ;

            var payloadEnum = streamResult.ToPayloadEnumerable();
            var a = payloadEnum.ToArray();
            Assert.IsTrue(Enumerable.Zip(input, payloadEnum, (integer, anon) => integer == anon.X).All(p => p));
        }

        // The point of this test is to have an anonymous type with a "field" in it
        // whose type does not have an overload for it in MemoryPool<TKey, TPayload>.Get(out ColumnBatch<>).
        [TestMethod, TestCategory("Gated")]
        public void SelectAnonymousTypeWithFloatFieldColumnarSmallBatchFloating()
            => TestSelect(e => new { floatField = e.field1 * 3.0, });

        [TestMethod, TestCategory("Gated")]
        public void SelectWhereAnonymousTypeColumnarSmallBatchFloating()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new { anonfield1 = e.field1 })
                .Where(e => e.anonfield1 == 0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithAutoPropsColumnarSmallBatchFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithAutoPropsColumnarSmallBatchFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, IntField = i + 1, StringField = i.ToString(), })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntField, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereFromClassWithOnlyAutoPropsColumnarSmallBatchFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToPayloadEnumerable()
                .ToArray()
                ;
            var linqResult = enumerable
                .Where(e => e.IntAutoProp.ToString().Equals(e.StringAutoProp))
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult, new ClassWithOnlyAutoProps()));

        }
        [TestMethod, TestCategory("Gated")]
        public void SelectFromClassWithOnlyAutoPropsColumnarSmallBatchFloating()
        {
            var enumerable = Enumerable.Range(0, NumEvents)
                .Select(i => new ClassWithOnlyAutoProps() { StringAutoProp = i.ToString(), IntAutoProp = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new { A = e.StringAutoProp, B = e.IntAutoProp + e.StringAutoProp.Length + e.IntAutoProp, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_SelectWhereColumnarSmallBatchFloating()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new MyStruct { field1 = e.field1, })
                .Where(e => e.field2 > 0.0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnassignedColumn_GroupApplyColumnarSmallBatchFloating()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .ShiftEventLifetime(0)
                .GroupApply(e => e.field1, str => str.Count(), (g, cnt) => new MyStruct { field1 = (int)cnt, })
                .Where(e => e.field2 >= 0.0)
                .ToPayloadEnumerable();
            Assert.IsTrue(streamResult.Count() == this.enumerable.Count());
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectWithCtorCallColumnarSmallBatchFloating()
            => TestSelect(e => new StructWithCtor(e.field1));

        [TestMethod, TestCategory("Gated")]
        public void SelectMany01ColumnarSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => e.field2.ToString().Split('.', StringSplitOptions.None).Select(s => s.Length))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany02ColumnarSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field1, 2))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany03ColumnarSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, e.field1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectMany04ColumnarSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                .Select(e => new MyStruct { field1 = e, field2 = (double)e, field3 = Guid.NewGuid(), })
                ;
            var streamResult = input
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                .ToPayloadEnumerable()
                ;
            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(e.field2, 1))
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart01ColumnarSmallBatchFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select((l, e) => e + ((int)l))
                ;
            var expected = input
                .Select(e => e + e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectStart02ColumnarSmallBatchFloating()
        {
            // tests simple parameter selection with start time parameter
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select((l, e) => l)
                ;
            var expected = input
                .Select(e => (long)e)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey01ColumnarSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectByKey((k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey02ColumnarSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectByKey((k, e) => k)
                ;
            var expected = input
                .Select(e => Empty.Default)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey03ColumnarSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectByKey((s, k, e) => new StructTuple<Empty, int>() { Item1 = k, Item2 = e + (int)s, })
                ;
            var expected = input
                .Select(e => new StructTuple<Empty, int>() { Item1 = Empty.Default, Item2 = e + e, })
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectKey04ColumnarSmallBatchFloating()
        {
            var input = Enumerable.Range(0, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .Select(e => (int?)(e + 1))
                .SelectByKey((k, e) => new object[] { e, })
                ;
            var expected = input
                .Select(e => new object[] { (int?)(e + 1), })
                .ToArray()
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.Length == output.Length);
            for (int i = 0; i < expected.Length; i++)
            {
                var e = (int?)(expected[i][0]);
                var o = (int?)(output[i][0]);
                Assert.IsTrue(e.HasValue && o.HasValue && (e.Value == o.Value));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStart01ColumnarSmallBatchFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectMany((l, e) => Enumerable.Repeat(33, e + ((int)l)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + e))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyKey01ColumnarSmallBatchFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectManyByKey((k, e) => Enumerable.Repeat(33, e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyStartKey01ColumnarSmallBatchFloating()
        {
            // tests computed field with start time parameter
            var input = Enumerable.Range(1, NumEvents)
                ;
            var streamResult = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e))
                .ToStreamable()
                .ShiftEventLifetime(0)
                .SelectManyByKey((ts, k, e) => Enumerable.Repeat(33 + ((int)ts), e + (k == Empty.Default ? 1 : 2)))
                ;

            var linqResult = input
                .SelectMany(e => Enumerable.Repeat(33 + e, e + 1))
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(linqResult.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereSelect1ColumnarSmallBatchFloating()
            => TestWhereSelect(e => true, e => e.field1);

        [TestMethod, TestCategory("Gated")]
        public void SelectStructColumnarSmallBatchFloating()
        {
            using (var modifier = new ConfigModifier().ForceRowBasedExecution(true).Modify())
            {
                var savedForceRowBasedExecution = Config.ForceRowBasedExecution;
                Config.ForceRowBasedExecution = true;

                var input = Enumerable.Range(0, 100);
                Expression<Func<int, MyStruct>> lambda = e => new MyStruct { field1 = e, field2 = e + 0.5, field3 = default };

                var streamResult = input
                    .ToStreamable()
                    .ShiftEventLifetime(0)
                    .Select(lambda)
                    .ToPayloadEnumerable();
                var linqResult = input
                    .Select(lambda.Compile());

                var query = input.Where(e => true);
                Assert.IsTrue(linqResult.SequenceEqual(streamResult));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantNoOpIntervalsColumnarSmallBatchFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 5, 'a'),
                StreamEvent.CreateInterval(2, 6, 'b'),
                StreamEvent.CreateInterval(3, 7, 'c'),
                StreamEvent.CreateInterval(4, 8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 5, 'a'),
                StreamEvent.CreateInterval(2, 6, 'b'),
                StreamEvent.CreateInterval(3, 7, 'c'),
                StreamEvent.CreateInterval(4, 8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(10)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantNoOpEdgesColumnarSmallBatchFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateEnd(6, 2, 'b'),
                StreamEvent.CreateEnd(7, 3, 'c'),
                StreamEvent.CreateEnd(8, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateEnd(6, 2, 'b'),
                StreamEvent.CreateEnd(7, 3, 'c'),
                StreamEvent.CreateEnd(8, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(10)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantClippedIntervalsColumnarSmallBatchFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 15, 'a'),
                StreamEvent.CreateInterval(2, 16, 'b'),
                StreamEvent.CreateInterval(3, 17, 'c'),
                StreamEvent.CreateInterval(4, 18, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 11, 'a'),
                StreamEvent.CreateInterval(2, 12, 'b'),
                StreamEvent.CreateInterval(3, 13, 'c'),
                StreamEvent.CreateInterval(4, 14, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(10)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ClipByConstantClippedEdgesColumnarSmallBatchFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateEnd(6, 2, 'b'),
                StreamEvent.CreateEnd(7, 3, 'c'),
                StreamEvent.CreateEnd(8, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateEnd(3, 1, 'a'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateEnd(4, 2, 'b'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(5, 3, 'c'),
                StreamEvent.CreateEnd(6, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var result = input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .ClipEventDuration(2)
                .ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void PunctuationPolicy2ColumnarSmallBatchFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(4, 'c'),
                StreamEvent.CreateStart(7, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(34, 4, 'c'),
                StreamEvent.CreateEnd(35, 7, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };

            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(4, 'c'),
                StreamEvent.CreateStart(7, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(34, 4, 'c'),
                StreamEvent.CreateEnd(35, 7, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(2), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void AlterDurationTest1ColumnarSmallBatchFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(3, 'x'),
                StreamEvent.CreateEnd(5, 3, 'x'),
                StreamEvent.CreateStart(7, 'y'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(3, 8, 'x'),
                StreamEvent.CreateInterval(7, 12, 'y'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var result = str.AlterEventDuration(5).ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void PunctuationPolicy3ColumnarSmallBatchFloating()
        { // Simulates the way Stat does ingress (but with 80K batches, not 3...)
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None);
            var result = str.ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void StreamCache1ColumnarSmallBatchFloating()
        {
            var input = Enumerable.Range(0, 1000000); // make sure it is enough to have more than one data batch
            ulong limit = 10000;
            var cachedStream = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e))
                .ToStreamable(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None)
                .Cache(limit);
            var streamResult = cachedStream
                .ToPayloadEnumerable();

            var c = (ulong)streamResult.Count();
            Assert.IsTrue(c == limit);
            cachedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void StreamCache2ColumnarSmallBatchFloating()
        {
            var limit = 10; // 00000;
            var input = Enumerable.Range(0, limit); // make sure it is enough to have more than one data batch
            var cachedStream = input
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(StreamEvent.MinSyncTime, e))
                .ToStreamable(DisorderPolicy.Throw())
                .Cache();
            var streamResult = cachedStream
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.Payload)
                .ToEnumerable();
            var foo = streamResult.Count();

            Assert.IsTrue(input.SequenceEqual(streamResult));
            cachedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals1ColumnarSmallBatchFloating()
        {
            var x = ComparerExpression<int>.Default.GetCompareExpr()
                .ExpressionEquals(ComparerExpression<int>.Default.GetCompareExpr());
            Assert.IsTrue(x);
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals2ColumnarSmallBatchFloating()
        {
            var x = new CompoundGroupKeyEqualityComparer<Empty, MyStruct2>(EqualityComparerExpression<Empty>.Default, EqualityComparerExpression<MyStruct2>.Default);
            var y = x.GetEqualsExpr().ExpressionEquals(x.GetEqualsExpr());
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals3ColumnarSmallBatchFloating()
        {
            var x1 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var x2 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var y = x1.GetCompareExpr().ExpressionEquals(x2.GetCompareExpr());
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void ExpressionEquals4ColumnarSmallBatchFloating()
        {
            var x1 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var x2 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);

            var xx1 = new CompoundGroupKeyComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x1, ComparerExpression<MyStruct2>.Default);
            var xx2 = new CompoundGroupKeyComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x2, ComparerExpression<MyStruct2>.Default);

            var y = xx1.GetCompareExpr().ExpressionEquals(xx2.GetCompareExpr());
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void CompoundGroupKeyEqualityComparerDefaultColumnarSmallBatchFloating()
        {
            var xx1 = EqualityComparerExpression<CompoundGroupKey<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>>.Default;

            var x2 = new CompoundGroupKeyEqualityComparer<Empty, MyStruct2>(EqualityComparerExpression<Empty>.Default, EqualityComparerExpression<MyStruct2>.Default);
            var xx2 = new CompoundGroupKeyEqualityComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x2, EqualityComparerExpression<MyStruct2>.Default);

            var y = xx1.ExpressionEquals(xx2);
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void CompoundGroupKeyComparerDefaultColumnarSmallBatchFloating()
        {
            var xx1 = ComparerExpression<CompoundGroupKey<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>>.Default;

            var x2 = new CompoundGroupKeyComparer<Empty, MyStruct2>(ComparerExpression<Empty>.Default, ComparerExpression<MyStruct2>.Default);
            var xx2 = new CompoundGroupKeyComparer<CompoundGroupKey<Empty, MyStruct2>, MyStruct2>(x2, ComparerExpression<MyStruct2>.Default);

            var y = xx1.ExpressionEquals(xx2);
            Assert.IsTrue(y);
        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForIComparableColumnarSmallBatchFloating()
        {
            var defaultComparerExpressionProvider = ComparerExpression<ClassImplementingIComparable>.Default;
            var defaultCompareExpression = defaultComparerExpressionProvider.GetCompareExpr();
            var compiledDefaultCompareExpression = defaultCompareExpression.Compile();
            var o = new ClassImplementingIComparable();
            var result = compiledDefaultCompareExpression(o, o);
            Assert.IsTrue(result == 3);

        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForGenericIComparerColumnarSmallBatchFloating()
        {
            var defaultComparerExpressionProvider = ComparerExpression<ClassImplementingGenericIComparer>.Default;
            var defaultCompareExpression = defaultComparerExpressionProvider.GetCompareExpr();
            var compiledDefaultCompareExpression = defaultCompareExpression.Compile();
            var o = new ClassImplementingGenericIComparer();
            var result = compiledDefaultCompareExpression(o, o);
            Assert.IsTrue(result == 3);

        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForNonGenericIComparerColumnarSmallBatchFloating()
        {
            var defaultComparerExpressionProvider = ComparerExpression<ClassImplementingNonGenericIComparer>.Default;
            var defaultCompareExpression = defaultComparerExpressionProvider.GetCompareExpr();
            var compiledDefaultCompareExpression = defaultCompareExpression.Compile();
            var o = new ClassImplementingNonGenericIComparer();
            var result = compiledDefaultCompareExpression(o, o);
            Assert.IsTrue(result == 3);

        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerExpressionForAnonymousType1ColumnarSmallBatchFloating()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var f = GetDefaultComparerExpression(a);
            var g = f.GetCompareExpr();
            var h = g.Compile();
            var k = h(a, a);
            Assert.IsTrue(k == 0);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType1ColumnarSmallBatchFloating()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, a);
            Assert.IsTrue(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType2ColumnarSmallBatchFloating()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var b = new { Z = i + 1, A = (char)('a' + i), W = "abc" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, b);
            Assert.IsFalse(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType3ColumnarSmallBatchFloating()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var b = new { Z = i, A = (char)('b' + i), W = "abc" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, b);
            Assert.IsFalse(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void EqualityComparerExpressionForAnonymousType4ColumnarSmallBatchFloating()
        {
            var i = 3;
            var a = new { Z = i, A = (char)('a' + i), W = "abc" };
            var b = new { Z = i, A = (char)('a' + i), W = "abcd" };
            var f = GetDefaultEqualityComparerExpression(a);
            var g = f.GetEqualsExpr();
            var h = g.Compile();
            var k = h(a, b);
            Assert.IsFalse(k);
        }

        [TestMethod, TestCategory("Gated")]
        public void NaryMulticast1ColumnarSmallBatchFloating()
        {
            var enumerable = Enumerable.Range(0, 10000).ToList();

            var stream = enumerable.ToStreamable();
            var multiStream = stream.Multicast(3);

            var output1 = multiStream[0].Where(e => e % 3 == 0);
            var output2 = multiStream[1].Where(e => e % 3 == 1);
            var output3 = multiStream[2].Where(e => e % 3 == 2);
            var union = output1.Union(output2).Union(output3);

            var output = union.ToPayloadEnumerable().ToList();
            output.Sort();
            Assert.IsTrue(enumerable.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void NaryMulticastContainer1ColumnarSmallBatchFloating()
        {
            var outputList = new List<int>();

            // Inputs
            var container = new QueryContainer();
            Stream state = null;

            // Input data
            var enumerable = Enumerable.Range(0, 10000).ToList();
            var inputObservable = enumerable.ToObservable().Select(e => StreamEvent.CreateStart(0, e));

            // Query start
            var stream = container.RegisterInput(inputObservable);
            var multiStream = stream.Multicast(3);

            var output1 = multiStream[0].Where(e => e % 3 == 0);
            var output2 = multiStream[1].Where(e => e % 3 == 1);
            var output3 = multiStream[2].Where(e => e % 3 == 2);
            var union = output1.Union(output2).Union(output3);

            var outputObservable = container.RegisterOutput(union);

            // Test the output
            var outputAsync = outputObservable.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
            container.Restore(state);
            outputAsync.Wait();

            outputList.Sort();
            Assert.IsTrue(enumerable.SequenceEqual(outputList));
        }

        [TestMethod, TestCategory("Gated")]
        public void NaryMulticastContainerErrorColumnarSmallBatchFloating()
        {
            // Inputs
            var container = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var enumerable = Enumerable.Range(0, 10000).ToList();
            var inputObservable = enumerable.ToObservable().Select(e => StreamEvent.CreateStart(0, e));

            // Query start
            var stream = container.RegisterInput(inputObservable);
            var multiStream = stream.Multicast(3);

            var output1 = multiStream[0].Where(e => e % 3 == 0);
            var output2 = multiStream[1].Where(e => e % 3 == 1);
            var output3 = multiStream[2].Where(e => e % 3 == 2);
            var union = output1.Union(output2).Union(output3);

            var outputObservable = container.RegisterOutput(union);

            // Validate that attempting to start the query before subscriptions will result in an error.
            bool foundException = false;
            try
            {
                container.Restore(state);
            }
            catch (Exception)
            {
                foundException = true;
            }

            Assert.IsTrue(foundException, "Expected an exception.");
        }

        [TestMethod, TestCategory("Gated")]
        public void StreamSortColumnarSmallBatchFloating()
        {
            var input = Enumerable.Range(0, 20).Select(i => (char)('a' + i));
            var reversedInput = input.Reverse();
            var str = reversedInput.ToStatStreamable();
            var sortedStream = str.Sort(c => c);
            var result = sortedStream.ToStreamEventObservable().Where(se => se.IsData).Select(se => se.Payload).ToEnumerable();
            Assert.IsTrue(input.SequenceEqual(result));
            sortedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void SortModifiedStreamColumnarSmallBatchFloating()
        {
            // The Sort method on a stream uses an observable cache. The cache
            // had been putting all rows from the messages into the list to be sorted,
            // even if it was an invalid row.
            var input = Enumerable.Range(0, 20).Select(i => (char)('a' + i));
            var str = input.ToStatStreamable();
            var str2 = str.Where(r => r != 'b');
            var sortedStream = str2.Sort(c => c);
            var result = sortedStream.ToStreamEventObservable().Where(se => se.IsData).Select(se => se.Payload).ToEnumerable();
            Assert.IsTrue(input.Count() == result.Count() + 1); // just make sure deleted element isn't counted anymore
            sortedStream.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void GroupStreamableCodeGenTest1ColumnarSmallBatchFloating()
        {
            var input1 = new[]
            {
                StreamEvent.CreateInterval(100, 101, 13),
                StreamEvent.CreateInterval(100, 110, 11),
                StreamEvent.CreateInterval(101, 105, 12),
                StreamEvent.CreateInterval(102, 112, 21),
                StreamEvent.CreateInterval(109, 112, 14),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };

            var input2 = new[]
            {
                StreamEvent.CreateInterval(100, 110, 'd'),
                StreamEvent.CreateInterval(101, 103, 'b'),
                StreamEvent.CreateInterval(106, 108, 'b'),
                StreamEvent.CreateInterval(106, 109, 'b'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime)
            };

            var inputStream1 = input1.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var inputStream2 = input2.ToObservable().ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var outputStream = inputStream1.WhereNotExists(
                inputStream2,
                l => l / 10,
                r => r - 'a');

            var expected = new[]
            {
                StreamEvent.CreateStart(100, 11),
                StreamEvent.CreateEnd(101, 100, 11),
                StreamEvent.CreateStart(103, 11),
                StreamEvent.CreateEnd(106, 103, 11),
                StreamEvent.CreateInterval(109, 110, 11),
                StreamEvent.CreateInterval(103, 105, 12),
                StreamEvent.CreateInterval(100, 101, 13),
                StreamEvent.CreateInterval(109, 112, 14),
                StreamEvent.CreateStart(102, 21),
                StreamEvent.CreateEnd(112, 102, 21),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };
            var outputEnumerable = outputStream.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void CountColumnar1ColumnarSmallBatchFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<ulong>[]
            {
                StreamEvent.CreateStart<ulong>(StreamEvent.MinSyncTime, 3),
                StreamEvent.CreatePunctuation<ulong>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateEnd<ulong>(StreamEvent.MinSyncTime + 1, StreamEvent.MinSyncTime, 3),
                StreamEvent.CreateStart<ulong>(StreamEvent.MinSyncTime + 1, 6),
                StreamEvent.CreatePunctuation<ulong>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateEnd<ulong>(StreamEvent.MinSyncTime + 2, StreamEvent.MinSyncTime + 1, 6),
                StreamEvent.CreateStart<ulong>(StreamEvent.MinSyncTime + 2, 9),
                StreamEvent.CreatePunctuation<ulong>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None);
            var count = str.Count();
            var outputEnumerable = count.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageColumnar1ColumnarSmallBatchFloating() // tests codegen for Snapshot_noecq
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),      // 97
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),      // 98
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),      // 99  running average = 98

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),  // 100
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),  // 101
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),  // 102 running average = 99.5

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),  // 103
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),  // 104
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),  // 105 running average = 101
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<double>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 1, StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 2, StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 101.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None).SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime);
            var avg = str.Average(c => (double)c);
            var outputEnumerable = avg.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageColumnar2ColumnarSmallBatchFloating() // tests codegen for Snapshot_q
        {
            var zero = StreamEvent.MinSyncTime;
            var one = StreamEvent.MinSyncTime + 1;
            var two = StreamEvent.MinSyncTime + 2;
            var three = StreamEvent.MinSyncTime + 3;
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(zero, one, 'a'),
                StreamEvent.CreateInterval(zero, one, 'b'),
                StreamEvent.CreateInterval(zero, one, 'c'),
                StreamEvent.CreateInterval(one, two, 'd'),
                StreamEvent.CreateInterval(one, two, 'e'),
                StreamEvent.CreateInterval(one, two, 'f'),
                StreamEvent.CreateInterval(two, three, 'g'),
                StreamEvent.CreateInterval(two, three, 'h'),
                StreamEvent.CreateInterval(two, three, 'i'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<double>[]
            {
                StreamEvent.CreateInterval(zero, one, 98.0),
                StreamEvent.CreatePunctuation<double>(one),
                StreamEvent.CreateInterval(one, two, 101.0),
                StreamEvent.CreatePunctuation<double>(two),
                StreamEvent.CreateInterval(two, three, 104.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None).SetProperty().IsConstantDuration(true, 1);
            var avg = str.Average(c => (double)c);
            var outputEnumerable = avg.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void AverageColumnar3ColumnarSmallBatchFloating() // tests codegen for Snapshot_pq
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'a'),      // 97
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'b'),      // 98
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 'c'),      // 99  running average = 98

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'd'),  // 100
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'e'),  // 101
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 'f'),  // 102 running average = 99.5

                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'g'),  // 103
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'h'),  // 104
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 'i'),  // 105 running average = 101
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expected = new StreamEvent<double>[]
            {
                StreamEvent.CreateStart(StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 1),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 1, StreamEvent.MinSyncTime, 98.0),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreatePunctuation<double>(StreamEvent.MinSyncTime + 2),
                StreamEvent.CreateEnd(StreamEvent.MinSyncTime + 2, StreamEvent.MinSyncTime + 1, 99.5),
                StreamEvent.CreateStart(StreamEvent.MinSyncTime + 2, 101.0),
                StreamEvent.CreatePunctuation<double>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1), OnCompletedPolicy.None);
            var avg = str.Average(c => (double)c);
            var outputEnumerable = avg.ToStreamEventObservable().ToEnumerable();
            var output = outputEnumerable.ToArray();
            Assert.IsTrue(expected.IsEquivalentTo(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SumColumnar1ColumnarSmallBatchFloating() // make sure expression is properly parenthesized in codegen
        {
            var input = Enumerable.Range(0, 100);
            Expression<Func<int, int>> query = e => e & 0x7;

            var streamResult = input
                .ToStreamable()
                .Sum(query)
                .ToPayloadEnumerable()
                .Last();

            var expected = input.Sum(query.Compile());
            Assert.IsTrue(expected == streamResult);

        }

        [TestMethod, TestCategory("Gated")]
        public void MultiAggregate1ColumnarSmallBatchFloating() // test codegen for multi-aggregates
        {
            var input = Enumerable.Range(0, 100).Select(i => new MyStruct { field1 = i, field2 = 0.0, field3 = Guid.Empty, });

            var streamResult = input
                .ToStreamable()
                .Aggregate(a => a.Sum(x => x.field1), a => a.Count(), (s, c) => new MyStruct { field1 = s + ((int)c) })
                .ToPayloadEnumerable()
                ;

            Assert.IsTrue(streamResult.Count() == 1);

            Assert.IsTrue(streamResult.First().field1 == 5050);

        }

        [TestMethod, TestCategory("Gated")]
        public void MultiAggregate2ColumnarSmallBatchFloating() // test codegen for multi-aggregates with anonymous type
        {
            var input = Enumerable.Range(0, 100).Select(i => new MyStruct { field1 = i, field2 = 0.0, field3 = Guid.Empty, });

            var streamResult = input
                .ToStreamable()
                .Aggregate(a => a.Sum(x => x.field1), a => a.Count(), (s, c) => new { field1 = s, field2 = c })
                .ToPayloadEnumerable()
                ;

            Assert.IsTrue(streamResult.Count() == 1);

            var f = streamResult.First();
            Assert.IsTrue(f.field1 == 4950 && f.field2 == 100);

        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest1ColumnarSmallBatchFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(24, 3, 'c'),
                StreamEvent.CreateEnd(26, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateInterval(2, 22, 'b'),
                StreamEvent.CreateInterval(3, 24, 'c'),
                StreamEvent.CreateInterval(4, 26, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Cache(0, false, true);
            var result = str.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest2ColumnarSmallBatchFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(20, 1, 'a'),
                StreamEvent.CreateEnd(24, 3, 'c'),
                StreamEvent.CreateEnd(26, 4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateInterval(3, 24, 'c'),
                StreamEvent.CreateInterval(4, 26, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Cache(0, false, true);
            var result = str.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest3ColumnarSmallBatchFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateEnd(22, 2, 'b'),
                StreamEvent.CreateEnd(24, 4, 'd'),
                StreamEvent.CreateEnd(26, 3, 'c'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateInterval(2, 22, 'b'),
                StreamEvent.CreateInterval(3, 26, 'c'),
                StreamEvent.CreateInterval(4, 24, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Cache(0, false, true);
            var result = str.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void BinaryUnionTest1ColumnarSmallBatchFloating()
        {
            var input1 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(2, 20, 'a'),
                StreamEvent.CreateStart(4, 'b'),
                StreamEvent.CreateStart(6, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var input2 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(3, 20, 'a'),
                StreamEvent.CreateStart(5, 'b'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(9, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };

            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(2, 20, 'a'),
                StreamEvent.CreateInterval(3, 20, 'a'),
                StreamEvent.CreateStart(4, 'b'),
                StreamEvent.CreateStart(5, 'b'),
                StreamEvent.CreateStart(6, 'c'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreateStart(9, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str1 = input1.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null).Cache();
            var str2 = input2.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null).Cache();
            var resultEnum = str1.Union(str2).ToStreamEventObservable().ToEnumerable();
            var result = resultEnum.ToList();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str1.Dispose();
            str2.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void BinaryUnionTest2ColumnarSmallBatchFloating()
        {
            var input1 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var input2 = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(5, 20, 'a'),
                StreamEvent.CreateStart(6, 'b'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };

            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 20, 'a'),
                StreamEvent.CreateStart(2, 'b'),
                StreamEvent.CreateStart(3, 'c'),
                StreamEvent.CreateStart(4, 'd'),
                StreamEvent.CreateInterval(5, 20, 'a'),
                StreamEvent.CreateStart(6, 'b'),
                StreamEvent.CreateStart(7, 'c'),
                StreamEvent.CreateStart(8, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str1 = input1.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation).Cache();
            var str2 = input2.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation).Cache();
            var result = str1.Union(str2).ToStreamEventObservable().ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
            str1.Dispose();
            str2.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void DeterministicUnionTestColumnarSmallBatchFloating()
        {
            int count = 100;
            DateTime now = DateTime.UtcNow;

            var input = new List<StreamEvent<int>> { StreamEvent.CreatePunctuation<int>(now.Ticks) };

            // Add right events.
            for (int i = 0; i < count; ++i)
                input.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i + 1));

            // Cti forces right batch to go through before reaching Config.DataBatchSize.
            input.Add(StreamEvent.CreatePunctuation<int>(now.Ticks));

            // Add left events.
            for (int i = 0; i < count; ++i)
                input.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i));
            input.Add(StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime));

            // Expect left before right even though right went through first.
            var expectedOutput = new List<StreamEvent<int>> { StreamEvent.CreatePunctuation<int>(now.Ticks) };
            for (int i = 0; i < count; i++)
                expectedOutput.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i));
            for (int i = 0; i < count; i++)
                expectedOutput.Add(StreamEvent.CreatePoint(now.Ticks, 2 * i + 1));
            expectedOutput.Add(StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime));

            var inputStreams = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Multicast(2);

            var left = inputStreams[0].Where(e => e % 2 == 0);
            var right = inputStreams[1].Where(e => e % 2 == 1);

            var result = left
                .Union(right)
                .ToStreamEventObservable()
                .ToEnumerable()
                .ToList();

            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest4ColumnarSmallBatchFloating()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateStart(1, 'a'),
                StreamEvent.CreateEnd(5, 1, 'a'),
                StreamEvent.CreateStart(6, 'b'),
                StreamEvent.CreateEnd(10, 6, 'b'),
                StreamEvent.CreateStart(11, 'c'),
                StreamEvent.CreateEnd(15, 11, 'c'),
                StreamEvent.CreateStart(16, 'd'),
                StreamEvent.CreateEnd(20, 16, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var expectedOutput = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 5, 'a'),
                StreamEvent.CreateInterval(6, 10, 'b'),
                StreamEvent.CreateInterval(11, 15, 'c'),
                StreamEvent.CreateInterval(16, 20, 'd'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation);
            var result = str.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).ToEnumerable().ToArray();
            Assert.IsTrue(expectedOutput.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void AggregateTest1ColumnarSmallBatchFloating()
        {
            long threeSecondTicks = TimeSpan.FromSeconds(3).Ticks;

            var stream = Events().ToEvents(e => e.Vs.Ticks, e => e.Vs.Ticks + threeSecondTicks)
                                 .ToObservable()
                                 .ToStreamable(DisorderPolicy.Adjust(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None());
            var expectedResult = new StreamEvent<ResultEvent>[]
            {
                StreamEvent.CreateStart(633979008010000000, new ResultEvent { Key = 1, Cnt = 1 }),
                StreamEvent.CreateEnd(633979008020000000, 633979008010000000, new ResultEvent { Key = 1, Cnt = 1 }),
                StreamEvent.CreateStart(633979008020000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateEnd(633979008030000000, 633979008020000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateStart(633979008030000000, new ResultEvent { Key = 1, Cnt = 3 }),
                StreamEvent.CreateEnd(633979008040000000, 633979008030000000, new ResultEvent { Key = 1, Cnt = 3 }),
                StreamEvent.CreateStart(633979008040000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateEnd(633979008050000000, 633979008040000000, new ResultEvent { Key = 1, Cnt = 2 }),
                StreamEvent.CreateStart(633979008050000000, new ResultEvent { Key = 1, Cnt = 1 }),
                StreamEvent.CreateEnd(633979008060000000, 633979008050000000, new ResultEvent { Key = 1, Cnt = 1 }),
            };
            var counts = stream.GroupApply(
                                    g => g.Key,
                                    e => e.Count(),
                                    (g, e) => new ResultEvent { Key = g.Key, Cnt = e })
                               .ToStreamEventObservable()
                               .ToEnumerable()
                               .Where(e => e.IsData && e.Payload.Key == 1);
            foreach (var x in counts)
            {
                Console.WriteLine("{0}, {1}", x.SyncTime, x.OtherTime);
            }
            Assert.IsTrue(expectedResult.SequenceEqual(counts));
        }

        public IEnumerable<Event> Events()
        {
            var vs = new DateTime(2010, 1, 1, 0, 00, 00, DateTimeKind.Utc);
            var rand = new Random(0);
            int eventCnt = 5;
            for (int i = 0; i < eventCnt; i++)
            {
                yield return new Event { Vs = vs, V = rand.Next(10), Key = rand.Next(3) };
                vs = vs.AddSeconds(1);
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void CodeGenWithFieldOfGenericTypeColumnarSmallBatchFloating()
        {
            // Taken from the PerformanceTest sample.
            // The GroupApply creates batches where the key is a generic type and codegen was doing a type cast without making sure the type's name was valid C# syntax
            var numEventsPerTumble = 2000;
            var tumblingWindowDataset =
                Observable.Range(0, 100)
                    .Select(e => StreamEvent.CreateInterval(((long)e / numEventsPerTumble) * numEventsPerTumble, ((long)e / numEventsPerTumble) * numEventsPerTumble + numEventsPerTumble, new Payload { field1 = 0, field2 = 0 }))
                    .ToStreamable()
                    .SetProperty().IsConstantDuration(true, numEventsPerTumble)
                    .SetProperty().IsSnapshotSorted(true, e => e.field1);
            var result = tumblingWindowDataset.GroupApply(e => e.field2, str => str.Count(), (g, c) => new { Key = g, Count = c });
            var a = result.ToStreamEventObservable().ToEnumerable().ToArray();
            Assert.IsTrue(a.Length == 3); // just make sure it doesn't crash
        }

        [TestMethod, TestCategory("Gated")]
        public void StringSerializationColumnarSmallBatchFloating()
        {
            var rand = new Random(0);
            var pool = MemoryManager.GetColumnPool<string>();

            for (int x = 0; x < 100; x++)
            {
                pool.Get(out ColumnBatch<string> inputStr);

                var toss1 = rand.NextDouble();
                inputStr.UsedLength = toss1 < 0.1 ? 0 : rand.Next(Config.DataBatchSize);

                for (int i = 0; i < inputStr.UsedLength; i++)
                {
                    var toss = rand.NextDouble();
                    inputStr.col[i] = toss < 0.2 ? string.Empty : (toss < 0.4 ? null : Guid.NewGuid().ToString());
                    if (x == 0) inputStr.col[i] = null;
                    if (x == 1) inputStr.col[i] = string.Empty;
                }

                StateSerializer<ColumnBatch<string>> s = StreamableSerializer.Create<ColumnBatch<string>>(new SerializerSettings { });
                var ms = new MemoryStream { Position = 0 };

                s.Serialize(ms, inputStr);
                ms.Position = 0;
                ColumnBatch<string> resultStr = s.Deserialize(ms);

                Assert.IsTrue(resultStr.UsedLength == inputStr.UsedLength);

                for (int j = 0; j < inputStr.UsedLength; j++)
                {
                    Assert.IsTrue(inputStr.col[j] == resultStr.col[j]);
                }
                resultStr.ReturnClear();
                inputStr.ReturnClear();
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void CharArraySerializationColumnarSmallBatchFloating()
        {
            using (var modifier = new ConfigModifier().SerializationCompressionLevel(SerializationCompressionLevel.CharArrayToUTF8).Modify())
            {
                var rand = new Random(0);

                for (int x = 0; x < 5; x++)
                {
                    var inputStr = new MultiString();

                    var toss1 = rand.NextDouble();
                    var usedLength = 1 + rand.Next(Config.DataBatchSize - 1);

                    for (int i = 0; i < usedLength; i++)
                    {
                        var toss = rand.NextDouble();
                        string str = toss < 0.2 ? string.Empty : Guid.NewGuid().ToString();
                        if (x == 0) str = string.Empty;
                        inputStr.AddString(str);
                    }

                    inputStr.Seal();

                    StateSerializer<MultiString> s = StreamableSerializer.Create<MultiString>(new SerializerSettings { });
                    var ms = new MemoryStream { Position = 0 };

                    s.Serialize(ms, inputStr);
                    ms.Position = 0;
                    var resultStr = s.Deserialize(ms);

                    Assert.IsTrue(resultStr.Count == inputStr.Count);

                    for (int j = 0; j < inputStr.Count; j++)
                    {
                        Assert.IsTrue(inputStr[j] == resultStr[j]);
                    }
                    resultStr.Dispose();
                    inputStr.Dispose();
                }
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void MultipleAggregatesWithCompiledDictionariesColumnarSmallBatchFloating()
        {
            IEnumerable<StreamEvent<StreamScope_InputEvent0>> bacon = Array.Empty<StreamEvent<StreamScope_InputEvent0>>();
            IObservableIngressStreamable<StreamScope_InputEvent0> input = bacon.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);
            var result =
                input
                .GroupAggregate(
                    s => new { s.OutputTs, s.DelayInMilliseconds, s.UserId },
                    s => s.Count(),
                    s => s.Min(r => r.Duration),
                    s => s.Max(r => r.Duration),
                    s => s.Average(r => r.Duration),
                    (key, cnt, minDuration, maxDuration, avgDuration) => new StreamScope_OutputEvent0()
                    {
                        OutputTs = key.Key.OutputTs,
                        DelayInMilliseconds = key.Key.DelayInMilliseconds,
                        UserId = key.Key.UserId,
                        Cnt = (long)cnt,
                        MinDuration = minDuration,
                        MaxDuration = maxDuration,
                        AvgDuration = avgDuration,
                    }).ToStreamEventObservable();

            var b = result;
        }

        [TestMethod, TestCategory("Gated")]
        public void PayloadWithFieldOfNestedType01ColumnarSmallBatchFloating()
        {
            var input = Enumerable
                .Range(0, NumEvents)
                .Select(i => new PayloadWithFieldOfNestedType1(i))
                ;
            var streamResult = input
                .ToStreamable()
                .Where(p => p.nt.x % 2 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var linqResult = input.Where(p => p.nt.x % 2 == 0);
            Assert.IsTrue(Enumerable.Zip(linqResult, a, (p1, p2) => p1.nt.x == p2.nt.x).All(p => p));
        }
        [TestMethod, TestCategory("Gated")]
        public void PayloadWithFieldOfNestedType02ColumnarSmallBatchFloating()
        {
            var input = Enumerable
                .Range(0, NumEvents)
                .Select(i => new PayloadWithFieldOfNestedType2(i))
                ;
            var streamResult = input
                .ToStreamable()
                .Where(p => p.nt.x % 2 == 0)
                .ToPayloadEnumerable();

            var a = streamResult.ToArray();
            var linqResult = input.Where(p => p.nt.x % 2 == 0);
            Assert.IsTrue(Enumerable.Zip(linqResult, a, (p1, p2) => p1.nt.x == p2.nt.x).All(p => p));
        }
    }
}
