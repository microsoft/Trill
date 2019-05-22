// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public static class Extensions
    {
        public static IStreamable<Empty, T> ToStreamable<T>(
            this IEnumerable<T> enumerable,
            int startEdge = 0)
            => enumerable
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(startEdge, e))
                .ToStreamable();

        public static bool IsEquivalentTo<TPayload>(this IStreamable<Empty, TPayload> input, StreamEvent<TPayload>[] comparison)
        {
            Invariant.IsNotNull(input, "input");
            Invariant.IsNotNull(comparison, "comparison");
            var events = input.ToStreamEventArray();
            var orderedEvents = events.OrderBy(e => e.SyncTime).ThenBy(e => e.OtherTime).ThenBy(e => e.Payload).ToArray();
            var orderedComparison = comparison.OrderBy(e => e.SyncTime).ThenBy(e => e.OtherTime).ThenBy(e => e.Payload).ToArray();

            return orderedEvents.SequenceEqual(orderedComparison);
        }

        public static StreamEvent<TPayload>[] ToStreamEventArray<TPayload>(this IStreamable<Empty, TPayload> input)
        {
            Invariant.IsNotNull(input, "input");
            return input.ToStreamEventObservable().ToEnumerable().ToArray();
        }

        public static IStreamable<Empty, TPayload> ToCleanStreamable<TPayload>(this StreamEvent<TPayload>[] input)
        {
            Invariant.IsNotNull(input, "input");
            return input.OrderBy(v => v.SyncTime).ToArray().ToStreamable();
        }

        public static IStreamable<Empty, TPayload> ToStreamable<TPayload>(this StreamEvent<TPayload>[] input)
        {
            Invariant.IsNotNull(input, "input");

            return input.ToObservable()
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation);
        }

        public static IEnumerable<T> TrillWhere<T>(IEnumerable<T> enumerable, Expression<Func<T, bool>> wherePredicate, bool disableFusion = true)
        {
            var temp = enumerable.ToStreamable();
            if (disableFusion) temp = temp.AlterEventLifetime(s => s, StreamEvent.InfinitySyncTime);
            var result = temp.Where(wherePredicate)
                .ToPayloadEnumerable();
            return result;
        }
        public static bool TestWhere<T>(this IEnumerable<T> enumerable, Expression<Func<T, bool>> wherePredicate, bool disableFusion = true)
        {
            var linqResult = enumerable.Where(wherePredicate.Compile());
            var streamResult = TrillWhere(enumerable, wherePredicate, disableFusion);
            return linqResult.SequenceEqual(streamResult);
        }

        public static IEnumerable<U> TrillSelect<T, U>(IEnumerable<T> enumerable, Expression<Func<T, U>> selectFunction, bool disableFusion = true)
        {
            var temp = enumerable.ToStreamable();
            if (disableFusion) temp = temp.AlterEventLifetime(s => s, StreamEvent.InfinitySyncTime);
            var result = temp.Select(selectFunction)
                .ToPayloadEnumerable();
            return result;
        }

        public static bool TestSelect<T, U>(this IEnumerable<T> enumerable, Expression<Func<T, U>> selectFunction, bool disableFusion = true)
        {
            var linqResult = enumerable.Select(selectFunction.Compile());
            var streamResult = TrillSelect(enumerable, selectFunction, disableFusion);
            var a = streamResult.ToArray();
            var b = linqResult.ToArray();
            return a.SequenceEqual(b);
        }

        public static bool TestWhereSelect<T, U>(this IEnumerable<T> enumerable, Expression<Func<T, bool>> wherePredicate, Expression<Func<T, U>> selectFunction, bool disableFusion = true)
        {
            var linqResult = enumerable.Where(wherePredicate.Compile()).Select(selectFunction.Compile());
            var trillTemp = enumerable.ToStreamable();
            if (disableFusion) trillTemp = trillTemp.AlterEventLifetime(s => s, StreamEvent.InfinitySyncTime);
            var streamResult = trillTemp.Where(wherePredicate)
                .Select(selectFunction)
                .ToPayloadEnumerable();
            return linqResult.SequenceEqual(streamResult);
        }

        public static IEnumerable<long> CountToInfinity(this long i) { var x = i; while (true) yield return x++; }
        public static IStreamable<Empty, T> ToStatStreamable<T>(this IEnumerable<T> source)
        {
            var batchSize = 80000;
            var timeStamps = StreamEvent.MinSyncTime.CountToInfinity().SelectMany(i => Enumerable.Repeat(i, batchSize));
            var streamEventEnumerable = Enumerable.Zip(timeStamps, source, (t, e) => StreamEvent.CreateStart(t, e))
                .Concat(new StreamEvent<T>[] { StreamEvent.CreatePunctuation<T>(StreamEvent.InfinitySyncTime), });
            var stream = streamEventEnumerable
                .ToObservable(Scheduler.Default)
                .ToStreamable(null, FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.Time(1))
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime);
            return stream;
        }
        public static StreamCache<Empty, T> Sort<T, U>(this IStreamable<Empty, T> stream, Expression<Func<T, U>> sortBySelector)
        {
            var u_comparer = Utility.CreateCompoundComparer(sortBySelector, ComparerExpression<U>.Default.GetCompareExpr()).Compile();
            IStreamable<Empty, T> stream2 = stream;
            stream.GetProperties(out StreamProperties<Empty, T> properties);
            if (!properties.IsStartEdgeOnly)
            {
                var secondStream = new StreamEvent<T>[]
                {
                    StreamEvent.CreateStart(StreamEvent.MaxSyncTime, default(T)),
                    StreamEvent.CreatePunctuation<T>(StreamEvent.InfinitySyncTime),
                }.ToObservable().ToStreamable();

                stream2 = stream.Join(secondStream, (x, y) => x);
            }
            var observableCache = stream2.ToPayloadEnumerable().ToList();
            observableCache.Sort(new Comparison<T>(u_comparer));
            var str = observableCache.ToObservable()
                .ToAtemporalStreamable(TimelinePolicy.Sequence(80000))
                ;
            var str2 = str
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                ;
            var str3 = str2.Cache();
            return str3;
        }

        public static bool IsEquivalentTo<T>(this IEnumerable<StreamEvent<T>> a, IEnumerable<StreamEvent<T>> b)
        {
            Contract.Requires(a != null);
            Contract.Requires(b != null);
            var orderedA = a.OrderBy(e => e.SyncTime).ThenBy(e => e.OtherTime).ThenBy(e => e.Payload);
            var orderedB = b.OrderBy(e => e.SyncTime).ThenBy(e => e.OtherTime).ThenBy(e => e.Payload);
            return orderedA.SequenceEqual(orderedB);
        }

        public static bool IsOrdered<T, TKey>(this IEnumerable<T> source, Func<T, TKey> keySelector)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));

            var comparer = Comparer<TKey>.Default;
            using (var iterator = source.GetEnumerator())
            {
                if (!iterator.MoveNext())
                    return true;

                TKey current = keySelector(iterator.Current);

                while (iterator.MoveNext())
                {
                    TKey next = keySelector(iterator.Current);
                    if (comparer.Compare(current, next) > 0)
                        return false;

                    current = next;
                }
            }

            return true;
        }
    }

    public static class MyTestExtensions
    {
        public static IEnumerable<StreamEvent<T>> ToEvents<T>(this IEnumerable<T> input, Func<T, long> vsSelector, Func<T, long> veSelector)
        {
            foreach (var e in input)
            {
                yield return StreamEvent.CreateInterval(vsSelector(e), veSelector(e), e);
            }
        }
    }

    public struct MyStruct
    {
        public int field1;
        public double field2;
        public Guid field3;
    }

    public class MyString
    {
        public string mystring;

        public MyString(string str) => this.mystring = str;
        public MyString()
        {
            // needed because the generated code calls the nullary ctor
        }
        public override bool Equals(object obj) => !(obj is MyString x) ? false : this.mystring.Equals(x.mystring);

        public override int GetHashCode() => this.mystring.GetHashCode();

        public override string ToString() => string.Format("MyString(\"{0}\")", this.mystring);
    }

    public struct NestedStruct
    {
        public int nestedField;

        public override string ToString() => string.Format("NestedStruct({0})", this.nestedField);
    }

    public struct WideStruct1
    {
        public string Key1;
        public int? i1;
        public int? i2;
        public int? i3;
        public int? i4;
        public int? i5;
        public int? i6;
        public int? i7;
        public int? i8;
        public int? i9;
        public int? i10;
        public int? i11;
        public int? i12;
        public int? i13;
        public int? i14;
        public int? i15;
        public int? o;

        public static IEnumerable<WideStruct1> GetTestData()
        {
            yield return new WideStruct1 { Key1 = "a", i5 = 5, i9 = 9 };
            yield return new WideStruct1 { Key1 = "b", i1 = 11, i14 = 100, i2 = 9 };
            yield return new WideStruct1 { Key1 = "c", i15 = 0, o = 9 };
        }
    }

    public struct WideStruct2
    {
        public string Key1;
        public string Key2;
        public int? i1;
        public int? i2;
        public int? i3;
        public int? i4;
        public int? i5;
        public int? i6;
        public int? i7;
        public int? i8;
        public int? i9;
        public int? i10;
        public int? i11;
        public int? i12;
        public int? i13;
        public int? i14;
        public int? i15;
        public int? o;

        public static IEnumerable<WideStruct2> GetTestData()
        {
            yield return new WideStruct2 { Key1 = "a", Key2 = "d", i5 = 5, i9 = 9 };
            yield return new WideStruct2 { Key1 = "b", Key2 = "d", i1 = 11, i14 = 100, i2 = 9 };
            yield return new WideStruct2 { Key1 = "c", Key2 = "e", i15 = 0, o = 9 };
        }
    }

    public class NonColumnarClass
    {
        public int x;
        public NonColumnarClass(int y, char c) => this.x = y;
        public override bool Equals(object obj)
        {
            // See the full q of guidelines at http://go.microsoft.com/fwlink/?LinkID=85237
            // and also the guidance for operator== at http://go.microsoft.com/fwlink/?LinkId=85238
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }

            var typedObj = (NonColumnarClass)obj;

            return this.x.Equals(typedObj.x);
        }

        public override int GetHashCode() => this.x.GetHashCode();
    }

    public struct MyStruct2
    {
        public int field1;
        public MyString field2;
        public NestedStruct field3;
        public double doubleField;
        public char[] charArrayField;
        public override string ToString() => string.Format("MyStruct({0}, {1}, {2}, {3})", this.field1, this.field2.mystring, this.field3.nestedField, this.doubleField);
        public StructWithCtor ReturnStructWithCtor(int x) => new StructWithCtor(x);
    }

    public struct StructWithCtor
    {
        public int f;
        public StructWithCtor(int x) => this.f = x;
    }

    public abstract class TestWithConfigSettingsAndMemoryLeakDetection
    {
        private IDisposable confmod = null;
        private readonly ConfigModifier modifier;

        internal TestWithConfigSettingsAndMemoryLeakDetection() : this(new ConfigModifier()) { }

        internal TestWithConfigSettingsAndMemoryLeakDetection(ConfigModifier modifier) => this.modifier = modifier;

        [TestInitialize]
        public virtual void Setup()
        {
            this.confmod = this.modifier.Modify();
            MemoryManager.Free(true);
        }

        [TestCleanup]
        public virtual void TearDown()
        {
            var cm = System.Threading.Interlocked.Exchange(ref this.confmod, null);
            if (cm != null) { cm.Dispose(); }
            var leaked = MemoryManager.Leaked().Any();
            var msg = string.Empty;
            if (leaked)
            {
                msg = "Memory Leak!\n" + MemoryManager.GetStatusReport();
            }
            Assert.IsFalse(leaked, msg);
        }
    }

    internal class Program
    {
        public static Expression EqualsExprForAnonymousType(Type t)
        {
            if (t == null || !t.IsAnonymousTypeName()) return null;
            var properties = t.GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance); // BUG: need to order them correctly!
            var left = Expression.Parameter(t, "left");
            var right = Expression.Parameter(t, "right");
            var objEqualsMethodInfo = typeof(object).GetMethod("Equals", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public);
            var equalsClauses = properties.Select(p =>
                Expression.Call(Expression.Property(left, p), objEqualsMethodInfo, Expression.Convert(Expression.Property(right, p), typeof(object))));
            var lambdaBody = equalsClauses.Aggregate<Expression, Expression>(Expression.Constant(true, typeof(bool)), (e1, e2) => Expression.AndAlso(e1, e2));
            return Expression.Lambda(lambdaBody, left, right);
        }

        public static Expression HashCodeExprForAnonymousType(Type t)
        {
            if (t == null || !t.IsAnonymousTypeName()) return null;
            var properties = t.GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
            var a = Expression.Parameter(t, "a");
            var objGetHashCodeMethodInfo = typeof(object).GetMethod("GetHashCode", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public);
            var hashClauses = (IEnumerable<Expression>)properties.Select(p =>
                Expression.Call(Expression.Property(a, p), objGetHashCodeMethodInfo));
            var lambdaBody = hashClauses.Aggregate((e1, e2) => Expression.ExclusiveOr(e1, e2));
            return Expression.Lambda(lambdaBody, a);
        }

        public static Expression ComparerExprForAnonymousType(Type t)
        {
            if (t == null || !t.IsAnonymousTypeName()) return null;
            var properties = t.GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
            if (properties.Length == 0) return null;
            var left = Expression.Parameter(t, "left");
            var right = Expression.Parameter(t, "right");
            var n = properties.Length;
            var p = properties[n - 1];
            var negativeOne = Expression.Constant(-1, typeof(int));
            var positiveOne = Expression.Constant(1, typeof(int));
            var zero = Expression.Constant(0, typeof(int));
            Expression body = Expression.Condition(Expression.LessThan(Expression.Property(left, p), Expression.Property(right, p)), negativeOne,
                Expression.Condition(Expression.Equal(Expression.Property(left, p), Expression.Property(right, p)), zero, positiveOne));
            for (int i = n - 2; i >= 0; i--)
            {
                p = properties[i];
                body = Expression.Condition(Expression.LessThan(Expression.Property(left, p), Expression.Property(right, p)), negativeOne,
                Expression.Condition(Expression.Equal(Expression.Property(left, p), Expression.Property(right, p)), body, positiveOne));
            }
            return Expression.Lambda(body, left, right);
        }

        public static bool TestEquality<T, U>(IEnumerable<T> enumerable, Expression<Func<T, U>> selectFunction)
        {
            var foo = enumerable.Select(selectFunction.Compile());
            var a = foo.ElementAt(0);
            var b = foo.ElementAt(1);
            var expr = EqualsExprForAnonymousType(typeof(U)) as Expression<Func<U, U, bool>>;
            var result = expr.Compile()(a, b);
            return result;
        }

        public static int TestHash<T, U>(IEnumerable<T> enumerable, Expression<Func<T, U>> selectFunction)
        {
            var foo = enumerable.Select(selectFunction.Compile());
            var a = foo.ElementAt(0);
            var expr = HashCodeExprForAnonymousType(typeof(U)) as Expression<Func<U, int>>;
            var result = expr.Compile()(a);
            return result;
        }

        public static int TestComparer<T, U>(IEnumerable<T> enumerable, Expression<Func<T, U>> selectFunction)
        {
            var foo = enumerable.Select(selectFunction.Compile());
            var a = foo.ElementAt(0);
            var b = foo.ElementAt(1);
            var expr = ComparerExprForAnonymousType(typeof(U)) as Expression<Func<U, U, int>>;
            var result = expr.Compile()(a, b);
            return result;
        }

        private static void Alpha()
        {
            var x3 = Utility.CreateCompoundComparer<int, MyStruct2>(e => e.field1, (x, y) => (x < y) ? -1 : (x == y) ? 0 : 1);
            var s = x3.ExpressionToCSharp();
        }

        public static void Main(string[] args)
        {
            Alpha();
            var meths = typeof(int).GetMethods();
            var x = new { A = 3, B = 'a' };
            var y = EqualsExprForAnonymousType(x.GetType());
            var z = TestEquality(Enumerable.Range(0, 5), i => new { X = i, Y = (char)('a' + i), });
            z = TestEquality(new int[] { 3, 3, 3 }, i => new { Z = i, A = (char)('a' + i), W = "abc", });
            var ii = TestHash(new int[] { 3, 3, 3 }, i => new { Z = i, A = (char)('a' + i), W = "abc", });
            ii = TestComparer(new int[] { 3, 3, 3 }, i => new { Z = i, A = (char)('a' + i), W = "abc" });
            NativeMethods.AffinitizeThread(0);
            Config.ForceRowBasedExecution = true;

            int numEvents = 500000;
            var leftStream = Observable.Range(0, numEvents / 1000).Select(e => StreamEvent.CreateStart(0, e * 1000)).ToStreamable();
            var rightStream = Observable.Range(0, numEvents).Select(e => StreamEvent.CreateStart(0, e)).ToStreamable();

            Console.WriteLine("Preloaded");
            var sw = new Stopwatch();
            sw.Start();
            var query = leftStream.Join(rightStream, e => e, e => e, (l, r) => l);
            query.ToStreamMessageObservable().ForEachAsync(e => { e.Free(); }).Wait();
            sw.Stop();

            Console.WriteLine("Throughput: {0}K ev/sec", numEvents / sw.ElapsedMilliseconds);
            Console.ReadLine();
        }
    }

    [DataContract]
    public struct StreamScope_InputEvent0
    {
        [DataMember]
        public DateTime OutputTs;
        [DataMember]
        public int DelayInMilliseconds;
        [DataMember]
        public DateTime Ts;
        [DataMember]
        public string UserId;
        [DataMember]
        public int Duration;
    }

    [DataContract]
    public struct StreamScope_OutputEvent0
    {
        [DataMember]
        public long Cnt;
        [DataMember]
        public int MinDuration;
        [DataMember]
        public int MaxDuration;
        [DataMember]
        public double? AvgDuration;
        [DataMember]
        public DateTime OutputTs;
        [DataMember]
        public int DelayInMilliseconds;
        [DataMember]
        public string UserId;
    }
}