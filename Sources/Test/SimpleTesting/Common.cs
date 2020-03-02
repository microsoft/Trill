// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Sharding;
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

        public static IEnumerable<long> CountToInfinity(this long initialValue)
            => Enumerable.Range(0, int.MaxValue).Select(i => i + initialValue);

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

        public static IEnumerable<StreamEvent<T>> ToEvents<T>(this IEnumerable<T> input, Func<T, long> vsSelector, Func<T, long> veSelector)
        {
            foreach (var e in input)
            {
                yield return StreamEvent.CreateInterval(vsSelector(e), veSelector(e), e);
            }
        }

#if NET472
        // string.Split has different signatures across .NET Framework/Core, so use an extension so we can use a single signature
        public static string[] Split(this string original, char separator, StringSplitOptions options) => original.Split(separator);
#endif
    }

    public static class Helpers
    {
        public static void RunTwiceForRowAndColumnar(Action action)
        {
            var savedForceRowBasedExecution = Config.ForceRowBasedExecution;
            var savedRowFallback = Config.CodegenOptions.DontFallBackToRowBasedExecution;
            try
            {
                foreach (var rowBased in new bool[] { true, false })
                {
                    Config.ForceRowBasedExecution = rowBased;
                    Config.CodegenOptions.DontFallBackToRowBasedExecution = !rowBased;
                    action();
                }
            }
            finally
            {
                Config.ForceRowBasedExecution = savedForceRowBasedExecution;
                Config.CodegenOptions.DontFallBackToRowBasedExecution = savedRowFallback;
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

    public abstract class TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        private IDisposable confmod = null;
        private readonly ConfigModifier modifier;

        protected TestWithConfigSettingsWithoutMemoryLeakDetection() : this(new ConfigModifier()) { }

        internal TestWithConfigSettingsWithoutMemoryLeakDetection(ConfigModifier modifier) => this.modifier = modifier;

        [TestInitialize]
        public virtual void Setup() => this.confmod = this.modifier.Modify();

        [TestCleanup]
        public virtual void TearDown()
        {
            var cm = System.Threading.Interlocked.Exchange(ref this.confmod, null);
            if (cm != null) { cm.Dispose(); }
        }
    }
}