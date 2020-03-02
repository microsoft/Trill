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