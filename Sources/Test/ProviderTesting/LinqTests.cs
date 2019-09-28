// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Aggregates;
using Microsoft.StreamProcessing.Provider;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ProviderTesting
{
    [TestClass]
    public class LinqTests
    {
        [TestMethod]
        public void PassthroughTest()
        {
            var input = Enumerable.Range(0, 2000);
            var output = new List<int>();
            var observable = input.ToObservable();
            var container = new QueryContainer();
            var stream = container.RegisterStream(observable, o => o, o => o + 1);
            var task = stream.ToTemporalObservable((s, e, p) => p).ForEachAsync(o => output.Add(o));
            container.Restore();
            task.Wait();

            Assert.IsTrue(input.SequenceEqual(output));
        }

        [TestMethod]
        public void Where()
        {
            var input = Enumerable.Range(1, 10)
                .Select(i => ValueTuple.Create((long)i /*start*/, (long)i + 1 /*end*/, i.ToString() /*payload*/));
            var inputObservable = input.ToObservable();
            var output = new List<ValueTuple<long, long, string>>();

            var queryContainer = new QueryContainer();
            var qstreamable = queryContainer.RegisterStream(inputObservable, o => o.Item1, o => o.Item2);
            var qstreamableFiltered =
                from t in qstreamable
                where t.Item1 % 2 == 0
                select t.Item3;

            var results = qstreamableFiltered
                .ToTemporalObservable((start, end, payload) => ValueTuple.Create(start, end, payload))
                .ForEachAsync(o => output.Add(o));
            queryContainer.Restore();
            results.Wait();

            // Validate against the same IEnumerable query
            var expected =
                (from t in input
                 where t.Item1 % 2 == 0
                 select ValueTuple.Create(t.Item1, t.Item2, t.Item3))
                .ToList();
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod]
        public void GroupBy()
        {
            var input = Enumerable.Range(1, 100);
            var inputObservable = input.ToObservable();
            var output = new List<int>();

            var queryContainer = new QueryContainer();
            var qstreamable = queryContainer.RegisterStream(inputObservable, o => 0, o => 1);
            var qstreamableCount =
                from t in qstreamable
                group t by (t % 3) into g
                select g.Key;

            var results = qstreamableCount
                .ToTemporalObservable((start, end, payload) => payload)
                .ForEachAsync(o => output.Add(o));

            queryContainer.Restore();
            results.Wait();

            // Validate against the same IEnumerable query
            var expected =
                (from t in input
                 group t by (t % 3) into g
                 select g.Key)
                .ToList();
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod]
        public void AggregateCount()
        {
            var input = Enumerable.Range(1, 100);
            var inputObservable = input.ToObservable();
            var output = new List<int>();

            var queryContainer = new QueryContainer();
            var qstreamable = queryContainer.RegisterStream(inputObservable, o => 0, o => 1);
            var qstreamableCount =
                from t in qstreamable
                group t by (t % 3) into g
                select g.Count();

            var results = qstreamableCount
                .ToTemporalObservable((start, end, payload) => payload)
                .ForEachAsync(o => output.Add(o));

            queryContainer.Restore();
            results.Wait();

            Assert.IsTrue(new int[] { 33, 34, 33 }.SequenceEqual(output));
        }

        [TestMethod]
        public void AggregateProduct()
        {
            var input = Enumerable.Range(0, 100).Select(i => (long)i);
            var inputObservable = input.ToObservable();
            var output = new List<ValueTuple<string, long>>();

            var queryContainer = new QueryContainer();
            var qstreamable = queryContainer.RegisterStream(inputObservable, o => 0, o => 1);
            var qstreamableCount =
                from t in qstreamable
                group t by (t % 10).ToString() into g
                select ValueTuple.Create(g.Key, g.Product());

            var results = qstreamableCount
                .ToTemporalObservable((start, end, payload) => payload)
                .ForEachAsync(o => output.Add(o));

            queryContainer.Restore();
            results.Wait();

            // Validate against the same IEnumerable query
            var expected =
                (from t in input
                 group t by (t % 10).ToString() into g
                 select ValueTuple.Create(g.Key, g.Product()))
                .ToList();
            Assert.IsTrue(expected.SequenceEqual(output.OrderBy(o => o.Item1)));
        }

        [TestMethod]
        public void AggregateSum()
        {
            var input = Enumerable.Range(0, 100).Select(i => (long)i);
            var inputObservable = input.ToObservable();
            var output = new List<ValueTuple<string, long>>();

            var queryContainer = new QueryContainer();
            var qstreamable = queryContainer.RegisterStream(inputObservable, o => 0, o => 1);
            var qstreamableCount =
                from t in qstreamable
                group t by (t % 10).ToString() into g
                select ValueTuple.Create(g.Key, g.Sum());

            var results = qstreamableCount
                .ToTemporalObservable((start, end, payload) => payload)
                .ForEachAsync(o => output.Add(o));

            queryContainer.Restore();
            results.Wait();

            // Validate against the same IEnumerable query
            var expected =
                (from t in input
                 group t by (t % 10).ToString() into g
                 select ValueTuple.Create(g.Key, g.Sum()))
                .ToList();
            Assert.IsTrue(expected.SequenceEqual(output.OrderBy(o => o.Item1)));
        }

        [TestMethod]
        public void AggregateAverage()
        {
            var input = Enumerable.Range(0, 100).Select(i => (long)i);
            var inputObservable = input.ToObservable();
            var output = new List<ValueTuple<string, double>>();

            var queryContainer = new QueryContainer();
            var qstreamable = queryContainer.RegisterStream(inputObservable, o => 0, o => 1);
            var qstreamableCount =
                from t in qstreamable
                group t by (t % 10).ToString() into g
                select ValueTuple.Create(g.Key, g.Average());

            var results = qstreamableCount
                .ToTemporalObservable((start, end, payload) => payload)
                .ForEachAsync(o => output.Add(o));

            queryContainer.Restore();
            results.Wait();

            // Validate against the same IEnumerable query
            var expected =
                (from t in input
                 group t by (t % 10).ToString() into g
                 select ValueTuple.Create(g.Key, g.Average()))
                .ToList();
            Assert.IsTrue(expected.SequenceEqual(output.OrderBy(o => o.Item1)));
        }

        [TestMethod]
        public void AggregateNullableProduct()
        {
            var input = Enumerable.Range(0, 100).Select(i => (long?)i);
            var inputObservable = input.ToObservable();
            var output = new List<ValueTuple<string, long?>>();

            var queryContainer = new QueryContainer();
            var qstreamable = queryContainer.RegisterStream(inputObservable, o => 0, o => 1);
            var qstreamableCount =
                from t in qstreamable
                group t by (t % 10).ToString() into g
                select ValueTuple.Create(g.Key, g.Product());

            var results = qstreamableCount
                .ToTemporalObservable((start, end, payload) => payload)
                .ForEachAsync(o => output.Add(o));

            queryContainer.Restore();
            results.Wait();

            // Validate against the same IEnumerable query
            var expected =
                (from t in input
                 group t by (t % 10).ToString() into g
                 select ValueTuple.Create(g.Key, g.Product()))
                .ToList();
            Assert.IsTrue(expected.SequenceEqual(output.OrderBy(o => o.Item1)));
        }

        [TestMethod]
        public void AggregateNullableSum()
        {
            var input = Enumerable.Range(0, 100).Select(i => (long?)i);
            var inputObservable = input.ToObservable();
            var output = new List<ValueTuple<string, long?>>();

            var queryContainer = new QueryContainer();
            var qstreamable = queryContainer.RegisterStream(inputObservable, o => 0, o => 1);
            var qstreamableCount =
                from t in qstreamable
                group t by (t % 10).ToString() into g
                select ValueTuple.Create(g.Key, g.Sum());

            var results = qstreamableCount
                .ToTemporalObservable((start, end, payload) => payload)
                .ForEachAsync(o => output.Add(o));

            queryContainer.Restore();
            results.Wait();

            // Validate against the same IEnumerable query
            var expected =
                (from t in input
                 group t by (t % 10).ToString() into g
                 select ValueTuple.Create(g.Key, g.Sum()))
                .ToList();
            Assert.IsTrue(expected.SequenceEqual(output.OrderBy(o => o.Item1)));
        }

        [TestMethod]
        public void AggregateNullableAverage()
        {
            var input = Enumerable.Range(0, 100).Select(i => (long?)i);
            var inputObservable = input.ToObservable();
            var output = new List<ValueTuple<string, double?>>();

            var queryContainer = new QueryContainer();
            var qstreamable = queryContainer.RegisterStream(inputObservable, o => 0, o => 1);
            var qstreamableCount =
                from t in qstreamable
                group t by (t % 10).ToString() into g
                select ValueTuple.Create(g.Key, g.Average());

            var results = qstreamableCount
                .ToTemporalObservable((start, end, payload) => payload)
                .ForEachAsync(o => output.Add(o));

            queryContainer.Restore();
            results.Wait();

            // Validate against the same IEnumerable query
            var expected =
                (from t in input
                 group t by (t % 10).ToString() into g
                 select ValueTuple.Create(g.Key, g.Average()))
                .ToList();
            Assert.IsTrue(expected.SequenceEqual(output.OrderBy(o => o.Item1)));
        }

        [TestMethod]
        public void QuantizeLifetime()
        {
            var input = Enumerable.Range(0, 100);
            var inputObservable = input.ToObservable();
            var output = new List<ValueTuple<long, long, int>>();

            var queryContainer = new QueryContainer();
            var qstreamable = queryContainer.RegisterStream(inputObservable, o => o, o => o + 1);
            var qstreamableCount =
                from t in qstreamable.QuantizeLifetime(windowSize: 10, period: 10)
                select t;

            var results = qstreamableCount
                .ToTemporalObservable((start, end, payload) => ValueTuple.Create(start, end, payload))
                .ForEachAsync(o => output.Add(o));

            queryContainer.Restore();
            results.Wait();

            // Validate against the same IEnumerable query
            var expected = input
                .Select(i => ValueTuple.Create((long)i - i % 10 /*start*/, (long)i - i % 10 + 10/*end*/, i /*payload*/))
                .ToList();
            Assert.IsTrue(expected.SequenceEqual(output));
        }
    }
}
