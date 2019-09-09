using System;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Aggregates;
using Microsoft.StreamProcessing.Provider;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ProviderTesting
{
    [TestClass]
    public class AdHocTests
    {
        [TestMethod, Ignore]
        public void Passthrough()
        {
            var input = Enumerable.Range(1, 10)
                .Select(i => ValueTuple.Create((long)i /*start*/, (long)i + 1 /*end*/, i.ToString() /*payload*/));
            var inputObservable = input.ToObservable();

            var queryContainer = new QueryContainer();
            var qstreamable = queryContainer.RegisterStream(inputObservable, o => o.Item1, o => o.Item2);
            var qstreamablePassthrough = from t in qstreamable select t.Item3;

            var results = qstreamablePassthrough
                .ToTemporalObservable((start, end, payload) => ValueTuple.Create(start, end, payload))
                .ToEnumerable()
                .ToList();

            var expected = input.ToList();
            Assert.IsTrue(expected.SequenceEqual(results));
        }

        [TestMethod, Ignore]
        public void Where()
        {
            var input = Enumerable.Range(1, 10)
                .Select(i => ValueTuple.Create((long)i /*start*/, (long)i + 1 /*end*/, i.ToString() /*payload*/));
            var inputObservable = input.ToObservable();

            var queryContainer = new QueryContainer();
            var qstreamable = queryContainer.RegisterStream(inputObservable, o => o.Item1, o => o.Item2);
            var qstreamableFiltered =
                from t in qstreamable
                where t.Item1 % 2 == 0
                select t.Item3;

            var results = qstreamableFiltered
                .ToTemporalObservable((start, end, payload) => ValueTuple.Create(start, end, payload))
                .ToEnumerable()
                .ToList();

            // Validate against the same IEnumerable query
            var expected =
                (from t in input
                     where t.Item1 % 2 == 0
                     select ValueTuple.Create(t.Item1, t.Item2, t.Item3))
                .ToList();
            Assert.IsTrue(expected.SequenceEqual(results));
        }

        [TestMethod, Ignore]
        public void GroupBy()
        {
            var input = Enumerable.Range(1, 100);
            var inputObservable = input.ToObservable();

            var queryContainer = new QueryContainer();
            var qstreamable = queryContainer.RegisterStream(inputObservable, o => o, o => o + 1);
            var qstreamableCount =
                from t in qstreamable
                group t by (t % 3) into g
                select g.Key;

            var results = qstreamableCount
                .ToTemporalObservable((start, end, payload) => payload)
                .ToEnumerable()
                .ToList();

            // Validate against the same IEnumerable query
            var expected =
                (from t in input
                    group t by (t % 3) into g
                    select g.Key)
                .ToList();
            Assert.IsTrue(expected.SequenceEqual(results));
        }

        [TestMethod, Ignore]
        public void AggregateCount()
        {
            var input = Enumerable.Range(1, 100);
            var inputObservable = input.ToObservable();

            var queryContainer = new QueryContainer();
            var qstreamable = queryContainer.RegisterStream(inputObservable, o => o, o => o + 1);
            var qstreamableCount =
                from t in qstreamable
                group t by (t % 3) into g
                select g.Count();

            var results = qstreamableCount
                .ToTemporalObservable((start, end, payload) => payload)
                .ToEnumerable()
                .ToList();

            Assert.AreEqual(1, results.Count);
            Assert.AreEqual(3, results[0]);
        }

        [TestMethod, Ignore]
        public void AggregateProduct()
        {
            var input = Enumerable.Range(0, 100).Select(i => (long)i);
            var inputObservable = input.ToObservable();

            var queryContainer = new QueryContainer();
            var qstreamable = queryContainer.RegisterStream(inputObservable, o => o, o => o + 1);
            var qstreamableCount =
                from t in qstreamable
                group t by (t % 10).ToString() into g
                select ValueTuple.Create(g.Key, g.Product());

            var results = qstreamableCount
                .ToTemporalObservable((start, end, payload) => payload)
                .ToEnumerable()
                .ToList();

            // Validate against the same IEnumerable query
            var expected =
                (from t in input
                 group t by (t % 10).ToString() into g
                 select ValueTuple.Create(g.Key, g.Aggregate((t1, t2) => t1 * t2)))
                .ToList();
            Assert.IsTrue(expected.SequenceEqual(results));
        }

        [TestMethod, Ignore]
        public void QuantizeLifetime()
        {
            var input = Enumerable.Range(0, 100);
            var inputObservable = input.ToObservable();

            var queryContainer = new QueryContainer();
            var qstreamable = queryContainer.RegisterStream(inputObservable, o => o, o => o + 1);
            var qstreamableCount =
                from t in qstreamable.QuantizeLifetime(windowSize: 10, period: 10)
                select t;

            var results = qstreamableCount
                .ToTemporalObservable((start, end, payload) => ValueTuple.Create(start, end, payload))
                .ToEnumerable()
                .ToList();

            // Validate against the same IEnumerable query
            var expected = input
                .Select(i => ValueTuple.Create((long)i - i % 10 /*start*/, (long)i - i % 10 + 10/*end*/, i /*payload*/))
                .ToList();
            Assert.IsTrue(expected.SequenceEqual(results));
        }
    }
}
