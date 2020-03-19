// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class SnapshotTestsRow : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public SnapshotTestsRow() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .MapArity(1)
            .ReduceArity(1))
       { }

        [TestMethod, TestCategory("Gated")]
        public void TumblingSnapshot1Row()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(20, 30, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(30, 40, new MyData { field1 = 4, field2 = "D" })
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(10, 10)
                .GroupApply(e => e.field2, str => str.Sum(x => x.field1), (g, c) => new MyData { field1 = c, field2 = g.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void TumblingSnapshot2Row()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(20, 30, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(30, 40, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreateInterval(30, 40, new MyData { field1 = 2, field2 = "A" })
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(10, 10)
                .GroupApply(e => e.field2, str => str.Sum(x => x.field1), (g, c) => new MyData { field1 = c, field2 = g.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void TumblingSnapshot3Row()
        {

            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(20, 30, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(50, 60, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreateInterval(50, 60, new MyData { field1 = 2, field2 = "A" })
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(10, 10)
                .GroupApply(e => e.field2, str => str.Sum(x => x.field1), (g, c) => new MyData { field1 = c, field2 = g.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void TumblingSnapshot4Row() // like 2, but without grouping
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(20, 30, 2),
                StreamEvent.CreateInterval(30, 40, 4),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(10, 10)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void TumblingSnapshot5Row() // like 4, but with max
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(20, 30, 1),
                StreamEvent.CreateInterval(30, 40, 2),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(10, 10)
                .Max(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void TumblingSnapshot6Row() // like 5, but with min
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(20, 30, 1),
                StreamEvent.CreateInterval(30, 40, 2),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(10, 10)
                .Min(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void HoppingSnapshot1Row()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(20, 40, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(30, 50, new MyData { field1 = 4, field2 = "D" })
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(20, 10)
                .GroupApply(e => e.field2, str => str.Sum(x => x.field1), (g, c) => new MyData { field1 = c, field2 = g.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void HoppingSnapshot2Row()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(20, 30, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(30, 50, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreateInterval(30, 40, new MyData { field1 = 4, field2 = "A" }),
                StreamEvent.CreateInterval(40, 50, new MyData { field1 = 2, field2 = "A" })
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(20, 10)
                .GroupApply(e => e.field2, str => str.Sum(x => x.field1), (g, c) => new MyData { field1 = c, field2 = g.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.OrderBy(o => o.SyncTime).ThenBy(o => o.Payload.field1).SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void HoppingSnapshot3Row()
        {

            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(20, 40, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(50, 70, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(50, 70, new MyData { field1 = 2, field2 = "D" })
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(20, 10)
                .GroupApply(e => e.field2, str => str.Sum(x => x.field1), (g, c) => new MyData { field1 = c, field2 = g.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.OrderBy(o => o.SyncTime).ThenBy(o => o.Payload.field2).SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void HoppingSnapshot4Row() // like 2, but without grouping
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(20, 30, 2),
                StreamEvent.CreateInterval(30, 40, 6),
                StreamEvent.CreateInterval(40, 50, 4),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(20, 10)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void HoppingSnapshot5Row() // like 5, but with max
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(20, 30, 1),
                StreamEvent.CreateInterval(30, 40, 2),
                StreamEvent.CreateInterval(40, 50, 2),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(20, 10)
                .Max(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void HoppingSnapshot6Row() // like 5, but with min
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(20, 30, 1),
                StreamEvent.CreateInterval(30, 40, 1),
                StreamEvent.CreateInterval(40, 50, 2),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(20, 10)
                .Min(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshot1Row()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 21, 2),
                StreamEvent.CreateInterval(21, 25, 4),
                StreamEvent.CreateInterval(25, 75, 6),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.SessionTimeoutWindow(50)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshot2Row()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 18, 2),
                StreamEvent.CreateInterval(21, 25, 2),
                StreamEvent.CreateInterval(25, 31, 4),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.SessionTimeoutWindow(6)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshot3Row()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 21, 2),
                StreamEvent.CreateInterval(21, 25, 4),
                StreamEvent.CreateInterval(25, 31, 6),
                StreamEvent.CreateInterval(31, 32, 7),
                StreamEvent.CreateInterval(32, 41, 8),
                StreamEvent.CreateInterval(41, 45, 10),
                StreamEvent.CreateInterval(45, 60, 12),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.SessionTimeoutWindow(20, 30)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshot4Row()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(51, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(62, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(71, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(75, new MyData { field1 = 2, field2 = "D" }),
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 21, 2),
                StreamEvent.CreateInterval(21, 25, 4),
                StreamEvent.CreateInterval(25, 31, 6),
                StreamEvent.CreateInterval(31, 32, 7),
                StreamEvent.CreateInterval(32, 40, 8),
                StreamEvent.CreateInterval(41, 45, 2),
                StreamEvent.CreateInterval(45, 51, 4),
                StreamEvent.CreateInterval(51, 60, 5),
                StreamEvent.CreateInterval(62, 71, 1),
                StreamEvent.CreateInterval(71, 75, 3),
                StreamEvent.CreateInterval(75, 85, 5),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.SessionTimeoutWindow(10, 20)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshot5Row()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(0, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(5, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(51, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(62, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(71, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(75, new MyData { field1 = 2, field2 = "D" }),
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(0, 5, 1),
                StreamEvent.CreateInterval(5, 11, 3),
                StreamEvent.CreateInterval(11, 12, 4),
                StreamEvent.CreateInterval(12, 20, 5),
                StreamEvent.CreateInterval(21, 25, 2),
                StreamEvent.CreateInterval(25, 31, 4),
                StreamEvent.CreateInterval(31, 32, 5),
                StreamEvent.CreateInterval(32, 40, 6),
                StreamEvent.CreateInterval(41, 45, 2),
                StreamEvent.CreateInterval(45, 51, 4),
                StreamEvent.CreateInterval(51, 60, 5),
                StreamEvent.CreateInterval(62, 71, 1),
                StreamEvent.CreateInterval(71, 75, 3),
                StreamEvent.CreateInterval(75, 85, 5),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.SessionTimeoutWindow(10, 20)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotTrivialGroup1Row()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 21, 2),
                StreamEvent.CreateInterval(21, 25, 4),
                StreamEvent.CreateInterval(25, 75, 6),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => true, o => o.SessionTimeoutWindow(50).Sum(x => x.field1), (k, p) => p);

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotTrivialGroup2Row()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 18, 2),
                StreamEvent.CreateInterval(21, 25, 2),
                StreamEvent.CreateInterval(25, 31, 4),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => true, o => o.SessionTimeoutWindow(6).Sum(x => x.field1), (k, p) => p);

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotTrivialGroup3Row()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 21, 2),
                StreamEvent.CreateInterval(21, 25, 4),
                StreamEvent.CreateInterval(25, 31, 6),
                StreamEvent.CreateInterval(31, 32, 7),
                StreamEvent.CreateInterval(32, 41, 8),
                StreamEvent.CreateInterval(41, 45, 10),
                StreamEvent.CreateInterval(45, 60, 12),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => true, o => o.SessionTimeoutWindow(20, 30).Sum(x => x.field1), (k, p) => p);

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotTrivialGroup4Row()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(51, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(62, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(71, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(75, new MyData { field1 = 2, field2 = "D" }),
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 21, 2),
                StreamEvent.CreateInterval(21, 25, 4),
                StreamEvent.CreateInterval(25, 31, 6),
                StreamEvent.CreateInterval(31, 32, 7),
                StreamEvent.CreateInterval(32, 40, 8),
                StreamEvent.CreateInterval(41, 45, 2),
                StreamEvent.CreateInterval(45, 51, 4),
                StreamEvent.CreateInterval(51, 60, 5),
                StreamEvent.CreateInterval(62, 71, 1),
                StreamEvent.CreateInterval(71, 75, 3),
                StreamEvent.CreateInterval(75, 85, 5),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => true, o => o.SessionTimeoutWindow(10, 20).Sum(x => x.field1), (k, p) => p);

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotSimpleGroup1Row()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(11, 12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(12, 21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(21, 71, new MyData { field1 = 4, field2 = "A" }),
                StreamEvent.CreateInterval(25, 75, new MyData { field1 = 2, field2 = "D" }),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => o.field2, o => o.SessionTimeoutWindow(50).Sum(x => x.field1), (k, p) => new MyData { field1 = p, field2 = k.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotSimpleGroup2Row()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(11, 12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(12, 18, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(21, 27, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(25, 31, new MyData { field1 = 2, field2 = "D" }),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => o.field2, o => o.SessionTimeoutWindow(6).Sum(x => x.field1), (k, p) => new MyData { field1 = p, field2 = k.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotSimpleGroup3Row()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(11, 12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(12, 21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(21, 31, new MyData { field1 = 4, field2 = "A" }),
                StreamEvent.CreateInterval(25, 45, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreateInterval(31, 32, new MyData { field1 = 5, field2 = "A" }),
                StreamEvent.CreateInterval(32, 41, new MyData { field1 = 6, field2 = "A" }),
                StreamEvent.CreateInterval(41, 60, new MyData { field1 = 8, field2 = "A" }),
                StreamEvent.CreateInterval(45, 65, new MyData { field1 = 2, field2 = "D" }),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => o.field2, o => o.SessionTimeoutWindow(20, 30).Sum(x => x.field1), (k, p) => new MyData { field1 = p, field2 = k.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotSimpleGroup4Row()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(51, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(62, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(71, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(75, new MyData { field1 = 2, field2 = "D" }),
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(11, 12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(12, 21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(21, 31, new MyData { field1 = 4, field2 = "A" }),
                StreamEvent.CreateInterval(25, 35, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreateInterval(31, 32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(32, 41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(41, 51, new MyData { field1 = 4, field2 = "A" }),
                StreamEvent.CreateInterval(45, 55, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreateInterval(51, 61, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(62, 71, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(71, 81, new MyData { field1 = 3, field2 = "A" }),
                StreamEvent.CreateInterval(75, 85, new MyData { field1 = 2, field2 = "D" }),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => o.field2, o => o.SessionTimeoutWindow(10, 20).Sum(x => x.field1), (k, p) => new MyData { field1 = p, field2 = k.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }
    }

    [TestClass]
    public class SnapshotTestsRowSmallBatch : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public SnapshotTestsRowSmallBatch() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .MapArity(1)
            .ReduceArity(1))
       { }

        [TestMethod, TestCategory("Gated")]
        public void TumblingSnapshot1RowSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(20, 30, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(30, 40, new MyData { field1 = 4, field2 = "D" })
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(10, 10)
                .GroupApply(e => e.field2, str => str.Sum(x => x.field1), (g, c) => new MyData { field1 = c, field2 = g.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void TumblingSnapshot2RowSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(20, 30, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(30, 40, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreateInterval(30, 40, new MyData { field1 = 2, field2 = "A" })
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(10, 10)
                .GroupApply(e => e.field2, str => str.Sum(x => x.field1), (g, c) => new MyData { field1 = c, field2 = g.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void TumblingSnapshot3RowSmallBatch()
        {

            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(20, 30, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(50, 60, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreateInterval(50, 60, new MyData { field1 = 2, field2 = "A" })
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(10, 10)
                .GroupApply(e => e.field2, str => str.Sum(x => x.field1), (g, c) => new MyData { field1 = c, field2 = g.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void TumblingSnapshot4RowSmallBatch() // like 2, but without grouping
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(20, 30, 2),
                StreamEvent.CreateInterval(30, 40, 4),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(10, 10)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void TumblingSnapshot5RowSmallBatch() // like 4, but with max
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(20, 30, 1),
                StreamEvent.CreateInterval(30, 40, 2),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(10, 10)
                .Max(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void TumblingSnapshot6RowSmallBatch() // like 5, but with min
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(20, 30, 1),
                StreamEvent.CreateInterval(30, 40, 2),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(10, 10)
                .Min(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void HoppingSnapshot1RowSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(20, 40, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(30, 50, new MyData { field1 = 4, field2 = "D" })
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(20, 10)
                .GroupApply(e => e.field2, str => str.Sum(x => x.field1), (g, c) => new MyData { field1 = c, field2 = g.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void HoppingSnapshot2RowSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(20, 30, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(30, 50, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreateInterval(30, 40, new MyData { field1 = 4, field2 = "A" }),
                StreamEvent.CreateInterval(40, 50, new MyData { field1 = 2, field2 = "A" })
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(20, 10)
                .GroupApply(e => e.field2, str => str.Sum(x => x.field1), (g, c) => new MyData { field1 = c, field2 = g.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.OrderBy(o => o.SyncTime).ThenBy(o => o.Payload.field1).SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void HoppingSnapshot3RowSmallBatch()
        {

            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(20, 40, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(50, 70, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(50, 70, new MyData { field1 = 2, field2 = "D" })
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(20, 10)
                .GroupApply(e => e.field2, str => str.Sum(x => x.field1), (g, c) => new MyData { field1 = c, field2 = g.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.OrderBy(o => o.SyncTime).ThenBy(o => o.Payload.field2).SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void HoppingSnapshot4RowSmallBatch() // like 2, but without grouping
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(20, 30, 2),
                StreamEvent.CreateInterval(30, 40, 6),
                StreamEvent.CreateInterval(40, 50, 4),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(20, 10)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void HoppingSnapshot5RowSmallBatch() // like 5, but with max
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(20, 30, 1),
                StreamEvent.CreateInterval(30, 40, 2),
                StreamEvent.CreateInterval(40, 50, 2),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(20, 10)
                .Max(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void HoppingSnapshot6RowSmallBatch() // like 5, but with min
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(20, 30, 1),
                StreamEvent.CreateInterval(30, 40, 1),
                StreamEvent.CreateInterval(40, 50, 2),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(20, 10)
                .Min(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshot1RowSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 21, 2),
                StreamEvent.CreateInterval(21, 25, 4),
                StreamEvent.CreateInterval(25, 75, 6),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.SessionTimeoutWindow(50)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshot2RowSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 18, 2),
                StreamEvent.CreateInterval(21, 25, 2),
                StreamEvent.CreateInterval(25, 31, 4),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.SessionTimeoutWindow(6)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshot3RowSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 21, 2),
                StreamEvent.CreateInterval(21, 25, 4),
                StreamEvent.CreateInterval(25, 31, 6),
                StreamEvent.CreateInterval(31, 32, 7),
                StreamEvent.CreateInterval(32, 41, 8),
                StreamEvent.CreateInterval(41, 45, 10),
                StreamEvent.CreateInterval(45, 60, 12),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.SessionTimeoutWindow(20, 30)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshot4RowSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(51, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(62, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(71, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(75, new MyData { field1 = 2, field2 = "D" }),
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 21, 2),
                StreamEvent.CreateInterval(21, 25, 4),
                StreamEvent.CreateInterval(25, 31, 6),
                StreamEvent.CreateInterval(31, 32, 7),
                StreamEvent.CreateInterval(32, 40, 8),
                StreamEvent.CreateInterval(41, 45, 2),
                StreamEvent.CreateInterval(45, 51, 4),
                StreamEvent.CreateInterval(51, 60, 5),
                StreamEvent.CreateInterval(62, 71, 1),
                StreamEvent.CreateInterval(71, 75, 3),
                StreamEvent.CreateInterval(75, 85, 5),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.SessionTimeoutWindow(10, 20)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshot5RowSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(0, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(5, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(51, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(62, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(71, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(75, new MyData { field1 = 2, field2 = "D" }),
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(0, 5, 1),
                StreamEvent.CreateInterval(5, 11, 3),
                StreamEvent.CreateInterval(11, 12, 4),
                StreamEvent.CreateInterval(12, 20, 5),
                StreamEvent.CreateInterval(21, 25, 2),
                StreamEvent.CreateInterval(25, 31, 4),
                StreamEvent.CreateInterval(31, 32, 5),
                StreamEvent.CreateInterval(32, 40, 6),
                StreamEvent.CreateInterval(41, 45, 2),
                StreamEvent.CreateInterval(45, 51, 4),
                StreamEvent.CreateInterval(51, 60, 5),
                StreamEvent.CreateInterval(62, 71, 1),
                StreamEvent.CreateInterval(71, 75, 3),
                StreamEvent.CreateInterval(75, 85, 5),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.SessionTimeoutWindow(10, 20)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotTrivialGroup1RowSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 21, 2),
                StreamEvent.CreateInterval(21, 25, 4),
                StreamEvent.CreateInterval(25, 75, 6),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => true, o => o.SessionTimeoutWindow(50).Sum(x => x.field1), (k, p) => p);

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotTrivialGroup2RowSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 18, 2),
                StreamEvent.CreateInterval(21, 25, 2),
                StreamEvent.CreateInterval(25, 31, 4),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => true, o => o.SessionTimeoutWindow(6).Sum(x => x.field1), (k, p) => p);

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotTrivialGroup3RowSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 21, 2),
                StreamEvent.CreateInterval(21, 25, 4),
                StreamEvent.CreateInterval(25, 31, 6),
                StreamEvent.CreateInterval(31, 32, 7),
                StreamEvent.CreateInterval(32, 41, 8),
                StreamEvent.CreateInterval(41, 45, 10),
                StreamEvent.CreateInterval(45, 60, 12),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => true, o => o.SessionTimeoutWindow(20, 30).Sum(x => x.field1), (k, p) => p);

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotTrivialGroup4RowSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(51, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(62, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(71, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(75, new MyData { field1 = 2, field2 = "D" }),
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 21, 2),
                StreamEvent.CreateInterval(21, 25, 4),
                StreamEvent.CreateInterval(25, 31, 6),
                StreamEvent.CreateInterval(31, 32, 7),
                StreamEvent.CreateInterval(32, 40, 8),
                StreamEvent.CreateInterval(41, 45, 2),
                StreamEvent.CreateInterval(45, 51, 4),
                StreamEvent.CreateInterval(51, 60, 5),
                StreamEvent.CreateInterval(62, 71, 1),
                StreamEvent.CreateInterval(71, 75, 3),
                StreamEvent.CreateInterval(75, 85, 5),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => true, o => o.SessionTimeoutWindow(10, 20).Sum(x => x.field1), (k, p) => p);

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotSimpleGroup1RowSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(11, 12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(12, 21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(21, 71, new MyData { field1 = 4, field2 = "A" }),
                StreamEvent.CreateInterval(25, 75, new MyData { field1 = 2, field2 = "D" }),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => o.field2, o => o.SessionTimeoutWindow(50).Sum(x => x.field1), (k, p) => new MyData { field1 = p, field2 = k.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotSimpleGroup2RowSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(11, 12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(12, 18, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(21, 27, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(25, 31, new MyData { field1 = 2, field2 = "D" }),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => o.field2, o => o.SessionTimeoutWindow(6).Sum(x => x.field1), (k, p) => new MyData { field1 = p, field2 = k.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotSimpleGroup3RowSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(11, 12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(12, 21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(21, 31, new MyData { field1 = 4, field2 = "A" }),
                StreamEvent.CreateInterval(25, 45, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreateInterval(31, 32, new MyData { field1 = 5, field2 = "A" }),
                StreamEvent.CreateInterval(32, 41, new MyData { field1 = 6, field2 = "A" }),
                StreamEvent.CreateInterval(41, 60, new MyData { field1 = 8, field2 = "A" }),
                StreamEvent.CreateInterval(45, 65, new MyData { field1 = 2, field2 = "D" }),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => o.field2, o => o.SessionTimeoutWindow(20, 30).Sum(x => x.field1), (k, p) => new MyData { field1 = p, field2 = k.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotSimpleGroup4RowSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(51, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(62, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(71, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(75, new MyData { field1 = 2, field2 = "D" }),
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(11, 12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(12, 21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(21, 31, new MyData { field1 = 4, field2 = "A" }),
                StreamEvent.CreateInterval(25, 35, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreateInterval(31, 32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(32, 41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(41, 51, new MyData { field1 = 4, field2 = "A" }),
                StreamEvent.CreateInterval(45, 55, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreateInterval(51, 61, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(62, 71, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(71, 81, new MyData { field1 = 3, field2 = "A" }),
                StreamEvent.CreateInterval(75, 85, new MyData { field1 = 2, field2 = "D" }),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => o.field2, o => o.SessionTimeoutWindow(10, 20).Sum(x => x.field1), (k, p) => new MyData { field1 = p, field2 = k.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }
    }

    [TestClass]
    public class SnapshotTestsColumnar : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public SnapshotTestsColumnar() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .MapArity(1)
            .ReduceArity(1))
       { }

        [TestMethod, TestCategory("Gated")]
        public void TumblingSnapshot1Columnar()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(20, 30, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(30, 40, new MyData { field1 = 4, field2 = "D" })
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(10, 10)
                .GroupApply(e => e.field2, str => str.Sum(x => x.field1), (g, c) => new MyData { field1 = c, field2 = g.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void TumblingSnapshot2Columnar()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(20, 30, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(30, 40, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreateInterval(30, 40, new MyData { field1 = 2, field2 = "A" })
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(10, 10)
                .GroupApply(e => e.field2, str => str.Sum(x => x.field1), (g, c) => new MyData { field1 = c, field2 = g.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void TumblingSnapshot3Columnar()
        {

            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(20, 30, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(50, 60, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreateInterval(50, 60, new MyData { field1 = 2, field2 = "A" })
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(10, 10)
                .GroupApply(e => e.field2, str => str.Sum(x => x.field1), (g, c) => new MyData { field1 = c, field2 = g.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void TumblingSnapshot4Columnar() // like 2, but without grouping
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(20, 30, 2),
                StreamEvent.CreateInterval(30, 40, 4),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(10, 10)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void TumblingSnapshot5Columnar() // like 4, but with max
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(20, 30, 1),
                StreamEvent.CreateInterval(30, 40, 2),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(10, 10)
                .Max(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void TumblingSnapshot6Columnar() // like 5, but with min
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(20, 30, 1),
                StreamEvent.CreateInterval(30, 40, 2),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(10, 10)
                .Min(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void HoppingSnapshot1Columnar()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(20, 40, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(30, 50, new MyData { field1 = 4, field2 = "D" })
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(20, 10)
                .GroupApply(e => e.field2, str => str.Sum(x => x.field1), (g, c) => new MyData { field1 = c, field2 = g.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void HoppingSnapshot2Columnar()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(20, 30, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(30, 50, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreateInterval(30, 40, new MyData { field1 = 4, field2 = "A" }),
                StreamEvent.CreateInterval(40, 50, new MyData { field1 = 2, field2 = "A" })
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(20, 10)
                .GroupApply(e => e.field2, str => str.Sum(x => x.field1), (g, c) => new MyData { field1 = c, field2 = g.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.OrderBy(o => o.SyncTime).ThenBy(o => o.Payload.field1).SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void HoppingSnapshot3Columnar()
        {

            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(20, 40, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(50, 70, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(50, 70, new MyData { field1 = 2, field2 = "D" })
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(20, 10)
                .GroupApply(e => e.field2, str => str.Sum(x => x.field1), (g, c) => new MyData { field1 = c, field2 = g.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.OrderBy(o => o.SyncTime).ThenBy(o => o.Payload.field2).SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void HoppingSnapshot4Columnar() // like 2, but without grouping
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(20, 30, 2),
                StreamEvent.CreateInterval(30, 40, 6),
                StreamEvent.CreateInterval(40, 50, 4),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(20, 10)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void HoppingSnapshot5Columnar() // like 5, but with max
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(20, 30, 1),
                StreamEvent.CreateInterval(30, 40, 2),
                StreamEvent.CreateInterval(40, 50, 2),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(20, 10)
                .Max(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void HoppingSnapshot6Columnar() // like 5, but with min
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(20, 30, 1),
                StreamEvent.CreateInterval(30, 40, 1),
                StreamEvent.CreateInterval(40, 50, 2),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(20, 10)
                .Min(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshot1Columnar()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 21, 2),
                StreamEvent.CreateInterval(21, 25, 4),
                StreamEvent.CreateInterval(25, 75, 6),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.SessionTimeoutWindow(50)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshot2Columnar()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 18, 2),
                StreamEvent.CreateInterval(21, 25, 2),
                StreamEvent.CreateInterval(25, 31, 4),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.SessionTimeoutWindow(6)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshot3Columnar()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 21, 2),
                StreamEvent.CreateInterval(21, 25, 4),
                StreamEvent.CreateInterval(25, 31, 6),
                StreamEvent.CreateInterval(31, 32, 7),
                StreamEvent.CreateInterval(32, 41, 8),
                StreamEvent.CreateInterval(41, 45, 10),
                StreamEvent.CreateInterval(45, 60, 12),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.SessionTimeoutWindow(20, 30)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshot4Columnar()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(51, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(62, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(71, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(75, new MyData { field1 = 2, field2 = "D" }),
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 21, 2),
                StreamEvent.CreateInterval(21, 25, 4),
                StreamEvent.CreateInterval(25, 31, 6),
                StreamEvent.CreateInterval(31, 32, 7),
                StreamEvent.CreateInterval(32, 40, 8),
                StreamEvent.CreateInterval(41, 45, 2),
                StreamEvent.CreateInterval(45, 51, 4),
                StreamEvent.CreateInterval(51, 60, 5),
                StreamEvent.CreateInterval(62, 71, 1),
                StreamEvent.CreateInterval(71, 75, 3),
                StreamEvent.CreateInterval(75, 85, 5),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.SessionTimeoutWindow(10, 20)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshot5Columnar()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(0, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(5, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(51, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(62, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(71, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(75, new MyData { field1 = 2, field2 = "D" }),
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(0, 5, 1),
                StreamEvent.CreateInterval(5, 11, 3),
                StreamEvent.CreateInterval(11, 12, 4),
                StreamEvent.CreateInterval(12, 20, 5),
                StreamEvent.CreateInterval(21, 25, 2),
                StreamEvent.CreateInterval(25, 31, 4),
                StreamEvent.CreateInterval(31, 32, 5),
                StreamEvent.CreateInterval(32, 40, 6),
                StreamEvent.CreateInterval(41, 45, 2),
                StreamEvent.CreateInterval(45, 51, 4),
                StreamEvent.CreateInterval(51, 60, 5),
                StreamEvent.CreateInterval(62, 71, 1),
                StreamEvent.CreateInterval(71, 75, 3),
                StreamEvent.CreateInterval(75, 85, 5),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.SessionTimeoutWindow(10, 20)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotTrivialGroup1Columnar()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 21, 2),
                StreamEvent.CreateInterval(21, 25, 4),
                StreamEvent.CreateInterval(25, 75, 6),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => true, o => o.SessionTimeoutWindow(50).Sum(x => x.field1), (k, p) => p);

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotTrivialGroup2Columnar()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 18, 2),
                StreamEvent.CreateInterval(21, 25, 2),
                StreamEvent.CreateInterval(25, 31, 4),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => true, o => o.SessionTimeoutWindow(6).Sum(x => x.field1), (k, p) => p);

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotTrivialGroup3Columnar()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 21, 2),
                StreamEvent.CreateInterval(21, 25, 4),
                StreamEvent.CreateInterval(25, 31, 6),
                StreamEvent.CreateInterval(31, 32, 7),
                StreamEvent.CreateInterval(32, 41, 8),
                StreamEvent.CreateInterval(41, 45, 10),
                StreamEvent.CreateInterval(45, 60, 12),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => true, o => o.SessionTimeoutWindow(20, 30).Sum(x => x.field1), (k, p) => p);

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotTrivialGroup4Columnar()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(51, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(62, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(71, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(75, new MyData { field1 = 2, field2 = "D" }),
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 21, 2),
                StreamEvent.CreateInterval(21, 25, 4),
                StreamEvent.CreateInterval(25, 31, 6),
                StreamEvent.CreateInterval(31, 32, 7),
                StreamEvent.CreateInterval(32, 40, 8),
                StreamEvent.CreateInterval(41, 45, 2),
                StreamEvent.CreateInterval(45, 51, 4),
                StreamEvent.CreateInterval(51, 60, 5),
                StreamEvent.CreateInterval(62, 71, 1),
                StreamEvent.CreateInterval(71, 75, 3),
                StreamEvent.CreateInterval(75, 85, 5),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => true, o => o.SessionTimeoutWindow(10, 20).Sum(x => x.field1), (k, p) => p);

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotSimpleGroup1Columnar()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(11, 12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(12, 21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(21, 71, new MyData { field1 = 4, field2 = "A" }),
                StreamEvent.CreateInterval(25, 75, new MyData { field1 = 2, field2 = "D" }),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => o.field2, o => o.SessionTimeoutWindow(50).Sum(x => x.field1), (k, p) => new MyData { field1 = p, field2 = k.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotSimpleGroup2Columnar()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(11, 12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(12, 18, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(21, 27, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(25, 31, new MyData { field1 = 2, field2 = "D" }),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => o.field2, o => o.SessionTimeoutWindow(6).Sum(x => x.field1), (k, p) => new MyData { field1 = p, field2 = k.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotSimpleGroup3Columnar()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(11, 12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(12, 21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(21, 31, new MyData { field1 = 4, field2 = "A" }),
                StreamEvent.CreateInterval(25, 45, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreateInterval(31, 32, new MyData { field1 = 5, field2 = "A" }),
                StreamEvent.CreateInterval(32, 41, new MyData { field1 = 6, field2 = "A" }),
                StreamEvent.CreateInterval(41, 60, new MyData { field1 = 8, field2 = "A" }),
                StreamEvent.CreateInterval(45, 65, new MyData { field1 = 2, field2 = "D" }),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => o.field2, o => o.SessionTimeoutWindow(20, 30).Sum(x => x.field1), (k, p) => new MyData { field1 = p, field2 = k.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotSimpleGroup4Columnar()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(51, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(62, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(71, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(75, new MyData { field1 = 2, field2 = "D" }),
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(11, 12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(12, 21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(21, 31, new MyData { field1 = 4, field2 = "A" }),
                StreamEvent.CreateInterval(25, 35, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreateInterval(31, 32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(32, 41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(41, 51, new MyData { field1 = 4, field2 = "A" }),
                StreamEvent.CreateInterval(45, 55, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreateInterval(51, 61, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(62, 71, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(71, 81, new MyData { field1 = 3, field2 = "A" }),
                StreamEvent.CreateInterval(75, 85, new MyData { field1 = 2, field2 = "D" }),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => o.field2, o => o.SessionTimeoutWindow(10, 20).Sum(x => x.field1), (k, p) => new MyData { field1 = p, field2 = k.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }
    }

    [TestClass]
    public class SnapshotTestsColumnarSmallBatch : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public SnapshotTestsColumnarSmallBatch() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .MapArity(1)
            .ReduceArity(1))
       { }

        [TestMethod, TestCategory("Gated")]
        public void TumblingSnapshot1ColumnarSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(20, 30, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(30, 40, new MyData { field1 = 4, field2 = "D" })
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(10, 10)
                .GroupApply(e => e.field2, str => str.Sum(x => x.field1), (g, c) => new MyData { field1 = c, field2 = g.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void TumblingSnapshot2ColumnarSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(20, 30, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(30, 40, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreateInterval(30, 40, new MyData { field1 = 2, field2 = "A" })
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(10, 10)
                .GroupApply(e => e.field2, str => str.Sum(x => x.field1), (g, c) => new MyData { field1 = c, field2 = g.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void TumblingSnapshot3ColumnarSmallBatch()
        {

            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(20, 30, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(50, 60, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreateInterval(50, 60, new MyData { field1 = 2, field2 = "A" })
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(10, 10)
                .GroupApply(e => e.field2, str => str.Sum(x => x.field1), (g, c) => new MyData { field1 = c, field2 = g.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void TumblingSnapshot4ColumnarSmallBatch() // like 2, but without grouping
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(20, 30, 2),
                StreamEvent.CreateInterval(30, 40, 4),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(10, 10)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void TumblingSnapshot5ColumnarSmallBatch() // like 4, but with max
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(20, 30, 1),
                StreamEvent.CreateInterval(30, 40, 2),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(10, 10)
                .Max(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void TumblingSnapshot6ColumnarSmallBatch() // like 5, but with min
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(20, 30, 1),
                StreamEvent.CreateInterval(30, 40, 2),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(10, 10)
                .Min(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void HoppingSnapshot1ColumnarSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(20, 40, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(30, 50, new MyData { field1 = 4, field2 = "D" })
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(20, 10)
                .GroupApply(e => e.field2, str => str.Sum(x => x.field1), (g, c) => new MyData { field1 = c, field2 = g.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void HoppingSnapshot2ColumnarSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(20, 30, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(30, 50, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreateInterval(30, 40, new MyData { field1 = 4, field2 = "A" }),
                StreamEvent.CreateInterval(40, 50, new MyData { field1 = 2, field2 = "A" })
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(20, 10)
                .GroupApply(e => e.field2, str => str.Sum(x => x.field1), (g, c) => new MyData { field1 = c, field2 = g.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.OrderBy(o => o.SyncTime).ThenBy(o => o.Payload.field1).SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void HoppingSnapshot3ColumnarSmallBatch()
        {

            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(20, 40, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(50, 70, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(50, 70, new MyData { field1 = 2, field2 = "D" })
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(20, 10)
                .GroupApply(e => e.field2, str => str.Sum(x => x.field1), (g, c) => new MyData { field1 = c, field2 = g.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.OrderBy(o => o.SyncTime).ThenBy(o => o.Payload.field2).SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void HoppingSnapshot4ColumnarSmallBatch() // like 2, but without grouping
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(20, 30, 2),
                StreamEvent.CreateInterval(30, 40, 6),
                StreamEvent.CreateInterval(40, 50, 4),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(20, 10)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void HoppingSnapshot5ColumnarSmallBatch() // like 5, but with max
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(20, 30, 1),
                StreamEvent.CreateInterval(30, 40, 2),
                StreamEvent.CreateInterval(40, 50, 2),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(20, 10)
                .Max(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void HoppingSnapshot6ColumnarSmallBatch() // like 5, but with min
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(20, 30, 1),
                StreamEvent.CreateInterval(30, 40, 1),
                StreamEvent.CreateInterval(40, 50, 2),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.HoppingWindowLifetime(20, 10)
                .Min(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshot1ColumnarSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 21, 2),
                StreamEvent.CreateInterval(21, 25, 4),
                StreamEvent.CreateInterval(25, 75, 6),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.SessionTimeoutWindow(50)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshot2ColumnarSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 18, 2),
                StreamEvent.CreateInterval(21, 25, 2),
                StreamEvent.CreateInterval(25, 31, 4),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.SessionTimeoutWindow(6)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshot3ColumnarSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 21, 2),
                StreamEvent.CreateInterval(21, 25, 4),
                StreamEvent.CreateInterval(25, 31, 6),
                StreamEvent.CreateInterval(31, 32, 7),
                StreamEvent.CreateInterval(32, 41, 8),
                StreamEvent.CreateInterval(41, 45, 10),
                StreamEvent.CreateInterval(45, 60, 12),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.SessionTimeoutWindow(20, 30)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshot4ColumnarSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(51, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(62, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(71, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(75, new MyData { field1 = 2, field2 = "D" }),
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 21, 2),
                StreamEvent.CreateInterval(21, 25, 4),
                StreamEvent.CreateInterval(25, 31, 6),
                StreamEvent.CreateInterval(31, 32, 7),
                StreamEvent.CreateInterval(32, 40, 8),
                StreamEvent.CreateInterval(41, 45, 2),
                StreamEvent.CreateInterval(45, 51, 4),
                StreamEvent.CreateInterval(51, 60, 5),
                StreamEvent.CreateInterval(62, 71, 1),
                StreamEvent.CreateInterval(71, 75, 3),
                StreamEvent.CreateInterval(75, 85, 5),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.SessionTimeoutWindow(10, 20)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshot5ColumnarSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(0, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(5, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(51, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(62, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(71, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(75, new MyData { field1 = 2, field2 = "D" }),
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(0, 5, 1),
                StreamEvent.CreateInterval(5, 11, 3),
                StreamEvent.CreateInterval(11, 12, 4),
                StreamEvent.CreateInterval(12, 20, 5),
                StreamEvent.CreateInterval(21, 25, 2),
                StreamEvent.CreateInterval(25, 31, 4),
                StreamEvent.CreateInterval(31, 32, 5),
                StreamEvent.CreateInterval(32, 40, 6),
                StreamEvent.CreateInterval(41, 45, 2),
                StreamEvent.CreateInterval(45, 51, 4),
                StreamEvent.CreateInterval(51, 60, 5),
                StreamEvent.CreateInterval(62, 71, 1),
                StreamEvent.CreateInterval(71, 75, 3),
                StreamEvent.CreateInterval(75, 85, 5),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.SessionTimeoutWindow(10, 20)
                .Sum(x => x.field1)
                ;

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotTrivialGroup1ColumnarSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 21, 2),
                StreamEvent.CreateInterval(21, 25, 4),
                StreamEvent.CreateInterval(25, 75, 6),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => true, o => o.SessionTimeoutWindow(50).Sum(x => x.field1), (k, p) => p);

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotTrivialGroup2ColumnarSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 18, 2),
                StreamEvent.CreateInterval(21, 25, 2),
                StreamEvent.CreateInterval(25, 31, 4),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => true, o => o.SessionTimeoutWindow(6).Sum(x => x.field1), (k, p) => p);

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotTrivialGroup3ColumnarSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 21, 2),
                StreamEvent.CreateInterval(21, 25, 4),
                StreamEvent.CreateInterval(25, 31, 6),
                StreamEvent.CreateInterval(31, 32, 7),
                StreamEvent.CreateInterval(32, 41, 8),
                StreamEvent.CreateInterval(41, 45, 10),
                StreamEvent.CreateInterval(45, 60, 12),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => true, o => o.SessionTimeoutWindow(20, 30).Sum(x => x.field1), (k, p) => p);

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotTrivialGroup4ColumnarSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(51, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(62, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(71, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(75, new MyData { field1 = 2, field2 = "D" }),
            };

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(11, 12, 1),
                StreamEvent.CreateInterval(12, 21, 2),
                StreamEvent.CreateInterval(21, 25, 4),
                StreamEvent.CreateInterval(25, 31, 6),
                StreamEvent.CreateInterval(31, 32, 7),
                StreamEvent.CreateInterval(32, 40, 8),
                StreamEvent.CreateInterval(41, 45, 2),
                StreamEvent.CreateInterval(45, 51, 4),
                StreamEvent.CreateInterval(51, 60, 5),
                StreamEvent.CreateInterval(62, 71, 1),
                StreamEvent.CreateInterval(71, 75, 3),
                StreamEvent.CreateInterval(75, 85, 5),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => true, o => o.SessionTimeoutWindow(10, 20).Sum(x => x.field1), (k, p) => p);

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotSimpleGroup1ColumnarSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(11, 12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(12, 21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(21, 71, new MyData { field1 = 4, field2 = "A" }),
                StreamEvent.CreateInterval(25, 75, new MyData { field1 = 2, field2 = "D" }),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => o.field2, o => o.SessionTimeoutWindow(50).Sum(x => x.field1), (k, p) => new MyData { field1 = p, field2 = k.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotSimpleGroup2ColumnarSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(11, 12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(12, 18, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(21, 27, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(25, 31, new MyData { field1 = 2, field2 = "D" }),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => o.field2, o => o.SessionTimeoutWindow(6).Sum(x => x.field1), (k, p) => new MyData { field1 = p, field2 = k.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotSimpleGroup3ColumnarSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" })
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(11, 12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(12, 21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(21, 31, new MyData { field1 = 4, field2 = "A" }),
                StreamEvent.CreateInterval(25, 45, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreateInterval(31, 32, new MyData { field1 = 5, field2 = "A" }),
                StreamEvent.CreateInterval(32, 41, new MyData { field1 = 6, field2 = "A" }),
                StreamEvent.CreateInterval(41, 60, new MyData { field1 = 8, field2 = "A" }),
                StreamEvent.CreateInterval(45, 65, new MyData { field1 = 2, field2 = "D" }),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => o.field2, o => o.SessionTimeoutWindow(20, 30).Sum(x => x.field1), (k, p) => new MyData { field1 = p, field2 = k.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionSnapshotSimpleGroup4ColumnarSmallBatch()
        {
            var input = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(11, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(25, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(31, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(45, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(51, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(62, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(71, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreatePoint(75, new MyData { field1 = 2, field2 = "D" }),
            };

            var expected = new StreamEvent<MyData>[]
            {
                StreamEvent.CreateInterval(11, 12, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(12, 21, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(21, 31, new MyData { field1 = 4, field2 = "A" }),
                StreamEvent.CreateInterval(25, 35, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreateInterval(31, 32, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(32, 41, new MyData { field1 = 2, field2 = "A" }),
                StreamEvent.CreateInterval(41, 51, new MyData { field1 = 4, field2 = "A" }),
                StreamEvent.CreateInterval(45, 55, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreateInterval(51, 61, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(62, 71, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreateInterval(71, 81, new MyData { field1 = 3, field2 = "A" }),
                StreamEvent.CreateInterval(75, 85, new MyData { field1 = 2, field2 = "D" }),
            };

            var inputStream = input.ToObservable().ToStreamable();
            var query = inputStream.GroupApply(o => o.field2, o => o.SessionTimeoutWindow(10, 20).Sum(x => x.field1), (k, p) => new MyData { field1 = p, field2 = k.Key });

            var result = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();

            Assert.IsTrue(result.SequenceEqual(expected));
        }
    }

}
