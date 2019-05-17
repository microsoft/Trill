// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public struct MyData
    {
        public int field1;
        public string field2;

        public override string ToString() => $"field1 = {this.field1}, field2 = \"{this.field2}\"";
    }

    internal struct MyData2
    {
        public int field3;
        public string field4;
    }

    internal struct MyData3
    {
        public int field1;
        public string field2;
        public int field3;
        public string field4;
    }

    [TestClass]
    public class LeftOuterJoinTestsRow : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public LeftOuterJoinTestsRow() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .MapArity(1)
            .ReduceArity(1))
        { }

        [TestMethod, TestCategory("Gated")]
        public void LOJ1Row()
        {
            var container = new QueryContainer(null);

            var left = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(10, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(10, new MyData { field1 = 1, field2 = "B" }),
                StreamEvent.CreatePoint(10, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(10, new MyData { field1 = 2, field2 = "E" })
            };

            var right = new StreamEvent<MyData2>[]
            {
                StreamEvent.CreatePoint(10, new MyData2 { field3 = 1, field4 = "W" }),
                StreamEvent.CreatePoint(10, new MyData2 { field3 = 1, field4 = "X" }),
                StreamEvent.CreatePoint(10, new MyData2 { field3 = 2, field4 = "Y" })
            };

            var output = new List<StreamEvent<MyData3>>();
            var expected = new StreamEvent<MyData3>[]
            {
                StreamEvent.CreatePoint(10, new MyData3 { field1 = 2, field2 = "E", field3 = -1, field4 = "null" }),
                StreamEvent.CreatePoint(10, new MyData3 { field1 = 1, field2 = "B", field3 = 1, field4 = "W" }),
                StreamEvent.CreatePoint(10, new MyData3 { field1 = 1, field2 = "A", field3 = 1, field4 = "W" }),
                StreamEvent.CreatePoint(10, new MyData3 { field1 = 1, field2 = "B", field3 = 1, field4 = "X" }),
                StreamEvent.CreatePoint(10, new MyData3 { field1 = 1, field2 = "A", field3 = 1, field4 = "X" }),
                StreamEvent.CreatePoint(10, new MyData3 { field1 = 2, field2 = "D", field3 = 2, field4 = "Y" }),
            };

            var leftStream = container.RegisterInput(left.ToObservable());
            var rightStream = container.RegisterInput(right.ToObservable());

            int tmp1 = -1;
            string tmp2 = "null";

            var query =
            leftStream.LeftOuterJoin(rightStream, e => e.field1, e => e.field3,
               (l, r) => l.field2 != "E",
                (l) => new MyData3 { field1 = l.field1, field2 = l.field2, field3 = tmp1, field4 = tmp2 },
                (l, r) => new MyData3 { field1 = l.field1, field2 = l.field2, field3 = r.field3, field4 = r.field4 });

            var result = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData);
            var resultAsync = result.ForEachAsync(o => output.Add(o));
            container.Restore(null); // start the query

            Assert.IsTrue(output.ToArray().SequenceEqual(expected));
        }
    }

    [TestClass]
    public class LeftOuterJoinTestsRowSmallBatch : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public LeftOuterJoinTestsRowSmallBatch() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .MapArity(1)
            .ReduceArity(1))
        { }

        [TestMethod, TestCategory("Gated")]
        public void LOJ1RowSmallBatch()
        {
            var container = new QueryContainer(null);

            var left = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(10, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(10, new MyData { field1 = 1, field2 = "B" }),
                StreamEvent.CreatePoint(10, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(10, new MyData { field1 = 2, field2 = "E" })
            };

            var right = new StreamEvent<MyData2>[]
            {
                StreamEvent.CreatePoint(10, new MyData2 { field3 = 1, field4 = "W" }),
                StreamEvent.CreatePoint(10, new MyData2 { field3 = 1, field4 = "X" }),
                StreamEvent.CreatePoint(10, new MyData2 { field3 = 2, field4 = "Y" })
            };

            var output = new List<StreamEvent<MyData3>>();
            var expected = new StreamEvent<MyData3>[]
            {
                StreamEvent.CreatePoint(10, new MyData3 { field1 = 2, field2 = "E", field3 = -1, field4 = "null" }),
                StreamEvent.CreatePoint(10, new MyData3 { field1 = 1, field2 = "B", field3 = 1, field4 = "W" }),
                StreamEvent.CreatePoint(10, new MyData3 { field1 = 1, field2 = "A", field3 = 1, field4 = "W" }),
                StreamEvent.CreatePoint(10, new MyData3 { field1 = 1, field2 = "B", field3 = 1, field4 = "X" }),
                StreamEvent.CreatePoint(10, new MyData3 { field1 = 1, field2 = "A", field3 = 1, field4 = "X" }),
                StreamEvent.CreatePoint(10, new MyData3 { field1 = 2, field2 = "D", field3 = 2, field4 = "Y" }),
            };

            var leftStream = container.RegisterInput(left.ToObservable());
            var rightStream = container.RegisterInput(right.ToObservable());

            int tmp1 = -1;
            string tmp2 = "null";

            var query =
            leftStream.LeftOuterJoin(rightStream, e => e.field1, e => e.field3,
               (l, r) => l.field2 != "E",
                (l) => new MyData3 { field1 = l.field1, field2 = l.field2, field3 = tmp1, field4 = tmp2 },
                (l, r) => new MyData3 { field1 = l.field1, field2 = l.field2, field3 = r.field3, field4 = r.field4 });

            var result = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData);
            var resultAsync = result.ForEachAsync(o => output.Add(o));
            container.Restore(null); // start the query

            Assert.IsTrue(output.ToArray().SequenceEqual(expected));
        }
    }

    [TestClass]
    public class LeftOuterJoinTestsColumnar : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public LeftOuterJoinTestsColumnar() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .MapArity(1)
            .ReduceArity(1))
        { }

        [TestMethod, TestCategory("Gated")]
        public void LOJ1Columnar()
        {
            var container = new QueryContainer(null);

            var left = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(10, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(10, new MyData { field1 = 1, field2 = "B" }),
                StreamEvent.CreatePoint(10, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(10, new MyData { field1 = 2, field2 = "E" })
            };

            var right = new StreamEvent<MyData2>[]
            {
                StreamEvent.CreatePoint(10, new MyData2 { field3 = 1, field4 = "W" }),
                StreamEvent.CreatePoint(10, new MyData2 { field3 = 1, field4 = "X" }),
                StreamEvent.CreatePoint(10, new MyData2 { field3 = 2, field4 = "Y" })
            };

            var output = new List<StreamEvent<MyData3>>();
            var expected = new StreamEvent<MyData3>[]
            {
                StreamEvent.CreatePoint(10, new MyData3 { field1 = 2, field2 = "E", field3 = -1, field4 = "null" }),
                StreamEvent.CreatePoint(10, new MyData3 { field1 = 1, field2 = "B", field3 = 1, field4 = "W" }),
                StreamEvent.CreatePoint(10, new MyData3 { field1 = 1, field2 = "A", field3 = 1, field4 = "W" }),
                StreamEvent.CreatePoint(10, new MyData3 { field1 = 1, field2 = "B", field3 = 1, field4 = "X" }),
                StreamEvent.CreatePoint(10, new MyData3 { field1 = 1, field2 = "A", field3 = 1, field4 = "X" }),
                StreamEvent.CreatePoint(10, new MyData3 { field1 = 2, field2 = "D", field3 = 2, field4 = "Y" }),
            };

            var leftStream = container.RegisterInput(left.ToObservable());
            var rightStream = container.RegisterInput(right.ToObservable());

            int tmp1 = -1;
            string tmp2 = "null";

            var query =
            leftStream.LeftOuterJoin(rightStream, e => e.field1, e => e.field3,
               (l, r) => l.field2 != "E",
                (l) => new MyData3 { field1 = l.field1, field2 = l.field2, field3 = tmp1, field4 = tmp2 },
                (l, r) => new MyData3 { field1 = l.field1, field2 = l.field2, field3 = r.field3, field4 = r.field4 });

            var result = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData);
            var resultAsync = result.ForEachAsync(o => output.Add(o));
            container.Restore(null); // start the query

            Assert.IsTrue(output.ToArray().SequenceEqual(expected));
        }
    }

    [TestClass]
    public class LeftOuterJoinTestsColumnarSmallBatch : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public LeftOuterJoinTestsColumnarSmallBatch() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .MapArity(1)
            .ReduceArity(1))
        { }

        [TestMethod, TestCategory("Gated")]
        public void LOJ1ColumnarSmallBatch()
        {
            var container = new QueryContainer(null);

            var left = new StreamEvent<MyData>[]
            {
                StreamEvent.CreatePoint(10, new MyData { field1 = 1, field2 = "A" }),
                StreamEvent.CreatePoint(10, new MyData { field1 = 1, field2 = "B" }),
                StreamEvent.CreatePoint(10, new MyData { field1 = 2, field2 = "D" }),
                StreamEvent.CreatePoint(10, new MyData { field1 = 2, field2 = "E" })
            };

            var right = new StreamEvent<MyData2>[]
            {
                StreamEvent.CreatePoint(10, new MyData2 { field3 = 1, field4 = "W" }),
                StreamEvent.CreatePoint(10, new MyData2 { field3 = 1, field4 = "X" }),
                StreamEvent.CreatePoint(10, new MyData2 { field3 = 2, field4 = "Y" })
            };

            var output = new List<StreamEvent<MyData3>>();
            var expected = new StreamEvent<MyData3>[]
            {
                StreamEvent.CreatePoint(10, new MyData3 { field1 = 2, field2 = "E", field3 = -1, field4 = "null" }),
                StreamEvent.CreatePoint(10, new MyData3 { field1 = 1, field2 = "B", field3 = 1, field4 = "W" }),
                StreamEvent.CreatePoint(10, new MyData3 { field1 = 1, field2 = "A", field3 = 1, field4 = "W" }),
                StreamEvent.CreatePoint(10, new MyData3 { field1 = 1, field2 = "B", field3 = 1, field4 = "X" }),
                StreamEvent.CreatePoint(10, new MyData3 { field1 = 1, field2 = "A", field3 = 1, field4 = "X" }),
                StreamEvent.CreatePoint(10, new MyData3 { field1 = 2, field2 = "D", field3 = 2, field4 = "Y" }),
            };

            var leftStream = container.RegisterInput(left.ToObservable());
            var rightStream = container.RegisterInput(right.ToObservable());

            int tmp1 = -1;
            string tmp2 = "null";

            var query =
            leftStream.LeftOuterJoin(rightStream, e => e.field1, e => e.field3,
               (l, r) => l.field2 != "E",
                (l) => new MyData3 { field1 = l.field1, field2 = l.field2, field3 = tmp1, field4 = tmp2 },
                (l, r) => new MyData3 { field1 = l.field1, field2 = l.field2, field3 = r.field3, field4 = r.field4 });

            var result = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData);
            var resultAsync = result.ForEachAsync(o => output.Add(o));
            container.Restore(null); // start the query

            Assert.IsTrue(output.ToArray().SequenceEqual(expected));
        }
    }
}
