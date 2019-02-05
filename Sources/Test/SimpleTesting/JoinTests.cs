// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading.Tasks;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class JoinTestsRowMultiString : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public JoinTestsRowMultiString() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MapArity(1)
            .ReduceArity(1))
       { }

        [TestMethod, TestCategory("Gated")]
        public void IOOEJ1RowMultiString()
        {
            var pack = Guid.NewGuid();
            var cached1 = Enumerable.Range(0, 200).Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var cachedStr1 = cached1
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input1 = cachedStr1
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x.field1, pack)
                ;
            var cached2 = Enumerable.Range(0, 200).Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var cachedStr2 = cached2
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input2 = cachedStr2
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x.field1, pack)
                ;

            var query = input1.Join(input2, e => e.field1, e => e.field1, (l, r) => new GameData() { EventType = l.field1, GameId = (int)r.field2, });
            var result = query.ToPayloadEnumerable().ToArray();

            Assert.IsTrue(result.Length == 200);
            Assert.IsTrue(result
                .Select((gd, i) => gd.EventType == i && gd.GameId == 2 * i)
                .All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void IOOEJ2RowMultiString()
        {
            var pack = Guid.NewGuid();
            var cached1 = Enumerable.Range(0, 200)
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var cachedStr1 = cached1
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input1 = cachedStr1
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x.field1, pack)
                ;
            var cached2 = Enumerable.Range(0, 200)
                .ToObservable();
            var cachedStr2 = cached2
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input2 = cachedStr2
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x, pack)
                ;

            var query = input1
                .Join(
                    input2,
                    e => e.field1,
                    e => e,
                (l, r) => new GameData() { EventType = l.field1, GameId = r, });
            var result = query
                .ToPayloadEnumerable()
                .ToArray();

            Assert.IsTrue(result.Length == 200);
            Assert.IsTrue(result
                .Select((gd, i) => gd.EventType == i && gd.GameId == i)
                .All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeEquiJoin1RowMultiString()
        {
            var pack = Guid.NewGuid();
            var result = new List<GameData>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Reverse()
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var d1Subject = new Subject<MyStruct>();

            var data2 = Enumerable.Range(0, 200)
                .ToObservable();
            var d2Subject = new Subject<int>();

            var input1 = container.RegisterAtemporalInput(d1Subject, TimelinePolicy.Sequence(100));
            var input2 = container.RegisterAtemporalInput(d2Subject, TimelinePolicy.Sequence(100));

            var query = input1.Join(input2, e => e.field1, e => e, (l, r) => new GameData() { EventType = l.field1, GameId = r, });

            var output = container.RegisterAtemporalOutput(query);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Count == 200);
            Assert.IsTrue(result.Select(gd => gd.EventType == gd.GameId).All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeEquiJoin2RowMultiString()
        {
            var pack = Guid.NewGuid();

            var result = new List<GameData>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Reverse()
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var d1Subject = new Subject<MyStruct>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var d2Subject = new Subject<MyStruct>();

            var input1 = container.RegisterAtemporalInput(data1, TimelinePolicy.Sequence(100));
            var input2 = container.RegisterAtemporalInput(data2, TimelinePolicy.Sequence(100));

            var query = input1.Join(input2, e => e.field1, e => e.field1, (l, r) => new GameData() { EventType = l.field1, GameId = (int)r.field2, });
            var output = container.RegisterAtemporalOutput(query);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Count == 200);
            Assert.IsTrue(result
                .Select(gd => gd.EventType * 2 == gd.GameId)
                .All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeEquiJoin3RowMultiString()
        {
            var pack = Guid.NewGuid();
            var input1 = Enumerable.Range(0, 200)
                .Reverse()
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable()
                .ToAtemporalStreamable(TimelinePolicy.Sequence(100))
                ;
            var input2 = Enumerable.Range(0, 200)
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable()
                .ToAtemporalStreamable(TimelinePolicy.Sequence(100))
                ;

            var query = input1.Join(input2, e => e.field1, e => e.field1, (l, r) => new { EventType = (l.field1 * 2).ToString(), GameId = ((int)r.field2).ToString(), });
            var result = query.ToPayloadEnumerable().ToArray();

            Assert.IsTrue(result.Length == 200);
            Assert.IsTrue(result.All(gd => gd.EventType == gd.GameId));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin1RowMultiString()
        {
            var result = new List<StreamEvent<int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 100, i))
                .ToObservable();
            var d1Subject = new Subject<StreamEvent<int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 50, i))
                .ToObservable();
            var d2Subject = new Subject<StreamEvent<int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreateInterval(i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, e => e, e => e, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin2RowMultiString()
        {
            var result = new List<StreamEvent<int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 50, i))
                .ToObservable();
            var d1Subject = new Subject<StreamEvent<int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 100, i))
                .ToObservable();
            var d2Subject = new Subject<StreamEvent<int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreateInterval(i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, e => e, e => e, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin3RowMultiString()
        {
            var result = new List<PartitionedStreamEvent<int, int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 50, i))
                .ToObservable();
            var d1Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 100, i))
                .ToObservable();
            var d2Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreateInterval(i, i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin4RowMultiString()
        {
            var result = new List<PartitionedStreamEvent<int, int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 50, i))
                .ToObservable();
            var d1Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 100, i))
                .ToObservable();
            var d2Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreateInterval(i, i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, e => e, e => e, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }
    }

    [TestClass]
    public class JoinTestsRowRegularString : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public JoinTestsRowRegularString() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(false)
            .MapArity(1)
            .ReduceArity(1))
       { }

        [TestMethod, TestCategory("Gated")]
        public void IOOEJ1RowRegularString()
        {
            var pack = Guid.NewGuid();
            var cached1 = Enumerable.Range(0, 200).Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var cachedStr1 = cached1
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input1 = cachedStr1
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x.field1, pack)
                ;
            var cached2 = Enumerable.Range(0, 200).Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var cachedStr2 = cached2
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input2 = cachedStr2
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x.field1, pack)
                ;

            var query = input1.Join(input2, e => e.field1, e => e.field1, (l, r) => new GameData() { EventType = l.field1, GameId = (int)r.field2, });
            var result = query.ToPayloadEnumerable().ToArray();

            Assert.IsTrue(result.Length == 200);
            Assert.IsTrue(result
                .Select((gd, i) => gd.EventType == i && gd.GameId == 2 * i)
                .All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void IOOEJ2RowRegularString()
        {
            var pack = Guid.NewGuid();
            var cached1 = Enumerable.Range(0, 200)
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var cachedStr1 = cached1
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input1 = cachedStr1
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x.field1, pack)
                ;
            var cached2 = Enumerable.Range(0, 200)
                .ToObservable();
            var cachedStr2 = cached2
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input2 = cachedStr2
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x, pack)
                ;

            var query = input1
                .Join(
                    input2,
                    e => e.field1,
                    e => e,
                (l, r) => new GameData() { EventType = l.field1, GameId = r, });
            var result = query
                .ToPayloadEnumerable()
                .ToArray();

            Assert.IsTrue(result.Length == 200);
            Assert.IsTrue(result
                .Select((gd, i) => gd.EventType == i && gd.GameId == i)
                .All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeEquiJoin1RowRegularString()
        {
            var pack = Guid.NewGuid();
            var result = new List<GameData>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Reverse()
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var d1Subject = new Subject<MyStruct>();

            var data2 = Enumerable.Range(0, 200)
                .ToObservable();
            var d2Subject = new Subject<int>();

            var input1 = container.RegisterAtemporalInput(d1Subject, TimelinePolicy.Sequence(100));
            var input2 = container.RegisterAtemporalInput(d2Subject, TimelinePolicy.Sequence(100));

            var query = input1.Join(input2, e => e.field1, e => e, (l, r) => new GameData() { EventType = l.field1, GameId = r, });

            var output = container.RegisterAtemporalOutput(query);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Count == 200);
            Assert.IsTrue(result.Select(gd => gd.EventType == gd.GameId).All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeEquiJoin2RowRegularString()
        {
            var pack = Guid.NewGuid();

            var result = new List<GameData>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Reverse()
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var d1Subject = new Subject<MyStruct>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var d2Subject = new Subject<MyStruct>();

            var input1 = container.RegisterAtemporalInput(data1, TimelinePolicy.Sequence(100));
            var input2 = container.RegisterAtemporalInput(data2, TimelinePolicy.Sequence(100));

            var query = input1.Join(input2, e => e.field1, e => e.field1, (l, r) => new GameData() { EventType = l.field1, GameId = (int)r.field2, });
            var output = container.RegisterAtemporalOutput(query);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Count == 200);
            Assert.IsTrue(result
                .Select(gd => gd.EventType * 2 == gd.GameId)
                .All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeEquiJoin3RowRegularString()
        {
            var pack = Guid.NewGuid();
            var input1 = Enumerable.Range(0, 200)
                .Reverse()
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable()
                .ToAtemporalStreamable(TimelinePolicy.Sequence(100))
                ;
            var input2 = Enumerable.Range(0, 200)
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable()
                .ToAtemporalStreamable(TimelinePolicy.Sequence(100))
                ;

            var query = input1.Join(input2, e => e.field1, e => e.field1, (l, r) => new { EventType = (l.field1 * 2).ToString(), GameId = ((int)r.field2).ToString(), });
            var result = query.ToPayloadEnumerable().ToArray();

            Assert.IsTrue(result.Length == 200);
            Assert.IsTrue(result.All(gd => gd.EventType == gd.GameId));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin1RowRegularString()
        {
            var result = new List<StreamEvent<int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 100, i))
                .ToObservable();
            var d1Subject = new Subject<StreamEvent<int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 50, i))
                .ToObservable();
            var d2Subject = new Subject<StreamEvent<int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreateInterval(i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, e => e, e => e, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin2RowRegularString()
        {
            var result = new List<StreamEvent<int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 50, i))
                .ToObservable();
            var d1Subject = new Subject<StreamEvent<int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 100, i))
                .ToObservable();
            var d2Subject = new Subject<StreamEvent<int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreateInterval(i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, e => e, e => e, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin3RowRegularString()
        {
            var result = new List<PartitionedStreamEvent<int, int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 50, i))
                .ToObservable();
            var d1Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 100, i))
                .ToObservable();
            var d2Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreateInterval(i, i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin4RowRegularString()
        {
            var result = new List<PartitionedStreamEvent<int, int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 50, i))
                .ToObservable();
            var d1Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 100, i))
                .ToObservable();
            var d2Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreateInterval(i, i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, e => e, e => e, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }
    }

    [TestClass]
    public class JoinTestsRowSmallBatchMultiString : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public JoinTestsRowSmallBatchMultiString() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MapArity(1)
            .ReduceArity(1))
       { }

        [TestMethod, TestCategory("Gated")]
        public void IOOEJ1RowSmallBatchMultiString()
        {
            var pack = Guid.NewGuid();
            var cached1 = Enumerable.Range(0, 200).Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var cachedStr1 = cached1
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input1 = cachedStr1
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x.field1, pack)
                ;
            var cached2 = Enumerable.Range(0, 200).Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var cachedStr2 = cached2
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input2 = cachedStr2
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x.field1, pack)
                ;

            var query = input1.Join(input2, e => e.field1, e => e.field1, (l, r) => new GameData() { EventType = l.field1, GameId = (int)r.field2, });
            var result = query.ToPayloadEnumerable().ToArray();

            Assert.IsTrue(result.Length == 200);
            Assert.IsTrue(result
                .Select((gd, i) => gd.EventType == i && gd.GameId == 2 * i)
                .All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void IOOEJ2RowSmallBatchMultiString()
        {
            var pack = Guid.NewGuid();
            var cached1 = Enumerable.Range(0, 200)
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var cachedStr1 = cached1
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input1 = cachedStr1
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x.field1, pack)
                ;
            var cached2 = Enumerable.Range(0, 200)
                .ToObservable();
            var cachedStr2 = cached2
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input2 = cachedStr2
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x, pack)
                ;

            var query = input1
                .Join(
                    input2,
                    e => e.field1,
                    e => e,
                (l, r) => new GameData() { EventType = l.field1, GameId = r, });
            var result = query
                .ToPayloadEnumerable()
                .ToArray();

            Assert.IsTrue(result.Length == 200);
            Assert.IsTrue(result
                .Select((gd, i) => gd.EventType == i && gd.GameId == i)
                .All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeEquiJoin1RowSmallBatchMultiString()
        {
            var pack = Guid.NewGuid();
            var result = new List<GameData>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Reverse()
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var d1Subject = new Subject<MyStruct>();

            var data2 = Enumerable.Range(0, 200)
                .ToObservable();
            var d2Subject = new Subject<int>();

            var input1 = container.RegisterAtemporalInput(d1Subject, TimelinePolicy.Sequence(100));
            var input2 = container.RegisterAtemporalInput(d2Subject, TimelinePolicy.Sequence(100));

            var query = input1.Join(input2, e => e.field1, e => e, (l, r) => new GameData() { EventType = l.field1, GameId = r, });

            var output = container.RegisterAtemporalOutput(query);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Count == 200);
            Assert.IsTrue(result.Select(gd => gd.EventType == gd.GameId).All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeEquiJoin2RowSmallBatchMultiString()
        {
            var pack = Guid.NewGuid();

            var result = new List<GameData>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Reverse()
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var d1Subject = new Subject<MyStruct>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var d2Subject = new Subject<MyStruct>();

            var input1 = container.RegisterAtemporalInput(data1, TimelinePolicy.Sequence(100));
            var input2 = container.RegisterAtemporalInput(data2, TimelinePolicy.Sequence(100));

            var query = input1.Join(input2, e => e.field1, e => e.field1, (l, r) => new GameData() { EventType = l.field1, GameId = (int)r.field2, });
            var output = container.RegisterAtemporalOutput(query);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Count == 200);
            Assert.IsTrue(result
                .Select(gd => gd.EventType * 2 == gd.GameId)
                .All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeEquiJoin3RowSmallBatchMultiString()
        {
            var pack = Guid.NewGuid();
            var input1 = Enumerable.Range(0, 200)
                .Reverse()
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable()
                .ToAtemporalStreamable(TimelinePolicy.Sequence(100))
                ;
            var input2 = Enumerable.Range(0, 200)
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable()
                .ToAtemporalStreamable(TimelinePolicy.Sequence(100))
                ;

            var query = input1.Join(input2, e => e.field1, e => e.field1, (l, r) => new { EventType = (l.field1 * 2).ToString(), GameId = ((int)r.field2).ToString(), });
            var result = query.ToPayloadEnumerable().ToArray();

            Assert.IsTrue(result.Length == 200);
            Assert.IsTrue(result.All(gd => gd.EventType == gd.GameId));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin1RowSmallBatchMultiString()
        {
            var result = new List<StreamEvent<int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 100, i))
                .ToObservable();
            var d1Subject = new Subject<StreamEvent<int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 50, i))
                .ToObservable();
            var d2Subject = new Subject<StreamEvent<int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreateInterval(i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, e => e, e => e, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin2RowSmallBatchMultiString()
        {
            var result = new List<StreamEvent<int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 50, i))
                .ToObservable();
            var d1Subject = new Subject<StreamEvent<int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 100, i))
                .ToObservable();
            var d2Subject = new Subject<StreamEvent<int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreateInterval(i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, e => e, e => e, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin3RowSmallBatchMultiString()
        {
            var result = new List<PartitionedStreamEvent<int, int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 50, i))
                .ToObservable();
            var d1Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 100, i))
                .ToObservable();
            var d2Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreateInterval(i, i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin4RowSmallBatchMultiString()
        {
            var result = new List<PartitionedStreamEvent<int, int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 50, i))
                .ToObservable();
            var d1Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 100, i))
                .ToObservable();
            var d2Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreateInterval(i, i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, e => e, e => e, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }
    }

    [TestClass]
    public class JoinTestsRowSmallBatchRegularString : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public JoinTestsRowSmallBatchRegularString() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(false)
            .MapArity(1)
            .ReduceArity(1))
       { }

        [TestMethod, TestCategory("Gated")]
        public void IOOEJ1RowSmallBatchRegularString()
        {
            var pack = Guid.NewGuid();
            var cached1 = Enumerable.Range(0, 200).Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var cachedStr1 = cached1
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input1 = cachedStr1
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x.field1, pack)
                ;
            var cached2 = Enumerable.Range(0, 200).Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var cachedStr2 = cached2
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input2 = cachedStr2
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x.field1, pack)
                ;

            var query = input1.Join(input2, e => e.field1, e => e.field1, (l, r) => new GameData() { EventType = l.field1, GameId = (int)r.field2, });
            var result = query.ToPayloadEnumerable().ToArray();

            Assert.IsTrue(result.Length == 200);
            Assert.IsTrue(result
                .Select((gd, i) => gd.EventType == i && gd.GameId == 2 * i)
                .All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void IOOEJ2RowSmallBatchRegularString()
        {
            var pack = Guid.NewGuid();
            var cached1 = Enumerable.Range(0, 200)
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var cachedStr1 = cached1
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input1 = cachedStr1
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x.field1, pack)
                ;
            var cached2 = Enumerable.Range(0, 200)
                .ToObservable();
            var cachedStr2 = cached2
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input2 = cachedStr2
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x, pack)
                ;

            var query = input1
                .Join(
                    input2,
                    e => e.field1,
                    e => e,
                (l, r) => new GameData() { EventType = l.field1, GameId = r, });
            var result = query
                .ToPayloadEnumerable()
                .ToArray();

            Assert.IsTrue(result.Length == 200);
            Assert.IsTrue(result
                .Select((gd, i) => gd.EventType == i && gd.GameId == i)
                .All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeEquiJoin1RowSmallBatchRegularString()
        {
            var pack = Guid.NewGuid();
            var result = new List<GameData>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Reverse()
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var d1Subject = new Subject<MyStruct>();

            var data2 = Enumerable.Range(0, 200)
                .ToObservable();
            var d2Subject = new Subject<int>();

            var input1 = container.RegisterAtemporalInput(d1Subject, TimelinePolicy.Sequence(100));
            var input2 = container.RegisterAtemporalInput(d2Subject, TimelinePolicy.Sequence(100));

            var query = input1.Join(input2, e => e.field1, e => e, (l, r) => new GameData() { EventType = l.field1, GameId = r, });

            var output = container.RegisterAtemporalOutput(query);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Count == 200);
            Assert.IsTrue(result.Select(gd => gd.EventType == gd.GameId).All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeEquiJoin2RowSmallBatchRegularString()
        {
            var pack = Guid.NewGuid();

            var result = new List<GameData>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Reverse()
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var d1Subject = new Subject<MyStruct>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var d2Subject = new Subject<MyStruct>();

            var input1 = container.RegisterAtemporalInput(data1, TimelinePolicy.Sequence(100));
            var input2 = container.RegisterAtemporalInput(data2, TimelinePolicy.Sequence(100));

            var query = input1.Join(input2, e => e.field1, e => e.field1, (l, r) => new GameData() { EventType = l.field1, GameId = (int)r.field2, });
            var output = container.RegisterAtemporalOutput(query);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Count == 200);
            Assert.IsTrue(result
                .Select(gd => gd.EventType * 2 == gd.GameId)
                .All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeEquiJoin3RowSmallBatchRegularString()
        {
            var pack = Guid.NewGuid();
            var input1 = Enumerable.Range(0, 200)
                .Reverse()
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable()
                .ToAtemporalStreamable(TimelinePolicy.Sequence(100))
                ;
            var input2 = Enumerable.Range(0, 200)
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable()
                .ToAtemporalStreamable(TimelinePolicy.Sequence(100))
                ;

            var query = input1.Join(input2, e => e.field1, e => e.field1, (l, r) => new { EventType = (l.field1 * 2).ToString(), GameId = ((int)r.field2).ToString(), });
            var result = query.ToPayloadEnumerable().ToArray();

            Assert.IsTrue(result.Length == 200);
            Assert.IsTrue(result.All(gd => gd.EventType == gd.GameId));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin1RowSmallBatchRegularString()
        {
            var result = new List<StreamEvent<int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 100, i))
                .ToObservable();
            var d1Subject = new Subject<StreamEvent<int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 50, i))
                .ToObservable();
            var d2Subject = new Subject<StreamEvent<int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreateInterval(i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, e => e, e => e, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin2RowSmallBatchRegularString()
        {
            var result = new List<StreamEvent<int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 50, i))
                .ToObservable();
            var d1Subject = new Subject<StreamEvent<int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 100, i))
                .ToObservable();
            var d2Subject = new Subject<StreamEvent<int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreateInterval(i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, e => e, e => e, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin3RowSmallBatchRegularString()
        {
            var result = new List<PartitionedStreamEvent<int, int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 50, i))
                .ToObservable();
            var d1Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 100, i))
                .ToObservable();
            var d2Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreateInterval(i, i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin4RowSmallBatchRegularString()
        {
            var result = new List<PartitionedStreamEvent<int, int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 50, i))
                .ToObservable();
            var d1Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 100, i))
                .ToObservable();
            var d2Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreateInterval(i, i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, e => e, e => e, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }
    }

    [TestClass]
    public class JoinTestsColumnarMultiString : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public JoinTestsColumnarMultiString() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MapArity(1)
            .ReduceArity(1))
       { }

        [TestMethod, TestCategory("Gated")]
        public void IOOEJ1ColumnarMultiString()
        {
            var pack = Guid.NewGuid();
            var cached1 = Enumerable.Range(0, 200).Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var cachedStr1 = cached1
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input1 = cachedStr1
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x.field1, pack)
                ;
            var cached2 = Enumerable.Range(0, 200).Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var cachedStr2 = cached2
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input2 = cachedStr2
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x.field1, pack)
                ;

            var query = input1.Join(input2, e => e.field1, e => e.field1, (l, r) => new GameData() { EventType = l.field1, GameId = (int)r.field2, });
            var result = query.ToPayloadEnumerable().ToArray();

            Assert.IsTrue(result.Length == 200);
            Assert.IsTrue(result
                .Select((gd, i) => gd.EventType == i && gd.GameId == 2 * i)
                .All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void IOOEJ2ColumnarMultiString()
        {
            var pack = Guid.NewGuid();
            var cached1 = Enumerable.Range(0, 200)
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var cachedStr1 = cached1
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input1 = cachedStr1
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x.field1, pack)
                ;
            var cached2 = Enumerable.Range(0, 200)
                .ToObservable();
            var cachedStr2 = cached2
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input2 = cachedStr2
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x, pack)
                ;

            var query = input1
                .Join(
                    input2,
                    e => e.field1,
                    e => e,
                (l, r) => new GameData() { EventType = l.field1, GameId = r, });
            var result = query
                .ToPayloadEnumerable()
                .ToArray();

            Assert.IsTrue(result.Length == 200);
            Assert.IsTrue(result
                .Select((gd, i) => gd.EventType == i && gd.GameId == i)
                .All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeEquiJoin1ColumnarMultiString()
        {
            var pack = Guid.NewGuid();
            var result = new List<GameData>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Reverse()
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var d1Subject = new Subject<MyStruct>();

            var data2 = Enumerable.Range(0, 200)
                .ToObservable();
            var d2Subject = new Subject<int>();

            var input1 = container.RegisterAtemporalInput(d1Subject, TimelinePolicy.Sequence(100));
            var input2 = container.RegisterAtemporalInput(d2Subject, TimelinePolicy.Sequence(100));

            var query = input1.Join(input2, e => e.field1, e => e, (l, r) => new GameData() { EventType = l.field1, GameId = r, });

            var output = container.RegisterAtemporalOutput(query);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Count == 200);
            Assert.IsTrue(result.Select(gd => gd.EventType == gd.GameId).All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeEquiJoin2ColumnarMultiString()
        {
            var pack = Guid.NewGuid();

            var result = new List<GameData>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Reverse()
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var d1Subject = new Subject<MyStruct>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var d2Subject = new Subject<MyStruct>();

            var input1 = container.RegisterAtemporalInput(data1, TimelinePolicy.Sequence(100));
            var input2 = container.RegisterAtemporalInput(data2, TimelinePolicy.Sequence(100));

            var query = input1.Join(input2, e => e.field1, e => e.field1, (l, r) => new GameData() { EventType = l.field1, GameId = (int)r.field2, });
            var output = container.RegisterAtemporalOutput(query);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Count == 200);
            Assert.IsTrue(result
                .Select(gd => gd.EventType * 2 == gd.GameId)
                .All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeEquiJoin3ColumnarMultiString()
        {
            var pack = Guid.NewGuid();
            var input1 = Enumerable.Range(0, 200)
                .Reverse()
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable()
                .ToAtemporalStreamable(TimelinePolicy.Sequence(100))
                ;
            var input2 = Enumerable.Range(0, 200)
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable()
                .ToAtemporalStreamable(TimelinePolicy.Sequence(100))
                ;

            var query = input1.Join(input2, e => e.field1, e => e.field1, (l, r) => new { EventType = (l.field1 * 2).ToString(), GameId = ((int)r.field2).ToString(), });
            var result = query.ToPayloadEnumerable().ToArray();

            Assert.IsTrue(result.Length == 200);
            Assert.IsTrue(result.All(gd => gd.EventType == gd.GameId));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin1ColumnarMultiString()
        {
            var result = new List<StreamEvent<int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 100, i))
                .ToObservable();
            var d1Subject = new Subject<StreamEvent<int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 50, i))
                .ToObservable();
            var d2Subject = new Subject<StreamEvent<int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreateInterval(i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, e => e, e => e, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin2ColumnarMultiString()
        {
            var result = new List<StreamEvent<int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 50, i))
                .ToObservable();
            var d1Subject = new Subject<StreamEvent<int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 100, i))
                .ToObservable();
            var d2Subject = new Subject<StreamEvent<int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreateInterval(i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, e => e, e => e, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin3ColumnarMultiString()
        {
            var result = new List<PartitionedStreamEvent<int, int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 50, i))
                .ToObservable();
            var d1Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 100, i))
                .ToObservable();
            var d2Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreateInterval(i, i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin4ColumnarMultiString()
        {
            var result = new List<PartitionedStreamEvent<int, int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 50, i))
                .ToObservable();
            var d1Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 100, i))
                .ToObservable();
            var d2Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreateInterval(i, i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, e => e, e => e, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }
    }

    [TestClass]
    public class JoinTestsColumnarRegularString : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public JoinTestsColumnarRegularString() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(false)
            .MapArity(1)
            .ReduceArity(1))
       { }

        [TestMethod, TestCategory("Gated")]
        public void IOOEJ1ColumnarRegularString()
        {
            var pack = Guid.NewGuid();
            var cached1 = Enumerable.Range(0, 200).Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var cachedStr1 = cached1
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input1 = cachedStr1
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x.field1, pack)
                ;
            var cached2 = Enumerable.Range(0, 200).Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var cachedStr2 = cached2
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input2 = cachedStr2
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x.field1, pack)
                ;

            var query = input1.Join(input2, e => e.field1, e => e.field1, (l, r) => new GameData() { EventType = l.field1, GameId = (int)r.field2, });
            var result = query.ToPayloadEnumerable().ToArray();

            Assert.IsTrue(result.Length == 200);
            Assert.IsTrue(result
                .Select((gd, i) => gd.EventType == i && gd.GameId == 2 * i)
                .All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void IOOEJ2ColumnarRegularString()
        {
            var pack = Guid.NewGuid();
            var cached1 = Enumerable.Range(0, 200)
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var cachedStr1 = cached1
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input1 = cachedStr1
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x.field1, pack)
                ;
            var cached2 = Enumerable.Range(0, 200)
                .ToObservable();
            var cachedStr2 = cached2
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input2 = cachedStr2
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x, pack)
                ;

            var query = input1
                .Join(
                    input2,
                    e => e.field1,
                    e => e,
                (l, r) => new GameData() { EventType = l.field1, GameId = r, });
            var result = query
                .ToPayloadEnumerable()
                .ToArray();

            Assert.IsTrue(result.Length == 200);
            Assert.IsTrue(result
                .Select((gd, i) => gd.EventType == i && gd.GameId == i)
                .All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeEquiJoin1ColumnarRegularString()
        {
            var pack = Guid.NewGuid();
            var result = new List<GameData>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Reverse()
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var d1Subject = new Subject<MyStruct>();

            var data2 = Enumerable.Range(0, 200)
                .ToObservable();
            var d2Subject = new Subject<int>();

            var input1 = container.RegisterAtemporalInput(d1Subject, TimelinePolicy.Sequence(100));
            var input2 = container.RegisterAtemporalInput(d2Subject, TimelinePolicy.Sequence(100));

            var query = input1.Join(input2, e => e.field1, e => e, (l, r) => new GameData() { EventType = l.field1, GameId = r, });

            var output = container.RegisterAtemporalOutput(query);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Count == 200);
            Assert.IsTrue(result.Select(gd => gd.EventType == gd.GameId).All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeEquiJoin2ColumnarRegularString()
        {
            var pack = Guid.NewGuid();

            var result = new List<GameData>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Reverse()
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var d1Subject = new Subject<MyStruct>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var d2Subject = new Subject<MyStruct>();

            var input1 = container.RegisterAtemporalInput(data1, TimelinePolicy.Sequence(100));
            var input2 = container.RegisterAtemporalInput(data2, TimelinePolicy.Sequence(100));

            var query = input1.Join(input2, e => e.field1, e => e.field1, (l, r) => new GameData() { EventType = l.field1, GameId = (int)r.field2, });
            var output = container.RegisterAtemporalOutput(query);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Count == 200);
            Assert.IsTrue(result
                .Select(gd => gd.EventType * 2 == gd.GameId)
                .All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeEquiJoin3ColumnarRegularString()
        {
            var pack = Guid.NewGuid();
            var input1 = Enumerable.Range(0, 200)
                .Reverse()
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable()
                .ToAtemporalStreamable(TimelinePolicy.Sequence(100))
                ;
            var input2 = Enumerable.Range(0, 200)
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable()
                .ToAtemporalStreamable(TimelinePolicy.Sequence(100))
                ;

            var query = input1.Join(input2, e => e.field1, e => e.field1, (l, r) => new { EventType = (l.field1 * 2).ToString(), GameId = ((int)r.field2).ToString(), });
            var result = query.ToPayloadEnumerable().ToArray();

            Assert.IsTrue(result.Length == 200);
            Assert.IsTrue(result.All(gd => gd.EventType == gd.GameId));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin1ColumnarRegularString()
        {
            var result = new List<StreamEvent<int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 100, i))
                .ToObservable();
            var d1Subject = new Subject<StreamEvent<int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 50, i))
                .ToObservable();
            var d2Subject = new Subject<StreamEvent<int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreateInterval(i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, e => e, e => e, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin2ColumnarRegularString()
        {
            var result = new List<StreamEvent<int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 50, i))
                .ToObservable();
            var d1Subject = new Subject<StreamEvent<int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 100, i))
                .ToObservable();
            var d2Subject = new Subject<StreamEvent<int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreateInterval(i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, e => e, e => e, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin3ColumnarRegularString()
        {
            var result = new List<PartitionedStreamEvent<int, int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 50, i))
                .ToObservable();
            var d1Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 100, i))
                .ToObservable();
            var d2Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreateInterval(i, i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin4ColumnarRegularString()
        {
            var result = new List<PartitionedStreamEvent<int, int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 50, i))
                .ToObservable();
            var d1Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 100, i))
                .ToObservable();
            var d2Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreateInterval(i, i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, e => e, e => e, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }
    }

    [TestClass]
    public class JoinTestsColumnarSmallBatchMultiString : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public JoinTestsColumnarSmallBatchMultiString() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(true)
            .MapArity(1)
            .ReduceArity(1))
       { }

        [TestMethod, TestCategory("Gated")]
        public void IOOEJ1ColumnarSmallBatchMultiString()
        {
            var pack = Guid.NewGuid();
            var cached1 = Enumerable.Range(0, 200).Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var cachedStr1 = cached1
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input1 = cachedStr1
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x.field1, pack)
                ;
            var cached2 = Enumerable.Range(0, 200).Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var cachedStr2 = cached2
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input2 = cachedStr2
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x.field1, pack)
                ;

            var query = input1.Join(input2, e => e.field1, e => e.field1, (l, r) => new GameData() { EventType = l.field1, GameId = (int)r.field2, });
            var result = query.ToPayloadEnumerable().ToArray();

            Assert.IsTrue(result.Length == 200);
            Assert.IsTrue(result
                .Select((gd, i) => gd.EventType == i && gd.GameId == 2 * i)
                .All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void IOOEJ2ColumnarSmallBatchMultiString()
        {
            var pack = Guid.NewGuid();
            var cached1 = Enumerable.Range(0, 200)
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var cachedStr1 = cached1
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input1 = cachedStr1
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x.field1, pack)
                ;
            var cached2 = Enumerable.Range(0, 200)
                .ToObservable();
            var cachedStr2 = cached2
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input2 = cachedStr2
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x, pack)
                ;

            var query = input1
                .Join(
                    input2,
                    e => e.field1,
                    e => e,
                (l, r) => new GameData() { EventType = l.field1, GameId = r, });
            var result = query
                .ToPayloadEnumerable()
                .ToArray();

            Assert.IsTrue(result.Length == 200);
            Assert.IsTrue(result
                .Select((gd, i) => gd.EventType == i && gd.GameId == i)
                .All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeEquiJoin1ColumnarSmallBatchMultiString()
        {
            var pack = Guid.NewGuid();
            var result = new List<GameData>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Reverse()
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var d1Subject = new Subject<MyStruct>();

            var data2 = Enumerable.Range(0, 200)
                .ToObservable();
            var d2Subject = new Subject<int>();

            var input1 = container.RegisterAtemporalInput(d1Subject, TimelinePolicy.Sequence(100));
            var input2 = container.RegisterAtemporalInput(d2Subject, TimelinePolicy.Sequence(100));

            var query = input1.Join(input2, e => e.field1, e => e, (l, r) => new GameData() { EventType = l.field1, GameId = r, });

            var output = container.RegisterAtemporalOutput(query);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Count == 200);
            Assert.IsTrue(result.Select(gd => gd.EventType == gd.GameId).All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeEquiJoin2ColumnarSmallBatchMultiString()
        {
            var pack = Guid.NewGuid();

            var result = new List<GameData>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Reverse()
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var d1Subject = new Subject<MyStruct>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var d2Subject = new Subject<MyStruct>();

            var input1 = container.RegisterAtemporalInput(data1, TimelinePolicy.Sequence(100));
            var input2 = container.RegisterAtemporalInput(data2, TimelinePolicy.Sequence(100));

            var query = input1.Join(input2, e => e.field1, e => e.field1, (l, r) => new GameData() { EventType = l.field1, GameId = (int)r.field2, });
            var output = container.RegisterAtemporalOutput(query);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Count == 200);
            Assert.IsTrue(result
                .Select(gd => gd.EventType * 2 == gd.GameId)
                .All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeEquiJoin3ColumnarSmallBatchMultiString()
        {
            var pack = Guid.NewGuid();
            var input1 = Enumerable.Range(0, 200)
                .Reverse()
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable()
                .ToAtemporalStreamable(TimelinePolicy.Sequence(100))
                ;
            var input2 = Enumerable.Range(0, 200)
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable()
                .ToAtemporalStreamable(TimelinePolicy.Sequence(100))
                ;

            var query = input1.Join(input2, e => e.field1, e => e.field1, (l, r) => new { EventType = (l.field1 * 2).ToString(), GameId = ((int)r.field2).ToString(), });
            var result = query.ToPayloadEnumerable().ToArray();

            Assert.IsTrue(result.Length == 200);
            Assert.IsTrue(result.All(gd => gd.EventType == gd.GameId));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin1ColumnarSmallBatchMultiString()
        {
            var result = new List<StreamEvent<int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 100, i))
                .ToObservable();
            var d1Subject = new Subject<StreamEvent<int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 50, i))
                .ToObservable();
            var d2Subject = new Subject<StreamEvent<int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreateInterval(i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, e => e, e => e, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin2ColumnarSmallBatchMultiString()
        {
            var result = new List<StreamEvent<int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 50, i))
                .ToObservable();
            var d1Subject = new Subject<StreamEvent<int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 100, i))
                .ToObservable();
            var d2Subject = new Subject<StreamEvent<int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreateInterval(i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, e => e, e => e, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin3ColumnarSmallBatchMultiString()
        {
            var result = new List<PartitionedStreamEvent<int, int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 50, i))
                .ToObservable();
            var d1Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 100, i))
                .ToObservable();
            var d2Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreateInterval(i, i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin4ColumnarSmallBatchMultiString()
        {
            var result = new List<PartitionedStreamEvent<int, int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 50, i))
                .ToObservable();
            var d1Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 100, i))
                .ToObservable();
            var d2Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreateInterval(i, i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, e => e, e => e, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }
    }

    [TestClass]
    public class JoinTestsColumnarSmallBatchRegularString : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public JoinTestsColumnarSmallBatchRegularString() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .UseMultiString(false)
            .MapArity(1)
            .ReduceArity(1))
       { }

        [TestMethod, TestCategory("Gated")]
        public void IOOEJ1ColumnarSmallBatchRegularString()
        {
            var pack = Guid.NewGuid();
            var cached1 = Enumerable.Range(0, 200).Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var cachedStr1 = cached1
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input1 = cachedStr1
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x.field1, pack)
                ;
            var cached2 = Enumerable.Range(0, 200).Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var cachedStr2 = cached2
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input2 = cachedStr2
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x.field1, pack)
                ;

            var query = input1.Join(input2, e => e.field1, e => e.field1, (l, r) => new GameData() { EventType = l.field1, GameId = (int)r.field2, });
            var result = query.ToPayloadEnumerable().ToArray();

            Assert.IsTrue(result.Length == 200);
            Assert.IsTrue(result
                .Select((gd, i) => gd.EventType == i && gd.GameId == 2 * i)
                .All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void IOOEJ2ColumnarSmallBatchRegularString()
        {
            var pack = Guid.NewGuid();
            var cached1 = Enumerable.Range(0, 200)
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var cachedStr1 = cached1
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input1 = cachedStr1
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x.field1, pack)
                ;
            var cached2 = Enumerable.Range(0, 200)
                .ToObservable();
            var cachedStr2 = cached2
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var input2 = cachedStr2
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .SetProperty().IsSnapshotSorted(true, x => x, pack)
                ;

            var query = input1
                .Join(
                    input2,
                    e => e.field1,
                    e => e,
                (l, r) => new GameData() { EventType = l.field1, GameId = r, });
            var result = query
                .ToPayloadEnumerable()
                .ToArray();

            Assert.IsTrue(result.Length == 200);
            Assert.IsTrue(result
                .Select((gd, i) => gd.EventType == i && gd.GameId == i)
                .All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeEquiJoin1ColumnarSmallBatchRegularString()
        {
            var pack = Guid.NewGuid();
            var result = new List<GameData>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Reverse()
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var d1Subject = new Subject<MyStruct>();

            var data2 = Enumerable.Range(0, 200)
                .ToObservable();
            var d2Subject = new Subject<int>();

            var input1 = container.RegisterAtemporalInput(d1Subject, TimelinePolicy.Sequence(100));
            var input2 = container.RegisterAtemporalInput(d2Subject, TimelinePolicy.Sequence(100));

            var query = input1.Join(input2, e => e.field1, e => e, (l, r) => new GameData() { EventType = l.field1, GameId = r, });

            var output = container.RegisterAtemporalOutput(query);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Count == 200);
            Assert.IsTrue(result.Select(gd => gd.EventType == gd.GameId).All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeEquiJoin2ColumnarSmallBatchRegularString()
        {
            var pack = Guid.NewGuid();

            var result = new List<GameData>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Reverse()
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var d1Subject = new Subject<MyStruct>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable();
            var d2Subject = new Subject<MyStruct>();

            var input1 = container.RegisterAtemporalInput(data1, TimelinePolicy.Sequence(100));
            var input2 = container.RegisterAtemporalInput(data2, TimelinePolicy.Sequence(100));

            var query = input1.Join(input2, e => e.field1, e => e.field1, (l, r) => new GameData() { EventType = l.field1, GameId = (int)r.field2, });
            var output = container.RegisterAtemporalOutput(query);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Count == 200);
            Assert.IsTrue(result
                .Select(gd => gd.EventType * 2 == gd.GameId)
                .All(b => b));
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeEquiJoin3ColumnarSmallBatchRegularString()
        {
            var pack = Guid.NewGuid();
            var input1 = Enumerable.Range(0, 200)
                .Reverse()
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable()
                .ToAtemporalStreamable(TimelinePolicy.Sequence(100))
                ;
            var input2 = Enumerable.Range(0, 200)
                .Select(i => new MyStruct() { field1 = i, field2 = 2.0 * i, })
                .ToObservable()
                .ToAtemporalStreamable(TimelinePolicy.Sequence(100))
                ;

            var query = input1.Join(input2, e => e.field1, e => e.field1, (l, r) => new { EventType = (l.field1 * 2).ToString(), GameId = ((int)r.field2).ToString(), });
            var result = query.ToPayloadEnumerable().ToArray();

            Assert.IsTrue(result.Length == 200);
            Assert.IsTrue(result.All(gd => gd.EventType == gd.GameId));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin1ColumnarSmallBatchRegularString()
        {
            var result = new List<StreamEvent<int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 100, i))
                .ToObservable();
            var d1Subject = new Subject<StreamEvent<int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 50, i))
                .ToObservable();
            var d2Subject = new Subject<StreamEvent<int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreateInterval(i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, e => e, e => e, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin2ColumnarSmallBatchRegularString()
        {
            var result = new List<StreamEvent<int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 50, i))
                .ToObservable();
            var d1Subject = new Subject<StreamEvent<int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreatePoint(i + 100, i))
                .ToObservable();
            var d2Subject = new Subject<StreamEvent<int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => StreamEvent.CreateInterval(i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, e => e, e => e, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin3ColumnarSmallBatchRegularString()
        {
            var result = new List<PartitionedStreamEvent<int, int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 50, i))
                .ToObservable();
            var d1Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 100, i))
                .ToObservable();
            var d2Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreateInterval(i, i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }

        [TestMethod, TestCategory("Gated")]
        public void FixedIntervalEquiJoin4ColumnarSmallBatchRegularString()
        {
            var result = new List<PartitionedStreamEvent<int, int>>();

            var container = new QueryContainer(null);

            var data1 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 50, i))
                .ToObservable();
            var d1Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var data2 = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreatePoint(i, i + 100, i))
                .ToObservable();
            var d2Subject = new Subject<PartitionedStreamEvent<int, int>>();

            var dataExpected = Enumerable.Range(0, 200)
                .Select(i => PartitionedStreamEvent.CreateInterval(i, i + 100, i + 150, i));

            var input1 = container.RegisterInput(d1Subject).AlterEventDuration(100);
            var input2 = container.RegisterInput(d2Subject).AlterEventDuration(100);

            var query = input1.Join(input2, e => e, e => e, (l, r) => l);

            var output = container.RegisterOutput(query, ReshapingPolicy.CoalesceEndEdges);
            var resultAsync = output.ForEachAsync(o => result.Add(o));

            container.Restore(null); // start the query

            var i1async = data1.ForEachAsync(e => d1Subject.OnNext(e)); // send data
            var i2async = data2.ForEachAsync(e => d2Subject.OnNext(e)); // send data

            Task.WaitAll(i1async, i2async); // wait for data to be processed.

            d1Subject.OnCompleted(); // send onCompleted event.
            d2Subject.OnCompleted();

            resultAsync.Wait(); // wait for results.

            Assert.IsTrue(result.Where(e => e.IsData).SequenceEqual(dataExpected));
        }
    }

}
