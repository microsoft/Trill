// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [DataContract]
    public struct MachineCountTest
    {
        [DataMember]
        public string MachineId;
        [DataMember]
        public ulong ActivityCount;
    }

    [TestClass]
    public class CheckpointRestoreTestsAllowFallbackRow : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public CheckpointRestoreTestsAllowFallbackRow() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(false)
            .MapArity(1)
            .ReduceArity(1))
        { }
        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointHoppingWindowSumRow()
        {
            for (int window = 1; window <= 10; window += 9)
            {
                for (int period = 1; period <= 20; period += 11)
                {
                    var preCheckpointSubject = new Subject<StreamEvent<int>>();
                    var postCheckpointSubject = new Subject<StreamEvent<int>>();

                    var outputListWithCheckpoint = new List<StreamEvent<ulong>>();
                    var outputListWithoutCheckpoint = new List<StreamEvent<ulong>>();

                    // Inputs
                    var container1 = new QueryContainer();
                    var container2 = new QueryContainer();
                    var container3 = new QueryContainer();
                    Stream state = new MemoryStream();

                    // Input data
                    var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                        .ToObservable()
                        .Select(e => StreamEvent.CreateStart(e, e));

                    var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                        .ToObservable()
                        .Select(e => StreamEvent.CreateStart(e, e));

                    var fullData = Enumerable.Range(0, 20000).ToList()
                        .ToObservable()
                        .Select(e => StreamEvent.CreateStart(e, e));

                    // Query 1: CheckpointBasicUnaryCheckpointEquiJoinRow
                    var input1 = container1.RegisterInput(preCheckpointSubject);
                    var query1 = input1.HoppingWindowLifetime(window, period).Sum(e => (ulong)e);

                    var output1 = container1.RegisterOutput(query1);

                    var outputAsync1 = output1.Where(e => e.IsData).ForEachAsync(o => outputListWithCheckpoint.Add(o));
                    var pipe1 = container1.Restore(null);
                    preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
                    pipe1.Checkpoint(state);

                    state.Seek(0, SeekOrigin.Begin);

                    // Query 2: Restore
                    var input2 = container2.RegisterInput(postCheckpointSubject);
                    var query2 = input2.HoppingWindowLifetime(window, period).Sum(e => (ulong)e);
                    var output2 = container2.RegisterOutput(query2);

                    var outputAsync2 = output2.Where(e => e.IsData).ForEachAsync(o => outputListWithCheckpoint.Add(o));

                    var pipe2 = container2.Restore(state);
                    postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
                    postCheckpointSubject.OnCompleted();
                    outputAsync2.Wait();
                    outputListWithCheckpoint.Sort((x, y) =>
                    {
                        var res = x.SyncTime.CompareTo(y.SyncTime);
                        if (res == 0)
                            res = x.OtherTime.CompareTo(y.OtherTime);
                        if (res == 0)
                            res = x.Payload.CompareTo(y.Payload);
                        return res;
                    });

                    // Query 3: Total
                    var input3 = container3.RegisterInput(fullData);
                    var query3 = input3.HoppingWindowLifetime(window, period).Sum(e => (ulong)e);
                    var output3 = container3.RegisterOutput(query3);

                    var outputAsync3 = output3.Where(e => e.IsData).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));
                    container3.Restore(null);
                    outputAsync3.Wait();
                    outputListWithoutCheckpoint.Sort((x, y) =>
                    {
                        var res = x.SyncTime.CompareTo(y.SyncTime);
                        if (res == 0)
                            res = x.OtherTime.CompareTo(y.OtherTime);
                        if (res == 0)
                            res = x.Payload.CompareTo(y.Payload);
                        return res;
                    });

                    Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
                }
            }
        }
    }

    [TestClass]
    public class CheckpointRestoreTestsRow : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public CheckpointRestoreTestsRow() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .MapArity(1)
            .ReduceArity(1))
        { }

        private static IStreamable<Empty, int> CreateBasicQuery(IStreamable<Empty, int> input)
            => input
                .Where(e => e % 2 == 1)
                .AlterEventLifetime(e => e + 1, StreamEvent.InfinitySyncTime)
                .Select(e => e / 2);

        [TestMethod, TestCategory("Gated")]
        public void BasicIngressDiagnosticRow()
        {
            var diagnosticList = new List<int>();

            // Inputs
            var container = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var data = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e % 2 == 0 ? e + 1000 : 0, e));

            var input = container.RegisterInput(data, DisorderPolicy.Drop());
            var diagnostic = input.GetDroppedAdjustedEventsDiagnostic();
            var diagnosticOutput = diagnostic.ForEachAsync(o => { if (o.Event.IsData) diagnosticList.Add(o.Event.Payload); });
            var query = CreateBasicQuery(input);
            var output = container.RegisterOutput(query);

            var outputAsync = output.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => { });
            var process = container.Restore(null);
            var bacon = process.QueryPlan;
            outputAsync.Wait();

            Assert.IsTrue(diagnosticList.SequenceEqual(Enumerable.Range(0, 10000).Where(e => e % 2 == 1)));
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpoint0Row()
        {
            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputListWithCheckpoint = new List<int>();
            var outputListWithoutCheckpoint = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var query1 = CreateBasicQuery(input1);
            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject);
            var query2 = CreateBasicQuery(input2);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort();

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var query3 = CreateBasicQuery(input3);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));
            container3.Restore(null);
            outputAsync3.Wait();

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointDisorderPolicyRow()
        {
            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputListWithCheckpoint = new List<int>();
            var outputListWithoutCheckpoint = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject, DisorderPolicy.Throw(10));
            var query1 = CreateBasicQuery(input1);
            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject, DisorderPolicy.Throw(10));
            var query2 = CreateBasicQuery(input2);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort();

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var query3 = CreateBasicQuery(input3);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));
            container3.Restore(null);
            outputAsync3.Wait();

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicMulticastCheckpoint0Row()
        {
            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputListWithCheckpoint = new List<int>();
            var outputListWithoutCheckpoint = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var array1 = input1.Multicast(2);
            var query1_left = array1[0].Where(e => e % 2 == 0);
            var query1right = array1[1].Where(e => e % 2 == 1);
            var query1 = query1_left.Union(query1right);
            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject);
            var array2 = input2.Multicast(2);
            var query2_left = array2[0].Where(e => e % 2 == 0);
            var query2right = array2[1].Where(e => e % 2 == 1);
            var query2 = query2_left.Union(query2right);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort();

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var array3 = input3.Multicast(2);
            var query3_left = array3[0].Where(e => e % 2 == 0);
            var query3right = array3[1].Where(e => e % 2 == 1);
            var query3 = query3_left.Union(query3right);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));
            container3.Restore(null);
            outputAsync3.Wait();
            outputListWithoutCheckpoint.Sort();

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointAnonTypeSelectManyRow()
        {
            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputListWithCheckpoint = new List<int>();
            var outputListWithoutCheckpoint = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var query1 = input1.Where(e => e % 2 == 1).AlterEventLifetime(e => e + 1, StreamEvent.InfinitySyncTime).Select(e => new { p = e / 2 }).SelectMany(e => Enumerable.Repeat(e, 2));

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o.p));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject);
            var query2 = input2.Where(e => e % 2 == 1).AlterEventLifetime(e => e + 1, StreamEvent.InfinitySyncTime).Select(e => new { p = e / 2 }).SelectMany(e => Enumerable.Repeat(e, 2));
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o.p));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort();

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var query3 = input3.Where(e => e % 2 == 1).AlterEventLifetime(e => e + 1, StreamEvent.InfinitySyncTime).Select(e => new { p = e / 2 }).SelectMany(e => Enumerable.Repeat(e, 2));
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o.p));
            container3.Restore(null);
            outputAsync3.Wait();

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointCountRow()
        {
            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputListWithCheckpoint = new List<ulong>();
            var outputListWithoutCheckpoint = new List<ulong>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var query1 = input1.Count();

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject);
            var query2 = input2.Count();
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort();

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var query3 = input3.Count();
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));
            container3.Restore(null);
            outputAsync3.Wait();

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointGroupedSumRow()
        {
            var preCheckpointSubject = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, ulong>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, ulong>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var query1 = input1.GroupApply(e => e.Item1, str => str.Sum(e => (ulong)e.Item2), (g, c) => new StructTuple<int, ulong> { Item1 = g.Key, Item2 = c });

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject);
            var query2 = input2.GroupApply(e => e.Item1, str => str.Sum(e => (ulong)e.Item2), (g, c) => new StructTuple<int, ulong> { Item1 = g.Key, Item2 = c });
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var query3 = input3.GroupApply(e => e.Item1, str => str.Sum(e => (ulong)e.Item2), (g, c) => new StructTuple<int, ulong> { Item1 = g.Key, Item2 = c });
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));
            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointEquiJoinRow()
        {
            var preCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var preCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, int>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, int>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData1 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var preCheckpointData2 = Enumerable.Range(0, 2).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var postCheckpointData1 = Enumerable.Range(10, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData2 = Enumerable.Range(2, 2).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var fullData1 = Enumerable.Range(0, 20).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData2 = Enumerable.Range(0, 4).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            // Query 1: Checkpoint
            var input11 = container1.RegisterInput(preCheckpointSubject1);
            var input12 = container1.RegisterInput(preCheckpointSubject2);
            var query1 = input11.Join(input12, e => e.Item1, e => e.Item1, (l, r) => new StructTuple<int, int> { Item1 = l.Item1, Item2 = r.Item2 });

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe1 = container1.Restore(null);
            preCheckpointData1.ForEachAsync(e => preCheckpointSubject1.OnNext(e)).Wait();
            preCheckpointData2.ForEachAsync(e => preCheckpointSubject2.OnNext(e)).Wait();

            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input21 = container2.RegisterInput(postCheckpointSubject1);
            var input22 = container2.RegisterInput(postCheckpointSubject2);
            var query2 = input21.Join(input22, e => e.Item1, e => e.Item1, (l, r) => new StructTuple<int, int> { Item1 = l.Item1, Item2 = r.Item2 });
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe2 = container2.Restore(state);
            postCheckpointData1.ForEachAsync(e => postCheckpointSubject1.OnNext(e)).Wait();
            postCheckpointData2.ForEachAsync(e => postCheckpointSubject2.OnNext(e)).Wait();
            postCheckpointSubject1.OnCompleted();
            postCheckpointSubject2.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input31 = container3.RegisterInput(fullData1);
            var input32 = container3.RegisterInput(fullData2);
            var query3 = input31.Join(input32, e => e.Item1, e => e.Item1, (l, r) => new StructTuple<int, int> { Item1 = l.Item1, Item2 = r.Item2 });
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));

            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject1.OnCompleted();
            preCheckpointSubject2.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointLASJRow()
        {
            var preCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var preCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, int>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, int>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData1 = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var preCheckpointData2 = Enumerable.Range(0, 5).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var postCheckpointData1 = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData2 = Enumerable.Range(5, 5).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var fullData1 = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData2 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            // Query 1: Checkpoint
            var input11 = container1.RegisterInput(preCheckpointSubject1);
            var input12 = container1.RegisterInput(preCheckpointSubject2);
            var query1 = input11.WhereNotExists(input12, e => e.Item1, e => e.Item1);

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe1 = container1.Restore(null);
            preCheckpointData1.ForEachAsync(e => preCheckpointSubject1.OnNext(e)).Wait();
            preCheckpointData2.ForEachAsync(e => preCheckpointSubject2.OnNext(e)).Wait();

            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input21 = container2.RegisterInput(postCheckpointSubject1);
            var input22 = container2.RegisterInput(postCheckpointSubject2);
            var query2 = input21.WhereNotExists(input22, e => e.Item1, e => e.Item1);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe2 = container2.Restore(state);
            postCheckpointData1.ForEachAsync(e => postCheckpointSubject1.OnNext(e)).Wait();
            postCheckpointData2.ForEachAsync(e => postCheckpointSubject2.OnNext(e)).Wait();
            postCheckpointSubject1.OnCompleted();
            postCheckpointSubject2.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input31 = container3.RegisterInput(fullData1);
            var input32 = container3.RegisterInput(fullData2);
            var query3 = input31.WhereNotExists(input32, e => e.Item1, e => e.Item1);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));

            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject1.OnCompleted();
            preCheckpointSubject2.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void LeftUnaryCheckpointLASJRow()
        {
            var preCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var preCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, int>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, int>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData1 = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var preCheckpointData2 = Enumerable.Range(0, 0).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var postCheckpointData1 = Enumerable.Range(20000, 0).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData2 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var fullData1 = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData2 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            // Query 1: Checkpoint
            var input11 = container1.RegisterInput(preCheckpointSubject1);
            var input12 = container1.RegisterInput(preCheckpointSubject2);
            var query1 = input11.WhereNotExists(input12, e => e.Item1, e => e.Item1);

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe1 = container1.Restore(null);
            preCheckpointData1.ForEachAsync(e => preCheckpointSubject1.OnNext(e)).Wait();
            preCheckpointData2.ForEachAsync(e => preCheckpointSubject2.OnNext(e)).Wait();

            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input21 = container2.RegisterInput(postCheckpointSubject1);
            var input22 = container2.RegisterInput(postCheckpointSubject2);
            var query2 = input21.WhereNotExists(input22, e => e.Item1, e => e.Item1);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe2 = container2.Restore(state);
            postCheckpointData1.ForEachAsync(e => postCheckpointSubject1.OnNext(e)).Wait();
            postCheckpointData2.ForEachAsync(e => postCheckpointSubject2.OnNext(e)).Wait();
            postCheckpointSubject1.OnCompleted();
            postCheckpointSubject2.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input31 = container3.RegisterInput(fullData1);
            var input32 = container3.RegisterInput(fullData2);
            var query3 = input31.WhereNotExists(input32, e => e.Item1, e => e.Item1);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));

            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject1.OnCompleted();
            preCheckpointSubject2.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void RightUnaryCheckpointLASJRow()
        {
            var preCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var preCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, int>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, int>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData1 = Enumerable.Range(0, 0).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var preCheckpointData2 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var postCheckpointData1 = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData2 = Enumerable.Range(10, 0).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var fullData1 = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData2 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            // Query 1: Checkpoint
            var input11 = container1.RegisterInput(preCheckpointSubject1);
            var input12 = container1.RegisterInput(preCheckpointSubject2);
            var query1 = input11.WhereNotExists(input12, e => e.Item1, e => e.Item1);

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe1 = container1.Restore(null);
            preCheckpointData1.ForEachAsync(e => preCheckpointSubject1.OnNext(e)).Wait();
            preCheckpointData2.ForEachAsync(e => preCheckpointSubject2.OnNext(e)).Wait();

            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input21 = container2.RegisterInput(postCheckpointSubject1);
            var input22 = container2.RegisterInput(postCheckpointSubject2);
            var query2 = input21.WhereNotExists(input22, e => e.Item1, e => e.Item1);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe2 = container2.Restore(state);
            postCheckpointData1.ForEachAsync(e => postCheckpointSubject1.OnNext(e)).Wait();
            postCheckpointData2.ForEachAsync(e => postCheckpointSubject2.OnNext(e)).Wait();
            postCheckpointSubject1.OnCompleted();
            postCheckpointSubject2.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input31 = container3.RegisterInput(fullData1);
            var input32 = container3.RegisterInput(fullData2);
            var query3 = input31.WhereNotExists(input32, e => e.Item1, e => e.Item1);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));

            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject1.OnCompleted();
            preCheckpointSubject2.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void SelfUnaryCheckpointLASJRow()
        {
            var preCheckpointSubject = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, int>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, int>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var query1 = input1.GroupApply(o => 1, s => s.Multicast(p => p.WhereNotExists(p.Where(i => false), e => e.Item1, e => e.Item1)));

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();

            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject);
            var query2 = input2.GroupApply(o => 1, s => s.Multicast(p => p.WhereNotExists(p.Where(i => false), e => e.Item1, e => e.Item1)));
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var query3 = input3.GroupApply(o => 1, s => s.Multicast(p => p.WhereNotExists(p.Where(i => false), e => e.Item1, e => e.Item1)));
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));

            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointClipRow()
        {
            var preCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var preCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, int>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, int>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData1 = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var preCheckpointData2 = Enumerable.Range(0, 5).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var postCheckpointData1 = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
               .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData2 = Enumerable.Range(5, 5).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var fullData1 = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData2 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            // Query 1: Checkpoint
            var input11 = container1.RegisterInput(preCheckpointSubject1);
            var input12 = container1.RegisterInput(preCheckpointSubject2);
            var query1 = input11.ClipEventDuration(input12, e => e.Item1, e => e.Item1);

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe1 = container1.Restore(null);
            preCheckpointData1.ForEachAsync(e => preCheckpointSubject1.OnNext(e)).Wait();
            preCheckpointData2.ForEachAsync(e => preCheckpointSubject2.OnNext(e)).Wait();

            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input21 = container2.RegisterInput(postCheckpointSubject1);
            var input22 = container2.RegisterInput(postCheckpointSubject2);
            var query2 = input21.ClipEventDuration(input22, e => e.Item1, e => e.Item1);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe2 = container2.Restore(state);
            postCheckpointData1.ForEachAsync(e => postCheckpointSubject1.OnNext(e)).Wait();
            postCheckpointData2.ForEachAsync(e => postCheckpointSubject2.OnNext(e)).Wait();
            postCheckpointSubject1.OnCompleted();
            postCheckpointSubject2.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input31 = container3.RegisterInput(fullData1);
            var input32 = container3.RegisterInput(fullData2);
            var query3 = input31.ClipEventDuration(input32, e => e.Item1, e => e.Item1);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));

            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject1.OnCompleted();
            preCheckpointSubject2.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicEmptyCheckpointErrorRow()
        {
            bool foundAppropriateError = false;

            var preCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputListWithCheckpoint = new List<int>();
            var outputListWithoutCheckpoint = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(1, e));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var array1 = input1.Multicast(2);
            var query1_left = array1[0].Where(e => e % 2 == 0).AlterEventLifetime(e => e + 1, StreamEvent.InfinitySyncTime);
            var query1right = array1[1].Where(e => e % 2 == 1);
            var query1 = query1_left.Union(query1right);
            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            try
            {
                container1.Restore(state);
            }
            catch (StreamProcessingException)
            {
                foundAppropriateError = true;
            }

            Assert.IsTrue(foundAppropriateError);
            container1.Restore();
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointErrorRow()
        {
            bool foundAppropriateError = false;

            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputList = new List<int>();

            try
            {
                // Inputs
                var container1 = new QueryContainer();
                var container2 = new QueryContainer();
                Stream state = new MemoryStream();

                // Input data
                var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                    .ToObservable()
                    .Select(e => StreamEvent.CreateStart(10000, e));
                var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                    .ToObservable()
                    .Select(e => StreamEvent.CreateStart(0, e));

                // Query 1: Checkpoint
                var input1 = container1.RegisterInput(preCheckpointSubject);
                var query1 = CreateBasicQuery(input1);
                var output1 = container1.RegisterOutput(query1);

                var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
                var pipe1 = container1.Restore(null);
                preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
                pipe1.Checkpoint(state);

                state.Seek(0, SeekOrigin.Begin);

                // Query 2: Restore
                var input2 = container2.RegisterInput(postCheckpointSubject);
                var query2 = CreateBasicQuery(input2);
                var output2 = container2.RegisterOutput(query2);

                var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
                var pipe2 = container2.Restore(state);
                postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
                outputAsync2.Wait();
            }
            catch (AggregateException e)
            {
                if (e.InnerExceptions.Where(x => x is IngressException).Any()) foundAppropriateError = true;
            }
            finally
            {
                postCheckpointSubject.OnCompleted();
            }

            Assert.IsTrue(foundAppropriateError);
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicDisorderPolicyErrorRow()
        {
            bool foundAppropriateError = false;

            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputList = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(10000, e));
            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var query1 = CreateBasicQuery(input1);
            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            try
            {
                // Query 2: Restore
                var input2 = container2.RegisterInput(postCheckpointSubject, DisorderPolicy.Adjust());
                var query2 = CreateBasicQuery(input2);
                var output2 = container2.RegisterOutput(query2);

                var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
                var pipe2 = container2.Restore(state);
                postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
                postCheckpointSubject.OnCompleted();
                outputAsync2.Wait();
            }
            catch (StreamProcessingException)
            {
                foundAppropriateError = true;
            }

            Assert.IsTrue(foundAppropriateError);
            container2.Restore();
            preCheckpointSubject.OnCompleted();
            postCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicDisorderAdjustPolicyRow()
        {
            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputList = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(10000, e));
            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject, DisorderPolicy.Adjust());
            var query1 = CreateBasicQuery(input1);
            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject, DisorderPolicy.Adjust());
            var query2 = CreateBasicQuery(input2);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();

            Assert.IsTrue(outputList.Count == 10000);
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void MaxBug0Row()
        {
            var data = Enumerable.Range(0, 100).Select((d, i) =>
                    i % 10 == 9
                    ? new[] { StreamEvent.CreateStart(i, new MachineCountTest { ActivityCount = (ulong)i, MachineId = "Machine__" + i.ToString() }), StreamEvent.CreatePunctuation<MachineCountTest>(i) }
                    : new[] { StreamEvent.CreateStart(i, new MachineCountTest { ActivityCount = (ulong)i, MachineId = "Machine__" + i.ToString() }) })
                .SelectMany(e => e)
                .Concat(new[] { StreamEvent.CreatePunctuation<MachineCountTest>(StreamEvent.InfinitySyncTime) })
                .ToArray();
            var splitIndex = data.Length / 2;
            var preCheckpointData = data.Take(splitIndex);
            var postCheckpointData = data.Skip(splitIndex);

            var result = new List<StreamEvent<MachineCountTest>>();
            var expected = new List<StreamEvent<MachineCountTest>>();
            IStreamable<Empty, MachineCountTest> query(IStreamable<Empty, MachineCountTest> input)
                => input.Max((x, y) => (x.ActivityCount == y.ActivityCount ? CompareMachineIds(y.MachineId, x.MachineId) : x.ActivityCount.CompareTo(y.ActivityCount)));

            var process = ProcessData(result, preCheckpointData, query, null);

            var checkpoint = new MemoryStream();
            process.Checkpoint(checkpoint);
            checkpoint.Position = 0;

            process = ProcessData(result, postCheckpointData, query, checkpoint);

            // Validation
            GetExpected(expected, data, query);
            Assert.IsTrue(expected.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void MaxBug1Row()
        {
            var data = Enumerable.Range(0, 100).Select((d, i) =>
                    i % 10 == 9
                    ? new[] { StreamEvent.CreateStart(i, new MachineCountTest { ActivityCount = (ulong)i, MachineId = "Machine__" + i.ToString() }), StreamEvent.CreatePunctuation<MachineCountTest>(i) }
                    : new[] { StreamEvent.CreateStart(i, new MachineCountTest { ActivityCount = (ulong)i, MachineId = "Machine__" + i.ToString() }) })
                .SelectMany(e => e)
                .Concat(new[] { StreamEvent.CreatePunctuation<MachineCountTest>(StreamEvent.InfinitySyncTime) })
                .ToArray();
            var splitIndex = data.Length / 2;
            var preCheckpointData = data.Take(splitIndex);
            var postCheckpointData = data.Skip(splitIndex);

            var result = new List<StreamEvent<MachineCountTest>>();
            var expected = new List<StreamEvent<MachineCountTest>>();
            IStreamable<Empty, MachineCountTest> query(IStreamable<Empty, MachineCountTest> input)
                => input.Aggregate(
                    o => o.Max(i => i, (x, y) => (x.ActivityCount == y.ActivityCount ? CompareMachineIds(y.MachineId, x.MachineId) : x.ActivityCount.CompareTo(y.ActivityCount))),
                    o => o.Min(i => i, (x, y) => (x.ActivityCount == y.ActivityCount ? CompareMachineIds(y.MachineId, x.MachineId) : x.ActivityCount.CompareTo(y.ActivityCount))),
                    (max, min) => max);

            var process = ProcessData(result, preCheckpointData, query, null);

            var checkpoint = new MemoryStream();
            process.Checkpoint(checkpoint);
            checkpoint.Position = 0;

            process = ProcessData(result, postCheckpointData, query, checkpoint);

            // Validation
            GetExpected(expected, data, query);
            Assert.IsTrue(expected.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void MaxBug2Row()
        {
            var data = Enumerable.Range(0, 100).Select((d, i) =>
                    i % 10 == 9
                    ? new[] { StreamEvent.CreateStart(i, new MachineCountTest { ActivityCount = (ulong)i, MachineId = "Machine__" + i.ToString() }), StreamEvent.CreatePunctuation<MachineCountTest>(i) }
                    : new[] { StreamEvent.CreateStart(i, new MachineCountTest { ActivityCount = (ulong)i, MachineId = "Machine__" + i.ToString() }) })
                .SelectMany(e => e)
                .Concat(new[] { StreamEvent.CreatePunctuation<MachineCountTest>(StreamEvent.InfinitySyncTime) })
                .ToArray();
            var splitIndex = data.Length / 2;
            var preCheckpointData = data.Take(splitIndex);
            var postCheckpointData = data.Skip(splitIndex);

            var result = new List<StreamEvent<MachineCountTest>>();
            var expected = new List<StreamEvent<MachineCountTest>>();
            IStreamable<Empty, MachineCountTest> query(IStreamable<Empty, MachineCountTest> input)
                => input.Aggregate(
                    o => o.Min(i => i, (x, y) => (x.ActivityCount == y.ActivityCount ? CompareMachineIds(y.MachineId, x.MachineId) : x.ActivityCount.CompareTo(y.ActivityCount))),
                    o => o.Max(i => i, (x, y) => (x.ActivityCount == y.ActivityCount ? CompareMachineIds(y.MachineId, x.MachineId) : x.ActivityCount.CompareTo(y.ActivityCount))),
                    (min, max) => max);

            var process = ProcessData(result, preCheckpointData, query, null);

            var checkpoint = new MemoryStream();
            process.Checkpoint(checkpoint);
            checkpoint.Position = 0;

            process = ProcessData(result, postCheckpointData, query, checkpoint);

            // Validation
            GetExpected(expected, data, query);
            Assert.IsTrue(expected.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CheckpointRegressionRow()
        {
            var data = Enumerable.Range(0, 100).Select((d, i) =>
                    i % 10 == 9
                    ? new[] { StreamEvent.CreatePoint(i, d), StreamEvent.CreatePunctuation<int>(i + 1) }
                    : new[] { StreamEvent.CreatePoint(i, d) })
                .SelectMany(e => e)
                .Concat(new[] { StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime) })
                .ToArray();
            var splitIndex = data.Length / 2;
            var preCheckpointData = data.Take(splitIndex);
            var postCheckpointData = data.Skip(splitIndex);

            var result = new List<StreamEvent<int>>();
            var expected = new List<StreamEvent<int>>();
            IStreamable<Empty, int> query(IStreamable<Empty, int> input)
                => input.AlterEventLifetime(vs => 100000, (vs, ve) => 1).Sum(e => e);

            var process = ProcessData(result, preCheckpointData, query, null);

            var checkpoint = new MemoryStream();
            process.Checkpoint(checkpoint);
            checkpoint.Position = 0;

            process = ProcessData(result, postCheckpointData, query, checkpoint);

            // Validation
            GetExpected(expected, data, query);
            Assert.IsTrue(expected.SequenceEqual(result));
        }

        private static int CompareMachineIds(string m1, string m2)
        {
            int suffix1 = int.Parse(m1.Substring(9));
            int suffix2 = int.Parse(m2.Substring(9));
            return suffix1.CompareTo(suffix2);
        }

        private static Microsoft.StreamProcessing.Process ProcessData<T, R>(
            List<StreamEvent<R>> result,
            IEnumerable<StreamEvent<T>> input,
            Func<IStreamable<Empty, T>, IStreamable<Empty, R>> query,
            Stream checkpoint = null)
        {
            var qc = new QueryContainer();
            var sin = new Subject<StreamEvent<T>>();
            var preDisp = qc.RegisterOutput(
                    query(
                        qc.RegisterInput(sin)))
                .Subscribe(result.Add);

            var process = qc.Restore(checkpoint);
            input.ToObservable().ForEachAsync(sin.OnNext).Wait();
            return process;
        }

        private static void GetExpected<T, R>(
            List<StreamEvent<R>> result,
            IEnumerable<StreamEvent<T>> input,
            Func<IStreamable<Empty, T>, IStreamable<Empty, R>> query)
        {
            var sin = new Subject<StreamEvent<T>>();
            var q = query(sin.ToStreamable())
                .ToStreamEventObservable().Subscribe(result.Add);
            input.ToObservable().ForEachAsync(sin.OnNext).Wait();
        }

    }

    [TestClass]
    public class CheckpointRestoreTestsAllowFallbackRowSmallBatch : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public CheckpointRestoreTestsAllowFallbackRowSmallBatch() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(false)
            .DataBatchSize(100)
            .MapArity(1)
            .ReduceArity(1))
        { }
        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointHoppingWindowSumRowSmallBatch()
        {
            for (int window = 1; window <= 10; window += 9)
            {
                for (int period = 1; period <= 20; period += 11)
                {
                    var preCheckpointSubject = new Subject<StreamEvent<int>>();
                    var postCheckpointSubject = new Subject<StreamEvent<int>>();

                    var outputListWithCheckpoint = new List<StreamEvent<ulong>>();
                    var outputListWithoutCheckpoint = new List<StreamEvent<ulong>>();

                    // Inputs
                    var container1 = new QueryContainer();
                    var container2 = new QueryContainer();
                    var container3 = new QueryContainer();
                    Stream state = new MemoryStream();

                    // Input data
                    var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                        .ToObservable()
                        .Select(e => StreamEvent.CreateStart(e, e));

                    var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                        .ToObservable()
                        .Select(e => StreamEvent.CreateStart(e, e));

                    var fullData = Enumerable.Range(0, 20000).ToList()
                        .ToObservable()
                        .Select(e => StreamEvent.CreateStart(e, e));

                    // Query 1: CheckpointBasicUnaryCheckpointEquiJoinRow
                    var input1 = container1.RegisterInput(preCheckpointSubject);
                    var query1 = input1.HoppingWindowLifetime(window, period).Sum(e => (ulong)e);

                    var output1 = container1.RegisterOutput(query1);

                    var outputAsync1 = output1.Where(e => e.IsData).ForEachAsync(o => outputListWithCheckpoint.Add(o));
                    var pipe1 = container1.Restore(null);
                    preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
                    pipe1.Checkpoint(state);

                    state.Seek(0, SeekOrigin.Begin);

                    // Query 2: Restore
                    var input2 = container2.RegisterInput(postCheckpointSubject);
                    var query2 = input2.HoppingWindowLifetime(window, period).Sum(e => (ulong)e);
                    var output2 = container2.RegisterOutput(query2);

                    var outputAsync2 = output2.Where(e => e.IsData).ForEachAsync(o => outputListWithCheckpoint.Add(o));

                    var pipe2 = container2.Restore(state);
                    postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
                    postCheckpointSubject.OnCompleted();
                    outputAsync2.Wait();
                    outputListWithCheckpoint.Sort((x, y) =>
                    {
                        var res = x.SyncTime.CompareTo(y.SyncTime);
                        if (res == 0)
                            res = x.OtherTime.CompareTo(y.OtherTime);
                        if (res == 0)
                            res = x.Payload.CompareTo(y.Payload);
                        return res;
                    });

                    // Query 3: Total
                    var input3 = container3.RegisterInput(fullData);
                    var query3 = input3.HoppingWindowLifetime(window, period).Sum(e => (ulong)e);
                    var output3 = container3.RegisterOutput(query3);

                    var outputAsync3 = output3.Where(e => e.IsData).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));
                    container3.Restore(null);
                    outputAsync3.Wait();
                    outputListWithoutCheckpoint.Sort((x, y) =>
                    {
                        var res = x.SyncTime.CompareTo(y.SyncTime);
                        if (res == 0)
                            res = x.OtherTime.CompareTo(y.OtherTime);
                        if (res == 0)
                            res = x.Payload.CompareTo(y.Payload);
                        return res;
                    });

                    Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
                }
            }
        }
    }

    [TestClass]
    public class CheckpointRestoreTestsRowSmallBatch : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public CheckpointRestoreTestsRowSmallBatch() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .MapArity(1)
            .ReduceArity(1))
        { }

        private static IStreamable<Empty, int> CreateBasicQuery(IStreamable<Empty, int> input)
            => input
                .Where(e => e % 2 == 1)
                .AlterEventLifetime(e => e + 1, StreamEvent.InfinitySyncTime)
                .Select(e => e / 2);

        [TestMethod, TestCategory("Gated")]
        public void BasicIngressDiagnosticRowSmallBatch()
        {
            var diagnosticList = new List<int>();

            // Inputs
            var container = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var data = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e % 2 == 0 ? e + 1000 : 0, e));

            var input = container.RegisterInput(data, DisorderPolicy.Drop());
            var diagnostic = input.GetDroppedAdjustedEventsDiagnostic();
            var diagnosticOutput = diagnostic.ForEachAsync(o => { if (o.Event.IsData) diagnosticList.Add(o.Event.Payload); });
            var query = CreateBasicQuery(input);
            var output = container.RegisterOutput(query);

            var outputAsync = output.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => { });
            var process = container.Restore(null);
            var bacon = process.QueryPlan;
            outputAsync.Wait();

            Assert.IsTrue(diagnosticList.SequenceEqual(Enumerable.Range(0, 10000).Where(e => e % 2 == 1)));
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpoint0RowSmallBatch()
        {
            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputListWithCheckpoint = new List<int>();
            var outputListWithoutCheckpoint = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var query1 = CreateBasicQuery(input1);
            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject);
            var query2 = CreateBasicQuery(input2);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort();

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var query3 = CreateBasicQuery(input3);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));
            container3.Restore(null);
            outputAsync3.Wait();

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointDisorderPolicyRowSmallBatch()
        {
            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputListWithCheckpoint = new List<int>();
            var outputListWithoutCheckpoint = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject, DisorderPolicy.Throw(10));
            var query1 = CreateBasicQuery(input1);
            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject, DisorderPolicy.Throw(10));
            var query2 = CreateBasicQuery(input2);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort();

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var query3 = CreateBasicQuery(input3);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));
            container3.Restore(null);
            outputAsync3.Wait();

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicMulticastCheckpoint0RowSmallBatch()
        {
            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputListWithCheckpoint = new List<int>();
            var outputListWithoutCheckpoint = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var array1 = input1.Multicast(2);
            var query1_left = array1[0].Where(e => e % 2 == 0);
            var query1right = array1[1].Where(e => e % 2 == 1);
            var query1 = query1_left.Union(query1right);
            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject);
            var array2 = input2.Multicast(2);
            var query2_left = array2[0].Where(e => e % 2 == 0);
            var query2right = array2[1].Where(e => e % 2 == 1);
            var query2 = query2_left.Union(query2right);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort();

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var array3 = input3.Multicast(2);
            var query3_left = array3[0].Where(e => e % 2 == 0);
            var query3right = array3[1].Where(e => e % 2 == 1);
            var query3 = query3_left.Union(query3right);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));
            container3.Restore(null);
            outputAsync3.Wait();
            outputListWithoutCheckpoint.Sort();

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointAnonTypeSelectManyRowSmallBatch()
        {
            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputListWithCheckpoint = new List<int>();
            var outputListWithoutCheckpoint = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var query1 = input1.Where(e => e % 2 == 1).AlterEventLifetime(e => e + 1, StreamEvent.InfinitySyncTime).Select(e => new { p = e / 2 }).SelectMany(e => Enumerable.Repeat(e, 2));

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o.p));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject);
            var query2 = input2.Where(e => e % 2 == 1).AlterEventLifetime(e => e + 1, StreamEvent.InfinitySyncTime).Select(e => new { p = e / 2 }).SelectMany(e => Enumerable.Repeat(e, 2));
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o.p));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort();

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var query3 = input3.Where(e => e % 2 == 1).AlterEventLifetime(e => e + 1, StreamEvent.InfinitySyncTime).Select(e => new { p = e / 2 }).SelectMany(e => Enumerable.Repeat(e, 2));
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o.p));
            container3.Restore(null);
            outputAsync3.Wait();

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointCountRowSmallBatch()
        {
            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputListWithCheckpoint = new List<ulong>();
            var outputListWithoutCheckpoint = new List<ulong>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var query1 = input1.Count();

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject);
            var query2 = input2.Count();
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort();

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var query3 = input3.Count();
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));
            container3.Restore(null);
            outputAsync3.Wait();

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointGroupedSumRowSmallBatch()
        {
            var preCheckpointSubject = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, ulong>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, ulong>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var query1 = input1.GroupApply(e => e.Item1, str => str.Sum(e => (ulong)e.Item2), (g, c) => new StructTuple<int, ulong> { Item1 = g.Key, Item2 = c });

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject);
            var query2 = input2.GroupApply(e => e.Item1, str => str.Sum(e => (ulong)e.Item2), (g, c) => new StructTuple<int, ulong> { Item1 = g.Key, Item2 = c });
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var query3 = input3.GroupApply(e => e.Item1, str => str.Sum(e => (ulong)e.Item2), (g, c) => new StructTuple<int, ulong> { Item1 = g.Key, Item2 = c });
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));
            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointEquiJoinRowSmallBatch()
        {
            var preCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var preCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, int>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, int>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData1 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var preCheckpointData2 = Enumerable.Range(0, 2).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var postCheckpointData1 = Enumerable.Range(10, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData2 = Enumerable.Range(2, 2).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var fullData1 = Enumerable.Range(0, 20).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData2 = Enumerable.Range(0, 4).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            // Query 1: Checkpoint
            var input11 = container1.RegisterInput(preCheckpointSubject1);
            var input12 = container1.RegisterInput(preCheckpointSubject2);
            var query1 = input11.Join(input12, e => e.Item1, e => e.Item1, (l, r) => new StructTuple<int, int> { Item1 = l.Item1, Item2 = r.Item2 });

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe1 = container1.Restore(null);
            preCheckpointData1.ForEachAsync(e => preCheckpointSubject1.OnNext(e)).Wait();
            preCheckpointData2.ForEachAsync(e => preCheckpointSubject2.OnNext(e)).Wait();

            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input21 = container2.RegisterInput(postCheckpointSubject1);
            var input22 = container2.RegisterInput(postCheckpointSubject2);
            var query2 = input21.Join(input22, e => e.Item1, e => e.Item1, (l, r) => new StructTuple<int, int> { Item1 = l.Item1, Item2 = r.Item2 });
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe2 = container2.Restore(state);
            postCheckpointData1.ForEachAsync(e => postCheckpointSubject1.OnNext(e)).Wait();
            postCheckpointData2.ForEachAsync(e => postCheckpointSubject2.OnNext(e)).Wait();
            postCheckpointSubject1.OnCompleted();
            postCheckpointSubject2.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input31 = container3.RegisterInput(fullData1);
            var input32 = container3.RegisterInput(fullData2);
            var query3 = input31.Join(input32, e => e.Item1, e => e.Item1, (l, r) => new StructTuple<int, int> { Item1 = l.Item1, Item2 = r.Item2 });
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));

            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject1.OnCompleted();
            preCheckpointSubject2.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointLASJRowSmallBatch()
        {
            var preCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var preCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, int>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, int>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData1 = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var preCheckpointData2 = Enumerable.Range(0, 5).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var postCheckpointData1 = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData2 = Enumerable.Range(5, 5).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var fullData1 = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData2 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            // Query 1: Checkpoint
            var input11 = container1.RegisterInput(preCheckpointSubject1);
            var input12 = container1.RegisterInput(preCheckpointSubject2);
            var query1 = input11.WhereNotExists(input12, e => e.Item1, e => e.Item1);

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe1 = container1.Restore(null);
            preCheckpointData1.ForEachAsync(e => preCheckpointSubject1.OnNext(e)).Wait();
            preCheckpointData2.ForEachAsync(e => preCheckpointSubject2.OnNext(e)).Wait();

            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input21 = container2.RegisterInput(postCheckpointSubject1);
            var input22 = container2.RegisterInput(postCheckpointSubject2);
            var query2 = input21.WhereNotExists(input22, e => e.Item1, e => e.Item1);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe2 = container2.Restore(state);
            postCheckpointData1.ForEachAsync(e => postCheckpointSubject1.OnNext(e)).Wait();
            postCheckpointData2.ForEachAsync(e => postCheckpointSubject2.OnNext(e)).Wait();
            postCheckpointSubject1.OnCompleted();
            postCheckpointSubject2.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input31 = container3.RegisterInput(fullData1);
            var input32 = container3.RegisterInput(fullData2);
            var query3 = input31.WhereNotExists(input32, e => e.Item1, e => e.Item1);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));

            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject1.OnCompleted();
            preCheckpointSubject2.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void LeftUnaryCheckpointLASJRowSmallBatch()
        {
            var preCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var preCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, int>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, int>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData1 = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var preCheckpointData2 = Enumerable.Range(0, 0).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var postCheckpointData1 = Enumerable.Range(20000, 0).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData2 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var fullData1 = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData2 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            // Query 1: Checkpoint
            var input11 = container1.RegisterInput(preCheckpointSubject1);
            var input12 = container1.RegisterInput(preCheckpointSubject2);
            var query1 = input11.WhereNotExists(input12, e => e.Item1, e => e.Item1);

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe1 = container1.Restore(null);
            preCheckpointData1.ForEachAsync(e => preCheckpointSubject1.OnNext(e)).Wait();
            preCheckpointData2.ForEachAsync(e => preCheckpointSubject2.OnNext(e)).Wait();

            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input21 = container2.RegisterInput(postCheckpointSubject1);
            var input22 = container2.RegisterInput(postCheckpointSubject2);
            var query2 = input21.WhereNotExists(input22, e => e.Item1, e => e.Item1);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe2 = container2.Restore(state);
            postCheckpointData1.ForEachAsync(e => postCheckpointSubject1.OnNext(e)).Wait();
            postCheckpointData2.ForEachAsync(e => postCheckpointSubject2.OnNext(e)).Wait();
            postCheckpointSubject1.OnCompleted();
            postCheckpointSubject2.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input31 = container3.RegisterInput(fullData1);
            var input32 = container3.RegisterInput(fullData2);
            var query3 = input31.WhereNotExists(input32, e => e.Item1, e => e.Item1);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));

            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject1.OnCompleted();
            preCheckpointSubject2.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void RightUnaryCheckpointLASJRowSmallBatch()
        {
            var preCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var preCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, int>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, int>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData1 = Enumerable.Range(0, 0).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var preCheckpointData2 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var postCheckpointData1 = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData2 = Enumerable.Range(10, 0).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var fullData1 = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData2 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            // Query 1: Checkpoint
            var input11 = container1.RegisterInput(preCheckpointSubject1);
            var input12 = container1.RegisterInput(preCheckpointSubject2);
            var query1 = input11.WhereNotExists(input12, e => e.Item1, e => e.Item1);

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe1 = container1.Restore(null);
            preCheckpointData1.ForEachAsync(e => preCheckpointSubject1.OnNext(e)).Wait();
            preCheckpointData2.ForEachAsync(e => preCheckpointSubject2.OnNext(e)).Wait();

            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input21 = container2.RegisterInput(postCheckpointSubject1);
            var input22 = container2.RegisterInput(postCheckpointSubject2);
            var query2 = input21.WhereNotExists(input22, e => e.Item1, e => e.Item1);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe2 = container2.Restore(state);
            postCheckpointData1.ForEachAsync(e => postCheckpointSubject1.OnNext(e)).Wait();
            postCheckpointData2.ForEachAsync(e => postCheckpointSubject2.OnNext(e)).Wait();
            postCheckpointSubject1.OnCompleted();
            postCheckpointSubject2.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input31 = container3.RegisterInput(fullData1);
            var input32 = container3.RegisterInput(fullData2);
            var query3 = input31.WhereNotExists(input32, e => e.Item1, e => e.Item1);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));

            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject1.OnCompleted();
            preCheckpointSubject2.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void SelfUnaryCheckpointLASJRowSmallBatch()
        {
            var preCheckpointSubject = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, int>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, int>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var query1 = input1.GroupApply(o => 1, s => s.Multicast(p => p.WhereNotExists(p.Where(i => false), e => e.Item1, e => e.Item1)));

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();

            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject);
            var query2 = input2.GroupApply(o => 1, s => s.Multicast(p => p.WhereNotExists(p.Where(i => false), e => e.Item1, e => e.Item1)));
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var query3 = input3.GroupApply(o => 1, s => s.Multicast(p => p.WhereNotExists(p.Where(i => false), e => e.Item1, e => e.Item1)));
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));

            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointClipRowSmallBatch()
        {
            var preCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var preCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, int>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, int>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData1 = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var preCheckpointData2 = Enumerable.Range(0, 5).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var postCheckpointData1 = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
               .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData2 = Enumerable.Range(5, 5).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var fullData1 = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData2 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            // Query 1: Checkpoint
            var input11 = container1.RegisterInput(preCheckpointSubject1);
            var input12 = container1.RegisterInput(preCheckpointSubject2);
            var query1 = input11.ClipEventDuration(input12, e => e.Item1, e => e.Item1);

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe1 = container1.Restore(null);
            preCheckpointData1.ForEachAsync(e => preCheckpointSubject1.OnNext(e)).Wait();
            preCheckpointData2.ForEachAsync(e => preCheckpointSubject2.OnNext(e)).Wait();

            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input21 = container2.RegisterInput(postCheckpointSubject1);
            var input22 = container2.RegisterInput(postCheckpointSubject2);
            var query2 = input21.ClipEventDuration(input22, e => e.Item1, e => e.Item1);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe2 = container2.Restore(state);
            postCheckpointData1.ForEachAsync(e => postCheckpointSubject1.OnNext(e)).Wait();
            postCheckpointData2.ForEachAsync(e => postCheckpointSubject2.OnNext(e)).Wait();
            postCheckpointSubject1.OnCompleted();
            postCheckpointSubject2.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input31 = container3.RegisterInput(fullData1);
            var input32 = container3.RegisterInput(fullData2);
            var query3 = input31.ClipEventDuration(input32, e => e.Item1, e => e.Item1);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));

            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject1.OnCompleted();
            preCheckpointSubject2.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicEmptyCheckpointErrorRowSmallBatch()
        {
            bool foundAppropriateError = false;

            var preCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputListWithCheckpoint = new List<int>();
            var outputListWithoutCheckpoint = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(1, e));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var array1 = input1.Multicast(2);
            var query1_left = array1[0].Where(e => e % 2 == 0).AlterEventLifetime(e => e + 1, StreamEvent.InfinitySyncTime);
            var query1right = array1[1].Where(e => e % 2 == 1);
            var query1 = query1_left.Union(query1right);
            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            try
            {
                container1.Restore(state);
            }
            catch (StreamProcessingException)
            {
                foundAppropriateError = true;
            }

            Assert.IsTrue(foundAppropriateError);
            container1.Restore();
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointErrorRowSmallBatch()
        {
            bool foundAppropriateError = false;

            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputList = new List<int>();

            try
            {
                // Inputs
                var container1 = new QueryContainer();
                var container2 = new QueryContainer();
                Stream state = new MemoryStream();

                // Input data
                var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                    .ToObservable()
                    .Select(e => StreamEvent.CreateStart(10000, e));
                var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                    .ToObservable()
                    .Select(e => StreamEvent.CreateStart(0, e));

                // Query 1: Checkpoint
                var input1 = container1.RegisterInput(preCheckpointSubject);
                var query1 = CreateBasicQuery(input1);
                var output1 = container1.RegisterOutput(query1);

                var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
                var pipe1 = container1.Restore(null);
                preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
                pipe1.Checkpoint(state);

                state.Seek(0, SeekOrigin.Begin);

                // Query 2: Restore
                var input2 = container2.RegisterInput(postCheckpointSubject);
                var query2 = CreateBasicQuery(input2);
                var output2 = container2.RegisterOutput(query2);

                var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
                var pipe2 = container2.Restore(state);
                postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
                outputAsync2.Wait();
            }
            catch (AggregateException e)
            {
                if (e.InnerExceptions.Where(x => x is IngressException).Any()) foundAppropriateError = true;
            }
            finally
            {
                postCheckpointSubject.OnCompleted();
            }

            Assert.IsTrue(foundAppropriateError);
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicDisorderPolicyErrorRowSmallBatch()
        {
            bool foundAppropriateError = false;

            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputList = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(10000, e));
            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var query1 = CreateBasicQuery(input1);
            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            try
            {
                // Query 2: Restore
                var input2 = container2.RegisterInput(postCheckpointSubject, DisorderPolicy.Adjust());
                var query2 = CreateBasicQuery(input2);
                var output2 = container2.RegisterOutput(query2);

                var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
                var pipe2 = container2.Restore(state);
                postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
                postCheckpointSubject.OnCompleted();
                outputAsync2.Wait();
            }
            catch (StreamProcessingException)
            {
                foundAppropriateError = true;
            }

            Assert.IsTrue(foundAppropriateError);
            container2.Restore();
            preCheckpointSubject.OnCompleted();
            postCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicDisorderAdjustPolicyRowSmallBatch()
        {
            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputList = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(10000, e));
            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject, DisorderPolicy.Adjust());
            var query1 = CreateBasicQuery(input1);
            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject, DisorderPolicy.Adjust());
            var query2 = CreateBasicQuery(input2);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();

            Assert.IsTrue(outputList.Count == 10000);
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void MaxBug0RowSmallBatch()
        {
            var data = Enumerable.Range(0, 100).Select((d, i) =>
                    i % 10 == 9
                    ? new[] { StreamEvent.CreateStart(i, new MachineCountTest { ActivityCount = (ulong)i, MachineId = "Machine__" + i.ToString() }), StreamEvent.CreatePunctuation<MachineCountTest>(i) }
                    : new[] { StreamEvent.CreateStart(i, new MachineCountTest { ActivityCount = (ulong)i, MachineId = "Machine__" + i.ToString() }) })
                .SelectMany(e => e)
                .Concat(new[] { StreamEvent.CreatePunctuation<MachineCountTest>(StreamEvent.InfinitySyncTime) })
                .ToArray();
            var splitIndex = data.Length / 2;
            var preCheckpointData = data.Take(splitIndex);
            var postCheckpointData = data.Skip(splitIndex);

            var result = new List<StreamEvent<MachineCountTest>>();
            var expected = new List<StreamEvent<MachineCountTest>>();
            IStreamable<Empty, MachineCountTest> query(IStreamable<Empty, MachineCountTest> input)
                => input.Max((x, y) => (x.ActivityCount == y.ActivityCount ? CompareMachineIds(y.MachineId, x.MachineId) : x.ActivityCount.CompareTo(y.ActivityCount)));

            var process = ProcessData(result, preCheckpointData, query, null);

            var checkpoint = new MemoryStream();
            process.Checkpoint(checkpoint);
            checkpoint.Position = 0;

            process = ProcessData(result, postCheckpointData, query, checkpoint);

            // Validation
            GetExpected(expected, data, query);
            Assert.IsTrue(expected.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void MaxBug1RowSmallBatch()
        {
            var data = Enumerable.Range(0, 100).Select((d, i) =>
                    i % 10 == 9
                    ? new[] { StreamEvent.CreateStart(i, new MachineCountTest { ActivityCount = (ulong)i, MachineId = "Machine__" + i.ToString() }), StreamEvent.CreatePunctuation<MachineCountTest>(i) }
                    : new[] { StreamEvent.CreateStart(i, new MachineCountTest { ActivityCount = (ulong)i, MachineId = "Machine__" + i.ToString() }) })
                .SelectMany(e => e)
                .Concat(new[] { StreamEvent.CreatePunctuation<MachineCountTest>(StreamEvent.InfinitySyncTime) })
                .ToArray();
            var splitIndex = data.Length / 2;
            var preCheckpointData = data.Take(splitIndex);
            var postCheckpointData = data.Skip(splitIndex);

            var result = new List<StreamEvent<MachineCountTest>>();
            var expected = new List<StreamEvent<MachineCountTest>>();
            IStreamable<Empty, MachineCountTest> query(IStreamable<Empty, MachineCountTest> input)
                => input.Aggregate(
                    o => o.Max(i => i, (x, y) => (x.ActivityCount == y.ActivityCount ? CompareMachineIds(y.MachineId, x.MachineId) : x.ActivityCount.CompareTo(y.ActivityCount))),
                    o => o.Min(i => i, (x, y) => (x.ActivityCount == y.ActivityCount ? CompareMachineIds(y.MachineId, x.MachineId) : x.ActivityCount.CompareTo(y.ActivityCount))),
                    (max, min) => max);

            var process = ProcessData(result, preCheckpointData, query, null);

            var checkpoint = new MemoryStream();
            process.Checkpoint(checkpoint);
            checkpoint.Position = 0;

            process = ProcessData(result, postCheckpointData, query, checkpoint);

            // Validation
            GetExpected(expected, data, query);
            Assert.IsTrue(expected.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void MaxBug2RowSmallBatch()
        {
            var data = Enumerable.Range(0, 100).Select((d, i) =>
                    i % 10 == 9
                    ? new[] { StreamEvent.CreateStart(i, new MachineCountTest { ActivityCount = (ulong)i, MachineId = "Machine__" + i.ToString() }), StreamEvent.CreatePunctuation<MachineCountTest>(i) }
                    : new[] { StreamEvent.CreateStart(i, new MachineCountTest { ActivityCount = (ulong)i, MachineId = "Machine__" + i.ToString() }) })
                .SelectMany(e => e)
                .Concat(new[] { StreamEvent.CreatePunctuation<MachineCountTest>(StreamEvent.InfinitySyncTime) })
                .ToArray();
            var splitIndex = data.Length / 2;
            var preCheckpointData = data.Take(splitIndex);
            var postCheckpointData = data.Skip(splitIndex);

            var result = new List<StreamEvent<MachineCountTest>>();
            var expected = new List<StreamEvent<MachineCountTest>>();
            IStreamable<Empty, MachineCountTest> query(IStreamable<Empty, MachineCountTest> input)
                => input.Aggregate(
                    o => o.Min(i => i, (x, y) => (x.ActivityCount == y.ActivityCount ? CompareMachineIds(y.MachineId, x.MachineId) : x.ActivityCount.CompareTo(y.ActivityCount))),
                    o => o.Max(i => i, (x, y) => (x.ActivityCount == y.ActivityCount ? CompareMachineIds(y.MachineId, x.MachineId) : x.ActivityCount.CompareTo(y.ActivityCount))),
                    (min, max) => max);

            var process = ProcessData(result, preCheckpointData, query, null);

            var checkpoint = new MemoryStream();
            process.Checkpoint(checkpoint);
            checkpoint.Position = 0;

            process = ProcessData(result, postCheckpointData, query, checkpoint);

            // Validation
            GetExpected(expected, data, query);
            Assert.IsTrue(expected.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CheckpointRegressionRowSmallBatch()
        {
            var data = Enumerable.Range(0, 100).Select((d, i) =>
                    i % 10 == 9
                    ? new[] { StreamEvent.CreatePoint(i, d), StreamEvent.CreatePunctuation<int>(i + 1) }
                    : new[] { StreamEvent.CreatePoint(i, d) })
                .SelectMany(e => e)
                .Concat(new[] { StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime) })
                .ToArray();
            var splitIndex = data.Length / 2;
            var preCheckpointData = data.Take(splitIndex);
            var postCheckpointData = data.Skip(splitIndex);

            var result = new List<StreamEvent<int>>();
            var expected = new List<StreamEvent<int>>();
            IStreamable<Empty, int> query(IStreamable<Empty, int> input)
                => input.AlterEventLifetime(vs => 100000, (vs, ve) => 1).Sum(e => e);

            var process = ProcessData(result, preCheckpointData, query, null);

            var checkpoint = new MemoryStream();
            process.Checkpoint(checkpoint);
            checkpoint.Position = 0;

            process = ProcessData(result, postCheckpointData, query, checkpoint);

            // Validation
            GetExpected(expected, data, query);
            Assert.IsTrue(expected.SequenceEqual(result));
        }

        private static int CompareMachineIds(string m1, string m2)
        {
            int suffix1 = int.Parse(m1.Substring(9));
            int suffix2 = int.Parse(m2.Substring(9));
            return suffix1.CompareTo(suffix2);
        }

        private static Microsoft.StreamProcessing.Process ProcessData<T, R>(
            List<StreamEvent<R>> result,
            IEnumerable<StreamEvent<T>> input,
            Func<IStreamable<Empty, T>, IStreamable<Empty, R>> query,
            Stream checkpoint = null)
        {
            var qc = new QueryContainer();
            var sin = new Subject<StreamEvent<T>>();
            var preDisp = qc.RegisterOutput(
                    query(
                        qc.RegisterInput(sin)))
                .Subscribe(result.Add);

            var process = qc.Restore(checkpoint);
            input.ToObservable().ForEachAsync(sin.OnNext).Wait();
            return process;
        }

        private static void GetExpected<T, R>(
            List<StreamEvent<R>> result,
            IEnumerable<StreamEvent<T>> input,
            Func<IStreamable<Empty, T>, IStreamable<Empty, R>> query)
        {
            var sin = new Subject<StreamEvent<T>>();
            var q = query(sin.ToStreamable())
                .ToStreamEventObservable().Subscribe(result.Add);
            input.ToObservable().ForEachAsync(sin.OnNext).Wait();
        }

    }

    [TestClass]
    public class CheckpointRestoreTestsAllowFallbackColumnar : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public CheckpointRestoreTestsAllowFallbackColumnar() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(false)
            .MapArity(1)
            .ReduceArity(1))
        { }
        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointHoppingWindowSumColumnar()
        {
            for (int window = 1; window <= 10; window += 9)
            {
                for (int period = 1; period <= 20; period += 11)
                {
                    var preCheckpointSubject = new Subject<StreamEvent<int>>();
                    var postCheckpointSubject = new Subject<StreamEvent<int>>();

                    var outputListWithCheckpoint = new List<StreamEvent<ulong>>();
                    var outputListWithoutCheckpoint = new List<StreamEvent<ulong>>();

                    // Inputs
                    var container1 = new QueryContainer();
                    var container2 = new QueryContainer();
                    var container3 = new QueryContainer();
                    Stream state = new MemoryStream();

                    // Input data
                    var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                        .ToObservable()
                        .Select(e => StreamEvent.CreateStart(e, e));

                    var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                        .ToObservable()
                        .Select(e => StreamEvent.CreateStart(e, e));

                    var fullData = Enumerable.Range(0, 20000).ToList()
                        .ToObservable()
                        .Select(e => StreamEvent.CreateStart(e, e));

                    // Query 1: CheckpointBasicUnaryCheckpointEquiJoinRow
                    var input1 = container1.RegisterInput(preCheckpointSubject);
                    var query1 = input1.HoppingWindowLifetime(window, period).Sum(e => (ulong)e);

                    var output1 = container1.RegisterOutput(query1);

                    var outputAsync1 = output1.Where(e => e.IsData).ForEachAsync(o => outputListWithCheckpoint.Add(o));
                    var pipe1 = container1.Restore(null);
                    preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
                    pipe1.Checkpoint(state);

                    state.Seek(0, SeekOrigin.Begin);

                    // Query 2: Restore
                    var input2 = container2.RegisterInput(postCheckpointSubject);
                    var query2 = input2.HoppingWindowLifetime(window, period).Sum(e => (ulong)e);
                    var output2 = container2.RegisterOutput(query2);

                    var outputAsync2 = output2.Where(e => e.IsData).ForEachAsync(o => outputListWithCheckpoint.Add(o));

                    var pipe2 = container2.Restore(state);
                    postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
                    postCheckpointSubject.OnCompleted();
                    outputAsync2.Wait();
                    outputListWithCheckpoint.Sort((x, y) =>
                    {
                        var res = x.SyncTime.CompareTo(y.SyncTime);
                        if (res == 0)
                            res = x.OtherTime.CompareTo(y.OtherTime);
                        if (res == 0)
                            res = x.Payload.CompareTo(y.Payload);
                        return res;
                    });

                    // Query 3: Total
                    var input3 = container3.RegisterInput(fullData);
                    var query3 = input3.HoppingWindowLifetime(window, period).Sum(e => (ulong)e);
                    var output3 = container3.RegisterOutput(query3);

                    var outputAsync3 = output3.Where(e => e.IsData).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));
                    container3.Restore(null);
                    outputAsync3.Wait();
                    outputListWithoutCheckpoint.Sort((x, y) =>
                    {
                        var res = x.SyncTime.CompareTo(y.SyncTime);
                        if (res == 0)
                            res = x.OtherTime.CompareTo(y.OtherTime);
                        if (res == 0)
                            res = x.Payload.CompareTo(y.Payload);
                        return res;
                    });

                    Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
                }
            }
        }
    }

    [TestClass]
    public class CheckpointRestoreTestsColumnar : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public CheckpointRestoreTestsColumnar() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .MapArity(1)
            .ReduceArity(1))
        { }

        private static IStreamable<Empty, int> CreateBasicQuery(IStreamable<Empty, int> input)
            => input
                .Where(e => e % 2 == 1)
                .AlterEventLifetime(e => e + 1, StreamEvent.InfinitySyncTime)
                .Select(e => e / 2);

        [TestMethod, TestCategory("Gated")]
        public void BasicIngressDiagnosticColumnar()
        {
            var diagnosticList = new List<int>();

            // Inputs
            var container = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var data = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e % 2 == 0 ? e + 1000 : 0, e));

            var input = container.RegisterInput(data, DisorderPolicy.Drop());
            var diagnostic = input.GetDroppedAdjustedEventsDiagnostic();
            var diagnosticOutput = diagnostic.ForEachAsync(o => { if (o.Event.IsData) diagnosticList.Add(o.Event.Payload); });
            var query = CreateBasicQuery(input);
            var output = container.RegisterOutput(query);

            var outputAsync = output.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => { });
            var process = container.Restore(null);
            var bacon = process.QueryPlan;
            outputAsync.Wait();

            Assert.IsTrue(diagnosticList.SequenceEqual(Enumerable.Range(0, 10000).Where(e => e % 2 == 1)));
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpoint0Columnar()
        {
            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputListWithCheckpoint = new List<int>();
            var outputListWithoutCheckpoint = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var query1 = CreateBasicQuery(input1);
            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject);
            var query2 = CreateBasicQuery(input2);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort();

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var query3 = CreateBasicQuery(input3);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));
            container3.Restore(null);
            outputAsync3.Wait();

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointDisorderPolicyColumnar()
        {
            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputListWithCheckpoint = new List<int>();
            var outputListWithoutCheckpoint = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject, DisorderPolicy.Throw(10));
            var query1 = CreateBasicQuery(input1);
            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject, DisorderPolicy.Throw(10));
            var query2 = CreateBasicQuery(input2);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort();

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var query3 = CreateBasicQuery(input3);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));
            container3.Restore(null);
            outputAsync3.Wait();

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicMulticastCheckpoint0Columnar()
        {
            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputListWithCheckpoint = new List<int>();
            var outputListWithoutCheckpoint = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var array1 = input1.Multicast(2);
            var query1_left = array1[0].Where(e => e % 2 == 0);
            var query1right = array1[1].Where(e => e % 2 == 1);
            var query1 = query1_left.Union(query1right);
            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject);
            var array2 = input2.Multicast(2);
            var query2_left = array2[0].Where(e => e % 2 == 0);
            var query2right = array2[1].Where(e => e % 2 == 1);
            var query2 = query2_left.Union(query2right);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort();

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var array3 = input3.Multicast(2);
            var query3_left = array3[0].Where(e => e % 2 == 0);
            var query3right = array3[1].Where(e => e % 2 == 1);
            var query3 = query3_left.Union(query3right);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));
            container3.Restore(null);
            outputAsync3.Wait();
            outputListWithoutCheckpoint.Sort();

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointAnonTypeSelectManyColumnar()
        {
            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputListWithCheckpoint = new List<int>();
            var outputListWithoutCheckpoint = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var query1 = input1.Where(e => e % 2 == 1).AlterEventLifetime(e => e + 1, StreamEvent.InfinitySyncTime).Select(e => new { p = e / 2 }).SelectMany(e => Enumerable.Repeat(e, 2));

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o.p));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject);
            var query2 = input2.Where(e => e % 2 == 1).AlterEventLifetime(e => e + 1, StreamEvent.InfinitySyncTime).Select(e => new { p = e / 2 }).SelectMany(e => Enumerable.Repeat(e, 2));
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o.p));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort();

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var query3 = input3.Where(e => e % 2 == 1).AlterEventLifetime(e => e + 1, StreamEvent.InfinitySyncTime).Select(e => new { p = e / 2 }).SelectMany(e => Enumerable.Repeat(e, 2));
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o.p));
            container3.Restore(null);
            outputAsync3.Wait();

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointCountColumnar()
        {
            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputListWithCheckpoint = new List<ulong>();
            var outputListWithoutCheckpoint = new List<ulong>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var query1 = input1.Count();

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject);
            var query2 = input2.Count();
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort();

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var query3 = input3.Count();
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));
            container3.Restore(null);
            outputAsync3.Wait();

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointGroupedSumColumnar()
        {
            var preCheckpointSubject = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, ulong>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, ulong>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var query1 = input1.GroupApply(e => e.Item1, str => str.Sum(e => (ulong)e.Item2), (g, c) => new StructTuple<int, ulong> { Item1 = g.Key, Item2 = c });

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject);
            var query2 = input2.GroupApply(e => e.Item1, str => str.Sum(e => (ulong)e.Item2), (g, c) => new StructTuple<int, ulong> { Item1 = g.Key, Item2 = c });
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var query3 = input3.GroupApply(e => e.Item1, str => str.Sum(e => (ulong)e.Item2), (g, c) => new StructTuple<int, ulong> { Item1 = g.Key, Item2 = c });
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));
            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointEquiJoinColumnar()
        {
            var preCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var preCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, int>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, int>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData1 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var preCheckpointData2 = Enumerable.Range(0, 2).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var postCheckpointData1 = Enumerable.Range(10, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData2 = Enumerable.Range(2, 2).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var fullData1 = Enumerable.Range(0, 20).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData2 = Enumerable.Range(0, 4).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            // Query 1: Checkpoint
            var input11 = container1.RegisterInput(preCheckpointSubject1);
            var input12 = container1.RegisterInput(preCheckpointSubject2);
            var query1 = input11.Join(input12, e => e.Item1, e => e.Item1, (l, r) => new StructTuple<int, int> { Item1 = l.Item1, Item2 = r.Item2 });

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe1 = container1.Restore(null);
            preCheckpointData1.ForEachAsync(e => preCheckpointSubject1.OnNext(e)).Wait();
            preCheckpointData2.ForEachAsync(e => preCheckpointSubject2.OnNext(e)).Wait();

            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input21 = container2.RegisterInput(postCheckpointSubject1);
            var input22 = container2.RegisterInput(postCheckpointSubject2);
            var query2 = input21.Join(input22, e => e.Item1, e => e.Item1, (l, r) => new StructTuple<int, int> { Item1 = l.Item1, Item2 = r.Item2 });
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe2 = container2.Restore(state);
            postCheckpointData1.ForEachAsync(e => postCheckpointSubject1.OnNext(e)).Wait();
            postCheckpointData2.ForEachAsync(e => postCheckpointSubject2.OnNext(e)).Wait();
            postCheckpointSubject1.OnCompleted();
            postCheckpointSubject2.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input31 = container3.RegisterInput(fullData1);
            var input32 = container3.RegisterInput(fullData2);
            var query3 = input31.Join(input32, e => e.Item1, e => e.Item1, (l, r) => new StructTuple<int, int> { Item1 = l.Item1, Item2 = r.Item2 });
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));

            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject1.OnCompleted();
            preCheckpointSubject2.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointLASJColumnar()
        {
            var preCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var preCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, int>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, int>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData1 = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var preCheckpointData2 = Enumerable.Range(0, 5).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var postCheckpointData1 = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData2 = Enumerable.Range(5, 5).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var fullData1 = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData2 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            // Query 1: Checkpoint
            var input11 = container1.RegisterInput(preCheckpointSubject1);
            var input12 = container1.RegisterInput(preCheckpointSubject2);
            var query1 = input11.WhereNotExists(input12, e => e.Item1, e => e.Item1);

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe1 = container1.Restore(null);
            preCheckpointData1.ForEachAsync(e => preCheckpointSubject1.OnNext(e)).Wait();
            preCheckpointData2.ForEachAsync(e => preCheckpointSubject2.OnNext(e)).Wait();

            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input21 = container2.RegisterInput(postCheckpointSubject1);
            var input22 = container2.RegisterInput(postCheckpointSubject2);
            var query2 = input21.WhereNotExists(input22, e => e.Item1, e => e.Item1);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe2 = container2.Restore(state);
            postCheckpointData1.ForEachAsync(e => postCheckpointSubject1.OnNext(e)).Wait();
            postCheckpointData2.ForEachAsync(e => postCheckpointSubject2.OnNext(e)).Wait();
            postCheckpointSubject1.OnCompleted();
            postCheckpointSubject2.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input31 = container3.RegisterInput(fullData1);
            var input32 = container3.RegisterInput(fullData2);
            var query3 = input31.WhereNotExists(input32, e => e.Item1, e => e.Item1);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));

            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject1.OnCompleted();
            preCheckpointSubject2.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void LeftUnaryCheckpointLASJColumnar()
        {
            var preCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var preCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, int>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, int>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData1 = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var preCheckpointData2 = Enumerable.Range(0, 0).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var postCheckpointData1 = Enumerable.Range(20000, 0).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData2 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var fullData1 = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData2 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            // Query 1: Checkpoint
            var input11 = container1.RegisterInput(preCheckpointSubject1);
            var input12 = container1.RegisterInput(preCheckpointSubject2);
            var query1 = input11.WhereNotExists(input12, e => e.Item1, e => e.Item1);

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe1 = container1.Restore(null);
            preCheckpointData1.ForEachAsync(e => preCheckpointSubject1.OnNext(e)).Wait();
            preCheckpointData2.ForEachAsync(e => preCheckpointSubject2.OnNext(e)).Wait();

            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input21 = container2.RegisterInput(postCheckpointSubject1);
            var input22 = container2.RegisterInput(postCheckpointSubject2);
            var query2 = input21.WhereNotExists(input22, e => e.Item1, e => e.Item1);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe2 = container2.Restore(state);
            postCheckpointData1.ForEachAsync(e => postCheckpointSubject1.OnNext(e)).Wait();
            postCheckpointData2.ForEachAsync(e => postCheckpointSubject2.OnNext(e)).Wait();
            postCheckpointSubject1.OnCompleted();
            postCheckpointSubject2.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input31 = container3.RegisterInput(fullData1);
            var input32 = container3.RegisterInput(fullData2);
            var query3 = input31.WhereNotExists(input32, e => e.Item1, e => e.Item1);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));

            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject1.OnCompleted();
            preCheckpointSubject2.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void RightUnaryCheckpointLASJColumnar()
        {
            var preCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var preCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, int>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, int>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData1 = Enumerable.Range(0, 0).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var preCheckpointData2 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var postCheckpointData1 = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData2 = Enumerable.Range(10, 0).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var fullData1 = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData2 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            // Query 1: Checkpoint
            var input11 = container1.RegisterInput(preCheckpointSubject1);
            var input12 = container1.RegisterInput(preCheckpointSubject2);
            var query1 = input11.WhereNotExists(input12, e => e.Item1, e => e.Item1);

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe1 = container1.Restore(null);
            preCheckpointData1.ForEachAsync(e => preCheckpointSubject1.OnNext(e)).Wait();
            preCheckpointData2.ForEachAsync(e => preCheckpointSubject2.OnNext(e)).Wait();

            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input21 = container2.RegisterInput(postCheckpointSubject1);
            var input22 = container2.RegisterInput(postCheckpointSubject2);
            var query2 = input21.WhereNotExists(input22, e => e.Item1, e => e.Item1);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe2 = container2.Restore(state);
            postCheckpointData1.ForEachAsync(e => postCheckpointSubject1.OnNext(e)).Wait();
            postCheckpointData2.ForEachAsync(e => postCheckpointSubject2.OnNext(e)).Wait();
            postCheckpointSubject1.OnCompleted();
            postCheckpointSubject2.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input31 = container3.RegisterInput(fullData1);
            var input32 = container3.RegisterInput(fullData2);
            var query3 = input31.WhereNotExists(input32, e => e.Item1, e => e.Item1);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));

            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject1.OnCompleted();
            preCheckpointSubject2.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void SelfUnaryCheckpointLASJColumnar()
        {
            var preCheckpointSubject = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, int>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, int>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var query1 = input1.GroupApply(o => 1, s => s.Multicast(p => p.WhereNotExists(p.Where(i => false), e => e.Item1, e => e.Item1)));

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();

            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject);
            var query2 = input2.GroupApply(o => 1, s => s.Multicast(p => p.WhereNotExists(p.Where(i => false), e => e.Item1, e => e.Item1)));
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var query3 = input3.GroupApply(o => 1, s => s.Multicast(p => p.WhereNotExists(p.Where(i => false), e => e.Item1, e => e.Item1)));
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));

            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointClipColumnar()
        {
            var preCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var preCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, int>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, int>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData1 = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var preCheckpointData2 = Enumerable.Range(0, 5).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var postCheckpointData1 = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
               .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData2 = Enumerable.Range(5, 5).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var fullData1 = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData2 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            // Query 1: Checkpoint
            var input11 = container1.RegisterInput(preCheckpointSubject1);
            var input12 = container1.RegisterInput(preCheckpointSubject2);
            var query1 = input11.ClipEventDuration(input12, e => e.Item1, e => e.Item1);

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe1 = container1.Restore(null);
            preCheckpointData1.ForEachAsync(e => preCheckpointSubject1.OnNext(e)).Wait();
            preCheckpointData2.ForEachAsync(e => preCheckpointSubject2.OnNext(e)).Wait();

            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input21 = container2.RegisterInput(postCheckpointSubject1);
            var input22 = container2.RegisterInput(postCheckpointSubject2);
            var query2 = input21.ClipEventDuration(input22, e => e.Item1, e => e.Item1);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe2 = container2.Restore(state);
            postCheckpointData1.ForEachAsync(e => postCheckpointSubject1.OnNext(e)).Wait();
            postCheckpointData2.ForEachAsync(e => postCheckpointSubject2.OnNext(e)).Wait();
            postCheckpointSubject1.OnCompleted();
            postCheckpointSubject2.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input31 = container3.RegisterInput(fullData1);
            var input32 = container3.RegisterInput(fullData2);
            var query3 = input31.ClipEventDuration(input32, e => e.Item1, e => e.Item1);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));

            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject1.OnCompleted();
            preCheckpointSubject2.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicEmptyCheckpointErrorColumnar()
        {
            bool foundAppropriateError = false;

            var preCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputListWithCheckpoint = new List<int>();
            var outputListWithoutCheckpoint = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(1, e));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var array1 = input1.Multicast(2);
            var query1_left = array1[0].Where(e => e % 2 == 0).AlterEventLifetime(e => e + 1, StreamEvent.InfinitySyncTime);
            var query1right = array1[1].Where(e => e % 2 == 1);
            var query1 = query1_left.Union(query1right);
            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            try
            {
                container1.Restore(state);
            }
            catch (StreamProcessingException)
            {
                foundAppropriateError = true;
            }

            Assert.IsTrue(foundAppropriateError);
            container1.Restore();
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointErrorColumnar()
        {
            bool foundAppropriateError = false;

            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputList = new List<int>();

            try
            {
                // Inputs
                var container1 = new QueryContainer();
                var container2 = new QueryContainer();
                Stream state = new MemoryStream();

                // Input data
                var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                    .ToObservable()
                    .Select(e => StreamEvent.CreateStart(10000, e));
                var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                    .ToObservable()
                    .Select(e => StreamEvent.CreateStart(0, e));

                // Query 1: Checkpoint
                var input1 = container1.RegisterInput(preCheckpointSubject);
                var query1 = CreateBasicQuery(input1);
                var output1 = container1.RegisterOutput(query1);

                var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
                var pipe1 = container1.Restore(null);
                preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
                pipe1.Checkpoint(state);

                state.Seek(0, SeekOrigin.Begin);

                // Query 2: Restore
                var input2 = container2.RegisterInput(postCheckpointSubject);
                var query2 = CreateBasicQuery(input2);
                var output2 = container2.RegisterOutput(query2);

                var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
                var pipe2 = container2.Restore(state);
                postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
                outputAsync2.Wait();
            }
            catch (AggregateException e)
            {
                if (e.InnerExceptions.Where(x => x is IngressException).Any()) foundAppropriateError = true;
            }
            finally
            {
                postCheckpointSubject.OnCompleted();
            }

            Assert.IsTrue(foundAppropriateError);
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicDisorderPolicyErrorColumnar()
        {
            bool foundAppropriateError = false;

            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputList = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(10000, e));
            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var query1 = CreateBasicQuery(input1);
            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            try
            {
                // Query 2: Restore
                var input2 = container2.RegisterInput(postCheckpointSubject, DisorderPolicy.Adjust());
                var query2 = CreateBasicQuery(input2);
                var output2 = container2.RegisterOutput(query2);

                var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
                var pipe2 = container2.Restore(state);
                postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
                postCheckpointSubject.OnCompleted();
                outputAsync2.Wait();
            }
            catch (StreamProcessingException)
            {
                foundAppropriateError = true;
            }

            Assert.IsTrue(foundAppropriateError);
            container2.Restore();
            preCheckpointSubject.OnCompleted();
            postCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicDisorderAdjustPolicyColumnar()
        {
            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputList = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(10000, e));
            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject, DisorderPolicy.Adjust());
            var query1 = CreateBasicQuery(input1);
            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject, DisorderPolicy.Adjust());
            var query2 = CreateBasicQuery(input2);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();

            Assert.IsTrue(outputList.Count == 10000);
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void MaxBug0Columnar()
        {
            var data = Enumerable.Range(0, 100).Select((d, i) =>
                    i % 10 == 9
                    ? new[] { StreamEvent.CreateStart(i, new MachineCountTest { ActivityCount = (ulong)i, MachineId = "Machine__" + i.ToString() }), StreamEvent.CreatePunctuation<MachineCountTest>(i) }
                    : new[] { StreamEvent.CreateStart(i, new MachineCountTest { ActivityCount = (ulong)i, MachineId = "Machine__" + i.ToString() }) })
                .SelectMany(e => e)
                .Concat(new[] { StreamEvent.CreatePunctuation<MachineCountTest>(StreamEvent.InfinitySyncTime) })
                .ToArray();
            var splitIndex = data.Length / 2;
            var preCheckpointData = data.Take(splitIndex);
            var postCheckpointData = data.Skip(splitIndex);

            var result = new List<StreamEvent<MachineCountTest>>();
            var expected = new List<StreamEvent<MachineCountTest>>();
            IStreamable<Empty, MachineCountTest> query(IStreamable<Empty, MachineCountTest> input)
                => input.Max((x, y) => (x.ActivityCount == y.ActivityCount ? CompareMachineIds(y.MachineId, x.MachineId) : x.ActivityCount.CompareTo(y.ActivityCount)));

            var process = ProcessData(result, preCheckpointData, query, null);

            var checkpoint = new MemoryStream();
            process.Checkpoint(checkpoint);
            checkpoint.Position = 0;

            process = ProcessData(result, postCheckpointData, query, checkpoint);

            // Validation
            GetExpected(expected, data, query);
            Assert.IsTrue(expected.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void MaxBug1Columnar()
        {
            var data = Enumerable.Range(0, 100).Select((d, i) =>
                    i % 10 == 9
                    ? new[] { StreamEvent.CreateStart(i, new MachineCountTest { ActivityCount = (ulong)i, MachineId = "Machine__" + i.ToString() }), StreamEvent.CreatePunctuation<MachineCountTest>(i) }
                    : new[] { StreamEvent.CreateStart(i, new MachineCountTest { ActivityCount = (ulong)i, MachineId = "Machine__" + i.ToString() }) })
                .SelectMany(e => e)
                .Concat(new[] { StreamEvent.CreatePunctuation<MachineCountTest>(StreamEvent.InfinitySyncTime) })
                .ToArray();
            var splitIndex = data.Length / 2;
            var preCheckpointData = data.Take(splitIndex);
            var postCheckpointData = data.Skip(splitIndex);

            var result = new List<StreamEvent<MachineCountTest>>();
            var expected = new List<StreamEvent<MachineCountTest>>();
            IStreamable<Empty, MachineCountTest> query(IStreamable<Empty, MachineCountTest> input)
                => input.Aggregate(
                    o => o.Max(i => i, (x, y) => (x.ActivityCount == y.ActivityCount ? CompareMachineIds(y.MachineId, x.MachineId) : x.ActivityCount.CompareTo(y.ActivityCount))),
                    o => o.Min(i => i, (x, y) => (x.ActivityCount == y.ActivityCount ? CompareMachineIds(y.MachineId, x.MachineId) : x.ActivityCount.CompareTo(y.ActivityCount))),
                    (max, min) => max);

            var process = ProcessData(result, preCheckpointData, query, null);

            var checkpoint = new MemoryStream();
            process.Checkpoint(checkpoint);
            checkpoint.Position = 0;

            process = ProcessData(result, postCheckpointData, query, checkpoint);

            // Validation
            GetExpected(expected, data, query);
            Assert.IsTrue(expected.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void MaxBug2Columnar()
        {
            var data = Enumerable.Range(0, 100).Select((d, i) =>
                    i % 10 == 9
                    ? new[] { StreamEvent.CreateStart(i, new MachineCountTest { ActivityCount = (ulong)i, MachineId = "Machine__" + i.ToString() }), StreamEvent.CreatePunctuation<MachineCountTest>(i) }
                    : new[] { StreamEvent.CreateStart(i, new MachineCountTest { ActivityCount = (ulong)i, MachineId = "Machine__" + i.ToString() }) })
                .SelectMany(e => e)
                .Concat(new[] { StreamEvent.CreatePunctuation<MachineCountTest>(StreamEvent.InfinitySyncTime) })
                .ToArray();
            var splitIndex = data.Length / 2;
            var preCheckpointData = data.Take(splitIndex);
            var postCheckpointData = data.Skip(splitIndex);

            var result = new List<StreamEvent<MachineCountTest>>();
            var expected = new List<StreamEvent<MachineCountTest>>();
            IStreamable<Empty, MachineCountTest> query(IStreamable<Empty, MachineCountTest> input)
                => input.Aggregate(
                    o => o.Min(i => i, (x, y) => (x.ActivityCount == y.ActivityCount ? CompareMachineIds(y.MachineId, x.MachineId) : x.ActivityCount.CompareTo(y.ActivityCount))),
                    o => o.Max(i => i, (x, y) => (x.ActivityCount == y.ActivityCount ? CompareMachineIds(y.MachineId, x.MachineId) : x.ActivityCount.CompareTo(y.ActivityCount))),
                    (min, max) => max);

            var process = ProcessData(result, preCheckpointData, query, null);

            var checkpoint = new MemoryStream();
            process.Checkpoint(checkpoint);
            checkpoint.Position = 0;

            process = ProcessData(result, postCheckpointData, query, checkpoint);

            // Validation
            GetExpected(expected, data, query);
            Assert.IsTrue(expected.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CheckpointRegressionColumnar()
        {
            var data = Enumerable.Range(0, 100).Select((d, i) =>
                    i % 10 == 9
                    ? new[] { StreamEvent.CreatePoint(i, d), StreamEvent.CreatePunctuation<int>(i + 1) }
                    : new[] { StreamEvent.CreatePoint(i, d) })
                .SelectMany(e => e)
                .Concat(new[] { StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime) })
                .ToArray();
            var splitIndex = data.Length / 2;
            var preCheckpointData = data.Take(splitIndex);
            var postCheckpointData = data.Skip(splitIndex);

            var result = new List<StreamEvent<int>>();
            var expected = new List<StreamEvent<int>>();
            IStreamable<Empty, int> query(IStreamable<Empty, int> input)
                => input.AlterEventLifetime(vs => 100000, (vs, ve) => 1).Sum(e => e);

            var process = ProcessData(result, preCheckpointData, query, null);

            var checkpoint = new MemoryStream();
            process.Checkpoint(checkpoint);
            checkpoint.Position = 0;

            process = ProcessData(result, postCheckpointData, query, checkpoint);

            // Validation
            GetExpected(expected, data, query);
            Assert.IsTrue(expected.SequenceEqual(result));
        }

        private static int CompareMachineIds(string m1, string m2)
        {
            int suffix1 = int.Parse(m1.Substring(9));
            int suffix2 = int.Parse(m2.Substring(9));
            return suffix1.CompareTo(suffix2);
        }

        private static Microsoft.StreamProcessing.Process ProcessData<T, R>(
            List<StreamEvent<R>> result,
            IEnumerable<StreamEvent<T>> input,
            Func<IStreamable<Empty, T>, IStreamable<Empty, R>> query,
            Stream checkpoint = null)
        {
            var qc = new QueryContainer();
            var sin = new Subject<StreamEvent<T>>();
            var preDisp = qc.RegisterOutput(
                    query(
                        qc.RegisterInput(sin)))
                .Subscribe(result.Add);

            var process = qc.Restore(checkpoint);
            input.ToObservable().ForEachAsync(sin.OnNext).Wait();
            return process;
        }

        private static void GetExpected<T, R>(
            List<StreamEvent<R>> result,
            IEnumerable<StreamEvent<T>> input,
            Func<IStreamable<Empty, T>, IStreamable<Empty, R>> query)
        {
            var sin = new Subject<StreamEvent<T>>();
            var q = query(sin.ToStreamable())
                .ToStreamEventObservable().Subscribe(result.Add);
            input.ToObservable().ForEachAsync(sin.OnNext).Wait();
        }

    }

    [TestClass]
    public class CheckpointRestoreTestsAllowFallbackColumnarSmallBatch : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public CheckpointRestoreTestsAllowFallbackColumnarSmallBatch() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(false)
            .DataBatchSize(100)
            .MapArity(1)
            .ReduceArity(1))
        { }
        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointHoppingWindowSumColumnarSmallBatch()
        {
            for (int window = 1; window <= 10; window += 9)
            {
                for (int period = 1; period <= 20; period += 11)
                {
                    var preCheckpointSubject = new Subject<StreamEvent<int>>();
                    var postCheckpointSubject = new Subject<StreamEvent<int>>();

                    var outputListWithCheckpoint = new List<StreamEvent<ulong>>();
                    var outputListWithoutCheckpoint = new List<StreamEvent<ulong>>();

                    // Inputs
                    var container1 = new QueryContainer();
                    var container2 = new QueryContainer();
                    var container3 = new QueryContainer();
                    Stream state = new MemoryStream();

                    // Input data
                    var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                        .ToObservable()
                        .Select(e => StreamEvent.CreateStart(e, e));

                    var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                        .ToObservable()
                        .Select(e => StreamEvent.CreateStart(e, e));

                    var fullData = Enumerable.Range(0, 20000).ToList()
                        .ToObservable()
                        .Select(e => StreamEvent.CreateStart(e, e));

                    // Query 1: CheckpointBasicUnaryCheckpointEquiJoinRow
                    var input1 = container1.RegisterInput(preCheckpointSubject);
                    var query1 = input1.HoppingWindowLifetime(window, period).Sum(e => (ulong)e);

                    var output1 = container1.RegisterOutput(query1);

                    var outputAsync1 = output1.Where(e => e.IsData).ForEachAsync(o => outputListWithCheckpoint.Add(o));
                    var pipe1 = container1.Restore(null);
                    preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
                    pipe1.Checkpoint(state);

                    state.Seek(0, SeekOrigin.Begin);

                    // Query 2: Restore
                    var input2 = container2.RegisterInput(postCheckpointSubject);
                    var query2 = input2.HoppingWindowLifetime(window, period).Sum(e => (ulong)e);
                    var output2 = container2.RegisterOutput(query2);

                    var outputAsync2 = output2.Where(e => e.IsData).ForEachAsync(o => outputListWithCheckpoint.Add(o));

                    var pipe2 = container2.Restore(state);
                    postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
                    postCheckpointSubject.OnCompleted();
                    outputAsync2.Wait();
                    outputListWithCheckpoint.Sort((x, y) =>
                    {
                        var res = x.SyncTime.CompareTo(y.SyncTime);
                        if (res == 0)
                            res = x.OtherTime.CompareTo(y.OtherTime);
                        if (res == 0)
                            res = x.Payload.CompareTo(y.Payload);
                        return res;
                    });

                    // Query 3: Total
                    var input3 = container3.RegisterInput(fullData);
                    var query3 = input3.HoppingWindowLifetime(window, period).Sum(e => (ulong)e);
                    var output3 = container3.RegisterOutput(query3);

                    var outputAsync3 = output3.Where(e => e.IsData).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));
                    container3.Restore(null);
                    outputAsync3.Wait();
                    outputListWithoutCheckpoint.Sort((x, y) =>
                    {
                        var res = x.SyncTime.CompareTo(y.SyncTime);
                        if (res == 0)
                            res = x.OtherTime.CompareTo(y.OtherTime);
                        if (res == 0)
                            res = x.Payload.CompareTo(y.Payload);
                        return res;
                    });

                    Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
                }
            }
        }
    }

    [TestClass]
    public class CheckpointRestoreTestsColumnarSmallBatch : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public CheckpointRestoreTestsColumnarSmallBatch() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .MapArity(1)
            .ReduceArity(1))
        { }

        private static IStreamable<Empty, int> CreateBasicQuery(IStreamable<Empty, int> input)
            => input
                .Where(e => e % 2 == 1)
                .AlterEventLifetime(e => e + 1, StreamEvent.InfinitySyncTime)
                .Select(e => e / 2);

        [TestMethod, TestCategory("Gated")]
        public void BasicIngressDiagnosticColumnarSmallBatch()
        {
            var diagnosticList = new List<int>();

            // Inputs
            var container = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var data = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e % 2 == 0 ? e + 1000 : 0, e));

            var input = container.RegisterInput(data, DisorderPolicy.Drop());
            var diagnostic = input.GetDroppedAdjustedEventsDiagnostic();
            var diagnosticOutput = diagnostic.ForEachAsync(o => { if (o.Event.IsData) diagnosticList.Add(o.Event.Payload); });
            var query = CreateBasicQuery(input);
            var output = container.RegisterOutput(query);

            var outputAsync = output.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => { });
            var process = container.Restore(null);
            var bacon = process.QueryPlan;
            outputAsync.Wait();

            Assert.IsTrue(diagnosticList.SequenceEqual(Enumerable.Range(0, 10000).Where(e => e % 2 == 1)));
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpoint0ColumnarSmallBatch()
        {
            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputListWithCheckpoint = new List<int>();
            var outputListWithoutCheckpoint = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var query1 = CreateBasicQuery(input1);
            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject);
            var query2 = CreateBasicQuery(input2);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort();

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var query3 = CreateBasicQuery(input3);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));
            container3.Restore(null);
            outputAsync3.Wait();

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointDisorderPolicyColumnarSmallBatch()
        {
            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputListWithCheckpoint = new List<int>();
            var outputListWithoutCheckpoint = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject, DisorderPolicy.Throw(10));
            var query1 = CreateBasicQuery(input1);
            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject, DisorderPolicy.Throw(10));
            var query2 = CreateBasicQuery(input2);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort();

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var query3 = CreateBasicQuery(input3);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));
            container3.Restore(null);
            outputAsync3.Wait();

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicMulticastCheckpoint0ColumnarSmallBatch()
        {
            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputListWithCheckpoint = new List<int>();
            var outputListWithoutCheckpoint = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var array1 = input1.Multicast(2);
            var query1_left = array1[0].Where(e => e % 2 == 0);
            var query1right = array1[1].Where(e => e % 2 == 1);
            var query1 = query1_left.Union(query1right);
            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject);
            var array2 = input2.Multicast(2);
            var query2_left = array2[0].Where(e => e % 2 == 0);
            var query2right = array2[1].Where(e => e % 2 == 1);
            var query2 = query2_left.Union(query2right);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort();

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var array3 = input3.Multicast(2);
            var query3_left = array3[0].Where(e => e % 2 == 0);
            var query3right = array3[1].Where(e => e % 2 == 1);
            var query3 = query3_left.Union(query3right);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));
            container3.Restore(null);
            outputAsync3.Wait();
            outputListWithoutCheckpoint.Sort();

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointAnonTypeSelectManyColumnarSmallBatch()
        {
            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputListWithCheckpoint = new List<int>();
            var outputListWithoutCheckpoint = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var query1 = input1.Where(e => e % 2 == 1).AlterEventLifetime(e => e + 1, StreamEvent.InfinitySyncTime).Select(e => new { p = e / 2 }).SelectMany(e => Enumerable.Repeat(e, 2));

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o.p));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject);
            var query2 = input2.Where(e => e % 2 == 1).AlterEventLifetime(e => e + 1, StreamEvent.InfinitySyncTime).Select(e => new { p = e / 2 }).SelectMany(e => Enumerable.Repeat(e, 2));
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o.p));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort();

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var query3 = input3.Where(e => e % 2 == 1).AlterEventLifetime(e => e + 1, StreamEvent.InfinitySyncTime).Select(e => new { p = e / 2 }).SelectMany(e => Enumerable.Repeat(e, 2));
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o.p));
            container3.Restore(null);
            outputAsync3.Wait();

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointCountColumnarSmallBatch()
        {
            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputListWithCheckpoint = new List<ulong>();
            var outputListWithoutCheckpoint = new List<ulong>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var query1 = input1.Count();

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject);
            var query2 = input2.Count();
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort();

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var query3 = input3.Count();
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));
            container3.Restore(null);
            outputAsync3.Wait();

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointGroupedSumColumnarSmallBatch()
        {
            var preCheckpointSubject = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, ulong>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, ulong>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var query1 = input1.GroupApply(e => e.Item1, str => str.Sum(e => (ulong)e.Item2), (g, c) => new StructTuple<int, ulong> { Item1 = g.Key, Item2 = c });

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject);
            var query2 = input2.GroupApply(e => e.Item1, str => str.Sum(e => (ulong)e.Item2), (g, c) => new StructTuple<int, ulong> { Item1 = g.Key, Item2 = c });
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var query3 = input3.GroupApply(e => e.Item1, str => str.Sum(e => (ulong)e.Item2), (g, c) => new StructTuple<int, ulong> { Item1 = g.Key, Item2 = c });
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));
            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointEquiJoinColumnarSmallBatch()
        {
            var preCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var preCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, int>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, int>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData1 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var preCheckpointData2 = Enumerable.Range(0, 2).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var postCheckpointData1 = Enumerable.Range(10, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData2 = Enumerable.Range(2, 2).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var fullData1 = Enumerable.Range(0, 20).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData2 = Enumerable.Range(0, 4).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            // Query 1: Checkpoint
            var input11 = container1.RegisterInput(preCheckpointSubject1);
            var input12 = container1.RegisterInput(preCheckpointSubject2);
            var query1 = input11.Join(input12, e => e.Item1, e => e.Item1, (l, r) => new StructTuple<int, int> { Item1 = l.Item1, Item2 = r.Item2 });

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe1 = container1.Restore(null);
            preCheckpointData1.ForEachAsync(e => preCheckpointSubject1.OnNext(e)).Wait();
            preCheckpointData2.ForEachAsync(e => preCheckpointSubject2.OnNext(e)).Wait();

            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input21 = container2.RegisterInput(postCheckpointSubject1);
            var input22 = container2.RegisterInput(postCheckpointSubject2);
            var query2 = input21.Join(input22, e => e.Item1, e => e.Item1, (l, r) => new StructTuple<int, int> { Item1 = l.Item1, Item2 = r.Item2 });
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe2 = container2.Restore(state);
            postCheckpointData1.ForEachAsync(e => postCheckpointSubject1.OnNext(e)).Wait();
            postCheckpointData2.ForEachAsync(e => postCheckpointSubject2.OnNext(e)).Wait();
            postCheckpointSubject1.OnCompleted();
            postCheckpointSubject2.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input31 = container3.RegisterInput(fullData1);
            var input32 = container3.RegisterInput(fullData2);
            var query3 = input31.Join(input32, e => e.Item1, e => e.Item1, (l, r) => new StructTuple<int, int> { Item1 = l.Item1, Item2 = r.Item2 });
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));

            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject1.OnCompleted();
            preCheckpointSubject2.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointLASJColumnarSmallBatch()
        {
            var preCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var preCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, int>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, int>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData1 = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var preCheckpointData2 = Enumerable.Range(0, 5).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var postCheckpointData1 = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData2 = Enumerable.Range(5, 5).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var fullData1 = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData2 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            // Query 1: Checkpoint
            var input11 = container1.RegisterInput(preCheckpointSubject1);
            var input12 = container1.RegisterInput(preCheckpointSubject2);
            var query1 = input11.WhereNotExists(input12, e => e.Item1, e => e.Item1);

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe1 = container1.Restore(null);
            preCheckpointData1.ForEachAsync(e => preCheckpointSubject1.OnNext(e)).Wait();
            preCheckpointData2.ForEachAsync(e => preCheckpointSubject2.OnNext(e)).Wait();

            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input21 = container2.RegisterInput(postCheckpointSubject1);
            var input22 = container2.RegisterInput(postCheckpointSubject2);
            var query2 = input21.WhereNotExists(input22, e => e.Item1, e => e.Item1);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe2 = container2.Restore(state);
            postCheckpointData1.ForEachAsync(e => postCheckpointSubject1.OnNext(e)).Wait();
            postCheckpointData2.ForEachAsync(e => postCheckpointSubject2.OnNext(e)).Wait();
            postCheckpointSubject1.OnCompleted();
            postCheckpointSubject2.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input31 = container3.RegisterInput(fullData1);
            var input32 = container3.RegisterInput(fullData2);
            var query3 = input31.WhereNotExists(input32, e => e.Item1, e => e.Item1);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));

            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject1.OnCompleted();
            preCheckpointSubject2.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void LeftUnaryCheckpointLASJColumnarSmallBatch()
        {
            var preCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var preCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, int>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, int>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData1 = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var preCheckpointData2 = Enumerable.Range(0, 0).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var postCheckpointData1 = Enumerable.Range(20000, 0).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData2 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var fullData1 = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData2 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            // Query 1: Checkpoint
            var input11 = container1.RegisterInput(preCheckpointSubject1);
            var input12 = container1.RegisterInput(preCheckpointSubject2);
            var query1 = input11.WhereNotExists(input12, e => e.Item1, e => e.Item1);

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe1 = container1.Restore(null);
            preCheckpointData1.ForEachAsync(e => preCheckpointSubject1.OnNext(e)).Wait();
            preCheckpointData2.ForEachAsync(e => preCheckpointSubject2.OnNext(e)).Wait();

            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input21 = container2.RegisterInput(postCheckpointSubject1);
            var input22 = container2.RegisterInput(postCheckpointSubject2);
            var query2 = input21.WhereNotExists(input22, e => e.Item1, e => e.Item1);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe2 = container2.Restore(state);
            postCheckpointData1.ForEachAsync(e => postCheckpointSubject1.OnNext(e)).Wait();
            postCheckpointData2.ForEachAsync(e => postCheckpointSubject2.OnNext(e)).Wait();
            postCheckpointSubject1.OnCompleted();
            postCheckpointSubject2.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input31 = container3.RegisterInput(fullData1);
            var input32 = container3.RegisterInput(fullData2);
            var query3 = input31.WhereNotExists(input32, e => e.Item1, e => e.Item1);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));

            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject1.OnCompleted();
            preCheckpointSubject2.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void RightUnaryCheckpointLASJColumnarSmallBatch()
        {
            var preCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var preCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, int>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, int>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData1 = Enumerable.Range(0, 0).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var preCheckpointData2 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var postCheckpointData1 = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData2 = Enumerable.Range(10, 0).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var fullData1 = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData2 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            // Query 1: Checkpoint
            var input11 = container1.RegisterInput(preCheckpointSubject1);
            var input12 = container1.RegisterInput(preCheckpointSubject2);
            var query1 = input11.WhereNotExists(input12, e => e.Item1, e => e.Item1);

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe1 = container1.Restore(null);
            preCheckpointData1.ForEachAsync(e => preCheckpointSubject1.OnNext(e)).Wait();
            preCheckpointData2.ForEachAsync(e => preCheckpointSubject2.OnNext(e)).Wait();

            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input21 = container2.RegisterInput(postCheckpointSubject1);
            var input22 = container2.RegisterInput(postCheckpointSubject2);
            var query2 = input21.WhereNotExists(input22, e => e.Item1, e => e.Item1);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe2 = container2.Restore(state);
            postCheckpointData1.ForEachAsync(e => postCheckpointSubject1.OnNext(e)).Wait();
            postCheckpointData2.ForEachAsync(e => postCheckpointSubject2.OnNext(e)).Wait();
            postCheckpointSubject1.OnCompleted();
            postCheckpointSubject2.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input31 = container3.RegisterInput(fullData1);
            var input32 = container3.RegisterInput(fullData2);
            var query3 = input31.WhereNotExists(input32, e => e.Item1, e => e.Item1);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));

            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject1.OnCompleted();
            preCheckpointSubject2.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void SelfUnaryCheckpointLASJColumnarSmallBatch()
        {
            var preCheckpointSubject = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, int>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, int>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var query1 = input1.GroupApply(o => 1, s => s.Multicast(p => p.WhereNotExists(p.Where(i => false), e => e.Item1, e => e.Item1)));

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();

            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject);
            var query2 = input2.GroupApply(o => 1, s => s.Multicast(p => p.WhereNotExists(p.Where(i => false), e => e.Item1, e => e.Item1)));
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input3 = container3.RegisterInput(fullData);
            var query3 = input3.GroupApply(o => 1, s => s.Multicast(p => p.WhereNotExists(p.Where(i => false), e => e.Item1, e => e.Item1)));
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));

            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointClipColumnarSmallBatch()
        {
            var preCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var preCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject1 = new Subject<StreamEvent<StructTuple<int, int>>>();
            var postCheckpointSubject2 = new Subject<StreamEvent<StructTuple<int, int>>>();

            var outputListWithCheckpoint = new List<StructTuple<int, int>>();
            var outputListWithoutCheckpoint = new List<StructTuple<int, int>>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            var container3 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData1 = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var preCheckpointData2 = Enumerable.Range(0, 5).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var postCheckpointData1 = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
               .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var postCheckpointData2 = Enumerable.Range(5, 5).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            var fullData1 = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(e, new StructTuple<int, int> { Item1 = e % 10, Item2 = e }));

            var fullData2 = Enumerable.Range(0, 10).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(5000, new StructTuple<int, int> { Item1 = e, Item2 = e + 31337 }));

            // Query 1: Checkpoint
            var input11 = container1.RegisterInput(preCheckpointSubject1);
            var input12 = container1.RegisterInput(preCheckpointSubject2);
            var query1 = input11.ClipEventDuration(input12, e => e.Item1, e => e.Item1);

            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe1 = container1.Restore(null);
            preCheckpointData1.ForEachAsync(e => preCheckpointSubject1.OnNext(e)).Wait();
            preCheckpointData2.ForEachAsync(e => preCheckpointSubject2.OnNext(e)).Wait();

            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input21 = container2.RegisterInput(postCheckpointSubject1);
            var input22 = container2.RegisterInput(postCheckpointSubject2);
            var query2 = input21.ClipEventDuration(input22, e => e.Item1, e => e.Item1);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            var pipe2 = container2.Restore(state);
            postCheckpointData1.ForEachAsync(e => postCheckpointSubject1.OnNext(e)).Wait();
            postCheckpointData2.ForEachAsync(e => postCheckpointSubject2.OnNext(e)).Wait();
            postCheckpointSubject1.OnCompleted();
            postCheckpointSubject2.OnCompleted();
            outputAsync2.Wait();
            outputListWithCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            // Query 3: Total
            var input31 = container3.RegisterInput(fullData1);
            var input32 = container3.RegisterInput(fullData2);
            var query3 = input31.ClipEventDuration(input32, e => e.Item1, e => e.Item1);
            var output3 = container3.RegisterOutput(query3);

            var outputAsync3 = output3.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithoutCheckpoint.Add(o));

            container3.Restore(null);
            outputAsync3.Wait();

            outputListWithoutCheckpoint.Sort((a, b) => a.Item1.CompareTo(b.Item1) == 0 ? a.Item2.CompareTo(b.Item2) : a.Item1.CompareTo(b.Item1));

            Assert.IsTrue(outputListWithCheckpoint.SequenceEqual(outputListWithoutCheckpoint));
            preCheckpointSubject1.OnCompleted();
            preCheckpointSubject2.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicEmptyCheckpointErrorColumnarSmallBatch()
        {
            bool foundAppropriateError = false;

            var preCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputListWithCheckpoint = new List<int>();
            var outputListWithoutCheckpoint = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(1, e));

            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var array1 = input1.Multicast(2);
            var query1_left = array1[0].Where(e => e % 2 == 0).AlterEventLifetime(e => e + 1, StreamEvent.InfinitySyncTime);
            var query1right = array1[1].Where(e => e % 2 == 1);
            var query1 = query1_left.Union(query1right);
            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputListWithCheckpoint.Add(o));

            try
            {
                container1.Restore(state);
            }
            catch (StreamProcessingException)
            {
                foundAppropriateError = true;
            }

            Assert.IsTrue(foundAppropriateError);
            container1.Restore();
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicUnaryCheckpointErrorColumnarSmallBatch()
        {
            bool foundAppropriateError = false;

            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputList = new List<int>();

            try
            {
                // Inputs
                var container1 = new QueryContainer();
                var container2 = new QueryContainer();
                Stream state = new MemoryStream();

                // Input data
                var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                    .ToObservable()
                    .Select(e => StreamEvent.CreateStart(10000, e));
                var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                    .ToObservable()
                    .Select(e => StreamEvent.CreateStart(0, e));

                // Query 1: Checkpoint
                var input1 = container1.RegisterInput(preCheckpointSubject);
                var query1 = CreateBasicQuery(input1);
                var output1 = container1.RegisterOutput(query1);

                var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
                var pipe1 = container1.Restore(null);
                preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
                pipe1.Checkpoint(state);

                state.Seek(0, SeekOrigin.Begin);

                // Query 2: Restore
                var input2 = container2.RegisterInput(postCheckpointSubject);
                var query2 = CreateBasicQuery(input2);
                var output2 = container2.RegisterOutput(query2);

                var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
                var pipe2 = container2.Restore(state);
                postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
                outputAsync2.Wait();
            }
            catch (AggregateException e)
            {
                if (e.InnerExceptions.Where(x => x is IngressException).Any()) foundAppropriateError = true;
            }
            finally
            {
                postCheckpointSubject.OnCompleted();
            }

            Assert.IsTrue(foundAppropriateError);
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicDisorderPolicyErrorColumnarSmallBatch()
        {
            bool foundAppropriateError = false;

            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputList = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(10000, e));
            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject);
            var query1 = CreateBasicQuery(input1);
            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            try
            {
                // Query 2: Restore
                var input2 = container2.RegisterInput(postCheckpointSubject, DisorderPolicy.Adjust());
                var query2 = CreateBasicQuery(input2);
                var output2 = container2.RegisterOutput(query2);

                var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
                var pipe2 = container2.Restore(state);
                postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
                postCheckpointSubject.OnCompleted();
                outputAsync2.Wait();
            }
            catch (StreamProcessingException)
            {
                foundAppropriateError = true;
            }

            Assert.IsTrue(foundAppropriateError);
            container2.Restore();
            preCheckpointSubject.OnCompleted();
            postCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void BasicDisorderAdjustPolicyColumnarSmallBatch()
        {
            var preCheckpointSubject = new Subject<StreamEvent<int>>();
            var postCheckpointSubject = new Subject<StreamEvent<int>>();

            var outputList = new List<int>();

            // Inputs
            var container1 = new QueryContainer();
            var container2 = new QueryContainer();
            Stream state = new MemoryStream();

            // Input data
            var preCheckpointData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(10000, e));
            var postCheckpointData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            // Query 1: Checkpoint
            var input1 = container1.RegisterInput(preCheckpointSubject, DisorderPolicy.Adjust());
            var query1 = CreateBasicQuery(input1);
            var output1 = container1.RegisterOutput(query1);

            var outputAsync1 = output1.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
            var pipe1 = container1.Restore(null);
            preCheckpointData.ForEachAsync(e => preCheckpointSubject.OnNext(e)).Wait();
            pipe1.Checkpoint(state);

            state.Seek(0, SeekOrigin.Begin);

            // Query 2: Restore
            var input2 = container2.RegisterInput(postCheckpointSubject, DisorderPolicy.Adjust());
            var query2 = CreateBasicQuery(input2);
            var output2 = container2.RegisterOutput(query2);

            var outputAsync2 = output2.Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(o => outputList.Add(o));
            var pipe2 = container2.Restore(state);
            postCheckpointData.ForEachAsync(e => postCheckpointSubject.OnNext(e)).Wait();
            postCheckpointSubject.OnCompleted();
            outputAsync2.Wait();

            Assert.IsTrue(outputList.Count == 10000);
            preCheckpointSubject.OnCompleted();
        }

        [TestMethod, TestCategory("Gated")]
        public void MaxBug0ColumnarSmallBatch()
        {
            var data = Enumerable.Range(0, 100).Select((d, i) =>
                    i % 10 == 9
                    ? new[] { StreamEvent.CreateStart(i, new MachineCountTest { ActivityCount = (ulong)i, MachineId = "Machine__" + i.ToString() }), StreamEvent.CreatePunctuation<MachineCountTest>(i) }
                    : new[] { StreamEvent.CreateStart(i, new MachineCountTest { ActivityCount = (ulong)i, MachineId = "Machine__" + i.ToString() }) })
                .SelectMany(e => e)
                .Concat(new[] { StreamEvent.CreatePunctuation<MachineCountTest>(StreamEvent.InfinitySyncTime) })
                .ToArray();
            var splitIndex = data.Length / 2;
            var preCheckpointData = data.Take(splitIndex);
            var postCheckpointData = data.Skip(splitIndex);

            var result = new List<StreamEvent<MachineCountTest>>();
            var expected = new List<StreamEvent<MachineCountTest>>();
            IStreamable<Empty, MachineCountTest> query(IStreamable<Empty, MachineCountTest> input)
                => input.Max((x, y) => (x.ActivityCount == y.ActivityCount ? CompareMachineIds(y.MachineId, x.MachineId) : x.ActivityCount.CompareTo(y.ActivityCount)));

            var process = ProcessData(result, preCheckpointData, query, null);

            var checkpoint = new MemoryStream();
            process.Checkpoint(checkpoint);
            checkpoint.Position = 0;

            process = ProcessData(result, postCheckpointData, query, checkpoint);

            // Validation
            GetExpected(expected, data, query);
            Assert.IsTrue(expected.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void MaxBug1ColumnarSmallBatch()
        {
            var data = Enumerable.Range(0, 100).Select((d, i) =>
                    i % 10 == 9
                    ? new[] { StreamEvent.CreateStart(i, new MachineCountTest { ActivityCount = (ulong)i, MachineId = "Machine__" + i.ToString() }), StreamEvent.CreatePunctuation<MachineCountTest>(i) }
                    : new[] { StreamEvent.CreateStart(i, new MachineCountTest { ActivityCount = (ulong)i, MachineId = "Machine__" + i.ToString() }) })
                .SelectMany(e => e)
                .Concat(new[] { StreamEvent.CreatePunctuation<MachineCountTest>(StreamEvent.InfinitySyncTime) })
                .ToArray();
            var splitIndex = data.Length / 2;
            var preCheckpointData = data.Take(splitIndex);
            var postCheckpointData = data.Skip(splitIndex);

            var result = new List<StreamEvent<MachineCountTest>>();
            var expected = new List<StreamEvent<MachineCountTest>>();
            IStreamable<Empty, MachineCountTest> query(IStreamable<Empty, MachineCountTest> input)
                => input.Aggregate(
                    o => o.Max(i => i, (x, y) => (x.ActivityCount == y.ActivityCount ? CompareMachineIds(y.MachineId, x.MachineId) : x.ActivityCount.CompareTo(y.ActivityCount))),
                    o => o.Min(i => i, (x, y) => (x.ActivityCount == y.ActivityCount ? CompareMachineIds(y.MachineId, x.MachineId) : x.ActivityCount.CompareTo(y.ActivityCount))),
                    (max, min) => max);

            var process = ProcessData(result, preCheckpointData, query, null);

            var checkpoint = new MemoryStream();
            process.Checkpoint(checkpoint);
            checkpoint.Position = 0;

            process = ProcessData(result, postCheckpointData, query, checkpoint);

            // Validation
            GetExpected(expected, data, query);
            Assert.IsTrue(expected.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void MaxBug2ColumnarSmallBatch()
        {
            var data = Enumerable.Range(0, 100).Select((d, i) =>
                    i % 10 == 9
                    ? new[] { StreamEvent.CreateStart(i, new MachineCountTest { ActivityCount = (ulong)i, MachineId = "Machine__" + i.ToString() }), StreamEvent.CreatePunctuation<MachineCountTest>(i) }
                    : new[] { StreamEvent.CreateStart(i, new MachineCountTest { ActivityCount = (ulong)i, MachineId = "Machine__" + i.ToString() }) })
                .SelectMany(e => e)
                .Concat(new[] { StreamEvent.CreatePunctuation<MachineCountTest>(StreamEvent.InfinitySyncTime) })
                .ToArray();
            var splitIndex = data.Length / 2;
            var preCheckpointData = data.Take(splitIndex);
            var postCheckpointData = data.Skip(splitIndex);

            var result = new List<StreamEvent<MachineCountTest>>();
            var expected = new List<StreamEvent<MachineCountTest>>();
            IStreamable<Empty, MachineCountTest> query(IStreamable<Empty, MachineCountTest> input)
                => input.Aggregate(
                    o => o.Min(i => i, (x, y) => (x.ActivityCount == y.ActivityCount ? CompareMachineIds(y.MachineId, x.MachineId) : x.ActivityCount.CompareTo(y.ActivityCount))),
                    o => o.Max(i => i, (x, y) => (x.ActivityCount == y.ActivityCount ? CompareMachineIds(y.MachineId, x.MachineId) : x.ActivityCount.CompareTo(y.ActivityCount))),
                    (min, max) => max);

            var process = ProcessData(result, preCheckpointData, query, null);

            var checkpoint = new MemoryStream();
            process.Checkpoint(checkpoint);
            checkpoint.Position = 0;

            process = ProcessData(result, postCheckpointData, query, checkpoint);

            // Validation
            GetExpected(expected, data, query);
            Assert.IsTrue(expected.SequenceEqual(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CheckpointRegressionColumnarSmallBatch()
        {
            var data = Enumerable.Range(0, 100).Select((d, i) =>
                    i % 10 == 9
                    ? new[] { StreamEvent.CreatePoint(i, d), StreamEvent.CreatePunctuation<int>(i + 1) }
                    : new[] { StreamEvent.CreatePoint(i, d) })
                .SelectMany(e => e)
                .Concat(new[] { StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime) })
                .ToArray();
            var splitIndex = data.Length / 2;
            var preCheckpointData = data.Take(splitIndex);
            var postCheckpointData = data.Skip(splitIndex);

            var result = new List<StreamEvent<int>>();
            var expected = new List<StreamEvent<int>>();
            IStreamable<Empty, int> query(IStreamable<Empty, int> input)
                => input.AlterEventLifetime(vs => 100000, (vs, ve) => 1).Sum(e => e);

            var process = ProcessData(result, preCheckpointData, query, null);

            var checkpoint = new MemoryStream();
            process.Checkpoint(checkpoint);
            checkpoint.Position = 0;

            process = ProcessData(result, postCheckpointData, query, checkpoint);

            // Validation
            GetExpected(expected, data, query);
            Assert.IsTrue(expected.SequenceEqual(result));
        }

        private static int CompareMachineIds(string m1, string m2)
        {
            int suffix1 = int.Parse(m1.Substring(9));
            int suffix2 = int.Parse(m2.Substring(9));
            return suffix1.CompareTo(suffix2);
        }

        private static Microsoft.StreamProcessing.Process ProcessData<T, R>(
            List<StreamEvent<R>> result,
            IEnumerable<StreamEvent<T>> input,
            Func<IStreamable<Empty, T>, IStreamable<Empty, R>> query,
            Stream checkpoint = null)
        {
            var qc = new QueryContainer();
            var sin = new Subject<StreamEvent<T>>();
            var preDisp = qc.RegisterOutput(
                    query(
                        qc.RegisterInput(sin)))
                .Subscribe(result.Add);

            var process = qc.Restore(checkpoint);
            input.ToObservable().ForEachAsync(sin.OnNext).Wait();
            return process;
        }

        private static void GetExpected<T, R>(
            List<StreamEvent<R>> result,
            IEnumerable<StreamEvent<T>> input,
            Func<IStreamable<Empty, T>, IStreamable<Empty, R>> query)
        {
            var sin = new Subject<StreamEvent<T>>();
            var q = query(sin.ToStreamable())
                .ToStreamEventObservable().Subscribe(result.Add);
            input.ToObservable().ForEachAsync(sin.OnNext).Wait();
        }

    }
}