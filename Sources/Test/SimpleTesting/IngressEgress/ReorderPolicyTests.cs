// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class ReorderPolicyTestsRow : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public ReorderPolicyTestsRow() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)) { }

        /// <summary>
        /// Tests sorter with multiple partitions
        /// //Input:
        /// {0,1}
        /// {1,1}
        /// {0,3}
        /// {0,5}
        /// {1,3}
        /// {1,20}
        /// {0, 6}
        /// {0, 7}
        /// {0, 8}
        /// {1, 2}
        /// {0, 2}
        /// {1, 32}
        /// {0, 13}
        ///
        /// Expected Output:
        /// {1, 1}
        /// {1, 3}
        /// {1, 20}
        /// {0, 1}
        /// {0, 2}
        /// {0, 3}
        /// {0, 5}
        /// {0, 6}
        /// {0, 7}
        /// {0, 8} //{1, 2} shoudl be dropped.
        /// {0, 13}
        /// {1, 30}
        /// </summary>
        [TestMethod, TestCategory("Gated")]
        public void SorterDequeueUntillRow()
        {
            var oldSortingTechnique = Config.IngressSortingTechnique;
            Config.IngressSortingTechnique = SortingTechnique.ImpatienceSort;

            var input = new List<Tuple<int, int>>()
            {
                Tuple.Create(0, 1),
                Tuple.Create(1, 1),
                Tuple.Create(0, 3),
                Tuple.Create(0, 5),
                Tuple.Create(1, 3),
                Tuple.Create(1, 20),
                Tuple.Create(0, 6),
                Tuple.Create(0, 7),
                Tuple.Create(0, 8),
                Tuple.Create(1, 2),
                Tuple.Create(0, 2),
                Tuple.Create(1, 30),
                Tuple.Create(0, 13)
            };

            var expectedoutput = new List<Tuple<int, int>>()
            {
                Tuple.Create(1, 1),
                Tuple.Create(1, 3),
                Tuple.Create(1, 20),
                Tuple.Create(0, 1),
                Tuple.Create(0, 2),
                Tuple.Create(0, 3),
                Tuple.Create(0, 5),
                Tuple.Create(0, 6),
                Tuple.Create(0, 7),
                Tuple.Create(0, 8),
                Tuple.Create(0, 13),
                Tuple.Create(1, 30)
            };

            var prog = input.Select(x => PartitionedStreamEvent.CreateStart(x.Item1, x.Item2, x.Item2)).ToObservable()
                .ToStreamable(DisorderPolicy.Drop(10)).ToStreamEventObservable();
            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => o.IsData).ToList();
            var success = output.SequenceEqual(expectedoutput.Select(t => PartitionedStreamEvent.CreateStart(t.Item1, t.Item2, t.Item2)));
            Config.IngressSortingTechnique = oldSortingTechnique;

            Assert.IsTrue(success);
        }

        [TestMethod, TestCategory("Gated")]
        public void ReorderTest1Row()
        {
            var oldSortingTechnique = Config.IngressSortingTechnique;
            Config.IngressSortingTechnique = SortingTechnique.ImpatienceSort;

            var outputList = new List<StreamEvent<int>>();

            double disorderFraction = 0.5;
            int reorderLatency = 202;
            int disorderAmount = 200;
            var rand = new Random(2);
            var disorderedData =
                Enumerable.Range(disorderAmount, 500000).ToList()
                .Select(e => StreamEvent.CreateStart(rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e, 0))
                .ToList();

            var stream = disorderedData.ToObservable().ToStreamable(DisorderPolicy.Drop(reorderLatency));

            stream.ToStreamEventObservable().ForEachAsync(e => { if (e.IsData) outputList.Add(e); }).Wait();

            disorderedData.Sort((a, b) => a.SyncTime.CompareTo(b.SyncTime));

            Assert.IsTrue(disorderedData.SequenceEqual(outputList));

            Config.IngressSortingTechnique = oldSortingTechnique;
        }

        [TestMethod, TestCategory("Gated")]
        public void ReorderTest2Row()
        {
            var oldSortingTechnique = Config.IngressSortingTechnique;
            Config.IngressSortingTechnique = SortingTechnique.PriorityQueue;

            var outputList = new List<StreamEvent<int>>();

            double disorderFraction = 0.5;
            int reorderLatency = 202;
            int disorderAmount = 200;
            var rand = new Random(2);
            var disorderedData =
                Enumerable.Range(disorderAmount, 500000).ToList()
                .Select(e => StreamEvent.CreateStart(rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e, 0))
                .ToList();

            var stream = disorderedData.ToObservable().ToStreamable(DisorderPolicy.Drop(reorderLatency));

            stream.ToStreamEventObservable().ForEachAsync(e => { if (e.IsData) outputList.Add(e); }).Wait();

            disorderedData.Sort((a, b) => a.SyncTime.CompareTo(b.SyncTime));

            Assert.IsTrue(disorderedData.SequenceEqual(outputList));

            Config.IngressSortingTechnique = oldSortingTechnique;
        }
    }

    [TestClass]
    public class ReorderPolicyTestsRowSmallBatch : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public ReorderPolicyTestsRowSmallBatch() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DataBatchSize(100)
            .DontFallBackToRowBasedExecution(true)) { }

        /// <summary>
        /// Tests sorter with multiple partitions
        /// //Input:
        /// {0,1}
        /// {1,1}
        /// {0,3}
        /// {0,5}
        /// {1,3}
        /// {1,20}
        /// {0, 6}
        /// {0, 7}
        /// {0, 8}
        /// {1, 2}
        /// {0, 2}
        /// {1, 32}
        /// {0, 13}
        ///
        /// Expected Output:
        /// {1, 1}
        /// {1, 3}
        /// {1, 20}
        /// {0, 1}
        /// {0, 2}
        /// {0, 3}
        /// {0, 5}
        /// {0, 6}
        /// {0, 7}
        /// {0, 8} //{1, 2} shoudl be dropped.
        /// {0, 13}
        /// {1, 30}
        /// </summary>
        [TestMethod, TestCategory("Gated")]
        public void SorterDequeueUntillRowSmallBatch()
        {
            var oldSortingTechnique = Config.IngressSortingTechnique;
            Config.IngressSortingTechnique = SortingTechnique.ImpatienceSort;

            var input = new List<Tuple<int, int>>()
            {
                Tuple.Create(0, 1),
                Tuple.Create(1, 1),
                Tuple.Create(0, 3),
                Tuple.Create(0, 5),
                Tuple.Create(1, 3),
                Tuple.Create(1, 20),
                Tuple.Create(0, 6),
                Tuple.Create(0, 7),
                Tuple.Create(0, 8),
                Tuple.Create(1, 2),
                Tuple.Create(0, 2),
                Tuple.Create(1, 30),
                Tuple.Create(0, 13)
            };

            var expectedoutput = new List<Tuple<int, int>>()
            {
                Tuple.Create(1, 1),
                Tuple.Create(1, 3),
                Tuple.Create(1, 20),
                Tuple.Create(0, 1),
                Tuple.Create(0, 2),
                Tuple.Create(0, 3),
                Tuple.Create(0, 5),
                Tuple.Create(0, 6),
                Tuple.Create(0, 7),
                Tuple.Create(0, 8),
                Tuple.Create(0, 13),
                Tuple.Create(1, 30)
            };

            var prog = input.Select(x => PartitionedStreamEvent.CreateStart(x.Item1, x.Item2, x.Item2)).ToObservable()
                .ToStreamable(DisorderPolicy.Drop(10)).ToStreamEventObservable();
            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => o.IsData).ToList();
            var success = output.SequenceEqual(expectedoutput.Select(t => PartitionedStreamEvent.CreateStart(t.Item1, t.Item2, t.Item2)));
            Config.IngressSortingTechnique = oldSortingTechnique;

            Assert.IsTrue(success);
        }

        [TestMethod, TestCategory("Gated")]
        public void ReorderTest1RowSmallBatch()
        {
            var oldSortingTechnique = Config.IngressSortingTechnique;
            Config.IngressSortingTechnique = SortingTechnique.ImpatienceSort;

            var outputList = new List<StreamEvent<int>>();

            double disorderFraction = 0.5;
            int reorderLatency = 202;
            int disorderAmount = 200;
            var rand = new Random(2);
            var disorderedData =
                Enumerable.Range(disorderAmount, 500000).ToList()
                .Select(e => StreamEvent.CreateStart(rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e, 0))
                .ToList();

            var stream = disorderedData.ToObservable().ToStreamable(DisorderPolicy.Drop(reorderLatency));

            stream.ToStreamEventObservable().ForEachAsync(e => { if (e.IsData) outputList.Add(e); }).Wait();

            disorderedData.Sort((a, b) => a.SyncTime.CompareTo(b.SyncTime));

            Assert.IsTrue(disorderedData.SequenceEqual(outputList));

            Config.IngressSortingTechnique = oldSortingTechnique;
        }

        [TestMethod, TestCategory("Gated")]
        public void ReorderTest2RowSmallBatch()
        {
            var oldSortingTechnique = Config.IngressSortingTechnique;
            Config.IngressSortingTechnique = SortingTechnique.PriorityQueue;

            var outputList = new List<StreamEvent<int>>();

            double disorderFraction = 0.5;
            int reorderLatency = 202;
            int disorderAmount = 200;
            var rand = new Random(2);
            var disorderedData =
                Enumerable.Range(disorderAmount, 500000).ToList()
                .Select(e => StreamEvent.CreateStart(rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e, 0))
                .ToList();

            var stream = disorderedData.ToObservable().ToStreamable(DisorderPolicy.Drop(reorderLatency));

            stream.ToStreamEventObservable().ForEachAsync(e => { if (e.IsData) outputList.Add(e); }).Wait();

            disorderedData.Sort((a, b) => a.SyncTime.CompareTo(b.SyncTime));

            Assert.IsTrue(disorderedData.SequenceEqual(outputList));

            Config.IngressSortingTechnique = oldSortingTechnique;
        }
    }

    [TestClass]
    public class ReorderPolicyTestsColumnar : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public ReorderPolicyTestsColumnar() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)) { }

        /// <summary>
        /// Tests sorter with multiple partitions
        /// //Input:
        /// {0,1}
        /// {1,1}
        /// {0,3}
        /// {0,5}
        /// {1,3}
        /// {1,20}
        /// {0, 6}
        /// {0, 7}
        /// {0, 8}
        /// {1, 2}
        /// {0, 2}
        /// {1, 32}
        /// {0, 13}
        ///
        /// Expected Output:
        /// {1, 1}
        /// {1, 3}
        /// {1, 20}
        /// {0, 1}
        /// {0, 2}
        /// {0, 3}
        /// {0, 5}
        /// {0, 6}
        /// {0, 7}
        /// {0, 8} //{1, 2} shoudl be dropped.
        /// {0, 13}
        /// {1, 30}
        /// </summary>
        [TestMethod, TestCategory("Gated")]
        public void SorterDequeueUntillColumnar()
        {
            var oldSortingTechnique = Config.IngressSortingTechnique;
            Config.IngressSortingTechnique = SortingTechnique.ImpatienceSort;

            var input = new List<Tuple<int, int>>()
            {
                Tuple.Create(0, 1),
                Tuple.Create(1, 1),
                Tuple.Create(0, 3),
                Tuple.Create(0, 5),
                Tuple.Create(1, 3),
                Tuple.Create(1, 20),
                Tuple.Create(0, 6),
                Tuple.Create(0, 7),
                Tuple.Create(0, 8),
                Tuple.Create(1, 2),
                Tuple.Create(0, 2),
                Tuple.Create(1, 30),
                Tuple.Create(0, 13)
            };

            var expectedoutput = new List<Tuple<int, int>>()
            {
                Tuple.Create(1, 1),
                Tuple.Create(1, 3),
                Tuple.Create(1, 20),
                Tuple.Create(0, 1),
                Tuple.Create(0, 2),
                Tuple.Create(0, 3),
                Tuple.Create(0, 5),
                Tuple.Create(0, 6),
                Tuple.Create(0, 7),
                Tuple.Create(0, 8),
                Tuple.Create(0, 13),
                Tuple.Create(1, 30)
            };

            var prog = input.Select(x => PartitionedStreamEvent.CreateStart(x.Item1, x.Item2, x.Item2)).ToObservable()
                .ToStreamable(DisorderPolicy.Drop(10)).ToStreamEventObservable();
            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => o.IsData).ToList();
            var success = output.SequenceEqual(expectedoutput.Select(t => PartitionedStreamEvent.CreateStart(t.Item1, t.Item2, t.Item2)));
            Config.IngressSortingTechnique = oldSortingTechnique;

            Assert.IsTrue(success);
        }

        [TestMethod, TestCategory("Gated")]
        public void ReorderTest1Columnar()
        {
            var oldSortingTechnique = Config.IngressSortingTechnique;
            Config.IngressSortingTechnique = SortingTechnique.ImpatienceSort;

            var outputList = new List<StreamEvent<int>>();

            double disorderFraction = 0.5;
            int reorderLatency = 202;
            int disorderAmount = 200;
            var rand = new Random(2);
            var disorderedData =
                Enumerable.Range(disorderAmount, 500000).ToList()
                .Select(e => StreamEvent.CreateStart(rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e, 0))
                .ToList();

            var stream = disorderedData.ToObservable().ToStreamable(DisorderPolicy.Drop(reorderLatency));

            stream.ToStreamEventObservable().ForEachAsync(e => { if (e.IsData) outputList.Add(e); }).Wait();

            disorderedData.Sort((a, b) => a.SyncTime.CompareTo(b.SyncTime));

            Assert.IsTrue(disorderedData.SequenceEqual(outputList));

            Config.IngressSortingTechnique = oldSortingTechnique;
        }

        [TestMethod, TestCategory("Gated")]
        public void ReorderTest2Columnar()
        {
            var oldSortingTechnique = Config.IngressSortingTechnique;
            Config.IngressSortingTechnique = SortingTechnique.PriorityQueue;

            var outputList = new List<StreamEvent<int>>();

            double disorderFraction = 0.5;
            int reorderLatency = 202;
            int disorderAmount = 200;
            var rand = new Random(2);
            var disorderedData =
                Enumerable.Range(disorderAmount, 500000).ToList()
                .Select(e => StreamEvent.CreateStart(rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e, 0))
                .ToList();

            var stream = disorderedData.ToObservable().ToStreamable(DisorderPolicy.Drop(reorderLatency));

            stream.ToStreamEventObservable().ForEachAsync(e => { if (e.IsData) outputList.Add(e); }).Wait();

            disorderedData.Sort((a, b) => a.SyncTime.CompareTo(b.SyncTime));

            Assert.IsTrue(disorderedData.SequenceEqual(outputList));

            Config.IngressSortingTechnique = oldSortingTechnique;
        }
    }

    [TestClass]
    public class ReorderPolicyTestsColumnarSmallBatch : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public ReorderPolicyTestsColumnarSmallBatch() : base(
            new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DataBatchSize(100)
            .DontFallBackToRowBasedExecution(true)) { }

        /// <summary>
        /// Tests sorter with multiple partitions
        /// //Input:
        /// {0,1}
        /// {1,1}
        /// {0,3}
        /// {0,5}
        /// {1,3}
        /// {1,20}
        /// {0, 6}
        /// {0, 7}
        /// {0, 8}
        /// {1, 2}
        /// {0, 2}
        /// {1, 32}
        /// {0, 13}
        ///
        /// Expected Output:
        /// {1, 1}
        /// {1, 3}
        /// {1, 20}
        /// {0, 1}
        /// {0, 2}
        /// {0, 3}
        /// {0, 5}
        /// {0, 6}
        /// {0, 7}
        /// {0, 8} //{1, 2} shoudl be dropped.
        /// {0, 13}
        /// {1, 30}
        /// </summary>
        [TestMethod, TestCategory("Gated")]
        public void SorterDequeueUntillColumnarSmallBatch()
        {
            var oldSortingTechnique = Config.IngressSortingTechnique;
            Config.IngressSortingTechnique = SortingTechnique.ImpatienceSort;

            var input = new List<Tuple<int, int>>()
            {
                Tuple.Create(0, 1),
                Tuple.Create(1, 1),
                Tuple.Create(0, 3),
                Tuple.Create(0, 5),
                Tuple.Create(1, 3),
                Tuple.Create(1, 20),
                Tuple.Create(0, 6),
                Tuple.Create(0, 7),
                Tuple.Create(0, 8),
                Tuple.Create(1, 2),
                Tuple.Create(0, 2),
                Tuple.Create(1, 30),
                Tuple.Create(0, 13)
            };

            var expectedoutput = new List<Tuple<int, int>>()
            {
                Tuple.Create(1, 1),
                Tuple.Create(1, 3),
                Tuple.Create(1, 20),
                Tuple.Create(0, 1),
                Tuple.Create(0, 2),
                Tuple.Create(0, 3),
                Tuple.Create(0, 5),
                Tuple.Create(0, 6),
                Tuple.Create(0, 7),
                Tuple.Create(0, 8),
                Tuple.Create(0, 13),
                Tuple.Create(1, 30)
            };

            var prog = input.Select(x => PartitionedStreamEvent.CreateStart(x.Item1, x.Item2, x.Item2)).ToObservable()
                .ToStreamable(DisorderPolicy.Drop(10)).ToStreamEventObservable();
            var outevents = prog.ToEnumerable().ToList();
            var output = outevents.Where(o => o.IsData).ToList();
            var success = output.SequenceEqual(expectedoutput.Select(t => PartitionedStreamEvent.CreateStart(t.Item1, t.Item2, t.Item2)));
            Config.IngressSortingTechnique = oldSortingTechnique;

            Assert.IsTrue(success);
        }

        [TestMethod, TestCategory("Gated")]
        public void ReorderTest1ColumnarSmallBatch()
        {
            var oldSortingTechnique = Config.IngressSortingTechnique;
            Config.IngressSortingTechnique = SortingTechnique.ImpatienceSort;

            var outputList = new List<StreamEvent<int>>();

            double disorderFraction = 0.5;
            int reorderLatency = 202;
            int disorderAmount = 200;
            var rand = new Random(2);
            var disorderedData =
                Enumerable.Range(disorderAmount, 500000).ToList()
                .Select(e => StreamEvent.CreateStart(rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e, 0))
                .ToList();

            var stream = disorderedData.ToObservable().ToStreamable(DisorderPolicy.Drop(reorderLatency));

            stream.ToStreamEventObservable().ForEachAsync(e => { if (e.IsData) outputList.Add(e); }).Wait();

            disorderedData.Sort((a, b) => a.SyncTime.CompareTo(b.SyncTime));

            Assert.IsTrue(disorderedData.SequenceEqual(outputList));

            Config.IngressSortingTechnique = oldSortingTechnique;
        }

        [TestMethod, TestCategory("Gated")]
        public void ReorderTest2ColumnarSmallBatch()
        {
            var oldSortingTechnique = Config.IngressSortingTechnique;
            Config.IngressSortingTechnique = SortingTechnique.PriorityQueue;

            var outputList = new List<StreamEvent<int>>();

            double disorderFraction = 0.5;
            int reorderLatency = 202;
            int disorderAmount = 200;
            var rand = new Random(2);
            var disorderedData =
                Enumerable.Range(disorderAmount, 500000).ToList()
                .Select(e => StreamEvent.CreateStart(rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e, 0))
                .ToList();

            var stream = disorderedData.ToObservable().ToStreamable(DisorderPolicy.Drop(reorderLatency));

            stream.ToStreamEventObservable().ForEachAsync(e => { if (e.IsData) outputList.Add(e); }).Wait();

            disorderedData.Sort((a, b) => a.SyncTime.CompareTo(b.SyncTime));

            Assert.IsTrue(disorderedData.SequenceEqual(outputList));

            Config.IngressSortingTechnique = oldSortingTechnique;
        }
    }

}
