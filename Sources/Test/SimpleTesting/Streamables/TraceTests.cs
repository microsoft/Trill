// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting.Trace
{
    [TestClass]
    public class TraceTests : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public class TracePlanNodeVisitor : IPlanNodeVisitor
        {
            public TracePlanNodeVisitor()
            {
                this.TraceMetrics = new Dictionary<string, ITraceMetrics>();
            }

            public Dictionary<string, ITraceMetrics> TraceMetrics { get; }

            public void Visit(PlanNode node)
            {
                if (node is TracePlanNode traceNode && !this.TraceMetrics.ContainsKey(traceNode.TraceMetrics.TraceId))
                {
                    this.TraceMetrics.Add(traceNode.TraceMetrics.TraceId, traceNode.TraceMetrics);
                }
            }
        }

        [TestMethod]
        public void TracePassThrough_Row() => ExecuteRowBased(TracePassThrough);
        [TestMethod]
        public void TracePassThrough_Columnar() => ExecuteColumnar(TracePassThrough);
        [TestMethod]
        public void TracedQuery_Row() => ExecuteRowBased(TracedQuery);
        [TestMethod]
        public void TracedQuery_Columnar() => ExecuteColumnar(TracedQuery);

        private void TracePassThrough()
        {
            const string traceId = "PassThroughTraceId";
            var inputList = new[]
            {
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateEnd(2, 1, "A"),
                StreamEvent.CreateInterval(2, 3, "A"),
                StreamEvent.CreatePunctuation<string>(3),
                StreamEvent.CreateStart(3, "A"),
                StreamEvent.CreateEnd(4, 3, "A"),
                StreamEvent.CreateStart(4, "A"),
                StreamEvent.CreateEnd(5, 4, "A"),
                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime),
            };

            var container = new QueryContainer();
            var input = container.RegisterInput(inputList.ToList().ToObservable());
            var outputStream = input.Trace(traceId);

            var output = container.RegisterOutput(outputStream);
            var result = new List<StreamEvent<string>>();
            output.Subscribe(t => result.Add(t));

            var process = container.Restore();
            process.Flush();

            var traceNodeVisitor = new TracePlanNodeVisitor();
            foreach (var planNode in process.QueryPlan.Values)
            {
                planNode.Accept(traceNodeVisitor);
            }

            Assert.AreEqual(1, traceNodeVisitor.TraceMetrics.Count);
            var traceMetrics = traceNodeVisitor.TraceMetrics[traceId];
            Assert.AreEqual(traceId, traceMetrics.TraceId);
            VerifyMetrics(traceMetrics, eventCount: 4, maxSyncTime: 5, maxPunctuationTime: StreamEvent.InfinitySyncTime);

            Assert.IsTrue(result.IsEquivalentTo(inputList));
        }

        private void TracedQuery()
        {
            var inputSubject = new Subject<StreamEvent<int>>();
            var container = new QueryContainer();
            var input = container.RegisterInput(inputSubject);

            var query = input
                .Trace("IngressTrace")
                .Select(i => i.ToString())
                .Trace("SelectTrace")
                .Where(s => s.Length > 1)
                .Trace("WhereTrace");

            var output = container.RegisterOutput(query);
            var result = new List<StreamEvent<string>>();
            output.Subscribe(t => result.Add(t));

            var process = container.Restore();

            var traceNodeVisitor = new TracePlanNodeVisitor();
            foreach (var planNode in process.QueryPlan.Values)
            {
                planNode.Accept(traceNodeVisitor);
            }

            var ingressMetrics = traceNodeVisitor.TraceMetrics["IngressTrace"];
            var selectMetrics = traceNodeVisitor.TraceMetrics["SelectTrace"];
            var whereMetrics = traceNodeVisitor.TraceMetrics["WhereTrace"];

            VerifyMetrics(ingressMetrics, eventCount: 0, maxSyncTime: 0, maxPunctuationTime: 0);
            VerifyMetrics(selectMetrics, eventCount: 0, maxSyncTime: 0, maxPunctuationTime: 0);
            VerifyMetrics(whereMetrics, eventCount: 0, maxSyncTime: 0, maxPunctuationTime: 0);

            inputSubject.OnNext(StreamEvent.CreatePoint(100, 5));
            inputSubject.OnNext(StreamEvent.CreatePoint(101, 8));
            inputSubject.OnNext(StreamEvent.CreatePoint(102, 9));
            inputSubject.OnNext(StreamEvent.CreatePunctuation<int>(110));
            process.Flush();

            VerifyMetrics(ingressMetrics, eventCount: 3, maxSyncTime: 102, maxPunctuationTime: 110);
            VerifyMetrics(selectMetrics, eventCount: 3, maxSyncTime: 102, maxPunctuationTime: 110);
            VerifyMetrics(whereMetrics, eventCount: 0, maxSyncTime: 0, maxPunctuationTime: 110);

            inputSubject.OnNext(StreamEvent.CreatePoint(110, 15));
            inputSubject.OnNext(StreamEvent.CreatePoint(111, 18));
            inputSubject.OnNext(StreamEvent.CreatePoint(112, 19));
            process.Flush();

            VerifyMetrics(ingressMetrics, eventCount: 6, maxSyncTime: 112, maxPunctuationTime: 110);
            VerifyMetrics(selectMetrics, eventCount: 6, maxSyncTime: 112, maxPunctuationTime: 110);
            VerifyMetrics(whereMetrics, eventCount: 3, maxSyncTime: 112, maxPunctuationTime: 110);

            inputSubject.OnCompleted();

            VerifyMetrics(ingressMetrics, eventCount: 6, maxSyncTime: 112, maxPunctuationTime: StreamEvent.InfinitySyncTime);
            VerifyMetrics(selectMetrics, eventCount: 6, maxSyncTime: 112, maxPunctuationTime: StreamEvent.InfinitySyncTime);
            VerifyMetrics(whereMetrics, eventCount: 3, maxSyncTime: 112, maxPunctuationTime: StreamEvent.InfinitySyncTime);
        }

        private void VerifyMetrics(ITraceMetrics metrics, long eventCount, long maxSyncTime, long maxPunctuationTime)
        {
            Assert.AreEqual(eventCount, metrics.EventCount);
            Assert.AreEqual(maxSyncTime, metrics.MaxSyncTime);
            Assert.AreEqual(maxPunctuationTime, metrics.MaxPunctuationTime);
        }
        private void ExecuteRowBased(Action action)
        {
            using (var rowBased = new ConfigModifier().ForceRowBasedExecution(true).Modify())
            {
                action.Invoke();
            }
        }
        private void ExecuteColumnar(Action action)
        {
            using (var rowBased = new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(true).Modify())
            {
                action.Invoke();
            }
        }
    }
}