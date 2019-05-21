// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting.Flush
{
    public class FlushTestBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private const int IntervalLength = 5;
        private const int BatchSize = 10; // TODO: this will be identical for FlushPolicy.None and FlushOnBatchBoundary without some filter operator
        private const int PunctuationGenerationPeriod = 7;
        private const int IngressEventCount = 1000;

        internal FlushTestBase(
            DisorderPolicy disorderPolicy,
            FlushPolicy flushPolicy,
            PeriodicPunctuationPolicy punctuationPolicy,
            OnCompletedPolicy completedPolicy) : base(baseConfigModifier)
        {
            this.disorderPolicy = disorderPolicy;
            this.flushPolicy = flushPolicy;
            this.punctuationPolicy = punctuationPolicy;
            this.completedPolicy = completedPolicy;
        }

        [TestMethod, TestCategory("Gated")]
        public void FilterTest()
        {
            // Creates a basic filter for even-timestamped events, punctuations
            var inputSubject = new Subject<StreamEvent<int>>();
            var qc = new QueryContainer();
            var input = qc.RegisterInput(inputSubject, this.disorderPolicy, this.flushPolicy, this.punctuationPolicy, this.completedPolicy);
            IStreamable<Empty, int> query = input;

            // Add a no-op operator that isn't simply a batch-in-batch-out operator
            query = query.ClipEventDuration(IntervalLength);

            query = query.Where(this.FilterExpression);
            query = query.ClipEventDuration(IntervalLength);
            var filtered = qc.RegisterOutput(query).ForEachAsync(o => OnEgress(o));
            var process = qc.Restore();

            for (int i = 0; i < IngressEventCount; i++)
            {
                OnIngress(inputSubject, StreamEvent.CreateInterval(i, i + IntervalLength, i));
                if (i > 0 && i % PunctuationGenerationPeriod == 0)
                    OnIngress(inputSubject, StreamEvent.CreatePunctuation<int>(i));

                // Make sure we don't have any pending events we expected to be egressed at this point
                Assert.IsTrue(this.expectedOutput.Count == 0);
            }

            OnCompleted(inputSubject);
        }

        private readonly Expression<Func<int, bool>> FilterExpression = payload => payload % 2 == 0;

        private void OnIngress(Subject<StreamEvent<int>> inputSubject, StreamEvent<int> ingressEvent)
        {
            // Add expectations
            this.ingressCount++;

            if (!ingressEvent.IsData || ingressEvent.Payload % 2 == 0) this.expectedBatch.Enqueue(ingressEvent);

            switch (this.flushPolicy)
            {
                case FlushPolicy.FlushOnBatchBoundary:
                    // Will flush anytime ingress reaches batch boundary
                    if (this.ingressCount % BatchSize == 0) MoveExpectedBatchToOutput();
                    break;
                case FlushPolicy.FlushOnPunctuation:
                    // Will flush on any punctuation
                    if (ingressEvent.IsPunctuation) MoveExpectedBatchToOutput();
                    break;
                case FlushPolicy.None:
                    // Will only flush once stream events cascade from ingress->filter->egress
                    if (this.ingressCount % BatchSize == 0) MoveExpectedBatchToFilteredBatch();
                    break;
            }

            // Ingress
            inputSubject.OnNext(ingressEvent);
        }

        private void MoveExpectedBatchToOutput()
        {
            while (this.expectedBatch.Count > 0) this.expectedOutput.Enqueue(this.expectedBatch.Dequeue());
        }

        private void MoveExpectedBatchToFilteredBatch()
        {
            while (this.expectedBatch.Count > 0)
            {
                var streamEvent = this.expectedBatch.Dequeue();
                if (!streamEvent.IsData || streamEvent.Payload % 2 == 0)
                {
                    this.filteredBatch.Enqueue(streamEvent);
                    if (this.filteredBatch.Count % BatchSize == 0) MoveFilteredBatchBatchToOutput();
                }
            }
        }

        private void MoveFilteredBatchBatchToOutput()
        {
            while (this.filteredBatch.Count > 0) this.expectedOutput.Enqueue(this.filteredBatch.Dequeue());
        }

        private void OnEgress(StreamEvent<int> egressEvent)
        {
            Assert.IsTrue(this.expectedOutput.Count > 0);
            var expectedEvent = this.expectedOutput.Dequeue();

            Assert.IsTrue(expectedEvent.Equals(egressEvent));
            this.validatedOutput.Add(egressEvent);
        }

        private void OnCompleted(Subject<StreamEvent<int>> inputSubject)
        {
            // Add expectations
            if (this.completedPolicy != OnCompletedPolicy.None)
            {
                long time = this.completedPolicy == OnCompletedPolicy.EndOfStream ? StreamEvent.InfinitySyncTime : IngressEventCount - 1;
                this.expectedBatch.Enqueue(StreamEvent.CreatePunctuation<int>(time));
                MoveExpectedBatchToOutput();
            }

            // Ingress
            inputSubject.OnCompleted();
        }

        internal static readonly ConfigModifier baseConfigModifier = new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(
                Config.CodegenOptions.MultiStringFlags.Wrappers |
                Config.CodegenOptions.MultiStringFlags.VectorOperations)
            .DataBatchSize(BatchSize);

        private readonly DisorderPolicy disorderPolicy;
        private readonly FlushPolicy flushPolicy;
        private readonly PeriodicPunctuationPolicy punctuationPolicy;
        private readonly OnCompletedPolicy completedPolicy;

        private readonly Queue<StreamEvent<int>> expectedOutput = new Queue<StreamEvent<int>>();

        // Events expected to be batched at ingress but not egressed
        private readonly Queue<StreamEvent<int>> expectedBatch = new Queue<StreamEvent<int>>();

        // Events expected to be batched at the filtered operator but not egressed (only used for FlushPolicy.None)
        private readonly Queue<StreamEvent<int>> filteredBatch = new Queue<StreamEvent<int>>();
        private int ingressCount = 0;

        // Events that have already been validated (kept around for debugging purposes)
        private readonly List<StreamEvent<int>> validatedOutput = new List<StreamEvent<int>>();
    }
}