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
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    /* This testcase verifies fix for bug where PartitionedSessionWindowPipe is not restored (from checkpoint) properly causing an exception
     *
     * Cause of the Bug:
     *     The PartitionedSessionWindowPipe keeps multiple dictionary states.
     *     One of the dictionary does not have a [DataMember] attribute, Because the value type is a LinkedList which does not support serialization.
     *     On checkpoint and then Restore, this dictionary is re-created using other data members in UpdatePointers callback.
     *     During this, the scenario of empty LinkedList value is missed and not restored.
     *     When next data event appears for the partition, the partitionKey is indexed on the dictionary resulting in KeyNotFoundException
    */
    [TestClass]
    public class PartitionedStreamCheckpointTests : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        [TestMethod, TestCategory("Gated")]
        public void CheckpointPartitionedSessionWindow()
        {
            Config.DataBatchSize = 1;

            var data = new PartitionedStreamEvent<int, double>[]
            {
                PartitionedStreamEvent.CreatePoint(0, 5, 1.0),
                PartitionedStreamEvent.CreatePunctuation<int, double>(0, 8),
                PartitionedStreamEvent.CreatePunctuation<int, double>(0, 11),
                PartitionedStreamEvent.CreatePunctuation<int, double>(0, 14),
                PartitionedStreamEvent.CreatePunctuation<int, double>(0, 17),
                PartitionedStreamEvent.CreatePunctuation<int, double>(0, 21),
                PartitionedStreamEvent.CreatePoint(0, 24, 1.0),
                PartitionedStreamEvent.CreatePunctuation<int, double>(0, 100),
            };

            var expected = new PartitionedStreamEvent<int, double>[]
            {
                PartitionedStreamEvent.CreateStart(0, 5, 1.0),
                PartitionedStreamEvent.CreatePunctuation<int, double>(0, 8),
                PartitionedStreamEvent.CreateEnd(0, 9, 5, 1.0),
                PartitionedStreamEvent.CreatePunctuation<int, double>(0, 11),
                PartitionedStreamEvent.CreatePunctuation<int, double>(0, 14),
                PartitionedStreamEvent.CreatePunctuation<int, double>(0, 17),
                PartitionedStreamEvent.CreatePunctuation<int, double>(0, 21),
                PartitionedStreamEvent.CreateStart(0, 24, 1.0),
                PartitionedStreamEvent.CreateEnd(0, 28, 24, 1.0),
                PartitionedStreamEvent.CreatePunctuation<int, double>(0, 100),
            };

            // This index represents the point when the checkpoint restore needs to happen to trigger the bug.
            const int checkpointIndex = 6;

            var subject = new Subject<PartitionedStreamEvent<int, double>>();
            var output = new List<PartitionedStreamEvent<int, double>>();
            var process = CreateQueryContainerForPartitionedStream(subject, output);

            for (int i = 0; i < data.Length; i++)
            {
                if (i == checkpointIndex)
                {
                    using (var ms = new MemoryStream())
                    {
                        process.Checkpoint(ms);
                        ms.Seek(0, SeekOrigin.Begin);

                        subject = new Subject<PartitionedStreamEvent<int, double>>();
                        process = CreateQueryContainerForPartitionedStream(subject, output, ms);
                    }
                }

                subject.OnNext(data[i]);
            }

            Assert.IsTrue(expected.SequenceEqual(output));
        }

        private Process CreateQueryContainerForPartitionedStream(
            Subject<PartitionedStreamEvent<int, double>> subject,
            List<PartitionedStreamEvent<int, double>> output,
            Stream stream = null)
        {
            var qc = new QueryContainer();
            var input = qc.RegisterInput(subject);
            var streamableOutput = input.SessionTimeoutWindow(4, 5).Sum(o => o);
            var egress = qc.RegisterOutput(streamableOutput).ForEachAsync(o => output.Add(o));
            var process = qc.Restore(stream);

            return process;
        }
    }
}
