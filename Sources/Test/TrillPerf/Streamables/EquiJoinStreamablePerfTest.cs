// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Collections.Generic;
using System.Diagnostics;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Internal.Collections;

namespace PerformanceTesting.Streamables
{
    public class EquiJoinStreamablePerfTest
    {
        [PerfTest("EquiJoinStreamable")]
        public static void EquiJoinStreamableTest(IPerfTestState state)
        {
            const int LeftSize = 1_000_000;
            const int GroupingSize = 10;
            const int RightSize = LeftSize * GroupingSize;
            const int NumTests = 20;
            var pool = new MemoryPool<Empty, int>();

            state.Action = "creating input data...";
            var leftInputData = CreateInputData(LeftSize, pool);
            var rightInputData = CreateInputData(RightSize, pool);

            for (int j = 1; j <= NumTests; j++)
            {
                state.Action = string.Format("running {0}/{1}...", j, NumTests);

                // Clone input sources to new streamables for this iteration's query
                var leftInputStreamable = CloneInputToStreamable(leftInputData);
                var rightInputStreamable = CloneInputToStreamable(rightInputData);

                // Setup query
                var output = leftInputStreamable.Join(rightInputStreamable, l => l, r => r / GroupingSize, (l, r) => l);
                int outputCount = 0;

                // Process Data
                var timer = new Stopwatch();
                timer.Start();
                output.ToStreamMessageObservable().ForEachAsync(b => outputCount += b.Count).Wait();
                timer.Stop();

                state.AddResult(LeftSize + RightSize, outputCount, timer.Elapsed);
            }

            state.Action = "DONE";
        }

        private static List<StreamMessage<Empty, int>> CreateInputData(int length, MemoryPool<Empty, int> pool)
        {
            // Construct event batches from input.
            var batches = new List<StreamMessage<Empty, int>>();
            var batch = StreamMessageManager.GetStreamMessage(pool);
            batch.Allocate();
            for (int i = 0; i < length; i++)
            {
                batch.Add(0, StreamEvent.InfinitySyncTime, Empty.Default, i);
                if (batch.Count == Config.DataBatchSize)
                {
                    batches.Add(batch);
                    batch = StreamMessageManager.GetStreamMessage(pool);
                    batch.Allocate();
                }
            }

            // Add last CTI of infinity.
            batch.AddPunctuation(StreamEvent.InfinitySyncTime);
            batches.Add(batch);

            return batches;
        }

        private static IStreamable<Empty, int> CloneInputToStreamable(List<StreamMessage<Empty, int>> inputSequence)
        {
            // Clone each batch in the input sequence
            var batches = new List<StreamMessage<Empty, int>>();
            foreach (var inputBatch in inputSequence)
            {
                var batch = StreamMessageManager.GetStreamMessage(inputBatch.memPool);
                batch.CloneFrom(inputBatch);
                batches.Add(batch);
            }

            // Convert the batches to IStreamable.
            return batches.ToObservable().CreateStreamable();
        }
    }
}
