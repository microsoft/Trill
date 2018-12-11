// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
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
            const int LeftSize = 1000000;
            const int GroupingSize = 10;
            const int RightSize = LeftSize * GroupingSize;
            const int NumTests = 20;
            var pool = new MemoryPool<Empty, int>();

            state.Action = "creating input data...";
            var input1 = CreateSequentialStreamable(LeftSize, pool);
            var input2 = CreateSequentialStreamable(RightSize, pool);

            for (int j = 1; j <= NumTests; j++)
            {
                state.Action = string.Format("running {0}/{1}...", j, NumTests);

                var timer = new Stopwatch();
                timer.Start();
                var output = input1.Join(input2, l => l, r => r / GroupingSize, (l, r) => l);
                int outputCount = 0;
                output.ToStreamMessageObservable().ForEachAsync(b => outputCount += b.Count).Wait();
                timer.Stop();

                state.AddResult(LeftSize + RightSize, outputCount, timer.Elapsed);
            }

            state.Action = "DONE";
        }

        private static IStreamable<Empty, int> CreateSequentialStreamable(int length, MemoryPool<Empty, int> pool)
        {
            // Construct event batches from input.
            var batches = new List<StreamMessage<Empty, int>>();
            var batch = StreamMessageManager.GetStreamMessage(pool);
            for (int i = 0; i < length; i++)
            {
                batch.Add(0, DateTimeOffset.MaxValue.UtcTicks, Empty.Default, i);
                if (batch.Count == Config.DataBatchSize)
                {
                    batches.Add(batch);
                    batch = StreamMessageManager.GetStreamMessage(pool);
                }
            }

            if (batch.Count > 0)
            {
                batches.Add(batch);
            }

            // Add last CTI of infinity.
            // TODO: inline this punctuation
            // batches.Add(new StreamMessage<Empty, int>(StreamMessageKind.Punctuation, DateTimeOffset.MaxValue.UtcTicks));

            // Convert to IStreamable.
            return batches.ToObservable().CreateStreamable();
        }
    }
}
