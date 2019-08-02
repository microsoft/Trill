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

namespace SimpleTesting.StatefulSelect
{
    [TestClass]
    public class StatefulSelect : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private class IsFirstState
        {
            private readonly long duration;
            private long nextEmissionIntervalStart = -1;

            public IsFirstState(long duration) => this.duration = duration;

            public bool IsFirst(long timestamp)
            {
                if (timestamp <= this.nextEmissionIntervalStart)
                {
                    return false;
                }

                this.nextEmissionIntervalStart = timestamp == 0
                    ? 0
                    : ((timestamp - 1) / this.duration) * this.duration + this.duration; // Subtract 1 for right boundary inclusion
                return true;
            }
        }

        [TestMethod]
        public void IsFirst()
        {
            var inputSubject = new Subject<StreamEvent<int>>();
            var container = new QueryContainer();
            var input = container.RegisterInput(inputSubject);

            const long stateTimeout = 10;
            var query = input
                .StatefulSelect(
                    statefulSelect: (time, state, value) => state.IsFirst(time),
                    initialState: () => new IsFirstState(stateTimeout),
                    stateTimeout: stateTimeout);

            var output = container.RegisterOutput(query);
            var result = new List<StreamEvent<bool>>();
            output.Subscribe(t => result.Add(t));

            var process = container.Restore();

            inputSubject.OnNext(StreamEvent.CreatePoint(1001, 1)); // true
            inputSubject.OnNext(StreamEvent.CreatePoint(1004, 1)); // false
            inputSubject.OnNext(StreamEvent.CreatePoint(1009, 1)); // false
            inputSubject.OnNext(StreamEvent.CreatePoint(1014, 1)); // true
            inputSubject.OnNext(StreamEvent.CreatePoint(1019, 1)); // false
            inputSubject.OnNext(StreamEvent.CreatePoint(1024, 1)); // true
            process.Flush();

            var expected = new StreamEvent<bool>[]
            {
                StreamEvent.CreatePoint(1001, true),
                StreamEvent.CreatePoint(1004, false),
                StreamEvent.CreatePoint(1009, false),
                StreamEvent.CreatePoint(1014, true),
                StreamEvent.CreatePoint(1019, false),
                StreamEvent.CreatePoint(1024, true),
            };

            Assert.IsTrue(expected.SequenceEqual(expected));
            inputSubject.OnCompleted();
        }


        [TestMethod]
        public void IsFirstPartitioned()
        {
            var inputSubject = new Subject<PartitionedStreamEvent<int, int>>();
            var container = new QueryContainer();
            var input = container.RegisterInput(inputSubject);

            const long stateTimeout = 10;
            var query = input
                .StatefulSelect(
                    statefulSelect: (time, state, value) => state.IsFirst(time),
                    initialState: () => new IsFirstState(stateTimeout),
                    stateTimeout: stateTimeout);

            var output = container.RegisterOutput(query);
            var result = new List<PartitionedStreamEvent<int, bool>>();
            output.Subscribe(t => result.Add(t));

            var process = container.Restore();

            inputSubject.OnNext(PartitionedStreamEvent.CreatePoint(1, 1001, 1));
            inputSubject.OnNext(PartitionedStreamEvent.CreatePoint(1, 1004, 1));
            inputSubject.OnNext(PartitionedStreamEvent.CreatePoint(1, 1009, 1));
            inputSubject.OnNext(PartitionedStreamEvent.CreatePoint(1, 1014, 1));
            inputSubject.OnNext(PartitionedStreamEvent.CreatePoint(1, 1019, 1));
            inputSubject.OnNext(PartitionedStreamEvent.CreatePoint(1, 1024, 1));

            inputSubject.OnNext(PartitionedStreamEvent.CreatePoint(2, 1001, 1));
            inputSubject.OnNext(PartitionedStreamEvent.CreatePoint(2, 1004, 1));
            inputSubject.OnNext(PartitionedStreamEvent.CreatePoint(2, 1024, 1));
            process.Flush();

            var expected = new PartitionedStreamEvent<int, bool>[]
            {
                PartitionedStreamEvent.CreatePoint(1, 1001, true),
                PartitionedStreamEvent.CreatePoint(1, 1004, false),
                PartitionedStreamEvent.CreatePoint(1, 1009, false),
                PartitionedStreamEvent.CreatePoint(1, 1014, true),
                PartitionedStreamEvent.CreatePoint(1, 1019, false),
                PartitionedStreamEvent.CreatePoint(1, 1024, true),
                PartitionedStreamEvent.CreatePoint(2, 1001, true),
                PartitionedStreamEvent.CreatePoint(2, 1004, false),
                PartitionedStreamEvent.CreatePoint(2, 1024, true),
            };

            Assert.IsTrue(expected.SequenceEqual(expected));
            inputSubject.OnCompleted();
        }
    }
}