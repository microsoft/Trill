// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Serializer;
using Microsoft.StreamProcessing.Sharding;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class AdHoc : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public AdHoc() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true))
        { }

        /// <summary>
        /// This works, but it uses a single field to point to the container object.
        /// </summary>
        [TestMethod, TestCategory("Gated")]
        public void Properties()
        {
            var inputs = new List<MyPropertiesContainer>
            {
                new MyPropertiesContainer { A = 1, B = true, C = 3M, D = "4", E = new Nested() },
            };

            var q1 = inputs.Select(x => StreamEvent.CreatePoint(10, x))
                .ToObservable()
                .ToStreamable(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.None);

            var q2 = q1.Select(x => string.Join(",", x.A, x.B, x.C, x.D, x.E));

            int count = 0;
            q2.ToStreamEventObservable().ForEachAsync(x => count++).Wait();

            Assert.AreEqual(0, count);
        }

        /// <summary>
        /// This works as expected.
        /// </summary>
        [TestMethod, TestCategory("Gated")]
        public void Fields()
        {
            var inputs = new List<MyFieldsContainer>
            {
                new MyFieldsContainer { A = 1, B = true, C = 3M, D = "4", E = new Nested() },
            };

            var q1 = inputs.Select(x => StreamEvent.CreatePoint(10, x))
                .ToObservable()
                .ToStreamable(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.None);

            var q2 = q1.Select(x => string.Join(",", x.A, x.B, x.C, x.D, x.E));

            int count = 0;
            q2.ToStreamEventObservable().ForEachAsync(x => count++).Wait();

            Assert.AreEqual(0, count);
        }

        /// <summary>
        /// Throws an exception about no fields.
        /// </summary>
        [TestMethod, TestCategory("Gated")]
        public void Initializer()
        {
            var inputs = new List<MyFieldsContainer>
            {
                new MyFieldsContainer { A = 1, B = true, C = 3M, D = "4", E = new Nested() { Value = "five" } },
            };

            var q1 = inputs.Select(x => StreamEvent.CreatePoint(10, x))
                .ToObservable()
                .ToStreamable(DisorderPolicy.Throw(), FlushPolicy.FlushOnPunctuation, PeriodicPunctuationPolicy.None(), OnCompletedPolicy.None);

            var q2 = q1.Select(x => new Nested { Value = x.E.Value });
            var q3 = q2.Select(x => x.Value);

            int count = 0;
            q2.ToStreamEventObservable().ForEachAsync(x => count++).Wait();

            Assert.AreEqual(0, count);
        }

        [TestMethod, TestCategory("Gated")]
        public void StringSerializationDataBatchSizeChange()
        {
            using (new ConfigModifier().DataBatchSize(1000).Modify())
            {
                TestSerialization();
            }
            using (new ConfigModifier().DataBatchSize(80000).Modify())
            {
                TestSerialization();
            }
        }

        private void TestSerialization()
        {
            var rand = new Random(0);
            var pool = MemoryManager.GetColumnPool<string>();

            for (int x = 0; x < 100; x++)
            {
                pool.Get(out var inputStr);

                var toss1 = rand.NextDouble();
                inputStr.UsedLength = toss1 < 0.1 ? 0 : rand.Next(Config.DataBatchSize);

                for (int i = 0; i < inputStr.UsedLength; i++)
                {
                    var toss = rand.NextDouble();
                    inputStr.col[i] = toss < 0.2 ? string.Empty : (toss < 0.4 ? null : Guid.NewGuid().ToString());
                    if (x == 0) inputStr.col[i] = null;
                    if (x == 1) inputStr.col[i] = string.Empty;
                }

                var s = StreamableSerializer.Create<ColumnBatch<string>>(new SerializerSettings { });
                var ms = new MemoryStream
                {
                    Position = 0
                };
                s.Serialize(ms, inputStr);
                ms.Position = 0;
                ColumnBatch<string> resultStr = s.Deserialize(ms);

                Assert.IsTrue(resultStr.UsedLength == inputStr.UsedLength);

                for (int j = 0; j < inputStr.UsedLength; j++)
                {
                    Assert.IsTrue(inputStr.col[j] == resultStr.col[j]);
                }
                resultStr.ReturnClear();
                inputStr.ReturnClear();
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void StreamEventArrayIngress1()
        {
            // Test where full array can fit into one batch
            var startTime = 0;
            var length = 100;
            var a = Enumerable.Range(0, length)
                .Select(i => StreamEvent.CreateStart(startTime++ / 80000, i))
                .ToArray();
            var input = new ArraySegment<StreamEvent<int>>[] { new ArraySegment<StreamEvent<int>>(a), };
            var s = input.ToObservable();
            var str = s.ToStreamable();
            var output = str.ToStreamMessageObservable().ToEnumerable().ToArray();
            Assert.IsTrue(output.Length == 1); // first batch with all data and one punctuation
            Assert.IsTrue(output[0].Count == length + 1);
            Assert.IsTrue(output[0].vother.col[output[0].Count - 1] == StreamEvent.PunctuationOtherTime);
            for (int i = 0; i < output.Length; i++)
                output[i].Free();
        }

        [TestMethod, TestCategory("Gated")]
        public void StreamEventArrayIngress2()
        {
            // Test where full array spans multiple batches
            var startTime = 0;
            var length = (Config.DataBatchSize * 3) + (Config.DataBatchSize / 2);
            var a = Enumerable.Range(0, length)
                .Select(i => StreamEvent.CreateStart(startTime++ / 80000, i))
                .ToArray();
            var input = new ArraySegment<StreamEvent<int>>[] { new ArraySegment<StreamEvent<int>>(a) };
            var s = input.ToObservable();
            var str = s.ToStreamable();
            var output = str.ToStreamMessageObservable().ToEnumerable().ToArray();
            Assert.IsTrue(output.Length == 4); // four data batches
            Assert.IsTrue(output[0].Count + output[1].Count + output[2].Count + output[3].Count == length + 1);

            // fourth data batch should have a punctuation at the end
            Assert.IsTrue(output[3].vother.col[output[3].Count - 1] == StreamEvent.PunctuationOtherTime);
            for (int i = 0; i < output.Length; i++)
                output[i].Free();
        }

        [TestMethod, TestCategory("Gated")]
        public void StreamEventArrayIngress3()
        {
            // Test where full array contains punctuations
            var length = 100;
            var a = Enumerable.Range(1, length)
                .Select(i => i % 10 == 0 ?
                    StreamEvent.CreatePunctuation<int>(i)
                    : StreamEvent.CreateStart(i, i))
                .ToArray();
            var input = new ArraySegment<StreamEvent<int>>[] { new ArraySegment<StreamEvent<int>>(a) };
            var s = input.ToObservable();
            var str = s.ToStreamable();
            var output = str.ToStreamMessageObservable().ToEnumerable().ToArray();

            // 10 pairs of data and punc, then one with just a punctuation at infinity
            int expectedDataBatches = 11;
            Assert.IsTrue(output.Length == expectedDataBatches);
            var dataEventCount = 0;
            for (int i = 0; i < output.Length; i++)
            {
                // Data batch should end with a punctuation
                Assert.IsTrue(output[i].vother.col[output[i].Count - 1] == StreamEvent.PunctuationOtherTime);
                dataEventCount += output[i].Count - 1; // exclude punctuation
            }
            Assert.IsTrue(dataEventCount == length - 10); // because every 10th row was a punctuation, not a data row
            for (int i = 0; i < output.Length; i++)
                output[i].Free();
        }

        [TestMethod, TestCategory("Gated")]
        public void AlterLifetimeStartDependentDuration()
        {
            var input = Enumerable.Range(1, 1000);
            var streamable = input.Select(o => StreamEvent.CreateStart(o, o)).ToObservable().ToStreamable().AlterEventDuration(o => o);
            var output = streamable.ToTemporalObservable((s, e, p) => e).ToEnumerable();
            var expected = input.Select(o => (long)(2 * o));
            Assert.IsTrue(Enumerable.SequenceEqual(output, expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void PartitionedStreamAdjustIngressPolicy()
        {
            var qc = new QueryContainer();
            var input = new PartitionedStreamEvent<string, string>[]
                {
                    PartitionedStreamEvent.CreatePunctuation<string, string>("k1", 10),
                    PartitionedStreamEvent.CreatePoint("k2", 70, "v1"),
                    PartitionedStreamEvent.CreatePoint("k1", 11, "v2"),
                    PartitionedStreamEvent.CreatePoint("k1", 22, "v3")
                };
            var output = new List<PartitionedStreamEvent<string, string>>();
            var ingress = qc.RegisterInput(input.ToObservable(), DisorderPolicy.Adjust(0),
                PartitionedFlushPolicy.FlushOnLowWatermark, null, PeriodicLowWatermarkPolicy.Time(10, 50));

            var egress = qc.RegisterOutput(ingress).ForEachAsync(o => output.Add(o));
            var process = qc.Restore();
            process.Flush();
            egress.Wait();

            output = output.Where(o => o.IsData).ToList();
            Assert.AreEqual(2, output.Count);
            Assert.AreEqual(70, output[0].SyncTime);
            Assert.AreEqual(22, output[1].SyncTime);
        }

        public class MyPropertiesContainer
        {
            public int A { get; set; }
            public bool B { get; set; }
            public decimal C { get; set; }
            public string D { get; set; }
            public Nested E { get; set; }
        }

        public class MyFieldsContainer
        {
            public int A;
            public bool B;
            public decimal C;
            public string D;
            public Nested E;

            public override bool Equals(object obj)
                => !(obj is MyFieldsContainer other)
                    ? false
                    : other.A == this.A && other.B == this.B && other.C.Equals(this.C) && (other.D == null || other.D.Equals(this.D)) && (other.E == null || other.E.Equals(this.E));
            public override int GetHashCode()
            {
                uint hash = (uint)this.A.GetHashCode();
                hash = (hash << 2) | (hash >> 30);
                hash ^= (uint)this.B.GetHashCode();
                hash = (hash << 2) | (hash >> 30);
                hash ^= (uint)this.C.GetHashCode();
                if (this.D != null)
                {
                    hash = (hash << 2) | (hash >> 30);
                    hash ^= (uint)this.D.GetHashCode();
                }
                if (this.E != null)
                {
                    hash = (hash << 2) | (hash >> 30);
                    hash ^= (uint)this.E.GetHashCode();
                }
                return (int)hash;
            }
        }

        public class Nested
        {
            public string Value { get; set; }
        }

        private static IList<StreamEvent<T>> Execute<T>(IEnumerable<StreamEvent<T>> input, Func<IStreamable<Empty, T>, IStreamable<Empty, T>> selector)
            => selector(input
            .ToObservable()
            .ToStreamable())
            .ToStreamEventObservable()
            .ToList()
            .Wait();

        [TestMethod]
        public void AlterDuration_Negative()
        {
            StreamEvent<int>[] input = new[] { StreamEvent.CreateInterval(100, 200, 0) };
            IList<StreamEvent<int>> result;

            // silently converts input to end edges
            result = Execute(input, x => x.AlterEventDuration(s => 0));

            foreach (StreamEvent<int> q in result)
                Console.WriteLine(q);

            // in debug, throws assert; in release, silently converts input to end edges
            result = Execute(input, x => x.AlterEventDuration((s, e) => 0));

            foreach (StreamEvent<int> q in result)
                Console.WriteLine(q);
        }

        [TestMethod]
        public void AlterLifetime_Negative()
        {
            StreamEvent<int>[] input = new[] { StreamEvent.CreateInterval(100, 200, 0) };
            IList<StreamEvent<int>> result;

            // silently converts input to end edges
            result = Execute(input, x => x.AlterEventLifetime(s => s, s => 0));

            foreach (StreamEvent<int> q in result)
                Console.WriteLine(q);

            // in debug, throws assert; in release, silently converts input to end edges
            result = Execute(input, x => x.AlterEventLifetime(s => s, (s, e) => 0));

            foreach (StreamEvent<int> q in result)
                Console.WriteLine(q);
        }

        [TestMethod]
        public void ClipEventDuration_Constant()
        {
            StreamEvent<int>[] input = new[]
            {
                StreamEvent.CreateStart(100,      0),
                StreamEvent.CreateStart(100,      1),
                StreamEvent.CreateStart(100,      2),
                StreamEvent.CreateEnd(150, 100, 0), // duration < truncate amount
                StreamEvent.CreateEnd(200, 100, 1), // duration == truncate amount
                StreamEvent.CreateEnd(250, 100, 2), // duration > truncate amount
            };

            // throws KeyNotFoundException
            IList<StreamEvent<int>> result = Execute(input, x => x.ClipEventDuration(150));

            foreach (StreamEvent<int> q in result)
                Console.WriteLine(q);
        }

        [TestMethod, TestCategory("Gated")]
        public void UnderFlowFalseFlush()
        {
            const int reorderLatency = 100;
            const int periodicPunctuationFrequency = 100;

            var input = new PartitionedStreamEvent<int, int>[]
            {
                // This establishes three partitions with keys 1,2,3
                PartitionedStreamEvent.CreatePoint(0, 0, 0),
                PartitionedStreamEvent.CreatePoint(1, 0, 1),
                PartitionedStreamEvent.CreatePoint(2, 0, 2),

                PartitionedStreamEvent.CreatePoint(0, 20, 0),
                PartitionedStreamEvent.CreatePoint(0, 10, 0),

                // When in OnNext for 1000, we call Process(10), which used to yield a false flush due to arthimetic underflow
                PartitionedStreamEvent.CreatePoint(0, 1000, 0),
            };

            var expected = Array.Empty<PartitionedStreamEvent<int, int>>();

            var qc = new QueryContainer();
            var ingress = qc.RegisterInput(
                input.ToObservable(),
                DisorderPolicy.Drop(reorderLatency),
                PartitionedFlushPolicy.FlushOnLowWatermark,
                PeriodicPunctuationPolicy.Time(periodicPunctuationFrequency),
                PeriodicLowWatermarkPolicy.None(),
                OnCompletedPolicy.None);

            var output = new List<PartitionedStreamEvent<int, int>>();
            var egress = qc.RegisterOutput(ingress).ForEachAsync(o => output.Add(o));
            var process = qc.Restore();
            process.Flush();
            egress.Wait();

            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void InactiveAndNewPartitions()
        {
            const int reorderLatency = 5;
            const int periodicPunctuationFrequency = 10;

            var input = new PartitionedStreamEvent<int, int>[]
            {
                PartitionedStreamEvent.CreatePoint(0, 0, 0),    // Partition 0 - always current
                PartitionedStreamEvent.CreatePoint(1, 0, 1),    // Partition 1 - becomes inactive, reactivates immediately after LowWatermark
                PartitionedStreamEvent.CreatePoint(2, 0, 2),    // Partition 2 - becomes inactive, reactivates after LowWatermark + punctuationFrequency

                // Partition 3 - appears for the first time immediately after LowWatermark
                // Partition 4 - appears for the first time after LowWatermark + punctuationFrequency
                PartitionedStreamEvent.CreatePoint(0, 100, 0),
                PartitionedStreamEvent.CreateLowWatermark<int, int>(100),

                // Partition 1 reactivates immediately after LowWatermark
                PartitionedStreamEvent.CreatePoint(1, 100, 1),

                // Partition 3 appears for the first time immediately after LowWatermark
                PartitionedStreamEvent.CreatePoint(3, 100, 3),

                // Partition 2 reactivates after LowWatermark + punctuationFrequency
                PartitionedStreamEvent.CreatePoint(0, 150, 0),
                PartitionedStreamEvent.CreatePoint(1, 150, 1),
                PartitionedStreamEvent.CreatePoint(2, 150, 2),
                PartitionedStreamEvent.CreatePoint(3, 150, 3),

                // Partition 4 appears for the first time after LowWatermark + punctuationFrequency
                PartitionedStreamEvent.CreatePoint(4, 150, 4),

                // This should generate punctuations for all partitions
                PartitionedStreamEvent.CreatePoint(0, 200, 0),
                PartitionedStreamEvent.CreatePoint(1, 200, 1),
                PartitionedStreamEvent.CreatePoint(2, 200, 2),
                PartitionedStreamEvent.CreatePoint(3, 200, 3),
                PartitionedStreamEvent.CreatePoint(4, 200, 4),
            };

            var expected = new PartitionedStreamEvent<int, int>[]
            {
                PartitionedStreamEvent.CreatePoint(0, 0, 0),
                PartitionedStreamEvent.CreatePoint(1, 0, 1),
                PartitionedStreamEvent.CreatePoint(2, 0, 2),

                PartitionedStreamEvent.CreatePunctuation<int, int>(0, 100),
                PartitionedStreamEvent.CreatePoint(0, 100, 0),
                PartitionedStreamEvent.CreateLowWatermark<int, int>(100),

                PartitionedStreamEvent.CreatePoint(1, 100, 1),

                PartitionedStreamEvent.CreatePoint(3, 100, 3),

                PartitionedStreamEvent.CreatePunctuation<int, int>(0, 150),
                PartitionedStreamEvent.CreatePoint(0, 150, 0),
                PartitionedStreamEvent.CreatePunctuation<int, int>(1, 150),
                PartitionedStreamEvent.CreatePoint(1, 150, 1),
                PartitionedStreamEvent.CreatePunctuation<int, int>(2, 150),
                PartitionedStreamEvent.CreatePoint(2, 150, 2),
                PartitionedStreamEvent.CreatePunctuation<int, int>(3, 150),
                PartitionedStreamEvent.CreatePoint(3, 150, 3),

                // As a side effect of cleaning up expired key state, currently, for both expired keys and brand new
                // keys, we could either generate punctuations or not. The implementation happens to generate for both,
                // which is fine, since the spurious punctuation for brand new keys is harmless.
                PartitionedStreamEvent.CreatePunctuation<int, int>(4, 150),
                PartitionedStreamEvent.CreatePoint(4, 150, 4),

                // This should generate punctuations for all partitions
                PartitionedStreamEvent.CreatePunctuation<int, int>(0, 200),
                PartitionedStreamEvent.CreatePoint(0, 200, 0),
                PartitionedStreamEvent.CreatePunctuation<int, int>(1, 200),
                PartitionedStreamEvent.CreatePoint(1, 200, 1),
                PartitionedStreamEvent.CreatePunctuation<int, int>(2, 200),
                PartitionedStreamEvent.CreatePoint(2, 200, 2),
                PartitionedStreamEvent.CreatePunctuation<int, int>(3, 200),
                PartitionedStreamEvent.CreatePoint(3, 200, 3),
                PartitionedStreamEvent.CreatePunctuation<int, int>(4, 200),
                PartitionedStreamEvent.CreatePoint(4, 200, 4),

                PartitionedStreamEvent.CreateLowWatermark<int, int>(StreamEvent.InfinitySyncTime),
            };

            var qc = new QueryContainer();
            var ingress = qc.RegisterInput(
                input.ToObservable(),
                DisorderPolicy.Throw(reorderLatency),
                PartitionedFlushPolicy.FlushOnLowWatermark,
                PeriodicPunctuationPolicy.Time(periodicPunctuationFrequency),
                PeriodicLowWatermarkPolicy.None(),
                OnCompletedPolicy.EndOfStream);

            var output = new List<PartitionedStreamEvent<int, int>>();
            var egress = qc.RegisterOutput(ingress).ForEachAsync(o => output.Add(o));
            var process = qc.Restore();
            process.Flush();
            egress.Wait();

            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void FileStreamPassthrough()
        {
            // Including stream properties must be consistent across serialization/deserialization
            const bool serializeStreamProperties = true;

            var inputData = new StreamEvent<long>[]
            {
                StreamEvent.CreateInterval(1, 2, 0L),
                StreamEvent.CreateInterval(2, 3, 20L),
                StreamEvent.CreateInterval(3, 4, 15L),
                StreamEvent.CreateInterval(4, 5, 30L),
                StreamEvent.CreateInterval(5, 6, 45L),
                StreamEvent.CreateInterval(6, 7, 50L),
                StreamEvent.CreateInterval(7, 8, 30L),
                StreamEvent.CreateInterval(8, 9, 35L),
                StreamEvent.CreateInterval(9, 10, 60L),
                StreamEvent.CreateInterval(10, 11, 20L),
            };

            var input = inputData.ToObservable().ToStreamable();

            // Stream to file
            string filePath = $"{Path.GetTempPath()}\\{nameof(FileStreamPassthrough)}.bin";
            using (var fileStream = new FileStream(filePath, FileMode.Create))
            {
                input.ToBinaryStream(fileStream, writePropertiesToStream: serializeStreamProperties);
            }

            // Stream from file
            var fromFile = filePath.ToStreamableFromFile<long>(readPropertiesFromStream: serializeStreamProperties);
            var output = new List<StreamEvent<long>>();
            fromFile.ToStreamEventObservable().Where(e => e.IsData).ForEachAsync(r => output.Add(r)).Wait();

            Assert.IsTrue(inputData.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void FileStreamDoubleQuery()
        {
            const bool serializeStreamProperties = true;

            string filePath = $"{Path.GetTempPath()}\\{nameof(FileStreamDoubleQuery)}.bin";
            const int inputEventCount = 100;
            var inputData = Enumerable.Range(0, inputEventCount)
                .Select(e => StreamEvent.CreatePoint<long>(0, e))
                .ToList();
            var input = inputData.ToObservable().ToStreamable();

            // Stream to file
            using (var stream = File.Create(filePath))
            {
                input.ToBinaryStream(stream, writePropertiesToStream: serializeStreamProperties);
            }

            // Stream from file, twice. This should be supported for streams that support Seek.
            using (var stream = File.OpenRead(filePath))
            {
                Assert.IsTrue(stream.CanSeek);

                var streamable = stream.ToStreamable<long>(readPropertiesFromStream: serializeStreamProperties);
                for (int i = 0; i < 2; i++)
                {
                    var output = new List<StreamEvent<long>>();
                    streamable.ToStreamEventObservable().Where(e => e.IsData).ForEachAsync(e => output.Add(e)).Wait();
                    Assert.IsTrue(inputData.SequenceEqual(output));
                }
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void ExplicitAndGeneratedPunctuations()
        {
            Helpers.RunTwiceForRowAndColumnar(() =>
            {
                var input = new StreamEvent<int>[]
                {
                    StreamEvent.CreateInterval(90, 91, 0),
                    StreamEvent.CreatePunctuation<int>(80),
                    StreamEvent.CreateInterval(110, 111, 0),
                    StreamEvent.CreateInterval(195, 200, 0),

                    StreamEvent.CreateInterval(200, 201, 0),
                    StreamEvent.CreatePunctuation<int>(300),
                    StreamEvent.CreateInterval(300, 301, 0),
                }.ToObservable().ToStreamable(periodicPunctuationPolicy: PeriodicPunctuationPolicy.Time(100));

                var output = new List<StreamEvent<int>>();
                input.ToStreamEventObservable()
                    .ForEachAsync(x => output.Add(x))
                    .Wait();

                var expected = new List<StreamEvent<int>>
                {
                    StreamEvent.CreateInterval(90, 91, 0),
                    StreamEvent.CreatePunctuation<int>(90),  // Explicitly ingressed punctuation is adjusted to currentTime
                    StreamEvent.CreatePunctuation<int>(100), // Generated punctuation based on quantized previous punctuation
                    StreamEvent.CreateInterval(110, 111, 0),
                    StreamEvent.CreateInterval(195, 200, 0),

                    StreamEvent.CreatePunctuation<int>(200), // Generated punctuation
                    StreamEvent.CreateInterval(200, 201, 0),
                    StreamEvent.CreatePunctuation<int>(300), // Explicitly ingressed punctuation should not be replicated
                    StreamEvent.CreateInterval(300, 301, 0),

                    StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
                };

                Assert.IsTrue(expected.SequenceEqual(output));
            });
        }

        [TestMethod, TestCategory("Gated")]
        public void ExplicitAndGeneratedLowWatermarks()
        {
            var input = new PartitionedStreamEvent<int, int>[]
            {
                PartitionedStreamEvent.CreateInterval(0, 90, 91, 0),
                PartitionedStreamEvent.CreateLowWatermark<int, int>(80),
                PartitionedStreamEvent.CreateInterval(0, 110, 111, 0),
                PartitionedStreamEvent.CreateInterval(0, 195, 196, 0),

                PartitionedStreamEvent.CreateInterval(0, 200, 201, 0),
                PartitionedStreamEvent.CreateLowWatermark<int, int>(300),
                PartitionedStreamEvent.CreateInterval(0, 300, 301, 0),

                PartitionedStreamEvent.CreateInterval(0, 400, 401, 0),
                PartitionedStreamEvent.CreatePunctuation<int, int>(0, 450),
                PartitionedStreamEvent.CreateInterval(0, 450, 451, 0),
            }.ToObservable()
            .ToStreamable(
                periodicPunctuationPolicy: PeriodicPunctuationPolicy.Time(50),
                periodicLowWatermarkPolicy: PeriodicLowWatermarkPolicy.Time(generationPeriod: 100, lowWatermarkTimestampLag: 0));

            var output = new List<PartitionedStreamEvent<int, int>>();
            input.ToStreamEventObservable()
                .ForEachAsync(x => output.Add(x))
                .Wait();

            var expected = new List<PartitionedStreamEvent<int, int>>
            {
                PartitionedStreamEvent.CreatePunctuation<int, int>(0, 50),
                PartitionedStreamEvent.CreateInterval(0, 90, 91, 0),
                PartitionedStreamEvent.CreateLowWatermark<int, int>(80),    // Explicitly ingressed low watermark
                PartitionedStreamEvent.CreateLowWatermark<int, int>(100),   // Generated punctuation based on quantized previous lowWatermark
                PartitionedStreamEvent.CreateInterval(0, 110, 111, 0),
                PartitionedStreamEvent.CreatePunctuation<int, int>(0, 150), // Generated punctuation
                PartitionedStreamEvent.CreateInterval(0, 195, 196, 0),

                PartitionedStreamEvent.CreateLowWatermark<int, int>(200),   // Generated low watermark
                PartitionedStreamEvent.CreateInterval(0, 200, 201, 0),
                PartitionedStreamEvent.CreateLowWatermark<int, int>(300),   // Explicitly ingressed low watermark should not be replicated
                PartitionedStreamEvent.CreateInterval(0, 300, 301, 0),

                PartitionedStreamEvent.CreateLowWatermark<int, int>(400),   // Generated low watermark
                PartitionedStreamEvent.CreateInterval(0, 400, 401, 0),
                PartitionedStreamEvent.CreatePunctuation<int, int>(0, 450), // Explicitly ingressed punctuation should not be replicated
                PartitionedStreamEvent.CreateInterval(0, 450, 451, 0),

                PartitionedStreamEvent.CreateLowWatermark<int, int>(PartitionedStreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void ExplicitLowWatermarkInMiddleOfLowWatermarkGenerationPeriod()
        {
            const long generationPeriod = 100;
            var qc = new QueryContainer();
            var input = new PartitionedStreamEvent<int, int>[]
            {
                PartitionedStreamEvent.CreatePoint(0, 0, 0),
                PartitionedStreamEvent.CreatePoint(0, 105, 0),  // Should generate lwm quantized to 100

                PartitionedStreamEvent.CreateLowWatermark<int, int>(150),

                PartitionedStreamEvent.CreatePoint(0, 240, 0),  // Should generate lwm quantized to 200
                PartitionedStreamEvent.CreatePoint(0, 255, 0),
            };

            var expected = new PartitionedStreamEvent<int, int>[]
            {
                PartitionedStreamEvent.CreatePoint(0, 0, 0),
                PartitionedStreamEvent.CreateLowWatermark<int, int>(100),
                PartitionedStreamEvent.CreatePoint(0, 105, 0),

                PartitionedStreamEvent.CreateLowWatermark<int, int>(150),

                PartitionedStreamEvent.CreateLowWatermark<int, int>(200),
                PartitionedStreamEvent.CreatePoint(0, 240, 0),
                PartitionedStreamEvent.CreatePoint(0, 255, 0),

                PartitionedStreamEvent.CreateLowWatermark<int, int>(StreamEvent.InfinitySyncTime),
            };

            var ingress = qc.RegisterInput(
                input.ToObservable(),
                periodicLowWatermarkPolicy: PeriodicLowWatermarkPolicy.Time(generationPeriod, lowWatermarkTimestampLag: 0));

            var output = new List<PartitionedStreamEvent<int, int>>();
            var egress = qc.RegisterOutput(ingress).ForEachAsync(o => output.Add(o));
            var process = qc.Restore();
            process.Flush();
            egress.Wait();

            output = output.ToList();
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void ExplicitLowWatermarkInMiddleOfPunctuationGenerationPeriod()
        {
            const long generationPeriod = 100;
            var qc = new QueryContainer();
            var input = new PartitionedStreamEvent<int, int>[]
            {
                PartitionedStreamEvent.CreatePoint(0, 0, 0),
                PartitionedStreamEvent.CreatePoint(0, 105, 0),  // Should generate punctuation quantized to 100

                PartitionedStreamEvent.CreateLowWatermark<int, int>(150),

                PartitionedStreamEvent.CreatePoint(0, 240, 0),  // Should generate a punctuation quantized to 200
                PartitionedStreamEvent.CreatePoint(0, 255, 0),
            };

            var expected = new PartitionedStreamEvent<int, int>[]
            {
                PartitionedStreamEvent.CreatePoint(0, 0, 0),
                PartitionedStreamEvent.CreatePunctuation<int, int>(0, 100),
                PartitionedStreamEvent.CreatePoint(0, 105, 0),

                PartitionedStreamEvent.CreateLowWatermark<int, int>(150),

                PartitionedStreamEvent.CreatePunctuation<int, int>(0, 200),
                PartitionedStreamEvent.CreatePoint(0, 240, 0),
                PartitionedStreamEvent.CreatePoint(0, 255, 0),

                PartitionedStreamEvent.CreateLowWatermark<int, int>(StreamEvent.InfinitySyncTime),
            };

            var ingress = qc.RegisterInput(
                input.ToObservable(),
                periodicPunctuationPolicy: PeriodicPunctuationPolicy.Time(generationPeriod));

            var output = new List<PartitionedStreamEvent<int, int>>();
            var egress = qc.RegisterOutput(ingress).ForEachAsync(o => output.Add(o));
            var process = qc.Restore();
            process.Flush();
            egress.Wait();

            output = output.ToList();
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void ExplicitPartitionedPunctuationInMiddleOfPunctuationGenerationPeriod()
        {
            const long generationPeriod = 100;
            var qc = new QueryContainer();
            var input = new PartitionedStreamEvent<int, int>[]
            {
                PartitionedStreamEvent.CreatePoint(0, 0, 0),
                PartitionedStreamEvent.CreatePoint(0, 105, 0),  // Should generate punctuation quantized to 100

                PartitionedStreamEvent.CreatePunctuation<int, int>(0, 150),

                PartitionedStreamEvent.CreatePoint(0, 240, 0),  // Should this generate a punctuation at 200?
                PartitionedStreamEvent.CreatePoint(0, 255, 0),  // Cannot generate punctuation at 200, since it's before the preceding data event!
            };

            var expected = new PartitionedStreamEvent<int, int>[]
            {
                PartitionedStreamEvent.CreatePoint(0, 0, 0),
                PartitionedStreamEvent.CreatePunctuation<int, int>(0, 100),
                PartitionedStreamEvent.CreatePoint(0, 105, 0),

                PartitionedStreamEvent.CreatePunctuation<int, int>(0, 150),

                PartitionedStreamEvent.CreatePunctuation<int, int>(0, 200),
                PartitionedStreamEvent.CreatePoint(0, 240, 0),
                PartitionedStreamEvent.CreatePoint(0, 255, 0),

                PartitionedStreamEvent.CreateLowWatermark<int, int>(StreamEvent.InfinitySyncTime),
            };

            var ingress = qc.RegisterInput(
                input.ToObservable(),
                periodicPunctuationPolicy: PeriodicPunctuationPolicy.Time(generationPeriod));

            var output = new List<PartitionedStreamEvent<int, int>>();
            var egress = qc.RegisterOutput(ingress).ForEachAsync(o => output.Add(o));
            var process = qc.Restore();
            process.Flush();
            egress.Wait();

            output = output.ToList();
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void LASJ_OutOfOrderLWM()
        {
            const int key = 0; // Just use one key as the key and payload
            const long duration = 5;
            PartitionedStreamEvent<int, int> CreateInterval(long time) => PartitionedStreamEvent.CreateInterval(key, time, time + duration, key);
            PartitionedStreamEvent<int, int> CreateLowWatermark(long time) => PartitionedStreamEvent.CreateLowWatermark<int, int>(time);

            var leftData = new PartitionedStreamEvent<int, int>[]
            {
                CreateInterval(10),
                CreateInterval(15),     // This will not be processed until right side's LWM at 20
                CreateInterval(30),
                CreateInterval(40),
                CreateInterval(50),
            };

            var rightData = new PartitionedStreamEvent<int, int>[]
            {
                CreateInterval(5),
                CreateInterval(6),
                CreateInterval(7),
                CreateInterval(10),     // Matches all of 10-15
                CreateLowWatermark(20), // This should process left's 15, then output the low watermark at 10 (not vice versa)

                CreateInterval(20),     // No match
            };

            var expected = new PartitionedStreamEvent<int, int>[]
            {
                CreateInterval(15),
                CreateLowWatermark(20),
                CreateInterval(30),
                CreateInterval(40),
                CreateInterval(50),
                CreateLowWatermark(StreamEvent.InfinitySyncTime),
            };

            var qc = new QueryContainer();
            var leftInput = qc.RegisterInput(leftData.ToObservable(), flushPolicy: PartitionedFlushPolicy.FlushOnLowWatermark);
            var rightInput = qc.RegisterInput(rightData.ToObservable(), flushPolicy: PartitionedFlushPolicy.FlushOnLowWatermark);

            var query = leftInput.WhereNotExists(
                rightInput,
                e => e,
                e => e);

            var output = new List<PartitionedStreamEvent<int, int>>();
            var egress = qc.RegisterOutput(query).ForEachAsync(o => output.Add(o));
            var process = qc.Restore();
            process.Flush();
            egress.Wait();

            Assert.IsTrue(expected.SequenceEqual(output));
        }

        internal struct SimpleStruct
        {
            public long One { get; set; }
        }

        [TestMethod, TestCategory("Gated")]
        public void SimpleShardTest()
        {
            const int BatchCount = 1000;
            const int SourceCount = 6;

            var input =
                Enumerable.Range(0, BatchCount * SourceCount)
                .Select(i => StreamEvent.CreateInterval(DateTime.Now.Ticks, 1, new SimpleStruct() { One = 1 }))
                .ToList();

            var generatorShard = input
                .ToObservable()
                .ToStreamable()
                .Shard(SourceCount);

            var output = new List<StreamEvent<SimpleStruct>>();
            var passthrough = generatorShard
                .Query(e => e.Select(x => x))
                .Unshard()
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .ForEachAsync(e => output.Add(e));

            Assert.IsTrue(output.SequenceEqual(input));
        }

        // Tests single-punctuation batches to make sure MinTimestamp/MaxTimestamp are computed correctly.
        [TestMethod, TestCategory("Gated")]
        public void SinglePunctuationBatchTimestamps()
        {
            using (new ConfigModifier().DataBatchSize(5).Modify())
            {
                var qc = new QueryContainer();
                long currentMin = 0;
                long currentMax = 0;
                Observable.Range(0, Config.DataBatchSize * 10)
                    .Select(e => e % (Config.DataBatchSize + 1) == 0 ? StreamEvent.CreatePunctuation<int>(e) : StreamEvent.CreatePoint(e, e))
                    .ToStreamable()
                    .ToStreamMessageObservable()
                    .ForEachAsync(
                        batch =>
                        {
                            var min = batch.MinTimestamp;
                            Assert.IsTrue(min >= currentMin);
                            currentMin = min;

                            var max = batch.MaxTimestamp;
                            Assert.IsTrue(max >= currentMax);
                            currentMax = max;

                            batch.Free();
                        });
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void AggressivePartitionCleanup_LASJ()
        {
            using (new ConfigModifier().DataBatchSize(4).Modify())
            {
                const long duration = 5;
                PartitionedStreamEvent<int, int> CreateStart(int key, long time) => PartitionedStreamEvent.CreateStart(key, time, key);
                PartitionedStreamEvent<int, int> CreateEnd(int key, long time) => PartitionedStreamEvent.CreateEnd(key, time, time - duration, key);
                PartitionedStreamEvent<int, int> CreateInterval(int key, long time) => PartitionedStreamEvent.CreateInterval(key, time, time + duration, key);
                PartitionedStreamEvent<int, int> CreatePunctuation(int key, long time) => PartitionedStreamEvent.CreatePunctuation<int, int>(key, time);
                PartitionedStreamEvent<int, int> CreateLowWatermark(long time) => PartitionedStreamEvent.CreateLowWatermark<int, int>(time);

                var left = new Subject<PartitionedStreamEvent<int, int>>();
                var right = new Subject<PartitionedStreamEvent<int, int>>();

                var qc = new QueryContainer();
                var leftInput = qc.RegisterInput(left, flushPolicy: PartitionedFlushPolicy.FlushOnLowWatermark);
                var rightInput = qc.RegisterInput(right, flushPolicy: PartitionedFlushPolicy.FlushOnLowWatermark);

                var query = leftInput.WhereNotExists(
                    rightInput,
                    e => e,
                    e => e);

                var output = new List<PartitionedStreamEvent<int, int>>();
                qc.RegisterOutput(query).ForEachAsync(o => output.Add(o));
                var process = qc.Restore();

                // Set up state so that the left has an "invisible" (i.e. unmatched on the right) interval.
                right.OnNext(CreateStart(0, 85));
                right.OnNext(CreateEnd(0, 90));
                right.OnNext(CreateLowWatermark(50));

                left.OnNext(CreateInterval(0, 90));
                left.OnNext(CreateLowWatermark(50));

                // Ingress batches without key 0, trying to evict partition 0
                for (int i = 0; i < 10; i++)
                {
                    left.OnNext(CreateInterval(1, 100 + i));
                    right.OnNext(CreateInterval(1, 100 + i));
                }

                // Ingress two LWMs after the original interval
                left.OnNext(CreateLowWatermark(100));
                right.OnNext(CreateLowWatermark(100));

                left.OnNext(CreateInterval(0, 105));

                // Make sure a dry partition (right) is cleaned/reactivated properly
                left.OnNext(CreatePunctuation(0, 115));
                right.OnNext(CreatePunctuation(0, 115));
                process.Flush();

                left.OnNext(CreateInterval(0, 115));
                left.OnNext(CreateInterval(0, 120));

                right.OnNext(CreateInterval(0, 125));
                process.Flush();

                left.OnNext(CreateLowWatermark(120));
                right.OnNext(CreateLowWatermark(120));

                left.OnNext(CreateInterval(0, 125));

                left.OnCompleted();
                right.OnCompleted();

                var expected = new PartitionedStreamEvent<int, int>[]
                {
                    CreateLowWatermark(50),
                    CreateInterval(0, 90),
                    CreateLowWatermark(100),
                    CreateInterval(0, 105),

                    CreatePunctuation(0, 115),

                    CreateInterval(0, 115),
                    CreateInterval(0, 120),

                    CreateLowWatermark(120),
                    CreateLowWatermark(StreamEvent.InfinitySyncTime),
                };
                Assert.IsTrue(expected.SequenceEqual(output));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void AggressivePartitionCleanup_EquiJoin()
        {
            using (new ConfigModifier().DataBatchSize(4).Modify())
            {
                const long duration = 5;
                PartitionedStreamEvent<int, int> CreateStart(int key, long time) => PartitionedStreamEvent.CreateStart(key, time, key);
                PartitionedStreamEvent<int, int> CreateEnd(int key, long time, long originalStart) => PartitionedStreamEvent.CreateEnd(key, time, originalStart, key);
                PartitionedStreamEvent<int, int> CreateInterval(int key, long time) => PartitionedStreamEvent.CreateInterval(key, time, time + duration, key);
                PartitionedStreamEvent<int, int> CreatePunctuation(int key, long time) => PartitionedStreamEvent.CreatePunctuation<int, int>(key, time);
                PartitionedStreamEvent<int, int> CreateLowWatermark(long time) => PartitionedStreamEvent.CreateLowWatermark<int, int>(time);

                var left = new Subject<PartitionedStreamEvent<int, int>>();
                var right = new Subject<PartitionedStreamEvent<int, int>>();

                var qc = new QueryContainer();
                var leftInput = qc.RegisterInput(left, flushPolicy: PartitionedFlushPolicy.FlushOnLowWatermark);
                var rightInput = qc.RegisterInput(right, flushPolicy: PartitionedFlushPolicy.FlushOnLowWatermark);

                var query = leftInput.Join(
                    rightInput,
                    e => e,
                    e => e,
                    (l, r) => l);

                var output = new List<PartitionedStreamEvent<int, int>>();
                qc.RegisterOutput(query).ForEachAsync(o => output.Add(o));
                var process = qc.Restore();

                // Set up state so that there is outstanding start edges/intervals
                right.OnNext(CreateStart(0, 90));
                right.OnNext(CreateLowWatermark(50));

                left.OnNext(CreateInterval(0, 90));
                left.OnNext(CreateLowWatermark(50));

                // Ingress batches without key 0, trying to evict partition 0
                for (int i = 0; i < 10; i++)
                {
                    left.OnNext(CreateInterval(1, 100 + i));
                    right.OnNext(CreateInterval(1, 100 + i));
                }

                // Ingress two LWMs after the original interval. This should force partition 0 to output its interval
                left.OnNext(CreateLowWatermark(100));
                right.OnNext(CreateLowWatermark(100));

                // Make sure a dry partition (right) is cleaned/reactivated properly
                left.OnNext(CreatePunctuation(0, 115));
                right.OnNext(CreateEnd(0, 115, 90));
                right.OnNext(CreatePunctuation(0, 115));
                process.Flush();

                left.OnNext(CreateInterval(0, 115));
                left.OnNext(CreateInterval(0, 120));

                right.OnNext(CreateInterval(0, 125));
                process.Flush();

                left.OnNext(CreateLowWatermark(120));
                right.OnNext(CreateLowWatermark(120));

                left.OnNext(CreateInterval(0, 125));

                left.OnCompleted();
                right.OnCompleted();

                var expected = new PartitionedStreamEvent<int, int>[]
                {
                    CreateLowWatermark(50),
                    CreateStart(0, 90),
                    CreateEnd(0, 90 + duration, 90),
                    CreateLowWatermark(100),

                    CreateLowWatermark(120),
                    CreateInterval(0, 125),

                    CreateLowWatermark(StreamEvent.InfinitySyncTime),
                };

                var key0Output = output.Where(e => e.IsLowWatermark || (e.IsData && e.PartitionKey == 0)).ToList();
                Assert.IsTrue(expected.SequenceEqual(key0Output));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void AggressivePartitionCleanup_Clip()
        {
            using (new ConfigModifier().DataBatchSize(4).Modify())
            {
                const long duration = 5;
                PartitionedStreamEvent<int, int> CreateStart(int key, long time) => PartitionedStreamEvent.CreateStart(key, time, key);
                PartitionedStreamEvent<int, int> CreateEnd(int key, long time, long originalStart) => PartitionedStreamEvent.CreateEnd(key, time, originalStart, key);
                PartitionedStreamEvent<int, int> CreateInterval(int key, long time) => PartitionedStreamEvent.CreateInterval(key, time, time + duration, key);
                PartitionedStreamEvent<int, int> CreatePunctuation(int key, long time) => PartitionedStreamEvent.CreatePunctuation<int, int>(key, time);
                PartitionedStreamEvent<int, int> CreateLowWatermark(long time) => PartitionedStreamEvent.CreateLowWatermark<int, int>(time);

                var left = new Subject<PartitionedStreamEvent<int, int>>();
                var right = new Subject<PartitionedStreamEvent<int, int>>();

                var qc = new QueryContainer();
                var leftInput = qc.RegisterInput(left, flushPolicy: PartitionedFlushPolicy.FlushOnLowWatermark);
                var rightInput = qc.RegisterInput(right, flushPolicy: PartitionedFlushPolicy.FlushOnLowWatermark);

                var query = leftInput.ClipEventDuration(
                    rightInput,
                    e => e,
                    e => e);

                var output = new List<PartitionedStreamEvent<int, int>>();
                qc.RegisterOutput(query).ForEachAsync(o => output.Add(o));
                var process = qc.Restore();

                // Set up state so that there is outstanding start edges/intervals
                left.OnNext(CreateInterval(0, 90));
                left.OnNext(CreateLowWatermark(50));

                right.OnNext(CreateInterval(0, 90));
                right.OnNext(CreateLowWatermark(50));

                // Ingress batches without key 0, trying to evict partition 0
                for (int i = 0; i < 10; i++)
                {
                    left.OnNext(CreateInterval(1, 100 + i));
                    right.OnNext(CreateInterval(1, 100 + i));
                }

                // Ingress two LWMs after the original interval. This should force partition 0 to output its end edge
                left.OnNext(CreateLowWatermark(100));
                right.OnNext(CreateLowWatermark(100));

                // Make sure a dry partition is cleaned/reactivated properly
                left.OnNext(CreatePunctuation(0, 115));
                right.OnNext(CreatePunctuation(0, 115));
                process.Flush();

                right.OnNext(CreateInterval(0, 121));

                left.OnNext(CreateLowWatermark(120));
                right.OnNext(CreateLowWatermark(120));

                left.OnNext(CreateInterval(0, 120));

                left.OnCompleted();
                right.OnCompleted();

                var expected = new PartitionedStreamEvent<int, int>[]
                {
                    CreateStart(0, 90),
                    CreateLowWatermark(50),
                    CreateEnd(0, 90 + duration, 90),
                    CreateLowWatermark(100),

                    CreateLowWatermark(120),
                    CreateStart(0, 120),
                    CreateEnd(0, 121, 120),
                    CreateLowWatermark(StreamEvent.InfinitySyncTime),
                };

                var key0Output = output.Where(e => e.IsLowWatermark || (e.IsData && e.PartitionKey == 0)).ToList();
                Assert.IsTrue(expected.SequenceEqual(key0Output));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void OutOfOrderRepro()
        {
            var qc = new QueryContainer();
            var input = new Subject<PartitionedStreamEvent<int, int>>();
            var ingress = qc.RegisterInput(input, DisorderPolicy.Drop(reorderLatency: 500));
            var output = new List<PartitionedStreamEvent<int, int>>();
            var egress = qc.RegisterOutput(ingress).ForEachAsync(o => output.Add(o));
            var process = qc.Restore();

            // These will be buffered due to reorderLatency
            input.OnNext(PartitionedStreamEvent.CreatePoint(0, 1, 0));    // key 0: 1-2
            input.OnNext(PartitionedStreamEvent.CreatePoint(0, 101, 0));  // key 0: 101-102

            // This will egress the first point (0: 1-2)); but the second point ( key 0: 101-102) should batched, since 101 > 100.
            input.OnNext(PartitionedStreamEvent.CreateLowWatermark<int, int>(100));

            // This should be dropped
            input.OnNext(PartitionedStreamEvent.CreatePoint(0, 99, 0));

            input.OnCompleted();

            var expected = new PartitionedStreamEvent<int, int>[]
            {
                PartitionedStreamEvent.CreatePoint(0, 1, 0),
                PartitionedStreamEvent.CreateLowWatermark<int, int>(100),
                PartitionedStreamEvent.CreatePoint(0, 101, 0),
                PartitionedStreamEvent.CreateLowWatermark<int, int>(StreamEvent.InfinitySyncTime),
            };
            Assert.IsTrue(expected.SequenceEqual(output));
        }
    }

    [TestClass]
    public class AdHocWithoutMemoryLeakDetection : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public AdHocWithoutMemoryLeakDetection() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true))
        { }

        [TestMethod, TestCategory("Gated")]
        public async Task DisposeTest1()
        {
            var cancelTokenSource = new CancellationTokenSource();
            var inputSubject = new Subject<int>();
            var lastSeenSubscription = 0;
            var observableInput = inputSubject.AsObservable();

            var inputTask = new Task(() =>
            {
                var n = 0;
                while (!cancelTokenSource.Token.IsCancellationRequested)
                {
                    inputSubject.OnNext(++n);
                    Thread.Sleep(10);
                }
                inputSubject.OnCompleted();
            });

            var semaphore = new SemaphoreSlim(0, 1);
            var subscription = observableInput.ToAtemporalStreamable(TimelinePolicy.Sequence(1)).Count().ToStreamEventObservable().Where(c => c.IsData).Subscribe(c =>
            {
                Interlocked.Exchange(ref lastSeenSubscription, (int)c.Payload);
                if (semaphore.CurrentCount == 0) semaphore.Release();
            });

            // Start the input feed.
            inputTask.Start();

            // Wait until we have at least one output data event.
            await semaphore.WaitAsync();

            // Dispose the subscription.
            subscription.Dispose();

            // Keep the input feed going, before cancel. This should behave properly if the subscription is disposed of properly.
            await Task.Delay(200);
            cancelTokenSource.Cancel();
            await inputTask;

            // Make sure we really got an output data event.
            Assert.IsTrue(lastSeenSubscription > 0);
        }
    }

    [TestClass]
    public class PublishTests : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public PublishTests() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(false)
            .MapArity(1)
            .ReduceArity(1))
        { }

        [TestMethod, TestCategory("Gated")]
        public void PublishTest1()
        {
            var inputSubject = new Subject<StreamEvent<int>>();

            var outputList1 = new List<int>();
            var outputList2 = new List<int>();

            var preConnectData = Enumerable.Range(0, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            var postConnectData = Enumerable.Range(10000, 10000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(1, e));

            var fullData = Enumerable.Range(0, 20000).ToList()
                .ToObservable()
                .Select(e => StreamEvent.CreateStart(0, e));

            var input = inputSubject.ToStreamable().SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime).Publish();

            var output1 = input.ToStreamEventObservable().Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(x => outputList1.Add(x));

            input.Connect();

            preConnectData.ForEachAsync(x => inputSubject.OnNext(x)).Wait();
            inputSubject.OnNext(StreamEvent.CreatePunctuation<int>(1));

            var output2 = input.ToStreamEventObservable().Where(e => e.IsData).Select(e => e.Payload).ForEachAsync(x => outputList2.Add(x));

            postConnectData.ForEachAsync(x => inputSubject.OnNext(x)).Wait();

            inputSubject.OnCompleted();

            Assert.IsTrue(outputList1.SequenceEqual(Enumerable.Range(0, 20000).ToList()));
            Assert.IsTrue(outputList2.SequenceEqual(Enumerable.Range(10000, 10000).ToList()));
        }
    }

    [TestClass]
    public class PointEventRegressionTest : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public PointEventRegressionTest() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        public struct NormalizedHttpEvent
        {
            public long Timestamp;
            public string TrimmedHost;
            public IPAddress IpSource;
        }

        [TestMethod, TestCategory("Gated")]
        public void PointEventRegression() // this test is from a user: it crashed codegen because of a duplicate assembly reference being passed to the C# compiler for System.dll
        {
            var inputObservable = new[]
            {
                new NormalizedHttpEvent() { Timestamp = 3, TrimmedHost = "msn.com", IpSource = IPAddress.Parse("157.59.185.162") },
                new NormalizedHttpEvent() { Timestamp = 7, TrimmedHost = "msn.com", IpSource = IPAddress.Parse("157.59.0.162") },
                new NormalizedHttpEvent() { Timestamp = 10, TrimmedHost = "msn.com", IpSource = IPAddress.Parse("157.59.185.162") },
                new NormalizedHttpEvent() { Timestamp = 13, TrimmedHost = "msn.com", IpSource = IPAddress.Parse("157.59.185.162") },
                new NormalizedHttpEvent() { Timestamp = 16, TrimmedHost = "msn.com", IpSource = IPAddress.Parse("157.59.185.162") },
                new NormalizedHttpEvent() { Timestamp = 20, TrimmedHost = "msn.com", IpSource = IPAddress.Parse("157.59.185.162") },
                new NormalizedHttpEvent() { Timestamp = 22, TrimmedHost = "test.cloudapp.net", IpSource = IPAddress.Parse("157.59.185.162") },
                new NormalizedHttpEvent() { Timestamp = 25, TrimmedHost = "bing.com", IpSource = IPAddress.Parse("157.59.185.162") },
                new NormalizedHttpEvent() { Timestamp = 27, TrimmedHost = "bing.com", IpSource = IPAddress.Parse("157.59.185.162") },
                new NormalizedHttpEvent() { Timestamp = 41, TrimmedHost = "bing.com", IpSource = IPAddress.Parse("157.59.185.162") },
                new NormalizedHttpEvent() { Timestamp = 45, TrimmedHost = "msn.com", IpSource = IPAddress.Parse("157.59.185.162") },
                new NormalizedHttpEvent() { Timestamp = 49, TrimmedHost = "msn.com", IpSource = IPAddress.Parse("157.59.185.162") },
                new NormalizedHttpEvent() { Timestamp = 53, TrimmedHost = "msn.com", IpSource = IPAddress.Parse("157.59.185.162") },
            }.ToObservable();

            var input = inputObservable.Select(e => StreamEvent.CreateStart(e.Timestamp, e)).ToStreamable(DisorderPolicy.Drop());

            var countedNE = input.GroupApply(
                e => new { e.TrimmedHost, e.IpSource },
                s => s.HoppingWindowLifetime(10, 10).Count(),
                (g, p) => new { g.Key.TrimmedHost, g.Key.IpSource, Count = p });

            countedNE = countedNE.AlterEventDuration(1);

            var output2 = countedNE.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).ToEnumerable().ToArray();
            Assert.IsTrue(output2.Count() == 9);
        }
    }

    [TestClass]
    public class LeftOuterJoinMacro : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public LeftOuterJoinMacro() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true))
        { }

        [DataContract]
        public struct InputEvent0
        {
            [DataMember]
            public DateTime Ts;
            [DataMember]
            public string VmId;
            [DataMember]
            public int Status;
            [DataMember]
            public string IncId;
        }

        [DataContract]
        public struct InputEvent1
        {
            [DataMember]
            public DateTime? Ts1;
            [DataMember]
            public string VmId2;
            [DataMember]
            public int? Status3;
            [DataMember]
            public string IncId4;
        }

        [DataContract]
        public struct OutputEvent0
        {
            [DataMember]
            public DateTime Ts;
            [DataMember]
            public string VmId;
            [DataMember]
            public int Status;
            [DataMember]
            public string IncId;
            [DataMember]
            public DateTime? Ts1;
            [DataMember]
            public string VmId2;
            [DataMember]
            public int? Status3;
            [DataMember]
            public string IncId4;
        }
        [TestMethod, TestCategory("Gated")]
        public void LeftOuterJoinScenario()
        {
            var savedForceRowBasedExecution = Config.ForceRowBasedExecution;
            try
            {
                Config.ForceRowBasedExecution = true;

                var input1Enum = new StreamEvent<InputEvent0>[]
            {
                StreamEvent.CreatePoint(10, new InputEvent0()
                {
                    Ts = DateTime.UtcNow,
                    VmId = "vm1",
                    Status = 0,
                    IncId = "000"
                }),
            };

                var input2Enum = new StreamEvent<InputEvent1>[]
            {
                StreamEvent.CreatePoint(10, new InputEvent1()
                {
                    Ts1 = DateTime.UtcNow,
                    VmId2 = "vm2",
                    Status3 = 1,
                    IncId4 = "100"
                }),
            };

                var input1 = input1Enum.ToObservable().ToStreamable();
                var input2 = input2Enum.ToObservable().ToStreamable();

                var result = input1
                    .LeftOuterJoin(
                        input2,
                        s => s.VmId,
                        s => s.VmId2,
                        e1 => new OutputEvent0()
                        {
                            Ts = e1.Ts,
                            VmId = e1.VmId,
                            Status = e1.Status,
                            IncId = e1.IncId,
                            Ts1 = null,
                            VmId2 = null,
                            Status3 = null,
                            IncId4 = null,
                        },
                        (e1, e2) => new OutputEvent0()
                        {
                            Ts = e1.Ts,
                            VmId = e1.VmId,
                            Status = e1.Status,
                            IncId = e1.IncId,
                            Ts1 = e2.Ts1,
                            VmId2 = e2.VmId2,
                            Status3 = e2.Status3,
                            IncId4 = e2.IncId4,
                        });

                var resultObs = result.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges);

                foreach (var x in resultObs.ToEnumerable())
                {
                    Console.WriteLine("{0}, {1}, {2}, {3}", x.Kind, x.StartTime, x.EndTime, x.Payload);
                }
            }
            finally
            {
                Config.ForceRowBasedExecution = savedForceRowBasedExecution;
            }
        }

    }

    [TestClass]
    public class FullOuterJoinMacro : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public FullOuterJoinMacro() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true))
        { }

        [DataContract]
        public struct InputEvent0
        {
            [DataMember]
            public DateTime Ts;
            [DataMember]
            public string VmId;
            [DataMember]
            public int Status;
            [DataMember]
            public string IncId;
        }

        [DataContract]
        public struct InputEvent1
        {
            [DataMember]
            public DateTime? Ts1;
            [DataMember]
            public string VmId2;
            [DataMember]
            public int? Status3;
            [DataMember]
            public string IncId4;
        }

        [DataContract]
        public struct OutputEvent0
        {
            // Left
            [DataMember]
            public DateTime? Ts;
            [DataMember]
            public string VmId;
            [DataMember]
            public int? Status;
            [DataMember]
            public string IncId;

            // Right
            [DataMember]
            public DateTime? Ts1;
            [DataMember]
            public string VmId2;
            [DataMember]
            public int? Status3;
            [DataMember]
            public string IncId4;

            public override string ToString()
            {
                return new
                {
                    this.Ts,
                    this.VmId,
                    this.Status,
                    this.IncId,
                    this.Ts1,
                    this.VmId2,
                    this.Status3,
                    this.IncId4,
                }.ToString();
            }
        }
        [TestMethod, TestCategory("Gated")]
        public void FullOuterJoinScenario()
        {
            var savedForceRowBasedExecution = Config.ForceRowBasedExecution;
            try
            {
                Config.ForceRowBasedExecution = true;

                var input1Enum = new StreamEvent<InputEvent0>[]
                {
                    StreamEvent.CreateInterval(10, 20, new InputEvent0()
                    {
                        Ts = DateTime.UtcNow,
                        VmId = "vm1",
                        Status = 0,
                        IncId = "000"
                    }),
                };
                var input2Enum = new StreamEvent<InputEvent1>[]
                {
                    StreamEvent.CreateInterval(15, 25, new InputEvent1()
                    {
                        Ts1 = DateTime.UtcNow,
                        VmId2 = "vm1",
                        Status3 = 1,
                        IncId4 = "100"
                    }),
                };

                var input1 = input1Enum.ToObservable().ToStreamable();
                var input2 = input2Enum.ToObservable().ToStreamable();

                var result = input1
                    .FullOuterJoin(
                        right: input2,
                        leftKeySelector: s => s.VmId,
                        rightKeySelector: s => s.VmId2,
                        leftResultSelector: l => new OutputEvent0()
                        {
                            Ts = l.Ts,
                            VmId = l.VmId,
                            Status = l.Status,
                            IncId = l.IncId,
                            Ts1 = null,
                            VmId2 = null,
                            Status3 = null,
                            IncId4 = null,
                        },
                        rightResultSelector: r => new OutputEvent0()
                        {
                            Ts = null,
                            VmId = null,
                            Status = null,
                            IncId = null,
                            Ts1 = r.Ts1,
                            VmId2 = r.VmId2,
                            Status3 = r.Status3,
                            IncId4 = r.IncId4,
                        },
                        innerResultSelector: (l, r) => new OutputEvent0()
                        {
                            Ts = l.Ts,
                            VmId = l.VmId,
                            Status = l.Status,
                            IncId = l.IncId,
                            Ts1 = r.Ts1,
                            VmId2 = r.VmId2,
                            Status3 = r.Status3,
                            IncId4 = r.IncId4,
                        });

                var resultObs = result.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges);

                foreach (var x in resultObs.ToEnumerable())
                {
                    System.Diagnostics.Debug.Print("{0}, {1}, {2}, {3}", x.Kind, x.StartTime, x.EndTime, x.Payload);
                }
                /*
                    Interval, 10, 15, { Ts = 5/14/2019 1:39:20 AM, VmId = vm1, Status = 0, IncId = 000, Ts1 = , VmId2 = , Status3 = , IncId4 =  }                                   LEFT
                    Interval, 15, 20, { Ts = 5/14/2019 1:39:20 AM, VmId = vm1, Status = 0, IncId = 000, Ts1 = 5/14/2019 1:39:20 AM, VmId2 = vm1, Status3 = 1, IncId4 = 100 }        INNER
                    Interval, 20, 25, { Ts = , VmId = , Status = , IncId = , Ts1 = 5/14/2019 1:39:20 AM, VmId2 = vm1, Status3 = 1, IncId4 = 100 }                                   RIGHT
                    Punctuation, 3155378975999999999, -9223372036854775808, { Ts = , VmId = , Status = , IncId = , Ts1 = , VmId2 = , Status3 = , IncId4 =  }
                */
            }
            finally
            {
                Config.ForceRowBasedExecution = savedForceRowBasedExecution;
            }
        }
    }

    [TestClass]
    public class LeftOuterJoinMacroButWithoutMemoryLeakDetection : TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        public LeftOuterJoinMacroButWithoutMemoryLeakDetection() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true))
        { }

        [DataContract]
        public struct InputEvent
        {
            [DataMember]
            public string ClusterName;
            [DataMember]
            public int IntValue;
        }

        [DataContract]
        public struct OutputEvent
        {
            [DataMember]
            public string ClusterName;
        }

        private class QueryProcessor : IDisposable
        {
            private IObserver<StreamEvent<InputEvent>> observer;
            private readonly Process process = null;
            private readonly Queue<StreamEvent<OutputEvent>> results = new Queue<StreamEvent<OutputEvent>>();
            private readonly QueryContainer container = new QueryContainer();

            private void CommonInitialize()
            {
                var inputObs = Observable.Create((Func<IObserver<StreamEvent<InputEvent>>, IDisposable>)InitializeStream);
                var inputEvents = this.container.RegisterInput(inputObs, DisorderPolicy.Adjust(),
                    FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.Flush);
                var result = Query(inputEvents);
                var resultObs = this.container.RegisterOutput(result);

                resultObs.Subscribe(x =>
                {
                    this.results.Enqueue(x);
                });
            }

            internal QueryProcessor()
            {
                CommonInitialize();
                this.process = this.container.Restore();
            }

            internal QueryProcessor(bool fromCheckpoint)
            {
                CommonInitialize();
                string fname = Path.Combine(@"chk.bin");
                using (var stream = new FileStream(fname, FileMode.Open))
                {
                    this.process = this.container.Restore(stream);
                }
            }

            private IDisposable InitializeStream(IObserver<StreamEvent<InputEvent>> obs)
            {
                this.observer = obs;
                return this;
            }

            public void Dispose() => this.observer = null;

            internal void Checkpoint()
            {
                string fname = Path.Combine(@"chk.bin");
                using (var stream = new FileStream(fname, FileMode.Create))
                {
                    this.process.Checkpoint(stream);
                }
            }

            internal void SendEvent(StreamEvent<InputEvent> evt) => this.observer.OnNext(evt);

            internal void Flush() => this.observer.OnCompleted();

            private IStreamable<Empty, OutputEvent> Query(IObservableIngressStreamable<InputEvent> input)
            {
                var result =
                    input
                    .Distinct(r => new OutputEvent()
                    {
                        ClusterName = r.ClusterName,
                    });

                return result;
            }

        }

        private static IEnumerable<StreamEvent<InputEvent>> ReadSource()
        {
            var startTime = DateTime.Parse("2015-10-05T19:00:00.0Z");

            for (long i = 0; i < 50000; i++)
            {
                string clusterName = (i % 15).ToString();
                var evt = new InputEvent() { ClusterName = clusterName, IntValue = 0, };

                var start = startTime.AddMilliseconds(i * 111); // we want to cover about one hour with about 50,000 events --> 111 milliseconds
                var end = start.AddMinutes(60);
                bool isPunc = i % 5000 == 0;

                yield return isPunc
                    ? StreamEvent.CreatePunctuation<InputEvent>(start.Ticks)
                    : StreamEvent.CreateInterval(start.Ticks, end.Ticks, evt);
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void LeftJoinOutOfOrder()
        {
            var s1 = new Subject<PartitionedStreamEvent<string, string>>();
            var s2 = new Subject<PartitionedStreamEvent<string, string>>();

            var qc = new QueryContainer();
            var output = new List<PartitionedStreamEvent<string, string>>();

            var input1 = qc.RegisterInput(s1);
            var input2 = qc.RegisterInput(s2);

            var egress = qc.RegisterOutput(input1
                                .LeftOuterJoin(
                                    input2,
                                    s => s,
                                    s => s,
                                    e1 => e1,
                                    (e1, e2) => null)).ForEachAsync(o => output.Add(o));
            var process = qc.Restore();

            // This test used to rely on lagAllowance=50 and (Global)PeriodicPunctuationPolicy=10 to generate global punctuations and flush contents.
            // Now that lagAllowance and (Global)PeriodicPunctuationPolicy are removed, use LowWatermarks to reproduce the same scenario.
            s1.OnNext(PartitionedStreamEvent.CreatePunctuation<string, string>("c1", 5));
            s2.OnNext(PartitionedStreamEvent.CreatePunctuation<string, string>("c1", 5));

            s2.OnNext(PartitionedStreamEvent.CreateLowWatermark<string, string>(11));   // 61-lagAllowance:50=11
            s2.OnNext(PartitionedStreamEvent.CreatePunctuation<string, string>("c1", 61));

            s1.OnNext(PartitionedStreamEvent.CreateInterval("k1", 10, 30, "v1"));

            s1.OnNext(PartitionedStreamEvent.CreateLowWatermark<string, string>(11));   // 61-lagAllowance:50=11
            s1.OnNext(PartitionedStreamEvent.CreatePunctuation<string, string>("c1", 61));

            s1.OnNext(PartitionedStreamEvent.CreateLowWatermark<string, string>(450));  // 500-lagAllowance:50=450
            s1.OnNext(PartitionedStreamEvent.CreatePunctuation<string, string>("c1", 500));
            s1.OnNext(PartitionedStreamEvent.CreateLowWatermark<string, string>(950));  // 1000-lagAllowance:50=950
            s1.OnNext(PartitionedStreamEvent.CreatePunctuation<string, string>("c1", 1000));

            s2.OnNext(PartitionedStreamEvent.CreateLowWatermark<string, string>(450));  // 500-lagAllowance:50=450
            s2.OnNext(PartitionedStreamEvent.CreatePunctuation<string, string>("c1", 500));
            s2.OnNext(PartitionedStreamEvent.CreateLowWatermark<string, string>(949));  // 500-lagAllowance:50=949
            s2.OnNext(PartitionedStreamEvent.CreatePunctuation<string, string>("c1", 999));

            process.Flush();

            var outputData = output.Where(o => o.IsData).ToList();
            Assert.IsTrue(outputData.Count == 2);
        }

        [TestMethod, TestCategory("Gated")]
        public void SerializerRegressionTest()
        {
            using (var modifier = new ConfigModifier().ForceRowBasedExecution(true).Modify())
            {
                var processor = new QueryProcessor();
                var inputEvents = ReadSource();

                int cnt = 0;
                foreach (var x in inputEvents)
                {
                    if (cnt++ == 45000)
                    {
                        Console.WriteLine("Taking a checkpoint after {0} input events", cnt);
                        processor.Checkpoint();
                        processor.Flush();

                        Console.WriteLine("Restoring from the checkpoint");
                        processor = new QueryProcessor(true);
                        Console.WriteLine("Done restoring");
                    }

                    if (cnt % 5000 == 0) Console.WriteLine(cnt);

                    processor.SendEvent(x);
                }
                processor.Flush();
            }
        }
    }

    [TestClass]
    public class StartEdgeTests : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public StartEdgeTests() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .UseMultiString(false)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations))
        { }

        public struct S
        {
            public string FilePath { get; set; }
        }

        [TestMethod, TestCategory("Gated")]
        public void StartEdge1()
        {

            var inputEnumerable = new[]
            {
                StreamEvent.CreateStart(9900, 100),
                StreamEvent.CreateStart(10000, 200),
                StreamEvent.CreateStart(10020, 300),
                StreamEvent.CreateStart(10025, 400),
                StreamEvent.CreateStart(10030, 500),
                StreamEvent.CreateStart(10040, 600)
            };
            var inputStream = inputEnumerable.ToObservable().ToStreamable();
            var outputStream = inputStream.GroupApply(
                e => new S()
                {
                    FilePath = e.ToString(),
                },
                s => s
                    .HoppingWindowLifetime(100000, 100000)
                    .Count(),
                (g, result) => new { K = g, P = result, });
            var events = outputStream.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .ToEnumerable()
                .ToArray();

            var x = events.Length;
            Assert.IsTrue(x == inputEnumerable.Count());
        }
    }

    [TestClass]
    public class SelfJoinTests : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public class SomeClass : IEqualityComparerExpression<SomeClass>
        {
            public long Timestamp { get; set; }
            public long CycleStart { get; set; }
            public uint BC { get; set; }

            public Expression<Func<SomeClass, SomeClass, bool>> GetEqualsExpr()
                => (sc1, sc2) => sc1.Timestamp == sc2.Timestamp && sc1.CycleStart == sc2.CycleStart && sc1.BC == sc2.BC;

            public Expression<Func<SomeClass, int>> GetGetHashCodeExpr()
                => sc => sc.Timestamp.GetHashCode() ^ sc.CycleStart.GetHashCode() ^ sc.BC.GetHashCode();
        }

        private static long GetDateTimeTicks(int year, int month, int day, int hours, int minutes, int seconds)
            => new DateTime(year, month, day, hours, minutes, seconds).Ticks;

        [TestMethod, TestCategory("Gated")]
        public void What_is_wrong()
        {
            var rollup = new[]
                    {
                        StreamEvent.CreateInterval
                        (
                            GetDateTimeTicks(2009, 06, 25, 12, 01, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 03, 0),
                            new SomeClass { Timestamp = 4, CycleStart = 3, BC = 5 }),
                        StreamEvent.CreateInterval
                        (
                            GetDateTimeTicks(2009, 06, 25, 12, 04, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 14, 0),
                            new SomeClass { Timestamp = 5, CycleStart = 3, BC = 6 }),
                        StreamEvent.CreateInterval
                        (
                            GetDateTimeTicks(2009, 06, 25, 12, 18, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 20, 0),
                            new SomeClass { Timestamp = 7, CycleStart = 3, BC = 4 }),
                        StreamEvent.CreatePunctuation<SomeClass>(StreamEvent.InfinitySyncTime)
                    }.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);

            var battcap = new Dictionary<long, uint>();
            rollup.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ForEachAsync(
                e =>
                {
                    battcap[e.Payload.Timestamp] = e.Payload.BC;
                }).Wait();

            var k = rollup.Multicast(
                s => s.AlterEventDuration(StreamEvent.InfinitySyncTime)
                .Multicast(s2 => s2.ClipEventDuration(s2))
                .ShiftEventLifetime(TimeSpan.TicksPerSecond)
                .Join(
                    s,
                    (l, r) => new
                    {
                        l.Timestamp,
                        r.CycleStart,
                        r.BC,
                        RecentRate = (l.Timestamp > r.CycleStart ? ((double)(((long)r.BC - (long)l.BC) * TimeSpan.TicksPerHour)) / (r.Timestamp - l.Timestamp) : 0),
                        CycleRate = (r.Timestamp > r.CycleStart ? ((double)(((long)r.BC - (long)battcap.Where(kvp => kvp.Key <= r.Timestamp).OrderBy(kvp => kvp.Key).Last().Value) * TimeSpan.TicksPerHour)) / (r.Timestamp - r.CycleStart) : 0),
                    }));

            var output = k.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();
            Assert.IsTrue(output != null && output.Length == 5);
        }

        /// <summary>
        /// Same as what_is_wrong, but with the query modified to allow codegen for
        /// EquiJoin
        /// </summary>
        [TestMethod, TestCategory("Gated")]
        public void What_is_wrong2()
        {
            var rollup = new[]
                    {
                        StreamEvent.CreateInterval
                        (
                            GetDateTimeTicks(2009, 06, 25, 12, 01, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 03, 0),
                            new SomeClass { Timestamp = 4, CycleStart = 3, BC = 5 }),
                        StreamEvent.CreateInterval
                        (
                            GetDateTimeTicks(2009, 06, 25, 12, 04, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 14, 0),
                            new SomeClass { Timestamp = 5, CycleStart = 3, BC = 6 }),
                        StreamEvent.CreateInterval
                        (
                            GetDateTimeTicks(2009, 06, 25, 12, 18, 0),
                            GetDateTimeTicks(2009, 06, 25, 12, 20, 0),
                            new SomeClass { Timestamp = 7, CycleStart = 3, BC = 4 }),
                        StreamEvent.CreatePunctuation<SomeClass>(StreamEvent.InfinitySyncTime)
                    }.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None);

            var k = rollup.Multicast(
                s => s.AlterEventDuration(StreamEvent.InfinitySyncTime).
                Multicast(s2 => s2.ClipEventDuration(s2)).
                ShiftEventLifetime(TimeSpan.TicksPerSecond).
                Join(
                    s,
                    (l, r) => new
                    {
                        l.Timestamp,
                        r.CycleStart,
                        r.BC,
                        RecentRate = (l.Timestamp > r.CycleStart ? ((double)(((long)r.BC - (long)l.BC) * TimeSpan.TicksPerHour)) / (r.Timestamp - l.Timestamp) : 0),
                    }));

            var output = k.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).Where(e => e.IsData).ToEnumerable().ToArray();
            Assert.IsTrue(output != null && output.Length == 5);
        }
    }

    [TestClass]
    public class JoinAndUnionTests : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public JoinAndUnionTests()
            : base(new ConfigModifier()
                .ForceRowBasedExecution(false)
                .DontFallBackToRowBasedExecution(true))
        {
        }

        public class UlsSessionBootStartEvent
        {
            public DateTime Timestamp;
            public Guid SessionID;
            public override string ToString() => new { this.Timestamp, this.SessionID }.ToString();
        }

        public class UlsSessionBootFinishedEvent
        {
            public DateTime Timestamp;
            public Guid SessionID;
            public override string ToString() => new { this.Timestamp, this.SessionID }.ToString();
        }

        [TestMethod]
        public void JoinClipTest()
        {
            var inputDataStarts = new[]
                    {
                           new UlsSessionBootStartEvent { Timestamp = new DateTime(2015, 5, 13, 2, 0, 20, 00, DateTimeKind.Utc), SessionID = new Guid("9B498314-DB27-4B70-ABBB-922697E15DA7") },
                           new UlsSessionBootStartEvent { Timestamp = new DateTime(2015, 5, 13, 2, 0, 20, 00, DateTimeKind.Utc), SessionID = new Guid("698B7CC5-DCBB-4820-AF4C-F80D51860B3A") },
                           new UlsSessionBootStartEvent { Timestamp = new DateTime(2015, 5, 13, 2, 0, 20, 00, DateTimeKind.Utc), SessionID = new Guid("65BA17E1-7BC8-4BD7-8C49-47234F7F559D") }
                    };

            var inputDataFinishes = new[]
                    {
                           new UlsSessionBootFinishedEvent { Timestamp = new DateTime(2015, 5, 13, 2, 1, 10, 00, DateTimeKind.Utc), SessionID = new Guid("9B498314-DB27-4B70-ABBB-922697E15DA7") },
                           new UlsSessionBootFinishedEvent { Timestamp = new DateTime(2015, 5, 13, 2, 2, 25, 00, DateTimeKind.Utc), SessionID = new Guid("698B7CC5-DCBB-4820-AF4C-F80D51860B3A") }
                    };

            var inputStreamStarts =
                   inputDataStarts
                         .Select(e => StreamEvent.CreatePoint(e.Timestamp.Ticks, e))
                         .ToObservable()
                         .ToStreamable();

            var inputStreamFinishes =
                   inputDataFinishes
                         .Select(e => StreamEvent.CreatePoint(e.Timestamp.Ticks, e))
                         .ToObservable()
                         .ToStreamable();

            try
            {
                var successes = inputStreamStarts
                       .AlterEventDuration(TimeSpan.FromMinutes(2).Ticks)
                       .ClipEventDuration(inputStreamFinishes, e => e.SessionID, e => e.SessionID)
                       .ShiftEventLifetime(1)
                       .Join(inputStreamFinishes, e => e.SessionID, e => e.SessionID,
                             (left, right) => new
                             {
                                 left.SessionID,
                                 Success = true
                             });

                var result = successes.ToStreamEventObservable().ToEnumerable().ToArray();
                Assert.IsTrue(false); // should never reach here.
            }
            catch (Exception e)
            {
                Assert.IsTrue(e is InvalidOperationException);
            }
        }

        /// <summary>
        /// The purpose of this test is to get a ConnectableStreamable that calls CloneFrom on a generated
        /// batch but passing it a generic batch. That tests the codegen for CloneFrom.
        /// </summary>
        [TestMethod]
        public void UnionWithOneColumnarAndOneNot01()
        {
            var input = Enumerable.Range(0, 3)
                ;
            var rowStream = input
                .ToObservable()
                .Select(i => StreamEvent.CreateStart(0, i))
                .ToStreamable()
                .ColumnToRow()
                ;
            var colStream = input
                .ToObservable()
                .Select(i => StreamEvent.CreateStart(0, i))
                .ToStreamable()
                ;
            var streamResult = colStream.Union(rowStream)
                .Publish();

            var outputList = new List<int>();

            var output1 = streamResult
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.Payload)
                .ForEachAsync(x => outputList.Add(x));

            streamResult.Connect();

            var expected = input.Concat(input);
            Assert.IsTrue(expected.SequenceEqual(outputList));
        }

        /// <summary>
        /// The purpose of this test is to get a ConnectableStreamable that calls CloneFrom on a generated
        /// batch but passing it a generic batch. That tests the codegen for CloneFrom.
        /// </summary>
        [TestMethod]
        public void UnionWithOneColumnarAndOneNot02()
        {
            var input = Enumerable.Range(0, 3)
                .Select(i => new MyStruct2 { field1 = i, })
                ;
            var rowStream = input
                .ToObservable()
                .Select(i => StreamEvent.CreateStart(0, i))
                .ToStreamable()
                .ColumnToRow()
                ;
            var colStream = input
                .ToObservable()
                .Select(i => StreamEvent.CreateStart(0, i))
                .ToStreamable()
                ;
            var streamResult = colStream.Union(rowStream)
                .Publish();

            var outputList = new List<MyStruct2>();

            var output1 = streamResult
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.Payload)
                .ForEachAsync(x => outputList.Add(x));

            streamResult.Connect();

            var expected = input.Concat(input);
            Assert.IsTrue(expected.SequenceEqual(outputList));
        }

        public class ClassWithAutoProps : IEqualityComparerExpression<ClassWithAutoProps>
        {
            public int IntField;
            public int IntAutoProp { get; set; }
            public override bool Equals(object obj)
                => !(obj is ClassWithAutoProps other) ? false : this.IntAutoProp == other.IntAutoProp && this.IntField == other.IntField;
            public override int GetHashCode() => this.IntAutoProp.GetHashCode() ^ this.IntField.GetHashCode();

            public Expression<Func<ClassWithAutoProps, ClassWithAutoProps, bool>> GetEqualsExpr()
                => (c1, c2) => c1.IntAutoProp == c2.IntAutoProp && c1.IntField == c2.IntField;

            public Expression<Func<ClassWithAutoProps, int>> GetGetHashCodeExpr() => c => c.IntAutoProp ^ c.IntField;
        }

        [TestMethod, TestCategory("Gated")]
        public void Select8()
        {
            // a selector that does a "fan-out" to a type R that has autoprops
            var input = Enumerable.Range(0, 10000)
                .Select(e => new StructTuple<ClassWithAutoProps, int> { Item1 = new ClassWithAutoProps() { IntAutoProp = e, }, Item2 = e })
                ;
            var streamResult = input
                .ToStreamable()
                .Select(e => e.Item1)
                ;
            var expected = input
                .Select(e => e.Item1)
                ;
            var output = streamResult
                .ToTemporalObservable((s, e, p) => p)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.Count() == output.Length);
            for (int i = 0; i < output.Length; i++)
                Assert.IsTrue(expected.ElementAt(i).Equals(output[i]));
        }

        [TestMethod, TestCategory("Gated")]
        public void Select9()
        {
            // a selector that does a simple member access (e => e.f) where f is a property, not a field
            var input = Enumerable.Range(0, 10000)
                .Select(e => new { A = new ClassWithAutoProps() { IntAutoProp = e, }, })
                ;
            var streamResult = input
                .ToStreamable()
                .Select(e => e.A)
                ;
            var expected = input
                .Select(e => e.A)
                ;
            var output = streamResult
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges)
                .Where(e => e.IsData)
                .Select(se => se.Payload)
                .ToEnumerable()
                .ToArray()
                ;
            Assert.IsTrue(expected.SequenceEqual(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnionWithBatchEndingInPastPunctuation()
        {
            Helpers.RunTwiceForRowAndColumnar(() =>
            {
                var leftInput = new StreamEvent<int>[]
                {
                        StreamEvent.CreateInterval(100, 101, 0),
                        StreamEvent.CreatePunctuation<int>(50)
                }.ToObservable().ToStreamable();
                var rightInput = new StreamEvent<int>[]
                {
                        StreamEvent.CreateInterval(50, 51, 0),
                        StreamEvent.CreatePunctuation<int>(200)
                }.ToObservable().ToStreamable();

                var query = leftInput.Union(rightInput);

                var output = new List<StreamEvent<int>>();
                query.ToStreamEventObservable()
                    .ForEachAsync(x => output.Add(x))
                    .Wait();

                var expected = new List<StreamEvent<int>>
                    {
                        StreamEvent.CreateInterval(50, 51, 0),
                        StreamEvent.CreateInterval(100, 101, 0),
                        StreamEvent.CreatePunctuation<int>(100),
                        StreamEvent.CreatePunctuation<int>(200),
                        StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
                    };

                Assert.IsTrue(expected.SequenceEqual(output));
            });
        }
    }

    [TestClass]
    public class LeftComparerPayload_WithCodegen : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public LeftComparerPayload_WithCodegen()
            : base(new ConfigModifier().DontFallBackToRowBasedExecution(true)) { }

        public class ClassOverridingEquals
        {
            public int x;

            public override bool Equals(object obj) => obj is ClassOverridingEquals other && other.x == this.x;
            public override int GetHashCode() => this.x.GetHashCode();
        }

        /// <summary>
        /// This test has a left comparer which has a reference to the left
        /// payload instead of just to its fields. This causes the streamable
        /// to thrown an exception.
        /// </summary>
        [TestMethod]
        public void JoinTestWithException()
        {
            var stream1 = Enumerable.Range(0, 3)
                .ToObservable()
                .Select(i => StreamEvent.CreateStart(i, new ClassOverridingEquals() { x = i }))
                .ToStreamable()
                ;

            var stream2 = Enumerable.Range(0, 3)
                .ToObservable()
                .Select(i => StreamEvent.CreateStart(i, i))
                .ToStreamable()
                ;

            bool exceptionHappened = false;
            try
            {
                var result = stream1
                       .Join(stream2, e => e.x, e => e, (left, right) => new { LeftX = left.x, RightX = right, })
                       .ToStreamEventObservable()
                       .ToEnumerable()
                       .ToArray();
            }
            catch (StreamProcessingException)
            {
                exceptionHappened = true;
            }
            Assert.IsTrue(exceptionHappened);
        }

    }

    [TestClass]
    public class LeftComparerPayload_WithOutCodegen : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public LeftComparerPayload_WithOutCodegen() : base(new ConfigModifier()
            .ForceRowBasedExecution(true))
        { }

        public class ClassOverridingEquals
        {
            public int x;

            public override bool Equals(object obj) => obj is ClassOverridingEquals other && other.x == this.x;

            public override int GetHashCode() => this.x.GetHashCode();
        }

        /// <summary>
        /// This test has a left comparer which has a reference to the left
        /// payload instead of just to its fields. This results in a comparer
        /// that has to reconstitute the payload for each comparison, when in columnar
        /// mode. This test is just to make sure the row-mode behaves the same.
        /// </summary>
        [TestMethod]
        public void JoinTestWithoutException()
        {
            var stream1 = Enumerable.Range(0, 3)
                .ToObservable()
                .Select(i => StreamEvent.CreateStart(i, new ClassOverridingEquals() { x = i }))
                .ToStreamable()
                ;

            var stream2 = Enumerable.Range(0, 3)
                .ToObservable()
                .Select(i => StreamEvent.CreateStart(i, i))
                .ToStreamable()
                ;

            try
            {
                var result = stream1
                       .Join(stream2, e => e.x, e => e, (left, right) => new { LeftX = left.x, RightX = right, })
                       .ToStreamEventObservable()
                       .ToEnumerable()
                       .ToArray();
                Assert.IsTrue(true); // just test that no exception happened
            }
            catch (Exception)
            {
                Assert.IsTrue(false); // should never reach here.
            }
        }

    }

    [TestClass]
    public class SuperStrictTests : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public SuperStrictTests() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .SuperStrictColumnar(true))
        { }

        public class Basetype
        {
            public int x;
            public override bool Equals(object obj) => !(obj is Basetype other) ? false : other.x == this.x;
            public override int GetHashCode() => this.x.GetHashCode();
        }

        public class Subtype : Basetype
        {
            public int fieldOfSubtype;

            public override bool Equals(object obj) => obj is Subtype other && this.fieldOfSubtype == other.fieldOfSubtype && base.Equals(obj);
            public override int GetHashCode() => this.fieldOfSubtype.GetHashCode() ^ base.GetHashCode();
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectUsingSupertypeFields()
        {
            var enumerable = Enumerable.Range(0, 10)
                .Select(i => new Basetype() { x = i, })
                ;
            var streamResult = enumerable
                .ToStreamable()
                .Select(e => new Subtype() { x = e.x + 1, fieldOfSubtype = e.x, })
                .ToPayloadEnumerable()
                ;
            var linqResult = enumerable
                .Select(e => new Subtype() { x = e.x + 1, fieldOfSubtype = e.x, })
                ;
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }
    }

    [TestClass]
    public class SimpleTestsThatDontNecessarilyRunInColumnar : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public SimpleTestsThatDontNecessarilyRunInColumnar() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .UseMultiString(true)
            .MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations)) { }

        public static bool MyIsEqual(List<RankedEvent<char>> a, List<RankedEvent<char>> b)
        {
            if (a.Count != b.Count) return false;

            for (int j = 0; j < a.Count; j++)
            {
                if (!a[j].Equals(b[j])) return false;
            }
            return true;
        }

        public static int MyGetHashCode(List<RankedEvent<char>> a)
        {
            int ret = 0;
            foreach (var l in a)
            {
                ret ^= l.GetHashCode();
            }
            return ret;
        }

        [TestMethod, TestCategory("Gated")]
        public void ToEndEdgeFreeTest5()
        {
            var input = new StreamEvent<char>[]
            {
                StreamEvent.CreateInterval(1, 2, 'a'),
                StreamEvent.CreateInterval(2, 3, 'a'),
                StreamEvent.CreateInterval(3, 4, 'b'),
                StreamEvent.CreatePunctuation<char>(StreamEvent.InfinitySyncTime),
            };

            var expectedOutput = new StreamEvent<List<RankedEvent<char>>>[]
            {
                StreamEvent.CreateInterval(1, 2, new List<RankedEvent<char>> { new RankedEvent<char>(1, 'a') }),
                StreamEvent.CreateInterval(2, 3, new List<RankedEvent<char>> { new RankedEvent<char>(1, 'a') }),
                StreamEvent.CreateInterval(3, 4, new List<RankedEvent<char>> { new RankedEvent<char>(1, 'b') }),
                StreamEvent.CreatePunctuation<List<RankedEvent<char>>>(StreamEvent.InfinitySyncTime),
            };
            var str = input.ToObservable().ToStreamable(null, FlushPolicy.FlushOnPunctuation, null, OnCompletedPolicy.None).Cache();
            var result = str.Aggregate(w => w.TopK(e => e, 5)).
                SetProperty().PayloadEqualityComparer(new EqualityComparerExpression<List<RankedEvent<char>>>
                    ((a, b) => MyIsEqual(a, b), a => MyGetHashCode(a))).
                ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).ToEnumerable();
            Assert.IsTrue(expectedOutput.SequenceEqual(result, new SERListEqualityComparer()));
            str.Dispose();
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereWithNonColumnar() => Enumerable.Range(0, 100).Select(i => new NonColumnarClass(i, 'x')).TestWhere(r => r.x > 3);

        // Total number of input events
        private const int NumEvents = 100;
        private readonly IEnumerable<MyStruct2> enumerable = Enumerable.Range(0, NumEvents)
            .Select(e =>
                new MyStruct2 { field1 = e, field2 = new MyString(Convert.ToString("string" + e)), field3 = new NestedStruct { nestedField = e } });

        private void TestWhere(Expression<Func<MyStruct2, bool>> predicate) => Assert.IsTrue(this.enumerable.TestWhere(predicate));

        [TestMethod, TestCategory("Gated")]
        public void SelectWhereAnonymousTypeWithColToRow()
        {
            var streamResult = this.enumerable
                .ToStreamable()
                .Select(e => new { anonfield1 = e.field1 })
                .ColumnToRow()
                .RowToColumn()
                .Where(e => e.anonfield1 == 0)
                .ToPayloadEnumerable();
            var linqResult = this.enumerable
                .Select(e => new { anonfield1 = e.field1 }).Where(e => e.anonfield1 == 0);
            Assert.IsTrue(linqResult.SequenceEqual(streamResult));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereWithClosure()
        {
            using (var modifier = new ConfigModifier().ForceRowBasedExecution(true).Modify())
            {
                // Should cause fallback to row-oriented.
                // BUG? Should codegen be able to handle this?
                var s = "string";
                var t = "another string";
                TestWhere(e => e.field2.mystring.Contains(s + t));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereNonColumnarWithClass() // Test to make sure that ColToRow creates an instance of a payload when it is a class
        {
            using (var modifier = new ConfigModifier().ForceRowBasedExecution(true).Modify())
            {
                Config.ForceRowBasedExecution = true;
                var input = Enumerable.Range(0, 20).Select(i => new MyString(i.ToString()));
                var foo = "1";
                var observable = input
                    .ToObservable();
                var stream = observable
                    .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
                var streamResult = stream
                    .Where(r => r.mystring.Contains(foo))
                    .ToPayloadEnumerable();

                var a = streamResult.ToArray();
                var expected = input.Where(r => r.mystring.Contains(foo));
                Assert.IsTrue(expected.SequenceEqual(a));
            }
        }
    }

    [TestClass]
    public class ClipJoinMulticast : TestWithConfigSettingsAndMemoryLeakDetection
    {
        [TestMethod]
        public void SelfClipAndSelfJoin()
        {
            // constants
            const int StartId = 1000;
            const int EndId = 1001;
            long maxRequestDuration = TimeSpan.FromSeconds(600).Ticks;

            var inputs = Enumerable.Range(0, 3)
                .Select(i => new Request
                {
                    TIMESTAMP = i.ToString(),
                    EventId = i,
                    Pid = i,
                    CorrelationId = i.ToString(),
                    RequestId = i.ToString(),
                })
                .ToObservable()
                .ToTemporalStreamable(r => r.EventId)
                .Multicast(2)
                ;

            var requestStarts = inputs[0].Where(r => r.EventId == StartId).AlterEventDuration(maxRequestDuration);
            var requestEnds = inputs[1].Where(r => r.EventId == EndId);

            var requests = requestStarts.ClipEventDuration(
                                            requestEnds,
                                            se => new { se.CorrelationId, se.RequestId, se.Pid },
                                            ee => new { ee.CorrelationId, ee.RequestId, ee.Pid })
                                        .Select(r => new { r.TIMESTAMP, r.CorrelationId, r.RequestId, r.Pid })
                                        .Multicast(2);

            // any requests with the same correlationId and Pid that overlap in execution
            var output = requests[0].Join(
                    requests[1],
                    l => new { l.CorrelationId, l.Pid },
                    r => new { r.CorrelationId, r.Pid },
                    (l, r) => new { FirstRequest = l, SecondRequest = r })
                .ToStreamEventObservable()
                .ToEnumerable()
                .ToArray();
        }

        public struct Request
        {
            public string TIMESTAMP { get; set; }
            public int EventId { get; set; }
            public int Pid { get; set; }
            public string CorrelationId { get; set; }
            public string RequestId { get; set; }
        }
    }

    [TestClass]
    public class FloatingDisorder : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public FloatingDisorder() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .AllowFloatingReorderPolicy(true))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedPassthrough()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize)
                .Select(e => rand.NextDouble() < disorderFraction ? e - rand.Next(0, disorderAmount) : e)
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o,
                DisorderPolicy.Drop(5));

            var disorderedOutputData = ingress.ToTemporalObservable((s, p) => p).ToEnumerable().ToList();

            Assert.IsTrue(disorderedOutputData.SequenceEqual(disorderedInputData));
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedWhere()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize)
                .Select(e => rand.NextDouble() < disorderFraction ? new { time = e - rand.Next(0, disorderAmount), key = e } : new { time = e, key = e })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o.time,
                DisorderPolicy.Drop(5)).Where(o => o.key % 2 == 0);

            var disorderedOutputData = ingress.ToTemporalObservable((s, p) => p).ToEnumerable().ToList();

            Assert.IsTrue(disorderedOutputData.SequenceEqual(disorderedInputData.Where(o => o.key % 2 == 0)));
        }

        [TestMethod, TestCategory("Gated")]
        public void DisorderedSelect()
        {
            double disorderFraction = 0.5;
            int disorderAmount = 10;
            const int inputsize = 30;
            var rand = new Random(2);
            var disorderedInputData =
                Enumerable.Range(disorderAmount, inputsize)
                .Select(e => rand.NextDouble() < disorderFraction ? new { time = e - rand.Next(0, disorderAmount), key = e } : new { time = e, key = e })
                .ToList();

            var ingress = disorderedInputData.ToObservable().ToTemporalStreamable(
                o => o.time,
                DisorderPolicy.Drop(5)).Select((s, o) => s);

            var disorderedOutputData = ingress.ToTemporalObservable((s, p) => p).ToEnumerable().ToList();

            Assert.IsTrue(disorderedOutputData.SequenceEqual(disorderedInputData.Select(o => (long)o.time)));
        }
    }
}