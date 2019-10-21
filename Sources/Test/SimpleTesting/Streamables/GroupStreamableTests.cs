// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Sharding;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public struct StructWithProperty
    {
        public int P { get; set; }
    }

    public struct GameData
    {
        public int EventType; // 0: start game, 1: kill event, 2: end game
        public int GameId;
        public int UserId;
        public ulong NumKills;
    }

    public struct ThresholdData
    {
        public ulong Threshold;
        public int Medal;
    }

    internal static class StreamableInternal
    {
        /// <summary>
        /// Convert a stream (with a Empty key) to a simple enumerable of the payloads.
        /// </summary>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        public static IEnumerable<TPayload> ToPayloadEnumerable<TPayload>(this IStreamable<Empty, TPayload> source)
        {
            return source.ToAtemporalObservable().ToEnumerable();
        }

        public static IStreamable<TKey, StructTuple<TPayload, TPayload>> ComputeSignalChangeStream<TKey, TPayload>(this IStreamable<TKey, TPayload> input)
        {
            return
                input.Multicast(xs => xs.ShiftEventLifetime(vs => vs + 1).Join(xs.AlterEventDuration(1), (l, r) => new StructTuple<TPayload, TPayload> { Item1 = l, Item2 = r }));
        }
    }

    [TestClass]
    public class GroupStreamableTestsRow : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public GroupStreamableTestsRow() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true))
        { }

        [TestMethod, TestCategory("Gated")]
        public void Group0Row()
        {
            using (var modifier = new ConfigModifier().MapArity(1).ReduceArity(1).Modify())
            {
                // Simple pass through
                var input = Enumerable.Range(0, 20);
                var cached = input
                    .ToObservable();
                var cachedStr = cached
                    .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
                var streamResult = cachedStr
                    .GroupApply(i => i % 2, g => g)
                    .ToPayloadEnumerable()
                    ;

                var a = streamResult.ToArray();

                Assert.IsTrue(input.SequenceEqual(a));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void Group1Row()
        {
            // Simple grouping: create two groups, one of evens and one of odds
            var input = Enumerable.Range(0, 20);
            var cached = input
                .ToObservable();
            var cachedStr = cached
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var streamResult = cachedStr
                .GroupApply(i => i % 2, g => g.Count())
                .ToPayloadEnumerable()
                ;

            var a = streamResult.ToArray();
            Assert.IsTrue(a.Length == 2 && a[0] == 10 && a[1] == 10);
        }

        [TestMethod, TestCategory("Gated")]
        public void Group2Row()
        {
            var gameInput = new[]
            {
                StreamEvent.CreateStart(9900, new GameData { EventType = 0, GameId = 10, UserId = 100 }), // start game
                StreamEvent.CreateStart(10000, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 1 }),
                StreamEvent.CreateStart(10020, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 1 }),
                StreamEvent.CreateStart(10025, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 30 }),
                StreamEvent.CreateStart(10030, new GameData { EventType = 1, GameId = 10, UserId = 100 }), // end game
                StreamEvent.CreateStart(10040, new GameData { EventType = 2, GameId = 10 })
            }.ToObservable().ToStreamable();

            var thresholdInput = new[]
            {
                StreamEvent.CreateStart(9000, new ThresholdData { Threshold = 2, Medal = 43 }),
                StreamEvent.CreateStart(9000, new ThresholdData { Threshold = 10, Medal = 53 }),
                StreamEvent.CreateStart(9000, new ThresholdData { Threshold = 20, Medal = 63 }),
                StreamEvent.CreateEnd(10010, 9000, new ThresholdData { Threshold = 2, Medal = 43 }),
                StreamEvent.CreateStart(10010, new ThresholdData { Threshold = 3, Medal = 43 }) // at time 10010, we change threshold for medal 43, from 2 to 3
            }.ToObservable().ToStreamable();

            // clip each game event to end at the time of game completion
            var clippedGameInput =
                gameInput.Where(e => e.EventType < 2).ClipEventDuration(gameInput.Where(e => e.EventType == 2), e => e.GameId, e => e.GameId);

            var result =
                clippedGameInput
                .GroupApply(e => new { e.GameId, e.UserId }, str => StreamableInternal.ComputeSignalChangeStream(str.Sum(e => e.NumKills)), (g, c) => new { g.Key.GameId, g.Key.UserId, FromKills = c.Item1, ToKills = c.Item2 }) // count #kills per {game,user} combination
                ;

            var finalResultSequence = result
                .ToStreamEventObservable()
                .ToEnumerable();
            var finalResult = finalResultSequence.First();

            Assert.IsTrue(finalResultSequence.Count() == 1 &&
                finalResult.IsPunctuation && finalResult.SyncTime == StreamEvent.InfinitySyncTime);

            // Result interpretation:
            // 1. we award user 100 with medal 43 at timestamp 10030 (3rd kill)
            // 2. the event ends at 10040 because the game ends at that timestamp (due to clip)
        }

        [TestMethod, TestCategory("Gated")]
        public void Group3Row()
        {
            // Simple grouping: create two groups, one of evens and one of odds
            var input = Enumerable.Range(0, 20);
            var cached = input
                .ToObservable();
            var cachedStr = cached
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var streamResult = cachedStr
                .GroupApply(i => i % 2, g => g.Count(), (g, p) => new StructWithProperty() { P = (int)p, })
                .ToPayloadEnumerable()
                ;

            var a = streamResult.ToArray();
            Assert.IsTrue(a.Length == 2 && a[0].P == 10 && a[1].P == 10);
        }

        [TestMethod, TestCategory("Gated")]
        public void Group4Row()
        {
            var gameInputEnumerable = new[]
            {
                StreamEvent.CreateStart(9900, new GameData { EventType = 0, GameId = 10, UserId = 100 }), // start game
                StreamEvent.CreateStart(10000, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 1 }),
                StreamEvent.CreateStart(10020, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 1 }),
                StreamEvent.CreateStart(10025, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 30 }),
                StreamEvent.CreateStart(10030, new GameData { EventType = 1, GameId = 10, UserId = 100 }), // end game
                StreamEvent.CreateStart(10040, new GameData { EventType = 2, GameId = 10 })
            };
            var gameInput = gameInputEnumerable.ToObservable().ToStreamable();

            var result =
                gameInput
                .Shard(1)
                .Shuffle(e => e.EventType)
                .Unshuffle()
                .Unshard()
                ;

            var finalResultSequence = result
                .ToStreamEventObservable()
                .Where(se => se.IsData)
                .ToEnumerable()
                ;

            Assert.IsTrue(finalResultSequence.SequenceEqual(gameInputEnumerable));
        }

        [TestMethod, TestCategory("Gated")]
        public void Group5Row()
        {
            var input = Enumerable.Range(0, 20);
            var result = input
                .ToStreamable()
                .Aggregate(a => a.Sum(x => x), a => a.Count(), (s, c) => new { field1 = s + ((int)c) })
                .ToStreamEventObservable()
                .Where(se => se.IsData)
                .ToEnumerable()
                .ToArray();
            Assert.IsTrue(result.Length == 1 && result[0].Payload.field1 == 210);
        }

        [TestMethod, TestCategory("Gated")]
        public void Group6Row()
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

            var result =
                inputStream
                .Shard(1)
                .Shuffle(e => e)
                .SelectKey((k, p) => new { K = k, P = p })
                .Unshuffle()
                .Unshard()
                ;

            var finalResultSequence = result
                .ToStreamEventObservable()
                .Where(se => se.IsData)
                .Select(se => StreamEvent.CreateStart(se.StartTime, se.Payload.P))
                .ToEnumerable()
                ;

            Assert.IsTrue(finalResultSequence.SequenceEqual(inputEnumerable));
        }

        [TestMethod, TestCategory("Gated")]
        public void Group7Row()
        {
            using (var modifier = new ConfigModifier().UseMultiString(true).Modify())
            {
                var input = Enumerable.Range(0, 100)
                    .Select(i => new MyData { field1 = i, field2 = (i % 10).ToString(), })
                    ;
                var stream = input
                    .ToStatStreamable();
                var result = stream
                    .GroupApply(e => e.field2, str => str.Count(), (g, c) => new StructTuple<string, int> { Item1 = g.Key, Item2 = (int)c, })
                    .ToStreamEventObservable().Where(e => e.IsData)
                    .Select(e => e.Payload)
                    .ToEnumerable()
                    .OrderBy(e => e.Item1)
                    .ToArray();
                var expected = input
                    .GroupBy(e => e.field2, (k, v) => new StructTuple<string, int> { Item1 = k, Item2 = v.Count(), })
                    .ToArray()
                    ;
                Assert.IsTrue(expected.SequenceEqual(result));
            }
        }
    }

    [TestClass]
    public class GroupStreamableTestsRowSmallBatch : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public GroupStreamableTestsRowSmallBatch() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100))
        { }

        [TestMethod, TestCategory("Gated")]
        public void Group0RowSmallBatch()
        {
            using (var modifier = new ConfigModifier().MapArity(1).ReduceArity(1).Modify())
            {
                // Simple pass through
                var input = Enumerable.Range(0, 20);
                var cached = input
                    .ToObservable();
                var cachedStr = cached
                    .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
                var streamResult = cachedStr
                    .GroupApply(i => i % 2, g => g)
                    .ToPayloadEnumerable()
                    ;

                var a = streamResult.ToArray();

                Assert.IsTrue(input.SequenceEqual(a));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void Group1RowSmallBatch()
        {
            // Simple grouping: create two groups, one of evens and one of odds
            var input = Enumerable.Range(0, 20);
            var cached = input
                .ToObservable();
            var cachedStr = cached
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var streamResult = cachedStr
                .GroupApply(i => i % 2, g => g.Count())
                .ToPayloadEnumerable()
                ;

            var a = streamResult.ToArray();
            Assert.IsTrue(a.Length == 2 && a[0] == 10 && a[1] == 10);
        }

        [TestMethod, TestCategory("Gated")]
        public void Group2RowSmallBatch()
        {
            var gameInput = new[]
            {
                StreamEvent.CreateStart(9900, new GameData { EventType = 0, GameId = 10, UserId = 100 }), // start game
                StreamEvent.CreateStart(10000, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 1 }),
                StreamEvent.CreateStart(10020, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 1 }),
                StreamEvent.CreateStart(10025, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 30 }),
                StreamEvent.CreateStart(10030, new GameData { EventType = 1, GameId = 10, UserId = 100 }), // end game
                StreamEvent.CreateStart(10040, new GameData { EventType = 2, GameId = 10 })
            }.ToObservable().ToStreamable();

            var thresholdInput = new[]
            {
                StreamEvent.CreateStart(9000, new ThresholdData { Threshold = 2, Medal = 43 }),
                StreamEvent.CreateStart(9000, new ThresholdData { Threshold = 10, Medal = 53 }),
                StreamEvent.CreateStart(9000, new ThresholdData { Threshold = 20, Medal = 63 }),
                StreamEvent.CreateEnd(10010, 9000, new ThresholdData { Threshold = 2, Medal = 43 }),
                StreamEvent.CreateStart(10010, new ThresholdData { Threshold = 3, Medal = 43 }) // at time 10010, we change threshold for medal 43, from 2 to 3
            }.ToObservable().ToStreamable();

            // clip each game event to end at the time of game completion
            var clippedGameInput =
                gameInput.Where(e => e.EventType < 2).ClipEventDuration(gameInput.Where(e => e.EventType == 2), e => e.GameId, e => e.GameId);

            var result =
                clippedGameInput
                .GroupApply(e => new { e.GameId, e.UserId }, str => StreamableInternal.ComputeSignalChangeStream(str.Sum(e => e.NumKills)), (g, c) => new { g.Key.GameId, g.Key.UserId, FromKills = c.Item1, ToKills = c.Item2 }) // count #kills per {game,user} combination
                ;

            var finalResultSequence = result
                .ToStreamEventObservable()
                .ToEnumerable();
            var finalResult = finalResultSequence.First();

            Assert.IsTrue(finalResultSequence.Count() == 1 &&
                finalResult.IsPunctuation && finalResult.SyncTime == StreamEvent.InfinitySyncTime);

            // Result interpretation:
            // 1. we award user 100 with medal 43 at timestamp 10030 (3rd kill)
            // 2. the event ends at 10040 because the game ends at that timestamp (due to clip)
        }

        [TestMethod, TestCategory("Gated")]
        public void Group3RowSmallBatch()
        {
            // Simple grouping: create two groups, one of evens and one of odds
            var input = Enumerable.Range(0, 20);
            var cached = input
                .ToObservable();
            var cachedStr = cached
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var streamResult = cachedStr
                .GroupApply(i => i % 2, g => g.Count(), (g, p) => new StructWithProperty() { P = (int)p, })
                .ToPayloadEnumerable()
                ;

            var a = streamResult.ToArray();
            Assert.IsTrue(a.Length == 2 && a[0].P == 10 && a[1].P == 10);
        }

        [TestMethod, TestCategory("Gated")]
        public void Group4RowSmallBatch()
        {
            var gameInputEnumerable = new[]
            {
                StreamEvent.CreateStart(9900, new GameData { EventType = 0, GameId = 10, UserId = 100 }), // start game
                StreamEvent.CreateStart(10000, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 1 }),
                StreamEvent.CreateStart(10020, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 1 }),
                StreamEvent.CreateStart(10025, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 30 }),
                StreamEvent.CreateStart(10030, new GameData { EventType = 1, GameId = 10, UserId = 100 }), // end game
                StreamEvent.CreateStart(10040, new GameData { EventType = 2, GameId = 10 })
            };
            var gameInput = gameInputEnumerable.ToObservable().ToStreamable();

            var result =
                gameInput
                .Shard(1)
                .Shuffle(e => e.EventType)
                .Unshuffle()
                .Unshard()
                ;

            var finalResultSequence = result
                .ToStreamEventObservable()
                .Where(se => se.IsData)
                .ToEnumerable()
                ;

            Assert.IsTrue(finalResultSequence.SequenceEqual(gameInputEnumerable));
        }

        [TestMethod, TestCategory("Gated")]
        public void Group5RowSmallBatch()
        {
            var input = Enumerable.Range(0, 20);
            var result = input
                .ToStreamable()
                .Aggregate(a => a.Sum(x => x), a => a.Count(), (s, c) => new { field1 = s + ((int)c) })
                .ToStreamEventObservable()
                .Where(se => se.IsData)
                .ToEnumerable()
                .ToArray();
            Assert.IsTrue(result.Length == 1 && result[0].Payload.field1 == 210);
        }

        [TestMethod, TestCategory("Gated")]
        public void Group6RowSmallBatch()
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

            var result =
                inputStream
                .Shard(1)
                .Shuffle(e => e)
                .SelectKey((k, p) => new { K = k, P = p })
                .Unshuffle()
                .Unshard()
                ;

            var finalResultSequence = result
                .ToStreamEventObservable()
                .Where(se => se.IsData)
                .Select(se => StreamEvent.CreateStart(se.StartTime, se.Payload.P))
                .ToEnumerable()
                ;

            Assert.IsTrue(finalResultSequence.SequenceEqual(inputEnumerable));
        }

        [TestMethod, TestCategory("Gated")]
        public void Group7RowSmallBatch()
        {
            using (var modifier = new ConfigModifier().UseMultiString(true).Modify())
            {
                var input = Enumerable.Range(0, 100)
                    .Select(i => new MyData { field1 = i, field2 = (i % 10).ToString(), })
                    ;
                var stream = input
                    .ToStatStreamable();
                var result = stream
                    .GroupApply(e => e.field2, str => str.Count(), (g, c) => new StructTuple<string, int> { Item1 = g.Key, Item2 = (int)c, })
                    .ToStreamEventObservable().Where(e => e.IsData)
                    .Select(e => e.Payload)
                    .ToEnumerable()
                    .OrderBy(e => e.Item1)
                    .ToArray();
                var expected = input
                    .GroupBy(e => e.field2, (k, v) => new StructTuple<string, int> { Item1 = k, Item2 = v.Count(), })
                    .ToArray()
                    ;
                Assert.IsTrue(expected.SequenceEqual(result));
            }
        }
    }

    [TestClass]
    public class GroupStreamableTestsColumnar : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public GroupStreamableTestsColumnar() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true))
        { }

        [TestMethod, TestCategory("Gated")]
        public void Group0Columnar()
        {
            using (var modifier = new ConfigModifier().MapArity(1).ReduceArity(1).Modify())
            {
                // Simple pass through
                var input = Enumerable.Range(0, 20);
                var cached = input
                    .ToObservable();
                var cachedStr = cached
                    .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
                var streamResult = cachedStr
                    .GroupApply(i => i % 2, g => g)
                    .ToPayloadEnumerable()
                    ;

                var a = streamResult.ToArray();

                Assert.IsTrue(input.SequenceEqual(a));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void Group1Columnar()
        {
            // Simple grouping: create two groups, one of evens and one of odds
            var input = Enumerable.Range(0, 20);
            var cached = input
                .ToObservable();
            var cachedStr = cached
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var streamResult = cachedStr
                .GroupApply(i => i % 2, g => g.Count())
                .ToPayloadEnumerable()
                ;

            var a = streamResult.ToArray();
            Assert.IsTrue(a.Length == 2 && a[0] == 10 && a[1] == 10);
        }

        [TestMethod, TestCategory("Gated")]
        public void Group2Columnar()
        {
            var gameInput = new[]
            {
                StreamEvent.CreateStart(9900, new GameData { EventType = 0, GameId = 10, UserId = 100 }), // start game
                StreamEvent.CreateStart(10000, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 1 }),
                StreamEvent.CreateStart(10020, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 1 }),
                StreamEvent.CreateStart(10025, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 30 }),
                StreamEvent.CreateStart(10030, new GameData { EventType = 1, GameId = 10, UserId = 100 }), // end game
                StreamEvent.CreateStart(10040, new GameData { EventType = 2, GameId = 10 })
            }.ToObservable().ToStreamable();

            var thresholdInput = new[]
            {
                StreamEvent.CreateStart(9000, new ThresholdData { Threshold = 2, Medal = 43 }),
                StreamEvent.CreateStart(9000, new ThresholdData { Threshold = 10, Medal = 53 }),
                StreamEvent.CreateStart(9000, new ThresholdData { Threshold = 20, Medal = 63 }),
                StreamEvent.CreateEnd(10010, 9000, new ThresholdData { Threshold = 2, Medal = 43 }),
                StreamEvent.CreateStart(10010, new ThresholdData { Threshold = 3, Medal = 43 }) // at time 10010, we change threshold for medal 43, from 2 to 3
            }.ToObservable().ToStreamable();

            // clip each game event to end at the time of game completion
            var clippedGameInput =
                gameInput.Where(e => e.EventType < 2).ClipEventDuration(gameInput.Where(e => e.EventType == 2), e => e.GameId, e => e.GameId);

            var result =
                clippedGameInput
                .GroupApply(e => new { e.GameId, e.UserId }, str => StreamableInternal.ComputeSignalChangeStream(str.Sum(e => e.NumKills)), (g, c) => new { g.Key.GameId, g.Key.UserId, FromKills = c.Item1, ToKills = c.Item2 }) // count #kills per {game,user} combination
                ;

            var finalResultSequence = result
                .ToStreamEventObservable()
                .ToEnumerable();
            var finalResult = finalResultSequence.First();

            Assert.IsTrue(finalResultSequence.Count() == 1 &&
                finalResult.IsPunctuation && finalResult.SyncTime == StreamEvent.InfinitySyncTime);

            // Result interpretation:
            // 1. we award user 100 with medal 43 at timestamp 10030 (3rd kill)
            // 2. the event ends at 10040 because the game ends at that timestamp (due to clip)
        }

        [TestMethod, TestCategory("Gated")]
        public void Group3Columnar()
        {
            // Simple grouping: create two groups, one of evens and one of odds
            var input = Enumerable.Range(0, 20);
            var cached = input
                .ToObservable();
            var cachedStr = cached
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var streamResult = cachedStr
                .GroupApply(i => i % 2, g => g.Count(), (g, p) => new StructWithProperty() { P = (int)p, })
                .ToPayloadEnumerable()
                ;

            var a = streamResult.ToArray();
            Assert.IsTrue(a.Length == 2 && a[0].P == 10 && a[1].P == 10);
        }

        [TestMethod, TestCategory("Gated")]
        public void Group4Columnar()
        {
            var gameInputEnumerable = new[]
            {
                StreamEvent.CreateStart(9900, new GameData { EventType = 0, GameId = 10, UserId = 100 }), // start game
                StreamEvent.CreateStart(10000, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 1 }),
                StreamEvent.CreateStart(10020, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 1 }),
                StreamEvent.CreateStart(10025, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 30 }),
                StreamEvent.CreateStart(10030, new GameData { EventType = 1, GameId = 10, UserId = 100 }), // end game
                StreamEvent.CreateStart(10040, new GameData { EventType = 2, GameId = 10 })
            };
            var gameInput = gameInputEnumerable.ToObservable().ToStreamable();

            var result =
                gameInput
                .Shard(1)
                .Shuffle(e => e.EventType)
                .Unshuffle()
                .Unshard()
                ;

            var finalResultSequence = result
                .ToStreamEventObservable()
                .Where(se => se.IsData)
                .ToEnumerable()
                ;

            Assert.IsTrue(finalResultSequence.SequenceEqual(gameInputEnumerable));
        }

        [TestMethod, TestCategory("Gated")]
        public void Group5Columnar()
        {
            var input = Enumerable.Range(0, 20);
            var result = input
                .ToStreamable()
                .Aggregate(a => a.Sum(x => x), a => a.Count(), (s, c) => new { field1 = s + ((int)c) })
                .ToStreamEventObservable()
                .Where(se => se.IsData)
                .ToEnumerable()
                .ToArray();
            Assert.IsTrue(result.Length == 1 && result[0].Payload.field1 == 210);
        }

        [TestMethod, TestCategory("Gated")]
        public void Group6Columnar()
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

            var result =
                inputStream
                .Shard(1)
                .Shuffle(e => e)
                .SelectKey((k, p) => new { K = k, P = p })
                .Unshuffle()
                .Unshard()
                ;

            var finalResultSequence = result
                .ToStreamEventObservable()
                .Where(se => se.IsData)
                .Select(se => StreamEvent.CreateStart(se.StartTime, se.Payload.P))
                .ToEnumerable()
                ;

            Assert.IsTrue(finalResultSequence.SequenceEqual(inputEnumerable));
        }

        [TestMethod, TestCategory("Gated")]
        public void Group7Columnar()
        {
            using (var modifier = new ConfigModifier().UseMultiString(true).Modify())
            {
                var input = Enumerable.Range(0, 100)
                    .Select(i => new MyData { field1 = i, field2 = (i % 10).ToString(), })
                    ;
                var stream = input
                    .ToStatStreamable();
                var result = stream
                    .GroupApply(e => e.field2, str => str.Count(), (g, c) => new StructTuple<string, int> { Item1 = g.Key, Item2 = (int)c, })
                    .ToStreamEventObservable().Where(e => e.IsData)
                    .Select(e => e.Payload)
                    .ToEnumerable()
                    .OrderBy(e => e.Item1)
                    .ToArray();
                var expected = input
                    .GroupBy(e => e.field2, (k, v) => new StructTuple<string, int> { Item1 = k, Item2 = v.Count(), })
                    .ToArray()
                    ;
                Assert.IsTrue(expected.SequenceEqual(result));
            }
        }
    }

    [TestClass]
    public class GroupStreamableTestsColumnarSmallBatch : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public GroupStreamableTestsColumnarSmallBatch() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100))
        { }

        [TestMethod, TestCategory("Gated")]
        public void Group0ColumnarSmallBatch()
        {
            using (var modifier = new ConfigModifier().MapArity(1).ReduceArity(1).Modify())
            {
                // Simple pass through
                var input = Enumerable.Range(0, 20);
                var cached = input
                    .ToObservable();
                var cachedStr = cached
                    .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
                var streamResult = cachedStr
                    .GroupApply(i => i % 2, g => g)
                    .ToPayloadEnumerable()
                    ;

                var a = streamResult.ToArray();

                Assert.IsTrue(input.SequenceEqual(a));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void Group1ColumnarSmallBatch()
        {
            // Simple grouping: create two groups, one of evens and one of odds
            var input = Enumerable.Range(0, 20);
            var cached = input
                .ToObservable();
            var cachedStr = cached
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var streamResult = cachedStr
                .GroupApply(i => i % 2, g => g.Count())
                .ToPayloadEnumerable()
                ;

            var a = streamResult.ToArray();
            Assert.IsTrue(a.Length == 2 && a[0] == 10 && a[1] == 10);
        }

        [TestMethod, TestCategory("Gated")]
        public void Group2ColumnarSmallBatch()
        {
            var gameInput = new[]
            {
                StreamEvent.CreateStart(9900, new GameData { EventType = 0, GameId = 10, UserId = 100 }), // start game
                StreamEvent.CreateStart(10000, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 1 }),
                StreamEvent.CreateStart(10020, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 1 }),
                StreamEvent.CreateStart(10025, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 30 }),
                StreamEvent.CreateStart(10030, new GameData { EventType = 1, GameId = 10, UserId = 100 }), // end game
                StreamEvent.CreateStart(10040, new GameData { EventType = 2, GameId = 10 })
            }.ToObservable().ToStreamable();

            var thresholdInput = new[]
            {
                StreamEvent.CreateStart(9000, new ThresholdData { Threshold = 2, Medal = 43 }),
                StreamEvent.CreateStart(9000, new ThresholdData { Threshold = 10, Medal = 53 }),
                StreamEvent.CreateStart(9000, new ThresholdData { Threshold = 20, Medal = 63 }),
                StreamEvent.CreateEnd(10010, 9000, new ThresholdData { Threshold = 2, Medal = 43 }),
                StreamEvent.CreateStart(10010, new ThresholdData { Threshold = 3, Medal = 43 }) // at time 10010, we change threshold for medal 43, from 2 to 3
            }.ToObservable().ToStreamable();

            // clip each game event to end at the time of game completion
            var clippedGameInput =
                gameInput.Where(e => e.EventType < 2).ClipEventDuration(gameInput.Where(e => e.EventType == 2), e => e.GameId, e => e.GameId);

            var result =
                clippedGameInput
                .GroupApply(e => new { e.GameId, e.UserId }, str => StreamableInternal.ComputeSignalChangeStream(str.Sum(e => e.NumKills)), (g, c) => new { g.Key.GameId, g.Key.UserId, FromKills = c.Item1, ToKills = c.Item2 }) // count #kills per {game,user} combination
                ;

            var finalResultSequence = result
                .ToStreamEventObservable()
                .ToEnumerable();
            var finalResult = finalResultSequence.First();

            Assert.IsTrue(finalResultSequence.Count() == 1 &&
                finalResult.IsPunctuation && finalResult.SyncTime == StreamEvent.InfinitySyncTime);

            // Result interpretation:
            // 1. we award user 100 with medal 43 at timestamp 10030 (3rd kill)
            // 2. the event ends at 10040 because the game ends at that timestamp (due to clip)
        }

        [TestMethod, TestCategory("Gated")]
        public void Group3ColumnarSmallBatch()
        {
            // Simple grouping: create two groups, one of evens and one of odds
            var input = Enumerable.Range(0, 20);
            var cached = input
                .ToObservable();
            var cachedStr = cached
                .ToTemporalStreamable(s => 0, s => StreamEvent.InfinitySyncTime);
            var streamResult = cachedStr
                .GroupApply(i => i % 2, g => g.Count(), (g, p) => new StructWithProperty() { P = (int)p, })
                .ToPayloadEnumerable()
                ;

            var a = streamResult.ToArray();
            Assert.IsTrue(a.Length == 2 && a[0].P == 10 && a[1].P == 10);
        }

        [TestMethod, TestCategory("Gated")]
        public void Group4ColumnarSmallBatch()
        {
            var gameInputEnumerable = new[]
            {
                StreamEvent.CreateStart(9900, new GameData { EventType = 0, GameId = 10, UserId = 100 }), // start game
                StreamEvent.CreateStart(10000, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 1 }),
                StreamEvent.CreateStart(10020, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 1 }),
                StreamEvent.CreateStart(10025, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 30 }),
                StreamEvent.CreateStart(10030, new GameData { EventType = 1, GameId = 10, UserId = 100 }), // end game
                StreamEvent.CreateStart(10040, new GameData { EventType = 2, GameId = 10 })
            };
            var gameInput = gameInputEnumerable.ToObservable().ToStreamable();

            var result =
                gameInput
                .Shard(1)
                .Shuffle(e => e.EventType)
                .Unshuffle()
                .Unshard()
                ;

            var finalResultSequence = result
                .ToStreamEventObservable()
                .Where(se => se.IsData)
                .ToEnumerable()
                ;

            Assert.IsTrue(finalResultSequence.SequenceEqual(gameInputEnumerable));
        }

        [TestMethod, TestCategory("Gated")]
        public void Group5ColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, 20);
            var result = input
                .ToStreamable()
                .Aggregate(a => a.Sum(x => x), a => a.Count(), (s, c) => new { field1 = s + ((int)c) })
                .ToStreamEventObservable()
                .Where(se => se.IsData)
                .ToEnumerable()
                .ToArray();
            Assert.IsTrue(result.Length == 1 && result[0].Payload.field1 == 210);
        }

        [TestMethod, TestCategory("Gated")]
        public void Group6ColumnarSmallBatch()
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

            var result =
                inputStream
                .Shard(1)
                .Shuffle(e => e)
                .SelectKey((k, p) => new { K = k, P = p })
                .Unshuffle()
                .Unshard()
                ;

            var finalResultSequence = result
                .ToStreamEventObservable()
                .Where(se => se.IsData)
                .Select(se => StreamEvent.CreateStart(se.StartTime, se.Payload.P))
                .ToEnumerable()
                ;

            Assert.IsTrue(finalResultSequence.SequenceEqual(inputEnumerable));
        }

        [TestMethod, TestCategory("Gated")]
        public void Group7ColumnarSmallBatch()
        {
            using (var modifier = new ConfigModifier().UseMultiString(true).Modify())
            {
                var input = Enumerable.Range(0, 100)
                    .Select(i => new MyData { field1 = i, field2 = (i % 10).ToString(), })
                    ;
                var stream = input
                    .ToStatStreamable();
                var result = stream
                    .GroupApply(e => e.field2, str => str.Count(), (g, c) => new StructTuple<string, int> { Item1 = g.Key, Item2 = (int)c, })
                    .ToStreamEventObservable().Where(e => e.IsData)
                    .Select(e => e.Payload)
                    .ToEnumerable()
                    .OrderBy(e => e.Item1)
                    .ToArray();
                var expected = input
                    .GroupBy(e => e.field2, (k, v) => new StructTuple<string, int> { Item1 = k, Item2 = v.Count(), })
                    .ToArray()
                    ;
                Assert.IsTrue(expected.SequenceEqual(result));
            }
        }
    }
}
