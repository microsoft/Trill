// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Sharding;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class ShuffleStreamableTestsRow : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public ShuffleStreamableTestsRow() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .MapArity(1)
            .ReduceArity(1))
        { }

        [TestMethod, TestCategory("Gated")]
        public void ShuffleStreamable1Row()
        {
            var savedScheduler = Config.StreamScheduler;
            Config.StreamScheduler = StreamScheduler.OwnedThreads(2);
            var gameInput = new[]
            {
                StreamEvent.CreateStart(9900, new GameData { EventType = 0, GameId = 10, UserId = 100 }), // start game
                StreamEvent.CreateStart(10000, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 1 }),
                StreamEvent.CreateStart(10020, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 1 }),
                StreamEvent.CreateStart(10025, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 30 }),
                StreamEvent.CreateStart(10030, new GameData { EventType = 1, GameId = 10, UserId = 100 }), // end game
                StreamEvent.CreateStart(10040, new GameData { EventType = 2, GameId = 10 })
            }.ToObservable().ToStreamable();

            // clip each game event to end at the time of game completion
            var clippedGameInput =
                gameInput.Where(e => e.EventType < 2).ClipEventDuration(gameInput.Where(e => e.EventType == 2), e => e.GameId, e => e.GameId);

            var result =
                clippedGameInput
                .GroupApply(
                    e => new { e.GameId, e.UserId },
                    str => StreamableInternal.ComputeSignalChangeStream(str.Sum(e => e.NumKills)),
                    (g, c) => new { g.Key.GameId, g.Key.UserId, FromKills = c.Item1, ToKills = c.Item2 }) // count #kills per {game,user} combination
                ;

            var finalResultSequence = result
                .ToStreamEventObservable()
                .ToEnumerable()
                .ToList();
            var finalResult = finalResultSequence.First();

            Assert.IsTrue(finalResultSequence.Count() == 1 &&
                finalResult.IsPunctuation && finalResult.SyncTime == StreamEvent.InfinitySyncTime);

            Config.StreamScheduler.Stop();
            Config.StreamScheduler = savedScheduler;
        }

        [TestMethod, TestCategory("Gated")]
        public void ShuffleStreamable2Row()
        {
            var input = Enumerable.Range(0, 10000000)
                .ToStatStreamable();
            var result = input.GroupApply(e => e % 1000, str => str.Count(), (g, c) => new StructTuple<long, ulong> { Item1 = g.Key, Item2 = c });
            var finalResultSequence = result
                .ToStreamEventObservable().Where(e => e.IsData)
                .ToEnumerable();
            var ct = finalResultSequence.Count();
            Assert.IsTrue(ct == 249000);
        }

        [TestMethod, TestCategory("Gated")]
        public void ShuffleStreamable3Row()
        {
            var savedUseMultiString = Config.UseMultiString;
            var savedScheduler = Config.StreamScheduler;
            Config.StreamScheduler = StreamScheduler.OwnedThreads(2);
            Config.UseMultiString = true;
            var input = Enumerable.Range(0, 100)
                .Select(i => new MyData { field1 = i, field2 = (i % 10).ToString(), });
            var stream = input.ToStatStreamable();
            var result = stream
                .GroupApply(e => e.field2, str => str.Count(), (g, c) => new StructTuple<string, int> { Item1 = g.Key, Item2 = (int)c, })
                .ToAtemporalObservable()
                .ToEnumerable()
                .OrderBy(e => e.Item1)
                .ToArray();
            var expected = input
                .GroupBy(e => e.field2, (k, v) => new StructTuple<string, int> { Item1 = k, Item2 = v.Count(), })
                .ToArray();
            Assert.IsTrue(expected.SequenceEqual(result));

            Config.StreamScheduler.Stop();
            Config.StreamScheduler = savedScheduler;
            Config.UseMultiString = savedUseMultiString;
        }

        [TestMethod, TestCategory("Gated")]
        public void ShuffleStreamable4Row()
        {
            var savedUseMultiString = Config.UseMultiString;
            Config.UseMultiString = true;

            var input = Enumerable.Range(0, 100)
                .Select(i => new MyData { field1 = i, field2 = (i % 10).ToString(), });

            var stream = input.ToStatStreamable();
            var result = stream
                .Shard(2)
                .ReKey(e => e.field2)
                .ReDistribute()
                .Query(str => str.Count())
                .SelectKey((g, c) => new StructTuple<string, int> { Item1 = g, Item2 = (int)c, })
                .Unshuffle()
                .Unshard()
                .ToAtemporalObservable()
                .ToEnumerable()
                .OrderBy(e => e.Item1)
                .ToArray();
            var ct = result.Length;
            var expected = input
                .GroupBy(e => e.field2, (k, v) => new StructTuple<string, int> { Item1 = k, Item2 = v.Count(), })
                .ToArray();

            Assert.IsTrue(expected.SequenceEqual(result));
            Config.UseMultiString = savedUseMultiString;
        }
    }

    [TestClass]
    public class ShuffleStreamableTestsRowSmallBatch : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public ShuffleStreamableTestsRowSmallBatch() : base(new ConfigModifier()
            .ForceRowBasedExecution(true)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .MapArity(1)
            .ReduceArity(1))
        { }

        [TestMethod, TestCategory("Gated")]
        public void ShuffleStreamable1RowSmallBatch()
        {
            var savedScheduler = Config.StreamScheduler;
            Config.StreamScheduler = StreamScheduler.OwnedThreads(2);
            var gameInput = new[]
            {
                StreamEvent.CreateStart(9900, new GameData { EventType = 0, GameId = 10, UserId = 100 }), // start game
                StreamEvent.CreateStart(10000, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 1 }),
                StreamEvent.CreateStart(10020, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 1 }),
                StreamEvent.CreateStart(10025, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 30 }),
                StreamEvent.CreateStart(10030, new GameData { EventType = 1, GameId = 10, UserId = 100 }), // end game
                StreamEvent.CreateStart(10040, new GameData { EventType = 2, GameId = 10 })
            }.ToObservable().ToStreamable();

            // clip each game event to end at the time of game completion
            var clippedGameInput =
                gameInput.Where(e => e.EventType < 2).ClipEventDuration(gameInput.Where(e => e.EventType == 2), e => e.GameId, e => e.GameId);

            var result =
                clippedGameInput
                .GroupApply(
                    e => new { e.GameId, e.UserId },
                    str => StreamableInternal.ComputeSignalChangeStream(str.Sum(e => e.NumKills)),
                    (g, c) => new { g.Key.GameId, g.Key.UserId, FromKills = c.Item1, ToKills = c.Item2 }) // count #kills per {game,user} combination
                ;

            var finalResultSequence = result
                .ToStreamEventObservable()
                .ToEnumerable()
                .ToList();
            var finalResult = finalResultSequence.First();

            Assert.IsTrue(finalResultSequence.Count() == 1 &&
                finalResult.IsPunctuation && finalResult.SyncTime == StreamEvent.InfinitySyncTime);

            Config.StreamScheduler.Stop();
            Config.StreamScheduler = savedScheduler;
        }

        [TestMethod, TestCategory("Gated")]
        public void ShuffleStreamable2RowSmallBatch()
        {
            var input = Enumerable.Range(0, 10000000)
                .ToStatStreamable();
            var result = input.GroupApply(e => e % 1000, str => str.Count(), (g, c) => new StructTuple<long, ulong> { Item1 = g.Key, Item2 = c });
            var finalResultSequence = result
                .ToStreamEventObservable().Where(e => e.IsData)
                .ToEnumerable();
            var ct = finalResultSequence.Count();
            Assert.IsTrue(ct == 249000);
        }

        [TestMethod, TestCategory("Gated")]
        public void ShuffleStreamable3RowSmallBatch()
        {
            var savedUseMultiString = Config.UseMultiString;
            var savedScheduler = Config.StreamScheduler;
            Config.StreamScheduler = StreamScheduler.OwnedThreads(2);
            Config.UseMultiString = true;
            var input = Enumerable.Range(0, 100)
                .Select(i => new MyData { field1 = i, field2 = (i % 10).ToString(), });
            var stream = input.ToStatStreamable();
            var result = stream
                .GroupApply(e => e.field2, str => str.Count(), (g, c) => new StructTuple<string, int> { Item1 = g.Key, Item2 = (int)c, })
                .ToAtemporalObservable()
                .ToEnumerable()
                .OrderBy(e => e.Item1)
                .ToArray();
            var expected = input
                .GroupBy(e => e.field2, (k, v) => new StructTuple<string, int> { Item1 = k, Item2 = v.Count(), })
                .ToArray();
            Assert.IsTrue(expected.SequenceEqual(result));

            Config.StreamScheduler.Stop();
            Config.StreamScheduler = savedScheduler;
            Config.UseMultiString = savedUseMultiString;
        }

        [TestMethod, TestCategory("Gated")]
        public void ShuffleStreamable4RowSmallBatch()
        {
            var savedUseMultiString = Config.UseMultiString;
            Config.UseMultiString = true;

            var input = Enumerable.Range(0, 100)
                .Select(i => new MyData { field1 = i, field2 = (i % 10).ToString(), });

            var stream = input.ToStatStreamable();
            var result = stream
                .Shard(2)
                .ReKey(e => e.field2)
                .ReDistribute()
                .Query(str => str.Count())
                .SelectKey((g, c) => new StructTuple<string, int> { Item1 = g, Item2 = (int)c, })
                .Unshuffle()
                .Unshard()
                .ToAtemporalObservable()
                .ToEnumerable()
                .OrderBy(e => e.Item1)
                .ToArray();
            var ct = result.Length;
            var expected = input
                .GroupBy(e => e.field2, (k, v) => new StructTuple<string, int> { Item1 = k, Item2 = v.Count(), })
                .ToArray();

            Assert.IsTrue(expected.SequenceEqual(result));
            Config.UseMultiString = savedUseMultiString;
        }
    }

    [TestClass]
    public class ShuffleStreamableTestsColumnar : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public ShuffleStreamableTestsColumnar() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .MapArity(1)
            .ReduceArity(1))
        { }

        [TestMethod, TestCategory("Gated")]
        public void ShuffleStreamable1Columnar()
        {
            var savedScheduler = Config.StreamScheduler;
            Config.StreamScheduler = StreamScheduler.OwnedThreads(2);
            var gameInput = new[]
            {
                StreamEvent.CreateStart(9900, new GameData { EventType = 0, GameId = 10, UserId = 100 }), // start game
                StreamEvent.CreateStart(10000, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 1 }),
                StreamEvent.CreateStart(10020, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 1 }),
                StreamEvent.CreateStart(10025, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 30 }),
                StreamEvent.CreateStart(10030, new GameData { EventType = 1, GameId = 10, UserId = 100 }), // end game
                StreamEvent.CreateStart(10040, new GameData { EventType = 2, GameId = 10 })
            }.ToObservable().ToStreamable();

            // clip each game event to end at the time of game completion
            var clippedGameInput =
                gameInput.Where(e => e.EventType < 2).ClipEventDuration(gameInput.Where(e => e.EventType == 2), e => e.GameId, e => e.GameId);

            var result =
                clippedGameInput
                .GroupApply(
                    e => new { e.GameId, e.UserId },
                    str => StreamableInternal.ComputeSignalChangeStream(str.Sum(e => e.NumKills)),
                    (g, c) => new { g.Key.GameId, g.Key.UserId, FromKills = c.Item1, ToKills = c.Item2 }) // count #kills per {game,user} combination
                ;

            var finalResultSequence = result
                .ToStreamEventObservable()
                .ToEnumerable()
                .ToList();
            var finalResult = finalResultSequence.First();

            Assert.IsTrue(finalResultSequence.Count() == 1 &&
                finalResult.IsPunctuation && finalResult.SyncTime == StreamEvent.InfinitySyncTime);

            Config.StreamScheduler.Stop();
            Config.StreamScheduler = savedScheduler;
        }

        [TestMethod, TestCategory("Gated")]
        public void ShuffleStreamable2Columnar()
        {
            var input = Enumerable.Range(0, 10000000)
                .ToStatStreamable();
            var result = input.GroupApply(e => e % 1000, str => str.Count(), (g, c) => new StructTuple<long, ulong> { Item1 = g.Key, Item2 = c });
            var finalResultSequence = result
                .ToStreamEventObservable().Where(e => e.IsData)
                .ToEnumerable();
            var ct = finalResultSequence.Count();
            Assert.IsTrue(ct == 249000);
        }

        [TestMethod, TestCategory("Gated")]
        public void ShuffleStreamable3Columnar()
        {
            var savedUseMultiString = Config.UseMultiString;
            var savedScheduler = Config.StreamScheduler;
            Config.StreamScheduler = StreamScheduler.OwnedThreads(2);
            Config.UseMultiString = true;
            var input = Enumerable.Range(0, 100)
                .Select(i => new MyData { field1 = i, field2 = (i % 10).ToString(), });
            var stream = input.ToStatStreamable();
            var result = stream
                .GroupApply(e => e.field2, str => str.Count(), (g, c) => new StructTuple<string, int> { Item1 = g.Key, Item2 = (int)c, })
                .ToAtemporalObservable()
                .ToEnumerable()
                .OrderBy(e => e.Item1)
                .ToArray();
            var expected = input
                .GroupBy(e => e.field2, (k, v) => new StructTuple<string, int> { Item1 = k, Item2 = v.Count(), })
                .ToArray();
            Assert.IsTrue(expected.SequenceEqual(result));

            Config.StreamScheduler.Stop();
            Config.StreamScheduler = savedScheduler;
            Config.UseMultiString = savedUseMultiString;
        }

        [TestMethod, TestCategory("Gated")]
        public void ShuffleStreamable4Columnar()
        {
            var savedUseMultiString = Config.UseMultiString;
            Config.UseMultiString = true;

            var input = Enumerable.Range(0, 100)
                .Select(i => new MyData { field1 = i, field2 = (i % 10).ToString(), });

            var stream = input.ToStatStreamable();
            var result = stream
                .Shard(2)
                .ReKey(e => e.field2)
                .ReDistribute()
                .Query(str => str.Count())
                .SelectKey((g, c) => new StructTuple<string, int> { Item1 = g, Item2 = (int)c, })
                .Unshuffle()
                .Unshard()
                .ToAtemporalObservable()
                .ToEnumerable()
                .OrderBy(e => e.Item1)
                .ToArray();
            var ct = result.Length;
            var expected = input
                .GroupBy(e => e.field2, (k, v) => new StructTuple<string, int> { Item1 = k, Item2 = v.Count(), })
                .ToArray();

            Assert.IsTrue(expected.SequenceEqual(result));
            Config.UseMultiString = savedUseMultiString;
        }
    }

    [TestClass]
    public class ShuffleStreamableTestsColumnarSmallBatch : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public ShuffleStreamableTestsColumnarSmallBatch() : base(new ConfigModifier()
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true)
            .DataBatchSize(100)
            .MapArity(1)
            .ReduceArity(1))
        { }

        [TestMethod, TestCategory("Gated")]
        public void ShuffleStreamable1ColumnarSmallBatch()
        {
            var savedScheduler = Config.StreamScheduler;
            Config.StreamScheduler = StreamScheduler.OwnedThreads(2);
            var gameInput = new[]
            {
                StreamEvent.CreateStart(9900, new GameData { EventType = 0, GameId = 10, UserId = 100 }), // start game
                StreamEvent.CreateStart(10000, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 1 }),
                StreamEvent.CreateStart(10020, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 1 }),
                StreamEvent.CreateStart(10025, new GameData { EventType = 1, GameId = 10, UserId = 100, NumKills = 30 }),
                StreamEvent.CreateStart(10030, new GameData { EventType = 1, GameId = 10, UserId = 100 }), // end game
                StreamEvent.CreateStart(10040, new GameData { EventType = 2, GameId = 10 })
            }.ToObservable().ToStreamable();

            // clip each game event to end at the time of game completion
            var clippedGameInput =
                gameInput.Where(e => e.EventType < 2).ClipEventDuration(gameInput.Where(e => e.EventType == 2), e => e.GameId, e => e.GameId);

            var result =
                clippedGameInput
                .GroupApply(
                    e => new { e.GameId, e.UserId },
                    str => StreamableInternal.ComputeSignalChangeStream(str.Sum(e => e.NumKills)),
                    (g, c) => new { g.Key.GameId, g.Key.UserId, FromKills = c.Item1, ToKills = c.Item2 }) // count #kills per {game,user} combination
                ;

            var finalResultSequence = result
                .ToStreamEventObservable()
                .ToEnumerable()
                .ToList();
            var finalResult = finalResultSequence.First();

            Assert.IsTrue(finalResultSequence.Count() == 1 &&
                finalResult.IsPunctuation && finalResult.SyncTime == StreamEvent.InfinitySyncTime);

            Config.StreamScheduler.Stop();
            Config.StreamScheduler = savedScheduler;
        }

        [TestMethod, TestCategory("Gated")]
        public void ShuffleStreamable2ColumnarSmallBatch()
        {
            var input = Enumerable.Range(0, 10000000)
                .ToStatStreamable();
            var result = input.GroupApply(e => e % 1000, str => str.Count(), (g, c) => new StructTuple<long, ulong> { Item1 = g.Key, Item2 = c });
            var finalResultSequence = result
                .ToStreamEventObservable().Where(e => e.IsData)
                .ToEnumerable();
            var ct = finalResultSequence.Count();
            Assert.IsTrue(ct == 249000);
        }

        [TestMethod, TestCategory("Gated")]
        public void ShuffleStreamable3ColumnarSmallBatch()
        {
            var savedUseMultiString = Config.UseMultiString;
            var savedScheduler = Config.StreamScheduler;
            Config.StreamScheduler = StreamScheduler.OwnedThreads(2);
            Config.UseMultiString = true;
            var input = Enumerable.Range(0, 100)
                .Select(i => new MyData { field1 = i, field2 = (i % 10).ToString(), });
            var stream = input.ToStatStreamable();
            var result = stream
                .GroupApply(e => e.field2, str => str.Count(), (g, c) => new StructTuple<string, int> { Item1 = g.Key, Item2 = (int)c, })
                .ToAtemporalObservable()
                .ToEnumerable()
                .OrderBy(e => e.Item1)
                .ToArray();
            var expected = input
                .GroupBy(e => e.field2, (k, v) => new StructTuple<string, int> { Item1 = k, Item2 = v.Count(), })
                .ToArray();
            Assert.IsTrue(expected.SequenceEqual(result));

            Config.StreamScheduler.Stop();
            Config.StreamScheduler = savedScheduler;
            Config.UseMultiString = savedUseMultiString;
        }

        [TestMethod, TestCategory("Gated")]
        public void ShuffleStreamable4ColumnarSmallBatch()
        {
            var savedUseMultiString = Config.UseMultiString;
            Config.UseMultiString = true;

            var input = Enumerable.Range(0, 100)
                .Select(i => new MyData { field1 = i, field2 = (i % 10).ToString(), });

            var stream = input.ToStatStreamable();
            var result = stream
                .Shard(2)
                .ReKey(e => e.field2)
                .ReDistribute()
                .Query(str => str.Count())
                .SelectKey((g, c) => new StructTuple<string, int> { Item1 = g, Item2 = (int)c, })
                .Unshuffle()
                .Unshard()
                .ToAtemporalObservable()
                .ToEnumerable()
                .OrderBy(e => e.Item1)
                .ToArray();
            var ct = result.Length;
            var expected = input
                .GroupBy(e => e.field2, (k, v) => new StructTuple<string, int> { Item1 = k, Item2 = v.Count(), })
                .ToArray();

            Assert.IsTrue(expected.SequenceEqual(result));
            Config.UseMultiString = savedUseMultiString;
        }
    }

}
