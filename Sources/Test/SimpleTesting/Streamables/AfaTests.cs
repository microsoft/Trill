// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [DataContract]
    public class FList<T> : List<T>
    {
        public FList()
            : base()
        { }

        public FList(int capacity)
            : base(capacity)
        { }

        public FList(IEnumerable<T> collection)
            : base(collection)
        { }

        public new FList<T> Add(T t)
        {
            base.Add(t);
            return this;
        }

        public FList<T> Clone() => new FList<T>(this);

        public new FList<T> Clear()
        {
            base.Clear();
            return this;
        }

        public override string ToString() => $"{{ {string.Join(", ", this.Select(e => $"[{e}]"))} }}";
    }

    public struct AfaPayload
    {
        public string Field1;
        public int Field2;

        public override string ToString() => new { this.Field1, this.Field2 }.ToString();
    }

    public class Payload2 : IComparable<Payload2>
    {
        private static long nextTick = 0;

        public long Tick;
        public string Session;
        public long SessionLong;
        public string Field1;
        public bool IsStart = false;
        public bool IsEnd = false;

        public Payload2() => this.Tick = nextTick++;

        public override string ToString() => new { this.Field1 }.ToString();

        public int CompareTo(Payload2 other) => other != null ? this.Tick.CompareTo(other.Tick) : -1;
    }

    public class Register
    {
        public bool IsNegative { get; set; }
        public FList<Payload2> MatchedPayloads { get; set; }

        public static Register AggregateRegister(Register reg, Payload2 payload, bool isNegativeMatch) =>
            new Register
            {
                IsNegative = (reg?.IsNegative ?? default) || isNegativeMatch,
                MatchedPayloads = (reg?.MatchedPayloads.Clone() ?? new FList<Payload2>()).Add(payload)
            };
    }

    public abstract class AfaTests : TestWithConfigSettingsAndMemoryLeakDetection
    {
        internal AfaTests(ConfigModifier modifer) : base(modifer)
        { }

        [TestMethod, TestCategory("Gated")]
        public void CoreAfaList01()
        {
            var pat1 = Afa.Create<long>();
            pat1.AddListElementArc(0, 1, fence: (ts, ev, reg) => ev.Contains(0));
            pat1.AddListElementArc(1, 1, fence: (ts, ev, reg) => !ev.Contains(1));
            pat1.AddListElementArc(1, 2, fence: (ts, ev, reg) => ev.Contains(1));

            var result =
                new long[] { 0, 0, 1 }
                .Select((e, i) => StreamEvent.CreatePoint<long>(i, e))
                .ToObservable()
                .ToStreamable()
                .SetProperty().IsSyncTimeSimultaneityFree(true)
                .AlterEventDuration(10)
                .Detect(pat1, isDeterministic: true, allowOverlappingInstances: false)
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .ToEnumerable();
            var expected = new StreamEvent<Empty>[]
            {
                StreamEvent.CreateInterval(2, 10, Empty.Default),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void CoreAfaList02()
        {
            var pat1 = Afa.Create<AfaPayload>();
            pat1.AddListElementArc(0, 1, fence: (ts, events, reg) => events.Any(p => p.Field2 == 0));
            pat1.AddListElementArc(1, 1, fence: (ts, events, reg) => !events.Any(p => p.Field2 == 1));
            pat1.AddListElementArc(1, 2, fence: (ts, events, reg) => events.Any(p => p.Field2 == 1));

            var input = new StreamEvent<AfaPayload>[]
            {
                StreamEvent.CreatePoint(100, new AfaPayload { Field1 = "A", Field2 = 0 }),
                StreamEvent.CreatePoint(110, new AfaPayload { Field1 = "C", Field2 = 0 }),
                StreamEvent.CreatePoint(140, new AfaPayload { Field1 = "B", Field2 = 1 }),
            };
            var result = input
                .ToObservable()
                .ToStreamable()
                .SetProperty().IsSyncTimeSimultaneityFree(true)
                .AlterEventDuration(1000)
                .Detect(pat1, isDeterministic: true, allowOverlappingInstances: false)
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .ToEnumerable();
            var expected = new StreamEvent<Empty>[]
            {
                StreamEvent.CreateInterval(140, 1100, Empty.Default),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void CoreAfaMultiEvent01()
        {
            var pat1 = new Afa<long, Empty, bool>();
            pat1.AddMultiElementArc(0, 1,
                initialize: (ts, reg) => false,
                accumulate: (ts, ev, reg, acc) => acc ? acc : (ev == 0), // Any element is 0
                fence: (ts, acc, reg) => acc);

            pat1.AddMultiElementArc(1, 1,
                initialize: (ts, reg) => false,
                accumulate: (ts, ev, reg, acc) => acc ? acc : (ev == 1), // Any element is 1
                fence: (ts, acc, reg) => !acc);

            pat1.AddMultiElementArc(1, 2,
                initialize: (ts, reg) => false,
                accumulate: (ts, ev, reg, acc) => acc ? acc : (ev == 1), // Any element is 1
                fence: (ts, acc, reg) => acc);

            var result =
                new long[] { 0, 0, 1 }
                .Select((e, i) => StreamEvent.CreatePoint<long>(i, e))
                    .ToObservable()
                    .ToStreamable()
                    .SetProperty().IsSyncTimeSimultaneityFree(true)
                    .AlterEventDuration(10)
                    .Detect(pat1, isDeterministic: true, allowOverlappingInstances: false)
                    .ToStreamEventObservable()
                    .Where(e => e.IsData)
                    .ToEnumerable()
                    .ToArray();
            var expected = new StreamEvent<Empty>[]
            {
                StreamEvent.CreateInterval(2, 10, Empty.Default),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void CoreAfaMultiEvent02()
        {
            var pat1 = new Afa<long, Empty, bool>();
            pat1.AddMultiElementArc(0, 1,
                initialize: (ts, reg) => false,
                accumulate: (ts, ev, reg, acc) => acc ? acc : (ev == 0), // Any element is 0
                fence: (ts, acc, reg) => acc);

            pat1.AddMultiElementArc(1, 1,
                initialize: (ts, reg) => false,
                accumulate: (ts, ev, reg, acc) => acc ? acc : (ev == 1), // Any element is 1
                fence: (ts, acc, reg) => !acc);

            pat1.AddMultiElementArc(1, 2,
                    initialize: (ts, reg) => false,
                    accumulate: (ts, ev, reg, acc) => acc ? acc : (ev == 1), // Any element is 1
                    fence: (ts, acc, reg) => acc);

            var result =
                new Tuple<int, int>[] { Tuple.Create(0, 0), Tuple.Create(0, 0), Tuple.Create(1, 1) }
                .Select(e => StreamEvent.CreatePoint<long>(e.Item1, e.Item2))
                    .ToObservable()
                    .ToStreamable()
                    .SetProperty().IsSyncTimeSimultaneityFree(true)
                    .AlterEventDuration(10)
                    .Detect(pat1, isDeterministic: true, allowOverlappingInstances: false)
                    .ToStreamEventObservable()
                    .Where(e => e.IsData)
                    .ToEnumerable();
            var x = result.ToArray();
            var expected = new StreamEvent<Empty>[] { StreamEvent.CreateInterval(1, 10, Empty.Default) };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void CoreAfaMultiEvent03()
        {
            var pat1 = new Afa<long, Empty, bool>();
            pat1.AddSingleElementArc(0, 1,
                fence: (ts, ev, reg) => ev == 0);

            pat1.AddMultiElementArc(1, 1,
                initialize: (ts, reg) => false,
                accumulate: (ts, ev, reg, acc) => acc ? acc : (ev == 1), // Any element is 1
                fence: (ts, acc, reg) => !acc);

            pat1.AddMultiElementArc(1, 2,
                initialize: (ts, reg) => false,
                accumulate: (ts, ev, reg, acc) => acc ? acc : (ev == 1), // Any element is 1
                fence: (ts, acc, reg) => acc);

            var result =
                new StreamEvent<long>[]
                {
                    StreamEvent.CreateStart<long>(0, 0),
                    StreamEvent.CreateStart<long>(0, 0),
                    StreamEvent.CreateStart<long>(1, 0),
                    StreamEvent.CreateStart<long>(2, 0),
                    StreamEvent.CreateStart<long>(2, 0),
                    StreamEvent.CreateStart<long>(2, 0),
                    StreamEvent.CreateStart<long>(3, 1),
                    StreamEvent.CreateStart<long>(3, 0),
                }
                    .ToObservable()
                    .ToStreamable()
                    .SetProperty().IsSyncTimeSimultaneityFree(true)
                    .AlterEventDuration(10)
                    .Detect(pat1, isDeterministic: true, allowOverlappingInstances: false)
                    .ToStreamEventObservable()
                    .Where(e => e.IsData)
                    .ToEnumerable()
                    .ToArray();
            var expected = new StreamEvent<Empty>[] { StreamEvent.CreateInterval(3, 11, Empty.Default) };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void CoreAfaMultiEvent04()
        {
            var pat1 = new Afa<long, Empty, bool>();
            pat1.AddSingleElementArc(0, 1,
                fence: (ts, ev, reg) => ev == 0);

            pat1.AddMultiElementArc(1, 1,
                initialize: (ts, reg) => false,
                accumulate: (ts, ev, reg, acc) => acc ? acc : (ev == 1), // Any element is 1
                fence: (ts, acc, reg) => !acc);

            pat1.AddListElementArc(1, 2,
                fence: (ts, listev, reg) => listev.Contains(1));

            var result =
                new StreamEvent<long>[]
                {
                    StreamEvent.CreateStart<long>(0, 0),
                    StreamEvent.CreateStart<long>(0, 0),
                    StreamEvent.CreateStart<long>(1, 0),
                    StreamEvent.CreateStart<long>(2, 0),
                    StreamEvent.CreateStart<long>(2, 0),
                    StreamEvent.CreateStart<long>(2, 0),
                    StreamEvent.CreateStart<long>(3, 1),
                    StreamEvent.CreateStart<long>(3, 0),
                }
                    .ToObservable()
                    .ToStreamable()
                    .SetProperty().IsSyncTimeSimultaneityFree(true)
                    .AlterEventDuration(10)
                    .Detect(pat1, isDeterministic: true, allowOverlappingInstances: false)
                    .ToStreamEventObservable()
                    .Where(e => e.IsData)
                    .ToEnumerable()
                    .ToArray();
            var expected = new StreamEvent<Empty>[] { StreamEvent.CreateInterval(3, 11, Empty.Default) };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void AfaDefinePattern01()
        {
            var source1 = new StreamEvent<AfaPayload>[]
            {
                StreamEvent.CreatePoint(100, new AfaPayload { Field1 = "A", Field2 = 4 }),
                StreamEvent.CreatePoint(110, new AfaPayload { Field1 = "C", Field2 = 3 }),
                StreamEvent.CreatePoint(120, new AfaPayload { Field1 = "A", Field2 = 1 }),
                StreamEvent.CreatePoint(130, new AfaPayload { Field1 = "B", Field2 = 6 }),
                StreamEvent.CreatePoint(140, new AfaPayload { Field1 = "B", Field2 = 8 }),
                StreamEvent.CreatePoint(150, new AfaPayload { Field1 = "C", Field2 = 7 }),
                StreamEvent.CreatePoint(160, new AfaPayload { Field1 = "B", Field2 = 9 }),
            }.ToObservable().ToStreamable().AlterEventDuration(1000);
            var result =
                source1
                .Detect(
                    0,
                    p => p
                        .SingleElement(e => e.Field1 == "A")
                        .KleeneStar(r => r.SingleElement(e => e.Field1 == "B", (ev, d) => d + ev.Field2))
                        .SingleElement(e => e.Field1 == "C"))
                        .ToStreamEventObservable()
                        .Where(e => e.IsData)
                        .ToEnumerable()
                        .ToArray();

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(110, 1100, 0),
                StreamEvent.CreateInterval(150, 1120, 14),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void AfaDefinePattern02()
        {
            var source1 = new StreamEvent<AfaPayload>[]
            {
                StreamEvent.CreatePoint(100, new AfaPayload { Field1 = "A", Field2 = 4 }),
                StreamEvent.CreatePoint(110, new AfaPayload { Field1 = "C", Field2 = 3 }),
                StreamEvent.CreatePoint(120, new AfaPayload { Field1 = "A", Field2 = 1 }),
                StreamEvent.CreatePoint(130, new AfaPayload { Field1 = "B", Field2 = 6 }),
                StreamEvent.CreatePoint(140, new AfaPayload { Field1 = "B", Field2 = 8 }),
                StreamEvent.CreatePoint(150, new AfaPayload { Field1 = "C", Field2 = 7 }),
                StreamEvent.CreatePoint(160, new AfaPayload { Field1 = "B", Field2 = 9 }),
            }.ToObservable().ToStreamable().AlterEventDuration(1000);
            var result =
                source1
                .DefinePattern(0)
                .SingleElement(e => e.Field1 == "A")
                .KleeneStar(r => r.SingleElement(e => e.Field1 == "B", (ev, d) => d + ev.Field2))
                .SingleElement(e => e.Field1 == "C")
                .Detect()
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .ToEnumerable()
                .ToArray();

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(110, 1100, 0),
                StreamEvent.CreateInterval(150, 1120, 14),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void AfaDefinePattern03()
        {
            var source1 = new StreamEvent<AfaPayload>[]
            {
                StreamEvent.CreatePoint(100, new AfaPayload { Field1 = "A", Field2 = 4 }),
                StreamEvent.CreatePoint(110, new AfaPayload { Field1 = "C", Field2 = 3 }),
                StreamEvent.CreatePoint(120, new AfaPayload { Field1 = "A", Field2 = 1 }),
                StreamEvent.CreatePoint(130, new AfaPayload { Field1 = "B", Field2 = 6 }),
                StreamEvent.CreatePoint(140, new AfaPayload { Field1 = "B", Field2 = 8 }),
                StreamEvent.CreatePoint(150, new AfaPayload { Field1 = "C", Field2 = 7 }),
                StreamEvent.CreatePoint(160, new AfaPayload { Field1 = "B", Field2 = 9 }),
            }.ToObservable().ToStreamable().AlterEventDuration(1000);
            var result =
                source1
                .DefinePattern(10)
                .SingleElement(e => e.Field1 == "A", (l, p, i) => i + p.Field2)
                .SingleElement(e => e.Field1 == "C", (l, p, i) => i + p.Field2)
                .Detect()
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .ToEnumerable()
                .ToArray();

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(110, 1100, 17),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void AfaPatternAiBi01()
        {
            var source1 = new StreamEvent<AfaPayload>[]
            {
                StreamEvent.CreatePoint(100, new AfaPayload { Field1 = "A", Field2 = 4 }),
                StreamEvent.CreatePoint(110, new AfaPayload { Field1 = "A", Field2 = 3 }),
                StreamEvent.CreatePoint(120, new AfaPayload { Field1 = "A", Field2 = 1 }),
                StreamEvent.CreatePoint(130, new AfaPayload { Field1 = "B", Field2 = 6 }),
                StreamEvent.CreatePoint(140, new AfaPayload { Field1 = "B", Field2 = 8 }),
                StreamEvent.CreatePoint(150, new AfaPayload { Field1 = "B", Field2 = 9 }),
                StreamEvent.CreatePoint(160, new AfaPayload { Field1 = "B", Field2 = 9 }),
            }.ToObservable().ToStreamable().AlterEventDuration(1000);
            var result =
                source1
                .Detect(0, p => p
                    .KleeneStar(x => x.SingleElement((ts, e, r) => e.Field1 == "A", (ev, r) => r + 1))
                    .KleeneStar(x => x.SingleElement((ts, e, r) => e.Field1 == "B" && r > 1, (ev, r) => r - 1))
                    .SingleElement((ts, e, r) => e.Field1 == "B" && r == 1))
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .ToEnumerable()
                .ToArray();
            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(130, 1120, 1),
                StreamEvent.CreateInterval(140, 1110, 1),
                StreamEvent.CreateInterval(150, 1100, 1),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void AfaPatternAiBi02()
        {
            var source1 = new StreamEvent<AfaPayload>[]
            {
                StreamEvent.CreatePoint(100, new AfaPayload { Field1 = "A", Field2 = 4 }),
                StreamEvent.CreatePoint(110, new AfaPayload { Field1 = "A", Field2 = 3 }),
                StreamEvent.CreatePoint(120, new AfaPayload { Field1 = "A", Field2 = 1 }),
                StreamEvent.CreatePoint(130, new AfaPayload { Field1 = "B", Field2 = 6 }),
                StreamEvent.CreatePoint(140, new AfaPayload { Field1 = "B", Field2 = 8 }),
                StreamEvent.CreatePoint(150, new AfaPayload { Field1 = "B", Field2 = 9 }),
                StreamEvent.CreatePoint(160, new AfaPayload { Field1 = "B", Field2 = 9 }),
            }.ToObservable().ToStreamable().AlterEventDuration(1000);
            var result =
                source1
                .Detect(0, p => p
                    .KleeneStar(x => x.SingleElement((ts, e, r) => e.Field1 == "A", (ev, r) => r + 1))
                    .KleeneStar(x => x.SingleElement((ts, e, r) => e.Field1 == "B" && r > 1, (ev, r) => r - 1))
                    .SingleElement((ts, e, r) => e.Field1 == "B" && r == 1),
                    allowOverlappingInstances: false)
                .ToStreamEventObservable()
                .Where(evt => evt.IsData)
                .ToEnumerable()
                .ToArray();
            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(150, 1100, 1),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void DAfa01()
        {
            var count = 0;
            var source = new List<string>() { "A", "B", "C", "A", "A", "A", "B" }
                .Select(a => new Payload2() { Session = "1", Field1 = a, Tick = count++ })
                .OrderBy(e => e.Tick)
                .ToList()
                .Select(e => StreamEvent.CreateStart(e.Tick, e))
                .ToObservable().ToStreamable(DisorderPolicy.Drop())
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime);
            var pattern3 = ARegex.SingleElement<Payload2, Register>(
                (t, ev, reg) => ev.Field1 == "C",
                (t, ev, reg) => reg);
            var registers =
                source
                .Detect(pattern3, StreamEvent.InfinitySyncTime, false, false)
                .ToStreamEventObservable()
                .Where(evt => evt.IsData)
                .Select(evt => evt.Payload)
                .ToEnumerable()
                .ToList();
            Assert.IsTrue(registers.Count == 1 && registers[0] == null); // really just want to test that code gen didn't fail.
        }

        [TestMethod, TestCategory("Gated")]
        public void DAfa02()
        {
            var count = 0;
            var source = new List<string>() { "A", "B", "C", "A", "A", "A", "B" }
                .Select(a => new Payload2() { Session = "1", Field1 = a, Tick = count++ })
                .OrderBy(e => e.Tick)
                .Select(e => StreamEvent.CreateStart(e.Tick, e))
                .ToList()
                .ToObservable().ToStreamable(DisorderPolicy.Drop())
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime);
            var pattern3 = ARegex.SingleElement<Payload2, Register>(
                (t, ev, reg) => ev.Field1 == "C",
                (t, ev, reg) => Register.AggregateRegister(reg, ev, false));
            var registers =
                source
                .Detect(pattern3, StreamEvent.InfinitySyncTime, false, false)
                .Where(register => register.MatchedPayloads.Count > 0)
                .ToStreamEventObservable()
                .Where(evt => evt.IsData)
                .Select(evt => evt.Payload)
                .ToEnumerable()
                .ToList();
            Assert.IsTrue(registers.Count == 1 && registers[0].MatchedPayloads.Count == 1);
        }

        [TestMethod, TestCategory("Gated")]
        public void AfaZeroOrOne()
        {
            var source = new StreamEvent<string>[]
            {
                StreamEvent.CreateStart(0, "A"),
                StreamEvent.CreateStart(1, "C"),
                StreamEvent.CreateStart(2, "B"),
                StreamEvent.CreateStart(3, "B"),
                StreamEvent.CreateStart(4, "C"),
                StreamEvent.CreateStart(5, "A"),
                StreamEvent.CreateStart(6, "B"),
                StreamEvent.CreateStart(7, "B"),
                StreamEvent.CreateStart(8, "B"),
                StreamEvent.CreateStart(9, "A"),
            }.ToObservable()
                .ToStreamable()
                .AlterEventDuration(2);

            var afa = ARegex.Concat(
                ARegex.SingleElement<string, string>(
                    (time, @event, state) => @event == "A",
                    (time, @event, state) => @event),
                ARegex.ZeroOrOne(
                    ARegex.SingleElement<string, string>(
                        (time, @event, state) => @event == "B",
                        (time, @event, state) => state + @event)));

            var result = source
                .Detect(
                    afa,
                    allowOverlappingInstances: false,
                    isDeterministic: false)
                .ToStreamEventObservable()
                .Where(evt => evt.IsData)
                .ToEnumerable()
                .ToArray();
            var expected = new StreamEvent<string>[]
            {
                StreamEvent.CreateInterval(0, 2, "A"),
                StreamEvent.CreateInterval(5, 7, "A"),
                StreamEvent.CreateInterval(6, 7, "AB"),
                StreamEvent.CreateInterval(9, 11, "A"),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        internal class State
        {
            // This cannot be an enum because we want represent a concatinated state in terms of digits in int value
            public const int A = 1;
            public const int B = 2;
            public const int C = 3;
        }

        [TestMethod, TestCategory("Gated")]
        public void AfaZeroOrOneWithStartEvent()
        {
            var source = new StreamEvent<int>[]
            {
                StreamEvent.CreateStart(0, State.A),
                StreamEvent.CreateStart(1, State.C),
                StreamEvent.CreateStart(2, State.B),
                StreamEvent.CreateStart(3, State.B),
                StreamEvent.CreateStart(4, State.C),
                StreamEvent.CreateStart(5, State.A),
                StreamEvent.CreateStart(6, State.B),
                StreamEvent.CreateStart(7, State.B),
                StreamEvent.CreateStart(8, State.B),
                StreamEvent.CreateStart(9, State.A),
            }.ToObservable()
                .ToStreamable()
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime);

            // Assert we are actually testing columnar
            Assert.IsTrue(source.Properties.IsColumnar);

            var afa = ARegex.Concat(
                ARegex.SingleElement<int, int>(
                    (time, @event, state) => @event == State.A,
                    (time, @event, state) => @event),
                ARegex.ZeroOrOne(
                    ARegex.SingleElement<int, int>(
                        (time, @event, state) => @event == State.B,
                        (time, @event, state) => state * 10 + @event)));

            var result = source
                .Detect(
                    afa,
                    allowOverlappingInstances: false,
                    isDeterministic: false)
                .ToStreamEventObservable()
                .Where(evt => evt.IsData)
                .ToEnumerable()
                .ToArray();

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateStart(0, State.A),
                StreamEvent.CreateStart(5, State.A),
                StreamEvent.CreateStart(6, State.A * 10 + State.B),
                StreamEvent.CreateStart(9, State.A),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void AfaZeroOrOneInsideOr()
        {
            var source = new StreamEvent<string>[]
            {
                StreamEvent.CreateStart(0, "A"),
                StreamEvent.CreateStart(1, "C"),
                StreamEvent.CreateStart(2, "B"),
                StreamEvent.CreateStart(3, "B"),
                StreamEvent.CreateStart(4, "C"),
                StreamEvent.CreateStart(5, "A"),
                StreamEvent.CreateStart(6, "B"),
                StreamEvent.CreateStart(7, "B"),
                StreamEvent.CreateStart(8, "B"),
                StreamEvent.CreateStart(9, "A"),
            }.ToObservable()
                .ToStreamable()
                .AlterEventDuration(3);

            // Create Regex for "A (C? | BB)
            var afa = ARegex.Concat(
                ARegex.SingleElement<string, string>(
                    (time, @event, state) => @event == "A",
                    (time, @event, state) => @event),
                ARegex.Or(
                    ARegex.ZeroOrOne(
                        ARegex.SingleElement<string, string>(
                            (time, @event, state) => @event == "C",
                            (time, @event, state) => state + @event)),
                    ARegex.Concat(
                        ARegex.SingleElement<string, string>(
                            (time, @event, state) => @event == "B",
                            (time, @event, state) => state + @event),
                        ARegex.SingleElement<string, string>(
                            (time, @event, state) => @event == "B",
                            (time, @event, state) => state + @event))));

            var result = source
                .Detect(
                    afa,
                    allowOverlappingInstances: false,
                    isDeterministic: false)
                .ToStreamEventObservable()
                .Where(evt => evt.IsData)
                .ToEnumerable()
                .ToArray();
            var expected = new StreamEvent<string>[]
            {
                StreamEvent.CreateInterval(0, 3, "A"),
                StreamEvent.CreateInterval(1, 3, "AC"),
                StreamEvent.CreateInterval(5, 8, "A"),
                StreamEvent.CreateInterval(7, 8, "ABB"),
                StreamEvent.CreateInterval(9, 12, "A"),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }
    }

    [TestClass]
    public class AfaTestsWithoutCodegen : AfaTests
    {
        public AfaTestsWithoutCodegen() : base(new ConfigModifier().CodeGenAfa(false))
        { }
    }

    [TestClass]
    public class AfaTestsWithCodegen : AfaTests
    {
        public AfaTestsWithCodegen() : base(new ConfigModifier()
            .CodeGenAfa(true)
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true))
        { }
    }
}