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

        public override string ToString()
        {
            string str = "{ ";
            bool first = true;
            foreach (var e in this)
            {
                if (!first) str += ", ";
                str += "[" + e.ToString() + "]";
                first = false;
            }
            str += " }";
            return str;
        }
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
    }


    internal class AfaTests
    {
        public static void CoreAfaList01()
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
                .ToEnumerable()
                ;
            var expected = new StreamEvent<Empty>[]
            {
                StreamEvent.CreateInterval(2, 10, Empty.Default),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        public static void CoreAfaList02()
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
                .ToEnumerable()
                ;
            var expected = new StreamEvent<Empty>[]
            {
                StreamEvent.CreateInterval(140, 1100, Empty.Default),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        public static void CoreAfaMultiEvent01()
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
                    .ToArray()
                    ;
            var expected = new StreamEvent<Empty>[]
            {
                StreamEvent.CreateInterval(2, 10, Empty.Default),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        public static void CoreAfaMultiEvent02()
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
                    .ToEnumerable()
                    ;
            var x = result.ToArray();
            var expected = new StreamEvent<Empty>[] { StreamEvent.CreateInterval(1, 10, Empty.Default) };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        public static void CoreAfaMultiEvent03()
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
                    .ToArray()
                    ;
            var expected = new StreamEvent<Empty>[] { StreamEvent.CreateInterval(3, 11, Empty.Default) };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        public static void CoreAfaMultiEvent04()
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
                    .ToArray()
                    ;
            var expected = new StreamEvent<Empty>[] { StreamEvent.CreateInterval(3, 11, Empty.Default) };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        public static void AfaDefinePattern01()
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

        public static void AfaDefinePattern02()
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
                .ToArray()
                ;

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(110, 1100, 0),
                StreamEvent.CreateInterval(150, 1120, 14),
            };
            Assert.IsTrue(result.SequenceEqual(expected));

        }

        public static void AfaDefinePattern03()
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
                .ToArray()
                ;

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(110, 1100, 17),
            };
            Assert.IsTrue(result.SequenceEqual(expected));

        }

        public static void AfaPatternAiBi01()
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
                .ToArray()
                ;
            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(130, 1120, 1),
                StreamEvent.CreateInterval(140, 1110, 1),
                StreamEvent.CreateInterval(150, 1100, 1),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        public static void AfaPatternAiBi02()
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
                .Where(e => e.IsData)
                .ToEnumerable()
                .ToArray()
                ;
            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(150, 1100, 1),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        public static Register CreateOrAddToRegister(Payload2 p, Register r, bool isNegativeMatch)
        {
            var ret = new Register();
            if (r != null)
            {
                ret.IsNegative = ret.IsNegative || isNegativeMatch;
                ret.MatchedPayloads = r.MatchedPayloads.Add(p);
            }
            else
            {
                ret.IsNegative = isNegativeMatch;
                ret.MatchedPayloads = new FList<Payload2>() { p };
            }
            return ret;
        }

        public static void DAfa01()
        {
           // Config.CodegenOptions.BreakIntoCodeGen = Config.CodegenOptions.DebugFlags.Operators;
            var sequence = new List<string>() { "A", "B", "C", "A", "A", "A", "B" };
            var r = new Random(0);
            var source = new List<Payload2>();

            int count = 0;
            source = sequence.Select(a => new Payload2() { Session = "1", Field1 = a, Tick = count++ }).ToList();

            var trillSource = source.OrderBy(e => e.Tick)
                .Select(e => StreamEvent.CreateStart(e.Tick, e))
                .ToObservable().ToStreamable(DisorderPolicy.Drop())
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime);
            Afa<Payload2, Register, bool> pattern3 = ARegex.SingleElement<Payload2, Register>((t, ev, reg) => ev.Field1 == "C", (t, ev, reg) => reg);
            var registers =
                trillSource
                .Detect(pattern3, StreamEvent.InfinitySyncTime, false, false)
                .ToStreamEventObservable()
                .Where(evt => evt.IsData)
                .Select(evt => evt.Payload)
                .ToEnumerable()
                .ToList();
            Assert.IsTrue(registers.Count == 1 && registers[0] == null); // really just want to test that code gen didn't fail.
        }
    }

    [TestClass]
    public class AfaTestsWithoutCodegen : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public AfaTestsWithoutCodegen() : base(new ConfigModifier()
            .CodeGenAfa(false))
        { }

        [TestMethod, TestCategory("Gated")]
        public void CoreAfaListWithoutCodegen01() => AfaTests.CoreAfaList01();

        [TestMethod, TestCategory("Gated")]
        public void CoreAfaListWithoutCodegen02() => AfaTests.CoreAfaList02();

        [TestMethod, TestCategory("Gated")]
        public void CoreAfaMultiEventWithoutCodegen01() => AfaTests.CoreAfaMultiEvent01();

        [TestMethod, TestCategory("Gated")]
        public void CoreAfaMultiEventWithoutCodegen02() => AfaTests.CoreAfaMultiEvent02();

        [TestMethod, TestCategory("Gated")]
        public void CoreAfaMultiEventWithoutCodegen03() => AfaTests.CoreAfaMultiEvent03();

        [TestMethod, TestCategory("Gated")]
        public void CoreAfaMultiEventWithoutCodegen04() => AfaTests.CoreAfaMultiEvent04();

        [TestMethod, TestCategory("Gated")]
        public void AfaDefinePatternWithoutCodegen01() => AfaTests.AfaDefinePattern01();

        [TestMethod, TestCategory("Gated")]
        public void AfaDefinePatternWithoutCodegen02() => AfaTests.AfaDefinePattern02();

        [TestMethod, TestCategory("Gated")]
        public void AfaDefinePatternWithoutCodegen03() => AfaTests.AfaDefinePattern03();

        [TestMethod, TestCategory("Gated")]
        public void DAfaWithoutCodegen01() => AfaTests.DAfa01();

        [TestMethod, TestCategory("Gated")]
        public void AfaDefinePatternAiBi01WithoutCodegen() => AfaTests.AfaPatternAiBi01();

        [TestMethod, TestCategory("Gated")]
        public void AfaPatternAiBi02WithoutCodegen() => AfaTests.AfaPatternAiBi02();
    }
    [TestClass]
    public class AfaTestsWithCodegen : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public AfaTestsWithCodegen() : base(new ConfigModifier()
            .CodeGenAfa(true)
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true))
        { }

        [TestMethod, TestCategory("Gated")]
        public void CoreAfaListWithCodegen01() => AfaTests.CoreAfaList01();

        [TestMethod, TestCategory("Gated")]
        public void CoreAfaListWithCodegen02() => AfaTests.CoreAfaList02();

        [TestMethod, TestCategory("Gated")]
        public void CoreAfaMultiEventWithCodegen01() => AfaTests.CoreAfaMultiEvent01();

        [TestMethod, TestCategory("Gated")]
        public void CoreAfaMultiEventWithCodegen02() => AfaTests.CoreAfaMultiEvent02();

        [TestMethod, TestCategory("Gated")]
        public void CoreAfaMultiEventWithCodegen03() => AfaTests.CoreAfaMultiEvent03();

        [TestMethod, TestCategory("Gated")]
        public void CoreAfaMultiEventWithCodegen04() => AfaTests.CoreAfaMultiEvent04();

        [TestMethod, TestCategory("Gated")]
        public void AfaDefinePatternWithCodegen01() => AfaTests.AfaDefinePattern01();

        [TestMethod, TestCategory("Gated")]
        public void AfaDefinePatternWithCodegen02() => AfaTests.AfaDefinePattern02();

        [TestMethod, TestCategory("Gated")]
        public void AfaDefinePatternWithCodegen03() => AfaTests.AfaDefinePattern03();

        [TestMethod, TestCategory("Gated")]
        public void DAfaWithCodegen01() => AfaTests.DAfa01();

        [TestMethod, TestCategory("Gated")]
        public void AfaDefinePatternAiBi01WithCodegen() => AfaTests.AfaPatternAiBi01();

        [TestMethod, TestCategory("Gated")]
        public void AfaPatternAiBi02WithCodegen() => AfaTests.AfaPatternAiBi02();
    }
}