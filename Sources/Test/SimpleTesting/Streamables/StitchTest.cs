// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class StitchTest : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private static readonly StreamEvent<string> END = StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime);

        public StitchTest()
            : base(new ConfigModifier().ForceRowBasedExecution(true))
        { }

        public static void StitchTestMultisetMultiStart()
        {
            // nothing interesting happens here
            var inputList = new[]
            {
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateStart(2, "A"),
                StreamEvent.CreateEnd(3, 2, "A"),
                StreamEvent.CreateStart(3, "A"),
                StreamEvent.CreateEnd(4, 3, "A"),
                StreamEvent.CreateStart(4, "A"),
                StreamEvent.CreateEnd(5, 1, "A"),
                StreamEvent.CreateEnd(6, 5, "A"),
                StreamEvent.CreateEnd(7, 4, "A")
            };

            var compareTo = new[]
            {
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateStart(2, "A"),
                StreamEvent.CreateEnd(6, 1, "A"), // 6->5->1
                StreamEvent.CreateEnd(7, 2, "A"), // 7->4->3->2
                END
            };

            var input = inputList.ToList().ToObservable().ToStreamable();
            var outputStream = input.Stitch();

            Assert.IsTrue(outputStream.IsEquivalentTo(compareTo));
        }

        [TestMethod, TestCategory("Gated")]
        public void StitchTestPassthrough()
        {
            var inputList = new List<StreamEvent<string>>
            {
                // nothing interesting happens here
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateStart(2, "B"),
                StreamEvent.CreateEnd(3, 1, "A"),
                StreamEvent.CreateEnd(4, 2, "B")
            };
            var input = inputList.ToObservable().ToStreamable();
            var outputStream = input.Stitch();

            var compareTo = new[]
            {
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateStart(2, "B"),
                StreamEvent.CreateEnd(3, 1, "A"),
                StreamEvent.CreateEnd(4, 2, "B"),
                END
            };

            Assert.IsTrue(outputStream.IsEquivalentTo(compareTo));
        }

        [TestMethod, TestCategory("Gated")]
        public void StitchTestSimpleSplice()
        {
            // nothing interesting happens here
            var inputList = new[]
            {
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateEnd(2, 1, "A"),
                StreamEvent.CreateStart(2, "A"),
                StreamEvent.CreateEnd(3, 2, "A"),
                StreamEvent.CreateStart(3, "A"),
                StreamEvent.CreateEnd(4, 3, "A"),
                StreamEvent.CreateStart(4, "A"),
                StreamEvent.CreateEnd(5, 4, "A")
            };

            var compareTo = new[]
            {
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateEnd(5, 1, "A"),
                END
            };

            var input = inputList.ToObservable().ToStreamable();
            var outputStream = input.Stitch();

            Assert.IsTrue(outputStream.IsEquivalentTo(compareTo));
        }

        [TestMethod, TestCategory("Gated")]
        public void StitchTestMultisetOneStart()
        {
            // Two different start times
            var inputList = new[]
            {
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateEnd(2, 1, "A"),
                StreamEvent.CreateStart(2, "A"),
                StreamEvent.CreateEnd(3, 2, "A"),
                StreamEvent.CreateStart(3, "A"),
                StreamEvent.CreateEnd(4, 3, "A"),
                StreamEvent.CreateStart(4, "A"),
                StreamEvent.CreateEnd(5, 4, "A"),
                StreamEvent.CreateEnd(6, 1, "A")
            };

            var compareTo = new[]
            {
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateEnd(5, 1, "A"), // 5->4->3->2->1
                StreamEvent.CreateEnd(6, 1, "A"), // 6->1
                END
            };

            var input = inputList.ToObservable().ToStreamable();
            var outputStream = input.Stitch();

            Assert.IsTrue(outputStream.IsEquivalentTo(compareTo));
        }

        // Is this even slower?
        [TestMethod, TestCategory("Gated")]
        public void StitchTestMultisetExtraSlowStart()
        {
            // Two different start times
            var inputList = new[]
            {
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateEnd(2, 1, "A"),
                StreamEvent.CreateEnd(2, 1, "A"),
                StreamEvent.CreateStart(2, "A"),
                StreamEvent.CreateStart(2, "A"),
                StreamEvent.CreateEnd(3, 2, "A"),
                StreamEvent.CreateEnd(3, 2, "A"),
                StreamEvent.CreateStart(3, "A"),
                StreamEvent.CreateStart(3, "A"),
                StreamEvent.CreateEnd(4, 3, "A"),
                StreamEvent.CreateEnd(4, 3, "A"),
                StreamEvent.CreateStart(4, "A"),
                StreamEvent.CreateStart(4, "A"),
                StreamEvent.CreateEnd(5, 4, "A"),
                StreamEvent.CreateEnd(5, 4, "A"),
                StreamEvent.CreateEnd(6, 1, "A")
            };

            var compareTo = new[]
            {
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateEnd(5, 1, "A"), // 5->4->3->2->1
                StreamEvent.CreateEnd(5, 1, "A"),
                StreamEvent.CreateEnd(6, 1, "A"),
                END
            };

            var input = inputList.ToObservable().ToStreamable();
            var outputStream = input.Stitch();

            Assert.IsTrue(outputStream.IsEquivalentTo(compareTo));
        }

        [TestMethod, TestCategory("Gated")]
        public void StitchTestTimeInvert()
        {
            // This is the odd little case where we swapped "BEGIN" and "END" at both times 3 and 4
            // so that they arrive in the opposite order. This shouldn't trouble the system at all:
            // Everything that happens at Time 3 is identical. Right? Right!
            var inputList = new[]
            {
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateEnd(2, 1, "A"),
                StreamEvent.CreateStart(2, "A"),
                StreamEvent.CreateStart(3, "A"),
                StreamEvent.CreateEnd(3, 2, "A"),
                StreamEvent.CreateStart(4, "A"),
                StreamEvent.CreateEnd(4, 3, "A"),
                StreamEvent.CreateEnd(5, 4, "A")
            };

            var compareTo = new[]
            {
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateEnd(5, 1, "A"),
                END
            };

            var input = inputList.ToObservable().ToStreamable();
            var outputStream = input.Stitch();

            Assert.IsTrue(outputStream.IsEquivalentTo(compareTo));
        }

        [TestMethod, TestCategory("Gated")]
        public void StitchTestDictionary()
        {
            // We have done horrible thigns to the dictionary: all elements are
            // identical!
            var inputList = new[]
            {
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateEnd(2, 1, "A"),
                StreamEvent.CreateStart(2, "B"),
                StreamEvent.CreateEnd(3, 2, "B"),
                StreamEvent.CreateStart(3, "B"),
                StreamEvent.CreateEnd(4, 3, "A"),
                StreamEvent.CreateStart(4, "A"),
                StreamEvent.CreateEnd(5, 4, "B")
            };

            var compareTo = new[]
            {
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateEnd(5, 1, "A"),
                END
            };

            var input = inputList.ToObservable().ToStreamable()
                .SetProperty()
                .PayloadEqualityComparer(new EqualityComparerExpression<string>(
                    (str1, str2) => (str1 == str2) ||
                    (str1 == "A" && str2 == "B") ||
                    (str1 == "B" && str2 == "A"),
                    str => 7));
            var outputStream = input.Stitch();

            Assert.IsTrue(outputStream.IsEquivalentTo(compareTo));
        }

        private static IList<StreamEvent<T>> Execute<T>(
            IEnumerable<StreamEvent<T>> input,
            Func<IStreamable<Empty, T>, IStreamable<Empty, T>> selector)
            => selector(input
                .ToObservable()
                .ToStreamable())
            .ToStreamEventObservable()
            .ToList()
            .Wait();

        [TestMethod]
        public void Stitch_Points()
        {
            StreamEvent<int>[] input = new[]
            {
                StreamEvent.CreatePoint(100, 1),
                StreamEvent.CreatePoint(102, 1),
            };

            IList<StreamEvent<int>> result = Execute(input, x => x.Stitch());

            StreamEvent<int>[] expected = new[]
            {
                StreamEvent.CreateStart(100, 1),
                StreamEvent.CreateEnd(101, 100, 1),
                StreamEvent.CreateStart(102, 1),
                StreamEvent.CreateEnd(103, 102, 1),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime),
            };

            Assert.IsTrue(expected.SequenceEqual(result));
        }

        [TestMethod]
        public void Stitch_Edges()
        {
            StreamEvent<int>[] input = new[]
            {
                StreamEvent.CreateStart(104,      1),
                StreamEvent.CreateStart(105,      2),
                StreamEvent.CreateEnd(106, 104, 1),
                StreamEvent.CreateEnd(106, 105, 2),
            };

            IList<StreamEvent<int>> result = Execute(input, x => x.Stitch());

            StreamEvent<int>[] expected = new[]
            {
                StreamEvent.CreateStart(104,      1),
                StreamEvent.CreateStart(105,      2),
                StreamEvent.CreateEnd(106, 104, 1),
                StreamEvent.CreateEnd(106, 105, 2),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime),
            };

            Assert.IsTrue(expected.SequenceEqual(result));
        }
    }

    [TestClass]
    public class StitchTestCodegen : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public StitchTestCodegen()
            : base(new ConfigModifier()
                        .ForceRowBasedExecution(false)
                        .DontFallBackToRowBasedExecution(true))
        { }

        private static readonly StreamEvent<StructTuple<string, int>> END = StreamEvent.CreatePunctuation<StructTuple<string, int>>(StreamEvent.InfinitySyncTime);

        public static void StitchTestMultisetMultiStartCodegen()
        {
            // nothing interesting happens here
            var inputList = new[]
            {
                StreamEvent.CreateStart(1, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(2, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(3, 2, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(3, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(4, 3, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(4, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(5, 1, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(6, 5, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(7, 4, StructTuple.Create("A", 3))
            };

            var compareTo = new[]
            {
                StreamEvent.CreateStart(1, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(2, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(6, 1, StructTuple.Create("A", 3)), // 6->5->1
                StreamEvent.CreateEnd(7, 2, StructTuple.Create("A", 3)), // 7->4->3->2
                END
            };

            var input = inputList.ToList().ToObservable().ToStreamable();
            var outputStream = input.Stitch();

            var output = outputStream.ToStreamEventObservable(ReshapingPolicy.None).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(compareTo));
        }

        [TestMethod, TestCategory("Gated")]
        public void StitchTestPassthroughCodegen()
        {
            var inputList = new List<StreamEvent<StructTuple<string, int>>>
            {
                // nothing interesting happens here
                StreamEvent.CreateStart(1, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(2, StructTuple.Create("B", 3)),
                StreamEvent.CreateEnd(3, 1, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(4, 2, StructTuple.Create("B", 3))
            };
            var input = inputList.ToObservable().ToStreamable();
            var outputStream = input.Stitch();

            var compareTo = new[]
            {
                StreamEvent.CreateStart(1, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(2, StructTuple.Create("B", 3)),
                StreamEvent.CreateEnd(3, 1, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(4, 2, StructTuple.Create("B", 3)),
                END
            };
            var output = outputStream.ToStreamEventObservable(ReshapingPolicy.None).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(compareTo));
        }

        [TestMethod, TestCategory("Gated")]
        public void StitchTestSimpleSpliceCodegen()
        {
            // nothing interesting happens here
            var inputList = new[]
            {
                StreamEvent.CreateStart(1, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(2, 1, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(2, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(3, 2, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(3, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(4, 3, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(4, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(5, 4, StructTuple.Create("A", 3))
            };

            var compareTo = new[]
            {
                StreamEvent.CreateStart(1, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(5, 1, StructTuple.Create("A", 3)),
                END
            };
            var input = inputList.ToObservable().ToStreamable();
            var outputStream = input.Stitch();
            var output = outputStream.ToStreamEventObservable(ReshapingPolicy.None).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(compareTo));
        }

        [TestMethod, TestCategory("Gated")]
        public void StitchTestMultisetOneStartCodegen()
        {
            // Two different start times
            var inputList = new[]
            {
                StreamEvent.CreateStart(1, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(1, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(2, 1, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(2, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(3, 2, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(3, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(4, 3, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(4, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(5, 4, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(6, 1, StructTuple.Create("A", 3))
            };

            var compareTo = new[]
            {
                StreamEvent.CreateStart(1, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(1, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(5, 1, StructTuple.Create("A", 3)), // 5->4->3->2->1
                StreamEvent.CreateEnd(6, 1, StructTuple.Create("A", 3)), // 6->1
                END
            };

            var input = inputList.ToObservable().ToStreamable();
            var outputStream = input.Stitch();

            var output = outputStream.ToStreamEventObservable(ReshapingPolicy.None).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(compareTo));
        }

        // Is this even slower?
        [TestMethod, TestCategory("Gated")]
        public void StitchTestMultisetExtraSlowStartCodegen()
        {
            // Two different start times
            var inputList = new[]
            {
                StreamEvent.CreateStart(1, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(1, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(1, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(2, 1, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(2, 1, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(2, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(2, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(3, 2, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(3, 2, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(3, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(3, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(4, 3, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(4, 3, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(4, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(4, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(5, 4, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(5, 4, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(6, 1, StructTuple.Create("A", 3))
            };

            var compareTo = new[]
            {
                StreamEvent.CreateStart(1, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(1, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(1, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(5, 1, StructTuple.Create("A", 3)), // 5->4->3->2->1
                StreamEvent.CreateEnd(5, 1, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(6, 1, StructTuple.Create("A", 3)),
                END
            };

            var input = inputList.ToObservable().ToStreamable();
            var outputStream = input.Stitch();

            var output = outputStream.ToStreamEventObservable(ReshapingPolicy.None).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(compareTo));
        }

        [TestMethod, TestCategory("Gated")]
        public void StitchTestTimeInvertCodegen()
        {
            // This is the odd little case where we swapped "BEGIN" and "END" at both times 3 and 4
            // so that they arrive in the opposite order. This shouldn't trouble the system at all:
            // Everything that happens at Time 3 is identical. Right? Right!
            var inputList = new[]
            {
                StreamEvent.CreateStart(1, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(2, 1, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(2, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(3, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(3, 2, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(4, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(4, 3, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(5, 4, StructTuple.Create("A", 3))
            };

            var compareTo = new[]
            {
                StreamEvent.CreateStart(1, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(5, 1, StructTuple.Create("A", 3)),
                END
            };

            var input = inputList.ToObservable().ToStreamable();
            var outputStream = input.Stitch();

            var output = outputStream.ToStreamEventObservable(ReshapingPolicy.None).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(compareTo));
        }

        [TestMethod, TestCategory("Gated")]
        public void StitchTestDictionaryCodegen()
        {
            // We have done horrible thigns to the dictionary: all elements are
            // identical!
            var inputList = new[]
            {
                StreamEvent.CreateStart(1, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(2, 1, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(2, StructTuple.Create("B", 3)),
                StreamEvent.CreateEnd(3, 2, StructTuple.Create("B", 3)),
                StreamEvent.CreateStart(3, StructTuple.Create("B", 3)),
                StreamEvent.CreateEnd(4, 3, StructTuple.Create("A", 3)),
                StreamEvent.CreateStart(4, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(5, 4, StructTuple.Create("B", 3))
            };

            var compareTo = new[]
            {
                StreamEvent.CreateStart(1, StructTuple.Create("A", 3)),
                StreamEvent.CreateEnd(5, 1, StructTuple.Create("A", 3)),
                END
            };

            var input = inputList.ToObservable().ToStreamable()
                .SetProperty()
                .PayloadEqualityComparer(new EqualityComparerExpression<StructTuple<string, int>>(
                    (st1, st2) => (st1.Item1 == st2.Item1) ||
                    (st1.Item1 == "A" && st2.Item1 == "B") ||
                    (st1.Item1 == "B" && st2.Item1 == "A"),
                    str => 7));
            var outputStream = input.Stitch();

            var output = outputStream.ToStreamEventObservable(ReshapingPolicy.None).ToEnumerable().ToArray();

            Assert.IsTrue(output.SequenceEqual(compareTo));
        }

        private static IList<StreamEvent<T>> Execute<T>(
            IEnumerable<StreamEvent<T>> input,
            Func<IStreamable<Empty, T>, IStreamable<Empty, T>> selector)
            => selector(input
                .ToObservable()
                .ToStreamable())
            .ToStreamEventObservable()
            .ToList()
            .Wait();

        [TestMethod]
        public void Stitch_PointsCodegen()
        {
            StreamEvent<int>[] input = new[]
            {
                StreamEvent.CreatePoint(100, 1),
                StreamEvent.CreatePoint(102, 1),
            };

            IList<StreamEvent<int>> result = Execute(input, x => x.Stitch());

            StreamEvent<int>[] expected = new[]
            {
                StreamEvent.CreateStart(100, 1),
                StreamEvent.CreateEnd(101, 100, 1),
                StreamEvent.CreateStart(102, 1),
                StreamEvent.CreateEnd(103, 102, 1),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime),
            };

            Assert.IsTrue(expected.SequenceEqual(result));
        }

        [TestMethod]
        public void Stitch_EdgesCodegen()
        {
            StreamEvent<int>[] input = new[]
            {
                StreamEvent.CreateStart(104,      1),
                StreamEvent.CreateStart(105,      2),
                StreamEvent.CreateEnd(106, 104, 1),
                StreamEvent.CreateEnd(106, 105, 2),
            };

            IList<StreamEvent<int>> result = Execute(input, x => x.Stitch());

            StreamEvent<int>[] expected = new[]
            {
                StreamEvent.CreateStart(104,      1),
                StreamEvent.CreateStart(105,      2),
                StreamEvent.CreateEnd(106, 104, 1),
                StreamEvent.CreateEnd(106, 105, 2),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime),
            };

            Assert.IsTrue(expected.SequenceEqual(result));
        }
    }
}
