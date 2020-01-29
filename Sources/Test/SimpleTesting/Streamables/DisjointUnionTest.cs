using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class DisjointUnionTest : TestWithConfigSettingsAndMemoryLeakDetection
    {
        [TestMethod, TestCategory("Gated")]
        public void DisjointUnionPunctuations()
        {
            var left = new Subject<StreamEvent<int>>();
            var right = new Subject<StreamEvent<int>>();

            var qc = new QueryContainer();
            var leftInput = qc.RegisterInput(left);
            var rightInput = qc.RegisterInput(right);

            var actualOutput = new List<StreamEvent<int>>();
            var union = new MultiUnionStreamable<Empty, int>(new IStreamable<Empty, int>[] { leftInput, rightInput }, guaranteedDisjoint: true);
            var egress = qc.RegisterOutput(union).ForEachAsync(o => actualOutput.Add(o));
            var process = qc.Restore();

            left.OnNext(StreamEvent.CreatePoint(100, 1));
            left.OnNext(StreamEvent.CreatePunctuation<int>(101));

            right.OnNext(StreamEvent.CreatePoint(100, 1));
            right.OnNext(StreamEvent.CreatePunctuation<int>(110));

            process.Flush();

            left.OnNext(StreamEvent.CreatePoint(101, 1));
            right.OnNext(StreamEvent.CreatePoint(110, 1));

            process.Flush();

            left.OnCompleted();
            right.OnCompleted();

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreatePoint(100, 1),
                StreamEvent.CreatePoint(100, 1),
                StreamEvent.CreatePunctuation<int>(101),
                StreamEvent.CreatePoint(101, 1),
                StreamEvent.CreatePoint(110, 1),
                StreamEvent.CreatePunctuation<int>(110),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime),
            };

            Assert.IsTrue(expected.SequenceEqual(actualOutput));
        }

        [TestMethod, TestCategory("Gated")]
        public void DisjointUnionLowWatermarks()
        {
            const int leftKey = 1;
            const int rightKey = 2;
            var left = new Subject<PartitionedStreamEvent<int, int>>();
            var right = new Subject<PartitionedStreamEvent<int, int>>();

            var qc = new QueryContainer();
            var leftInput = qc.RegisterInput(left);
            var rightInput = qc.RegisterInput(right);

            var actualOutput = new List<PartitionedStreamEvent<int, int>>();
            var inputs = new IStreamable<PartitionKey<int>, int>[] { leftInput, rightInput };
            var union = new MultiUnionStreamable<PartitionKey<int>, int>(inputs, guaranteedDisjoint: true);
            var egress = qc.RegisterOutput(union).ForEachAsync(o => actualOutput.Add(o));
            var process = qc.Restore();

            left.OnNext(PartitionedStreamEvent.CreatePoint(leftKey, 100, 1));
            left.OnNext(PartitionedStreamEvent.CreateLowWatermark<int, int>(101));

            right.OnNext(PartitionedStreamEvent.CreatePoint(rightKey, 100, 1));
            right.OnNext(PartitionedStreamEvent.CreateLowWatermark<int, int>(110));

            process.Flush();

            left.OnNext(PartitionedStreamEvent.CreatePoint(leftKey, 101, 1));
            right.OnNext(PartitionedStreamEvent.CreatePoint(rightKey, 110, 1));

            process.Flush();

            left.OnCompleted();
            right.OnCompleted();

            var expected = new PartitionedStreamEvent<int, int>[]
            {
                PartitionedStreamEvent.CreatePoint(leftKey, 100, 1),
                PartitionedStreamEvent.CreatePoint(rightKey, 100, 1),
                PartitionedStreamEvent.CreateLowWatermark<int, int>(101),
                PartitionedStreamEvent.CreatePoint(leftKey, 101, 1),
                PartitionedStreamEvent.CreatePoint(rightKey, 110, 1),
                PartitionedStreamEvent.CreateLowWatermark<int, int>(110),
                PartitionedStreamEvent.CreateLowWatermark<int, int>(StreamEvent.InfinitySyncTime),
            };

            Assert.IsTrue(expected.SequenceEqual(actualOutput));
        }

        [TestMethod, TestCategory("Gated")]
        public void DisjointUnionNegativeWatermarkRepro()
        {
            const int leftKey = 1;
            const int rightKey = 2;
            var left = new Subject<PartitionedStreamEvent<int, int>>();
            var right = new Subject<PartitionedStreamEvent<int, int>>();

            var qc = new QueryContainer();
            var leftInput = qc.RegisterInput(left);
            var rightInput = qc.RegisterInput(right);

            var actualOutput = new List<PartitionedStreamEvent<int, int>>();
            var inputs = new IStreamable<PartitionKey<int>, int>[] { leftInput, rightInput };
            var union = new MultiUnionStreamable<PartitionKey<int>, int>(inputs, guaranteedDisjoint: true);
            var egress = qc.RegisterOutput(union).ForEachAsync(o => actualOutput.Add(o));
            var process = qc.Restore();

            left.OnNext(PartitionedStreamEvent.CreatePoint(leftKey, 100, 1));
            right.OnNext(PartitionedStreamEvent.CreatePoint(rightKey, 100, 1));

            process.Flush();

            left.OnCompleted();
            right.OnCompleted();

            var expected = new PartitionedStreamEvent<int, int>[]
            {
                PartitionedStreamEvent.CreatePoint(leftKey, 100, 1),
                PartitionedStreamEvent.CreatePoint(rightKey, 100, 1),
                PartitionedStreamEvent.CreateLowWatermark<int, int>(StreamEvent.InfinitySyncTime),
            };

            Assert.IsTrue(expected.SequenceEqual(actualOutput));
        }
    }
}
