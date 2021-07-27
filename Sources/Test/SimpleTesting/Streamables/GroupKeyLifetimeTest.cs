using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Subjects;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class GroupKeyLifetimeTest : TestWithConfigSettingsAndMemoryLeakDetection
    {
        [TestMethod, TestCategory("Gated")]
        public void GroupKeyLifetimeAccessTest()
        {
            var inputEvents = new StreamEvent<int>[]
            {
                StreamEvent.CreateStart(100, 1),
                StreamEvent.CreateStart(200, 2),
                StreamEvent.CreateStart(300, 3),
                StreamEvent.CreateStart(400, 4),
                StreamEvent.CreatePunctuation<int>(400),

                StreamEvent.CreateStart(500, 5),
                StreamEvent.CreateStart(600, 6),
                StreamEvent.CreateStart(700, 7),
                StreamEvent.CreateStart(800, 8),
                StreamEvent.CreatePunctuation<int>(800),

                StreamEvent.CreateStart(900, 9),
                StreamEvent.CreateStart(1000, 10),
                StreamEvent.CreateStart(1100, 11),
                StreamEvent.CreateStart(1200, 12),
                StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime)
            };

            var keyFactory = new TestKeyFactory();

            var subject = new Subject<StreamEvent<int>>();
            var output = subject
                            .ToStreamable()
                            .HoppingWindowLifetime(300, 100)
                            .GroupApply(
                                v => keyFactory.Create(v),
                                w => w.Count(),
                                (key, count) => new { key.Key, count });

            var outputCounts = new List<ValueTuple<int, ulong>>();

            var subscription = output.ToStreamEventObservable().Subscribe(onNext: item =>
            {
                if (item.IsData)
                {
                    outputCounts.Add((item.Payload.Key.Key, item.Payload.count));
                }
            });

            foreach (var inputEvent in inputEvents.Take(5))
            {
                subject.OnNext(inputEvent);
            }

            var oldKeys = keyFactory.KeysCreated.ToArray();

            foreach (var inputEvent in inputEvents.Skip(5).Take(5))
            {
                subject.OnNext(inputEvent);
            }

            TestKeyFactory.MarkDirty(oldKeys);

            foreach (var inputEvent in inputEvents.Skip(10))
            {
                subject.OnNext(inputEvent);
            }

            // Asserts are done while accessing records in TestKey methods
            subject.OnCompleted();

            subject.Dispose();
            subscription.Dispose();
        }

        private class TestKey : IEquatable<TestKey>
        {
            public readonly int Key;
            public bool isDeleted;

            public TestKey(int key)
            {
                this.Key = key;
                this.isDeleted = false;
            }

            public bool Equals(TestKey other)
            {
                Assert.IsFalse(isDeleted);
                return (other != null && this.Key == other.Key);
            }

            public override bool Equals(object obj)
            {
                Assert.IsFalse(isDeleted);
                return Equals(obj as TestKey);
            }

            public override int GetHashCode()
            {
                Assert.IsFalse(isDeleted);
                return Key;
            }

            public override string ToString()
                => $"(Key: {Key}, isDeleted: {isDeleted})";
        }

        private class TestKeyFactory
        {
            public readonly List<TestKey> KeysCreated = new List<TestKey>();

            public static void MarkDirty(IEnumerable<TestKey> keys)
            {
                foreach (var key in keys)
                {
                    key.isDeleted = true;
                }
            }

            public TestKey Create(int value)
            {
                var keyValue = value % 2;
                var key = new TestKey(keyValue);
                KeysCreated.Add(key);
                return key;
            }
        }
    }
}
