// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting.CustomIngress
{
    [TestClass]
    public class CustomIngressTests
    {
        private class CustomIntIngressPipe : CustomIngressStreamable<int, int>.CustomIngressPipe
        {
            public CustomIntIngressPipe(
                CustomIngressStreamable<int, int> streamable,
                IStreamObserver<Empty, int> observer) : base(streamable, observer)
            {
            }

            public override void OnNext(int value) => AddInterval(startTime: value, endTime: value + 5, payload: value);

            public override void OnCompleted()
            {
                FlushContents();
                base.OnCompleted();
            }
        }

        private class PartitionedCustomIntIngressPipe : PartitionedCustomIngressStreamable<int, string, int>.CustomIngressPipe
        {
            public PartitionedCustomIntIngressPipe(
                PartitionedCustomIngressStreamable<int, string, int> streamable,
                IStreamObserver<PartitionKey<string>, int> observer) : base(streamable, observer)
            {
            }

            public override void OnNext(int value) => AddInterval(key: value.ToString(), startTime: value, endTime: value + 5, payload: value);

            public override void OnCompleted()
            {
                FlushContents();
                base.OnCompleted();
            }
        }

        [TestMethod]
        public void SimpleCustomIngress()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var ingress = input.ToObservable().ToStreamable<int, int>(
                pipeCreator: (streamable, observer) => new CustomIntIngressPipe(streamable, observer));
            var egressObservable = ingress.ToStreamEventObservable();

            var outevents = egressObservable.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => StreamEvent.CreateInterval(i, i + 5, i))));
        }


        [TestMethod]
        public void SimpleCustomIngressPartitioned()
        {
            var input = Enumerable.Range(1, 1000).ToList();

            var ingress = input.ToObservable().ToStreamable<int, string, int>(
                pipeCreator: (streamable, observer) => new PartitionedCustomIntIngressPipe(streamable, observer));
            var egressObservable = ingress.ToStreamEventObservable();

            var outevents = egressObservable.ToEnumerable().ToList();
            var output = outevents.Where(o => !o.IsPunctuation);
            var punctuations = outevents.Where(o => o.IsPunctuation && o.SyncTime != StreamEvent.InfinitySyncTime);
            Assert.IsTrue(output.SequenceEqual(input.Select(i => PartitionedStreamEvent.CreateInterval(i.ToString(), i, i + 5, i))));
        }
    }
}