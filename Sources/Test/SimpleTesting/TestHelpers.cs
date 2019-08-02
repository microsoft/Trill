// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public static class StreamableInternal
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

    public abstract class TestWithConfigSettingsWithoutMemoryLeakDetection
    {
        private IDisposable confmod = null;
        private readonly ConfigModifier modifier;

        protected TestWithConfigSettingsWithoutMemoryLeakDetection() : this(new ConfigModifier()) { }

        internal TestWithConfigSettingsWithoutMemoryLeakDetection(ConfigModifier modifier) => this.modifier = modifier;

        [TestInitialize]
        public virtual void Setup() => this.confmod = this.modifier.Modify();

        [TestCleanup]
        public virtual void TearDown()
        {
            var cm = System.Threading.Interlocked.Exchange(ref this.confmod, null);
            if (cm != null) { cm.Dispose(); }
        }
    }
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

    public class SERListEqualityComparer : IEqualityComparer<StreamEvent<List<RankedEvent<char>>>>
    {
        public bool Equals(StreamEvent<List<RankedEvent<char>>> a, StreamEvent<List<RankedEvent<char>>> b)
        {
            if (a.SyncTime != b.SyncTime) return false;
            if (a.OtherTime != b.OtherTime) return false;

            if (a.Kind != b.Kind) return false;

            if (a.Kind != StreamEventKind.Punctuation)
            {
                if (a.Payload.Count != b.Payload.Count) return false;

                for (int j = 0; j < a.Payload.Count; j++)
                {
                    if (!a.Payload[j].Equals(b.Payload[j])) return false;
                }
            }

            return true;
        }

        public int GetHashCode(StreamEvent<List<RankedEvent<char>>> obj)
        {
            int ret = (int)(obj.SyncTime ^ obj.OtherTime);
            foreach (var l in obj.Payload)
            {
                ret ^= l.GetHashCode();
            }
            return ret;
        }
    }
}