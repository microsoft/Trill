// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class CompiledPartitionedAfaPipe_SingleEvent<TKey, TPayload, TRegister, TAccumulator, TPartitionKey> : CompiledAfaPipeBase<TKey, TPayload, TRegister, TAccumulator>
    {
        private readonly Func<TKey, TPartitionKey> getPartitionKey = GetPartitionExtractor<TPartitionKey, TKey>();

        [DataMember]
        private FastMap<GroupedActiveState<TKey, TRegister>> activeStates = new FastMap<GroupedActiveState<TKey, TRegister>>();
        [DataMember]
        private FastDictionary2<TKey, byte> seenEvent;

        // The follow three fields' keys need to be updated in lock step
        [DataMember]
        private FastDictionary2<TPartitionKey, FastMap<OutputEvent<TKey, TRegister>>> tentativeOutput;
        [DataMember]
        private FastDictionary2<TPartitionKey, long> lastSyncTime = new FastDictionary2<TPartitionKey, long>();
        [DataMember]
        private FastDictionary2<TPartitionKey, List<TKey>> seenPartitions = new FastDictionary2<TPartitionKey, List<TKey>>();

        [Obsolete("Used only by serialization. Do not call directly.")]
        public CompiledPartitionedAfaPipe_SingleEvent() { }

        public CompiledPartitionedAfaPipe_SingleEvent(Streamable<TKey, TRegister> stream, IStreamObserver<TKey, TRegister> observer, object afa, long maxDuration)
            : base(stream, observer, afa, maxDuration)
        {
            if (!this.IsSyncTimeSimultaneityFree)
            {
                var comparer = stream.Properties.KeyEqualityComparer;
                var equalsFunc = comparer.GetEqualsExpr().Compile();
                var getHashCodeFunc = comparer.GetGetHashCodeExpr().Compile();
                this.seenEvent = comparer.CreateFastDictionary2Generator<TKey, byte>(10, equalsFunc, getHashCodeFunc, stream.Properties.QueryContainer).Invoke();
                this.tentativeOutput = new FastDictionary2<TPartitionKey, FastMap<OutputEvent<TKey, TRegister>>>();
            }
        }

        public override int CurrentlyBufferedInputCount => this.activeStates.Count;

        public override unsafe void OnNext(StreamMessage<TKey, TPayload> batch)
        {
            var stack = new Stack<int>();

            var count = batch.Count;

            var dest_vsync = this.batch.vsync.col;
            var dest_vother = this.batch.vother.col;
            var destkey = this.batch.key.col;
            var dest_hash = this.batch.hash.col;

            var srckey = batch.key.col;

            var activeFindTraverser = new FastMap<GroupedActiveState<TKey, TRegister>>.FindTraverser(this.activeStates);

            fixed (long* src_bv = batch.bitvector.col, src_vsync = batch.vsync.col, src_vother = batch.vother.col)
            {
                fixed (int* src_hash = batch.hash.col)
                {
                    for (int i = 0; i < count; i++)
                    {
                        if ((src_bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                        {
                            var key = srckey[i];
                            var partitionKey = this.getPartitionKey(key);
                            int partitionIndex = EnsurePartition(partitionKey);
                            var tentativeVisibleTraverser = new FastMap<OutputEvent<TKey, TRegister>>.VisibleTraverser(this.tentativeOutput.entries[partitionIndex].value);
                            long synctime = src_vsync[i];

                            if (!this.IsSyncTimeSimultaneityFree)
                            {
                                int index;

                                if (synctime > this.lastSyncTime.entries[partitionIndex].value) // move time forward
                                {
                                    foreach (var mapIndex in this.seenPartitions.entries[partitionIndex].value) this.seenEvent.Remove(mapIndex);

                                    if (this.tentativeOutput.Count > 0)
                                    {
                                        tentativeVisibleTraverser.currIndex = 0;

                                        while (tentativeVisibleTraverser.Next(out index, out int hash))
                                        {
                                            var elem = this.tentativeOutput.entries[partitionIndex].value.Values[index];

                                            dest_vsync[this.iter] = this.lastSyncTime.entries[partitionIndex].value;
                                            dest_vother[this.iter] = elem.other;
                                            this.batch.payload.col[this.iter] = elem.payload;
                                            destkey[this.iter] = elem.key;
                                            dest_hash[this.iter] = hash;
                                            this.iter++;

                                            if (this.iter == Config.DataBatchSize)
                                            {
                                                FlushContents();
                                                dest_vsync = this.batch.vsync.col;
                                                dest_vother = this.batch.vother.col;
                                                destkey = this.batch.key.col;
                                                dest_hash = this.batch.hash.col;
                                            }
                                        }

                                        this.tentativeOutput.entries[partitionIndex].value.Clear(); // Clear the tentative output list
                                    }

                                    this.lastSyncTime.entries[partitionIndex].value = synctime;
                                }

                                if (this.seenEvent.Lookup(srckey[i], out index)) // Incoming event is a simultaneous one
                                {
                                    if (this.seenEvent.entries[index].value == 1) // Detecting first duplicate, need to adjust state
                                    {
                                        this.seenEvent.entries[index].value = 2;

                                        // Delete tentative output for that key
                                        var tentativeFindTraverser = new FastMap<OutputEvent<TKey, TRegister>>.FindTraverser(this.tentativeOutput.entries[partitionIndex].value);
                                        if (tentativeFindTraverser.Find(src_hash[i]))
                                        {
                                            while (tentativeFindTraverser.Next(out index))
                                            {
                                                if (this.keyEqualityComparer(this.tentativeOutput.entries[partitionIndex].value.Values[index].key, srckey[i]))
                                                {
                                                    tentativeFindTraverser.Remove();
                                                }
                                            }
                                        }

                                        // Delete active states for that key
                                        if (activeFindTraverser.Find(src_hash[i]))
                                        {
                                            while (activeFindTraverser.Next(out index))
                                            {
                                                if (this.keyEqualityComparer(this.activeStates.Values[index].key, srckey[i]))
                                                {
                                                    activeFindTraverser.Remove();
                                                }
                                            }
                                        }
                                    }

                                    // Dont process this event
                                    continue;
                                }
                                else
                                    this.seenEvent.Insert(srckey[i], 1);
                            }

                            /* (1) Process currently active states */
                            bool ended = true;
                            if (activeFindTraverser.Find(src_hash[i]))
                            {
                                int orig_index;

                                while (activeFindTraverser.Next(out int index))
                                {
                                    orig_index = index;

                                    var state = this.activeStates.Values[index];
                                    if (!this.keyEqualityComparer(state.key, srckey[i])) continue;

                                    if (state.PatternStartTimestamp + this.MaxDuration > synctime)
                                    {
                                        var currentStateMap = this.singleEventStateMap[state.state];
                                        if (currentStateMap != null)
                                        {
                                            var m = currentStateMap.Length;
                                            for (int cnt = 0; cnt < m; cnt++)
                                            {
                                                var arcinfo = currentStateMap[cnt];

                                                if (arcinfo.Fence(synctime, batch[i], state.register))
                                                {
                                                    var newReg = arcinfo.Transfer == null
                                                        ? state.register
                                                        : arcinfo.Transfer(synctime, batch[i], state.register);
                                                    int ns = arcinfo.toState;
                                                    while (true)
                                                    {
                                                        if (this.isFinal[ns])
                                                        {
                                                            var otherTime = Math.Min(state.PatternStartTimestamp + this.MaxDuration, StreamEvent.InfinitySyncTime);

                                                            if (!this.IsSyncTimeSimultaneityFree)
                                                            {
                                                                var tentativeOutputEntry = this.tentativeOutput.entries[partitionIndex].value;

                                                                int ind = tentativeOutputEntry.Insert(src_hash[i]);
                                                                tentativeOutputEntry.Values[ind].other = otherTime;
                                                                tentativeOutputEntry.Values[ind].key = srckey[i];
                                                                tentativeOutputEntry.Values[ind].payload = newReg;
                                                            }
                                                            else
                                                            {
                                                                dest_vsync[this.iter] = synctime;
                                                                dest_vother[this.iter] = otherTime;
                                                                this.batch[this.iter] = newReg;
                                                                destkey[this.iter] = srckey[i];
                                                                dest_hash[this.iter] = src_hash[i];
                                                                this.iter++;

                                                                if (this.iter == Config.DataBatchSize)
                                                                {
                                                                    FlushContents();
                                                                    dest_vsync = this.batch.vsync.col;
                                                                    dest_vother = this.batch.vother.col;
                                                                    destkey = this.batch.key.col;
                                                                    dest_hash = this.batch.hash.col;
                                                                }
                                                            }
                                                        }

                                                        if (this.hasOutgoingArcs[ns])
                                                        {
                                                            if (index == -1) index = this.activeStates.Insert(src_hash[i]);
                                                            this.activeStates.Values[index].key = srckey[i];
                                                            this.activeStates.Values[index].state = ns;
                                                            this.activeStates.Values[index].register = newReg;
                                                            this.activeStates.Values[index].PatternStartTimestamp = state.PatternStartTimestamp;

                                                            index = -1;

                                                            ended = false;

                                                            // Add epsilon arc destinations to stack
                                                            if (this.epsilonStateMap == null) break;
                                                            if (this.epsilonStateMap[ns] != null)
                                                            {
                                                                for (int cnt2 = 0; cnt2 < this.epsilonStateMap[ns].Length; cnt2++)
                                                                    stack.Push(this.epsilonStateMap[ns][cnt2]);
                                                            }
                                                        }
                                                        if (stack.Count == 0) break;
                                                        ns = stack.Pop();
                                                    }
                                                    if (this.IsDeterministic) break; // We are guaranteed to have only one successful transition
                                                }
                                            }
                                        }
                                    }

                                    if (index == orig_index) activeFindTraverser.Remove();
                                    if (this.IsDeterministic) break; // We are guaranteed to have only one active state
                                }
                            }

                            /* (2) Start new activations from the start state(s) */
                            if (!this.AllowOverlappingInstances && !ended) continue;

                            for (int counter = 0; counter < this.numStartStates;  counter++)
                            {
                                int startState = this.startStates[counter];
                                var startStateMap = this.singleEventStateMap[startState];
                                if (startStateMap != null)
                                {
                                    var m = startStateMap.Length;
                                    for (int cnt = 0; cnt < m; cnt++)
                                    {
                                        var arcinfo = startStateMap[cnt];
                                        if (arcinfo.Fence(synctime, batch[i], this.defaultRegister))
                                        {
                                            var newReg = arcinfo.Transfer == null
                                                ? this.defaultRegister
                                                : arcinfo.Transfer(synctime, batch[i], this.defaultRegister);
                                            int ns = arcinfo.toState;
                                            while (true)
                                            {
                                                if (this.isFinal[ns])
                                                {
                                                    var otherTime = Math.Min(synctime + this.MaxDuration, StreamEvent.InfinitySyncTime);

                                                    if (!this.IsSyncTimeSimultaneityFree)
                                                    {
                                                        var tentativeOutputEntry = this.tentativeOutput.entries[partitionIndex].value;

                                                        int ind = tentativeOutputEntry.Insert(src_hash[i]);
                                                        tentativeOutputEntry.Values[ind].other = otherTime;
                                                        tentativeOutputEntry.Values[ind].key = srckey[i];
                                                        tentativeOutputEntry.Values[ind].payload = newReg;
                                                    }
                                                    else
                                                    {
                                                        dest_vsync[this.iter] = synctime;
                                                        dest_vother[this.iter] = otherTime;
                                                        this.batch[this.iter] = newReg;
                                                        destkey[this.iter] = srckey[i];
                                                        dest_hash[this.iter] = src_hash[i];
                                                        this.iter++;

                                                        if (this.iter == Config.DataBatchSize)
                                                        {
                                                            FlushContents();
                                                            dest_vsync = this.batch.vsync.col;
                                                            dest_vother = this.batch.vother.col;
                                                            destkey = this.batch.key.col;
                                                            dest_hash = this.batch.hash.col;

                                                        }
                                                    }
                                                }
                                                if (this.hasOutgoingArcs[ns])
                                                {
                                                    int index = this.activeStates.Insert(src_hash[i]);
                                                    this.activeStates.Values[index].key = srckey[i];
                                                    this.activeStates.Values[index].state = ns;
                                                    this.activeStates.Values[index].register = newReg;
                                                    this.activeStates.Values[index].PatternStartTimestamp = synctime;

                                                    // Add epsilon arc destinations to stack
                                                    if (this.epsilonStateMap == null) break;
                                                    if (this.epsilonStateMap[ns] != null)
                                                    {
                                                        for (int cnt2 = 0; cnt2 < this.epsilonStateMap[ns].Length; cnt2++)
                                                            stack.Push(this.epsilonStateMap[ns][cnt2]);
                                                    }
                                                }

                                                if (stack.Count == 0) break;
                                                ns = stack.Pop();
                                            }

                                            if (this.IsDeterministic) break; // We are guaranteed to have only one successful transition
                                        }
                                    }
                                }

                                if (this.IsDeterministic) break; // We are guaranteed to have only one start state
                            }
                        }
                        else if (src_vother[i] == PartitionedStreamEvent.LowWatermarkOtherTime)
                        {
                            long synctime = src_vsync[i];
                            if (!this.IsSyncTimeSimultaneityFree)
                            {
                                var partitionIndex = FastDictionary2<TPartitionKey, long>.IteratorStart;
                                while (this.tentativeOutput.Iterate(ref partitionIndex))
                                {
                                    if (synctime > this.lastSyncTime.entries[partitionIndex].value) // move time forward
                                    {
                                        var tentativeVisibleTraverser = new FastMap<OutputEvent<TKey, TRegister>>.VisibleTraverser(this.tentativeOutput.entries[partitionIndex].value);

                                        foreach (var mapIndex in this.seenPartitions.entries[partitionIndex].value) this.seenEvent.Remove(mapIndex);

                                        if (this.tentativeOutput.Count > 0)
                                        {
                                            tentativeVisibleTraverser.currIndex = 0;

                                            while (tentativeVisibleTraverser.Next(out int index, out int hash))
                                            {
                                                var elem = this.tentativeOutput.entries[partitionIndex].value.Values[index];

                                                this.batch.vsync.col[this.iter] = this.lastSyncTime.entries[partitionIndex].value;
                                                this.batch.vother.col[this.iter] = elem.other;
                                                this.batch.payload.col[this.iter] = elem.payload;
                                                this.batch.key.col[this.iter] = elem.key;
                                                this.batch.hash.col[this.iter] = hash;
                                                this.iter++;

                                                if (this.iter == Config.DataBatchSize) FlushContents();
                                            }

                                            this.tentativeOutput.entries[partitionIndex].value.Clear(); // Clear the tentative output list
                                        }

                                        this.lastSyncTime.entries[partitionIndex].value = synctime;
                                    }
                                }
                            }

                            OnLowWatermark(synctime);
                        }
                        else if (src_vother[i] == PartitionedStreamEvent.PunctuationOtherTime)
                        {
                            var key = srckey[i];
                            long synctime = src_vsync[i];

                            if (!this.IsSyncTimeSimultaneityFree)
                            {
                                var partitionKey = this.getPartitionKey(key);
                                int partitionIndex = EnsurePartition(partitionKey);

                                if (synctime > this.lastSyncTime.entries[partitionIndex].value) // move time forward
                                {
                                    var tentativeVisibleTraverser = new FastMap<OutputEvent<TKey, TRegister>>.VisibleTraverser(this.tentativeOutput.entries[partitionIndex].value);

                                    foreach (var mapIndex in this.seenPartitions.entries[partitionIndex].value) this.seenEvent.Remove(mapIndex);

                                    if (this.tentativeOutput.Count > 0)
                                    {
                                        tentativeVisibleTraverser.currIndex = 0;

                                        while (tentativeVisibleTraverser.Next(out int index, out int hash))
                                        {
                                            var elem = this.tentativeOutput.entries[partitionIndex].value.Values[index];

                                            this.batch.vsync.col[this.iter] = this.lastSyncTime.entries[partitionIndex].value;
                                            this.batch.vother.col[this.iter] = elem.other;
                                            this.batch.payload.col[this.iter] = elem.payload;
                                            this.batch.key.col[this.iter] = elem.key;
                                            this.batch.hash.col[this.iter] = hash;
                                            this.iter++;

                                            if (this.iter == Config.DataBatchSize) FlushContents();
                                        }

                                        this.tentativeOutput.entries[partitionIndex].value.Clear(); // Clear the tentative output list
                                    }

                                    this.lastSyncTime.entries[partitionIndex].value = synctime;
                                }
                            }

                            this.batch.vsync.col[this.iter] = synctime;
                            this.batch.vother.col[this.iter] = long.MinValue;
                            this.batch.payload.col[this.iter] = default;
                            this.batch.key.col[this.iter] = key;
                            this.batch.hash.col[this.iter] = src_hash[i];
                            this.batch.bitvector.col[this.iter >> 6] |= (1L << (this.iter & 0x3f));
                            this.iter++;

                            if (this.iter == Config.DataBatchSize) FlushContents();
                        }
                    }
                }
            }
            batch.Free();
        }

        private int EnsurePartition(TPartitionKey partitionKey)
        {
            if (!this.lastSyncTime.Lookup(partitionKey, out int index))
            {
                index = this.lastSyncTime.Insert(partitionKey, long.MinValue);
                this.tentativeOutput.Insert(partitionKey, new FastMap<OutputEvent<TKey, TRegister>>());
                this.seenPartitions.Insert(partitionKey, new List<TKey>());
            }
            return index;
        }
    }
}