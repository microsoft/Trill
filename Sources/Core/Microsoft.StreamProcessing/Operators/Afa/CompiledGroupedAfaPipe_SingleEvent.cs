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
    internal sealed class CompiledGroupedAfaPipe_SingleEvent<TKey, TPayload, TRegister, TAccumulator> : CompiledAfaPipeBase<TKey, TPayload, TRegister, TAccumulator>
    {
        [DataMember]
        private FastMap<GroupedActiveState<TKey, TRegister>> activeStates;
        [DataMember]
        private FastDictionary<TKey, byte> seenEvent;
        [DataMember]
        private FastMap<OutputEvent<TKey, TRegister>> tentativeOutput;
        [DataMember]
        private long lastSyncTime;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public CompiledGroupedAfaPipe_SingleEvent() { }

        public CompiledGroupedAfaPipe_SingleEvent(Streamable<TKey, TRegister> stream, IStreamObserver<TKey, TRegister> observer, object afa, long maxDuration)
            : base(stream, observer, afa, maxDuration)
        {
            this.activeStates = new FastMap<GroupedActiveState<TKey, TRegister>>();

            if (!this.IsSyncTimeSimultaneityFree)
            {
                var comparer = stream.Properties.KeyEqualityComparer;
                var equalsComp = comparer.GetEqualsExpr().Compile();
                var getHashCode = comparer.GetGetHashCodeExpr().Compile();
                this.seenEvent = comparer.CreateFastDictionaryGenerator<TKey, byte>(10, equalsComp, getHashCode, stream.Properties.QueryContainer).Invoke();
                this.tentativeOutput = new FastMap<OutputEvent<TKey, TRegister>>();
                this.lastSyncTime = -1;
            }
        }

        public override int CurrentlyBufferedInputCount => this.activeStates.Count;

        public override unsafe void OnNext(StreamMessage<TKey, TPayload> batch)
        {
            var stack = new Stack<int>();
            var activeFindTraverser = new FastMap<GroupedActiveState<TKey, TRegister>>.FindTraverser(this.activeStates);
            var tentativeFindTraverser = new FastMap<OutputEvent<TKey, TRegister>>.FindTraverser(this.tentativeOutput);
            var tentativeVisibleTraverser = new FastMap<OutputEvent<TKey, TRegister>>.VisibleTraverser(this.tentativeOutput);

            var count = batch.Count;

            var dest_vsync = this.batch.vsync.col;
            var dest_vother = this.batch.vother.col;
            var destkey = this.batch.key.col;
            var dest_hash = this.batch.hash.col;

            var srckey = batch.key.col;

            fixed (long* src_bv = batch.bitvector.col, src_vsync = batch.vsync.col)
            {
                fixed (int* src_hash = batch.hash.col)
                {
                    for (int i = 0; i < count; i++)
                    {
                        if ((src_bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                        {
                            long synctime = src_vsync[i];

                            if (!this.IsSyncTimeSimultaneityFree)
                            {
                                int index;

                                if (synctime > this.lastSyncTime) // move time forward
                                {
                                    this.seenEvent.Clear();
                                    if (this.tentativeOutput.Count > 0)
                                    {
                                        tentativeVisibleTraverser.currIndex = 0;

                                        while (tentativeVisibleTraverser.Next(out index, out int hash))
                                        {
                                            var elem = this.tentativeOutput.Values[index];

                                            dest_vsync[this.iter] = this.lastSyncTime;
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

                                        this.tentativeOutput.Clear(); // Clear the tentative output list
                                    }

                                    this.lastSyncTime = synctime;
                                }

                                if (this.seenEvent.Lookup(srckey[i], out index)) // Incoming event is a simultaneous one
                                {
                                    if (this.seenEvent.entries[index].value == 1) // Detecting first duplicate, need to adjust state
                                    {
                                        this.seenEvent.entries[index].value = 2;

                                        // Delete tentative output for that key
                                        if (tentativeFindTraverser.Find(src_hash[i]))
                                        {
                                            while (tentativeFindTraverser.Next(out index))
                                            {
                                                if (this.keyEqualityComparer(this.tentativeOutput.Values[index].key, srckey[i]))
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
                                {
                                    this.seenEvent.Insert(ref index, srckey[i], 1);
                                }
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
                                                                int ind = this.tentativeOutput.Insert(src_hash[i]);
                                                                this.tentativeOutput.Values[ind].other = otherTime;
                                                                this.tentativeOutput.Values[ind].key = srckey[i];
                                                                this.tentativeOutput.Values[ind].payload = newReg;
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
                                                        int ind = this.tentativeOutput.Insert(src_hash[i]);
                                                        this.tentativeOutput.Values[ind].other = otherTime;
                                                        this.tentativeOutput.Values[ind].key = srckey[i];
                                                        this.tentativeOutput.Values[ind].payload = newReg;
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
                        else if (batch.vother.col[i] < 0 && !this.IsSyncTimeSimultaneityFree)
                        {
                            long synctime = src_vsync[i];
                            if (synctime > this.lastSyncTime) // move time forward
                            {
                                this.seenEvent.Clear();

                                if (this.tentativeOutput.Count > 0)
                                {
                                    tentativeVisibleTraverser.currIndex = 0;

                                    while (tentativeVisibleTraverser.Next(out int index, out int hash))
                                    {
                                        var elem = this.tentativeOutput.Values[index];

                                        this.batch.vsync.col[this.iter] = this.lastSyncTime;
                                        this.batch.vother.col[this.iter] = elem.other;
                                        this.batch.payload.col[this.iter] = elem.payload;
                                        this.batch.key.col[this.iter] = elem.key;
                                        this.batch.hash.col[this.iter] = hash;
                                        this.iter++;

                                        if (this.iter == Config.DataBatchSize) FlushContents();
                                    }

                                    this.tentativeOutput.Clear(); // Clear the tentative output list
                                }

                                this.lastSyncTime = synctime;
                            }
                            OnPunctuation(synctime);
                        }
                    }
                }
            }
            batch.Free();
        }
    }
}