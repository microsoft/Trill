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
    internal sealed class CompiledUngroupedAfaPipe<TPayload, TRegister, TAccumulator> : CompiledAfaPipeBase<Empty, TPayload, TRegister, TAccumulator>
    {
        [DataMember]
        private FastLinkedList<GroupedActiveState<Empty, TRegister>> activeStates;
        [DataMember]
        private byte seenEvent;
        [DataMember]
        private FastLinkedList<OutputEvent<Empty, TRegister>> tentativeOutput;
        [DataMember]
        private long lastSyncTime;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public CompiledUngroupedAfaPipe() { }

        public CompiledUngroupedAfaPipe(Streamable<Empty, TRegister> stream, IStreamObserver<Empty, TRegister> observer, object afa, long maxDuration)
            : base(stream, observer, afa, maxDuration)
        {
            this.activeStates = new FastLinkedList<GroupedActiveState<Empty, TRegister>>();

            if (!this.IsSyncTimeSimultaneityFree)
            {
                this.seenEvent = 0;
                this.tentativeOutput = new FastLinkedList<OutputEvent<Empty, TRegister>>();
                this.lastSyncTime = -1;
            }
        }

        public override int CurrentlyBufferedInputCount => this.activeStates.Count;

        public override unsafe void OnNext(StreamMessage<Empty, TPayload> batch)
        {
            var stack = new Stack<int>();
            var activeFindTraverser = new FastLinkedList<GroupedActiveState<Empty, TRegister>>.ListTraverser(this.activeStates);
            var tentativeFindTraverser = new FastLinkedList<OutputEvent<Empty, TRegister>>.ListTraverser(this.tentativeOutput);
            var tentativeOutputIndex = 0;

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
                                if (synctime > this.lastSyncTime) // move time forward
                                {
                                    this.seenEvent = 0;

                                    if (this.tentativeOutput.Count > 0)
                                    {
                                        tentativeOutputIndex = 0;

                                        while (this.tentativeOutput.Iterate(ref tentativeOutputIndex))
                                        {
                                            var elem = this.tentativeOutput.Values[tentativeOutputIndex];

                                            dest_vsync[this.iter] = this.lastSyncTime;
                                            dest_vother[this.iter] = elem.other;
                                            this.batch.payload.col[this.iter] = elem.payload;
                                            dest_hash[this.iter] = 0;
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

                                if (this.seenEvent > 0) // Incoming event is a simultaneous one
                                {
                                    if (this.seenEvent == 1) // Detecting first duplicate, need to adjust state
                                    {
                                        this.seenEvent = 2;

                                        // Delete tentative output for that key
                                        this.tentativeOutput.Clear();

                                        // Delete active states for that key
                                        this.activeStates.Clear();
                                    }

                                    // Dont process this event
                                    continue;
                                }
                                else
                                {
                                    this.seenEvent = 1;
                                }
                            }

                            /* (1) Process currently active states */

                            if (activeFindTraverser.Find())
                            {
                                int orig_index;

                                while (activeFindTraverser.Next(out int index))
                                {
                                    orig_index = index;

                                    var state = this.activeStates.Values[index];

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
                                                                int ind = this.tentativeOutput.Insert();
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
                                                            if (index == -1) index = this.activeStates.Insert();
                                                            this.activeStates.Values[index].key = srckey[i];
                                                            this.activeStates.Values[index].state = ns;
                                                            this.activeStates.Values[index].register = newReg;
                                                            this.activeStates.Values[index].PatternStartTimestamp = state.PatternStartTimestamp;

                                                            index = -1;

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
                                                }
                                            }
                                        }
                                    }
                                    if (index == orig_index) activeFindTraverser.Remove();
                                }
                            }

                            /* (2) Start new activations from the start state(s) */
                            if (!this.AllowOverlappingInstances && this.activeStates.Count > 0) continue;

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
                                                        int ind = this.tentativeOutput.Insert();
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
                                                    int index = this.activeStates.Insert();
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
                                        }
                                    }
                                }
                            }

                        }
                        else if (batch.vother.col[i] < 0)
                        {
                            long synctime = src_vsync[i];
                            if (!this.IsSyncTimeSimultaneityFree)
                            {
                                if (synctime > this.lastSyncTime) // move time forward
                                {
                                    this.seenEvent = 0;

                                    if (this.tentativeOutput.Count > 0)
                                    {
                                        tentativeOutputIndex = 0;

                                        while (this.tentativeOutput.Iterate(ref tentativeOutputIndex))
                                        {
                                            var elem = this.tentativeOutput.Values[tentativeOutputIndex];

                                            this.batch.vsync.col[this.iter] = this.lastSyncTime;
                                            this.batch.vother.col[this.iter] = elem.other;
                                            this.batch.payload.col[this.iter] = elem.payload;
                                            this.batch.hash.col[this.iter] = 0;
                                            this.iter++;

                                            if (this.iter == Config.DataBatchSize) FlushContents();
                                        }

                                        this.tentativeOutput.Clear(); // Clear the tentative output list
                                    }

                                    this.lastSyncTime = synctime;
                                }
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