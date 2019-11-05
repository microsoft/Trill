// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class CompiledUngroupedDAfaPipe<TPayload, TRegister, TAccumulator> : CompiledAfaPipeBase<Empty, TPayload, TRegister, TAccumulator>
    {
        [DataMember]
        private int activeState_state;
        [DataMember]
        private TRegister activeState_register;
        [DataMember]
        private long activeState_PatternStartTimestamp;

        [DataMember]
        private byte seenEvent;
        [DataMember]
        private FastLinkedList<OutputEvent<Empty, TRegister>> tentativeOutput;
        [DataMember]
        private long lastSyncTime;

        private int startState;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public CompiledUngroupedDAfaPipe() { }

        public CompiledUngroupedDAfaPipe(Streamable<Empty, TRegister> stream, IStreamObserver<Empty, TRegister> observer, object afa, long maxDuration)
            : base(stream, observer, afa, maxDuration)
        {
            this.activeState_state = -1;
            this.startState = this.startStates[0];

            if (!this.IsSyncTimeSimultaneityFree)
            {
                this.seenEvent = 0;
                this.tentativeOutput = new FastLinkedList<OutputEvent<Empty, TRegister>>();
                this.lastSyncTime = -1;
            }
        }

        public override int CurrentlyBufferedInputCount => 0;

        public override unsafe void OnNext(StreamMessage<Empty, TPayload> batch)
        {
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
                                            this.batch[this.iter] = elem.payload;
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
                                        this.activeState_state = -1;
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

                            if (this.activeState_state >= 0)
                            {
                                if (this.activeState_PatternStartTimestamp + this.MaxDuration > synctime)
                                {
                                    var currentStateMap = this.singleEventStateMap[this.activeState_state];
                                    this.activeState_state = -1; // assume the arc does not fire
                                    if (currentStateMap != null)
                                    {
                                        var m = currentStateMap.Length;
                                        for (int cnt = 0; cnt < m; cnt++)
                                        {
                                            var arcinfo = currentStateMap[cnt];

                                            if (arcinfo.Fence(synctime, batch[i], this.activeState_register))
                                            {
                                                if (arcinfo.Transfer != null) this.activeState_register = arcinfo.Transfer(synctime, batch[i], this.activeState_register);

                                                this.activeState_state = arcinfo.toState;
                                                if (this.isFinal[this.activeState_state])
                                                {
                                                    var otherTime = Math.Min(this.activeState_PatternStartTimestamp + this.MaxDuration, StreamEvent.InfinitySyncTime);

                                                    if (!this.IsSyncTimeSimultaneityFree)
                                                    {
                                                        int ind = this.tentativeOutput.Insert();
                                                        this.tentativeOutput.Values[ind].other = otherTime;
                                                        this.tentativeOutput.Values[ind].key = srckey[i];
                                                        this.tentativeOutput.Values[ind].payload = this.activeState_register;
                                                    }
                                                    else
                                                    {
                                                        dest_vsync[this.iter] = synctime;
                                                        dest_vother[this.iter] = otherTime;
                                                        this.batch[this.iter] = this.activeState_register;
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

                                                if (this.hasOutgoingArcs[this.activeState_state])
                                                {
                                                    this.activeState_PatternStartTimestamp = synctime;
                                                }
                                                else
                                                {
                                                    this.activeState_state = -1;
                                                }
                                                break; // DFA, so only one arc fires
                                            }
                                        }
                                    }
                                }
                            }

                            /* (2) Start new activations from the start state(s) */
                            if (this.activeState_state >= 0) continue;

                            var startStateMap = this.singleEventStateMap[this.startState];
                            if (startStateMap != null)
                            {
                                var m = startStateMap.Length;
                                for (int cnt = 0; cnt < m; cnt++)
                                {
                                    var arcinfo = startStateMap[cnt];
                                    if (arcinfo.Fence(synctime, batch[i], this.defaultRegister))
                                    {
                                        this.activeState_register = arcinfo.Transfer != null
                                            ? arcinfo.Transfer(synctime, batch[i], this.defaultRegister)
                                            : this.defaultRegister;

                                        this.activeState_state = arcinfo.toState;
                                        if (this.isFinal[this.activeState_state])
                                        {
                                            var otherTime = Math.Min(synctime + this.MaxDuration, StreamEvent.InfinitySyncTime);

                                            if (!this.IsSyncTimeSimultaneityFree)
                                            {
                                                int ind = this.tentativeOutput.Insert();
                                                this.tentativeOutput.Values[ind].other = otherTime;
                                                this.tentativeOutput.Values[ind].key = srckey[i];
                                                this.tentativeOutput.Values[ind].payload = this.activeState_register;
                                            }
                                            else
                                            {
                                                dest_vsync[this.iter] = synctime;
                                                dest_vother[this.iter] = otherTime;
                                                this.batch[this.iter] = this.activeState_register;
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
                                        if (this.hasOutgoingArcs[this.activeState_state])
                                        {
                                            this.activeState_PatternStartTimestamp = synctime;
                                        }
                                        else
                                        {
                                            this.activeState_state = -1;
                                        }
                                        break;
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

        protected override void UpdatePointers() => this.startState = this.startStates[0];
    }
}