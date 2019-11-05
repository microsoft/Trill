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
    internal sealed class CompiledUngroupedAfaPipe_EventList<TPayload, TRegister, TAccumulator> : CompiledAfaPipeBase<Empty, TPayload, TRegister, TAccumulator>
    {
        [DataMember]
        private FastLinkedList<GroupedActiveState<Empty, TRegister>> activeStates;

        private FastLinkedList<GroupedActiveState<Empty, TRegister>>.ListTraverser activeStatesTraverser;

        [DataMember]
        private List<TPayload> currentList;

        [DataMember]
        private long lastSyncTime;

        // Field instead of local variable to avoid re-initializing it
        private readonly Stack<int> stack = new Stack<int>();

        [Obsolete("Used only by serialization. Do not call directly.")]
        public CompiledUngroupedAfaPipe_EventList() { }

        public CompiledUngroupedAfaPipe_EventList(Streamable<Empty, TRegister> stream, IStreamObserver<Empty, TRegister> observer, object afa, long maxDuration)
            : base(stream, observer, afa, maxDuration)
        {
            this.activeStates = new FastLinkedList<GroupedActiveState<Empty, TRegister>>();
            this.activeStatesTraverser = new FastLinkedList<GroupedActiveState<Empty, TRegister>>.ListTraverser(this.activeStates);

            this.currentList = new List<TPayload>();
            this.lastSyncTime = -1;
        }

        public override int CurrentlyBufferedInputCount => this.activeStates.Count;

        private void ProcessCurrentTimestamp()
        {
            if (this.currentList.Count == 0) return;

            long synctime = this.lastSyncTime;

            /* (1) Process currently active states */
            bool ended = true;
            if (this.activeStatesTraverser.Find())
            {
                int orig_index;

                while (this.activeStatesTraverser.Next(out int index))
                {
                    orig_index = index;

                    var state = this.activeStates.Values[index];

                    if (state.PatternStartTimestamp + this.MaxDuration > synctime)
                    {
                        #region eventListStateMap
                        if (this.eventListStateMap != null)
                        {
                            var currentStateMap = this.eventListStateMap[state.state];
                            if (currentStateMap != null)
                            {
                                var m = currentStateMap.Length;
                                for (int cnt = 0; cnt < m; cnt++)
                                {
                                    var arcinfo = currentStateMap[cnt];

                                    if (arcinfo.Fence(synctime, this.currentList, state.register))
                                    {
                                        var newReg = arcinfo.Transfer == null
                                            ? state.register
                                            : arcinfo.Transfer(synctime, this.currentList, state.register);
                                        int ns = arcinfo.toState;
                                        while (true)
                                        {
                                            if (this.isFinal[ns])
                                            {
                                                this.batch.vsync.col[this.iter] = synctime;
                                                this.batch.vother.col[this.iter] = Math.Min(state.PatternStartTimestamp + this.MaxDuration, StreamEvent.InfinitySyncTime);
                                                this.batch[this.iter] = newReg;
                                                this.batch.hash.col[this.iter] = 0;
                                                this.iter++;

                                                if (this.iter == Config.DataBatchSize) FlushContents();
                                            }

                                            if (this.hasOutgoingArcs[ns])
                                            {
                                                if (index == -1) index = this.activeStates.Insert();
                                                this.activeStates.Values[index].state = ns;
                                                this.activeStates.Values[index].register = newReg;
                                                this.activeStates.Values[index].PatternStartTimestamp = state.PatternStartTimestamp;

                                                index = -1;

                                                ended = false;

                                                // Add epsilon arc destinations to stack
                                                if (this.epsilonStateMap == null) break;
                                                if (this.epsilonStateMap[ns] != null)
                                                {
                                                    for (int cnt2 = 0; cnt2 < this.epsilonStateMap[ns].Length; cnt2++) this.stack.Push(this.epsilonStateMap[ns][cnt2]);
                                                }
                                            }
                                            if (this.stack.Count == 0) break;
                                            ns = this.stack.Pop();
                                        }
                                        if (this.IsDeterministic) break; // We are guaranteed to have only one successful transition
                                    }
                                }
                            }
                        }
                        #endregion

                        #region singleEventStateMap
                        if ((this.singleEventStateMap != null) && (this.currentList.Count == 1))
                        {
                            var currentStateMap = this.singleEventStateMap[state.state];
                            if (currentStateMap != null)
                            {
                                var m = currentStateMap.Length;
                                for (int cnt = 0; cnt < m; cnt++)
                                {
                                    var arcinfo = currentStateMap[cnt];

                                    if (arcinfo.Fence(synctime, this.currentList[0], state.register))
                                    {
                                        var newReg = arcinfo.Transfer == null
                                            ? state.register
                                            : arcinfo.Transfer(synctime, this.currentList[0], state.register);
                                        int ns = arcinfo.toState;
                                        while (true)
                                        {
                                            if (this.isFinal[ns])
                                            {
                                                this.batch.vsync.col[this.iter] = synctime;
                                                this.batch.vother.col[this.iter] = Math.Min(state.PatternStartTimestamp + this.MaxDuration, StreamEvent.InfinitySyncTime);
                                                this.batch[this.iter] = newReg;
                                                this.batch.hash.col[this.iter] = 0;
                                                this.iter++;

                                                if (this.iter == Config.DataBatchSize) FlushContents();
                                            }

                                            if (this.hasOutgoingArcs[ns])
                                            {
                                                if (index == -1) index = this.activeStates.Insert();
                                                this.activeStates.Values[index].state = ns;
                                                this.activeStates.Values[index].register = newReg;
                                                this.activeStates.Values[index].PatternStartTimestamp = state.PatternStartTimestamp;

                                                index = -1;

                                                ended = false;

                                                // Add epsilon arc destinations to stack
                                                if (this.epsilonStateMap == null) break;
                                                if (this.epsilonStateMap[ns] != null)
                                                {
                                                    for (int cnt2 = 0; cnt2 < this.epsilonStateMap[ns].Length; cnt2++) this.stack.Push(this.epsilonStateMap[ns][cnt2]);
                                                }
                                            }
                                            if (this.stack.Count == 0) break;
                                            ns = this.stack.Pop();
                                        }
                                        if (this.IsDeterministic) break; // We are guaranteed to have only one successful transition
                                    }
                                }
                            }
                        }
                        #endregion
                    }

                    if (index == orig_index) this.activeStatesTraverser.Remove();
                    if (this.IsDeterministic) break; // We are guaranteed to have only one active state
                }
            }

            /* (2) Start new activations from the start state(s) */
            if (this.AllowOverlappingInstances || ended)
            {

                for (int counter = 0; counter < this.numStartStates; counter++)
                {
                    int startState = this.startStates[counter];

                    #region eventListStateMap
                    if (this.eventListStateMap != null)
                    {
                        var startStateMap = this.eventListStateMap[startState];
                        if (startStateMap != null)
                        {
                            var m = startStateMap.Length;
                            for (int cnt = 0; cnt < m; cnt++)
                            {
                                var arcinfo = startStateMap[cnt];
                                if (arcinfo.Fence(synctime, this.currentList, this.defaultRegister))
                                {
                                    var newReg = arcinfo.Transfer == null
                                        ? this.defaultRegister
                                        : arcinfo.Transfer(synctime, this.currentList, this.defaultRegister);
                                    int ns = arcinfo.toState;
                                    while (true)
                                    {
                                        if (this.isFinal[ns])
                                        {
                                            this.batch.vsync.col[this.iter] = synctime;
                                            this.batch.vother.col[this.iter] = Math.Min(synctime + this.MaxDuration, StreamEvent.InfinitySyncTime);
                                            this.batch[this.iter] = newReg;
                                            this.batch.hash.col[this.iter] = 0;
                                            this.iter++;

                                            if (this.iter == Config.DataBatchSize) FlushContents();
                                        }
                                        if (this.hasOutgoingArcs[ns])
                                        {
                                            int index = this.activeStates.Insert();
                                            this.activeStates.Values[index].state = ns;
                                            this.activeStates.Values[index].register = newReg;
                                            this.activeStates.Values[index].PatternStartTimestamp = synctime;

                                            // Add epsilon arc destinations to stack
                                            if (this.epsilonStateMap == null) break;
                                            if (this.epsilonStateMap[ns] != null)
                                            {
                                                for (int cnt2 = 0; cnt2 < this.epsilonStateMap[ns].Length; cnt2++) this.stack.Push(this.epsilonStateMap[ns][cnt2]);
                                            }
                                        }
                                        if (this.stack.Count == 0) break;
                                        ns = this.stack.Pop();
                                    }
                                    if (this.IsDeterministic) break; // We are guaranteed to have only one successful transition
                                }
                            }
                        }
                    }
                    #endregion

                    #region singleEventStateMap
                    if ((this.singleEventStateMap != null) && (this.currentList.Count == 1))
                    {
                        var startStateMap = this.singleEventStateMap[startState];
                        if (startStateMap != null)
                        {

                            var m = startStateMap.Length;
                            for (int cnt = 0; cnt < m; cnt++)
                            {
                                var arcinfo = startStateMap[cnt];
                                if (arcinfo.Fence(synctime, this.currentList[0], this.defaultRegister))
                                {
                                    var newReg = arcinfo.Transfer == null
                                        ? this.defaultRegister
                                        : arcinfo.Transfer(synctime, this.currentList[0], this.defaultRegister);
                                    int ns = arcinfo.toState;
                                    while (true)
                                    {
                                        if (this.isFinal[ns])
                                        {
                                            this.batch.vsync.col[this.iter] = synctime;
                                            this.batch.vother.col[this.iter] = Math.Min(synctime + this.MaxDuration, StreamEvent.InfinitySyncTime);
                                            this.batch[this.iter] = newReg;
                                            this.batch.hash.col[this.iter] = 0;
                                            this.iter++;

                                            if (this.iter == Config.DataBatchSize) FlushContents();
                                        }
                                        if (this.hasOutgoingArcs[ns])
                                        {
                                            int index = this.activeStates.Insert();
                                            this.activeStates.Values[index].state = ns;
                                            this.activeStates.Values[index].register = newReg;
                                            this.activeStates.Values[index].PatternStartTimestamp = synctime;

                                            // Add epsilon arc destinations to stack
                                            if (this.epsilonStateMap == null) break;
                                            if (this.epsilonStateMap[ns] != null)
                                            {
                                                for (int cnt2 = 0; cnt2 < this.epsilonStateMap[ns].Length; cnt2++) this.stack.Push(this.epsilonStateMap[ns][cnt2]);
                                            }
                                        }
                                        if (this.stack.Count == 0) break;
                                        ns = this.stack.Pop();
                                    }
                                    if (this.IsDeterministic) break; // We are guaranteed to have only one successful transition
                                }
                            }
                        }
                    }
                    #endregion

                    if (this.IsDeterministic) break; // We are guaranteed to have only one start state
                }
            }

            this.currentList.Clear();
        }

        public override unsafe void OnNext(StreamMessage<Empty, TPayload> batch)
        {
            var count = batch.Count;

            fixed (long* src_bv = batch.bitvector.col, src_vsync = batch.vsync.col)
            {
                for (int i = 0; i < count; i++)
                {
                    if ((src_bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                    {
                        long synctime = src_vsync[i];

                        if (synctime > this.lastSyncTime) // move time forward
                        {
                            ProcessCurrentTimestamp();
                            this.lastSyncTime = synctime;
                            this.currentList = new List<TPayload>();
                        }

                        this.currentList.Add(batch.payload.col[i]);
                    }
                    else if (batch.vother.col[i] < 0)
                    {
                        long synctime = src_vsync[i];
                        if (synctime > this.lastSyncTime) // move time forward
                        {
                            ProcessCurrentTimestamp();
                            this.lastSyncTime = synctime;
                        }

                        OnPunctuation(synctime);
                    }
                }
            }
            batch.Free();
        }

        protected override void UpdatePointers()
            => this.activeStatesTraverser = new FastLinkedList<GroupedActiveState<Empty, TRegister>>.ListTraverser(this.activeStates);
    }
}