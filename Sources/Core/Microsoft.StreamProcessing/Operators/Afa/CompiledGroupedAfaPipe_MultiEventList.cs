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
    internal sealed class CompiledGroupedAfaPipe_MultiEventList<TKey, TPayload, TRegister, TAccumulator> : CompiledAfaPipeBase<TKey, TPayload, TRegister, TAccumulator>
    {
        [DataMember]
        private FastMap<GroupedActiveState<TKey, TRegister>> activeStates;

        private FastMap<GroupedActiveState<TKey, TRegister>>.FindTraverser activeFindTraverser;

        [DataMember]
        private FastMap<SavedEventList<TKey, TPayload>> currentTimestampEventList;

        private FastMap<SavedEventList<TKey, TPayload>>.FindTraverser eventListTraverser;
        private FastMap<SavedEventList<TKey, TPayload>>.VisibleTraverser allEventListTraverser;

        [DataMember]
        private long lastSyncTime;

        // Field instead of local variable to avoid re-initializing it
        private readonly Stack<int> stack = new Stack<int>();

        [Obsolete("Used only by serialization. Do not call directly.")]
        public CompiledGroupedAfaPipe_MultiEventList() { }

        public CompiledGroupedAfaPipe_MultiEventList(Streamable<TKey, TRegister> stream, IStreamObserver<TKey, TRegister> observer, object afa, long maxDuration)
            : base(stream, observer, afa, maxDuration)
        {
            this.activeStates = new FastMap<GroupedActiveState<TKey, TRegister>>();
            this.activeFindTraverser = new FastMap<GroupedActiveState<TKey, TRegister>>.FindTraverser(this.activeStates);

            this.currentTimestampEventList = new FastMap<SavedEventList<TKey, TPayload>>();
            this.eventListTraverser = new FastMap<SavedEventList<TKey, TPayload>>.FindTraverser(this.currentTimestampEventList);
            this.allEventListTraverser = new FastMap<SavedEventList<TKey, TPayload>>.VisibleTraverser(this.currentTimestampEventList);
            this.lastSyncTime = -1;
        }

        public override int CurrentlyBufferedInputCount => this.activeStates.Count;

        private void ProcessCurrentTimestamp()
        {
            if (this.currentTimestampEventList.Count == 0) return;

            long synctime = this.lastSyncTime;

            this.allEventListTraverser.currIndex = 0;

            while (this.allEventListTraverser.Next(out int el_index, out int el_hash))
            {
                var currentList = this.currentTimestampEventList.Values[el_index];
                List<TPayload> payloadList = null;

                /* (1) Process currently active states */
                bool ended = true;
                if (this.activeFindTraverser.Find(el_hash))
                {
                    int orig_index;

                    while (this.activeFindTraverser.Next(out int index))
                    {
                        orig_index = index;

                        var state = this.activeStates.Values[index];
                        if (!this.keyEqualityComparer(state.key, currentList.key)) continue;

                        if (state.PatternStartTimestamp + this.MaxDuration > synctime)
                        {
                            #region multiEventStateMap
                            if (this.multiEventStateMap != null)
                            {
                                var currentStateMap = this.multiEventStateMap[state.state];
                                if (currentStateMap != null)
                                {
                                    var m = currentStateMap.Length;
                                    for (int cnt = 0; cnt < m; cnt++)
                                    {
                                        var arcinfo = currentStateMap[cnt];

                                        var acc = arcinfo.Initialize(synctime, state.register);
                                        for (int i = 0; i < currentList.payloads.Count; i++)
                                        {
                                            var payload = currentList.payloads[i];
                                            acc = arcinfo.Accumulate(synctime, payload, state.register, acc);
                                            if ((arcinfo.SkipToEnd != null) && arcinfo.SkipToEnd(synctime, payload, acc)) break;
                                        }

                                        if (arcinfo.Fence(synctime, acc, state.register))
                                        {
                                            var newReg = arcinfo.Transfer == null
                                                ? state.register
                                                : arcinfo.Transfer(synctime, acc, state.register);
                                            int ns = arcinfo.toState;
                                            while (true)
                                            {
                                                if (this.isFinal[ns])
                                                {
                                                    this.batch.vsync.col[this.iter] = synctime;
                                                    this.batch.vother.col[this.iter] = Math.Min(state.PatternStartTimestamp + this.MaxDuration, StreamEvent.InfinitySyncTime);
                                                    this.batch[this.iter] = newReg;
                                                    this.batch.key.col[this.iter] = currentList.key;
                                                    this.batch.hash.col[this.iter] = el_hash;
                                                    this.iter++;

                                                    if (this.iter == Config.DataBatchSize) FlushContents();
                                                }

                                                if (this.hasOutgoingArcs[ns])
                                                {
                                                    if (index == -1) index = this.activeStates.Insert(el_hash);
                                                    this.activeStates.Values[index].key = currentList.key;
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
                                        arcinfo.Dispose?.Invoke(acc);
                                    }
                                }
                            }
                            #endregion

                            #region singleEventStateMap
                            if ((this.singleEventStateMap != null) && (currentList.payloads.Count == 1))
                            {
                                var item = currentList.payloads[0];
                                var currentStateMap = this.singleEventStateMap[state.state];
                                if (currentStateMap != null)
                                {
                                    var m = currentStateMap.Length;
                                    for (int cnt = 0; cnt < m; cnt++)
                                    {
                                        var arcinfo = currentStateMap[cnt];

                                        if (arcinfo.Fence(synctime, item, state.register))
                                        {
                                            var newReg = arcinfo.Transfer == null
                                                ? state.register
                                                : arcinfo.Transfer(synctime, item, state.register);
                                            int ns = arcinfo.toState;
                                            while (true)
                                            {
                                                if (this.isFinal[ns])
                                                {
                                                    this.batch.vsync.col[this.iter] = synctime;
                                                    this.batch.vother.col[this.iter] = Math.Min(state.PatternStartTimestamp + this.MaxDuration, StreamEvent.InfinitySyncTime);
                                                    this.batch[this.iter] = newReg;
                                                    this.batch.key.col[this.iter] = currentList.key;
                                                    this.batch.hash.col[this.iter] = el_hash;
                                                    this.iter++;

                                                    if (this.iter == Config.DataBatchSize) FlushContents();
                                                }

                                                if (this.hasOutgoingArcs[ns])
                                                {
                                                    if (index == -1) index = this.activeStates.Insert(el_hash);
                                                    this.activeStates.Values[index].key = currentList.key;
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

                            #region eventListStateMap
                            if (this.eventListStateMap != null)
                            {
                                var currentStateMap = this.eventListStateMap[state.state];
                                if (currentStateMap != null)
                                {
                                    if (payloadList == null)
                                    {
                                        payloadList = new List<TPayload>(currentList.payloads.Count);
                                        for (int i = 0; i < currentList.payloads.Count; i++)
                                        {
                                            var payload = currentList.payloads[i];
                                            payloadList.Add(payload);
                                        }
                                    }

                                    var m = currentStateMap.Length;
                                    for (int cnt = 0; cnt < m; cnt++)
                                    {
                                        var arcinfo = currentStateMap[cnt];

                                        if (arcinfo.Fence(synctime, payloadList, state.register))
                                        {
                                            var newReg = arcinfo.Transfer == null
                                                ? state.register
                                                : arcinfo.Transfer(synctime, payloadList, state.register);
                                            int ns = arcinfo.toState;
                                            while (true)
                                            {
                                                if (this.isFinal[ns])
                                                {
                                                    this.batch.vsync.col[this.iter] = synctime;
                                                    this.batch.vother.col[this.iter] = Math.Min(state.PatternStartTimestamp + this.MaxDuration, StreamEvent.InfinitySyncTime);
                                                    this.batch[this.iter] = newReg;
                                                    this.batch.key.col[this.iter] = currentList.key;
                                                    this.batch.hash.col[this.iter] = el_hash;
                                                    this.iter++;

                                                    if (this.iter == Config.DataBatchSize) FlushContents();
                                                }

                                                if (this.hasOutgoingArcs[ns])
                                                {
                                                    if (index == -1) index = this.activeStates.Insert(el_hash);
                                                    this.activeStates.Values[index].key = currentList.key;
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
                        if (index == orig_index) this.activeFindTraverser.Remove();
                        if (this.IsDeterministic) break; // We are guaranteed to have only one active state
                    }
                }

                /* (2) Start new activations from the start state(s) */
                if (!this.AllowOverlappingInstances && !ended) continue;

                for (int counter = 0; counter < this.numStartStates; counter++)
                {
                    int startState = this.startStates[counter];

                    #region multiEventStateMap
                    if (this.multiEventStateMap != null)
                    {
                        var startStateMap = this.multiEventStateMap[startState];
                        if (startStateMap != null)
                        {
                            var m = startStateMap.Length;
                            for (int cnt = 0; cnt < m; cnt++)
                            {
                                var arcinfo = startStateMap[cnt];

                                var acc = arcinfo.Initialize(synctime, this.defaultRegister);
                                for (int i = 0; i < currentList.payloads.Count; i++)
                                {
                                    var payload = currentList.payloads[i];
                                    acc = arcinfo.Accumulate(synctime, payload, this.defaultRegister, acc);
                                    if ((arcinfo.SkipToEnd != null) && arcinfo.SkipToEnd(synctime, payload, acc)) break;
                                }

                                if (arcinfo.Fence(synctime, acc, this.defaultRegister))
                                {
                                    var newReg = arcinfo.Transfer == null
                                        ? this.defaultRegister
                                        : arcinfo.Transfer(synctime, acc, this.defaultRegister);
                                    int ns = arcinfo.toState;
                                    while (true)
                                    {
                                        if (this.isFinal[ns])
                                        {
                                            this.batch.vsync.col[this.iter] = synctime;
                                            this.batch.vother.col[this.iter] = Math.Min(synctime + this.MaxDuration, StreamEvent.InfinitySyncTime);
                                            this.batch[this.iter] = newReg;
                                            this.batch.key.col[this.iter] = currentList.key;
                                            this.batch.hash.col[this.iter] = el_hash;
                                            this.iter++;

                                            if (this.iter == Config.DataBatchSize) FlushContents();
                                        }
                                        if (this.hasOutgoingArcs[ns])
                                        {
                                            int index = this.activeStates.Insert(el_hash);
                                            this.activeStates.Values[index].key = currentList.key;
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
                                arcinfo.Dispose?.Invoke(acc);
                            }
                        }
                    }
                    #endregion

                    #region singleEventStateMap
                    if ((this.singleEventStateMap != null) && (currentList.payloads.Count == 1))
                    {
                        var item = currentList.payloads[0];

                        var startStateMap = this.singleEventStateMap[startState];
                        if (startStateMap != null)
                        {
                            var m = startStateMap.Length;
                            for (int cnt = 0; cnt < m; cnt++)
                            {
                                var arcinfo = startStateMap[cnt];

                                if (arcinfo.Fence(synctime, item, this.defaultRegister))
                                {
                                    var newReg = arcinfo.Transfer == null
                                        ? this.defaultRegister
                                        : arcinfo.Transfer(synctime, item, this.defaultRegister);
                                    int ns = arcinfo.toState;
                                    while (true)
                                    {
                                        if (this.isFinal[ns])
                                        {
                                            this.batch.vsync.col[this.iter] = synctime;
                                            this.batch.vother.col[this.iter] = Math.Min(synctime + this.MaxDuration, StreamEvent.InfinitySyncTime);
                                            this.batch[this.iter] = newReg;
                                            this.batch.key.col[this.iter] = currentList.key;
                                            this.batch.hash.col[this.iter] = el_hash;
                                            this.iter++;

                                            if (this.iter == Config.DataBatchSize) FlushContents();
                                        }
                                        if (this.hasOutgoingArcs[ns])
                                        {
                                            int index = this.activeStates.Insert(el_hash);
                                            this.activeStates.Values[index].key = currentList.key;
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

                    #region eventListStateMap
                    if (this.eventListStateMap != null)
                    {
                        var startStateMap = this.eventListStateMap[startState];
                        if (startStateMap != null)
                        {
                            if (payloadList == null)
                            {
                                payloadList = new List<TPayload>(currentList.payloads.Count);
                                for (int i = 0; i < currentList.payloads.Count; i++)
                                {
                                    var payload = currentList.payloads[i];
                                    payloadList.Add(payload);
                                }
                            }

                            var m = startStateMap.Length;
                            for (int cnt = 0; cnt < m; cnt++)
                            {
                                var arcinfo = startStateMap[cnt];

                                if (arcinfo.Fence(synctime, payloadList, this.defaultRegister))
                                {
                                    var newReg = arcinfo.Transfer == null
                                        ? this.defaultRegister
                                        : arcinfo.Transfer(synctime, payloadList, this.defaultRegister);
                                    int ns = arcinfo.toState;
                                    while (true)
                                    {
                                        if (this.isFinal[ns])
                                        {
                                            this.batch.vsync.col[this.iter] = synctime;
                                            this.batch.vother.col[this.iter] = Math.Min(synctime + this.MaxDuration, StreamEvent.InfinitySyncTime);
                                            this.batch[this.iter] = newReg;
                                            this.batch.key.col[this.iter] = currentList.key;
                                            this.batch.hash.col[this.iter] = el_hash;
                                            this.iter++;

                                            if (this.iter == Config.DataBatchSize) FlushContents();
                                        }
                                        if (this.hasOutgoingArcs[ns])
                                        {
                                            int index = this.activeStates.Insert(el_hash);
                                            this.activeStates.Values[index].key = currentList.key;
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

                currentList.payloads = null; // Let GC handle this
                payloadList = null;
            }

            this.currentTimestampEventList.Clear();
        }

        public override unsafe void OnNext(StreamMessage<TKey, TPayload> batch)
        {
            var count = batch.Count;

            var srckey = batch.key.col;

            SavedEventList<TKey, TPayload> sevref = default;
            fixed (long* src_bv = batch.bitvector.col, src_vsync = batch.vsync.col)
            {
                fixed (int* src_hash = batch.hash.col)
                {
                    for (int i = 0; i < count; i++)
                    {
                        if ((src_bv[i >> 6] & (1L << (i & 0x3f))) == 0 || batch.vother.col[i] < 0)
                        {
                            long synctime = src_vsync[i];

                            int index;

                            if (synctime > this.lastSyncTime) // move time forward
                            {
                                ProcessCurrentTimestamp();
                                this.lastSyncTime = synctime;
                            }

                            if (batch.vother.col[i] < 0)
                            {
                                OnPunctuation(synctime);
                                continue;
                            }

                            bool done = false;

                            if (this.eventListTraverser.Find(src_hash[i]))
                            {
                                while (this.eventListTraverser.Next(out index))
                                {
                                    var state = this.currentTimestampEventList.Values[index];

                                    if (this.keyEqualityComparer(state.key, srckey[i]))
                                    {
                                        state.payloads.Add(batch.payload.col[i]);

                                        done = true;
                                        break;
                                    }
                                }
                            }

                            if (!done)
                            {
                                index = this.currentTimestampEventList.Insert(src_hash[i]);
                                sevref.payloads = new List<TPayload>(10)
                                {
                                    batch.payload.col[i]
                                };

                                sevref.key = srckey[i];
                                this.currentTimestampEventList.Values[index] = sevref;
                            }
                        }
                    }
                }
            }
            batch.Free();
        }

        protected override void UpdatePointers()
        {
            this.activeFindTraverser = new FastMap<GroupedActiveState<TKey, TRegister>>.FindTraverser(this.activeStates);
            this.eventListTraverser = new FastMap<SavedEventList<TKey, TPayload>>.FindTraverser(this.currentTimestampEventList);
            this.allEventListTraverser = new FastMap<SavedEventList<TKey, TPayload>>.VisibleTraverser(this.currentTimestampEventList);
        }
    }
}