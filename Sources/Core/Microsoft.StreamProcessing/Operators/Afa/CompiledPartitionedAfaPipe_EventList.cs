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
    internal sealed class CompiledPartitionedAfaPipe_EventList<TKey, TPayload, TRegister, TAccumulator, TPartitionKey> : CompiledAfaPipeBase<TKey, TPayload, TRegister, TAccumulator>
    {
        private readonly Func<TKey, TPartitionKey> getPartitionKey = GetPartitionExtractor<TPartitionKey, TKey>();

        [DataMember]
        private FastMap<GroupedActiveState<TKey, TRegister>> activeStates;

        private FastMap<GroupedActiveState<TKey, TRegister>>.FindTraverser activeFindTraverser;

        // The follow three fields' keys need to be updated in lock step
        [DataMember]
        private FastDictionary2<TPartitionKey, FastMap<SavedEventList<TKey, TPayload>>> currentTimestampEventList = new FastDictionary2<TPartitionKey, FastMap<SavedEventList<TKey, TPayload>>>();
        [DataMember]
        private FastDictionary2<TPartitionKey, long> lastSyncTime = new FastDictionary2<TPartitionKey, long>();

        // Field instead of local variable to avoid re-initializing it
        private readonly Stack<int> stack = new Stack<int>();

        [Obsolete("Used only by serialization. Do not call directly.")]
        public CompiledPartitionedAfaPipe_EventList() { }

        public CompiledPartitionedAfaPipe_EventList(Streamable<TKey, TRegister> stream, IStreamObserver<TKey, TRegister> observer, object afa, long maxDuration)
            : base(stream, observer, afa, maxDuration)
        {
            this.activeStates = new FastMap<GroupedActiveState<TKey, TRegister>>();
            this.activeFindTraverser = new FastMap<GroupedActiveState<TKey, TRegister>>.FindTraverser(this.activeStates);
        }

        public override int CurrentlyBufferedInputCount => this.activeStates.Count;

        private void ProcessCurrentTimestamp(int partitionIndex)
        {
            if (this.currentTimestampEventList.Count == 0) return;

            long synctime = this.lastSyncTime.entries[partitionIndex].value;

            var allEventListTraverser = new FastMap<SavedEventList<TKey, TPayload>>.VisibleTraverser(this.currentTimestampEventList.entries[partitionIndex].value)
            {
                currIndex = 0
            };

            while (allEventListTraverser.Next(out int el_index, out int el_hash))
            {
                var currentList = this.currentTimestampEventList.entries[partitionIndex].value.Values[el_index];

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

                                        if (arcinfo.Fence(synctime, currentList.payloads, state.register))
                                        {
                                            var newReg = arcinfo.Transfer == null
                                                ? state.register
                                                : arcinfo.Transfer(synctime, currentList.payloads, state.register);
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

                            #region singleEventStateMap
                            if ((this.singleEventStateMap != null) && (currentList.payloads.Count == 1))
                            {
                                var currentStateMap = this.singleEventStateMap[state.state];
                                if (currentStateMap != null)
                                {
                                    var m = currentStateMap.Length;
                                    for (int cnt = 0; cnt < m; cnt++)
                                    {
                                        var arcinfo = currentStateMap[cnt];

                                        if (arcinfo.Fence(synctime, currentList.payloads[0], state.register))
                                        {
                                            var newReg = arcinfo.Transfer == null
                                                ? state.register
                                                : arcinfo.Transfer(synctime, currentList.payloads[0], state.register);
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
                                if (arcinfo.Fence(synctime, currentList.payloads, this.defaultRegister))
                                {
                                    var newReg = arcinfo.Transfer == null
                                        ? this.defaultRegister
                                        : arcinfo.Transfer(synctime, currentList.payloads, this.defaultRegister);
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

                    #region singleEventStateMap
                    if ((this.singleEventStateMap != null) && (currentList.payloads.Count == 1))
                    {
                        var startStateMap = this.singleEventStateMap[startState];
                        if (startStateMap != null)
                        {

                            var m = startStateMap.Length;
                            for (int cnt = 0; cnt < m; cnt++)
                            {
                                var arcinfo = startStateMap[cnt];
                                if (arcinfo.Fence(synctime, currentList.payloads[0], this.defaultRegister))
                                {
                                    var newReg = arcinfo.Transfer == null
                                        ? this.defaultRegister
                                        : arcinfo.Transfer(synctime, currentList.payloads[0], this.defaultRegister);
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
            }

            this.currentTimestampEventList.entries[partitionIndex].value.Clear();
        }

        public override unsafe void OnNext(StreamMessage<TKey, TPayload> batch)
        {
            var count = batch.Count;

            var srckey = batch.key.col;

            fixed (long* src_bv = batch.bitvector.col, src_vsync = batch.vsync.col, src_vother = batch.vother.col)
            {
                fixed (int* src_hash = batch.hash.col)
                {
                    for (int i = 0; i < count; i++)
                    {
                        if ((src_bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                        {
                            var partitionKey = this.getPartitionKey(srckey[i]);
                            int partitionIndex = EnsurePartition(partitionKey);

                            long synctime = src_vsync[i];

                            int index;

                            if (synctime > this.lastSyncTime.entries[partitionIndex].value) // move time forward
                            {
                                ProcessCurrentTimestamp(partitionIndex);
                                this.lastSyncTime.entries[partitionIndex].value = synctime;
                            }

                            bool done = false;

                            var eventListTraverser = new FastMap<SavedEventList<TKey, TPayload>>.FindTraverser(this.currentTimestampEventList.entries[partitionIndex].value);
                            if (eventListTraverser.Find(src_hash[i]))
                            {
                                while (eventListTraverser.Next(out index))
                                {
                                    var state = this.currentTimestampEventList.entries[partitionIndex].value.Values[index];

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
                                index = this.currentTimestampEventList.entries[partitionIndex].value.Insert(src_hash[i]);
                                var list = new List<TPayload>(10)
                                {
                                    batch[i]
                                };
                                this.currentTimestampEventList.entries[partitionIndex].value.Values[index] = new SavedEventList<TKey, TPayload> { key = srckey[i], payloads = list };
                            }
                        }
                        else if (src_vother[i] == PartitionedStreamEvent.LowWatermarkOtherTime)
                        {
                            long synctime = src_vsync[i];
                            int partitionIndex = FastDictionary2<TPartitionKey, List<TKey>>.IteratorStart;
                            while (this.lastSyncTime.Iterate(ref partitionIndex))
                            {
                                if (synctime > this.lastSyncTime.entries[partitionIndex].value) // move time forward
                                {
                                    ProcessCurrentTimestamp(partitionIndex);
                                    this.lastSyncTime.entries[partitionIndex].value = synctime;
                                }
                            }

                            OnLowWatermark(synctime);
                        }
                        else if (src_vother[i] == PartitionedStreamEvent.PunctuationOtherTime)
                        {
                            var partitionKey = this.getPartitionKey(srckey[i]);
                            int partitionIndex = EnsurePartition(partitionKey);

                            long synctime = src_vsync[i];

                            if (synctime > this.lastSyncTime.entries[partitionIndex].value) // move time forward
                            {
                                ProcessCurrentTimestamp(partitionIndex);
                                this.lastSyncTime.entries[partitionIndex].value = synctime;
                            }
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
                this.lastSyncTime.Insert(partitionKey, long.MinValue);
                index = this.currentTimestampEventList.Insert(partitionKey, new FastMap<SavedEventList<TKey, TPayload>>());
            }
            return index;
        }

        protected override void UpdatePointers()
            => this.activeFindTraverser = new FastMap<GroupedActiveState<TKey, TRegister>>.FindTraverser(this.activeStates);
    }
}