// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class StatefulSelectPipe<TKey, TSource, TState, TResult> : UnaryPipe<TKey, TSource, TResult>
    {
        private readonly MemoryPool<TKey, TResult> pool;
        private readonly string errorMessages;

        [SchemaSerialization]
        private Expression<Func<TState>> initialState;
        private readonly Func<TState> initialStateFunc;

        [SchemaSerialization]
        private Expression<Func<long, TState, TSource, TResult>> statefulSelect;
        private readonly Func<long, TState, TSource, TResult> statefulSelectFunc;

        [SchemaSerialization]
        private long stateTimeout;

        /// <summary>
        /// Time-ordered collection of states
        /// </summary>
        [DataMember]
        private LinkedList<StateWrapper<TKey, TState>> statesByTime = new LinkedList<StateWrapper<TKey, TState>>();

        /// <summary>
        /// Dictionary of states by the key. This dictionary is not serialized and will be recomputed after deserialization from statesByTime.
        /// </summary>
        private readonly Dictionary<TKey, LinkedListNode<StateWrapper<TKey, TState>>> statesByKey = new Dictionary<TKey, LinkedListNode<StateWrapper<TKey, TState>>>();

        [Obsolete("Used only by serialization. Do not call directly.")]
        public StatefulSelectPipe() { }

        public StatefulSelectPipe(
            StatefulSelectStreamable<TKey, TSource, TState, TResult> stream,
            IStreamObserver<TKey, TResult> observer,
            Expression<Func<long, TState, TSource, TResult>> statefulSelect,
            Expression<Func<TState>> initialState,
            long stateTimeout)
            : base(stream, observer)
        {
            this.statefulSelect = statefulSelect;
            this.statefulSelectFunc = this.statefulSelect.Compile();
            this.pool = MemoryManager.GetMemoryPool<TKey, TResult>(stream.Properties.IsColumnar);
            this.errorMessages = stream.ErrorMessages;

            this.stateTimeout = stateTimeout;

            this.initialState = initialState;
            this.initialStateFunc = this.initialState.Compile();
        }

        public override int CurrentlyBufferedOutputCount => 0;
        public override int CurrentlyBufferedInputCount => 0;

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(
                new StatefulSelectPlanNode(
                    previous,
                    pipe: this,
                    keyType: typeof(TKey),
                    inputType: typeof(TSource),
                    payloadType: typeof(TResult),
                    selector: this.statefulSelect,
                    isGenerated: false,
                    errorMessages: this.errorMessages));

        public override unsafe void OnNext(StreamMessage<TKey, TSource> batch)
        {
            this.pool.Get(out StreamMessage<TKey, TResult> outputBatch);

            outputBatch.vsync = batch.vsync;
            outputBatch.vother = batch.vother;
            outputBatch.key = batch.key;
            outputBatch.hash = batch.hash;
            outputBatch.iter = batch.iter;
            this.pool.GetPayload(out outputBatch.payload);
            outputBatch.bitvector = batch.bitvector;

            var count = batch.Count;
            var maxVsync = -1L;
            var sourceKey = batch.key.col;
            fixed (long* bv = batch.bitvector.col,
                   vsync = batch.vsync.col)
            {
                for (var i = 0; i < count; i++)
                {
                    if ((bv[i >> 6] & (1L << (i & 0x3f))) != 0)
                    {
                        if (batch.vother.col[i] == StreamEvent.PunctuationOtherTime)
                        {
                            maxVsync = Math.Max(maxVsync, vsync[i]);
                        }

                        continue;
                    }

                    maxVsync = Math.Max(maxVsync, vsync[i]);

                    // Retrieve or create the state node
                    if (this.statesByKey.TryGetValue(sourceKey[i], out var stateNode))
                    {
                        this.statesByTime.Remove(stateNode); // Node will be reinserted at the end
                        if (stateNode.Value.LastVsync < vsync[i] - this.stateTimeout)
                        {
                            stateNode.Value.State = this.initialStateFunc(); // Reset expired state
                        }
                    }
                    else
                    {
                        stateNode = new LinkedListNode<StateWrapper<TKey, TState>>(
                            new StateWrapper<TKey, TState> { State = this.initialStateFunc(), Key = sourceKey[i] });
                        this.statesByKey.Add(sourceKey[i], stateNode);
                    }

                    // Update timestamp and add to the end of the time index
                    stateNode.Value.LastVsync = vsync[i];
                    this.statesByTime.AddLast(stateNode);

                    outputBatch[i] = this.statefulSelectFunc(vsync[i], stateNode.Value.State, batch.payload.col[i]);
                }
            }

            // Remove expired states
            while (this.statesByTime.Count > 0 &&
                this.statesByTime.First.Value.LastVsync < maxVsync - this.stateTimeout)
            {
                this.statesByKey.Remove(this.statesByTime.First.Value.Key);
                this.statesByTime.RemoveFirst();
            }

            outputBatch.Count = count;
            batch.payload.Return();
            batch.Return();
            this.Observer.OnNext(outputBatch);
        }

        protected override void UpdatePointers()
        {
            var node = this.statesByTime.First;
            while (node != null)
            {
                this.statesByKey.Add(node.Value.Key, node);
                node = node.Next;
            }
        }
    }

    /// <summary>
    /// State wrapper allowing efficient update within dictionary
    /// </summary>
    [DataContract]
    internal class StateWrapper<TK, T>
    {
        [DataMember]
        internal T State { get; set; }

        [DataMember]
        internal TK Key { get; set; }

        [DataMember]
        internal long LastVsync { get; set; }
    }
}