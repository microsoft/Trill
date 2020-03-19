// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;

namespace Microsoft.StreamProcessing
{
    internal sealed class CompiledAfa<TPayload, TRegister, TAccumulator>
    {
        public bool[] isFinal;
        public bool[] hasOutgoingArcs;

        public SingleEventArcInfo<TPayload, TRegister>[][] singleEventStateMap;
        public EventListArcInfo<TPayload, TRegister>[][] eventListStateMap;
        public MultiEventArcInfo<TPayload, TRegister, TAccumulator>[][] multiEventStateMap;

        public int[][] epsilonStateMap;
        public int[] startStates;
        public int numStartStates;
        public readonly TRegister defaultRegister;
        private readonly TAccumulator defaultAccumulator;

        internal Afa<TPayload, TRegister, TAccumulator> uncompiledAfa;

        public CompiledAfa(Afa<TPayload, TRegister, TAccumulator> afa)
        {
            if (!afa.IsSealed()) throw new InvalidOperationException("Cannot compile an unsealed AFA");
            this.uncompiledAfa = afa;
            this.defaultRegister = afa.DefaultRegister;
            this.defaultAccumulator = afa.DefaultAccumulator;
            CompileAfa(afa);
        }

        private void CompileAfa(Afa<TPayload, TRegister, TAccumulator> afa)
        {
            this.isFinal = new bool[afa.MaxState + 1];
            this.hasOutgoingArcs = new bool[afa.MaxState + 1];

            int nst = afa.StartState;
            var startStatesList = new List<int>();
            var stack = new Stack<int>();

            while (true)
            {
                startStatesList.Add(nst);

                if (afa.transitionInfo[nst] != null)
                {
                    foreach (var kvp in afa.transitionInfo[nst])
                    {
                        var to = kvp.Key;
                        var arc = kvp.Value;
                        if (arc.ArcType == ArcType.Epsilon)
                        {
                            stack.Push(to);
                        }
                    }
                }

                if (stack.Count == 0) break;
                nst = stack.Pop();
            }

            this.startStates = startStatesList.ToArray();
            this.numStartStates = startStatesList.Count;

            // Compile the automaton
            foreach (var x in afa.finalStates)
            {
                this.isFinal[x] = true;
            }

            bool knownDet = true;

            for (int from = 0; from <= afa.MaxState; from++)
            {
                this.hasOutgoingArcs[from] = false;

                int epsilonCount = 0;
                int singleEventCount = 0;
                int eventListCount = 0;
                int multiEventCount = 0;

                if (afa.transitionInfo.ContainsKey(from))
                {
                    this.hasOutgoingArcs[from] = true;
                    foreach (var kvp in afa.transitionInfo[from])
                    {
                        var to = kvp.Key;
                        var arc = kvp.Value;
                        switch (arc.ArcType)
                        {
                            case ArcType.Epsilon:
                                if (from == to) throw new InvalidOperationException("Self-looping epsilon states are not allowed");

                                if (this.epsilonStateMap == null) this.epsilonStateMap = new int[afa.MaxState + 1][];
                                epsilonCount++;
                                break;
                            case ArcType.SingleElement:
                                if (this.singleEventStateMap == null) this.singleEventStateMap = new SingleEventArcInfo<TPayload, TRegister>[afa.MaxState + 1][];
                                singleEventCount++;
                                break;
                            case ArcType.ListElement:
                                if (this.eventListStateMap == null) this.eventListStateMap = new EventListArcInfo<TPayload, TRegister>[afa.MaxState + 1][];
                                eventListCount++;
                                break;
                            case ArcType.MultiElement:
                                if (this.multiEventStateMap == null) this.multiEventStateMap = new MultiEventArcInfo<TPayload, TRegister, TAccumulator>[afa.MaxState + 1][];
                                multiEventCount++;
                                break;
                            default:
                                throw new NotSupportedException();
                        }
                    }
                }
                if (singleEventCount > 0) this.singleEventStateMap[from] = new SingleEventArcInfo<TPayload, TRegister>[singleEventCount];

                if (eventListCount > 0) this.eventListStateMap[from] = new EventListArcInfo<TPayload, TRegister>[eventListCount];

                if (multiEventCount > 0) this.multiEventStateMap[from] = new MultiEventArcInfo<TPayload, TRegister, TAccumulator>[multiEventCount];

                if (epsilonCount > 0) this.epsilonStateMap[from] = new int[epsilonCount];

                if (singleEventCount + eventListCount + multiEventCount + epsilonCount > 1) knownDet = false;

                singleEventCount = epsilonCount = eventListCount = multiEventCount = 0;

                ListElementArc<TPayload, TRegister> learc;
                MultiElementArc<TPayload, TRegister, TAccumulator> mearc;

                if (afa.transitionInfo.ContainsKey(from))
                {
                    foreach (var kvp in afa.transitionInfo[from])
                    {
                        var to = kvp.Key;
                        var arc = kvp.Value;
                        switch (arc.ArcType)
                        {
                            case ArcType.Epsilon:
                                this.epsilonStateMap[from][epsilonCount] = to;
                                epsilonCount++;
                                break;
                            case ArcType.SingleElement:
                                var searc = arc as SingleElementArc<TPayload, TRegister>;

                                this.singleEventStateMap[from][singleEventCount] =
                                    new SingleEventArcInfo<TPayload, TRegister>
                                    {
                                        toState = to,
                                        Fence = searc.Fence.Compile(),
                                        Transfer = searc.Transfer?.Compile(),
                                        arcType = searc.ArcType
                                    };
                                singleEventCount++;
                                break;
                            case ArcType.MultiElement:
                                mearc = arc as MultiElementArc<TPayload, TRegister, TAccumulator>;

                                this.multiEventStateMap[from][multiEventCount] =
                                    new MultiEventArcInfo<TPayload, TRegister, TAccumulator>
                                    {
                                        toState = to,
                                        Initialize = mearc.Initialize != null
                                            ? mearc.Initialize.Compile()
                                            : (ts, reg) => this.defaultAccumulator,
                                        Accumulate = mearc.Accumulate != null ? mearc.Accumulate.Compile() : (ts, ev, reg, acc) => acc,
                                        SkipToEnd = mearc.SkipToEnd?.Compile(),
                                        Dispose = mearc.Dispose?.Compile(),
                                        Fence = mearc.Fence.Compile(),
                                        Transfer = mearc.Transfer?.Compile(),
                                        arcType = mearc.ArcType
                                    };
                                for (int i = 0; i < this.numStartStates; i++)
                                {
                                    if (from == this.startStates[i])
                                    {
                                        this.multiEventStateMap[from][multiEventCount].fromStartState = true;
                                    }
                                }
                                multiEventCount++;
                                break;
                            case ArcType.ListElement:
                                learc = arc as ListElementArc<TPayload, TRegister>;

                                this.eventListStateMap[from][eventListCount] =
                                    new EventListArcInfo<TPayload, TRegister>
                                    {
                                        toState = to,
                                        Fence = learc.Fence.Compile(),
                                        Transfer = learc.Transfer?.Compile(),
                                        arcType = learc.ArcType
                                    };
                                eventListCount++;
                                break;
                            default:
                                throw new NotSupportedException();
                        }
                    }
                }
            }

            if (knownDet) this.uncompiledAfa.IsDeterministic = true;

            // Deterministic, but overlapping instances allowed => effectively non-deterministic
            if (this.uncompiledAfa.IsDeterministic && this.uncompiledAfa.AllowOverlappingInstances) this.uncompiledAfa.IsDeterministic = false;
        }
    }
}