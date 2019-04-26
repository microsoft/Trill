// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Extension methods for creating an AFA object
    /// </summary>
    public static class Afa
    {
        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TInput"></typeparam>
        /// <typeparam name="TRegister"></typeparam>
        /// <typeparam name="TAccumulator"></typeparam>
        /// <param name="defaultRegister"></param>
        /// <param name="defaultAccumulator"></param>
        /// <returns></returns>
        public static Afa<TInput, TRegister, TAccumulator> Create<TInput, TRegister, TAccumulator>(TRegister defaultRegister = default, TAccumulator defaultAccumulator = default)
            => new Afa<TInput, TRegister, TAccumulator>(defaultRegister, defaultAccumulator);

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TInput"></typeparam>
        /// <typeparam name="TRegister"></typeparam>
        /// <param name="defaultRegister"></param>
        /// <returns></returns>
        public static Afa<TInput, TRegister, bool> Create<TInput, TRegister>(TRegister defaultRegister = default)
            => new Afa<TInput, TRegister, bool>(defaultRegister);

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TInput"></typeparam>
        /// <returns></returns>
        public static Afa<TInput, Empty, bool> Create<TInput>()
            => new Afa<TInput, Empty, bool>();
    }

    /// <summary>
    /// Class that describes an AFA completely, along with its properties.
    /// </summary>
    /// <typeparam name="TInput">The event type.</typeparam>
    /// <typeparam name="TRegister">The register type.</typeparam>
    /// <typeparam name="TAccumulator">The accumulator type.</typeparam>
    public sealed class Afa<TInput, TRegister, TAccumulator>
    {
        /// <summary>
        /// Create a new instance of an AFA object.
        /// </summary>
        /// <param name="defaultRegister">The default register for new instances of the AFA.</param>
        /// <param name="defaultAccumulator">The default accumulator for new instances of the AFA.</param>
        public Afa(TRegister defaultRegister = default, TAccumulator defaultAccumulator = default)
        {
            this.DefaultRegister = defaultRegister;
            this.DefaultAccumulator = defaultAccumulator;
        }

        /// <summary>
        /// AFA is sealed or not.
        /// </summary>
        private bool isSealed = false;

        /// <summary>
        /// The set of final states in the AFA.
        /// </summary>
        internal List<int> finalStates = new List<int>();

        /// <summary>
        /// The arcs present in the AFA.
        /// </summary>
        internal Dictionary<int, Dictionary<int, Arc<TInput, TRegister>>> transitionInfo = new Dictionary<int, Dictionary<int, Arc<TInput, TRegister>>>();

        /// <summary>
        /// Start state of the AFA.
        /// </summary>
        public int StartState { get; set; }

        internal int MaxState = 0;

        /// <summary>
        /// The default value of the register.
        /// </summary>
        internal TRegister DefaultRegister = default;

        /// <summary>
        /// The default value of the accumulator.
        /// </summary>
        internal TAccumulator DefaultAccumulator = default;

        /// <summary>
        /// This property specifies whether, on a new incoming event, we allow new
        /// matches to be initiated (from AFA start state) while an existing pattern is still
        /// being matched by the AFA. If set to false, we do not start matching an incoming event from
        /// the start state of the AFA unless all currently active states have expired or retired.
        /// </summary>
        internal bool AllowOverlappingInstances = true;

        /// <summary>
        /// This property is used to assert whether or not the automaton is deterministic
        /// (i.e., an active state may transition to more than one new state on an incoming event)
        /// </summary>
        internal bool IsDeterministic = false;

        /// <summary>
        /// Adds a transition to the AFA triggered by a single element
        /// </summary>
        /// <param name="fromState">Starting state of the transition</param>
        /// <param name="toState">Ending state of the transition</param>
        /// <param name="fence">An added condition that must be met for the transition to occur</param>
        /// <param name="transfer">An expression to mutate the register value when the transition occurs</param>
        public void AddSingleElementArc(int fromState, int toState, Expression<Func<long, TInput, TRegister, bool>> fence, Expression<Func<long, TInput, TRegister, TRegister>> transfer = null)
            => AddArc(fromState, toState, new SingleElementArc<TInput, TRegister> { Fence = fence, Transfer = transfer });

        /// <summary>
        /// Adds a transition to the AFA triggered by a list of concurrent elements
        /// </summary>
        /// <param name="fromState">Starting state of the transition</param>
        /// <param name="toState">Ending state of the transition</param>
        /// <param name="fence">An added condition that must be met for the transition to occur</param>
        /// <param name="transfer">An expression to mutate the register value when the transition occurs</param>
        public void AddListElementArc(int fromState, int toState, Expression<Func<long, List<TInput>, TRegister, bool>> fence, Expression<Func<long, List<TInput>, TRegister, TRegister>> transfer = null)
            => AddArc(fromState, toState, new ListElementArc<TInput, TRegister> { Fence = fence, Transfer = transfer });

        /// <summary>
        /// Adds an epsilon (no action) arc to the AFA
        /// </summary>
        /// <param name="fromState">Starting state of the transition</param>
        /// <param name="toState">Ending state of the transition</param>
        public void AddEpsilonElementArc(int fromState, int toState)
            => AddArc(fromState, toState, new EpsilonArc<TInput, TRegister>());

        /// <summary>
        /// Adds a transition that handles multiple elements (events) at a given timestamp
        /// </summary>
        /// <param name="fromState">Starting state of the transition</param>
        /// <param name="toState">Ending state of the transition</param>
        /// <param name="initialize">An initializer statement for the accumulator</param>
        /// <param name="accumulate">A description of how to update the accumulator on transition</param>
        /// <param name="skipToEnd">A description of when to skip to the end as a part of the transition</param>
        /// <param name="fence">An added condition that must be met for the transition to occur</param>
        /// <param name="transfer">An expression to mutate the register value when the transition occurs</param>
        /// <param name="dispose">Dispose action on the accumulator</param>
        public void AddMultiElementArc(int fromState, int toState,
            Expression<Func<long, TRegister, TAccumulator>> initialize,
            Expression<Func<long, TInput, TRegister, TAccumulator, TAccumulator>> accumulate,
            Expression<Func<long, TInput, TAccumulator, bool>> skipToEnd = null,
            Expression<Func<long, TAccumulator, TRegister, bool>> fence = null,
            Expression<Func<long, TAccumulator, TRegister, TRegister>> transfer = null,
            Expression<Action<TAccumulator>> dispose = null)
            => AddArc(fromState, toState, new MultiElementArc<TInput, TRegister, TAccumulator>
            {
                Initialize = initialize,
                Accumulate = accumulate,
                SkipToEnd = skipToEnd,
                Fence = fence,
                Transfer = transfer,
                Dispose = dispose
            });

        /// <summary>
        /// Adds an arc to the AFA.
        /// </summary>
        /// <param name="fromState">The state from which the arc begins.</param>
        /// <param name="toState">The state at which the arc ends.</param>
        /// <param name="arc">The arc, defined as a method.</param>
        internal void AddArc(int fromState, int toState, Arc<TInput, TRegister> arc)
        {
            if (this.isSealed) throw new InvalidOperationException("Cannot add arcs to a sealed AFA");

            if (fromState > this.MaxState) this.MaxState = fromState;
            if (toState > this.MaxState) this.MaxState = toState;

            if (!this.transitionInfo.ContainsKey(fromState))
            {
                this.transitionInfo.Add(fromState, new Dictionary<int, Arc<TInput, TRegister>>());
            }

            if (!this.transitionInfo[fromState].ContainsKey(toState))
            {
                this.transitionInfo[fromState].Add(toState, arc);
            }
            else
            {
                this.transitionInfo[fromState][toState] = arc;
            }
        }

        /// <summary>
        /// Takes the current AFA object and performs a deep copy
        /// </summary>
        /// <returns>A deep copy of the current AFA</returns>
        public Afa<TInput, TRegister, TAccumulator> Clone()
        {
            var result = new Afa<TInput, TRegister, TAccumulator>()
            {
                StartState = this.StartState,
                MaxState = this.MaxState,
                DefaultRegister = this.DefaultRegister,
                AllowOverlappingInstances = this.AllowOverlappingInstances,
                IsDeterministic = this.IsDeterministic
            };
            foreach (var kvp1 in this.transitionInfo)
            {
                foreach (var kvp2 in kvp1.Value)
                {
                    result.AddArc(kvp1.Key, kvp2.Key, kvp2.Value);
                }
            }
            foreach (var s in this.finalStates) result.finalStates.Add(s);
            return result;
        }

        internal void Seal()
        {
            if (this.finalStates.Count == 0)
            {
                this.finalStates.Add(this.MaxState);
            }

            this.isSealed = true;
        }

        /// <summary>
        /// Whether or not the AFA is sealed.
        /// </summary>
        /// <returns></returns>
        internal bool IsSealed() => this.isSealed;

        /// <summary>
        /// Add a final state to the AFA.
        /// </summary>
        /// <param name="state">The state being added as a final state.</param>
        public void AddFinalState(int state)
        {
            if (!this.finalStates.Contains(state)) this.finalStates.Add(state);
        }

        /// <summary>
        /// Remove a final state from the AFA.
        /// </summary>
        /// <param name="state">The state being added as a final state.</param>
        public void RemoveFinalState(int state)
        {
            while (this.finalStates.Contains(state)) this.finalStates.Remove(state);
        }

        /// <summary>
        /// Set default value of register.
        /// </summary>
        /// <param name="register"></param>
        public void SetDefaultRegister(TRegister register) => this.DefaultRegister = register;

        internal CompiledAfa<TInput, TRegister, TAccumulator> Compile() => new CompiledAfa<TInput, TRegister, TAccumulator>(this);

        /// <summary>
        /// Returns a string representation of the AFA
        /// </summary>
        /// <returns>A string representation of the AFA</returns>
        public override string ToString()
        {
            string result = string.Empty;
            result += $"Start State: {this.StartState}\n";
            result += "Final States:";
            this.finalStates.ForEach(x => result += " " + x);
            result += "\n";
            result += "Arcs:\n";
            foreach (var kvp1 in this.transitionInfo)
            {
                foreach (var kvp2 in kvp1.Value)
                {
                    result += kvp1.Key + " -> " + kvp2.Key + ": " + kvp2.Value.ToString() + "\n";
                }
            }

            return result;
        }
    }
}