// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    internal class PatternMatcher<TKey, TPayload, TRegister, TAccumulator> : IPattern<TKey, TPayload, TRegister, TAccumulator>, IAbstractPattern<TKey, TPayload, TRegister, TAccumulator>
    {
        private readonly IStreamable<TKey, TPayload> source;
        private readonly TRegister defaultRegister;
        private readonly TAccumulator defaultAccumulator;

        #region Constructors
        public PatternMatcher(IStreamable<TKey, TPayload> source = default, Afa<TPayload, TRegister, TAccumulator> afa = default, TRegister defaultRegister = default, TAccumulator defaultAccumulator = default)
        {
            this.source = source;
            this.AFA = afa;
            this.defaultRegister = defaultRegister;
            this.defaultAccumulator = defaultAccumulator;
        }
        #endregion

        #region Properties
        public Afa<TPayload, TRegister, TAccumulator> AFA { get; private set; }
        #endregion

        #region SingleElement
        public IPattern<TKey, TPayload, TRegister, TAccumulator> SingleElement(Expression<Func<long, TPayload, TRegister, bool>> condition = null, Expression<Func<long, TPayload, TRegister, TRegister>> aggregator = null)
        {
            var afa = new Afa<TPayload, TRegister, TAccumulator>(this.defaultRegister, this.defaultAccumulator);
            afa.AddArc(0, 1, new SingleElementArc<TPayload, TRegister> { Fence = condition, Transfer = aggregator });
            afa.Seal();

            return Concat(x => new PatternMatcher<TKey, TPayload, TRegister, TAccumulator>(this.source, afa));
        }

        public IPattern<TKey, TPayload, TRegister, TAccumulator> SingleElement(Expression<Func<long, TPayload, TRegister, bool>> condition, Expression<Func<TPayload, TRegister, TRegister>> aggregator)
        {
            Expression<Func<long, TPayload, TRegister, TRegister>> aggregatorTemplate = (ts, ev, r) => CallInliner.Call(aggregator, ev, r);
            return SingleElement(condition, aggregatorTemplate.InlineCalls());
        }

        public IPattern<TKey, TPayload, TRegister, TAccumulator> SingleElement(Expression<Func<long, TPayload, TRegister, bool>> condition, Expression<Func<TPayload, TRegister>> aggregator)
        {
            Expression<Func<long, TPayload, TRegister, TRegister>> aggregatorTemplate = (ts, ev, r) => CallInliner.Call(aggregator, ev);
            return SingleElement(condition, aggregatorTemplate.InlineCalls());
        }

        public IPattern<TKey, TPayload, TRegister, TAccumulator> SingleElement(Expression<Func<TPayload, TRegister, bool>> condition, Expression<Func<long, TPayload, TRegister, TRegister>> aggregator = null)
        {
            Expression<Func<long, TPayload, TRegister, bool>> conditionTemplate = (ts, ev, r) => CallInliner.Call(condition, ev, r);
            return SingleElement(conditionTemplate.InlineCalls(), aggregator);
        }

        public IPattern<TKey, TPayload, TRegister, TAccumulator> SingleElement(Expression<Func<TPayload, TRegister, bool>> condition, Expression<Func<TPayload, TRegister, TRegister>> aggregator)
        {
            Expression<Func<long, TPayload, TRegister, bool>> conditionTemplate = (ts, ev, r) => CallInliner.Call(condition, ev, r);
            Expression<Func<long, TPayload, TRegister, TRegister>> aggregatorTemplate = (ts, ev, r) => CallInliner.Call(aggregator, ev, r);
            return SingleElement(conditionTemplate.InlineCalls(), aggregatorTemplate.InlineCalls());
        }

        public IPattern<TKey, TPayload, TRegister, TAccumulator> SingleElement(Expression<Func<TPayload, TRegister, bool>> condition, Expression<Func<TPayload, TRegister>> aggregator)
        {
            Expression<Func<long, TPayload, TRegister, bool>> conditionTemplate = (ts, ev, r) => CallInliner.Call(condition, ev, r);
            Expression<Func<long, TPayload, TRegister, TRegister>> aggregatorTemplate = (ts, ev, r) => CallInliner.Call(aggregator, ev);
            return SingleElement(conditionTemplate.InlineCalls(), aggregatorTemplate.InlineCalls());
        }

        public IPattern<TKey, TPayload, TRegister, TAccumulator> SingleElement(Expression<Func<TPayload, bool>> condition, Expression<Func<long, TPayload, TRegister, TRegister>> aggregator = null)
        {
            Expression<Func<long, TPayload, TRegister, bool>> conditionTemplate = (ts, ev, r) => CallInliner.Call(condition, ev);
            return SingleElement(conditionTemplate.InlineCalls(), aggregator);
        }

        public IPattern<TKey, TPayload, TRegister, TAccumulator> SingleElement(Expression<Func<TPayload, bool>> condition, Expression<Func<TPayload, TRegister, TRegister>> aggregator)
        {
            Expression<Func<long, TPayload, TRegister, bool>> conditionTemplate = (ts, ev, r) => CallInliner.Call(condition, ev);
            Expression<Func<long, TPayload, TRegister, TRegister>> aggregatorTemplate = (ts, ev, r) => CallInliner.Call(aggregator, ev, r);
            return SingleElement(conditionTemplate.InlineCalls(), aggregatorTemplate.InlineCalls());
        }

        public IPattern<TKey, TPayload, TRegister, TAccumulator> SingleElement(Expression<Func<TPayload, bool>> condition, Expression<Func<TPayload, TRegister>> aggregator)
        {
            Expression<Func<long, TPayload, TRegister, bool>> conditionTemplate = (ts, ev, r) => CallInliner.Call(condition, ev);
            Expression<Func<long, TPayload, TRegister, TRegister>> aggregatorTemplate = (ts, ev, r) => CallInliner.Call(aggregator, ev);
            return SingleElement(conditionTemplate.InlineCalls(), aggregatorTemplate.InlineCalls());
        }
        #endregion

        #region ListElement
        public IPattern<TKey, TPayload, TRegister, TAccumulator> ListElement(Expression<Func<long, List<TPayload>, TRegister, bool>> condition = null, Expression<Func<long, List<TPayload>, TRegister, TRegister>> aggregator = null)
        {
            var afa = new Afa<TPayload, TRegister, TAccumulator>(this.defaultRegister, this.defaultAccumulator);
            afa.AddArc(0, 1, new ListElementArc<TPayload, TRegister> { Fence = condition, Transfer = aggregator });
            afa.Seal();

            return Concat(x => new PatternMatcher<TKey, TPayload, TRegister, TAccumulator>(this.source, afa));
        }

        public IPattern<TKey, TPayload, TRegister, TAccumulator> ListElement(Expression<Func<long, List<TPayload>, TRegister, bool>> condition, Expression<Func<List<TPayload>, TRegister, TRegister>> aggregator)
        {
            Expression<Func<long, List<TPayload>, TRegister, TRegister>> aggregatorTemplate = (ts, ev, r) => CallInliner.Call(aggregator, ev, r);
            return ListElement(condition, aggregatorTemplate.InlineCalls());
        }

        public IPattern<TKey, TPayload, TRegister, TAccumulator> ListElement(Expression<Func<long, List<TPayload>, TRegister, bool>> condition, Expression<Func<List<TPayload>, TRegister>> aggregator)
        {
            Expression<Func<long, List<TPayload>, TRegister, TRegister>> aggregatorTemplate = (ts, ev, r) => CallInliner.Call(aggregator, ev);
            return ListElement(condition, aggregatorTemplate.InlineCalls());
        }

        public IPattern<TKey, TPayload, TRegister, TAccumulator> ListElement(Expression<Func<List<TPayload>, TRegister, bool>> condition, Expression<Func<long, List<TPayload>, TRegister, TRegister>> aggregator = null)
        {
            Expression<Func<long, List<TPayload>, TRegister, bool>> conditionTemplate = (ts, ev, r) => CallInliner.Call(condition, ev, r);
            return ListElement(conditionTemplate.InlineCalls(), aggregator);
        }

        public IPattern<TKey, TPayload, TRegister, TAccumulator> ListElement(Expression<Func<List<TPayload>, TRegister, bool>> condition, Expression<Func<List<TPayload>, TRegister, TRegister>> aggregator)
        {
            Expression<Func<long, List<TPayload>, TRegister, bool>> conditionTemplate = (ts, ev, r) => CallInliner.Call(condition, ev, r);
            Expression<Func<long, List<TPayload>, TRegister, TRegister>> aggregatorTemplate = (ts, ev, r) => CallInliner.Call(aggregator, ev, r);
            return ListElement(conditionTemplate.InlineCalls(), aggregatorTemplate.InlineCalls());
        }

        public IPattern<TKey, TPayload, TRegister, TAccumulator> ListElement(Expression<Func<List<TPayload>, TRegister, bool>> condition, Expression<Func<List<TPayload>, TRegister>> aggregator)
        {
            Expression<Func<long, List<TPayload>, TRegister, bool>> conditionTemplate = (ts, ev, r) => CallInliner.Call(condition, ev, r);
            Expression<Func<long, List<TPayload>, TRegister, TRegister>> aggregatorTemplate = (ts, ev, r) => CallInliner.Call(aggregator, ev);
            return ListElement(conditionTemplate.InlineCalls(), aggregatorTemplate.InlineCalls());
        }

        public IPattern<TKey, TPayload, TRegister, TAccumulator> ListElement(Expression<Func<List<TPayload>, bool>> condition, Expression<Func<long, List<TPayload>, TRegister, TRegister>> aggregator = null)
        {
            Expression<Func<long, List<TPayload>, TRegister, bool>> conditionTemplate = (ts, ev, r) => CallInliner.Call(condition, ev);
            return ListElement(conditionTemplate.InlineCalls(), aggregator);
        }

        public IPattern<TKey, TPayload, TRegister, TAccumulator> ListElement(Expression<Func<List<TPayload>, bool>> condition, Expression<Func<List<TPayload>, TRegister, TRegister>> aggregator)
        {
            Expression<Func<long, List<TPayload>, TRegister, bool>> conditionTemplate = (ts, ev, r) => CallInliner.Call(condition, ev);
            Expression<Func<long, List<TPayload>, TRegister, TRegister>> aggregatorTemplate = (ts, ev, r) => CallInliner.Call(aggregator, ev, r);
            return ListElement(conditionTemplate.InlineCalls(), aggregatorTemplate.InlineCalls());
        }

        public IPattern<TKey, TPayload, TRegister, TAccumulator> ListElement(Expression<Func<List<TPayload>, bool>> condition, Expression<Func<List<TPayload>, TRegister>> aggregator)
        {
            Expression<Func<long, List<TPayload>, TRegister, bool>> conditionTemplate = (ts, ev, r) => CallInliner.Call(condition, ev);
            Expression<Func<long, List<TPayload>, TRegister, TRegister>> aggregatorTemplate = (ts, ev, r) => CallInliner.Call(aggregator, ev);
            return ListElement(conditionTemplate.InlineCalls(), aggregatorTemplate.InlineCalls());
        }
        #endregion

        #region EpsilonElement
        public IPattern<TKey, TPayload, TRegister, TAccumulator> Epsilon()
        {
            var afa = new Afa<TPayload, TRegister, TAccumulator>(this.defaultRegister, this.defaultAccumulator);
            afa.AddArc(0, 1, new EpsilonArc<TPayload, TRegister> { });
            afa.Seal();

            return Concat(x => new PatternMatcher<TKey, TPayload, TRegister, TAccumulator>(this.source, afa));
        }
        #endregion

        #region MultiElement
        public IPattern<TKey, TPayload, TRegister, TAccumulator> MultiElement(Expression<Func<long, TRegister, TAccumulator>> initialize, Expression<Func<long, TPayload, TRegister, TAccumulator, TAccumulator>> accumulate, Expression<Func<long, TPayload, TAccumulator, bool>> skipToEnd, Expression<Func<long, TAccumulator, TRegister, bool>> fence, Expression<Func<long, TAccumulator, TRegister, TRegister>> transfer, Expression<Action<TAccumulator>> dispose)
        {
            var afa = new Afa<TPayload, TRegister, TAccumulator>(this.defaultRegister, this.defaultAccumulator);
            afa.AddArc(0, 1, new MultiElementArc<TPayload, TRegister, TAccumulator>
            {
                Initialize = initialize,
                Accumulate = accumulate,
                SkipToEnd = skipToEnd,
                Fence = fence,
                Transfer = transfer,
                Dispose = dispose
            });
            afa.Seal();

            return Concat(x => new PatternMatcher<TKey, TPayload, TRegister, TAccumulator>(this.source, afa));
        }
        #endregion

        #region KleeneStar, KleenePlus, Concat, OrConcat, Or
        public IPattern<TKey, TPayload, TRegister, TAccumulator> KleeneStar(
            Func<IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator>, IPattern<TKey, TPayload, TRegister, TAccumulator>> pattern)
        {
            var newConcreteRegex = pattern(new PatternMatcher<TKey, TPayload, TRegister, TAccumulator>());

            var result = new Afa<TPayload, TRegister, TAccumulator>(this.defaultRegister, this.defaultAccumulator);

            var pattern_ = newConcreteRegex.AFA;

            // Every final state maps back to the start state
            foreach (var kvp1 in pattern_.transitionInfo)
            {
                foreach (var kvp2 in kvp1.Value)
                {
                    var to = kvp2.Key;
                    if (pattern_.finalStates.Contains(to))
                    {
                        to = pattern_.StartState;
                    }
                    result.AddArc(kvp1.Key, to, kvp2.Value);
                }
            }

            // Start state becomes the single final state
            result.finalStates.Add(pattern_.StartState);
            result.StartState = pattern_.StartState;

            return Concat(x => new PatternMatcher<TKey, TPayload, TRegister, TAccumulator>(this.source, result));
        }

        public IPattern<TKey, TPayload, TRegister, TAccumulator> KleenePlus(Func<IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator>, IPattern<TKey, TPayload, TRegister, TAccumulator>> pattern)
            => Concat(pattern, x => x.KleeneStar(pattern));

        public IPattern<TKey, TPayload, TRegister, TAccumulator> Concat(
            Func<IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator>, IPattern<TKey, TPayload, TRegister, TAccumulator>> pattern,
            params Func<IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator>, IPattern<TKey, TPayload, TRegister, TAccumulator>>[] patterns)
        {
            var pattern1 = this.AFA;
            var pattern2 = pattern(new PatternMatcher<TKey, TPayload, TRegister, TAccumulator>()).AFA;

            var afa = new Afa<TPayload, TRegister, TAccumulator>[patterns.Length];
            for (int i = 0; i < patterns.Length; i++)
                afa[i] = patterns[i](new PatternMatcher<TKey, TPayload, TRegister, TAccumulator>()).AFA;

            var result = ConcatWorker(false, pattern1, pattern2, afa);

            return new PatternMatcher<TKey, TPayload, TRegister, TAccumulator>(this.source, result);
        }

        public IPattern<TKey, TPayload, TRegister, TAccumulator> OrConcat(
            Func<IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator>, IPattern<TKey, TPayload, TRegister, TAccumulator>> pattern1,
            Func<IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator>, IPattern<TKey, TPayload, TRegister, TAccumulator>> pattern2,
            params Func<IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator>, IPattern<TKey, TPayload, TRegister, TAccumulator>>[] patterns)
        {
            var afa1 = pattern1(new PatternMatcher<TKey, TPayload, TRegister, TAccumulator>()).AFA;
            var afa2 = pattern2(new PatternMatcher<TKey, TPayload, TRegister, TAccumulator>()).AFA;

            var afa = new Afa<TPayload, TRegister, TAccumulator>[patterns.Length];
            for (int i = 0; i < patterns.Length; i++)
                afa[i] = patterns[i](new PatternMatcher<TKey, TPayload, TRegister, TAccumulator>()).AFA;

            var result = ConcatWorker(true, afa1, afa2, afa);

            return Concat(x => new PatternMatcher<TKey, TPayload, TRegister, TAccumulator>(this.source, result));
        }

        public IPattern<TKey, TPayload, TRegister, TAccumulator> Or(
            Func<IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator>, IPattern<TKey, TPayload, TRegister, TAccumulator>> pattern1,
            Func<IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator>, IPattern<TKey, TPayload, TRegister, TAccumulator>> pattern2,
            params Func<IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator>, IPattern<TKey, TPayload, TRegister, TAccumulator>>[] patterns)
        {
            var allPatterns = new Afa<TPayload, TRegister, TAccumulator>[patterns.Length + 2];
            allPatterns[0] = pattern1(new PatternMatcher<TKey, TPayload, TRegister, TAccumulator>()).AFA;
            allPatterns[1] = pattern2(new PatternMatcher<TKey, TPayload, TRegister, TAccumulator>()).AFA;

            for (int i = 0; i < patterns.Length; i++)
                allPatterns[i + 2] = patterns[i](new PatternMatcher<TKey, TPayload, TRegister, TAccumulator>()).AFA;

            var result = new Afa<TPayload, TRegister, TAccumulator>(this.defaultRegister, this.defaultAccumulator);

            int oldMax;

            for (int i = 0; i < allPatterns.Length; i++)
            {
                var nextPattern = allPatterns[i];

                oldMax = result.MaxState + 1;

                result.AddArc(0, nextPattern.StartState + oldMax, new EpsilonArc<TPayload, TRegister>());

                // If the next pattern start state is also a final state, add it directly, as it will not necessarily have a transition entry
                if (nextPattern.finalStates.Contains(nextPattern.StartState))
                {
                    if (!result.finalStates.Contains(nextPattern.StartState + oldMax))
                        result.finalStates.Add(nextPattern.StartState + oldMax);
                }

                foreach (var kvp1 in nextPattern.transitionInfo)
                {
                    foreach (var kvp2 in kvp1.Value)
                    {
                        int from = kvp1.Key + oldMax;
                        int to = kvp2.Key + oldMax;

                        result.AddArc(from, to, kvp2.Value);

                        if (nextPattern.finalStates.Contains(kvp2.Key))
                        {
                            if (!result.finalStates.Contains(to))
                                result.finalStates.Add(to);
                        }
                    }
                }
            }
            result.StartState = 0;

            return Concat(x => new PatternMatcher<TKey, TPayload, TRegister, TAccumulator>(this.source, result));
        }
        #endregion

        #region Set Register and Accumulator
        public IAbstractPattern<TKey, TPayload, TRegister, TAccumulatorNew> SetAccumulator<TAccumulatorNew>(TAccumulatorNew defaultAccumulator = default)
            => new PatternMatcher<TKey, TPayload, TRegister, TAccumulatorNew>(this.source, null, this.defaultRegister, defaultAccumulator);

        public IAbstractPattern<TKey, TPayload, TRegisterNew, TAccumulator> SetRegister<TRegisterNew>(TRegisterNew defaultRegister = default)
            => new PatternMatcher<TKey, TPayload, TRegisterNew, TAccumulator>(this.source, null, defaultRegister, this.defaultAccumulator);
        #endregion

        #region Detect
        public IStreamable<TKey, TRegister> Detect(long maxDuration = 0, bool allowOverlappingInstances = true, bool isDeterministic = false)
        {
            if (maxDuration == 0)
            {
                if (!(this.source.Properties.IsConstantDuration && (this.source.Properties.ConstantDurationLength != null)))
                {
                    throw new InvalidOperationException("Either specify a MaxDuration parameter or use an input stream that is windowed by a constant");
                }

                maxDuration = this.source.Properties.ConstantDurationLength.Value;
            }

            this.AFA.AllowOverlappingInstances = allowOverlappingInstances;
            this.AFA.IsDeterministic = isDeterministic;

            return new AfaStreamable<TKey, TPayload, TRegister, TAccumulator>(this.source, this.AFA, maxDuration);
        }

        public IStreamable<TKey, TRegister> Detect(Func<IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator>, IPattern<TKey, TPayload, TRegister, TAccumulator>> pattern, long maxDuration = 0, bool allowOverlappingInstances = true, bool isDeterministic = false)
        {
            var newConcreteRegex = pattern(this);
            return newConcreteRegex.Detect(maxDuration, allowOverlappingInstances, isDeterministic);
        }
        #endregion

        #region Edit
        public IPattern<TKey, TPayload, TRegister, TAccumulator> Edit(Action<Afa<TPayload, TRegister, TAccumulator>> edit)
        {
            var afa = this.AFA.Clone();
            edit(afa);
            return new PatternMatcher<TKey, TPayload, TRegister, TAccumulator>(this.source, afa);
        }
        #endregion

        #region Private Methods
        private static Afa<TPayload, TRegister, TAccumulator> ConcatWorker(bool isOr, Afa<TPayload, TRegister, TAccumulator> pattern1, Afa<TPayload, TRegister, TAccumulator> pattern2, params Afa<TPayload, TRegister, TAccumulator>[] patterns)
        {
            int offset = 1;
            if (pattern1 != null)
            {
                offset++;
            }
            var allPatterns = new Afa<TPayload, TRegister, TAccumulator>[patterns.Length + offset];

            int o = 0;
            if (pattern1 != null)
                allPatterns[o++] = pattern1;
            allPatterns[o++] = pattern2;
            patterns.CopyTo(allPatterns, offset);

            var extraFinalStates = new List<int>();

            var result = allPatterns[0].Clone();

            for (int i = 1; i < allPatterns.Length; i++)
            {
                var nextPattern = allPatterns[i];

                var newFinal = result.MaxState + 1;
                var origFinalStates = new List<int>();
                var epsilonArcAdded = false;
                foreach (var finalState in result.finalStates)
                {
                    if (result.transitionInfo.TryGetValue(finalState, out var outgoingEdges))
                    {
                        result.AddArc(finalState, newFinal, new EpsilonArc<TPayload, TRegister> { });
                        if (!epsilonArcAdded)
                        {
                            epsilonArcAdded = true;
                            origFinalStates.Add(newFinal);
                        }
                    }
                    else
                    {
                        origFinalStates.Add(finalState);
                    }
                }

                result.finalStates.Clear();

                foreach (var oldFinal in origFinalStates)
                {
                    extraFinalStates.Add(oldFinal);
                    int oldMax = result.MaxState;

                    // If the next pattern start state is also a final state, add it directly, as it will not necessarily have a transition entry
                    if (nextPattern.finalStates.Contains(nextPattern.StartState))
                    {
                        if (!result.finalStates.Contains(oldFinal))
                            result.finalStates.Add(oldFinal);
                    }

                    foreach (var kvp1 in nextPattern.transitionInfo)
                    {
                        foreach (var kvp2 in kvp1.Value)
                        {
                            int from = kvp1.Key;
                            int to = kvp2.Key;

                            from = from == nextPattern.StartState
                                ? oldFinal
                                : from + oldMax;

                            to = to == nextPattern.StartState
                                ? oldFinal
                                : to + oldMax;

                            result.AddArc(from, to, kvp2.Value);

                            if (nextPattern.finalStates.Contains(kvp2.Key))
                            {
                                if (!result.finalStates.Contains(to)) result.finalStates.Add(to);
                            }
                        }
                    }
                }
            }

            if (isOr)
            {
                // Consider individual prefixes of the concat as final states
                foreach (var state in extraFinalStates)
                {
                    if (!result.finalStates.Contains(state))
                        result.finalStates.Add(state);
                }
            }
            return result;
        }
        #endregion
    }
}
