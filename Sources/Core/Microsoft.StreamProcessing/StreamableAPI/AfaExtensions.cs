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
    /// Static class for transformations to Streamables
    /// </summary>
    public static partial class Streamable
    {
        #region Detect extensions for IStreamable

        /// <summary>
        /// Detect a pattern over the incoming stream. Takes augmented finite automaton (AFA) as input. Create AFA using the Regex.* API or direct AFA specification.
        /// </summary>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TPayload">Payload type</typeparam>
        /// <typeparam name="TRegister">Result type (output of matcher is the register at an accepting state of the AFA)</typeparam>
        /// <typeparam name="TAccumulator">Accumulator type</typeparam>
        /// <param name="source">Source stream</param>
        /// <param name="afa">AFA specification</param>
        /// <param name="maxDuration">Maximum duration (window) for the pattern</param>
        /// <param name="allowOverlappingInstances">States whether to allow more than one state machine instance to be in effect at a time</param>
        /// <param name="isDeterministic">States whether to consider the AFA as deterministic</param>
        /// <returns>A stream of the matched results</returns>
        public static IStreamable<TKey, TRegister> Detect<TKey, TPayload, TRegister, TAccumulator>(
            this IStreamable<TKey, TPayload> source,
            Afa<TPayload, TRegister, TAccumulator> afa, long maxDuration = 0, bool allowOverlappingInstances = true, bool isDeterministic = false)
        {
            Invariant.IsNotNull(source, nameof(source));

            if (maxDuration == 0)
            {
                if (!(source.Properties.IsConstantDuration && (source.Properties.ConstantDurationLength != null)))
                    throw new InvalidOperationException("Either specify a MaxDuration parameter or use an input stream that is windowed by a constant");

                maxDuration = source.Properties.ConstantDurationLength.Value;
            }

            afa.AllowOverlappingInstances = allowOverlappingInstances;
            afa.IsDeterministic = isDeterministic;

            return new AfaStreamable<TKey, TPayload, TRegister, TAccumulator>(source, afa, maxDuration);
        }

        /// <summary>
        /// Detect a pattern over the incoming stream. Takes augmented regular expression as input. Create AFA using the Regex.* API or direct AFA specification.
        /// </summary>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TPayload">Payload type</typeparam>
        /// <param name="source">Source stream</param>
        /// <param name="pattern">Regular expression pattern specification</param>
        /// <param name="maxDuration">Maximum duration (window) for the pattern</param>
        /// <param name="allowOverlappingInstances">States whether to allow more than one state machine instance to be in effect at a time</param>
        /// <param name="isDeterministic">States whether to consider the AFA as deterministic</param>
        /// <returns>A stream of the matched results</returns>
        public static IStreamable<TKey, Empty> Detect<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source,
            Func<IAbstractPatternRoot<TKey, TPayload, Empty, bool>, IPattern<TKey, TPayload, Empty, bool>> pattern,
            long maxDuration = 0, bool allowOverlappingInstances = true, bool isDeterministic = false)
        {
            Invariant.IsNotNull(source, nameof(source));

            if (maxDuration == 0)
            {
                if (!(source.Properties.IsConstantDuration && (source.Properties.ConstantDurationLength != null)))
                    throw new InvalidOperationException("Either specify a MaxDuration parameter or use an input stream that is windowed by a constant");

                maxDuration = source.Properties.ConstantDurationLength.Value;
            }

            var afa = pattern(new PatternMatcher<TKey, TPayload, Empty, bool>()).AFA;
            afa.AllowOverlappingInstances = allowOverlappingInstances;
            afa.IsDeterministic = isDeterministic;

            return new AfaStreamable<TKey, TPayload, Empty, bool>(source, afa, maxDuration);
        }

        /// <summary>
        /// Detect a pattern over the incoming stream. Takes augmented regular expression as input. Create AFA using the Regex.* API or direct AFA specification.
        /// </summary>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TPayload">Payload type</typeparam>
        /// <typeparam name="TRegister">Result type (output of matcher is the register at an accepting state of the AFA)</typeparam>
        /// <param name="source">Source stream</param>
        /// <param name="defaultRegister">Default register value for the automata</param>
        /// <param name="pattern">Regular expression pattern specification</param>
        /// <param name="maxDuration">Maximum duration (window) for the pattern</param>
        /// <param name="allowOverlappingInstances">States whether to allow more than one state machine instance to be in effect at a time</param>
        /// <param name="isDeterministic">States whether to consider the AFA as deterministic</param>
        /// <returns>A stream of the matched results</returns>
        public static IStreamable<TKey, TRegister> Detect<TKey, TPayload, TRegister>(
            this IStreamable<TKey, TPayload> source,
            TRegister defaultRegister,
            Func<IAbstractPatternRoot<TKey, TPayload, TRegister, bool>, IPattern<TKey, TPayload, TRegister, bool>> pattern,
            long maxDuration = 0, bool allowOverlappingInstances = true, bool isDeterministic = false)
        {
            Invariant.IsNotNull(source, nameof(source));

            if (maxDuration == 0)
            {
                if (!(source.Properties.IsConstantDuration && (source.Properties.ConstantDurationLength != null)))
                    throw new InvalidOperationException("Either specify a MaxDuration parameter or use an input stream that is windowed by a constant");

                maxDuration = source.Properties.ConstantDurationLength.Value;
            }

            var afa = pattern(new PatternMatcher<TKey, TPayload, TRegister, bool>()).AFA;
            afa.AllowOverlappingInstances = allowOverlappingInstances;
            afa.IsDeterministic = isDeterministic;

            return new AfaStreamable<TKey, TPayload, TRegister, bool>(source, afa, maxDuration);
        }

        /// <summary>
        /// Detect a pattern over the incoming stream. Takes augmented regular expression as input. Create AFA using the Regex.* API or direct AFA specification.
        /// </summary>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TPayload">Payload type</typeparam>
        /// <typeparam name="TRegister">Result type (output of matcher is the register at an accepting state of the AFA)</typeparam>
        /// <typeparam name="TAccumulator">Accumulator type</typeparam>
        /// <param name="source">Source stream</param>
        /// <param name="defaultRegister">Default register value for the automata</param>
        /// <param name="defaultAccumulator">Default accumulator value for the automata</param>
        /// <param name="pattern">Regular expression pattern specification</param>
        /// <param name="maxDuration">Maximum duration (window) for the pattern</param>
        /// <param name="allowOverlappingInstances">States whether to allow more than one state machine instance to be in effect at a time</param>
        /// <param name="isDeterministic">States whether to consider the AFA as deterministic</param>
        /// <returns>A stream of the matched results</returns>
        public static IStreamable<TKey, TRegister> Detect<TKey, TPayload, TRegister, TAccumulator>(
            this IStreamable<TKey, TPayload> source,
            TRegister defaultRegister,
            TAccumulator defaultAccumulator,
            Func<IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator>, IPattern<TKey, TPayload, TRegister, TAccumulator>> pattern,
            long maxDuration = 0, bool allowOverlappingInstances = true, bool isDeterministic = false)
        {
            Invariant.IsNotNull(source, nameof(source));

            if (maxDuration == 0)
            {
                if (!(source.Properties.IsConstantDuration && (source.Properties.ConstantDurationLength != null)))
                    throw new ArgumentException("Either specify a MaxDuration parameter or use an input stream that is windowed by a constant");

                maxDuration = source.Properties.ConstantDurationLength.Value;
            }
            var afa = pattern(new PatternMatcher<TKey, TPayload, TRegister, TAccumulator>()).AFA;
            afa.AllowOverlappingInstances = allowOverlappingInstances;
            afa.IsDeterministic = isDeterministic;

            return new AfaStreamable<TKey, TPayload, TRegister, TAccumulator>(source, afa, maxDuration);
        }
        #endregion

        #region CreateAFA extensions for IStreamable

        /// <summary>
        /// Define a pattern against which data in the input stream may be matched
        /// </summary>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TPayload">Payload type</typeparam>
        /// <param name="source">Input stream over which to define a pattern</param>
        /// <returns>The beginning of a builder from which a pattern may be defined</returns>
        public static IAbstractPattern<TKey, TPayload, Empty, bool> DefinePattern<TKey, TPayload>(this IStreamable<TKey, TPayload> source)
            => new PatternMatcher<TKey, TPayload, Empty, bool>(source);

        /// <summary>
        /// Define a pattern against which data in the input stream may be matched
        /// </summary>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TPayload">Payload type</typeparam>
        /// <typeparam name="TRegister">Result type (output of matcher is the register at an accepting state of the AFA)</typeparam>
        /// <param name="source">Source stream</param>
        /// <param name="defaultRegister">Default register value for the automata</param>
        /// <returns>The beginning of a builder from which a pattern may be defined</returns>
        public static IAbstractPattern<TKey, TPayload, TRegister, bool> DefinePattern<TKey, TPayload, TRegister>(
            this IStreamable<TKey, TPayload> source, TRegister defaultRegister)
            => new PatternMatcher<TKey, TPayload, TRegister, bool>(source, null, defaultRegister);

        /// <summary>
        /// Define a pattern against which data in the input stream may be matched
        /// </summary>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TPayload">Payload type</typeparam>
        /// <typeparam name="TRegister">Result type (output of matcher is the register at an accepting state of the AFA)</typeparam>
        /// <typeparam name="TAccumulator">Accumulator type</typeparam>
        /// <param name="source">Source stream</param>
        /// <param name="defaultRegister">Default register value for the automata</param>
        /// <param name="defaultAccumulator">Default accumulator value for the automata</param>
        /// <returns>The beginning of a builder from which a pattern may be defined</returns>
        public static IAbstractPattern<TKey, TPayload, TRegister, TAccumulator> DefinePattern<TKey, TPayload, TRegister, TAccumulator>(
            this IStreamable<TKey, TPayload> source, TRegister defaultRegister, TAccumulator defaultAccumulator)
            => new PatternMatcher<TKey, TPayload, TRegister, TAccumulator>(source, null, defaultRegister);
        #endregion

        #region Macro extensions for pattern matching

        /// <summary>
        /// Add to the current pattern a new pattern symbol that matches any element
        /// </summary>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TPayload">Payload type</typeparam>
        /// <typeparam name="TRegister">Result type (output of matcher is the register at an accepting state of the AFA)</typeparam>
        /// <param name="source">Input pattern</param>
        /// <param name="fence">Condition that must be true for the transition to occur</param>
        /// <returns>The newly constructed pattern</returns>
        public static IPattern<TKey, TPayload, TRegister, bool> AnyElement<TKey, TPayload, TRegister>(
            this IAbstractPatternRoot<TKey, TPayload, TRegister, bool> source,
            Expression<Func<long, TPayload, TRegister, bool>> fence)
        {
            Expression<Func<long, TPayload, TRegister, bool, bool>> fenceTemplate = (ts, ev, reg, acc) => CallInliner.Call(fence, ts, ev, reg);

            return source.MultiElement((ts, reg) => false, fenceTemplate.InlineCalls(), (ts, ev, acc) => acc, (ts, acc, reg) => acc, null, null);
        }

        /// <summary>
        /// Add to the current pattern a new pattern symbol that matches any element
        /// </summary>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TPayload">Payload type</typeparam>
        /// <typeparam name="TRegister">Result type (output of matcher is the register at an accepting state of the AFA)</typeparam>
        /// <typeparam name="TAccumulator">Accumulator type</typeparam>
        /// <param name="source">Input pattern</param>
        /// <param name="accumulatorInitialization">Initializer function for the accumulator</param>
        /// <param name="accumulatorBoolField"></param>
        /// <param name="fence">Condition that must be true for the transition to occur</param>
        /// <returns>The newly constructed pattern</returns>
        public static IPattern<TKey, TPayload, TRegister, TAccumulator> AnyElement<TKey, TPayload, TRegister, TAccumulator>(
            this IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator> source,
            Expression<Func<TAccumulator>> accumulatorInitialization,
            Expression<Func<TAccumulator, bool>> accumulatorBoolField,
            Expression<Func<long, TPayload, TRegister, bool>> fence)
            => CreateMultiElementFunctions(source, accumulatorInitialization, accumulatorBoolField, fence, false, b => b);

        /// <summary>
        /// Add to the current pattern a new pattern symbol that matches all elements
        /// </summary>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TPayload">Payload type</typeparam>
        /// <typeparam name="TRegister">Result type (output of matcher is the register at an accepting state of the AFA)</typeparam>
        /// <param name="source">Input pattern</param>
        /// <param name="fence">Condition that must be true for the transition to occur</param>
        /// <returns>The newly constructed pattern</returns>
        public static IPattern<TKey, TPayload, TRegister, bool> AllElement<TKey, TPayload, TRegister>(
            this IAbstractPatternRoot<TKey, TPayload, TRegister, bool> source,
            Expression<Func<long, TPayload, TRegister, bool>> fence)
        {
            Expression<Func<long, TPayload, TRegister, bool, bool>> fenceTemplate = (ts, ev, reg, acc) => CallInliner.Call(fence, ts, ev, reg);

            return
                source.MultiElement((ts, reg) => true, fenceTemplate.InlineCalls(),
                (ts, ev, acc) => !acc,
                (ts, acc, reg) => acc, null, null);
        }

        /// <summary>
        /// Add to the current pattern a new pattern symbol that matches all elements
        /// </summary>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TPayload">Payload type</typeparam>
        /// <typeparam name="TRegister">Result type (output of matcher is the register at an accepting state of the AFA)</typeparam>
        /// <typeparam name="TAccumulator">Accumulator type</typeparam>
        /// <param name="source">Input pattern</param>
        /// <param name="accumulatorInitialization">Initializer function for the accumulator</param>
        /// <param name="accumulatorBoolField"></param>
        /// <param name="fence">Condition that must be true for the transition to occur</param>
        /// <returns>The newly constructed pattern</returns>
        public static IPattern<TKey, TPayload, TRegister, TAccumulator> AllElement<TKey, TPayload, TRegister, TAccumulator>(
            this IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator> source,
            Expression<Func<TAccumulator>> accumulatorInitialization,
            Expression<Func<TAccumulator, bool>> accumulatorBoolField,
            Expression<Func<long, TPayload, TRegister, bool>> fence)
            => CreateMultiElementFunctions(source, accumulatorInitialization, accumulatorBoolField, fence, true, b => !b);

        private static IPattern<TKey, TPayload, TRegister, TAccumulator> CreateMultiElementFunctions<TKey, TPayload, TRegister, TAccumulator>(IAbstractPatternRoot<TKey, TPayload, TRegister, TAccumulator> source, Expression<Func<TAccumulator>> accumulatorInitialization, Expression<Func<TAccumulator, bool>> accumulatorboolField, Expression<Func<long, TPayload, TRegister, bool>> fence, bool initialValueForBooleanField, Expression<Func<bool, bool>> shortCircuitCondition)
        {
            if (!(accumulatorboolField.Body is MemberExpression memberExpression)) throw new InvalidOperationException("accumulatorboolField must be a lambda that picks out one field from its parameter");
            if (!(memberExpression.Expression is ParameterExpression parameter)) throw new InvalidOperationException("accumulatorboolField must be a lambda that picks out one field from its parameter");
            var memberInfo = memberExpression.Member;
            if (memberInfo is System.Reflection.PropertyInfo propertyInfo && !propertyInfo.CanWrite) throw new InvalidOperationException("accumulatorboolField is specifying a property, " + propertyInfo.Name + "' that does not have a setter");

            // "f" is the field specified by the accumulatorboolField lambda.
            // Create the Initialize function as (ts, reg) => { var acc = accumulatorInitialization(); acc.f = initialValueForBooleanField; return acc; }
            Expression<Func<long, TRegister, TAccumulator>> initializeTemplate = (ts, reg) => CallInliner.Call(accumulatorInitialization);
            var userInitializeFunction = initializeTemplate.InlineCalls();
            var accumulatorLocalForInitialize = Expression.Parameter(typeof(TAccumulator), "acc");
            var initializeBody = new List<Expression>()
            {
                Expression.Assign(accumulatorLocalForInitialize, userInitializeFunction.Body),
                Expression.Assign(Expression.MakeMemberAccess(accumulatorLocalForInitialize, memberInfo), Expression.Constant(initialValueForBooleanField, typeof(bool))),
                accumulatorLocalForInitialize
            };
            var initializeFunction = (Expression<Func<long, TRegister, TAccumulator>>)Expression.Lambda(
                Expression.Block(new[] { accumulatorLocalForInitialize }, initializeBody),
                Expression.Parameter(typeof(long), "ts"), Expression.Parameter(typeof(TRegister), "reg"));

            // Create the Accumulate function as (ts, ev, reg, acc) => { acc.f = fence(ts, ev, reg); return acc; }
            Expression<Func<long, TPayload, TRegister, TAccumulator, bool>> userAccumulatorFunctionTemplate = (ts, ev, reg, acc) => CallInliner.Call(fence, ts, ev, reg);
            var userAccumulatorFunction = userAccumulatorFunctionTemplate.InlineCalls();
            var accParmeter = userAccumulatorFunction.Parameters[3];
            var accumulateBody = new List<Expression>() { Expression.Assign(Expression.MakeMemberAccess(accParmeter, memberInfo), userAccumulatorFunction.Body), accParmeter, };
            var parameters = new ParameterExpression[]
            {
                userAccumulatorFunction.Parameters[0],
                userAccumulatorFunction.Parameters[1],
                userAccumulatorFunction.Parameters[2],
                accParmeter,
            };
            var accumulateFunction = (Expression<Func<long, TPayload, TRegister, TAccumulator, TAccumulator>>)Expression.Lambda(Expression.Block(accumulateBody), parameters);

            // Create the SkipToEnd function as (ts, ev, acc) => acc.f
            Expression<Func<long, TPayload, TAccumulator, bool>> skipToEndTemplate = (ts, ev, acc) => CallInliner.Call(shortCircuitCondition, CallInliner.Call(accumulatorboolField, acc));
            var skipToEndFunction = skipToEndTemplate.InlineCalls();

            // Create the fence function as (ts, acc, reg) => acc.f
            Expression<Func<long, TAccumulator, TRegister, bool>> fenceTemplate = (ts, acc, reg) => CallInliner.Call(accumulatorboolField, acc);
            var fenceFunction = fenceTemplate.InlineCalls();

            return source.MultiElement(initializeFunction, accumulateFunction, skipToEndFunction, fenceFunction, null, null);
        }

        #endregion
    }
}
