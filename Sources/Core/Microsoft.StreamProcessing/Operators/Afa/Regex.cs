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
    /// Builder class for creating patterns against which to match stream data using a regular expression interface
    /// </summary>
    public static class ARegex
    {
        #region SingleElement

        /// <summary>
        /// Creates a new pattern without a register and adds a single-element transition that succeeds on every stream element seen
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <returns>A pattern whose first transition is the one just created</returns>
        public static Afa<TInput, Empty, bool> SingleElement<TInput>()
        {
            var afa = Afa.Create<TInput>();
            afa.AddArc(0, 1, new SingleElementArc<TInput, Empty> { Fence = (ts, ev, r) => true });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern without a register and adds a single-element transition that succeeds on stream elements that match a condition
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <param name="condition">The condition that must be met to consider the transition satisfied</param>
        /// <returns>A pattern whose first transition is the one just created</returns>
        public static Afa<TInput, Empty, bool> SingleElement<TInput>(Expression<Func<TInput, bool>> condition)
        {
            var afa = Afa.Create<TInput>();
            Expression<Func<long, TInput, Empty, bool>> template = (ts, ev, r) => CallInliner.Call(condition, ev);
            afa.AddArc(0, 1, new SingleElementArc<TInput, Empty> { Fence = template.InlineCalls() });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern with a register and adds a single-element transition that succeeds on every stream element seen
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <returns>A pattern with an updatable register whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, bool> SingleElement<TInput, TRegister>()
        {
            var afa = Afa.Create<TInput, TRegister>();
            afa.AddArc(0, 1, new SingleElementArc<TInput, TRegister> { Fence = (ts, ev, r) => true });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern with a register and adds a single-element transition that succeeds on stream elements that match a condition
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <param name="condition">The condition that must be met to consider the transition satisfied</param>
        /// <returns>A pattern with an updatable register whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, bool> SingleElement<TInput, TRegister>(Expression<Func<TInput, bool>> condition)
        {
            var afa = Afa.Create<TInput, TRegister>();
            Expression<Func<long, TInput, TRegister, bool>> template = (ts, ev, r) => CallInliner.Call(condition, ev);
            afa.AddArc(0, 1, new SingleElementArc<TInput, TRegister> { Fence = template.InlineCalls() });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern with a register and adds a single-element transition that succeeds on every stream element seen
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <param name="condition">The condition that must be met to consider the transition satisfied</param>
        /// <param name="aggregator">An aggregator mutator that sets the initial value of the register</param>
        /// <returns>A pattern with an updatable register whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, bool> SingleElement<TInput, TRegister>(Expression<Func<TInput, bool>> condition, Expression<Func<TInput, TRegister>> aggregator)
        {
            var afa = Afa.Create<TInput, TRegister>();
            Expression<Func<long, TInput, TRegister, bool>> conditionTemplate = (ts, ev, r) => CallInliner.Call(condition, ev);
            Expression<Func<long, TInput, TRegister, TRegister>> aggregatorTemplate = (ts, ev, r) => CallInliner.Call(aggregator, ev);
            afa.AddArc(0, 1, new SingleElementArc<TInput, TRegister> { Fence = conditionTemplate.InlineCalls(), Transfer = aggregatorTemplate.InlineCalls(), });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern with a register and adds a single-element transition that succeeds on stream elements that match a condition
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <param name="condition">The condition that must be met to consider the transition satisfied</param>
        /// <param name="aggregator">An aggregator mutator that updates the value of the register</param>
        /// <returns>A pattern with an updatable register whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, bool> SingleElement<TInput, TRegister>(Expression<Func<TInput, bool>> condition, Expression<Func<TInput, TRegister, TRegister>> aggregator)
        {
            var afa = Afa.Create<TInput, TRegister>();
            Expression<Func<long, TInput, TRegister, bool>> conditionTemplate = (ts, ev, r) => CallInliner.Call(condition, ev);
            Expression<Func<long, TInput, TRegister, TRegister>> aggregatorTemplate = (ts, ev, r) => CallInliner.Call(aggregator, ev, r);
            afa.AddArc(0, 1, new SingleElementArc<TInput, TRegister> { Fence = conditionTemplate.InlineCalls(), Transfer = aggregatorTemplate.InlineCalls(), });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern with a register and adds a single-element transition that succeeds on stream elements that match a condition
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <param name="condition">The condition that must be met to consider the transition satisfied</param>
        /// <param name="aggregator">A time-sensitive aggregator mutator that updates the value of the register</param>
        /// <returns>A pattern with an updatable register whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, bool> SingleElement<TInput, TRegister>(Expression<Func<TInput, bool>> condition, Expression<Func<long, TInput, TRegister, TRegister>> aggregator)
        {
            var afa = Afa.Create<TInput, TRegister>();
            Expression<Func<long, TInput, TRegister, bool>> conditionTemplate = (ts, ev, r) => CallInliner.Call(condition, ev);
            afa.AddArc(0, 1, new SingleElementArc<TInput, TRegister> { Fence = conditionTemplate.InlineCalls(), Transfer = aggregator, });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern with a register and adds a single-element transition that succeeds on stream elements that match a condition
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <param name="condition">The register-value sensitive condition that must be met to consider the transition satisfied</param>
        /// <returns>A pattern with an updatable register whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, bool> SingleElement<TInput, TRegister>(Expression<Func<TInput, TRegister, bool>> condition)
        {
            var afa = Afa.Create<TInput, TRegister>();
            Expression<Func<long, TInput, TRegister, bool>> conditionTemplate = (ts, ev, r) => CallInliner.Call(condition, ev, r);
            afa.AddArc(0, 1, new SingleElementArc<TInput, TRegister> { Fence = conditionTemplate.InlineCalls(), });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern with a register and adds a single-element transition that succeeds on stream elements that match a condition
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <param name="condition">The register-value sensitive condition that must be met to consider the transition satisfied</param>
        /// <param name="aggregator">An aggregator mutator initializing value of the register</param>
        /// <returns>A pattern with an updatable register whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, bool> SingleElement<TInput, TRegister>(Expression<Func<TInput, TRegister, bool>> condition, Expression<Func<TInput, TRegister>> aggregator)
        {
            var afa = Afa.Create<TInput, TRegister>();
            Expression<Func<long, TInput, TRegister, bool>> conditionTemplate = (ts, ev, r) => CallInliner.Call(condition, ev, r);
            Expression<Func<long, TInput, TRegister, TRegister>> aggregatorTemplate = (ts, ev, r) => CallInliner.Call(aggregator, ev);
            afa.AddArc(0, 1, new SingleElementArc<TInput, TRegister> { Fence = conditionTemplate.InlineCalls(), Transfer = aggregatorTemplate.InlineCalls(), });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern with a register and adds a single-element transition that succeeds on stream elements that match a condition
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <param name="condition">The register-value sensitive condition that must be met to consider the transition satisfied</param>
        /// <param name="aggregator">An aggregator mutator that updates the value of the register</param>
        /// <returns>A pattern with an updatable register whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, bool> SingleElement<TInput, TRegister>(Expression<Func<TInput, TRegister, bool>> condition, Expression<Func<TInput, TRegister, TRegister>> aggregator)
        {
            var afa = Afa.Create<TInput, TRegister>();
            Expression<Func<long, TInput, TRegister, bool>> conditionTemplate = (ts, ev, r) => CallInliner.Call(condition, ev, r);
            Expression<Func<long, TInput, TRegister, TRegister>> aggregatorTemplate = (ts, ev, r) => CallInliner.Call(aggregator, ev, r);
            afa.AddArc(0, 1, new SingleElementArc<TInput, TRegister> { Fence = conditionTemplate.InlineCalls(), Transfer = aggregatorTemplate.InlineCalls(), });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern with a register and adds a single-element transition that succeeds on stream elements that match a condition
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <param name="condition">The register-value sensitive condition that must be met to consider the transition satisfied</param>
        /// <param name="aggregator">A time-sensitive aggregator mutator that updates the value of the register</param>
        /// <returns>A pattern with an updatable register whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, bool> SingleElement<TInput, TRegister>(Expression<Func<TInput, TRegister, bool>> condition, Expression<Func<long, TInput, TRegister, TRegister>> aggregator)
        {
            var afa = Afa.Create<TInput, TRegister>();
            Expression<Func<long, TInput, TRegister, bool>> conditionTemplate = (ts, ev, r) => CallInliner.Call(condition, ev, r);
            afa.AddArc(0, 1, new SingleElementArc<TInput, TRegister> { Fence = conditionTemplate.InlineCalls(), Transfer = aggregator, });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern with a register and adds a single-element transition that succeeds on stream elements that match a condition
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <param name="condition">The time-sensitive, register-value sensitive condition that must be met to consider the transition satisfied</param>
        /// <returns>A pattern with an updatable register whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, bool> SingleElement<TInput, TRegister>(Expression<Func<long, TInput, TRegister, bool>> condition)
        {
            var afa = Afa.Create<TInput, TRegister>();
            afa.AddArc(0, 1, new SingleElementArc<TInput, TRegister> { Fence = condition });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern with a register and adds a single-element transition that succeeds on stream elements that match a condition
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <param name="condition">The time-sensitive, register-value sensitive condition that must be met to consider the transition satisfied</param>
        /// <param name="aggregator">An aggregator mutator initializing value of the register</param>
        /// <returns>A pattern with an updatable register whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, bool> SingleElement<TInput, TRegister>(Expression<Func<long, TInput, TRegister, bool>> condition, Expression<Func<TInput, TRegister>> aggregator)
        {
            var afa = Afa.Create<TInput, TRegister>();
            Expression<Func<long, TInput, TRegister, TRegister>> aggregatorTemplate = (ts, ev, reg) => CallInliner.Call(aggregator, ev);
            afa.AddArc(0, 1, new SingleElementArc<TInput, TRegister> { Fence = condition, Transfer = aggregatorTemplate.InlineCalls(), });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern with a register and adds a single-element transition that succeeds on stream elements that match a condition
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <param name="condition">The time-sensitive, register-value sensitive condition that must be met to consider the transition satisfied</param>
        /// <param name="aggregator">An aggregator mutator that updates the value of the register</param>
        /// <returns>A pattern with an updatable register whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, bool> SingleElement<TInput, TRegister>(Expression<Func<long, TInput, TRegister, bool>> condition, Expression<Func<TInput, TRegister, TRegister>> aggregator)
        {
            var afa = Afa.Create<TInput, TRegister>();
            Expression<Func<long, TInput, TRegister, TRegister>> aggregatorTemplate = (ts, ev, reg) => CallInliner.Call(aggregator, ev, reg);
            afa.AddArc(0, 1, new SingleElementArc<TInput, TRegister> { Fence = condition, Transfer = aggregatorTemplate.InlineCalls(), });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern with a register and adds a single-element transition that succeeds on stream elements that match a condition
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <param name="condition">The time-sensitive, register-value sensitive condition that must be met to consider the transition satisfied</param>
        /// <param name="aggregator">A time-sensitive aggregator mutator that updates the value of the register</param>
        /// <returns>A pattern with an updatable register whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, bool> SingleElement<TInput, TRegister>(Expression<Func<long, TInput, TRegister, bool>> condition, Expression<Func<long, TInput, TRegister, TRegister>> aggregator)
        {
            var afa = Afa.Create<TInput, TRegister>();
            afa.AddArc(0, 1, new SingleElementArc<TInput, TRegister> { Fence = condition, Transfer = aggregator });
            afa.Seal();
            return afa;
        }
        #endregion

        #region ListElement

        /// <summary>
        /// Creates a new pattern without a register and adds a time-synchronous list transition that succeeds on every list of events seen
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <returns>A pattern whose first transition is the one just created</returns>
        public static Afa<TInput, Empty, bool> ListElement<TInput>()
        {
            var afa = Afa.Create<TInput>();
            afa.AddArc(0, 1, new ListElementArc<TInput, Empty> { Fence = (ts, ev, r) => true });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern without a register and adds a time-synchronous list transition that succeeds on event lists that match a condition
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <param name="condition">The condition that must be met to consider the transition satisfied</param>
        /// <returns>A pattern whose first transition is the one just created</returns>
        public static Afa<TInput, Empty, bool> ListElement<TInput>(Expression<Func<List<TInput>, bool>> condition)
        {
            var afa = Afa.Create<TInput>();
            Expression<Func<long, List<TInput>, Empty, bool>> template = (ts, ev, r) => CallInliner.Call(condition, ev);
            afa.AddArc(0, 1, new ListElementArc<TInput, Empty> { Fence = template.InlineCalls() });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern with a register and adds a time-synchronous list transition that succeeds on every list of events seen
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <returns>A pattern with an updatable register whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, bool> ListElement<TInput, TRegister>()
        {
            var afa = Afa.Create<TInput, TRegister>();
            afa.AddArc(0, 1, new ListElementArc<TInput, TRegister> { Fence = (ts, ev, r) => true });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern with a register and adds a time-synchronous list transition that succeeds on event lists that match a condition
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <param name="condition">The condition that must be met to consider the transition satisfied</param>
        /// <returns>A pattern with an updatable register whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, bool> ListElement<TInput, TRegister>(Expression<Func<List<TInput>, bool>> condition)
        {
            var afa = Afa.Create<TInput, TRegister>();
            Expression<Func<long, List<TInput>, TRegister, bool>> template = (ts, ev, r) => CallInliner.Call(condition, ev);
            afa.AddArc(0, 1, new ListElementArc<TInput, TRegister> { Fence = template.InlineCalls() });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern with a register and adds a time-synchronous list transition that succeeds on event lists that match a condition
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <param name="condition">The condition that must be met to consider the transition satisfied</param>
        /// <param name="aggregator">An aggregator mutator that sets the initial value of the register</param>
        /// <returns>A pattern with an updatable register whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, bool> ListElement<TInput, TRegister>(Expression<Func<List<TInput>, bool>> condition, Expression<Func<List<TInput>, TRegister>> aggregator)
        {
            var afa = Afa.Create<TInput, TRegister>();
            Expression<Func<long, List<TInput>, TRegister, bool>> conditionTemplate = (ts, ev, r) => CallInliner.Call(condition, ev);
            Expression<Func<long, List<TInput>, TRegister, TRegister>> aggregatorTemplate = (ts, ev, r) => CallInliner.Call(aggregator, ev);
            afa.AddArc(0, 1, new ListElementArc<TInput, TRegister> { Fence = conditionTemplate.InlineCalls(), Transfer = aggregatorTemplate.InlineCalls(), });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern with a register and adds a time-synchronous list transition that succeeds on event lists that match a condition
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <param name="condition">The condition that must be met to consider the transition satisfied</param>
        /// <param name="aggregator">An aggregator mutator that updates the value of the register</param>
        /// <returns>A pattern with an updatable register whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, bool> ListElement<TInput, TRegister>(Expression<Func<List<TInput>, bool>> condition, Expression<Func<List<TInput>, TRegister, TRegister>> aggregator)
        {
            var afa = Afa.Create<TInput, TRegister>();
            Expression<Func<long, List<TInput>, TRegister, bool>> conditionTemplate = (ts, ev, r) => CallInliner.Call(condition, ev);
            Expression<Func<long, List<TInput>, TRegister, TRegister>> aggregatorTemplate = (ts, ev, r) => CallInliner.Call(aggregator, ev, r);
            afa.AddArc(0, 1, new ListElementArc<TInput, TRegister> { Fence = conditionTemplate.InlineCalls(), Transfer = aggregatorTemplate.InlineCalls(), });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern with a register and adds a time-synchronous list transition that succeeds on event lists that match a condition
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <param name="condition">The condition that must be met to consider the transition satisfied</param>
        /// <param name="aggregator">A time-sensitive aggregator mutator that updates the value of the register</param>
        /// <returns>A pattern with an updatable register whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, bool> ListElement<TInput, TRegister>(Expression<Func<List<TInput>, bool>> condition, Expression<Func<long, List<TInput>, TRegister, TRegister>> aggregator)
        {
            var afa = Afa.Create<TInput, TRegister>();
            Expression<Func<long, List<TInput>, TRegister, bool>> conditionTemplate = (ts, ev, r) => CallInliner.Call(condition, ev);
            afa.AddArc(0, 1, new ListElementArc<TInput, TRegister> { Fence = conditionTemplate.InlineCalls(), Transfer = aggregator, });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern with a register and adds a time-synchronous list transition that succeeds on event lists that match a condition
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <param name="condition">The register-value sensitive condition that must be met to consider the transition satisfied</param>
        /// <returns>A pattern with an updatable register whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, bool> ListElement<TInput, TRegister>(Expression<Func<List<TInput>, TRegister, bool>> condition)
        {
            var afa = Afa.Create<TInput, TRegister>();
            Expression<Func<long, List<TInput>, TRegister, bool>> conditionTemplate = (ts, ev, r) => CallInliner.Call(condition, ev, r);
            afa.AddArc(0, 1, new ListElementArc<TInput, TRegister> { Fence = conditionTemplate.InlineCalls(), });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern with a register and adds a time-synchronous list transition that succeeds on event lists that match a condition
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <param name="condition">The register-value sensitive condition that must be met to consider the transition satisfied</param>
        /// <param name="aggregator">An aggregator mutator initializing value of the register</param>
        /// <returns>A pattern with an updatable register whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, bool> ListElement<TInput, TRegister>(Expression<Func<List<TInput>, TRegister, bool>> condition, Expression<Func<List<TInput>, TRegister>> aggregator)
        {
            var afa = Afa.Create<TInput, TRegister>();
            Expression<Func<long, List<TInput>, TRegister, bool>> conditionTemplate = (ts, ev, r) => CallInliner.Call(condition, ev, r);
            Expression<Func<long, List<TInput>, TRegister, TRegister>> aggregatorTemplate = (ts, ev, r) => CallInliner.Call(aggregator, ev);
            afa.AddArc(0, 1, new ListElementArc<TInput, TRegister> { Fence = conditionTemplate.InlineCalls(), Transfer = aggregatorTemplate.InlineCalls(), });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern with a register and adds a time-synchronous list transition that succeeds on event lists that match a condition
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <param name="condition">The register-value sensitive condition that must be met to consider the transition satisfied</param>
        /// <param name="aggregator">An aggregator mutator that updates the value of the register</param>
        /// <returns>A pattern with an updatable register whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, bool> ListElement<TInput, TRegister>(Expression<Func<List<TInput>, TRegister, bool>> condition, Expression<Func<List<TInput>, TRegister, TRegister>> aggregator)
        {
            var afa = Afa.Create<TInput, TRegister>();
            Expression<Func<long, List<TInput>, TRegister, bool>> conditionTemplate = (ts, ev, r) => CallInliner.Call(condition, ev, r);
            Expression<Func<long, List<TInput>, TRegister, TRegister>> aggregatorTemplate = (ts, ev, r) => CallInliner.Call(aggregator, ev, r);
            afa.AddArc(0, 1, new ListElementArc<TInput, TRegister> { Fence = conditionTemplate.InlineCalls(), Transfer = aggregatorTemplate.InlineCalls(), });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern with a register and adds a time-synchronous list transition that succeeds on event lists that match a condition
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <param name="condition">The register-value sensitive condition that must be met to consider the transition satisfied</param>
        /// <param name="aggregator">A time-sensitive aggregator mutator that updates the value of the register</param>
        /// <returns>A pattern with an updatable register whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, bool> ListElement<TInput, TRegister>(Expression<Func<List<TInput>, TRegister, bool>> condition, Expression<Func<long, List<TInput>, TRegister, TRegister>> aggregator)
        {
            var afa = Afa.Create<TInput, TRegister>();
            Expression<Func<long, List<TInput>, TRegister, bool>> conditionTemplate = (ts, ev, r) => CallInliner.Call(condition, ev, r);
            afa.AddArc(0, 1, new ListElementArc<TInput, TRegister> { Fence = conditionTemplate.InlineCalls(), Transfer = aggregator, });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern with a register and adds a time-synchronous list transition that succeeds on event lists that match a condition
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <param name="condition">The time-sensitive, register-value sensitive condition that must be met to consider the transition satisfied</param>
        /// <returns>A pattern with an updatable register whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, bool> ListElement<TInput, TRegister>(Expression<Func<long, List<TInput>, TRegister, bool>> condition)
        {
            var afa = Afa.Create<TInput, TRegister>();
            afa.AddArc(0, 1, new ListElementArc<TInput, TRegister> { Fence = condition });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern with a register and adds a time-synchronous list transition that succeeds on event lists that match a condition
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <param name="condition">The time-sensitive, register-value sensitive condition that must be met to consider the transition satisfied</param>
        /// <param name="aggregator">An aggregator mutator initializing value of the register</param>
        /// <returns>A pattern with an updatable register whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, bool> ListElement<TInput, TRegister>(Expression<Func<long, List<TInput>, TRegister, bool>> condition, Expression<Func<List<TInput>, TRegister>> aggregator)
        {
            var afa = Afa.Create<TInput, TRegister>();
            Expression<Func<long, List<TInput>, TRegister, TRegister>> aggregatorTemplate = (ts, ev, reg) => CallInliner.Call(aggregator, ev);
            afa.AddArc(0, 1, new ListElementArc<TInput, TRegister> { Fence = condition, Transfer = aggregatorTemplate.InlineCalls(), });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern with a register and adds a time-synchronous list transition that succeeds on event lists that match a condition
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <param name="condition">The time-sensitive, register-value sensitive condition that must be met to consider the transition satisfied</param>
        /// <param name="aggregator">An aggregator mutator that updates the value of the register</param>
        /// <returns>A pattern with an updatable register whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, bool> ListElement<TInput, TRegister>(Expression<Func<long, List<TInput>, TRegister, bool>> condition, Expression<Func<List<TInput>, TRegister, TRegister>> aggregator)
        {
            var afa = Afa.Create<TInput, TRegister>();
            Expression<Func<long, List<TInput>, TRegister, TRegister>> aggregatorTemplate = (ts, ev, reg) => CallInliner.Call(aggregator, ev, reg);
            afa.AddArc(0, 1, new ListElementArc<TInput, TRegister> { Fence = condition, Transfer = aggregatorTemplate.InlineCalls(), });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern with a register and adds a time-synchronous list transition that succeeds on event lists that match a condition
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <param name="condition">The time-sensitive, register-value sensitive condition that must be met to consider the transition satisfied</param>
        /// <param name="aggregator">A time-sensitive aggregator mutator that updates the value of the register</param>
        /// <returns>A pattern with an updatable register whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, bool> ListElement<TInput, TRegister>(Expression<Func<long, List<TInput>, TRegister, bool>> condition, Expression<Func<long, List<TInput>, TRegister, TRegister>> aggregator)
        {
            var afa = Afa.Create<TInput, TRegister>();
            afa.AddArc(0, 1, new ListElementArc<TInput, TRegister> { Fence = condition, Transfer = aggregator });
            afa.Seal();
            return afa;
        }
        #endregion

        #region Epsilon

        /// <summary>
        /// Creates a new pattern without a register and adds an epsilon transition
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <returns>A pattern whose first transition is the one just created</returns>
        public static Afa<TInput, Empty, bool> Epsilon<TInput>()
        {
            var afa = Afa.Create<TInput>();
            afa.AddArc(0, 1, new EpsilonArc<TInput, Empty> { });
            afa.Seal();
            return afa;
        }

        /// <summary>
        /// Creates a new pattern with a register and adds an epsilon transition
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <returns>A pattern whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, bool> Epsilon<TInput, TRegister>()
        {
            var afa = Afa.Create<TInput, TRegister>();
            afa.AddArc(0, 1, new EpsilonArc<TInput, TRegister> { });
            afa.Seal();
            return afa;
        }
        #endregion

        #region Concat, OrConcat, Or

        /// <summary>
        /// Creates a new pattern resulting from the concatenation of other patterns
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <typeparam name="TAccumulator">The type of the accumulator in the underlying automaton</typeparam>
        /// <param name="pattern1">The first pattern in the concatenation</param>
        /// <param name="pattern2">The second pattern in the concatenation</param>
        /// <param name="patterns">Any remaining patterns to be concatenated, in order</param>
        /// <returns>A pattern whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, TAccumulator> Concat<TInput, TRegister, TAccumulator>(Afa<TInput, TRegister, TAccumulator> pattern1, Afa<TInput, TRegister, TAccumulator> pattern2, params Afa<TInput, TRegister, TAccumulator>[] patterns)
            => ConcatWorker(false, pattern1, pattern2, patterns);

        /// <summary>
        /// Creates a new pattern resulting from the concatenation of other patterns, but where each individual pattern may result in a final state
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <typeparam name="TAccumulator">The type of the accumulator in the underlying automaton</typeparam>
        /// <param name="pattern1">The first pattern in the concatenation</param>
        /// <param name="pattern2">The second pattern in the concatenation</param>
        /// <param name="patterns">Any remaining patterns to be concatenated, in order</param>
        /// <returns>A pattern whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, TAccumulator> OrConcat<TInput, TRegister, TAccumulator>(Afa<TInput, TRegister, TAccumulator> pattern1, Afa<TInput, TRegister, TAccumulator> pattern2, params Afa<TInput, TRegister, TAccumulator>[] patterns)
            => ConcatWorker(true, pattern1, pattern2, patterns);

        private static Afa<TInput, TRegister, TAccumulator> ConcatWorker<TInput, TRegister, TAccumulator>(bool isOr, Afa<TInput, TRegister, TAccumulator> pattern1, Afa<TInput, TRegister, TAccumulator> pattern2, params Afa<TInput, TRegister, TAccumulator>[] patterns)
        {
            var allPatterns = new Afa<TInput, TRegister, TAccumulator>[patterns.Length + 2];
            allPatterns[0] = pattern1;
            allPatterns[1] = pattern2;
            patterns.CopyTo(allPatterns, 2);

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
                        result.AddArc(finalState, newFinal, new EpsilonArc<TInput, TRegister> { });
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
                                if (!result.finalStates.Contains(to))
                                    result.finalStates.Add(to);
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

        /// <summary>
        /// Creates a new pattern resulting from the disjunction of other patterns
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <typeparam name="TAccumulator">The type of the accumulator in the underlying automaton</typeparam>
        /// <param name="pattern1">The first pattern in the disjunction</param>
        /// <param name="pattern2">The second pattern in the disjunction</param>
        /// <param name="patterns">Any remaining patterns to be disjunction, in order</param>
        /// <returns>A pattern whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, TAccumulator> Or<TInput, TRegister, TAccumulator>(Afa<TInput, TRegister, TAccumulator> pattern1, Afa<TInput, TRegister, TAccumulator> pattern2, params Afa<TInput, TRegister, TAccumulator>[] patterns)
        {
            var allPatterns = new Afa<TInput, TRegister, TAccumulator>[patterns.Length + 2];
            allPatterns[0] = pattern1;
            allPatterns[1] = pattern2;
            patterns.CopyTo(allPatterns, 2);

            var result = new Afa<TInput, TRegister, TAccumulator>();

            int oldMax;

            for (int i = 0; i < allPatterns.Length; i++)
            {
                var nextPattern = allPatterns[i];

                oldMax = result.MaxState + 1;

                result.AddArc(0, nextPattern.StartState + oldMax, new EpsilonArc<TInput, TRegister>());

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

            return result;
        }
        #endregion

        #region Kleene and ?

        /// <summary>
        /// Creates a new pattern resulting from zero to many iterations of the given pattern
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <typeparam name="TAccumulator">The type of the accumulator in the underlying automaton</typeparam>
        /// <param name="pattern">The pattern to iterate</param>
        /// <returns>A pattern whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, TAccumulator> KleeneStar<TInput, TRegister, TAccumulator>(Afa<TInput, TRegister, TAccumulator> pattern)
        {
            var result = new Afa<TInput, TRegister, TAccumulator>();

            // Every final state maps back to the start state
            foreach (var kvp1 in pattern.transitionInfo)
            {
                foreach (var kvp2 in kvp1.Value)
                {
                    var to = kvp2.Key;
                    if (pattern.finalStates.Contains(to))
                    {
                        to = pattern.StartState;
                    }
                    result.AddArc(kvp1.Key, to, kvp2.Value);
                }
            }

            // Start state becomes the single final state
            result.finalStates.Add(pattern.StartState);
            result.StartState = pattern.StartState;

            return result;
        }

        /// <summary>
        /// Creates a new pattern resulting from one to many iterations of the given pattern
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <typeparam name="TAccumulator">The type of the accumulator in the underlying automaton</typeparam>
        /// <param name="pattern">The pattern to iterate</param>
        /// <returns>A pattern whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, TAccumulator> KleenePlus<TInput, TRegister, TAccumulator>(Afa<TInput, TRegister, TAccumulator> pattern)
            => Concat(pattern, KleeneStar(pattern));

        /// <summary>
        /// Creates a new pattern resulting from zero or one instances of the given pattern
        /// </summary>
        /// <typeparam name="TInput">The type of stream input data</typeparam>
        /// <typeparam name="TRegister">The type of the register to be mutated as transitions occur</typeparam>
        /// <typeparam name="TAccumulator">The type of the accumulator in the underlying automaton</typeparam>
        /// <param name="pattern">The pattern to identify</param>
        /// <returns>A pattern whose first transition is the one just created</returns>
        public static Afa<TInput, TRegister, TAccumulator> ZeroOrOne<TInput, TRegister, TAccumulator>(Afa<TInput, TRegister, TAccumulator> pattern)
        {
            var result = pattern.Clone();

            if (!result.finalStates.Contains(result.StartState))
            {
                result.finalStates.Add(result.StartState);
            }
            return result;
        }
        #endregion
    }
}
