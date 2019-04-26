// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    internal sealed class MapDefinition<TMapKey, TMapInputLeft, TMapInputRight, TReduceKey, TReduceInput> : IMapDefinition<TMapKey, TMapInputLeft, TMapInputRight, TReduceKey, TReduceInput>
    {
        internal IStreamable<TMapKey, TMapInputLeft> sourceLeft;
        internal IStreamable<TMapKey, TMapInputRight> sourceRight;
        internal Func<IStreamable<TMapKey, TMapInputLeft>, IStreamable<TMapKey, TMapInputRight>, IStreamable<TMapKey, TReduceInput>> mapper;
        internal Expression<Func<TReduceInput, TReduceKey>> keySelector;
        internal bool leftAsymmetric;

        public MapDefinition(
            IStreamable<TMapKey, TMapInputLeft> sourceLeft,
            IStreamable<TMapKey, TMapInputRight> sourceRight,
            Func<IStreamable<TMapKey, TMapInputLeft>, IStreamable<TMapKey, TMapInputRight>, IStreamable<TMapKey, TReduceInput>> mapper,
            Expression<Func<TReduceInput, TReduceKey>> keySelector,
            bool leftAsymmetric = false)
        {
            this.sourceLeft = sourceLeft;
            this.sourceRight = sourceRight;
            this.mapper = mapper;
            this.keySelector = keySelector;
            this.leftAsymmetric = leftAsymmetric;
        }

        public IStreamable<TMapKey, TOutput> CreateStreamable<TBind, TOutput>(
            Func<IStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput>, IStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TBind>> reducer,
            Expression<Func<GroupSelectorInput<TReduceKey>, TBind, TOutput>> resultSelector)
        {
            Contract.Assume(sourceLeft != null);

            var sourceL = sourceLeft;
            var sourceR = sourceRight;

            Expression<Func<TReduceKey, TBind, TOutput>> resultSelector2 = (k, b) =>
            CallInliner.Call(resultSelector, new GroupSelectorInput<TReduceKey>(k), b);
            Expression<Func<TReduceKey, TBind, TOutput>> inlinedResultSelector = resultSelector2.InlineCalls();
            inlinedResultSelector = GroupInputAndKeyInliner<TReduceKey, TBind, TOutput>.Transform(inlinedResultSelector);

            return new MapReduceStreamable<TMapKey, TMapInputLeft, TMapInputRight, TReduceKey, TReduceInput, TBind, TOutput>(
                sourceL,
                sourceR,
                mapper,
                keySelector,
                reducer,
                inlinedResultSelector,
                leftAsymmetric);
        }

        public IStreamable<TMapKey, TOutput> CreateStreamable<TBind, TOutput>()
        {
            Contract.Assume(sourceLeft != null);

            var sourceL = sourceLeft;
            var sourceR = sourceRight;

            return new MapReduceStreamable<TMapKey, TMapInputLeft, TMapInputRight, TReduceKey, TReduceInput, TBind, TOutput>(
                sourceL,
                sourceR,
                mapper,
                keySelector,
                leftAsymmetric);
        }

        /* Create a streamable for a 2-input reducer, with two 2-input mappers */
        public IStreamable<TMapKey, TOutput> CreateStreamable<TMapInputLeft2, TMapInputRight2, TReduceInput2, TBind, TOutput>(
            IMapDefinition<TMapKey, TMapInputLeft2, TMapInputRight2, TReduceKey, TReduceInput2> imapDefinitionRight,
            Func<IStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput>,
                    IStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput2>,
                        IStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TBind>> reducer,
            Expression<Func<GroupSelectorInput<TReduceKey>, TBind, TOutput>> resultSelector,
            OperationalHint reduceOptions)
        {
            Contract.Assume(sourceLeft != null);

            var mapDefinitionRight = (MapDefinition<TMapKey, TMapInputLeft2, TMapInputRight2, TReduceKey, TReduceInput2>)imapDefinitionRight;
            var sourceL1 = sourceLeft;
            var sourceR1 = sourceRight;

            var sourceL2 = mapDefinitionRight.sourceLeft;
            var sourceR2 = mapDefinitionRight.sourceRight;

            Expression<Func<TReduceKey, TBind, TOutput>> resultSelector2 = (k, b) =>
                CallInliner.Call(resultSelector, new GroupSelectorInput<TReduceKey>(k), b);
            var inlinedResultSelector = resultSelector2.InlineCalls();

            return new Map2ReduceStreamable<TMapKey, TMapInputLeft, TMapInputRight, TMapInputLeft2, TMapInputRight2, TReduceKey, TReduceInput, TReduceInput2, TBind, TOutput>(
                sourceL1,
                sourceR1,
                mapper,
                keySelector,
                sourceL2,
                sourceR2,
                mapDefinitionRight.mapper,
                mapDefinitionRight.keySelector,
                reducer,
                inlinedResultSelector,
                leftAsymmetric,
                mapDefinitionRight.leftAsymmetric, reduceOptions);
        }
    }

    internal sealed class MapDefinition<TMapInputLeft, TMapInputRight, TReduceKey, TReduceInput> : IMapDefinition<TMapInputLeft, TMapInputRight, TReduceKey, TReduceInput>
    {
        internal IStreamable<Empty, TMapInputLeft> sourceLeft;
        internal IStreamable<Empty, TMapInputRight> sourceRight;
        internal Func<IStreamable<Empty, TMapInputLeft>, IStreamable<Empty, TMapInputRight>, IStreamable<Empty, TReduceInput>> mapper;
        internal Expression<Func<TReduceInput, TReduceKey>> keySelector;
        internal bool leftAsymmetric;

        public MapDefinition(
            IStreamable<Empty, TMapInputLeft> sourceLeft,
            IStreamable<Empty, TMapInputRight> sourceRight,
            Func<IStreamable<Empty, TMapInputLeft>, IStreamable<Empty, TMapInputRight>, IStreamable<Empty, TReduceInput>> mapper,
            Expression<Func<TReduceInput, TReduceKey>> keySelector,
            bool leftAsymmetric = false)
        {
            this.sourceLeft = sourceLeft;
            this.sourceRight = sourceRight;
            this.mapper = mapper;
            this.keySelector = keySelector;
            this.leftAsymmetric = leftAsymmetric;
        }

        public IStreamable<Empty, TOutput> CreateStreamable<TBind, TOutput>(
            Func<IStreamable<TReduceKey, TReduceInput>, IStreamable<TReduceKey, TBind>> reducer,
            Expression<Func<GroupSelectorInput<TReduceKey>, TBind, TOutput>> resultSelector)
        {
            Contract.Assume(sourceLeft != null);

            var sourceL = sourceLeft;
            var sourceR = sourceRight;

            Expression<Func<TReduceKey, TBind, TOutput>> resultSelector2 = (k, b) =>
            CallInliner.Call(resultSelector, new GroupSelectorInput<TReduceKey>(k), b);
            var inlinedResultSelector = resultSelector2.InlineCalls();

            return new MapReduceStreamable<TMapInputLeft, TMapInputRight, TReduceKey, TReduceInput, TBind, TOutput>(
                sourceL,
                sourceR,
                mapper,
                keySelector,
                reducer,
                inlinedResultSelector,
                leftAsymmetric);
        }

        public IStreamable<Empty, TOutput> CreateStreamable<TBind, TOutput>()
        {
            Contract.Assume(sourceLeft != null);

            var sourceL = sourceLeft;
            var sourceR = sourceRight;

            return new MapReduceStreamable<TMapInputLeft, TMapInputRight, TReduceKey, TReduceInput, TBind, TOutput>(
                sourceL,
                sourceR,
                mapper,
                keySelector,
                leftAsymmetric);
        }

        /* Create a streamable for a 2-input reducer, with two 2-input mappers */
        public IStreamable<Empty, TOutput> CreateStreamable<TMapInputLeft2, TMapInputRight2, TReduceInput2, TBind, TOutput>(
            IMapDefinition<TMapInputLeft2, TMapInputRight2, TReduceKey, TReduceInput2> imapDefinitionRight,
            Func<IStreamable<TReduceKey, TReduceInput>,
                    IStreamable<TReduceKey, TReduceInput2>,
                        IStreamable<TReduceKey, TBind>> reducer,
            Expression<Func<GroupSelectorInput<TReduceKey>, TBind, TOutput>> resultSelector,
            OperationalHint reduceOptions)
        {
            Contract.Assume(sourceLeft != null);

            var mapDefinitionRight = (MapDefinition<TMapInputLeft2, TMapInputRight2, TReduceKey, TReduceInput2>)imapDefinitionRight;
            var sourceL1 = sourceLeft;
            var sourceR1 = sourceRight;

            var sourceL2 = mapDefinitionRight.sourceLeft;
            var sourceR2 = mapDefinitionRight.sourceRight;

            Expression<Func<TReduceKey, TBind, TOutput>> resultSelector2 = (k, b) =>
                CallInliner.Call(resultSelector, new GroupSelectorInput<TReduceKey>(k), b);
            var inlinedResultSelector = resultSelector2.InlineCalls();

            return new Map2ReduceStreamable<TMapInputLeft, TMapInputRight, TMapInputLeft2, TMapInputRight2, TReduceKey, TReduceInput, TReduceInput2, TBind, TOutput>(
                sourceL1,
                sourceR1,
                mapper,
                keySelector,
                sourceL2,
                sourceR2,
                mapDefinitionRight.mapper,
                mapDefinitionRight.keySelector,
                reducer,
                inlinedResultSelector,
                leftAsymmetric,
                mapDefinitionRight.leftAsymmetric, reduceOptions);
        }
    }

}
