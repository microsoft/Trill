// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    internal static class StreamPropertyExtensions
    {
        /// <summary>
        /// Ungroup with group-selector
        /// </summary>
        internal static StreamProperties<TOuterKey, TResult> Ungroup<TOuterKey, TInnerKey, TPayload, TResult>(
            this StreamProperties<CompoundGroupKey<TOuterKey, TInnerKey>, TPayload> source,
            Expression<Func<TInnerKey, TPayload, TResult>> groupSelector)
        {
            IEqualityComparerExpression<TOuterKey> newKeyEqualityComparer = null;
            if (source.KeyEqualityComparer != null)
            {
                newKeyEqualityComparer = ((CompoundGroupKeyEqualityComparer<TOuterKey, TInnerKey>)source.KeyEqualityComparer).outerComparer;
            }

            IComparerExpression<TOuterKey> newKeyComparer = null;
            if (source.KeyComparer != null)
            {
                newKeyComparer = ((CompoundGroupKeyComparer<TOuterKey, TInnerKey>)source.KeyComparer).outerComparer;
            }

            return new StreamProperties<TOuterKey, TResult>(
                source.IsColumnar, source.IsConstantDuration, source.ConstantDurationLength,
                source.IsConstantHop, source.ConstantHopLength, source.ConstantHopOffset,
                source.IsIntervalFree, false,
                false, false,
                newKeyEqualityComparer,
                EqualityComparerExpression<TResult>.Default,
                newKeyComparer,
                null,
                new Dictionary<Expression, object>(),
                new Dictionary<Expression, Guid?>(),
                source.QueryContainer);
        }

        /// <summary>
        /// First-level Ungroup with group-selector
        /// </summary>
        internal static StreamProperties<Empty, TResult> Ungroup<TInnerKey, TPayload, TResult>(
            this StreamProperties<TInnerKey, TPayload> source,
            Expression<Func<TInnerKey, TPayload, TResult>> groupSelector)
        {
            return new StreamProperties<Empty, TResult>(
                source.IsColumnar, source.IsConstantDuration, source.ConstantDurationLength,
                source.IsConstantHop, source.ConstantHopLength, source.ConstantHopOffset,
                source.IsIntervalFree, false,
                false, false,
                EqualityComparerExpression<Empty>.Default,
                EqualityComparerExpression<TResult>.Default,
                null,
                null,
                new Dictionary<Expression, object>(),
                new Dictionary<Expression, Guid?>(),
                source.QueryContainer);
        }

        /// <summary>
        /// Afa
        /// </summary>
        internal static StreamProperties<TKey, TRegister> Afa<TKey, TPayload, TRegister>(this StreamProperties<TKey, TPayload> source)
        {
            // TODO: fix this to actually compute the correct stream properties for pattern detection
            Expression<Func<TPayload, TRegister>> func = x => default;
            return source.Select<TRegister>(func, false, false);
        }

        /// <summary>
        /// Map + single-input Reduce
        /// </summary>
        internal static StreamProperties<TKey, TOutput> MapReduce<TKey, TMapInputLeft, TMapInputRight, TReduceInput, TReduceKey, TBind, TOutput>(
            this StreamProperties<TKey, TMapInputLeft> leftStream,
            StreamProperties<TKey, TMapInputRight> rightStream,
            Func<IStreamable<TKey, TMapInputLeft>, IStreamable<TKey, TMapInputRight>, IStreamable<TKey, TReduceInput>> mapper,
            Expression<Func<TReduceInput, TReduceKey>> keySelector,
            Func<IStreamable<CompoundGroupKey<TKey, TReduceKey>, TReduceInput>, IStreamable<CompoundGroupKey<TKey, TReduceKey>, TBind>> reducer,
            Expression<Func<TReduceKey, TBind, TOutput>> resultSelector)
        {
            bool mc = leftStream.IsMulticore;
            var map = leftStream.ToMulticore(true).Derive(a => mapper(a, null));

            if (reducer != null)
            {
                // We need to test the reducer function to see what it does to properties.
                var group = map.GroupNested(keySelector);

                if (group.CanSpray(keySelector))
                {
                    var reduce = group.Derive(reducer).Ungroup(resultSelector);
                    if (Config.MapArity > 1)
                        reduce = reduce.Union(reduce);
                    var returnValue = reduce.ToMulticore(mc);
                    return returnValue;
                }
                else
                {
                    if (Config.MapArity > 1)
                    {
                        group = group.Union(group);
                    }

                    var reduce = group.Derive(reducer).Ungroup(resultSelector);

                    if (Config.ReduceArity > 1)
                        reduce = reduce.Union(reduce);

                    var returnValue = reduce.ToMulticore(mc);
                    return returnValue;
                }
            }
            else
            {
                var result = leftStream.Derive(a => mapper(a, null)) as StreamProperties<TKey, TOutput>;
                if (Config.MapArity > 1)
                    result = result.Union(result);
                return result.ToMulticore(mc);
            }
        }

        /// <summary>
        /// Map + single-input Reduce
        /// </summary>
        internal static StreamProperties<Empty, TOutput> MapReduce<TMapInputLeft, TMapInputRight, TReduceInput, TReduceKey, TBind, TOutput>(
            this StreamProperties<Empty, TMapInputLeft> leftStream,
            StreamProperties<Empty, TMapInputRight> rightStream,
            Func<IStreamable<Empty, TMapInputLeft>, IStreamable<Empty, TMapInputRight>, IStreamable<Empty, TReduceInput>> mapper,
            Expression<Func<TReduceInput, TReduceKey>> keySelector,
            Func<IStreamable<TReduceKey, TReduceInput>, IStreamable<TReduceKey, TBind>> reducer,
            Expression<Func<TReduceKey, TBind, TOutput>> resultSelector)
        {
            bool mc = leftStream.IsMulticore;
            var map = leftStream.ToMulticore(true).Derive(a => mapper(a, null));

            if (reducer != null)
            {
                var group = map.Group(keySelector);

                if (group.CanSpray(keySelector))
                {
                    var reduce = group.Derive(reducer).Ungroup(resultSelector);
                    if (Config.MapArity > 1)
                        reduce = reduce.Union(reduce);
                    return reduce.ToMulticore(mc);
                }
                else
                {
                    if (Config.MapArity > 1)
                    {
                        group = group.Union(group);
                    }

                    var reduce = group.Derive(reducer).Ungroup(resultSelector);

                    if (Config.ReduceArity > 1)
                        reduce = reduce.Union(reduce);

                    return reduce.ToMulticore(mc);
                }
            }
            else
            {
                var result = leftStream.Derive(a => mapper(a, null)) as StreamProperties<Empty, TOutput>;
                if (Config.MapArity > 1)
                    result = result.Union(result);
                return result.ToMulticore(mc);
            }
        }

        /// <summary>
        /// Map + 2-input Reduce
        /// </summary>
        internal static StreamProperties<TKey, TOutput>
            Map2Reduce<TKey, TMapInputLeft1, TMapInputRight1, TMapInputLeft2, TMapInputRight2, TReduceKey, TReduceInput1, TReduceInput2, TBind, TOutput>(
                this StreamProperties<TKey, TMapInputLeft1> leftStream1,
                StreamProperties<TKey, TMapInputRight1> rightStream1,
                StreamProperties<TKey, TMapInputLeft2> leftStream2,
                StreamProperties<TKey, TMapInputRight2> rightStream2,

                Func<IStreamable<TKey, TMapInputLeft1>, IStreamable<TKey, TMapInputRight1>, IStreamable<TKey, TReduceInput1>> mapper1,
                Func<IStreamable<TKey, TMapInputLeft2>, IStreamable<TKey, TMapInputRight2>, IStreamable<TKey, TReduceInput2>> mapper2,

                Func<IStreamable<CompoundGroupKey<TKey, TReduceKey>, TReduceInput1>,
                        IStreamable<CompoundGroupKey<TKey, TReduceKey>, TReduceInput2>,
                            IStreamable<CompoundGroupKey<TKey, TReduceKey>, TBind>> reducer,

                Expression<Func<TReduceInput1, TReduceKey>> keySelector1,
                Expression<Func<TReduceInput2, TReduceKey>> keySelector2,

                Expression<Func<TReduceKey, TBind, TOutput>> resultSelector,

                OperationalHint reduceOptions)
        {
            var map1 = leftStream1.Derive(a => mapper1(a, null));
            var map2 = leftStream2.Derive(a => mapper2(a, null));

            var group1 = map1.GroupNested(keySelector1);
            var group2 = map2.GroupNested(keySelector2);

            if (leftStream1.CanSpray(leftStream2, keySelector1, keySelector2) &&
                group1.CanSpray(group2, keySelector1, keySelector2))
            {
                var reduce = group1.Derive(group2, reducer).Ungroup(resultSelector);
                if (Config.MapArity > 1)
                    reduce = reduce.Union(reduce);
                return reduce;
            }
            else
            {
                if (Config.MapArity > 1)
                {
                    group1 = group1.Union(group1);
                    group2 = group2.Union(group2);
                }

                var reduce = group1.Derive(group2, reducer).Ungroup(resultSelector);

                if (Config.ReduceArity > 1)
                    reduce = reduce.Union(reduce);

                return reduce;
            }
        }

        /// <summary>
        /// Map + 2-input Reduce
        /// </summary>
        internal static StreamProperties<Empty, TOutput>
            Map2Reduce<TMapInputLeft1, TMapInputRight1, TMapInputLeft2, TMapInputRight2, TReduceKey, TReduceInput1, TReduceInput2, TBind, TOutput>(
                this StreamProperties<Empty, TMapInputLeft1> leftStream1,
                StreamProperties<Empty, TMapInputRight1> rightStream1,
                StreamProperties<Empty, TMapInputLeft2> leftStream2,
                StreamProperties<Empty, TMapInputRight2> rightStream2,

                Func<IStreamable<Empty, TMapInputLeft1>, IStreamable<Empty, TMapInputRight1>, IStreamable<Empty, TReduceInput1>> mapper1,
                Func<IStreamable<Empty, TMapInputLeft2>, IStreamable<Empty, TMapInputRight2>, IStreamable<Empty, TReduceInput2>> mapper2,

                Func<IStreamable<TReduceKey, TReduceInput1>,
                        IStreamable<TReduceKey, TReduceInput2>,
                            IStreamable<TReduceKey, TBind>> reducer,

                Expression<Func<TReduceInput1, TReduceKey>> keySelector1,
                Expression<Func<TReduceInput2, TReduceKey>> keySelector2,

                Expression<Func<TReduceKey, TBind, TOutput>> resultSelector,

                OperationalHint reduceOptions)
        {
            var map1 = leftStream1.Derive(a => mapper1(a, null));
            var map2 = leftStream2.Derive(a => mapper2(a, null));

            var group1 = map1.Group(keySelector1);
            var group2 = map2.Group(keySelector2);

            if (leftStream1.CanSpray(leftStream2, keySelector1, keySelector2) &&
                group1.CanSpray(group2, keySelector1, keySelector2))
            {
                var reduce = group1.Derive(group2, reducer).Ungroup(resultSelector);
                if (Config.MapArity > 1)
                    reduce = reduce.Union(reduce);
                return reduce;
            }
            else
            {
                if (Config.MapArity > 1)
                {
                    group1 = group1.Union(group1);
                    group2 = group2.Union(group2);
                }

                var reduce = group1.Derive(group2, reducer).Ungroup(resultSelector);

                if (Config.ReduceArity > 1)
                    reduce = reduce.Union(reduce);

                return reduce;
            }
        }
    }
}
