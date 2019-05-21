// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.StreamProcessing.Aggregates;
using Microsoft.StreamProcessing.Internal;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Streamable extension methods.
    /// </summary>
    public static class Properties
    {
        #region Property ops

        /// <summary>
        /// Enter into a mode where you can set a property for the stream.
        /// </summary>
        internal static IStreamable<TKey, TPayload> GetStreamable<TKey, TPayload>(this IPropertySetter<TKey, TPayload> source)
            => ((PropertySetter<TKey, TPayload>)source).GetStreamable();

        /// <summary>
        /// Enter into a mode where you can set a property for the stream.
        /// </summary>
        public static IPropertySetter<TKey, TPayload> SetProperty<TKey, TPayload>(this IStreamable<TKey, TPayload> source)
            => new PropertySetter<TKey, TPayload>(source);

        /// <summary>
        /// Set a property of whether or not the stream is devoid of intervals.
        /// </summary>
        public static IStreamable<TKey, TPayload> IsIntervalFree<TKey, TPayload>(this IPropertySetter<TKey, TPayload> source, bool isIntervalFree)
            => new SetPropertyStreamable<TKey, TPayload>(source.GetStreamable(), e => e.ToIntervalFree(isIntervalFree));

        /// <summary>
        /// Set a property of whether or not the stream is devoid of simultaneity w.r.t. sync-times.
        /// </summary>
        public static IStreamable<TKey, TPayload> IsSyncTimeSimultaneityFree<TKey, TPayload>(this IPropertySetter<TKey, TPayload> source, bool isSyncTimeSimultaneityFree)
            => new SetPropertyStreamable<TKey, TPayload>(source.GetStreamable(), e => e.ToSyncTimeSimultaneityFree(isSyncTimeSimultaneityFree));

        /// <summary>
        /// Set a property of whether or not the stream is devoid of simultaneity w.r.t. sync-times.
        /// </summary>
        public static IStreamable<TKey, TPayload> IsEventOverlappingFree<TKey, TPayload>(this IPropertySetter<TKey, TPayload> source, bool isEventOverlappingFree)
            => new SetPropertyStreamable<TKey, TPayload>(source.GetStreamable(), e => e.ToEventOverlappingFree(isEventOverlappingFree));

        /// <summary>
        /// Set a property of whether events in the stream all have constant duration, and optionally, the width of the constant duration (null if not known).
        /// </summary>
        public static IStreamable<TKey, TPayload> IsConstantDuration<TKey, TPayload>(this IPropertySetter<TKey, TPayload> source, bool isConstantDuration, long? constantDuration = null)
            => new SetPropertyStreamable<TKey, TPayload>(source.GetStreamable(), e => e.ToConstantDuration(isConstantDuration, constantDuration));

        /// <summary>
        /// Set a property of whether events in the stream all have constant hop, and optionally, the period and offset of the constant hop (null if not known).
        /// </summary>
        public static IStreamable<TKey, TPayload> IsConstantHop<TKey, TPayload>(this IPropertySetter<TKey, TPayload> source, bool isConstantHop, long? constantHopPeriod = null, long? constantHopOffset = null)
            => new SetPropertyStreamable<TKey, TPayload>(source.GetStreamable(), e => e.ToConstantHop(isConstantHop, constantHopPeriod, constantHopOffset));

        /// <summary>
        /// Sets the PayloadEqualityComparer property associated with the stream.
        /// This comparer is an efficient comparer optimized for this data instance, and does not apply to other streams with
        /// the same schema. For a schema-level equality comparer, implement the interface IEqualityComparerExpression.
        /// </summary>
        public static IStreamable<TKey, TPayload> PayloadEqualityComparer<TKey, TPayload>(
            this IPropertySetter<TKey, TPayload> source,
            IEqualityComparerExpression<TPayload> payloadEqualityComparer)
            => new SetPropertyStreamable<TKey, TPayload>(source.GetStreamable(), e => e.ToPayloadEqualityComparer(payloadEqualityComparer));

        /// <summary>
        /// Sets the PayloadEqualityComparer property associated with the stream.
        /// This comparer is an efficient comparer optimized for this data instance, and does not apply to other streams with
        /// the same schema. For a schema-level equality comparer, implement the interface IEqualityComparerExpression.
        /// </summary>
        public static IStreamable<TKey, TPayload> PayloadEqualityComparer<TKey, TPayload>(
            this IPropertySetter<TKey, TPayload> source,
            Expression<Func<TPayload, TPayload, bool>> equalsExpr,
            Expression<Func<TPayload, int>> getHashCodeExpr)
            => new SetPropertyStreamable<TKey, TPayload>(
                source.GetStreamable(), e => e.ToPayloadEqualityComparer(
                    new EqualityComparerExpression<TPayload>(equalsExpr, getHashCodeExpr)));

        /// <summary>
        /// Sets the PayloadComparer property associated with the stream.
        /// This comparer is an efficient comparer optimized for this data instance, and does not apply to other streams with
        /// the same schema. For a schema-level comparer, implement the interface IComparerExpression.
        /// </summary>
        public static IStreamable<TKey, TPayload> PayloadComparer<TKey, TPayload>(
            this IPropertySetter<TKey, TPayload> source,
            IComparerExpression<TPayload> payloadComparer)
            => new SetPropertyStreamable<TKey, TPayload>(source.GetStreamable(), e => e.ToPayloadComparer(payloadComparer));

        /// <summary>
        /// Sets the PayloadComparer property associated with the stream.
        /// This comparer is an efficient comparer optimized for this data instance, and does not apply to other streams with
        /// the same schema. For a schema-level comparer, implement the interface IComparerExpression.
        /// </summary>
        public static IStreamable<TKey, TPayload> PayloadComparer<TKey, TPayload>(
            this IPropertySetter<TKey, TPayload> source,
            Expression<Comparison<TPayload>> compareExpr)
            => new SetPropertyStreamable<TKey, TPayload>(
                source.GetStreamable(), e => e.ToPayloadComparer(
                    new ComparerExpression<TPayload>(compareExpr)));

        /// <summary>
        /// Sets the KeyEqualityComparer property associated with the stream.
        /// This comparer is an efficient comparer optimized for this data instance, and does not apply to other streams with
        /// the same schema. For a schema-level equality comparer, implement the interface IEqualityComparerExpression.
        /// </summary>
        public static IStreamable<TKey, TPayload> KeyEqualityComparer<TKey, TPayload>(
            this IPropertySetter<TKey, TPayload> source,
            IEqualityComparerExpression<TKey> keyEqualityComparer)
            => new SetPropertyStreamable<TKey, TPayload>(source.GetStreamable(), e => e.ToKeyEqualityComparer(keyEqualityComparer));

        /// <summary>
        /// Sets a selected-substream equality comparer for the stream. Multiple of these can be set for various selectors.
        /// This comparer is an efficient comparer optimized for this data instance, and does not apply to other streams with
        /// the same schema. For a schema-level equality comparer, implement the interface IEqualityComparerExpression.
        /// </summary>
        public static IStreamable<TKey, TPayload> EqualityComparer<TKey, TPayload, T>(
            this IPropertySetter<TKey, TPayload> source,
            Expression<Func<TPayload, T>> selectorExpr,
            IEqualityComparerExpression<T> equalityComparerExpr)
            => new SetPropertyStreamable<TKey, TPayload>(
                source.GetStreamable(),
                e => e.ToEqualityComparer(selectorExpr, equalityComparerExpr));

        /// <summary>
        /// Sets a selected-substream equality comparer for the stream. Multiple of these can be set for various selectors.
        /// This comparer is an efficient comparer optimized for this data instance, and does not apply to other streams with
        /// the same schema.
        /// </summary>
        public static IStreamable<TKey, TPayload> EqualityComparer<TKey, TPayload, T>(
            this IPropertySetter<TKey, TPayload> source,
            Expression<Func<TPayload, T>> selectorExpr,
            Expression<Func<T, T, bool>> equalsExpr,
            Expression<Func<T, int>> getHashCodeExpr)
            => new SetPropertyStreamable<TKey, TPayload>(
                source.GetStreamable(),
                e => e.ToEqualityComparer(selectorExpr, new EqualityComparerExpression<T>(equalsExpr, getHashCodeExpr)));

        /// <summary>
        /// Sets a property whether or nor the stream is sorted (by entire payload) per snapshot
        /// </summary>
        public static IStreamable<TKey, TPayload> IsSnapshotSorted<TKey, TPayload>(this IPropertySetter<TKey, TPayload> source, bool isSnapshotSorted, Guid? packingScheme = null)
            => new SetPropertyStreamable<TKey, TPayload>(source.GetStreamable(), e => e.ToSnapshotSorted(isSnapshotSorted, packingScheme));

        /// <summary>
        /// Sets a property whether or nor the stream is sorted per snapshot. If sorted, specifies the sort fields associated with the stream (entire payload by default)
        /// </summary>
        public static IStreamable<TKey, TPayload> IsSnapshotSorted<TKey, TPayload, T>
            (this IPropertySetter<TKey, TPayload> source, bool isSnapshotSorted, Expression<Func<TPayload, T>> sortFieldsSelector, Guid? packingScheme = null)
            => new SetPropertyStreamable<TKey, TPayload>(source.GetStreamable(), e => e.ToSnapshotSorted(isSnapshotSorted, sortFieldsSelector, packingScheme));

        /// <summary>
        /// Sets the current stream properties to the provided argument (newProperties)
        /// </summary>
        public static IStreamable<TKey, TPayload> SetProperties<TKey, TPayload>(this IStreamable<TKey, TPayload> source, StreamProperties<TKey, TPayload> newProperties)
            => new SetPropertyStreamable<TKey, TPayload>(source, e => newProperties);

        /// <summary>
        /// Sets the out parameter (properties) to the current properties of the stream
        /// </summary>
        public static IStreamable<TKey, TPayload> GetProperties<TKey, TPayload>(this IStreamable<TKey, TPayload> source, out StreamProperties<TKey, TPayload> currentProperties)
        {
            currentProperties = source.Properties;
            return source;
        }

        #endregion
    }

    /// <summary>
    /// Static class for transformations to Streamables
    /// </summary>
    public static partial class Streamable
    {
        #region Import/export ops

        /// <summary>
        /// Converts a stream into an interval-only stream (devoid of end edges).
        /// </summary>
        internal static IStreamable<TUnit, TPayload> ToEndEdgeFreeStream<TUnit, TPayload>(
            this IStreamable<TUnit, TPayload> stream)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return new EndEdgeFreeOutputStreamable<TUnit, TPayload>(stream);
        }

        /// <summary>
        /// Create a stream that reports all deaccumulations as an insert-only point stream (converts all events into point events at end).
        /// </summary>
        public static IStreamable<TKey, TPayload> PointAtEnd<TKey, TPayload>(
            this IStreamable<TKey, TPayload> stream)
        {
            Invariant.IsNotNull(stream, nameof(stream));

            return new PointAtEndStreamable<TKey, TPayload>(stream);
        }

        /// <summary>
        /// Create a stream that extends all event lifetimes by a fixed amount.
        /// </summary>
        public static IStreamable<TKey, TPayload> ExtendLifetime<TKey, TPayload>(
            this IStreamable<TKey, TPayload> stream,
            long duration)
        {
            Contract.Requires(stream != null);
            if (stream.Properties.IsConstantDuration)
            {
                var newDuration = duration + stream.Properties.ConstantDurationLength.Value;
                return newDuration > 0
                    ? AlterEventDuration(stream, newDuration)
                    : Where(stream, o => false);
            }
            return new ExtendLifetimeStreamable<TKey, TPayload>(stream, duration);
        }
        #endregion

        #region IStreamable<,> ops

        /// <summary>
        /// Performs dynamic version of multicast over a streamable. This allows query writers to execute
        /// multiple subqueries over the same physical input stream - streams can be added or removed. It is
        /// up to the user to ensure that the stream has no end edges, otherwise a dynamically subscribed subscriber may
        /// receive a malformed stream (end edge without corresponding start edge).
        /// Usage: stream.Publish(); stream.Subscribe(...); stream.Connect(); stream.Subscribe(...);
        /// </summary>
        public static IConnectableStreamable<TKey, TPayload> Publish<TKey, TPayload>(this IStreamable<TKey, TPayload> source)
        {
            Invariant.IsNotNull(source, nameof(source));

            return new ConnectableStreamable<TKey, TPayload>(source);
        }

        /// <summary>
        /// Performs multicast over a streamable. This allows query writers to execute multiple subqueries over the same physical input stream.
        /// </summary>
        public static IStreamable<TKey, TResult> Multicast<TKey, TPayload, TResult>(this IStreamable<TKey, TPayload> source, Func<IStreamable<TKey, TPayload>, IStreamable<TKey, TResult>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));

            return new MulticastStreamable<TKey, TPayload, TResult>(source, selector);
        }

        /// <summary>
        /// Performs multicast over a streamable. This allows query writers to execute multiple subqueries over the same physical input stream.
        /// </summary>
        public static IStreamable<TKey, TPayload>[] Multicast<TKey, TPayload>(this IStreamable<TKey, TPayload> source, int outputCount)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsPositive(outputCount, nameof(outputCount));

            return NWayMulticast<TKey, TPayload>.GenerateStreamableArray(source, outputCount);
        }

        /// <summary>
        /// Performs multicast over a streamable. This allows query writers to execute multiple subqueries over the same physical input stream.
        /// </summary>
        public static IStreamable<TKey, TResult> Multicast<TKey, TPayloadLeft, TPayloadRight, TResult>(this IStreamable<TKey, TPayloadLeft> sourceLeft, IStreamable<TKey, TPayloadRight> sourceRight, Func<IStreamable<TKey, TPayloadLeft>, IStreamable<TKey, TPayloadRight>, IStreamable<TKey, TResult>> selector)
        {
            Invariant.IsNotNull(sourceLeft, nameof(sourceLeft));
            Invariant.IsNotNull(sourceRight, nameof(sourceRight));
            Invariant.IsNotNull(selector, nameof(selector));

            return new BinaryMulticastStreamable<TKey, TPayloadLeft, TPayloadRight, TResult>(sourceLeft, sourceRight, selector);
        }

        /// <summary>
        /// Converts a stream of columnar batches into a stream of row oriented batches.
        /// </summary>
        internal static IStreamable<TKey, TPayload> ColumnToRow<TKey, TPayload>(this IStreamable<TKey, TPayload> source)
        {
            Invariant.IsNotNull(source, nameof(source));

            return !source.Properties.IsColumnar
                ? source
                : new ColumnToRowStreamable<TKey, TPayload>(source);
        }

        /// <summary>
        /// Converts a stream of row oriented batches into a stream of columnar batches
        /// </summary>
        internal static IStreamable<TKey, TPayload> RowToColumn<TKey, TPayload>(this IStreamable<TKey, TPayload> source)
        {
            Invariant.IsNotNull(source, nameof(source));

            return source.Properties.IsColumnar
                ? source
                : new RowToColumnStreamable<TKey, TPayload>(source);
        }

        /// <summary>
        /// Performs a filter over a streamable, excluding rows for which the predicate evaluates to false.
        /// </summary>
        /// <param name="source">The input stream to filter</param>
        /// <param name="predicate">The predicate to apply to all data in the stream</param>
        public static IStreamable<TKey, TPayload> Where<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, bool>> predicate)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(predicate, nameof(predicate));

            return source is IFusibleStreamable<TKey, TPayload> s
                ? s.FuseWhere(predicate)
                : (IStreamable<TKey, TPayload>)new WhereStreamable<TKey, TPayload>(source, predicate);
        }

        /// <summary>
        /// Performs the 'Chop' operator to chop (partition) long-lasting intervals and edges across beat boundaries.
        /// </summary>
        public static IStreamable<TKey, TPayload> Chop<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source,
            long offset,
            long period)
        {
            Invariant.IsNotNull(source, nameof(source));

            return new BeatStreamable<TKey, TPayload>(source, offset, period);
        }

        /// <summary>
        /// This select many is used for generating many payloads out of one
        /// </summary>
        public static IStreamable<TKey, TResult> SelectMany<TKey, TPayload, TResult>(
            this IStreamable<TKey, TPayload> source,
            Expression<Func<TPayload, IEnumerable<TResult>>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));

            return source is IFusibleStreamable<TKey, TPayload> s && s.CanFuseSelectMany(selector, false, false)
                ? s.FuseSelectMany(selector)
                : (IStreamable<TKey, TResult>)new SelectManyStreamable<TKey, TPayload, TResult>(source, selector);
        }

        /// <summary>
        /// This select many is used for generating many payloads out of one
        /// </summary>
        public static IStreamable<TKey, TResult> SelectMany<TKey, TPayload, TResult>(
            this IStreamable<TKey, TPayload> source,
            Expression<Func<long, TPayload, IEnumerable<TResult>>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));

            return source is IFusibleStreamable<TKey, TPayload> s && s.CanFuseSelectMany(selector, true, false)
                ? s.FuseSelectMany(selector)
                : (IStreamable<TKey, TResult>)new SelectManyStreamable<TKey, TPayload, TResult>(source, selector, hasStartEdge: true);
        }

        /// <summary>
        /// This select many is used for generating many payloads out of one
        /// </summary>
        public static IStreamable<TKey, TResult> SelectManyByKey<TKey, TPayload, TResult>(
            this IStreamable<TKey, TPayload> source,
            Expression<Func<TKey, TPayload, IEnumerable<TResult>>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));

            return source is IFusibleStreamable<TKey, TPayload> s && s.CanFuseSelectMany(selector, false, true)
                ? s.FuseSelectManyWithKey(selector)
                : (IStreamable<TKey, TResult>)new SelectManyStreamable<TKey, TPayload, TResult>(source, selector, hasKey: true);
        }

        /// <summary>
        /// This select many is used for generating many payloads out of one
        /// </summary>
        public static IStreamable<TKey, TResult> SelectManyByKey<TKey, TPayload, TResult>(
            this IStreamable<TKey, TPayload> source,
            Expression<Func<long, TKey, TPayload, IEnumerable<TResult>>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));

            return source is IFusibleStreamable<TKey, TPayload> s && s.CanFuseSelectMany(selector, true, true)
                ? s.FuseSelectManyWithKey(selector)
                : (IStreamable<TKey, TResult>)new SelectManyStreamable<TKey, TPayload, TResult>(source, selector, hasStartEdge: true, hasKey: true);
        }

        /// <summary>
        /// This select many is used for joins derived from the comprehension syntax as well as non-empty reducers in Group and Apply
        /// </summary>
        public static IStreamable<TKey, TResult> SelectMany<TKey, TLeft, TRight, TResult>(
            this IStreamable<TKey, TLeft> left,
            Func<Empty, IStreamable<TKey, TRight>> right,
            Expression<Func<TLeft, TRight, TResult>> resultSelector)
        {
            Invariant.IsNotNull(left, nameof(left));
            Invariant.IsNotNull(right, nameof(right));
            Invariant.IsNotNull(resultSelector, nameof(resultSelector));

            return new EquiJoinStreamable<TKey, TLeft, TRight, TResult>(left, right(Empty.Default), resultSelector);
        }

        /// <summary>
        /// Stitch is the reverse of the 'Chop' operator: when it finds an END payload and a BEGIN payload at the same
        /// time, it removes both from the stream. The net effect is that if a value in a signal doesn't change, then the
        /// stream won't have new events. This is mostly useful for human eyes; mathemtically, the two should be identical
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        public static IStreamable<TKey, TPayload> Stitch<TKey, TPayload>(this IStreamable<TKey, TPayload> source)
        {
            Invariant.IsNotNull(source, nameof(source));

            return new StitchStreamable<TKey, TPayload>(source);
        }

        /// <summary>
        /// Union is a temporal union that combines two streams of like schema
        /// </summary>
        /// <param name="left">Left source streamable for the operation.</param>
        /// <param name="right">Right source streamable for the operation.</param>
        public static IStreamable<TKey, TPayload> Union<TKey, TPayload>(
            this IStreamable<TKey, TPayload> left,
            IStreamable<TKey, TPayload> right)
        {
            Invariant.IsNotNull(left, nameof(left));
            Invariant.IsNotNull(right, nameof(right));

            return new UnionStreamable<TKey, TPayload>(left, right);
        }

        /// <summary>
        /// Performs an equijoin between 2 streams where the join key is of type TJoinKey and selectors are passed to extract
        /// join keys from payloads
        /// </summary>
        /// <typeparam name="TKey">The key type for the inputs.</typeparam>
        /// <typeparam name="TLeft">The type of the left input.</typeparam>
        /// <typeparam name="TRight">The type of the right input.</typeparam>
        /// <typeparam name="TJoinKey">The type of the join condition.</typeparam>
        /// <typeparam name="TResult">The type of the output of the join.</typeparam>
        /// <param name="left">The left input to the join operation.</param>
        /// <param name="right">The right input to the join operation.</param>
        /// <param name="leftKeySelector">An expression that specifies the left half of the join condition.</param>
        /// <param name="rightKeySelector">An expression that specifies the right half of the join condition.</param>
        /// <param name="resultSelector">An expression that describes how to create result instances.</param>
        /// <returns></returns>
        public static IStreamable<TKey, TResult> Join<TKey, TLeft, TRight, TJoinKey, TResult>(
            this IStreamable<TKey, TLeft> left,
            IStreamable<TKey, TRight> right,
            Expression<Func<TLeft, TJoinKey>> leftKeySelector,
            Expression<Func<TRight, TJoinKey>> rightKeySelector,
            Expression<Func<TLeft, TRight, TResult>> resultSelector)
            => left.Join(right, leftKeySelector, rightKeySelector, resultSelector, OperationalHint.None);

        /// <summary>
        /// Performs an equijoin between 2 streams where the join key is of type TJoinKey and selectors are passed to extract
        /// join keys from payloads
        /// </summary>
        /// <typeparam name="TKey">The key type for the inputs.</typeparam>
        /// <typeparam name="TLeft">The type of the left input.</typeparam>
        /// <typeparam name="TRight">The type of the right input.</typeparam>
        /// <typeparam name="TJoinKey">The type of the join condition.</typeparam>
        /// <typeparam name="TResult">The type of the output of the join.</typeparam>
        /// <param name="left">The left input to the join operation.</param>
        /// <param name="right">The right input to the join operation.</param>
        /// <param name="leftKeySelector">An expression that specifies the left half of the join condition.</param>
        /// <param name="rightKeySelector">An expression that specifies the right half of the join condition.</param>
        /// <param name="resultSelector">An expression that describes how to create result instances.</param>
        /// <param name="joinOptions">Additional parameter that specifies more information about the join.</param>
        /// <returns></returns>
        public static IStreamable<TKey, TResult> Join<TKey, TLeft, TRight, TJoinKey, TResult>(
            this IStreamable<TKey, TLeft> left,
            IStreamable<TKey, TRight> right,
            Expression<Func<TLeft, TJoinKey>> leftKeySelector,
            Expression<Func<TRight, TJoinKey>> rightKeySelector,
            Expression<Func<TLeft, TRight, TResult>> resultSelector,
            OperationalHint joinOptions = OperationalHint.None)
        {
            Invariant.IsNotNull(left, nameof(left));
            Invariant.IsNotNull(right, nameof(right));
            Invariant.IsNotNull(leftKeySelector, nameof(leftKeySelector));
            Invariant.IsNotNull(rightKeySelector, nameof(rightKeySelector));
            Invariant.IsNotNull(resultSelector, nameof(resultSelector));

            var map1 = left.Map(leftKeySelector);
            var map2 = right.Map(rightKeySelector);

            return map1.Reduce(map2, (a, b) => a.Join(b, resultSelector), (g, c) => c, joinOptions);
        }

        /// <summary>
        /// Performs a cross-product between 2 streams
        /// </summary>
        public static IStreamable<TKey, TResult> Join<TKey, TLeft, TRight, TResult>(
            this IStreamable<TKey, TLeft> left,
            IStreamable<TKey, TRight> right,
            Expression<Func<TLeft, TRight, TResult>> resultSelector)
        {
            Invariant.IsNotNull(left, nameof(left));
            Invariant.IsNotNull(right, nameof(right));

            return new EquiJoinStreamable<TKey, TLeft, TRight, TResult>(left, right, resultSelector);
        }

        /// <summary>
        /// Performs a left anti-semi join using the passed in predicate as the join condition.
        /// </summary>
        public static IStreamable<TKey, TLeft> WhereNotExists<TKey, TLeft, TRight, TJoinKey>(
            this IStreamable<TKey, TLeft> left,
            IStreamable<TKey, TRight> right,
            Expression<Func<TLeft, TJoinKey>> leftKeySelector,
            Expression<Func<TRight, TJoinKey>> rightKeySelector)
        {
            Invariant.IsNotNull(left, nameof(left));
            Invariant.IsNotNull(right, nameof(right));
            Invariant.IsNotNull(leftKeySelector, nameof(leftKeySelector));
            Invariant.IsNotNull(rightKeySelector, nameof(rightKeySelector));

            var map1 = left.Map(leftKeySelector);
            var map2 = right.Map(rightKeySelector);

            return
                map1.Reduce(
                    map2,
                    (a, b) => a.WhereNotExists(b),
                    (g, c) => c);
        }

        /// <summary>
        /// Performs a left anti-semi join without any join condition (join condition is true).
        /// </summary>
        public static IStreamable<TKey, TLeft> WhereNotExists<TKey, TLeft, TRight>(
            this IStreamable<TKey, TLeft> left,
            IStreamable<TKey, TRight> right)
        {
            Invariant.IsNotNull(left, nameof(left));
            Invariant.IsNotNull(right, nameof(right));

            return new LeftAntiSemiJoinStreamable<TKey, TLeft, TRight>(left, right);
        }

        /// <summary>
        /// Passes a truncated version of each event on the left, where the left event is truncated by the first event on the right
        /// whose Vs occurs after the event on the left. There is no restriction on join condition.
        /// </summary>
        public static IStreamable<TKey, TPayload> ClipEventDuration<TKey, TPayload, TClip>(
            this IStreamable<TKey, TPayload> source,
            IStreamable<TKey, TClip> clip)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(clip, nameof(clip));

            return new ClipJoinStreamable<TKey, TPayload, TClip>(source, clip);
        }

        /// <summary>
        /// Passes a truncated version of each event on the left, where the left event is truncated by the first event on the right
        /// whose Vs occurs after the event on the left, whose join condition is met, and where the keys for both streams match. A
        /// fast join key comparer is passed in for efficiency.
        /// </summary>
        public static IStreamable<TKey, TLeft> ClipEventDuration<TKey, TLeft, TRight, TJoinKey>(
            this IStreamable<TKey, TLeft> left,
            IStreamable<TKey, TRight> right,
            Expression<Func<TLeft, TJoinKey>> leftKeySelector,
            Expression<Func<TRight, TJoinKey>> rightKeySelector)
        {
            Invariant.IsNotNull(left, nameof(left));
            Invariant.IsNotNull(right, nameof(right));
            Invariant.IsNotNull(leftKeySelector, nameof(leftKeySelector));
            Invariant.IsNotNull(rightKeySelector, nameof(rightKeySelector));

            var map1 = left.Map(leftKeySelector);
            var map2 = right.Map(rightKeySelector);

            return
                map1.Reduce(
                    map2,
                    (a, b) => new ClipJoinStreamable<CompoundGroupKey<TKey, TJoinKey>, TLeft, TRight>
                                (a, b),
                    (g, c) => c);
        }

        /// <summary>
        /// Performs a group and apply operation on the stream.
        /// </summary>
        public static IStreamable<TOuterKey, TResult> GroupApply<TOuterKey, TPayload, TInnerKey, TBind, TResult>(
            this IStreamable<TOuterKey, TPayload> source,
            Expression<Func<TPayload, TInnerKey>> keySelector,
            Func<IStreamable<CompoundGroupKey<TOuterKey, TInnerKey>, TPayload>, IStreamable<CompoundGroupKey<TOuterKey, TInnerKey>, TBind>> applyFunc,
            Expression<Func<GroupSelectorInput<TInnerKey>, TBind, TResult>> resultSelector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(keySelector, nameof(keySelector));

            return source.Map(keySelector).Reduce(applyFunc, resultSelector);
        }

        /// <summary>
        /// Performs a group and apply operation on the stream.
        /// </summary>
        public static IStreamable<TOuterKey, TResult> GroupApply<TOuterKey, TPayload, TInnerKey, TResult>(
            this IStreamable<TOuterKey, TPayload> source,
            Expression<Func<TPayload, TInnerKey>> keySelector,
            Func<IStreamable<CompoundGroupKey<TOuterKey, TInnerKey>, TPayload>, IStreamable<CompoundGroupKey<TOuterKey, TInnerKey>, TResult>> applyFunc)
            => source.GroupApply(keySelector, applyFunc, (e1, e2) => e2);

        /// <summary>
        /// Needed to make the comprehension syntax happy. Consider using GroupApply instead.
        /// </summary>
        public static IMapDefinition<TOuterKey, TPayload, TPayload, TInnerKey, TPayload> GroupBy<TOuterKey, TPayload, TInnerKey>(
            this IStreamable<TOuterKey, TPayload> source,
            Expression<Func<TPayload, TInnerKey>> keySelector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(keySelector, nameof(keySelector));

            return new MapDefinition<TOuterKey, TPayload, TPayload, TInnerKey, TPayload>(source, null, (a, b) => a, keySelector);
        }

        /// <summary>
        /// This is here to make the comprehension syntax happy. Consider using GroupApply instead.
        /// </summary>
        public static IStreamable<TOuterKey, TOutput> SelectMany<TOuterKey, TInnerKey, TPayload, TResult, TBind, TOutput>(
            this IMapDefinition<TOuterKey, TPayload, TPayload, TInnerKey, TResult> groupDefinition,
            Func<IStreamable<CompoundGroupKey<TOuterKey, TInnerKey>, TResult>, IStreamable<CompoundGroupKey<TOuterKey, TInnerKey>, TBind>> apply,
            Expression<Func<GroupSelectorInput<TInnerKey>, TBind, TOutput>> resultSelector)
        {
            Invariant.IsNotNull(groupDefinition, nameof(groupDefinition));
            Invariant.IsNotNull(apply, nameof(apply));
            Invariant.IsNotNull(resultSelector, nameof(resultSelector));

            return ((MapDefinition<TOuterKey, TPayload, TPayload, TInnerKey, TResult>)groupDefinition).CreateStreamable(apply, resultSelector);
        }

        /// <summary>
        /// Takes a single stream and breaks it into a partitioned stream.
        /// </summary>
        public static IStreamable<PartitionKey<TPartitionKey>, TPayload> Partition<TPartitionKey, TPayload>(
            this IStreamable<Empty, TPayload> stream,
            Expression<Func<TPayload, TPartitionKey>> constructor,
            long partitionLag = long.MinValue)
            => new PartitionStreamable<TPartitionKey, TPayload>(stream, constructor, partitionLag);
        #endregion

        #region MapReduce

        /// <summary>
        /// The Map phase of a single input map-only operation. Allows the specification of streaming logic on the mapper.
        /// </summary>
        public static IMapDefinition<TOuterKey, TMapInput, TMapInput, TOuterKey, TReduceInput> Map<TOuterKey, TMapInput, TReduceInput>(
            this IStreamable<TOuterKey, TMapInput> source,
            Func<IStreamable<TOuterKey, TMapInput>, IStreamable<TOuterKey, TReduceInput>> mapper)
            => Map(source, mapper, (Expression<Func<TReduceInput, TOuterKey>>)null);

        /// <summary>
        /// The Map phase of a single input map operation.
        /// </summary>
        internal static IMapDefinition<TOuterKey, TMapInput, TMapInput, TInnerKey, TMapInput> Map<TOuterKey, TMapInput, TInnerKey>(
            this IStreamable<TOuterKey, TMapInput> source,
            Expression<Func<TMapInput, TInnerKey>> keySelector)
            => Map(source, a => a, keySelector);

        /// <summary>
        /// The Map phase of a single input map/reduce operation. Allows the specification of streaming logic on the mapper,
        /// and a key selector for the next (reduce) stage.
        /// </summary>
        public static IMapDefinition<TOuterKey, TMapInput, TMapInput, TInnerKey, TReduceInput> Map<TOuterKey, TMapInput, TInnerKey, TReduceInput>(
            this IStreamable<TOuterKey, TMapInput> source,
            Func<IStreamable<TOuterKey, TMapInput>, IStreamable<TOuterKey, TReduceInput>> mapper,
            Expression<Func<TReduceInput, TInnerKey>> keySelector)
            => new MapDefinition<TOuterKey, TMapInput, TMapInput, TInnerKey, TReduceInput>
                (source, null, (a, b) => mapper(a), keySelector);

        /// <summary>
        /// Empty reducer. Follows a map operator in situations where the mapper is being used to parallelize a stateless query. It simply unmaps the input
        /// </summary>
        public static IStreamable<TOuterKey, TReduceInput> Reduce<TOuterKey, TMapInputLeft, TMapInputRight, TInnerKey, TReduceInput>(
            this IMapDefinition<TOuterKey, TMapInputLeft, TMapInputRight, TInnerKey, TReduceInput> mapDefinition)
            => ((MapDefinition<TOuterKey, TMapInputLeft, TMapInputRight, TInnerKey, TReduceInput>)mapDefinition).CreateStreamable<TReduceInput, TReduceInput>();

        /// <summary>
        /// Single-input reducer. Takes a mapped stream and applies a stream valued function (reducer) to each of the partitions. The result selector allows the
        /// reintroduction of the grouping key to the output in case it has been lost in the apply function.
        /// </summary>
        public static IStreamable<TOuterKey, TOutput> Reduce<TOuterKey, TMapInputLeft, TMapInputRight, TInnerKey, TReduceInput, TSelectorInput, TOutput>(
            this IMapDefinition<TOuterKey, TMapInputLeft, TMapInputRight, TInnerKey, TReduceInput> mapDefinition,
            Func<IStreamable<CompoundGroupKey<TOuterKey, TInnerKey>, TReduceInput>, IStreamable<CompoundGroupKey<TOuterKey, TInnerKey>, TSelectorInput>> reducer,
            Expression<Func<GroupSelectorInput<TInnerKey>, TSelectorInput, TOutput>> resultSelector)
            => ((MapDefinition<TOuterKey, TMapInputLeft, TMapInputRight, TInnerKey, TReduceInput>)mapDefinition).CreateStreamable(reducer, resultSelector);

        /// <summary>
        /// Two-input reducer. This reducer takes the result of 2 map operators (one can be implicit by using . ) and applies a 2 input stream valued function (reducer) to each
        /// partitioned stream pair. Note that the two mapped input stream must have the same mapping type. The result selector allows the
        /// reintroduction of the grouping key to the output in case it has been lost in the apply function.
        /// Additional parameter allows specifying whether reduce is asymmetric (multicast left side, spray right side)
        /// </summary>
        public static IStreamable<TOuterKey, TOutput> Reduce<TOuterKey, TMapInputLeft1, TMapInputRight1, TMapInputLeft2, TMapInputRight2, TInnerKey, TReduceInput1, TReduceInput2, TSelectorInput, TOutput>(
            this IMapDefinition<TOuterKey, TMapInputLeft1, TMapInputRight1, TInnerKey, TReduceInput1> mapDefinitionLeft,
            IMapDefinition<TOuterKey, TMapInputLeft2, TMapInputRight2, TInnerKey, TReduceInput2> mapDefinitionRight,
            Func<IStreamable<CompoundGroupKey<TOuterKey, TInnerKey>, TReduceInput1>, IStreamable<CompoundGroupKey<TOuterKey, TInnerKey>, TReduceInput2>, IStreamable<CompoundGroupKey<TOuterKey, TInnerKey>, TSelectorInput>> reducer,
            Expression<Func<GroupSelectorInput<TInnerKey>, TSelectorInput, TOutput>> resultSelector,
            OperationalHint reduceOptions = OperationalHint.None)
            => ((MapDefinition<TOuterKey, TMapInputLeft1, TMapInputRight1, TInnerKey, TReduceInput1>)mapDefinitionLeft).CreateStreamable(mapDefinitionRight, reducer, resultSelector, reduceOptions);
        #endregion

        #region Aggregates

        /// <summary>
        /// Computes a time-sensitive count aggregate using snapshot semantics.
        /// </summary>
        public static IStreamable<TKey, ulong> Count<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source)
            => Aggregate(source, w => w.Count());

        /// <summary>
        /// Computes a time-sensitive count aggregate of the non-null values using snapshot semantics.
        /// </summary>
        public static IStreamable<TKey, ulong> CountNotNull<TKey, TPayload, TValue>(
            this IStreamable<TKey, TPayload> source,
            Expression<Func<TPayload, TValue>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.CountNotNull(selector));
        }

        /// <summary>
        /// Computes a time-sensitive minimum aggregate using snapshot semantics.
        /// </summary>
        public static IStreamable<TKey, TPayload> Min<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source)
            => source.Aggregate(w => w.Min(v => v));

        /// <summary>
        /// Computes a time-sensitive minimum aggregate using snapshot semantics.
        /// </summary>
        public static IStreamable<TKey, T> Min<TKey, TPayload, T>(
            this IStreamable<TKey, TPayload> source,
            Expression<Func<TPayload, T>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Min(selector));
        }

        /// <summary>
        /// Computes a time-sensitive minimum aggregate using snapshot semantics.
        /// </summary>
        public static IStreamable<TKey, TPayload> Min<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source,
            Expression<Comparison<TPayload>> comparer)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(comparer, nameof(comparer));
            return source.Aggregate(w => w.Min(v => v, new ComparerExpression<TPayload>(comparer)));
        }

        /// <summary>
        /// Computes a time-sensitive minimum aggregate using snapshot semantics with the provided ordering comparer.
        /// </summary>
        public static IStreamable<TKey, T> Min<TKey, TPayload, T>(
            this IStreamable<TKey, TPayload> source,
            Expression<Func<TPayload, T>> selector,
            Expression<Comparison<T>> comparer)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            Invariant.IsNotNull(comparer, nameof(comparer));
            return source.Aggregate(w => w.Min(selector, new ComparerExpression<T>(comparer)));
        }

        /// <summary>
        /// Computes a time-sensitive maximum aggregate using snapshot semantics.
        /// </summary>
        public static IStreamable<TKey, TPayload> Max<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source)
            => source.Aggregate(w => w.Max(v => v));

        /// <summary>
        /// Computes a time-sensitive maximum aggregate using snapshot semantics.
        /// </summary>
        public static IStreamable<TKey, T> Max<TKey, TPayload, T>(
            this IStreamable<TKey, TPayload> source,
            Expression<Func<TPayload, T>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Max(selector));
        }

        /// <summary>
        /// Computes a time-sensitive maximum aggregate using snapshot semantics.
        /// </summary>
        public static IStreamable<TKey, TPayload> Max<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source,
            Expression<Comparison<TPayload>> comparer)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(comparer, nameof(comparer));
            return source.Aggregate(w => w.Max(v => v, new ComparerExpression<TPayload>(comparer)));
        }

        /// <summary>
        /// Computes a time-sensitive maximum aggregate using snapshot semantics with the provided ordering comparer.
        /// </summary>
        public static IStreamable<TKey, T> Max<TKey, TPayload, T>(
            this IStreamable<TKey, TPayload> source,
            Expression<Func<TPayload, T>> selector,
            Expression<Comparison<T>> comparer)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            Invariant.IsNotNull(comparer, nameof(comparer));
            return source.Aggregate(w => w.Max(selector, new ComparerExpression<T>(comparer)));
        }

        /// <summary>
        /// Computes a time-sensitive top-k aggregate using snapshot semantics based on a key selector.
        /// </summary>
        public static IStreamable<TKey, List<RankedEvent<TPayload>>> TopK<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source,
            int k)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsPositive(k, nameof(k));
            return source.Aggregate(w => w.TopK(v => v, k));
        }

        /// <summary>
        /// Computes a time-sensitive top-k aggregate using snapshot semantics based on a key selector.
        /// </summary>
        public static IStreamable<TKey, List<RankedEvent<TPayload>>> TopK<TKey, TPayload, T>(
            this IStreamable<TKey, TPayload> source,
            Expression<Func<TPayload, T>> selector,
            int k)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            Invariant.IsPositive(k, nameof(k));
            return source.Aggregate(w => w.TopK(selector, k));
        }

        /// <summary>
        /// Computes a time-sensitive top-k aggregate using snapshot semantics based on a key selector.
        /// </summary>
        public static IStreamable<TKey, List<RankedEvent<TPayload>>> TopK<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source,
            Expression<Comparison<TPayload>> comparer,
            int k)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(comparer, nameof(comparer));
            Invariant.IsPositive(k, nameof(k));
            return source.Aggregate(w => w.TopK(v => v, new ComparerExpression<TPayload>(comparer), k));
        }

        /// <summary>
        /// Computes a time-sensitive top-k aggregate using snapshot semantics based on a key selector with the provided ordering comparer.
        /// </summary>
        public static IStreamable<TKey, List<RankedEvent<TPayload>>> TopK<TKey, TPayload, T>(
            this IStreamable<TKey, TPayload> source,
            Expression<Func<TPayload, T>> selector,
            Expression<Comparison<T>> comparer,
            int k)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            Invariant.IsNotNull(comparer, nameof(comparer));
            Invariant.IsPositive(k, nameof(k));
            return source.Aggregate(w => w.TopK(selector, new ComparerExpression<T>(comparer), k));
        }
        #endregion

        #region CompoundAggregates

        /// <summary>
        /// Applies an aggregate to snapshot windows on the input stream.
        /// </summary>
        public static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState, TOutput>(
            this IStreamable<TKey, TInput> source,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState, TOutput>> aggregate)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate, nameof(aggregate));
            return new SnapshotWindowStreamable<TKey, TInput, TState, TOutput>(source, aggregate(new Window<TKey, TInput>(source.Properties)));
        }

        /// <summary>
        /// Applies an aggregate to snapshot windows on the two merged input streams.
        /// </summary>
        internal static IStreamable<TKey, TOutput> Aggregate<TKey, TLeft, TRight, TState, TOutput>(
            this IStreamable<TKey, TLeft> left,
            IStreamable<TKey, TRight> right,
            Func<Window<TKey, TLeft>, Window<TKey, TRight>, IBinaryAggregate<TLeft, TRight, TState, TOutput>> aggregate)
        {
            Invariant.IsNotNull(left, nameof(left));
            Invariant.IsNotNull(right, nameof(right));
            Invariant.IsNotNull(aggregate, nameof(aggregate));

            var source = left.Select(o => new DiscriminatedUnion<TLeft, TRight> { Left = o, isLeft = true })
                .Union(right.Select(o => new DiscriminatedUnion<TLeft, TRight> { Right = o, isLeft = false }));

            return new SnapshotWindowStreamable<TKey, DiscriminatedUnion<TLeft, TRight>, TState, TOutput>(source, new DiscriminatedAggregate<TLeft, TRight, TState, TOutput>(aggregate(new Window<TKey, TLeft>(left.Properties), new Window<TKey, TRight>(right.Properties))));
        }

        /// <summary>
        /// Applies an aggregate to snapshot windows on the input stream.
        /// </summary>
        internal static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState, TOutput>(
            this IStreamable<TKey, TInput> source,
            IAggregate<TInput, TState, TOutput> aggregate)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate, nameof(aggregate));
            return new SnapshotWindowStreamable<TKey, TInput, TState, TOutput>(source, aggregate);
        }

        /// <summary>
        /// Applies an aggregate to snapshot windows on the two merged input streams.
        /// </summary>
        internal static IStreamable<TKey, TOutput> Aggregate<TKey, TLeft, TRight, TState, TOutput>(
            this IStreamable<TKey, TLeft> left,
            IStreamable<TKey, TRight> right,
            IBinaryAggregate<TLeft, TRight, TState, TOutput> aggregate)
        {
            Invariant.IsNotNull(left, nameof(left));
            Invariant.IsNotNull(right, nameof(right));
            Invariant.IsNotNull(aggregate, nameof(aggregate));

            var source = left.Select(o => new DiscriminatedUnion<TLeft, TRight> { Left = o, isLeft = true })
                .Union(right.Select(o => new DiscriminatedUnion<TLeft, TRight> { Right = o, isLeft = false }));

            return new SnapshotWindowStreamable<TKey, DiscriminatedUnion<TLeft, TRight>, TState, TOutput>(source, new DiscriminatedAggregate<TLeft, TRight, TState, TOutput>(aggregate));
        }
        #endregion

        /// <summary>
        /// Macro to perform a left-outer-join operation.
        /// </summary>
        /// <typeparam name="TKey">Type of (mapping) key in the stream</typeparam>
        /// <typeparam name="TLeft">Type of left input payload in the stream</typeparam>
        /// <typeparam name="TRight">Type of right input payload in the stream</typeparam>
        /// <typeparam name="TJoinKey">Type of join key for the join</typeparam>
        /// <typeparam name="TResult">Type of result payload in the stream</typeparam>
        /// <param name="left">Left input stream</param>
        /// <param name="right">Right input stream</param>
        /// <param name="leftKeySelector">Selector for the left-side join key</param>
        /// <param name="rightKeySelector">Selector for the right-side join key</param>
        /// <param name="outerResultSelector">Selector for the result for non-joining tuples</param>
        /// <param name="innerResultSelector">Selector for the result for joining tuples</param>
        /// <returns>Result (output) stream</returns>
        public static IStreamable<TKey, TResult> LeftOuterJoin<TKey, TLeft, TRight, TJoinKey, TResult>(
            this IStreamable<TKey, TLeft> left,
            IStreamable<TKey, TRight> right,
            Expression<Func<TLeft, TJoinKey>> leftKeySelector,
            Expression<Func<TRight, TJoinKey>> rightKeySelector,
            Expression<Func<TLeft, TResult>> outerResultSelector,
            Expression<Func<TLeft, TRight, TResult>> innerResultSelector)
        {
            Invariant.IsNotNull(left, nameof(left));
            Invariant.IsNotNull(right, nameof(right));

            return left.Multicast(right, (l_mc, r_mc) =>
            {
                var lasj = l_mc.WhereNotExists(r_mc, leftKeySelector, rightKeySelector).Select(outerResultSelector);
                var innerJoin = l_mc.Join(r_mc, leftKeySelector, rightKeySelector, innerResultSelector);
                return lasj.Union(innerJoin);
            });
        }

        /// <summary>
        /// Macro to perform a full-outer-join operation.
        /// </summary>
        /// <typeparam name="TKey">Type of (mapping) key in the stream</typeparam>
        /// <typeparam name="TLeft">Type of left input payload in the stream</typeparam>
        /// <typeparam name="TRight">Type of right input payload in the stream</typeparam>
        /// <typeparam name="TJoinKey">Type of join key for the join</typeparam>
        /// <typeparam name="TResult">Type of result payload in the stream</typeparam>
        /// <param name="left">Left input stream</param>
        /// <param name="right">Right input stream</param>
        /// <param name="leftKeySelector">Selector for the left-side join key</param>
        /// <param name="rightKeySelector">Selector for the right-side join key</param>
        /// <param name="leftResultSelector">Selector for the result for the left non-joining tuples</param>
        /// <param name="rightResultSelector">Selector for the result for the right non-joining tuples</param>
        /// <param name="innerResultSelector">Selector for the result for joining tuples</param>
        /// <returns>Result (output) stream</returns>
        public static IStreamable<TKey, TResult> FullOuterJoin<TKey, TLeft, TRight, TJoinKey, TResult>(
            this IStreamable<TKey, TLeft> left,
            IStreamable<TKey, TRight> right,
            Expression<Func<TLeft, TJoinKey>> leftKeySelector,
            Expression<Func<TRight, TJoinKey>> rightKeySelector,
            Expression<Func<TLeft, TResult>> leftResultSelector,
            Expression<Func<TRight, TResult>> rightResultSelector,
            Expression<Func<TLeft, TRight, TResult>> innerResultSelector)
        {
            Invariant.IsNotNull(left, nameof(left));
            Invariant.IsNotNull(right, nameof(right));

            return left.Multicast(right, (l_mc, r_mc) =>
            {
                var outerLeft = l_mc.WhereNotExists(r_mc, leftKeySelector, rightKeySelector).Select(leftResultSelector);
                var outerRight = r_mc.WhereNotExists(l_mc, rightKeySelector, leftKeySelector).Select(rightResultSelector);
                var innerJoin = l_mc.Join(r_mc, leftKeySelector, rightKeySelector, innerResultSelector);
                return outerLeft.Union(innerJoin).Union(outerRight);
            });
        }

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TLeft"></typeparam>
        /// <typeparam name="TRight"></typeparam>
        /// <typeparam name="TJoinKey"></typeparam>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="leftKeySelector"></param>
        /// <param name="rightKeySelector"></param>
        /// <param name="postPredicate"></param>
        /// <returns></returns>
        public static IStreamable<TKey, TLeft> WhereNotExists<TKey, TLeft, TRight, TJoinKey>(
            this IStreamable<TKey, TLeft> left,
            IStreamable<TKey, TRight> right,
            Expression<Func<TLeft, TJoinKey>> leftKeySelector,
            Expression<Func<TRight, TJoinKey>> rightKeySelector,
            Expression<Func<TLeft, TRight, bool>> postPredicate)
        {
            Invariant.IsNotNull(left, nameof(left));
            Invariant.IsNotNull(right, nameof(right));

            var e1 = Expression.Parameter(typeof(StructTuple<TLeft, TRight>), "e1");
            var postPredicateTransformed = Expression.Lambda<Func<StructTuple<TLeft, TRight>, bool>>
                (
                    Expression.Invoke(postPredicate, Expression.Field(e1, "Item1"), Expression.Field(e1, "Item2")),
                    new ParameterExpression[] { e1 });

            var leftMC = left.Multicast(2);

            return
               leftMC[0].WhereNotExists(
                leftMC[1]
                    .Join(right, leftKeySelector, rightKeySelector, (l, r) => new StructTuple<TLeft, TRight> { Item1 = l, Item2 = r })
                    .Where(postPredicateTransformed).Select(e => e.Item1),
                l => l, r => r)
               ;
        }

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TLeft"></typeparam>
        /// <typeparam name="TRight"></typeparam>
        /// <typeparam name="TJoinKey"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="leftKeySelector"></param>
        /// <param name="rightKeySelector"></param>
        /// <param name="postPredicate"></param>
        /// <param name="outerResultSelector"></param>
        /// <param name="innerResultSelector"></param>
        /// <returns></returns>
        public static IStreamable<TKey, TResult> LeftOuterJoin<TKey, TLeft, TRight, TJoinKey, TResult>(
            this IStreamable<TKey, TLeft> left,
            IStreamable<TKey, TRight> right,
            Expression<Func<TLeft, TJoinKey>> leftKeySelector,
            Expression<Func<TRight, TJoinKey>> rightKeySelector,
            Expression<Func<TLeft, TRight, bool>> postPredicate,
            Expression<Func<TLeft, TResult>> outerResultSelector,
            Expression<Func<TLeft, TRight, TResult>> innerResultSelector)
        {
            Invariant.IsNotNull(left, nameof(left));
            Invariant.IsNotNull(right, nameof(right));

            Expression<Func<StructTuple<TLeft, TRight>, bool>> postPredicateTemplate =
                st => CallInliner.Call(postPredicate, st.Item1, st.Item2);
            var postPredicateTransformed = postPredicateTemplate.InlineCalls();

            Expression<Func<StructTuple<TLeft, TRight>, TResult>> innerResultSelectorTemplate =
                st => CallInliner.Call(innerResultSelector, st.Item1, st.Item2);
            var innerResultSelectorTransformed = innerResultSelectorTemplate.InlineCalls();

            return left.Multicast(right, (l_mc, r_mc) =>
            {
                var lasj = l_mc.WhereNotExists(r_mc, leftKeySelector, rightKeySelector, postPredicate).Select(outerResultSelector);
                var innerJoin = l_mc.Join(r_mc, leftKeySelector, rightKeySelector, (l, r) => new StructTuple<TLeft, TRight> { Item1 = l, Item2 = r }).Where(postPredicateTransformed).Select(innerResultSelectorTransformed);
                return lasj.Union(innerJoin);
            });
        }

        /// <summary>
        /// Convert a stream of start and end sessions to a single per-session stream, where we care about the data in
        /// the end session. Sessions are output as point events at the timestamp of the end session.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TPayload"></typeparam>
        /// <typeparam name="TSessionKey"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="sessionStream"></param>
        /// <param name="sessionStartSelector"></param>
        /// <param name="sessionEndSelector"></param>
        /// <param name="sessionKey"></param>
        /// <param name="sessionResultSelector"></param>
        /// <returns></returns>
        public static IStreamable<TKey, TResult> Sessionize<TKey, TPayload, TSessionKey, TResult>(
            this IStreamable<TKey, TPayload> sessionStream,
            Expression<Func<TPayload, bool>> sessionStartSelector,
            Expression<Func<TPayload, bool>> sessionEndSelector,
            Expression<Func<TPayload, TSessionKey>> sessionKey,
            Expression<Func<TPayload, TPayload, TResult>> sessionResultSelector)
        {
            Invariant.IsNotNull(sessionStream, nameof(sessionStream));
            return
                sessionStream.GroupApply(
                    sessionKey,
                    str =>
                        str.Where(sessionStartSelector).AlterEventDuration(StreamEvent.InfinitySyncTime)
                        .Multicast(
                            str.Where(sessionEndSelector).AlterEventDuration(1),
                            (ss, se) =>
                                ss.ClipEventDuration(se)
                                .ShiftEventLifetime(1)
                                .Join(se, sessionResultSelector)));
        }

        /// <summary>
        /// Filter out any duplicate entries at each snapshot
        /// </summary>
        /// <typeparam name="TKey">Type of (mapping) key in the stream</typeparam>
        /// <typeparam name="TPayload">Type of result stream</typeparam>
        /// <param name="source"></param>
        /// <returns>A stream where all elements within each timestamp have only distinct payloads returned</returns>
        public static IStreamable<TKey, TPayload> Distinct<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source)
        {
            Invariant.IsNotNull(source, nameof(source));
            return source.GroupApply(e => e, apply => apply.Count(), (g, c) => g.Key);
        }

        /// <summary>
        /// Removes partial duplicates per timestamp.
        /// If two events match by the given expression and their timestamps, they are considered duplicates.
        /// </summary>
        /// <typeparam name="TKey">Type of (mapping) key in the stream</typeparam>
        /// <typeparam name="TInput">Type of input stream</typeparam>
        /// <typeparam name="TResult">Type of result stream</typeparam>
        /// <param name="source">Input stream.</param>
        /// <param name="selector">Expression that defines when two events are considered duplicates.
        /// If the expression evaluates to different values for two events, they are considered to be distinct.</param>
        /// <returns>For each distinct input timestamp, one result event with a payload based on the given expression.</returns>
        public static IStreamable<TKey, TResult> Distinct<TKey, TInput, TResult>(
            this IStreamable<TKey, TInput> source,
            Expression<Func<TInput, TResult>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.GroupApply(selector, apply => apply.Aggregate(w => w.Count()), (g, c) => g.Key);
        }

        /** following are internal for now **/

        internal static IStreamable<TKey, ulong> ScaledOutCount<TKey, TPayload>(this IStreamable<TKey, TPayload> source)
            => source.Map(g => g.Count()).Reduce().Sum(e => e);

#if DEBUG
        /// <summary>
        /// Create a stream that reports all known formatting errors in the streaming data.
        /// </summary>
        public static IStreamable<TKey, TPayload> Validate<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source)
        {
            Invariant.IsNotNull(source, nameof(source));
            return new VerifyPropertiesStreamable<TKey, TPayload>(source);
        }
#endif
    }
}
