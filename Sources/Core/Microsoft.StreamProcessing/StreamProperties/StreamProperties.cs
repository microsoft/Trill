// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Aggregates;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Interface for an intermediate state when setting properties on a stream.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public interface IPropertySetter<TMapID, TResult>
    {
    }

    internal class PropertySetter<TMapID, TResult> : IPropertySetter<TMapID, TResult>
    {
        private readonly IStreamable<TMapID, TResult> source;

        public PropertySetter(IStreamable<TMapID, TResult> source) => this.source = source;

        public IStreamable<TMapID, TResult> GetStreamable() => this.source;
    }

    /// <summary>
    /// Class that holds the set of stream properties for the stream
    /// </summary>
    /// <typeparam name="TKey">Type of mapping key for the stream</typeparam>
    /// <typeparam name="TPayload">Type of payload for the stream</typeparam>
    public class StreamProperties<TKey, TPayload>
    {
        internal static StreamProperties<TKey, TPayload> Default
            => new StreamProperties<TKey, TPayload>(
                Config.ForceRowBasedExecution ? false : true,
                false, null,
                false, null, null,
                false, false,
                false, false,
                EqualityComparerExpression<TKey>.Default,
                EqualityComparerExpression<TPayload>.Default,
                null,
                null,
                new Dictionary<Expression, object>(),
                new Dictionary<Expression, Guid?>(),
                null);

        internal static StreamProperties<TKey, TPayload> DefaultIngress(LambdaExpression startEdgeSelector, LambdaExpression endEdgeSelector)
        {
            var startEdge = startEdgeSelector.Body;
            var endEdge = endEdgeSelector.ReplaceParametersInBody(startEdgeSelector.Parameters[0]);
            Expression<Func<long, long>> inf = (o) => StreamEvent.InfinitySyncTime;

            long? duration = null;
            if (endEdge is ConstantExpression c && c.Value is long l && l == StreamEvent.InfinitySyncTime) duration = StreamEvent.InfinitySyncTime;
            else if (endEdge.ExpressionEquals(inf.Body)) duration = StreamEvent.InfinitySyncTime;
            else if (endEdge is BinaryExpression b && (b.NodeType == ExpressionType.Add || b.NodeType == ExpressionType.AddChecked))
            {
                if (b.Left.ExpressionEquals(startEdge) && b.Right is ConstantExpression cr && cr.Value is long lr) duration = lr;
                if (b.Right.ExpressionEquals(startEdge) && b.Left is ConstantExpression cl && cl.Value is long ll) duration = ll;
            }

            return
                new StreamProperties<TKey, TPayload>(
                    Config.ForceRowBasedExecution ? false : true,
                    duration.HasValue, duration,
                    false, null, null,
                    false, false,
                    false, false,
                    EqualityComparerExpression<TKey>.Default,
                    EqualityComparerExpression<TPayload>.Default,
                    null,
                    null,
                    new Dictionary<Expression, object>(),
                    new Dictionary<Expression, Guid?>(),
                    null);
        }

        private bool isColumnar;
        private Func<bool> predicate = () => true;

        /// <summary>
        /// Indicates whether the stream contains data in columnar payload format.
        /// </summary>
        internal bool IsColumnar
        {
            get
            {
                this.isColumnar = this.isColumnar && this.predicate();
                this.predicate = () => true;
                return this.isColumnar;
            }
            set
            {
                this.isColumnar = value;
                this.predicate = () => true;
            }
        }

        /// <summary>
        /// Query container
        /// </summary>
        public QueryContainer QueryContainer;

        /// <summary>
        /// Indicates whether, for all interval events in the stream, the end timestamps of the intervals are non-decreasing.
        /// </summary>
        public bool IsConstantDuration;

        /// <summary>
        /// Indicates for a constant-duration stream, the length of the constant duration.
        /// </summary>
        public long? ConstantDurationLength;

        /// <summary>
        /// Indicates whether the stream consists only of start edges.
        /// </summary>
        public bool IsStartEdgeOnly => this.IsConstantDuration && this.ConstantDurationLength == StreamEvent.InfinitySyncTime;

        /// <summary>
        /// Indicates whether the stream consists only of events with tumbling lifetimes.
        /// </summary>
        public bool IsTumbling => this.IsConstantDuration && this.ConstantDurationLength.HasValue
            && (this.ConstantDurationLength.Value == 1 || (this.IsConstantHop && this.ConstantHopLength.HasValue && this.ConstantHopLength.Value >= this.ConstantDurationLength.Value));

        /// <summary>
        /// Indicates that sync-times of events either stay the same or increment (hop) by a fixed amount.
        /// </summary>
        public bool IsConstantHop;

        /// <summary>
        /// Indicates for a constant-hop stream, the length of the constant hop.
        /// </summary>
        public long? ConstantHopLength;

        /// <summary>
        /// Indicates for a constant-duration stream, the offset (from 0) of the sync-time hops.
        /// </summary>
        public long? ConstantHopOffset;

        /// <summary>
        /// Indicates whether the stream consists only of start edges.
        /// </summary>
        internal bool IsIntervalFree;

        /// <summary>
        /// Indicates whether each group of stream is free of multiple events with the same sync-time
        /// </summary>
        internal bool IsSyncTimeSimultaneityFree;

        /// <summary>
        /// Indicates whether the stream is free of overlapping events (i.e., it is a signal).
        /// </summary>
        internal bool IsEventOverlappingFree;

        /// <summary>
        /// Indicates whether the stream is sorted on a per-snapshot basis
        /// </summary>
        private bool IsSnapshotSorted;

        /// <summary>
        /// Comparer for current key
        /// </summary>
        public IEqualityComparerExpression<TKey> KeyEqualityComparer;

        /// <summary>
        /// Comparer for current payload
        /// </summary>
        public IEqualityComparerExpression<TPayload> PayloadEqualityComparer;

        /// <summary>
        /// Comparer for sort-ordering of keys
        /// </summary>
        internal IComparerExpression<TKey> KeyComparer;

        /// <summary>
        /// Comparer for sort-ordering of payloads
        /// </summary>
        internal IComparerExpression<TPayload> PayloadComparer;

        /// <summary>
        /// Equality comparers for possible key selectors (selector -> IECE)
        /// </summary>
        private readonly Dictionary<Expression, object> EqualityComparerSelectorMap;

        /// <summary>
        /// Selectors that identify what the data is sorted by (could be more than one)
        /// (selector -> packing Guid)
        /// </summary>
        private Dictionary<Expression, Guid?> SortSelectorMap;

        internal bool IsMulticore;

        internal StreamProperties(
            bool isColumnar,
            bool isConstantDuration,
            long? constantDurationLength,
            bool isConstantHop,
            long? constantHopLength,
            long? constantHopOffset,
            bool isIntervalFree,
            bool isSyncTimeSimultaneityFree,
            bool isSnapshotSorted,
            bool isEventOverlappingFree,
            IEqualityComparerExpression<TKey> keyEqualityComparer,
            IEqualityComparerExpression<TPayload> payloadEqualityComparer,
            IComparerExpression<TKey> keyComparer,
            IComparerExpression<TPayload> payloadComparer,
            Dictionary<Expression, object> equalityComparerSelectorMap,
            Dictionary<Expression, Guid?> sortSelectorMap,
            QueryContainer container)
        {
            this.IsColumnar = isColumnar;
            this.IsConstantDuration = isConstantDuration;
            this.ConstantDurationLength = constantDurationLength;
            this.IsConstantHop = isConstantHop;
            this.ConstantHopLength = constantHopLength;
            this.ConstantHopOffset = constantHopOffset;
            this.IsIntervalFree = isIntervalFree;
            this.IsSyncTimeSimultaneityFree = isSyncTimeSimultaneityFree;
            this.IsSnapshotSorted = isSnapshotSorted;
            this.IsEventOverlappingFree = isEventOverlappingFree;
            this.KeyEqualityComparer = keyEqualityComparer;
            this.PayloadEqualityComparer = payloadEqualityComparer;
            this.KeyComparer = keyComparer;
            this.PayloadComparer = payloadComparer;
            this.EqualityComparerSelectorMap = equalityComparerSelectorMap;
            this.SortSelectorMap = sortSelectorMap;
            this.IsMulticore = false;
            this.QueryContainer = container;
        }

        internal StreamProperties(StreamProperties<TKey, TPayload> that)
        {
            this.IsColumnar = that.IsColumnar;
            this.IsConstantDuration = that.IsConstantDuration;
            this.ConstantDurationLength = that.ConstantDurationLength;
            this.IsConstantHop = that.IsConstantHop;
            this.ConstantHopLength = that.ConstantHopLength;
            this.ConstantHopOffset = that.ConstantHopOffset;
            this.IsIntervalFree = that.IsIntervalFree;
            this.IsSyncTimeSimultaneityFree = that.IsSyncTimeSimultaneityFree;
            this.IsSnapshotSorted = that.IsSnapshotSorted;
            this.IsEventOverlappingFree = that.IsEventOverlappingFree;
            this.KeyEqualityComparer = that.KeyEqualityComparer;
            this.PayloadEqualityComparer = that.PayloadEqualityComparer;
            this.KeyComparer = that.KeyComparer;
            this.PayloadComparer = that.PayloadComparer;
            this.EqualityComparerSelectorMap = that.EqualityComparerSelectorMap;
            this.SortSelectorMap = that.SortSelectorMap;
            this.IsMulticore = that.IsMulticore;
            this.QueryContainer = that.QueryContainer;
        }

        internal StreamProperties<TKey, TPayload> ToDecrementable()
        {
            var result = Clone();
            result.IsConstantDuration = false;
            result.ConstantDurationLength = null;
            return result;
        }

        internal StreamProperties<TKey, TPayload> ToColumnar()
        {
            var result = Clone();
            result.IsColumnar = true;
            return result;
        }

        internal StreamProperties<TKey, TPayload> ToDelayedColumnar(Func<bool> predicate)
        {
            var result = CloneDelayed();
            result.predicate = predicate;
            return result;
        }

        internal StreamProperties<TKey, TPayload> ToRowBased()
        {
            var result = Clone();
            result.IsColumnar = false;
            return result;
        }

        internal StreamProperties<TKey, TPayload> ToConstantDuration(bool value, long? durationLength = null)
        {
            var result = Clone();
            result.IsConstantDuration = value;
            result.ConstantDurationLength = durationLength;
            result.IsIntervalFree = false;
            result.IsEventOverlappingFree = false;
            return result;
        }

        internal StreamProperties<TKey, TPayload> SetQueryContainer(QueryContainer container)
        {
            var result = Clone();
            result.QueryContainer = container;
            return result;
        }

        internal StreamProperties<TKey, TPayload> ToConstantHop(bool value, long? hopLength = null, long? hopOffset = null)
        {
            var result = Clone();
            result.IsConstantHop = value;
            result.ConstantHopLength = hopLength;
            result.ConstantHopOffset = hopOffset;
            result.IsSyncTimeSimultaneityFree = false;
            result.IsEventOverlappingFree = false;
            return result;
        }

        internal StreamProperties<TKey, TPayload> ToIntervalFree(bool value)
        {
            var result = Clone();
            result.IsIntervalFree = value;
            return result;
        }

        internal StreamProperties<TKey, TPayload> ToSyncTimeSimultaneityFree(bool value)
        {
            var result = Clone();
            result.IsSyncTimeSimultaneityFree = value;
            if (!value) result.IsEventOverlappingFree = false;
            return result;
        }

        internal StreamProperties<TKey, TPayload> ToEventOverlappingFree(bool value)
        {
            var result = Clone();
            result.IsEventOverlappingFree = value;
            if (value) result.IsSyncTimeSimultaneityFree = true;
            return result;
        }

        internal StreamProperties<TKey, TPayload> ToMulticore(bool value)
        {
            var result = Clone();
            result.IsMulticore = value;
            return result;
        }

        internal StreamProperties<TKey, TPayload> ToPayloadEqualityComparer(IEqualityComparerExpression<TPayload> comparer)
        {
            var result = Clone();
            result.PayloadEqualityComparer = comparer;
            return result;
        }

        internal StreamProperties<TKey, TPayload> ToPayloadComparer(IComparerExpression<TPayload> comparer)
        {
            var result = Clone();
            result.PayloadComparer = comparer;
            return result;
        }

        internal StreamProperties<TKey, TPayload> ToKeyEqualityComparer(IEqualityComparerExpression<TKey> comparer)
        {
            var result = Clone();
            result.KeyEqualityComparer = comparer;
            return result;
        }

        internal StreamProperties<TKey, TPayload> ToEqualityComparer<T>(Expression<Func<TPayload, T>> streamSelector, IEqualityComparerExpression<T> comparer)
        {
            var result = Clone();
            result.EqualityComparerSelectorMap.Add(streamSelector, comparer);
            return result;
        }

        internal StreamProperties<TKey, TPayload> ToSnapshotSorted(bool isSnapshotSorted, Guid? packingScheme = null)
        {
            if (isSnapshotSorted)
            {
                Expression<Func<TPayload, TPayload>> sortFieldSelector = (e => e);
                return ToSnapshotSorted(isSnapshotSorted, sortFieldSelector, packingScheme);
            }
            else
            {
                var result = Clone();
                result.IsSnapshotSorted = false;
                result.PayloadComparer = null;
                result.KeyComparer = null;
                result.SortSelectorMap.Clear();
                return result;
            }
        }

        internal StreamProperties<TKey, TPayload> ToSnapshotSorted<T>(
            bool isSnapshotSorted, Expression<Func<TPayload, T>> sortFieldsSelector, Guid? packingScheme)
        {
            Contract.Requires(isSnapshotSorted == true);
            Contract.Requires(sortFieldsSelector != null);

            var result = Clone();
            result.IsSnapshotSorted = isSnapshotSorted;

            // Remove pre-existing selector to replace with new version
            foreach (var kvp in result.SortSelectorMap)
            {
                if (kvp.Key.ExpressionEquals(sortFieldsSelector))
                {
                    result.SortSelectorMap.Remove(kvp.Key);
                    break;
                }
            }

            result.SortSelectorMap.Add(sortFieldsSelector, packingScheme);
            result.PayloadComparer = new ComparerExpression<TPayload>(Utility.CreateCompoundComparer(sortFieldsSelector, ComparerExpression<T>.Default.GetCompareExpr()));

            return result;
        }

        internal StreamProperties<TResultKey, TResultPayload> Derive<TResultKey, TResultPayload>(
            Func<IStreamable<TKey, TPayload>, IStreamable<TResultKey, TResultPayload>> selector)
        {
            if (selector == null) return this as StreamProperties<TResultKey, TResultPayload>;

            var a = new NullStreamable<TKey, TPayload>(this);
            var x = selector(a);
            return x.Properties;
        }

        internal StreamProperties<TResultKey, TResultPayload> Derive<TRightKey, TRightPayload, TResultKey, TResultPayload>(
            StreamProperties<TRightKey, TRightPayload> rightProperties,
            Func<IStreamable<TKey, TPayload>, IStreamable<TRightKey, TRightPayload>, IStreamable<TResultKey, TResultPayload>> selector)
        {
            Contract.Requires(selector != null);

            return selector(new NullStreamable<TKey, TPayload>(this), new NullStreamable<TRightKey, TRightPayload>(rightProperties)).Properties;
        }

        internal IEqualityComparerExpression<T2> FindEqualityComparer<T1, T2>(Expression<Func<T1, T2>> keySelector)
        {
            foreach (var kvp in this.EqualityComparerSelectorMap)
            {
                if (kvp.Key.ExpressionEquals(keySelector))
                {
                    return kvp.Value as IEqualityComparerExpression<T2>;
                }
            }
            return null;
        }

        internal IComparerExpression<T2> FindComparer<T1, T2>(Expression<Func<T1, T2>> keySelector)
        {
            foreach (var kvp in this.SortSelectorMap)
            {
                if (kvp.Key.ExpressionEquals(keySelector))
                {
                    return ComparerExpression<T2>.Default;
                }
            }

            return null;
        }

        internal IComparerExpression<TPayload> GetSprayComparerExpression<T1, T2>(Expression<Func<T1, T2>> keySelector)
        {
            foreach (var kvp in this.SortSelectorMap)
            {
                if (kvp.Key.ExpressionEquals(keySelector))
                {
                    var packSelector = kvp.Key as Expression<Func<TPayload, T2>>;
                    return new ComparerExpression<TPayload>
                        (Utility.CreateCompoundComparer(packSelector, ComparerExpression<T2>.Default.GetCompareExpr()));
                }
            }

            return null;
        }

        internal bool CanSpray<T1, T2>(Expression<Func<T1, T2>> keySelector)
        {
            foreach (var kvp in this.SortSelectorMap)
            {
                if (kvp.Key.ExpressionEquals(keySelector) && (kvp.Value.HasValue))
                {
                    return true;
                }
            }

            return false;
        }

        internal bool CanSpray<T1, T2, T3, T4, TPayload2>(StreamProperties<TKey, TPayload2> otherProperties, Expression<Func<T1, T2>> keySelector1, Expression<Func<T3, T4>> keySelector2)
        {
            foreach (var kvp1 in this.SortSelectorMap)
            {
                if (kvp1.Key.ExpressionEquals(keySelector1) && (kvp1.Value.HasValue))
                {
                    foreach (var kvp2 in otherProperties.SortSelectorMap)
                    {
                        if (kvp2.Key.ExpressionEquals(keySelector2) && (kvp2.Value.HasValue))
                        {
                            if (kvp1.Value == kvp2.Value) return true;
                        }
                    }
                }
            }

            return false;
        }

        /// <summary>
        /// Clone
        /// </summary>
        internal StreamProperties<TKey, TPayload> Clone()
            => new StreamProperties<TKey, TPayload>
                (this.IsColumnar, this.IsConstantDuration, this.ConstantDurationLength, this.IsConstantHop, this.ConstantHopLength, this.ConstantHopOffset, this.IsIntervalFree, this.IsSyncTimeSimultaneityFree, this.IsSnapshotSorted, this.IsEventOverlappingFree, this.KeyEqualityComparer, this.PayloadEqualityComparer, this.KeyComparer, this.PayloadComparer, this.EqualityComparerSelectorMap.Clone(), this.SortSelectorMap.Clone(), this.QueryContainer);

        /// <summary>
        /// Clone
        /// </summary>
        internal StreamProperties<TKey, TPayload> CloneDelayed()
            => new StreamProperties<TKey, TPayload>
                (this.isColumnar, this.IsConstantDuration, this.ConstantDurationLength, this.IsConstantHop, this.ConstantHopLength, this.ConstantHopOffset, this.IsIntervalFree, this.IsSyncTimeSimultaneityFree, this.IsSnapshotSorted, this.IsEventOverlappingFree, this.KeyEqualityComparer, this.PayloadEqualityComparer, this.KeyComparer, this.PayloadComparer, this.EqualityComparerSelectorMap.Clone(), this.SortSelectorMap.Clone(), this.QueryContainer);

        /// <summary>
        /// Clone
        /// </summary>
        internal StreamProperties<TNewKey, TPayload> CloneToNewKeyType<TNewKey>(
            IEqualityComparerExpression<TNewKey> newKeyEqualityComparer,
            IComparerExpression<TNewKey> newKeyComparer)
            => new StreamProperties<TNewKey, TPayload>
                (this.IsColumnar, this.IsConstantDuration, this.ConstantDurationLength, this.IsConstantHop, this.ConstantHopLength, this.ConstantHopOffset, this.IsIntervalFree, this.IsSyncTimeSimultaneityFree, this.IsSnapshotSorted, this.IsEventOverlappingFree,
                    newKeyEqualityComparer, this.PayloadEqualityComparer,
                    newKeyComparer, this.PayloadComparer, this.EqualityComparerSelectorMap.Clone(), this.SortSelectorMap.Clone(), this.QueryContainer);

        /// <summary>
        /// Clone
        /// </summary>
        internal StreamProperties<TKey, TNewPayload> CloneToNewPayloadType<TNewPayload>(
            IEqualityComparerExpression<TNewPayload> newPayloadEqualityComparer,
            IComparerExpression<TNewPayload> newPayloadComparer)
            => new StreamProperties<TKey, TNewPayload>
                (this.IsColumnar, this.IsConstantDuration, this.ConstantDurationLength, this.IsConstantHop, this.ConstantHopLength, this.ConstantHopOffset, this.IsIntervalFree, this.IsSyncTimeSimultaneityFree, this.IsSnapshotSorted, this.IsEventOverlappingFree, this.KeyEqualityComparer,
                    newPayloadEqualityComparer, this.KeyComparer,
                    newPayloadComparer, this.EqualityComparerSelectorMap.Clone(), this.SortSelectorMap.Clone(), this.QueryContainer);

        /// <summary>
        /// Where
        /// </summary>
        internal StreamProperties<TKey, TPayload> Where(Expression<Func<TPayload, bool>> predicate) => this;

        /// <summary>
        /// Select
        /// </summary>
        internal StreamProperties<TKey, TResult> Select<TResult>(LambdaExpression selector, bool hasStartEdge, bool hasKey, bool delayed = false)
        {
            var newEqualityComparerSelectorMap = new Dictionary<Expression, object>();
            var newPayloadEqualityComparer = EqualityComparerExpression<TResult>.Default;

            if (hasStartEdge)
            {
                foreach (var kvp in this.EqualityComparerSelectorMap)
                {
                    if (kvp.Key.ExpressionEquals(selector))
                    {
                        newPayloadEqualityComparer = kvp.Value as IEqualityComparerExpression<TResult>;
                    }
                }
            }

            var newSortSelectorMap = new Dictionary<Expression, Guid?>();
            IComparerExpression<TResult> newPayloadComparer = null;
            if (hasStartEdge)
            {
                foreach (var kvp in this.SortSelectorMap)
                {
                    if (kvp.Key.ExpressionEquals(selector))
                    {
                        newPayloadComparer = ComparerExpression<TResult>.Default;
                        Expression<Func<TResult, TResult>> fullSelector = (x => x);
                        newSortSelectorMap.Add(fullSelector, kvp.Value);
                    }
                }
            }

            var p = new StreamProperties<TKey, TResult>(this.isColumnar, this.IsConstantDuration, this.ConstantDurationLength, this.IsConstantHop, this.ConstantHopLength, this.ConstantHopOffset, this.IsIntervalFree, this.IsSyncTimeSimultaneityFree, this.IsSnapshotSorted, this.IsEventOverlappingFree, this.KeyEqualityComparer, newPayloadEqualityComparer, this.KeyComparer, newPayloadComparer,
                newEqualityComparerSelectorMap,
                newSortSelectorMap, this.QueryContainer);
            if (delayed) p.predicate = this.predicate;

            return p;
        }

        /// <summary>
        /// SelectMany
        /// </summary>
        internal StreamProperties<TKey, TResult> SelectMany<TResult>(LambdaExpression selector)
        {
            IComparerExpression<TResult> newPayloadComparer = null;

            return new StreamProperties<TKey, TResult>(
                this.IsColumnar, this.IsConstantDuration, this.ConstantDurationLength,
                this.IsConstantHop, this.ConstantHopLength, this.ConstantHopOffset,
                this.IsIntervalFree, false, this.IsSnapshotSorted, false,
                this.KeyEqualityComparer, EqualityComparerExpression<TResult>.Default,
                this.KeyComparer, newPayloadComparer,
                new Dictionary<Expression, object>(),
                new Dictionary<Expression, Guid?>(),
                this.QueryContainer);
        }

        /// <summary>
        /// Partitioning
        /// </summary>
        internal StreamProperties<PartitionKey<TPartitionKey>, TPayload> Partition<TPartitionKey>(Expression<Func<TPayload, TPartitionKey>> keySelector)
        {
            var innerEqualityComparer = EqualityComparerExpression<PartitionKey<TPartitionKey>>.Default;
            var innerKeyComparer = ComparerExpression<PartitionKey<TPartitionKey>>.Default;

            return new StreamProperties<PartitionKey<TPartitionKey>, TPayload>(this.IsColumnar, this.IsConstantDuration, this.ConstantDurationLength, this.IsConstantHop, this.ConstantHopLength, this.ConstantHopOffset, this.IsIntervalFree, this.IsSyncTimeSimultaneityFree, this.IsSnapshotSorted, this.IsEventOverlappingFree,
                innerEqualityComparer, this.PayloadEqualityComparer, innerKeyComparer, this.PayloadComparer, this.EqualityComparerSelectorMap.Clone(), this.SortSelectorMap.Clone(), this.QueryContainer);
        }

        /// <summary>
        /// First-level Group
        /// </summary>
        internal StreamProperties<TInnerKey, TPayload> Group<TInnerKey>(Expression<Func<TPayload, TInnerKey>> keySelector)
        {
            var innerEqualityComparer = FindEqualityComparer(keySelector);
            if (innerEqualityComparer == null) innerEqualityComparer = EqualityComparerExpression<TInnerKey>.Default;

            var innerKeyComparer = FindComparer(keySelector);

            return new StreamProperties<TInnerKey, TPayload>(this.IsColumnar, this.IsConstantDuration, this.ConstantDurationLength, this.IsConstantHop, this.ConstantHopLength, this.ConstantHopOffset, this.IsIntervalFree, this.IsSyncTimeSimultaneityFree, this.IsSnapshotSorted, this.IsEventOverlappingFree,
                innerEqualityComparer, this.PayloadEqualityComparer, innerKeyComparer, this.PayloadComparer, this.EqualityComparerSelectorMap.Clone(), this.SortSelectorMap.Clone(), this.QueryContainer);
        }

        /// <summary>
        /// Group
        /// </summary>
        internal StreamProperties<CompoundGroupKey<TKey, TInnerKey>, TPayload> GroupNested<TInnerKey>
            (Expression<Func<TPayload, TInnerKey>> keySelector)
        {
            var innerEqualityComparer = FindEqualityComparer(keySelector);

            var newKeyEqualityComparer =
                new CompoundGroupKeyEqualityComparer<TKey, TInnerKey>(this.KeyEqualityComparer, innerEqualityComparer);

            IComparerExpression<CompoundGroupKey<TKey, TInnerKey>> newKeyComparer = null;
            if ((this.KeyComparer != null) || (typeof(TKey) == typeof(Empty)))
            {
                var innerKeyComparer = FindComparer(keySelector);
                if (innerKeyComparer != null)
                {
                    newKeyComparer = new CompoundGroupKeyComparer<TKey, TInnerKey>(this.KeyComparer, innerKeyComparer);
                }
            }

            return new StreamProperties<CompoundGroupKey<TKey, TInnerKey>, TPayload>(
                this.IsColumnar
                    && (typeof(TInnerKey).GetTypeInfo().IsVisible || typeof(TInnerKey).IsAnonymousTypeName())
                    && (typeof(TPayload).GetTypeInfo().IsVisible || typeof(TPayload).IsAnonymousTypeName())
                    && (typeof(TKey).GetTypeInfo().IsVisible || typeof(TKey).IsAnonymousTypeName()), this.IsConstantDuration, this.ConstantDurationLength, this.IsConstantHop, this.ConstantHopLength, this.ConstantHopOffset, this.IsIntervalFree, this.IsSyncTimeSimultaneityFree, this.IsSnapshotSorted, this.IsEventOverlappingFree,
                newKeyEqualityComparer, this.PayloadEqualityComparer, newKeyComparer, this.PayloadComparer, this.EqualityComparerSelectorMap.Clone(), this.SortSelectorMap.Clone(), this.QueryContainer);
        }

        /// <summary>
        /// Union
        /// </summary>
        internal StreamProperties<TKey, TPayload> Union(StreamProperties<TKey, TPayload> right)
        {
            var result = Clone();

            // find intersection
            foreach (var kvp1 in this.EqualityComparerSelectorMap)
            {
                foreach (var kvp2 in right.EqualityComparerSelectorMap)
                {
                    if (kvp1.Key.ExpressionEquals(kvp2.Key) && kvp1.Value.EqualityExpressionEquals(kvp2.Value))
                    {
                        result.EqualityComparerSelectorMap.Add(kvp1.Key, kvp1.Value);
                    }
                }
            }

            // determine constant duration
            if (this.ConstantDurationLength != null && right.ConstantDurationLength != null && (this.ConstantDurationLength == right.ConstantDurationLength))
            {
                result.IsConstantDuration = true;
                result.ConstantDurationLength = this.ConstantDurationLength;
            }
            else
            {
                result.IsConstantDuration = false;
                result.ConstantDurationLength = null;
            }

            // determine constant hop
            if (this.ConstantHopLength != null && right.ConstantHopLength != null && (this.ConstantHopLength == right.ConstantHopLength) && (this.ConstantHopOffset == right.ConstantHopOffset))
            {
                result.IsConstantHop = true;
                result.ConstantHopLength = this.ConstantHopLength;
                result.ConstantHopOffset = this.ConstantHopOffset;
            }
            else
            {
                result.IsConstantHop = false;
                result.ConstantHopLength = null;
                result.ConstantHopOffset = null;
            }

            // Union loses simultaneity-free property
            result.IsSyncTimeSimultaneityFree = false;
            result.IsEventOverlappingFree = false;

            // Union loses sort-related properties
            result.IsSnapshotSorted = false;
            result.KeyComparer = null;
            result.PayloadComparer = null;
            result.SortSelectorMap = new Dictionary<Expression, Guid?>();

            // Union can be columnar only if both are
            result.IsColumnar = this.IsColumnar && right.IsColumnar;
            if (result.QueryContainer != right.QueryContainer)
            {
                throw new StreamProcessingException("Left and right inputs do not belong to the same query");
            }
            return result;
        }

        /// <summary>
        /// Join
        /// </summary>
        internal StreamProperties<TKey, TResult> Join<TRight, TResult>
            (StreamProperties<TKey, TRight> right, Expression<Func<TPayload, TRight, TResult>> selector)
        {
            var newKeyEqualityComparer = this.KeyEqualityComparer.ExpressionEquals(right.KeyEqualityComparer)
                ? this.KeyEqualityComparer
                : EqualityComparerExpression<TKey>.Default;

            var newKeyComparer =
                ((this.KeyComparer != null) && (right.KeyComparer != null)
                && (this.KeyComparer.ExpressionEquals(right.KeyComparer)))
                ? this.KeyComparer
                : null;

            bool constantDuration = false;
            long? constantDurationLength = null;
            bool constantHop = false;
            long? constantHopLength = null;
            long? constantHopOffset = null;

            // Joining two signals gives a signal
            bool isEventOverlappingFree = this.IsEventOverlappingFree && right.IsEventOverlappingFree;

            if (this.QueryContainer != right.QueryContainer)
            {
                throw new StreamProcessingException("Left and right inputs do not belong to the same query");
            }

            return
                new StreamProperties<TKey, TResult>(
                    this.IsColumnar && right.IsColumnar,
                    constantDuration,
                    constantDurationLength,
                    constantHop,
                    constantHopLength,
                    constantHopOffset,
                    false, false,
                    false,
                    isEventOverlappingFree,
                    newKeyEqualityComparer,
                    EqualityComparerExpression<TResult>.Default,
                    newKeyComparer,
                    null,
                    new Dictionary<Expression, object>(),
                    new Dictionary<Expression, Guid?>(), this.QueryContainer);
        }

        /// <summary>
        /// LASJ
        /// </summary>
        internal StreamProperties<TKey, TPayload> LASJ<TRight>(StreamProperties<TKey, TRight> right)
        {
            bool constantDuration = false;
            long? constantDurationLength = null;
            bool constantHop = false;
            long? constantHopLength = null;
            long? constantHopOffset = null;

            if (this.QueryContainer != right.QueryContainer)
            {
                throw new StreamProcessingException("Left and right inputs do not belong to the same query");
            }

            return
                new StreamProperties<TKey, TPayload>(
                    this.IsColumnar && right.IsColumnar,
                    constantDuration,
                    constantDurationLength,
                    constantHop,
                    constantHopLength,
                    constantHopOffset,
                    false, false,
                    false, this.IsEventOverlappingFree, this.KeyEqualityComparer, this.PayloadEqualityComparer, this.KeyComparer, this.PayloadComparer, this.EqualityComparerSelectorMap.Clone(), this.SortSelectorMap.Clone(), this.QueryContainer);
        }

        /// <summary>
        /// Clip
        /// </summary>
        internal StreamProperties<TKey, TPayload> Clip<TRight>(StreamProperties<TKey, TRight> right) => LASJ(right.ToDecrementable().ToSnapshotSorted(false));

        /// <summary>
        /// AlterLifetime
        /// </summary>
        internal StreamProperties<TKey, TPayload> AlterLifetime(LambdaExpression durationExpression)
        {
            // if we are not altering duration, there is no change
            if (durationExpression == null) return this;

            if (durationExpression.Body is ConstantExpression constant)
                return ToConstantDuration(true, (long)(constant.Value));

            return ToDecrementable().ToSnapshotSorted(false);
        }

        internal StreamProperties<TKey, TOutput> Snapshot<TState, TOutput>
            (IAggregate<TPayload, TState, TOutput> aggregate)
        {
            var newEqualityComparerSelectorMap = new Dictionary<Expression, object>();
            var newPayloadEqualityComparer = EqualityComparerExpression<TOutput>.Default;

            var newSortSelectorMap = new Dictionary<Expression, Guid?>();

            if ((this.IsConstantDuration) &&
                (this.ConstantDurationLength.HasValue) &&
                (this.IsConstantHop) &&
                (this.ConstantHopLength.HasValue) &&
                (this.ConstantHopLength.Value >= this.ConstantDurationLength.Value))
            {
                // tumbling window aggregate produces interval output stream
                return new StreamProperties<TKey, TOutput>(this.IsColumnar, this.IsConstantDuration, this.ConstantDurationLength, this.IsConstantHop, this.ConstantHopLength, this.ConstantHopOffset,
                    false, true,
                    false, true, this.KeyEqualityComparer,
                    newPayloadEqualityComparer,
                    null,
                    null,
                    newEqualityComparerSelectorMap,
                    newSortSelectorMap, this.QueryContainer);
            }
            else
            {
                return new StreamProperties<TKey, TOutput>(
                    this.IsColumnar,
                    false, null,
                    false, null, null,
                    true, true,
                    false, true, this.KeyEqualityComparer,
                    newPayloadEqualityComparer,
                    null,
                    null,
                    newEqualityComparerSelectorMap,
                    newSortSelectorMap, this.QueryContainer);
            }
        }

        internal StreamProperties<TKey, TPayload> PointAtEnd()
        {
            var temp = ToConstantDuration(true, 1);
            return temp;
        }
    }

    internal sealed class NullStreamable<TKey, TPayload> : Streamable<TKey, TPayload>
    {
        public NullStreamable(StreamProperties<TKey, TPayload> properties) : base(properties) { }

        public override IDisposable Subscribe(IStreamObserver<TKey, TPayload> observer)
            => throw new InvalidOperationException("Only for use in property derivation");
    }

    internal sealed class VerifyPropertiesStreamable<TKey, TPayload> : UnaryStreamable<TKey, TPayload, TPayload>
    {
        public VerifyPropertiesStreamable(IStreamable<TKey, TPayload> source) : base(source, source.Properties) { }

        internal override IStreamObserver<TKey, TPayload> CreatePipe(IStreamObserver<TKey, TPayload> observer)
            => new VerifyPropertiesPipe<TKey, TPayload>(this.properties, observer);

        protected override bool CanGenerateColumnar() => true;
    }

    [DataContract]
    internal sealed class VerifyPropertiesPipe<TKey, TPayload> : UnaryPipe<TKey, TPayload, TPayload>
    {
        private readonly Action<long, long> ConstantDurationValidator = NoOp;
        private readonly Action<long, long> ConstantHopValidator = NoOp;
        private readonly Action<long, long> IntervalFreeValidator = NoOp;
        private readonly Action<long, long, TKey> SyncTimeSimultaneityFreeValidator = NoOpWithKey;
        private readonly Action<long, TKey> SyncTimeValidator;
        private readonly Func<TKey, object> getPartitionKey;

        [DataMember]
        private long? constantDuration = null;
        [DataMember]
        private long? constantHopLength = null;
        [DataMember]
        private long? constantHopOffset = null;
        [DataMember]
        private readonly Dictionary<TKey, long> lastSyncTimeForSimultaneous = new Dictionary<TKey, long>();
        [DataMember]
        private long lastSeenTimestamp = StreamEvent.MinSyncTime;
        [DataMember]
        private readonly Dictionary<object, long> lastSeenTimestampPartitioned = new Dictionary<object, long>();

        [Obsolete("Used only by serialization. Do not call directly.")]
        public VerifyPropertiesPipe() { }

        public VerifyPropertiesPipe(StreamProperties<TKey, TPayload> properties, IStreamObserver<TKey, TPayload> observer)
            : base(new NullStreamable<TKey, TPayload>(properties), observer)
        {
            if (properties.IsConstantDuration)
            {
                this.ConstantDurationValidator = ConstantDurationValidation;
                this.constantDuration = properties.ConstantDurationLength;
            }

            if (properties.IsConstantHop)
            {
                this.ConstantHopValidator = ConstantHopValidation;
                this.constantHopLength = properties.ConstantHopLength;
                this.constantHopOffset = properties.ConstantHopOffset;
            }

            if (properties.IsIntervalFree)
            {
                this.IntervalFreeValidator = IntervalFreeValidation;
            }
            if (properties.IsSyncTimeSimultaneityFree)
            {
                this.SyncTimeSimultaneityFreeValidator = SyncTimeSimulteneityFreeValidation;
            }

            if (typeof(TKey).GetPartitionType() != null)
            {
                this.SyncTimeValidator = PartitionedValidation;
                this.getPartitionKey = GetPartitionExtractor<object, TKey>();
            }
            else
            {
                this.SyncTimeValidator = SimpleValidation;
            }
        }

        public override void OnNext(StreamMessage<TKey, TPayload> batch)
        {
            var col_bv = batch.bitvector.col;
            var col_vsync = batch.vsync.col;
            var col_vother = batch.vother.col;
            var col_vkey = batch.key.col;

            for (int i = 0; i < batch.Count; i++)
            {
                if ((col_bv[i >> 6] & (1L << (i & 0x3f))) != 0) continue;
                var s = col_vsync[i];
                var o = col_vother[i];
                var k = col_vkey[i];

                this.ConstantDurationValidator(s, o);
                this.ConstantHopValidator(s, o);
                this.IntervalFreeValidator(s, o);
                this.SyncTimeValidator(s, k);
                this.SyncTimeSimultaneityFreeValidator(s, o, k);
            }

            this.Observer.OnNext(batch);
        }

        private static void NoOp(long sync, long other) { }
        private static void NoOpWithKey(long sync, long other, TKey key) { }

        private static void IntervalFreeValidation(long sync, long other)
        {
            if (other > sync && other != StreamEvent.InfinitySyncTime) throw new StreamProcessingException("Interval found when stream properties indicate that none should be present."); // No intervals expected
        }

        private void SimpleValidation(long sync, TKey key)
        {
            if (this.lastSeenTimestamp > sync)
            {
                throw new InvalidOperationException(
                    "The operator AlterLifetime produced output out of sync-time order on an input event. The current internal sync time is " + this.lastSeenTimestamp.ToString(CultureInfo.InvariantCulture)
                    + ". The event's sync time is " + sync.ToString(CultureInfo.InvariantCulture) + ".");
            }
            this.lastSeenTimestamp = sync;
        }

        private void PartitionedValidation(long sync, TKey key)
        {
            object partitionKey = this.getPartitionKey(key);
            if (this.lastSeenTimestampPartitioned.TryGetValue(partitionKey, out long current))
            {
                if (current > sync)
                {
                    throw new InvalidOperationException(
                        "The operator AlterLifetime produced output out of sync-time order on an input event. The current internal sync time is " + current.ToString(CultureInfo.InvariantCulture)
                        + ". The event's sync time is " + sync.ToString(CultureInfo.InvariantCulture)
                        + ". The event's partition key is " + current.ToString(CultureInfo.InvariantCulture) + ".");
                }
                this.lastSeenTimestampPartitioned[partitionKey] = sync;
            }
            else
                this.lastSeenTimestampPartitioned.Add(partitionKey, sync);
        }

        private void ConstantDurationValidation(long sync, long other)
        {
            if (other < sync)
            {
                throw new StreamProcessingException(string.Format(
                    CultureInfo.InvariantCulture,
                    "Expected only start and interval events, but encountered an end edge event at time {0}.",
                    sync.ToString(CultureInfo.InvariantCulture)));
            }
            if (!this.constantDuration.HasValue) this.constantDuration = sync - other;

            if (this.constantDuration.Value == StreamEvent.InfinitySyncTime)
            {
                if (other != StreamEvent.InfinitySyncTime)
                {
                    throw new StreamProcessingException(string.Format(
                        CultureInfo.InvariantCulture,
                        "Expected only start edges, found an interval of length {0} at time {1}.",
                        (other - sync).ToString(CultureInfo.InvariantCulture),
                        sync.ToString(CultureInfo.InvariantCulture)));
                }
            }
            else if (other - sync != this.constantDuration.Value)
            {
                throw new StreamProcessingException(string.Format(
                    CultureInfo.InvariantCulture,
                    "Expected only intervals of time {0}, found an interval of length {1} at time {2}.", this.constantDuration.Value.ToString(CultureInfo.InvariantCulture),
                    (other - sync).ToString(CultureInfo.InvariantCulture),
                    sync.ToString(CultureInfo.InvariantCulture)));
            }
        }

        private void ConstantHopValidation(long sync, long other)
        {
            if (!this.constantHopOffset.HasValue)
            {
                this.constantHopOffset = sync;
            }
            else if (!this.constantHopLength.HasValue)
            {
                if (this.constantHopOffset.Value != sync) this.constantHopLength = sync - this.constantHopOffset.Value;
            }
            else
            {
                if ((sync - this.constantHopOffset.Value) % this.constantHopLength.Value != 0)
                {
                    throw new StreamProcessingException(string.Format(
                        CultureInfo.InvariantCulture,
                        "The sync time {0} does not correspond to the given hopping window properties of length {1} and offset {2}.",
                        sync.ToString(CultureInfo.InvariantCulture), this.constantHopLength.Value.ToString(CultureInfo.InvariantCulture), this.constantHopOffset.Value.ToString(CultureInfo.InvariantCulture)));
                }
            }
        }

        private void SyncTimeSimulteneityFreeValidation(long sync, long other, TKey key)
        {
            if (!this.lastSyncTimeForSimultaneous.ContainsKey(key))
            {
                this.lastSyncTimeForSimultaneous.Add(key, sync);
            }
            else if (this.lastSyncTimeForSimultaneous[key] == sync)
            {
                throw new StreamProcessingException(string.Format(
                    CultureInfo.InvariantCulture,
                    "Was not expecting to see events happening simultaneously, but found simultaneous events at time {0} for key {1}.",
                    sync.ToString(CultureInfo.InvariantCulture),
                    key.ToString()));
            }
        }

        public override void ProduceQueryPlan(PlanNode previous) => throw new NotImplementedException();

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }
}
