// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************

using System;
using System.Linq.Expressions;
using System.Numerics;
using Microsoft.StreamProcessing.Aggregates;
using Microsoft.StreamProcessing.Internal;

namespace Microsoft.StreamProcessing
{
    public static partial class Streamable
    {

        /// <summary>
        /// Computes a time-sensitive sum aggregate over sbytes using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, sbyte> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, sbyte>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over bytes using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, byte> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, byte>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over shorts using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, short> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, short>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over ushorts using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, ushort> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ushort>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over ints using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, int> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, int>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over uints using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, uint> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, uint>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over longs using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, long> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, long>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over ulongs using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, ulong> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ulong>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over floats using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, float> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, float>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over doubles using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, double>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over decimals using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, decimal> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, decimal>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over BigIntegers using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, BigInteger> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, BigInteger>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over Complexs using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, Complex> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, Complex>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable sbytes using snapshot semantics.
        /// Note that nulls have no affect on the sum.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, sbyte> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, sbyte?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable bytes using snapshot semantics.
        /// Note that nulls have no affect on the sum.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, byte> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, byte?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable shorts using snapshot semantics.
        /// Note that nulls have no affect on the sum.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, short> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, short?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable ushorts using snapshot semantics.
        /// Note that nulls have no affect on the sum.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, ushort> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ushort?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable ints using snapshot semantics.
        /// Note that nulls have no affect on the sum.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, int> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, int?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable uints using snapshot semantics.
        /// Note that nulls have no affect on the sum.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, uint> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, uint?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable longs using snapshot semantics.
        /// Note that nulls have no affect on the sum.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, long> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, long?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable ulongs using snapshot semantics.
        /// Note that nulls have no affect on the sum.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, ulong> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ulong?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable floats using snapshot semantics.
        /// Note that nulls have no affect on the sum.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, float> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, float?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable doubles using snapshot semantics.
        /// Note that nulls have no affect on the sum.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, double?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable decimals using snapshot semantics.
        /// Note that nulls have no affect on the sum.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, decimal> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, decimal?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable BigIntegers using snapshot semantics.
        /// Note that nulls have no affect on the sum.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, BigInteger> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, BigInteger?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable Complexs using snapshot semantics.
        /// Note that nulls have no affect on the sum.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, Complex> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, Complex?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over sbytes using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, sbyte> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, sbyte>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over bytes using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, byte> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, byte>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over shorts using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, short> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, short>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over ushorts using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, ushort> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ushort>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over ints using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, int> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, int>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over uints using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, uint> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, uint>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over longs using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, long> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, long>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over ulongs using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, ulong> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ulong>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over floats using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, float> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, float>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over doubles using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, double>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over decimals using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, decimal> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, decimal>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over BigIntegers using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, BigInteger> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, BigInteger>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over Complexs using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, Complex> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, Complex>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over nullable sbytes using snapshot semantics.
        /// Note that nulls have no affect on the sum.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, sbyte> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, sbyte?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over nullable bytes using snapshot semantics.
        /// Note that nulls have no affect on the sum.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, byte> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, byte?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over nullable shorts using snapshot semantics.
        /// Note that nulls have no affect on the sum.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, short> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, short?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over nullable ushorts using snapshot semantics.
        /// Note that nulls have no affect on the sum.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, ushort> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ushort?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over nullable ints using snapshot semantics.
        /// Note that nulls have no affect on the sum.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, int> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, int?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over nullable uints using snapshot semantics.
        /// Note that nulls have no affect on the sum.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, uint> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, uint?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over nullable longs using snapshot semantics.
        /// Note that nulls have no affect on the sum.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, long> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, long?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over nullable ulongs using snapshot semantics.
        /// Note that nulls have no affect on the sum.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, ulong> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ulong?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over nullable floats using snapshot semantics.
        /// Note that nulls have no affect on the sum.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, float> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, float?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over nullable doubles using snapshot semantics.
        /// Note that nulls have no affect on the sum.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, double?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over nullable decimals using snapshot semantics.
        /// Note that nulls have no affect on the sum.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, decimal> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, decimal?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over nullable BigIntegers using snapshot semantics.
        /// Note that nulls have no affect on the sum.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, BigInteger> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, BigInteger?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over nullable Complexs using snapshot semantics.
        /// Note that nulls have no affect on the sum.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a sum of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares summed acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, Complex> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, Complex?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over sbytes using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a product to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been multiplied acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, sbyte> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, sbyte>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over bytes using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a product to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been multiplied acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, byte> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, byte>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over shorts using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a product to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been multiplied acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, short> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, short>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over ushorts using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a product to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been multiplied acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, ushort> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ushort>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over ints using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a product to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been multiplied acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, int> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, int>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over uints using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a product to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been multiplied acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, uint> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, uint>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over longs using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a product to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been multiplied acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, long> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, long>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over ulongs using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a product to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been multiplied acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, ulong> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ulong>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over floats using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a product to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been multiplied acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, float> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, float>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over doubles using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a product to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been multiplied acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, double>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over decimals using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a product to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been multiplied acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, decimal> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, decimal>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over BigIntegers using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a product to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been multiplied acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, BigInteger> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, BigInteger>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over Complexs using snapshot semantics.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a product to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been multiplied acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, Complex> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, Complex>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over nullable sbytes using snapshot semantics.
        /// Note that nulls have no affect on the product.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a product to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been multiplied acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, sbyte> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, sbyte?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over nullable bytes using snapshot semantics.
        /// Note that nulls have no affect on the product.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a product to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been multiplied acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, byte> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, byte?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over nullable shorts using snapshot semantics.
        /// Note that nulls have no affect on the product.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a product to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been multiplied acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, short> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, short?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over nullable ushorts using snapshot semantics.
        /// Note that nulls have no affect on the product.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a product to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been multiplied acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, ushort> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ushort?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over nullable ints using snapshot semantics.
        /// Note that nulls have no affect on the product.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a product to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been multiplied acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, int> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, int?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over nullable uints using snapshot semantics.
        /// Note that nulls have no affect on the product.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a product to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been multiplied acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, uint> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, uint?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over nullable longs using snapshot semantics.
        /// Note that nulls have no affect on the product.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a product to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been multiplied acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, long> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, long?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over nullable ulongs using snapshot semantics.
        /// Note that nulls have no affect on the product.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a product to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been multiplied acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, ulong> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ulong?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over nullable floats using snapshot semantics.
        /// Note that nulls have no affect on the product.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a product to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been multiplied acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, float> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, float?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over nullable doubles using snapshot semantics.
        /// Note that nulls have no affect on the product.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a product to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been multiplied acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, double?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over nullable decimals using snapshot semantics.
        /// Note that nulls have no affect on the product.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a product to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been multiplied acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, decimal> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, decimal?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over nullable BigIntegers using snapshot semantics.
        /// Note that nulls have no affect on the product.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a product to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been multiplied acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, BigInteger> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, BigInteger?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over nullable Complexs using snapshot semantics.
        /// Note that nulls have no affect on the product.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute a product to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been multiplied acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, Complex> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, Complex?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over sbytes using snapshot semantics.
        /// Note that the accumulator internally is a long datatype.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, sbyte>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over shorts using snapshot semantics.
        /// Note that the accumulator internally is a long datatype.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, short>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over ints using snapshot semantics.
        /// Note that the accumulator internally is a long datatype.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, int>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over longs using snapshot semantics.
        /// Note that the accumulator internally is a long datatype.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, long>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over bytes using snapshot semantics.
        /// Note that the accumulator internally is a ulong datatype.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, byte>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over ushorts using snapshot semantics.
        /// Note that the accumulator internally is a ulong datatype.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ushort>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over uints using snapshot semantics.
        /// Note that the accumulator internally is a ulong datatype.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, uint>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over ulongs using snapshot semantics.
        /// Note that the accumulator internally is a ulong datatype.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ulong>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over floats using snapshot semantics.
        /// Note that the accumulator internally is a float datatype.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, float> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, float>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over doubles using snapshot semantics.
        /// Note that the accumulator internally is a double datatype.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, double>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over decimals using snapshot semantics.
        /// Note that the accumulator internally is a decimal datatype.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, decimal> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, decimal>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over BigIntegers using snapshot semantics.
        /// Note that the accumulator internally is a BigInteger datatype.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, BigInteger>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over Complexs using snapshot semantics.
        /// Note that the accumulator internally is a Complex datatype.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, Complex> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, Complex>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable sbytes using snapshot semantics.
        /// Note that the accumulator internally is a long datatype and that nulls have no affect on the average.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double?> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, sbyte?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable shorts using snapshot semantics.
        /// Note that the accumulator internally is a long datatype and that nulls have no affect on the average.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double?> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, short?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable ints using snapshot semantics.
        /// Note that the accumulator internally is a long datatype and that nulls have no affect on the average.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double?> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, int?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable longs using snapshot semantics.
        /// Note that the accumulator internally is a long datatype and that nulls have no affect on the average.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double?> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, long?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable bytes using snapshot semantics.
        /// Note that the accumulator internally is a ulong datatype and that nulls have no affect on the average.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double?> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, byte?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable ushorts using snapshot semantics.
        /// Note that the accumulator internally is a ulong datatype and that nulls have no affect on the average.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double?> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ushort?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable uints using snapshot semantics.
        /// Note that the accumulator internally is a ulong datatype and that nulls have no affect on the average.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double?> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, uint?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable ulongs using snapshot semantics.
        /// Note that the accumulator internally is a ulong datatype and that nulls have no affect on the average.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double?> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ulong?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable floats using snapshot semantics.
        /// Note that the accumulator internally is a float datatype and that nulls have no affect on the average.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, float?> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, float?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable doubles using snapshot semantics.
        /// Note that the accumulator internally is a double datatype and that nulls have no affect on the average.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double?> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, double?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable decimals using snapshot semantics.
        /// Note that the accumulator internally is a decimal datatype and that nulls have no affect on the average.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, decimal?> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, decimal?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable BigIntegers using snapshot semantics.
        /// Note that the accumulator internally is a BigInteger datatype and that nulls have no affect on the average.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double?> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, BigInteger?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable Complexs using snapshot semantics.
        /// Note that the accumulator internally is a Complex datatype and that nulls have no affect on the average.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have been averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, Complex?> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, Complex?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over sbytes using snapshot semantics.
        /// Note that the accumulator internally is a long datatype.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, sbyte>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over shorts using snapshot semantics.
        /// Note that the accumulator internally is a long datatype.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, short>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over ints using snapshot semantics.
        /// Note that the accumulator internally is a long datatype.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, int>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over longs using snapshot semantics.
        /// Note that the accumulator internally is a long datatype.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, long>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over bytes using snapshot semantics.
        /// Note that the accumulator internally is a ulong datatype.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, byte>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over ushorts using snapshot semantics.
        /// Note that the accumulator internally is a ulong datatype.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ushort>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over uints using snapshot semantics.
        /// Note that the accumulator internally is a ulong datatype.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, uint>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over ulongs using snapshot semantics.
        /// Note that the accumulator internally is a ulong datatype.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ulong>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over floats using snapshot semantics.
        /// Note that the accumulator internally is a float datatype.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, float> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, float>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over doubles using snapshot semantics.
        /// Note that the accumulator internally is a double datatype.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, double>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over decimals using snapshot semantics.
        /// Note that the accumulator internally is a decimal datatype.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, decimal> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, decimal>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over BigIntegers using snapshot semantics.
        /// Note that the accumulator internally is a BigInteger datatype.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, BigInteger>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over Complexs using snapshot semantics.
        /// Note that the accumulator internally is a Complex datatype.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, Complex> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, Complex>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over nullable sbytes using snapshot semantics.
        /// Note that the accumulator internally is a long datatype and that nulls have no affect on the average.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double?> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, sbyte?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over nullable shorts using snapshot semantics.
        /// Note that the accumulator internally is a long datatype and that nulls have no affect on the average.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double?> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, short?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over nullable ints using snapshot semantics.
        /// Note that the accumulator internally is a long datatype and that nulls have no affect on the average.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double?> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, int?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over nullable longs using snapshot semantics.
        /// Note that the accumulator internally is a long datatype and that nulls have no affect on the average.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double?> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, long?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over nullable bytes using snapshot semantics.
        /// Note that the accumulator internally is a ulong datatype and that nulls have no affect on the average.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double?> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, byte?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over nullable ushorts using snapshot semantics.
        /// Note that the accumulator internally is a ulong datatype and that nulls have no affect on the average.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double?> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ushort?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over nullable uints using snapshot semantics.
        /// Note that the accumulator internally is a ulong datatype and that nulls have no affect on the average.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double?> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, uint?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over nullable ulongs using snapshot semantics.
        /// Note that the accumulator internally is a ulong datatype and that nulls have no affect on the average.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double?> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ulong?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over nullable floats using snapshot semantics.
        /// Note that the accumulator internally is a float datatype and that nulls have no affect on the average.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, float?> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, float?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over nullable doubles using snapshot semantics.
        /// Note that the accumulator internally is a double datatype and that nulls have no affect on the average.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double?> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, double?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over nullable decimals using snapshot semantics.
        /// Note that the accumulator internally is a decimal datatype and that nulls have no affect on the average.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, decimal?> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, decimal?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over nullable BigIntegers using snapshot semantics.
        /// Note that the accumulator internally is a BigInteger datatype and that nulls have no affect on the average.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, double?> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, BigInteger?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over nullable Complexs using snapshot semantics.
        /// Note that the accumulator internally is a Complex datatype and that nulls have no affect on the average.
        /// </summary>
        /// <typeparam name="TKey">The grouping key type of the incoming stream of data.</typeparam>
        /// <typeparam name="TPayload">The payload type of the incoming stream of data.</typeparam>
        /// <param name="source">The stream over which to compute an average of squares according to snapshot semantics.</param>
        /// <param name="selector">A selector expression describing what part of the incoming data is to be aggregated.</param>
        /// <returns>A stream of data whose payloads have had its squares averaged acccording to snapshot semanics.</returns>
        public static IStreamable<TKey, Complex?> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, Complex?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot windows on the input stream.
        /// </summary>
        /// <typeparam name="TKey">The type of the grouping key of the stream coming into the aggregation operation.</typeparam>
        /// <typeparam name="TInput">The type of the data payload in the input stream.</typeparam>
        /// <typeparam name="TState1">The type of the state object maintained by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TState2">The type of the state object maintained by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TOutput1">The type of the results generated by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TOutput2">The type of the results generated by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TOutput">The type of the payloads of the resulting stream.</typeparam>
        /// <param name="source">The stream over which to aggregate data.</param>
        /// <param name="aggregate1">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate2">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="merger">An expression describing how to merge the grouping key and the result of the aggregation into a result payload.</param>
        /// <returns>A stream of data with result payload type <typeparamref name="TOutput"/>.</returns>
        public static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TOutput>(
            this IStreamable<TKey, TInput> source,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Expression<Func<TOutput1, TOutput2, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(merger, nameof(merger));

            var window = new Window<TKey, TInput>(source.Properties);
            var agg1 = aggregate1(window);
            var agg2 = aggregate2(window);
            var compound = AggregateFunctions.Combine(agg1, agg2, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2>, TOutput>(source, compound);
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot semantics on the input stream.
        /// </summary>
        internal static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TOutput>(
            this IStreamable<TKey, TInput> source,
            IAggregate<TInput, TState1, TOutput1> aggregate1,
            IAggregate<TInput, TState2, TOutput2> aggregate2,
            Expression<Func<TOutput1, TOutput2, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(merger, nameof(merger));

            var compound = AggregateFunctions.Combine(aggregate1, aggregate2, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2>, TOutput>(source, compound);
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot windows on the input stream.
        /// </summary>
        /// <typeparam name="TKey">The type of the grouping key of the stream coming into the aggregation operation.</typeparam>
        /// <typeparam name="TInput">The type of the data payload in the input stream.</typeparam>
        /// <typeparam name="TState1">The type of the state object maintained by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TState2">The type of the state object maintained by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TState3">The type of the state object maintained by the aggregate operation in position 3.</typeparam>
        /// <typeparam name="TOutput1">The type of the results generated by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TOutput2">The type of the results generated by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TOutput3">The type of the results generated by the aggregate operation in position 3.</typeparam>
        /// <typeparam name="TOutput">The type of the payloads of the resulting stream.</typeparam>
        /// <param name="source">The stream over which to aggregate data.</param>
        /// <param name="aggregate1">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate2">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate3">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="merger">An expression describing how to merge the grouping key and the result of the aggregation into a result payload.</param>
        /// <returns>A stream of data with result payload type <typeparamref name="TOutput"/>.</returns>
        public static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TOutput>(
            this IStreamable<TKey, TInput> source,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState3, TOutput3>> aggregate3,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(merger, nameof(merger));

            var window = new Window<TKey, TInput>(source.Properties);
            var agg1 = aggregate1(window);
            var agg2 = aggregate2(window);
            var agg3 = aggregate3(window);
            var compound = AggregateFunctions.Combine(agg1, agg2, agg3, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2, TState3>, TOutput>(source, compound);
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot semantics on the input stream.
        /// </summary>
        internal static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TOutput>(
            this IStreamable<TKey, TInput> source,
            IAggregate<TInput, TState1, TOutput1> aggregate1,
            IAggregate<TInput, TState2, TOutput2> aggregate2,
            IAggregate<TInput, TState3, TOutput3> aggregate3,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(merger, nameof(merger));

            var compound = AggregateFunctions.Combine(aggregate1, aggregate2, aggregate3, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2, TState3>, TOutput>(source, compound);
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot windows on the input stream.
        /// </summary>
        /// <typeparam name="TKey">The type of the grouping key of the stream coming into the aggregation operation.</typeparam>
        /// <typeparam name="TInput">The type of the data payload in the input stream.</typeparam>
        /// <typeparam name="TState1">The type of the state object maintained by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TState2">The type of the state object maintained by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TState3">The type of the state object maintained by the aggregate operation in position 3.</typeparam>
        /// <typeparam name="TState4">The type of the state object maintained by the aggregate operation in position 4.</typeparam>
        /// <typeparam name="TOutput1">The type of the results generated by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TOutput2">The type of the results generated by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TOutput3">The type of the results generated by the aggregate operation in position 3.</typeparam>
        /// <typeparam name="TOutput4">The type of the results generated by the aggregate operation in position 4.</typeparam>
        /// <typeparam name="TOutput">The type of the payloads of the resulting stream.</typeparam>
        /// <param name="source">The stream over which to aggregate data.</param>
        /// <param name="aggregate1">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate2">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate3">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate4">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="merger">An expression describing how to merge the grouping key and the result of the aggregation into a result payload.</param>
        /// <returns>A stream of data with result payload type <typeparamref name="TOutput"/>.</returns>
        public static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TOutput>(
            this IStreamable<TKey, TInput> source,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState3, TOutput3>> aggregate3,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState4, TOutput4>> aggregate4,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(merger, nameof(merger));

            var window = new Window<TKey, TInput>(source.Properties);
            var agg1 = aggregate1(window);
            var agg2 = aggregate2(window);
            var agg3 = aggregate3(window);
            var agg4 = aggregate4(window);
            var compound = AggregateFunctions.Combine(agg1, agg2, agg3, agg4, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2, TState3, TState4>, TOutput>(source, compound);
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot semantics on the input stream.
        /// </summary>
        internal static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TOutput>(
            this IStreamable<TKey, TInput> source,
            IAggregate<TInput, TState1, TOutput1> aggregate1,
            IAggregate<TInput, TState2, TOutput2> aggregate2,
            IAggregate<TInput, TState3, TOutput3> aggregate3,
            IAggregate<TInput, TState4, TOutput4> aggregate4,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(merger, nameof(merger));

            var compound = AggregateFunctions.Combine(aggregate1, aggregate2, aggregate3, aggregate4, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2, TState3, TState4>, TOutput>(source, compound);
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot windows on the input stream.
        /// </summary>
        /// <typeparam name="TKey">The type of the grouping key of the stream coming into the aggregation operation.</typeparam>
        /// <typeparam name="TInput">The type of the data payload in the input stream.</typeparam>
        /// <typeparam name="TState1">The type of the state object maintained by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TState2">The type of the state object maintained by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TState3">The type of the state object maintained by the aggregate operation in position 3.</typeparam>
        /// <typeparam name="TState4">The type of the state object maintained by the aggregate operation in position 4.</typeparam>
        /// <typeparam name="TState5">The type of the state object maintained by the aggregate operation in position 5.</typeparam>
        /// <typeparam name="TOutput1">The type of the results generated by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TOutput2">The type of the results generated by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TOutput3">The type of the results generated by the aggregate operation in position 3.</typeparam>
        /// <typeparam name="TOutput4">The type of the results generated by the aggregate operation in position 4.</typeparam>
        /// <typeparam name="TOutput5">The type of the results generated by the aggregate operation in position 5.</typeparam>
        /// <typeparam name="TOutput">The type of the payloads of the resulting stream.</typeparam>
        /// <param name="source">The stream over which to aggregate data.</param>
        /// <param name="aggregate1">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate2">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate3">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate4">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate5">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="merger">An expression describing how to merge the grouping key and the result of the aggregation into a result payload.</param>
        /// <returns>A stream of data with result payload type <typeparamref name="TOutput"/>.</returns>
        public static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TOutput>(
            this IStreamable<TKey, TInput> source,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState3, TOutput3>> aggregate3,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState4, TOutput4>> aggregate4,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState5, TOutput5>> aggregate5,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(aggregate5, nameof(aggregate5));
            Invariant.IsNotNull(merger, nameof(merger));

            var window = new Window<TKey, TInput>(source.Properties);
            var agg1 = aggregate1(window);
            var agg2 = aggregate2(window);
            var agg3 = aggregate3(window);
            var agg4 = aggregate4(window);
            var agg5 = aggregate5(window);
            var compound = AggregateFunctions.Combine(agg1, agg2, agg3, agg4, agg5, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5>, TOutput>(source, compound);
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot semantics on the input stream.
        /// </summary>
        internal static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TOutput>(
            this IStreamable<TKey, TInput> source,
            IAggregate<TInput, TState1, TOutput1> aggregate1,
            IAggregate<TInput, TState2, TOutput2> aggregate2,
            IAggregate<TInput, TState3, TOutput3> aggregate3,
            IAggregate<TInput, TState4, TOutput4> aggregate4,
            IAggregate<TInput, TState5, TOutput5> aggregate5,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(aggregate5, nameof(aggregate5));
            Invariant.IsNotNull(merger, nameof(merger));

            var compound = AggregateFunctions.Combine(aggregate1, aggregate2, aggregate3, aggregate4, aggregate5, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5>, TOutput>(source, compound);
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot windows on the input stream.
        /// </summary>
        /// <typeparam name="TKey">The type of the grouping key of the stream coming into the aggregation operation.</typeparam>
        /// <typeparam name="TInput">The type of the data payload in the input stream.</typeparam>
        /// <typeparam name="TState1">The type of the state object maintained by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TState2">The type of the state object maintained by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TState3">The type of the state object maintained by the aggregate operation in position 3.</typeparam>
        /// <typeparam name="TState4">The type of the state object maintained by the aggregate operation in position 4.</typeparam>
        /// <typeparam name="TState5">The type of the state object maintained by the aggregate operation in position 5.</typeparam>
        /// <typeparam name="TState6">The type of the state object maintained by the aggregate operation in position 6.</typeparam>
        /// <typeparam name="TOutput1">The type of the results generated by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TOutput2">The type of the results generated by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TOutput3">The type of the results generated by the aggregate operation in position 3.</typeparam>
        /// <typeparam name="TOutput4">The type of the results generated by the aggregate operation in position 4.</typeparam>
        /// <typeparam name="TOutput5">The type of the results generated by the aggregate operation in position 5.</typeparam>
        /// <typeparam name="TOutput6">The type of the results generated by the aggregate operation in position 6.</typeparam>
        /// <typeparam name="TOutput">The type of the payloads of the resulting stream.</typeparam>
        /// <param name="source">The stream over which to aggregate data.</param>
        /// <param name="aggregate1">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate2">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate3">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate4">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate5">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate6">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="merger">An expression describing how to merge the grouping key and the result of the aggregation into a result payload.</param>
        /// <returns>A stream of data with result payload type <typeparamref name="TOutput"/>.</returns>
        public static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TOutput>(
            this IStreamable<TKey, TInput> source,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState3, TOutput3>> aggregate3,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState4, TOutput4>> aggregate4,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState5, TOutput5>> aggregate5,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState6, TOutput6>> aggregate6,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(aggregate5, nameof(aggregate5));
            Invariant.IsNotNull(aggregate6, nameof(aggregate6));
            Invariant.IsNotNull(merger, nameof(merger));

            var window = new Window<TKey, TInput>(source.Properties);
            var agg1 = aggregate1(window);
            var agg2 = aggregate2(window);
            var agg3 = aggregate3(window);
            var agg4 = aggregate4(window);
            var agg5 = aggregate5(window);
            var agg6 = aggregate6(window);
            var compound = AggregateFunctions.Combine(agg1, agg2, agg3, agg4, agg5, agg6, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>, TOutput>(source, compound);
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot semantics on the input stream.
        /// </summary>
        internal static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TOutput>(
            this IStreamable<TKey, TInput> source,
            IAggregate<TInput, TState1, TOutput1> aggregate1,
            IAggregate<TInput, TState2, TOutput2> aggregate2,
            IAggregate<TInput, TState3, TOutput3> aggregate3,
            IAggregate<TInput, TState4, TOutput4> aggregate4,
            IAggregate<TInput, TState5, TOutput5> aggregate5,
            IAggregate<TInput, TState6, TOutput6> aggregate6,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(aggregate5, nameof(aggregate5));
            Invariant.IsNotNull(aggregate6, nameof(aggregate6));
            Invariant.IsNotNull(merger, nameof(merger));

            var compound = AggregateFunctions.Combine(aggregate1, aggregate2, aggregate3, aggregate4, aggregate5, aggregate6, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>, TOutput>(source, compound);
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot windows on the input stream.
        /// </summary>
        /// <typeparam name="TKey">The type of the grouping key of the stream coming into the aggregation operation.</typeparam>
        /// <typeparam name="TInput">The type of the data payload in the input stream.</typeparam>
        /// <typeparam name="TState1">The type of the state object maintained by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TState2">The type of the state object maintained by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TState3">The type of the state object maintained by the aggregate operation in position 3.</typeparam>
        /// <typeparam name="TState4">The type of the state object maintained by the aggregate operation in position 4.</typeparam>
        /// <typeparam name="TState5">The type of the state object maintained by the aggregate operation in position 5.</typeparam>
        /// <typeparam name="TState6">The type of the state object maintained by the aggregate operation in position 6.</typeparam>
        /// <typeparam name="TState7">The type of the state object maintained by the aggregate operation in position 7.</typeparam>
        /// <typeparam name="TOutput1">The type of the results generated by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TOutput2">The type of the results generated by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TOutput3">The type of the results generated by the aggregate operation in position 3.</typeparam>
        /// <typeparam name="TOutput4">The type of the results generated by the aggregate operation in position 4.</typeparam>
        /// <typeparam name="TOutput5">The type of the results generated by the aggregate operation in position 5.</typeparam>
        /// <typeparam name="TOutput6">The type of the results generated by the aggregate operation in position 6.</typeparam>
        /// <typeparam name="TOutput7">The type of the results generated by the aggregate operation in position 7.</typeparam>
        /// <typeparam name="TOutput">The type of the payloads of the resulting stream.</typeparam>
        /// <param name="source">The stream over which to aggregate data.</param>
        /// <param name="aggregate1">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate2">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate3">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate4">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate5">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate6">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate7">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="merger">An expression describing how to merge the grouping key and the result of the aggregation into a result payload.</param>
        /// <returns>A stream of data with result payload type <typeparamref name="TOutput"/>.</returns>
        public static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TState7, TOutput7, TOutput>(
            this IStreamable<TKey, TInput> source,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState3, TOutput3>> aggregate3,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState4, TOutput4>> aggregate4,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState5, TOutput5>> aggregate5,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState6, TOutput6>> aggregate6,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState7, TOutput7>> aggregate7,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput7, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(aggregate5, nameof(aggregate5));
            Invariant.IsNotNull(aggregate6, nameof(aggregate6));
            Invariant.IsNotNull(aggregate7, nameof(aggregate7));
            Invariant.IsNotNull(merger, nameof(merger));

            var window = new Window<TKey, TInput>(source.Properties);
            var agg1 = aggregate1(window);
            var agg2 = aggregate2(window);
            var agg3 = aggregate3(window);
            var agg4 = aggregate4(window);
            var agg5 = aggregate5(window);
            var agg6 = aggregate6(window);
            var agg7 = aggregate7(window);
            var compound = AggregateFunctions.Combine(agg1, agg2, agg3, agg4, agg5, agg6, agg7, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>, TOutput>(source, compound);
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot semantics on the input stream.
        /// </summary>
        internal static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TState7, TOutput7, TOutput>(
            this IStreamable<TKey, TInput> source,
            IAggregate<TInput, TState1, TOutput1> aggregate1,
            IAggregate<TInput, TState2, TOutput2> aggregate2,
            IAggregate<TInput, TState3, TOutput3> aggregate3,
            IAggregate<TInput, TState4, TOutput4> aggregate4,
            IAggregate<TInput, TState5, TOutput5> aggregate5,
            IAggregate<TInput, TState6, TOutput6> aggregate6,
            IAggregate<TInput, TState7, TOutput7> aggregate7,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput7, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(aggregate5, nameof(aggregate5));
            Invariant.IsNotNull(aggregate6, nameof(aggregate6));
            Invariant.IsNotNull(aggregate7, nameof(aggregate7));
            Invariant.IsNotNull(merger, nameof(merger));

            var compound = AggregateFunctions.Combine(aggregate1, aggregate2, aggregate3, aggregate4, aggregate5, aggregate6, aggregate7, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>, TOutput>(source, compound);
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot windows on the input stream.
        /// </summary>
        /// <typeparam name="TKey">The type of the grouping key of the stream coming into the aggregation operation.</typeparam>
        /// <typeparam name="TInput">The type of the data payload in the input stream.</typeparam>
        /// <typeparam name="TState1">The type of the state object maintained by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TState2">The type of the state object maintained by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TState3">The type of the state object maintained by the aggregate operation in position 3.</typeparam>
        /// <typeparam name="TState4">The type of the state object maintained by the aggregate operation in position 4.</typeparam>
        /// <typeparam name="TState5">The type of the state object maintained by the aggregate operation in position 5.</typeparam>
        /// <typeparam name="TState6">The type of the state object maintained by the aggregate operation in position 6.</typeparam>
        /// <typeparam name="TState7">The type of the state object maintained by the aggregate operation in position 7.</typeparam>
        /// <typeparam name="TState8">The type of the state object maintained by the aggregate operation in position 8.</typeparam>
        /// <typeparam name="TOutput1">The type of the results generated by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TOutput2">The type of the results generated by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TOutput3">The type of the results generated by the aggregate operation in position 3.</typeparam>
        /// <typeparam name="TOutput4">The type of the results generated by the aggregate operation in position 4.</typeparam>
        /// <typeparam name="TOutput5">The type of the results generated by the aggregate operation in position 5.</typeparam>
        /// <typeparam name="TOutput6">The type of the results generated by the aggregate operation in position 6.</typeparam>
        /// <typeparam name="TOutput7">The type of the results generated by the aggregate operation in position 7.</typeparam>
        /// <typeparam name="TOutput8">The type of the results generated by the aggregate operation in position 8.</typeparam>
        /// <typeparam name="TOutput">The type of the payloads of the resulting stream.</typeparam>
        /// <param name="source">The stream over which to aggregate data.</param>
        /// <param name="aggregate1">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate2">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate3">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate4">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate5">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate6">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate7">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate8">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="merger">An expression describing how to merge the grouping key and the result of the aggregation into a result payload.</param>
        /// <returns>A stream of data with result payload type <typeparamref name="TOutput"/>.</returns>
        public static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TState7, TOutput7, TState8, TOutput8, TOutput>(
            this IStreamable<TKey, TInput> source,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState3, TOutput3>> aggregate3,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState4, TOutput4>> aggregate4,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState5, TOutput5>> aggregate5,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState6, TOutput6>> aggregate6,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState7, TOutput7>> aggregate7,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState8, TOutput8>> aggregate8,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput7, TOutput8, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(aggregate5, nameof(aggregate5));
            Invariant.IsNotNull(aggregate6, nameof(aggregate6));
            Invariant.IsNotNull(aggregate7, nameof(aggregate7));
            Invariant.IsNotNull(aggregate8, nameof(aggregate8));
            Invariant.IsNotNull(merger, nameof(merger));

            var window = new Window<TKey, TInput>(source.Properties);
            var agg1 = aggregate1(window);
            var agg2 = aggregate2(window);
            var agg3 = aggregate3(window);
            var agg4 = aggregate4(window);
            var agg5 = aggregate5(window);
            var agg6 = aggregate6(window);
            var agg7 = aggregate7(window);
            var agg8 = aggregate8(window);
            var compound = AggregateFunctions.Combine(agg1, agg2, agg3, agg4, agg5, agg6, agg7, agg8, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>, TOutput>(source, compound);
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot semantics on the input stream.
        /// </summary>
        internal static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TState7, TOutput7, TState8, TOutput8, TOutput>(
            this IStreamable<TKey, TInput> source,
            IAggregate<TInput, TState1, TOutput1> aggregate1,
            IAggregate<TInput, TState2, TOutput2> aggregate2,
            IAggregate<TInput, TState3, TOutput3> aggregate3,
            IAggregate<TInput, TState4, TOutput4> aggregate4,
            IAggregate<TInput, TState5, TOutput5> aggregate5,
            IAggregate<TInput, TState6, TOutput6> aggregate6,
            IAggregate<TInput, TState7, TOutput7> aggregate7,
            IAggregate<TInput, TState8, TOutput8> aggregate8,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput7, TOutput8, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(aggregate5, nameof(aggregate5));
            Invariant.IsNotNull(aggregate6, nameof(aggregate6));
            Invariant.IsNotNull(aggregate7, nameof(aggregate7));
            Invariant.IsNotNull(aggregate8, nameof(aggregate8));
            Invariant.IsNotNull(merger, nameof(merger));

            var compound = AggregateFunctions.Combine(aggregate1, aggregate2, aggregate3, aggregate4, aggregate5, aggregate6, aggregate7, aggregate8, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>, TOutput>(source, compound);
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot windows on the input stream.
        /// </summary>
        /// <typeparam name="TKey">The type of the grouping key of the stream coming into the aggregation operation.</typeparam>
        /// <typeparam name="TInput">The type of the data payload in the input stream.</typeparam>
        /// <typeparam name="TState1">The type of the state object maintained by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TState2">The type of the state object maintained by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TState3">The type of the state object maintained by the aggregate operation in position 3.</typeparam>
        /// <typeparam name="TState4">The type of the state object maintained by the aggregate operation in position 4.</typeparam>
        /// <typeparam name="TState5">The type of the state object maintained by the aggregate operation in position 5.</typeparam>
        /// <typeparam name="TState6">The type of the state object maintained by the aggregate operation in position 6.</typeparam>
        /// <typeparam name="TState7">The type of the state object maintained by the aggregate operation in position 7.</typeparam>
        /// <typeparam name="TState8">The type of the state object maintained by the aggregate operation in position 8.</typeparam>
        /// <typeparam name="TState9">The type of the state object maintained by the aggregate operation in position 9.</typeparam>
        /// <typeparam name="TOutput1">The type of the results generated by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TOutput2">The type of the results generated by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TOutput3">The type of the results generated by the aggregate operation in position 3.</typeparam>
        /// <typeparam name="TOutput4">The type of the results generated by the aggregate operation in position 4.</typeparam>
        /// <typeparam name="TOutput5">The type of the results generated by the aggregate operation in position 5.</typeparam>
        /// <typeparam name="TOutput6">The type of the results generated by the aggregate operation in position 6.</typeparam>
        /// <typeparam name="TOutput7">The type of the results generated by the aggregate operation in position 7.</typeparam>
        /// <typeparam name="TOutput8">The type of the results generated by the aggregate operation in position 8.</typeparam>
        /// <typeparam name="TOutput9">The type of the results generated by the aggregate operation in position 9.</typeparam>
        /// <typeparam name="TOutput">The type of the payloads of the resulting stream.</typeparam>
        /// <param name="source">The stream over which to aggregate data.</param>
        /// <param name="aggregate1">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate2">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate3">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate4">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate5">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate6">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate7">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate8">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate9">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="merger">An expression describing how to merge the grouping key and the result of the aggregation into a result payload.</param>
        /// <returns>A stream of data with result payload type <typeparamref name="TOutput"/>.</returns>
        public static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TState7, TOutput7, TState8, TOutput8, TState9, TOutput9, TOutput>(
            this IStreamable<TKey, TInput> source,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState3, TOutput3>> aggregate3,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState4, TOutput4>> aggregate4,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState5, TOutput5>> aggregate5,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState6, TOutput6>> aggregate6,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState7, TOutput7>> aggregate7,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState8, TOutput8>> aggregate8,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState9, TOutput9>> aggregate9,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput7, TOutput8, TOutput9, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(aggregate5, nameof(aggregate5));
            Invariant.IsNotNull(aggregate6, nameof(aggregate6));
            Invariant.IsNotNull(aggregate7, nameof(aggregate7));
            Invariant.IsNotNull(aggregate8, nameof(aggregate8));
            Invariant.IsNotNull(aggregate9, nameof(aggregate9));
            Invariant.IsNotNull(merger, nameof(merger));

            var window = new Window<TKey, TInput>(source.Properties);
            var agg1 = aggregate1(window);
            var agg2 = aggregate2(window);
            var agg3 = aggregate3(window);
            var agg4 = aggregate4(window);
            var agg5 = aggregate5(window);
            var agg6 = aggregate6(window);
            var agg7 = aggregate7(window);
            var agg8 = aggregate8(window);
            var agg9 = aggregate9(window);
            var compound = AggregateFunctions.Combine(agg1, agg2, agg3, agg4, agg5, agg6, agg7, agg8, agg9, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>, TOutput>(source, compound);
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot semantics on the input stream.
        /// </summary>
        internal static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TState7, TOutput7, TState8, TOutput8, TState9, TOutput9, TOutput>(
            this IStreamable<TKey, TInput> source,
            IAggregate<TInput, TState1, TOutput1> aggregate1,
            IAggregate<TInput, TState2, TOutput2> aggregate2,
            IAggregate<TInput, TState3, TOutput3> aggregate3,
            IAggregate<TInput, TState4, TOutput4> aggregate4,
            IAggregate<TInput, TState5, TOutput5> aggregate5,
            IAggregate<TInput, TState6, TOutput6> aggregate6,
            IAggregate<TInput, TState7, TOutput7> aggregate7,
            IAggregate<TInput, TState8, TOutput8> aggregate8,
            IAggregate<TInput, TState9, TOutput9> aggregate9,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput7, TOutput8, TOutput9, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(aggregate5, nameof(aggregate5));
            Invariant.IsNotNull(aggregate6, nameof(aggregate6));
            Invariant.IsNotNull(aggregate7, nameof(aggregate7));
            Invariant.IsNotNull(aggregate8, nameof(aggregate8));
            Invariant.IsNotNull(aggregate9, nameof(aggregate9));
            Invariant.IsNotNull(merger, nameof(merger));

            var compound = AggregateFunctions.Combine(aggregate1, aggregate2, aggregate3, aggregate4, aggregate5, aggregate6, aggregate7, aggregate8, aggregate9, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>, TOutput>(source, compound);
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot windows on the input stream.
        /// </summary>
        /// <typeparam name="TKey">The type of the grouping key of the stream coming into the aggregation operation.</typeparam>
        /// <typeparam name="TInput">The type of the data payload in the input stream.</typeparam>
        /// <typeparam name="TState1">The type of the state object maintained by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TState2">The type of the state object maintained by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TState3">The type of the state object maintained by the aggregate operation in position 3.</typeparam>
        /// <typeparam name="TState4">The type of the state object maintained by the aggregate operation in position 4.</typeparam>
        /// <typeparam name="TState5">The type of the state object maintained by the aggregate operation in position 5.</typeparam>
        /// <typeparam name="TState6">The type of the state object maintained by the aggregate operation in position 6.</typeparam>
        /// <typeparam name="TState7">The type of the state object maintained by the aggregate operation in position 7.</typeparam>
        /// <typeparam name="TState8">The type of the state object maintained by the aggregate operation in position 8.</typeparam>
        /// <typeparam name="TState9">The type of the state object maintained by the aggregate operation in position 9.</typeparam>
        /// <typeparam name="TState10">The type of the state object maintained by the aggregate operation in position 10.</typeparam>
        /// <typeparam name="TOutput1">The type of the results generated by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TOutput2">The type of the results generated by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TOutput3">The type of the results generated by the aggregate operation in position 3.</typeparam>
        /// <typeparam name="TOutput4">The type of the results generated by the aggregate operation in position 4.</typeparam>
        /// <typeparam name="TOutput5">The type of the results generated by the aggregate operation in position 5.</typeparam>
        /// <typeparam name="TOutput6">The type of the results generated by the aggregate operation in position 6.</typeparam>
        /// <typeparam name="TOutput7">The type of the results generated by the aggregate operation in position 7.</typeparam>
        /// <typeparam name="TOutput8">The type of the results generated by the aggregate operation in position 8.</typeparam>
        /// <typeparam name="TOutput9">The type of the results generated by the aggregate operation in position 9.</typeparam>
        /// <typeparam name="TOutput10">The type of the results generated by the aggregate operation in position 10.</typeparam>
        /// <typeparam name="TOutput">The type of the payloads of the resulting stream.</typeparam>
        /// <param name="source">The stream over which to aggregate data.</param>
        /// <param name="aggregate1">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate2">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate3">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate4">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate5">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate6">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate7">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate8">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate9">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate10">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="merger">An expression describing how to merge the grouping key and the result of the aggregation into a result payload.</param>
        /// <returns>A stream of data with result payload type <typeparamref name="TOutput"/>.</returns>
        public static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TState7, TOutput7, TState8, TOutput8, TState9, TOutput9, TState10, TOutput10, TOutput>(
            this IStreamable<TKey, TInput> source,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState3, TOutput3>> aggregate3,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState4, TOutput4>> aggregate4,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState5, TOutput5>> aggregate5,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState6, TOutput6>> aggregate6,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState7, TOutput7>> aggregate7,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState8, TOutput8>> aggregate8,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState9, TOutput9>> aggregate9,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState10, TOutput10>> aggregate10,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput7, TOutput8, TOutput9, TOutput10, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(aggregate5, nameof(aggregate5));
            Invariant.IsNotNull(aggregate6, nameof(aggregate6));
            Invariant.IsNotNull(aggregate7, nameof(aggregate7));
            Invariant.IsNotNull(aggregate8, nameof(aggregate8));
            Invariant.IsNotNull(aggregate9, nameof(aggregate9));
            Invariant.IsNotNull(aggregate10, nameof(aggregate10));
            Invariant.IsNotNull(merger, nameof(merger));

            var window = new Window<TKey, TInput>(source.Properties);
            var agg1 = aggregate1(window);
            var agg2 = aggregate2(window);
            var agg3 = aggregate3(window);
            var agg4 = aggregate4(window);
            var agg5 = aggregate5(window);
            var agg6 = aggregate6(window);
            var agg7 = aggregate7(window);
            var agg8 = aggregate8(window);
            var agg9 = aggregate9(window);
            var agg10 = aggregate10(window);
            var compound = AggregateFunctions.Combine(agg1, agg2, agg3, agg4, agg5, agg6, agg7, agg8, agg9, agg10, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, TOutput>(source, compound);
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot semantics on the input stream.
        /// </summary>
        internal static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TState7, TOutput7, TState8, TOutput8, TState9, TOutput9, TState10, TOutput10, TOutput>(
            this IStreamable<TKey, TInput> source,
            IAggregate<TInput, TState1, TOutput1> aggregate1,
            IAggregate<TInput, TState2, TOutput2> aggregate2,
            IAggregate<TInput, TState3, TOutput3> aggregate3,
            IAggregate<TInput, TState4, TOutput4> aggregate4,
            IAggregate<TInput, TState5, TOutput5> aggregate5,
            IAggregate<TInput, TState6, TOutput6> aggregate6,
            IAggregate<TInput, TState7, TOutput7> aggregate7,
            IAggregate<TInput, TState8, TOutput8> aggregate8,
            IAggregate<TInput, TState9, TOutput9> aggregate9,
            IAggregate<TInput, TState10, TOutput10> aggregate10,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput7, TOutput8, TOutput9, TOutput10, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(aggregate5, nameof(aggregate5));
            Invariant.IsNotNull(aggregate6, nameof(aggregate6));
            Invariant.IsNotNull(aggregate7, nameof(aggregate7));
            Invariant.IsNotNull(aggregate8, nameof(aggregate8));
            Invariant.IsNotNull(aggregate9, nameof(aggregate9));
            Invariant.IsNotNull(aggregate10, nameof(aggregate10));
            Invariant.IsNotNull(merger, nameof(merger));

            var compound = AggregateFunctions.Combine(aggregate1, aggregate2, aggregate3, aggregate4, aggregate5, aggregate6, aggregate7, aggregate8, aggregate9, aggregate10, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, TOutput>(source, compound);
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot windows on the input stream.
        /// </summary>
        /// <typeparam name="TKey">The type of the grouping key of the stream coming into the aggregation operation.</typeparam>
        /// <typeparam name="TInput">The type of the data payload in the input stream.</typeparam>
        /// <typeparam name="TState1">The type of the state object maintained by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TState2">The type of the state object maintained by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TState3">The type of the state object maintained by the aggregate operation in position 3.</typeparam>
        /// <typeparam name="TState4">The type of the state object maintained by the aggregate operation in position 4.</typeparam>
        /// <typeparam name="TState5">The type of the state object maintained by the aggregate operation in position 5.</typeparam>
        /// <typeparam name="TState6">The type of the state object maintained by the aggregate operation in position 6.</typeparam>
        /// <typeparam name="TState7">The type of the state object maintained by the aggregate operation in position 7.</typeparam>
        /// <typeparam name="TState8">The type of the state object maintained by the aggregate operation in position 8.</typeparam>
        /// <typeparam name="TState9">The type of the state object maintained by the aggregate operation in position 9.</typeparam>
        /// <typeparam name="TState10">The type of the state object maintained by the aggregate operation in position 10.</typeparam>
        /// <typeparam name="TState11">The type of the state object maintained by the aggregate operation in position 11.</typeparam>
        /// <typeparam name="TOutput1">The type of the results generated by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TOutput2">The type of the results generated by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TOutput3">The type of the results generated by the aggregate operation in position 3.</typeparam>
        /// <typeparam name="TOutput4">The type of the results generated by the aggregate operation in position 4.</typeparam>
        /// <typeparam name="TOutput5">The type of the results generated by the aggregate operation in position 5.</typeparam>
        /// <typeparam name="TOutput6">The type of the results generated by the aggregate operation in position 6.</typeparam>
        /// <typeparam name="TOutput7">The type of the results generated by the aggregate operation in position 7.</typeparam>
        /// <typeparam name="TOutput8">The type of the results generated by the aggregate operation in position 8.</typeparam>
        /// <typeparam name="TOutput9">The type of the results generated by the aggregate operation in position 9.</typeparam>
        /// <typeparam name="TOutput10">The type of the results generated by the aggregate operation in position 10.</typeparam>
        /// <typeparam name="TOutput11">The type of the results generated by the aggregate operation in position 11.</typeparam>
        /// <typeparam name="TOutput">The type of the payloads of the resulting stream.</typeparam>
        /// <param name="source">The stream over which to aggregate data.</param>
        /// <param name="aggregate1">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate2">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate3">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate4">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate5">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate6">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate7">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate8">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate9">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate10">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate11">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="merger">An expression describing how to merge the grouping key and the result of the aggregation into a result payload.</param>
        /// <returns>A stream of data with result payload type <typeparamref name="TOutput"/>.</returns>
        public static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TState7, TOutput7, TState8, TOutput8, TState9, TOutput9, TState10, TOutput10, TState11, TOutput11, TOutput>(
            this IStreamable<TKey, TInput> source,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState3, TOutput3>> aggregate3,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState4, TOutput4>> aggregate4,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState5, TOutput5>> aggregate5,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState6, TOutput6>> aggregate6,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState7, TOutput7>> aggregate7,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState8, TOutput8>> aggregate8,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState9, TOutput9>> aggregate9,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState10, TOutput10>> aggregate10,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState11, TOutput11>> aggregate11,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput7, TOutput8, TOutput9, TOutput10, TOutput11, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(aggregate5, nameof(aggregate5));
            Invariant.IsNotNull(aggregate6, nameof(aggregate6));
            Invariant.IsNotNull(aggregate7, nameof(aggregate7));
            Invariant.IsNotNull(aggregate8, nameof(aggregate8));
            Invariant.IsNotNull(aggregate9, nameof(aggregate9));
            Invariant.IsNotNull(aggregate10, nameof(aggregate10));
            Invariant.IsNotNull(aggregate11, nameof(aggregate11));
            Invariant.IsNotNull(merger, nameof(merger));

            var window = new Window<TKey, TInput>(source.Properties);
            var agg1 = aggregate1(window);
            var agg2 = aggregate2(window);
            var agg3 = aggregate3(window);
            var agg4 = aggregate4(window);
            var agg5 = aggregate5(window);
            var agg6 = aggregate6(window);
            var agg7 = aggregate7(window);
            var agg8 = aggregate8(window);
            var agg9 = aggregate9(window);
            var agg10 = aggregate10(window);
            var agg11 = aggregate11(window);
            var compound = AggregateFunctions.Combine(agg1, agg2, agg3, agg4, agg5, agg6, agg7, agg8, agg9, agg10, agg11, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, TOutput>(source, compound);
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot semantics on the input stream.
        /// </summary>
        internal static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TState7, TOutput7, TState8, TOutput8, TState9, TOutput9, TState10, TOutput10, TState11, TOutput11, TOutput>(
            this IStreamable<TKey, TInput> source,
            IAggregate<TInput, TState1, TOutput1> aggregate1,
            IAggregate<TInput, TState2, TOutput2> aggregate2,
            IAggregate<TInput, TState3, TOutput3> aggregate3,
            IAggregate<TInput, TState4, TOutput4> aggregate4,
            IAggregate<TInput, TState5, TOutput5> aggregate5,
            IAggregate<TInput, TState6, TOutput6> aggregate6,
            IAggregate<TInput, TState7, TOutput7> aggregate7,
            IAggregate<TInput, TState8, TOutput8> aggregate8,
            IAggregate<TInput, TState9, TOutput9> aggregate9,
            IAggregate<TInput, TState10, TOutput10> aggregate10,
            IAggregate<TInput, TState11, TOutput11> aggregate11,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput7, TOutput8, TOutput9, TOutput10, TOutput11, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(aggregate5, nameof(aggregate5));
            Invariant.IsNotNull(aggregate6, nameof(aggregate6));
            Invariant.IsNotNull(aggregate7, nameof(aggregate7));
            Invariant.IsNotNull(aggregate8, nameof(aggregate8));
            Invariant.IsNotNull(aggregate9, nameof(aggregate9));
            Invariant.IsNotNull(aggregate10, nameof(aggregate10));
            Invariant.IsNotNull(aggregate11, nameof(aggregate11));
            Invariant.IsNotNull(merger, nameof(merger));

            var compound = AggregateFunctions.Combine(aggregate1, aggregate2, aggregate3, aggregate4, aggregate5, aggregate6, aggregate7, aggregate8, aggregate9, aggregate10, aggregate11, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, TOutput>(source, compound);
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot windows on the input stream.
        /// </summary>
        /// <typeparam name="TKey">The type of the grouping key of the stream coming into the aggregation operation.</typeparam>
        /// <typeparam name="TInput">The type of the data payload in the input stream.</typeparam>
        /// <typeparam name="TState1">The type of the state object maintained by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TState2">The type of the state object maintained by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TState3">The type of the state object maintained by the aggregate operation in position 3.</typeparam>
        /// <typeparam name="TState4">The type of the state object maintained by the aggregate operation in position 4.</typeparam>
        /// <typeparam name="TState5">The type of the state object maintained by the aggregate operation in position 5.</typeparam>
        /// <typeparam name="TState6">The type of the state object maintained by the aggregate operation in position 6.</typeparam>
        /// <typeparam name="TState7">The type of the state object maintained by the aggregate operation in position 7.</typeparam>
        /// <typeparam name="TState8">The type of the state object maintained by the aggregate operation in position 8.</typeparam>
        /// <typeparam name="TState9">The type of the state object maintained by the aggregate operation in position 9.</typeparam>
        /// <typeparam name="TState10">The type of the state object maintained by the aggregate operation in position 10.</typeparam>
        /// <typeparam name="TState11">The type of the state object maintained by the aggregate operation in position 11.</typeparam>
        /// <typeparam name="TState12">The type of the state object maintained by the aggregate operation in position 12.</typeparam>
        /// <typeparam name="TOutput1">The type of the results generated by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TOutput2">The type of the results generated by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TOutput3">The type of the results generated by the aggregate operation in position 3.</typeparam>
        /// <typeparam name="TOutput4">The type of the results generated by the aggregate operation in position 4.</typeparam>
        /// <typeparam name="TOutput5">The type of the results generated by the aggregate operation in position 5.</typeparam>
        /// <typeparam name="TOutput6">The type of the results generated by the aggregate operation in position 6.</typeparam>
        /// <typeparam name="TOutput7">The type of the results generated by the aggregate operation in position 7.</typeparam>
        /// <typeparam name="TOutput8">The type of the results generated by the aggregate operation in position 8.</typeparam>
        /// <typeparam name="TOutput9">The type of the results generated by the aggregate operation in position 9.</typeparam>
        /// <typeparam name="TOutput10">The type of the results generated by the aggregate operation in position 10.</typeparam>
        /// <typeparam name="TOutput11">The type of the results generated by the aggregate operation in position 11.</typeparam>
        /// <typeparam name="TOutput12">The type of the results generated by the aggregate operation in position 12.</typeparam>
        /// <typeparam name="TOutput">The type of the payloads of the resulting stream.</typeparam>
        /// <param name="source">The stream over which to aggregate data.</param>
        /// <param name="aggregate1">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate2">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate3">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate4">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate5">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate6">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate7">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate8">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate9">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate10">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate11">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate12">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="merger">An expression describing how to merge the grouping key and the result of the aggregation into a result payload.</param>
        /// <returns>A stream of data with result payload type <typeparamref name="TOutput"/>.</returns>
        public static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TState7, TOutput7, TState8, TOutput8, TState9, TOutput9, TState10, TOutput10, TState11, TOutput11, TState12, TOutput12, TOutput>(
            this IStreamable<TKey, TInput> source,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState3, TOutput3>> aggregate3,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState4, TOutput4>> aggregate4,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState5, TOutput5>> aggregate5,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState6, TOutput6>> aggregate6,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState7, TOutput7>> aggregate7,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState8, TOutput8>> aggregate8,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState9, TOutput9>> aggregate9,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState10, TOutput10>> aggregate10,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState11, TOutput11>> aggregate11,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState12, TOutput12>> aggregate12,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput7, TOutput8, TOutput9, TOutput10, TOutput11, TOutput12, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(aggregate5, nameof(aggregate5));
            Invariant.IsNotNull(aggregate6, nameof(aggregate6));
            Invariant.IsNotNull(aggregate7, nameof(aggregate7));
            Invariant.IsNotNull(aggregate8, nameof(aggregate8));
            Invariant.IsNotNull(aggregate9, nameof(aggregate9));
            Invariant.IsNotNull(aggregate10, nameof(aggregate10));
            Invariant.IsNotNull(aggregate11, nameof(aggregate11));
            Invariant.IsNotNull(aggregate12, nameof(aggregate12));
            Invariant.IsNotNull(merger, nameof(merger));

            var window = new Window<TKey, TInput>(source.Properties);
            var agg1 = aggregate1(window);
            var agg2 = aggregate2(window);
            var agg3 = aggregate3(window);
            var agg4 = aggregate4(window);
            var agg5 = aggregate5(window);
            var agg6 = aggregate6(window);
            var agg7 = aggregate7(window);
            var agg8 = aggregate8(window);
            var agg9 = aggregate9(window);
            var agg10 = aggregate10(window);
            var agg11 = aggregate11(window);
            var agg12 = aggregate12(window);
            var compound = AggregateFunctions.Combine(agg1, agg2, agg3, agg4, agg5, agg6, agg7, agg8, agg9, agg10, agg11, agg12, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TOutput>(source, compound);
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot semantics on the input stream.
        /// </summary>
        internal static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TState7, TOutput7, TState8, TOutput8, TState9, TOutput9, TState10, TOutput10, TState11, TOutput11, TState12, TOutput12, TOutput>(
            this IStreamable<TKey, TInput> source,
            IAggregate<TInput, TState1, TOutput1> aggregate1,
            IAggregate<TInput, TState2, TOutput2> aggregate2,
            IAggregate<TInput, TState3, TOutput3> aggregate3,
            IAggregate<TInput, TState4, TOutput4> aggregate4,
            IAggregate<TInput, TState5, TOutput5> aggregate5,
            IAggregate<TInput, TState6, TOutput6> aggregate6,
            IAggregate<TInput, TState7, TOutput7> aggregate7,
            IAggregate<TInput, TState8, TOutput8> aggregate8,
            IAggregate<TInput, TState9, TOutput9> aggregate9,
            IAggregate<TInput, TState10, TOutput10> aggregate10,
            IAggregate<TInput, TState11, TOutput11> aggregate11,
            IAggregate<TInput, TState12, TOutput12> aggregate12,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput7, TOutput8, TOutput9, TOutput10, TOutput11, TOutput12, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(aggregate5, nameof(aggregate5));
            Invariant.IsNotNull(aggregate6, nameof(aggregate6));
            Invariant.IsNotNull(aggregate7, nameof(aggregate7));
            Invariant.IsNotNull(aggregate8, nameof(aggregate8));
            Invariant.IsNotNull(aggregate9, nameof(aggregate9));
            Invariant.IsNotNull(aggregate10, nameof(aggregate10));
            Invariant.IsNotNull(aggregate11, nameof(aggregate11));
            Invariant.IsNotNull(aggregate12, nameof(aggregate12));
            Invariant.IsNotNull(merger, nameof(merger));

            var compound = AggregateFunctions.Combine(aggregate1, aggregate2, aggregate3, aggregate4, aggregate5, aggregate6, aggregate7, aggregate8, aggregate9, aggregate10, aggregate11, aggregate12, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TOutput>(source, compound);
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot windows on the input stream.
        /// </summary>
        /// <typeparam name="TKey">The type of the grouping key of the stream coming into the aggregation operation.</typeparam>
        /// <typeparam name="TInput">The type of the data payload in the input stream.</typeparam>
        /// <typeparam name="TState1">The type of the state object maintained by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TState2">The type of the state object maintained by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TState3">The type of the state object maintained by the aggregate operation in position 3.</typeparam>
        /// <typeparam name="TState4">The type of the state object maintained by the aggregate operation in position 4.</typeparam>
        /// <typeparam name="TState5">The type of the state object maintained by the aggregate operation in position 5.</typeparam>
        /// <typeparam name="TState6">The type of the state object maintained by the aggregate operation in position 6.</typeparam>
        /// <typeparam name="TState7">The type of the state object maintained by the aggregate operation in position 7.</typeparam>
        /// <typeparam name="TState8">The type of the state object maintained by the aggregate operation in position 8.</typeparam>
        /// <typeparam name="TState9">The type of the state object maintained by the aggregate operation in position 9.</typeparam>
        /// <typeparam name="TState10">The type of the state object maintained by the aggregate operation in position 10.</typeparam>
        /// <typeparam name="TState11">The type of the state object maintained by the aggregate operation in position 11.</typeparam>
        /// <typeparam name="TState12">The type of the state object maintained by the aggregate operation in position 12.</typeparam>
        /// <typeparam name="TState13">The type of the state object maintained by the aggregate operation in position 13.</typeparam>
        /// <typeparam name="TOutput1">The type of the results generated by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TOutput2">The type of the results generated by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TOutput3">The type of the results generated by the aggregate operation in position 3.</typeparam>
        /// <typeparam name="TOutput4">The type of the results generated by the aggregate operation in position 4.</typeparam>
        /// <typeparam name="TOutput5">The type of the results generated by the aggregate operation in position 5.</typeparam>
        /// <typeparam name="TOutput6">The type of the results generated by the aggregate operation in position 6.</typeparam>
        /// <typeparam name="TOutput7">The type of the results generated by the aggregate operation in position 7.</typeparam>
        /// <typeparam name="TOutput8">The type of the results generated by the aggregate operation in position 8.</typeparam>
        /// <typeparam name="TOutput9">The type of the results generated by the aggregate operation in position 9.</typeparam>
        /// <typeparam name="TOutput10">The type of the results generated by the aggregate operation in position 10.</typeparam>
        /// <typeparam name="TOutput11">The type of the results generated by the aggregate operation in position 11.</typeparam>
        /// <typeparam name="TOutput12">The type of the results generated by the aggregate operation in position 12.</typeparam>
        /// <typeparam name="TOutput13">The type of the results generated by the aggregate operation in position 13.</typeparam>
        /// <typeparam name="TOutput">The type of the payloads of the resulting stream.</typeparam>
        /// <param name="source">The stream over which to aggregate data.</param>
        /// <param name="aggregate1">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate2">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate3">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate4">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate5">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate6">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate7">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate8">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate9">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate10">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate11">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate12">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate13">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="merger">An expression describing how to merge the grouping key and the result of the aggregation into a result payload.</param>
        /// <returns>A stream of data with result payload type <typeparamref name="TOutput"/>.</returns>
        public static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TState7, TOutput7, TState8, TOutput8, TState9, TOutput9, TState10, TOutput10, TState11, TOutput11, TState12, TOutput12, TState13, TOutput13, TOutput>(
            this IStreamable<TKey, TInput> source,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState3, TOutput3>> aggregate3,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState4, TOutput4>> aggregate4,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState5, TOutput5>> aggregate5,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState6, TOutput6>> aggregate6,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState7, TOutput7>> aggregate7,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState8, TOutput8>> aggregate8,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState9, TOutput9>> aggregate9,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState10, TOutput10>> aggregate10,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState11, TOutput11>> aggregate11,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState12, TOutput12>> aggregate12,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState13, TOutput13>> aggregate13,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput7, TOutput8, TOutput9, TOutput10, TOutput11, TOutput12, TOutput13, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(aggregate5, nameof(aggregate5));
            Invariant.IsNotNull(aggregate6, nameof(aggregate6));
            Invariant.IsNotNull(aggregate7, nameof(aggregate7));
            Invariant.IsNotNull(aggregate8, nameof(aggregate8));
            Invariant.IsNotNull(aggregate9, nameof(aggregate9));
            Invariant.IsNotNull(aggregate10, nameof(aggregate10));
            Invariant.IsNotNull(aggregate11, nameof(aggregate11));
            Invariant.IsNotNull(aggregate12, nameof(aggregate12));
            Invariant.IsNotNull(aggregate13, nameof(aggregate13));
            Invariant.IsNotNull(merger, nameof(merger));

            var window = new Window<TKey, TInput>(source.Properties);
            var agg1 = aggregate1(window);
            var agg2 = aggregate2(window);
            var agg3 = aggregate3(window);
            var agg4 = aggregate4(window);
            var agg5 = aggregate5(window);
            var agg6 = aggregate6(window);
            var agg7 = aggregate7(window);
            var agg8 = aggregate8(window);
            var agg9 = aggregate9(window);
            var agg10 = aggregate10(window);
            var agg11 = aggregate11(window);
            var agg12 = aggregate12(window);
            var agg13 = aggregate13(window);
            var compound = AggregateFunctions.Combine(agg1, agg2, agg3, agg4, agg5, agg6, agg7, agg8, agg9, agg10, agg11, agg12, agg13, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TOutput>(source, compound);
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot semantics on the input stream.
        /// </summary>
        internal static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TState7, TOutput7, TState8, TOutput8, TState9, TOutput9, TState10, TOutput10, TState11, TOutput11, TState12, TOutput12, TState13, TOutput13, TOutput>(
            this IStreamable<TKey, TInput> source,
            IAggregate<TInput, TState1, TOutput1> aggregate1,
            IAggregate<TInput, TState2, TOutput2> aggregate2,
            IAggregate<TInput, TState3, TOutput3> aggregate3,
            IAggregate<TInput, TState4, TOutput4> aggregate4,
            IAggregate<TInput, TState5, TOutput5> aggregate5,
            IAggregate<TInput, TState6, TOutput6> aggregate6,
            IAggregate<TInput, TState7, TOutput7> aggregate7,
            IAggregate<TInput, TState8, TOutput8> aggregate8,
            IAggregate<TInput, TState9, TOutput9> aggregate9,
            IAggregate<TInput, TState10, TOutput10> aggregate10,
            IAggregate<TInput, TState11, TOutput11> aggregate11,
            IAggregate<TInput, TState12, TOutput12> aggregate12,
            IAggregate<TInput, TState13, TOutput13> aggregate13,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput7, TOutput8, TOutput9, TOutput10, TOutput11, TOutput12, TOutput13, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(aggregate5, nameof(aggregate5));
            Invariant.IsNotNull(aggregate6, nameof(aggregate6));
            Invariant.IsNotNull(aggregate7, nameof(aggregate7));
            Invariant.IsNotNull(aggregate8, nameof(aggregate8));
            Invariant.IsNotNull(aggregate9, nameof(aggregate9));
            Invariant.IsNotNull(aggregate10, nameof(aggregate10));
            Invariant.IsNotNull(aggregate11, nameof(aggregate11));
            Invariant.IsNotNull(aggregate12, nameof(aggregate12));
            Invariant.IsNotNull(aggregate13, nameof(aggregate13));
            Invariant.IsNotNull(merger, nameof(merger));

            var compound = AggregateFunctions.Combine(aggregate1, aggregate2, aggregate3, aggregate4, aggregate5, aggregate6, aggregate7, aggregate8, aggregate9, aggregate10, aggregate11, aggregate12, aggregate13, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TOutput>(source, compound);
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot windows on the input stream.
        /// </summary>
        /// <typeparam name="TKey">The type of the grouping key of the stream coming into the aggregation operation.</typeparam>
        /// <typeparam name="TInput">The type of the data payload in the input stream.</typeparam>
        /// <typeparam name="TState1">The type of the state object maintained by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TState2">The type of the state object maintained by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TState3">The type of the state object maintained by the aggregate operation in position 3.</typeparam>
        /// <typeparam name="TState4">The type of the state object maintained by the aggregate operation in position 4.</typeparam>
        /// <typeparam name="TState5">The type of the state object maintained by the aggregate operation in position 5.</typeparam>
        /// <typeparam name="TState6">The type of the state object maintained by the aggregate operation in position 6.</typeparam>
        /// <typeparam name="TState7">The type of the state object maintained by the aggregate operation in position 7.</typeparam>
        /// <typeparam name="TState8">The type of the state object maintained by the aggregate operation in position 8.</typeparam>
        /// <typeparam name="TState9">The type of the state object maintained by the aggregate operation in position 9.</typeparam>
        /// <typeparam name="TState10">The type of the state object maintained by the aggregate operation in position 10.</typeparam>
        /// <typeparam name="TState11">The type of the state object maintained by the aggregate operation in position 11.</typeparam>
        /// <typeparam name="TState12">The type of the state object maintained by the aggregate operation in position 12.</typeparam>
        /// <typeparam name="TState13">The type of the state object maintained by the aggregate operation in position 13.</typeparam>
        /// <typeparam name="TState14">The type of the state object maintained by the aggregate operation in position 14.</typeparam>
        /// <typeparam name="TOutput1">The type of the results generated by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TOutput2">The type of the results generated by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TOutput3">The type of the results generated by the aggregate operation in position 3.</typeparam>
        /// <typeparam name="TOutput4">The type of the results generated by the aggregate operation in position 4.</typeparam>
        /// <typeparam name="TOutput5">The type of the results generated by the aggregate operation in position 5.</typeparam>
        /// <typeparam name="TOutput6">The type of the results generated by the aggregate operation in position 6.</typeparam>
        /// <typeparam name="TOutput7">The type of the results generated by the aggregate operation in position 7.</typeparam>
        /// <typeparam name="TOutput8">The type of the results generated by the aggregate operation in position 8.</typeparam>
        /// <typeparam name="TOutput9">The type of the results generated by the aggregate operation in position 9.</typeparam>
        /// <typeparam name="TOutput10">The type of the results generated by the aggregate operation in position 10.</typeparam>
        /// <typeparam name="TOutput11">The type of the results generated by the aggregate operation in position 11.</typeparam>
        /// <typeparam name="TOutput12">The type of the results generated by the aggregate operation in position 12.</typeparam>
        /// <typeparam name="TOutput13">The type of the results generated by the aggregate operation in position 13.</typeparam>
        /// <typeparam name="TOutput14">The type of the results generated by the aggregate operation in position 14.</typeparam>
        /// <typeparam name="TOutput">The type of the payloads of the resulting stream.</typeparam>
        /// <param name="source">The stream over which to aggregate data.</param>
        /// <param name="aggregate1">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate2">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate3">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate4">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate5">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate6">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate7">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate8">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate9">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate10">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate11">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate12">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate13">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate14">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="merger">An expression describing how to merge the grouping key and the result of the aggregation into a result payload.</param>
        /// <returns>A stream of data with result payload type <typeparamref name="TOutput"/>.</returns>
        public static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TState7, TOutput7, TState8, TOutput8, TState9, TOutput9, TState10, TOutput10, TState11, TOutput11, TState12, TOutput12, TState13, TOutput13, TState14, TOutput14, TOutput>(
            this IStreamable<TKey, TInput> source,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState3, TOutput3>> aggregate3,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState4, TOutput4>> aggregate4,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState5, TOutput5>> aggregate5,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState6, TOutput6>> aggregate6,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState7, TOutput7>> aggregate7,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState8, TOutput8>> aggregate8,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState9, TOutput9>> aggregate9,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState10, TOutput10>> aggregate10,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState11, TOutput11>> aggregate11,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState12, TOutput12>> aggregate12,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState13, TOutput13>> aggregate13,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState14, TOutput14>> aggregate14,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput7, TOutput8, TOutput9, TOutput10, TOutput11, TOutput12, TOutput13, TOutput14, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(aggregate5, nameof(aggregate5));
            Invariant.IsNotNull(aggregate6, nameof(aggregate6));
            Invariant.IsNotNull(aggregate7, nameof(aggregate7));
            Invariant.IsNotNull(aggregate8, nameof(aggregate8));
            Invariant.IsNotNull(aggregate9, nameof(aggregate9));
            Invariant.IsNotNull(aggregate10, nameof(aggregate10));
            Invariant.IsNotNull(aggregate11, nameof(aggregate11));
            Invariant.IsNotNull(aggregate12, nameof(aggregate12));
            Invariant.IsNotNull(aggregate13, nameof(aggregate13));
            Invariant.IsNotNull(aggregate14, nameof(aggregate14));
            Invariant.IsNotNull(merger, nameof(merger));

            var window = new Window<TKey, TInput>(source.Properties);
            var agg1 = aggregate1(window);
            var agg2 = aggregate2(window);
            var agg3 = aggregate3(window);
            var agg4 = aggregate4(window);
            var agg5 = aggregate5(window);
            var agg6 = aggregate6(window);
            var agg7 = aggregate7(window);
            var agg8 = aggregate8(window);
            var agg9 = aggregate9(window);
            var agg10 = aggregate10(window);
            var agg11 = aggregate11(window);
            var agg12 = aggregate12(window);
            var agg13 = aggregate13(window);
            var agg14 = aggregate14(window);
            var compound = AggregateFunctions.Combine(agg1, agg2, agg3, agg4, agg5, agg6, agg7, agg8, agg9, agg10, agg11, agg12, agg13, agg14, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TOutput>(source, compound);
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot semantics on the input stream.
        /// </summary>
        internal static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TState7, TOutput7, TState8, TOutput8, TState9, TOutput9, TState10, TOutput10, TState11, TOutput11, TState12, TOutput12, TState13, TOutput13, TState14, TOutput14, TOutput>(
            this IStreamable<TKey, TInput> source,
            IAggregate<TInput, TState1, TOutput1> aggregate1,
            IAggregate<TInput, TState2, TOutput2> aggregate2,
            IAggregate<TInput, TState3, TOutput3> aggregate3,
            IAggregate<TInput, TState4, TOutput4> aggregate4,
            IAggregate<TInput, TState5, TOutput5> aggregate5,
            IAggregate<TInput, TState6, TOutput6> aggregate6,
            IAggregate<TInput, TState7, TOutput7> aggregate7,
            IAggregate<TInput, TState8, TOutput8> aggregate8,
            IAggregate<TInput, TState9, TOutput9> aggregate9,
            IAggregate<TInput, TState10, TOutput10> aggregate10,
            IAggregate<TInput, TState11, TOutput11> aggregate11,
            IAggregate<TInput, TState12, TOutput12> aggregate12,
            IAggregate<TInput, TState13, TOutput13> aggregate13,
            IAggregate<TInput, TState14, TOutput14> aggregate14,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput7, TOutput8, TOutput9, TOutput10, TOutput11, TOutput12, TOutput13, TOutput14, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(aggregate5, nameof(aggregate5));
            Invariant.IsNotNull(aggregate6, nameof(aggregate6));
            Invariant.IsNotNull(aggregate7, nameof(aggregate7));
            Invariant.IsNotNull(aggregate8, nameof(aggregate8));
            Invariant.IsNotNull(aggregate9, nameof(aggregate9));
            Invariant.IsNotNull(aggregate10, nameof(aggregate10));
            Invariant.IsNotNull(aggregate11, nameof(aggregate11));
            Invariant.IsNotNull(aggregate12, nameof(aggregate12));
            Invariant.IsNotNull(aggregate13, nameof(aggregate13));
            Invariant.IsNotNull(aggregate14, nameof(aggregate14));
            Invariant.IsNotNull(merger, nameof(merger));

            var compound = AggregateFunctions.Combine(aggregate1, aggregate2, aggregate3, aggregate4, aggregate5, aggregate6, aggregate7, aggregate8, aggregate9, aggregate10, aggregate11, aggregate12, aggregate13, aggregate14, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TOutput>(source, compound);
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot windows on the input stream.
        /// </summary>
        /// <typeparam name="TKey">The type of the grouping key of the stream coming into the aggregation operation.</typeparam>
        /// <typeparam name="TInput">The type of the data payload in the input stream.</typeparam>
        /// <typeparam name="TState1">The type of the state object maintained by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TState2">The type of the state object maintained by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TState3">The type of the state object maintained by the aggregate operation in position 3.</typeparam>
        /// <typeparam name="TState4">The type of the state object maintained by the aggregate operation in position 4.</typeparam>
        /// <typeparam name="TState5">The type of the state object maintained by the aggregate operation in position 5.</typeparam>
        /// <typeparam name="TState6">The type of the state object maintained by the aggregate operation in position 6.</typeparam>
        /// <typeparam name="TState7">The type of the state object maintained by the aggregate operation in position 7.</typeparam>
        /// <typeparam name="TState8">The type of the state object maintained by the aggregate operation in position 8.</typeparam>
        /// <typeparam name="TState9">The type of the state object maintained by the aggregate operation in position 9.</typeparam>
        /// <typeparam name="TState10">The type of the state object maintained by the aggregate operation in position 10.</typeparam>
        /// <typeparam name="TState11">The type of the state object maintained by the aggregate operation in position 11.</typeparam>
        /// <typeparam name="TState12">The type of the state object maintained by the aggregate operation in position 12.</typeparam>
        /// <typeparam name="TState13">The type of the state object maintained by the aggregate operation in position 13.</typeparam>
        /// <typeparam name="TState14">The type of the state object maintained by the aggregate operation in position 14.</typeparam>
        /// <typeparam name="TState15">The type of the state object maintained by the aggregate operation in position 15.</typeparam>
        /// <typeparam name="TOutput1">The type of the results generated by the aggregate operation in position 1.</typeparam>
        /// <typeparam name="TOutput2">The type of the results generated by the aggregate operation in position 2.</typeparam>
        /// <typeparam name="TOutput3">The type of the results generated by the aggregate operation in position 3.</typeparam>
        /// <typeparam name="TOutput4">The type of the results generated by the aggregate operation in position 4.</typeparam>
        /// <typeparam name="TOutput5">The type of the results generated by the aggregate operation in position 5.</typeparam>
        /// <typeparam name="TOutput6">The type of the results generated by the aggregate operation in position 6.</typeparam>
        /// <typeparam name="TOutput7">The type of the results generated by the aggregate operation in position 7.</typeparam>
        /// <typeparam name="TOutput8">The type of the results generated by the aggregate operation in position 8.</typeparam>
        /// <typeparam name="TOutput9">The type of the results generated by the aggregate operation in position 9.</typeparam>
        /// <typeparam name="TOutput10">The type of the results generated by the aggregate operation in position 10.</typeparam>
        /// <typeparam name="TOutput11">The type of the results generated by the aggregate operation in position 11.</typeparam>
        /// <typeparam name="TOutput12">The type of the results generated by the aggregate operation in position 12.</typeparam>
        /// <typeparam name="TOutput13">The type of the results generated by the aggregate operation in position 13.</typeparam>
        /// <typeparam name="TOutput14">The type of the results generated by the aggregate operation in position 14.</typeparam>
        /// <typeparam name="TOutput15">The type of the results generated by the aggregate operation in position 15.</typeparam>
        /// <typeparam name="TOutput">The type of the payloads of the resulting stream.</typeparam>
        /// <param name="source">The stream over which to aggregate data.</param>
        /// <param name="aggregate1">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate2">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate3">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate4">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate5">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate6">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate7">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate8">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate9">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate10">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate11">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate12">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate13">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate14">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="aggregate15">A function from a window to an aggregate object. Used to give the developer an autocomplete experience in Visual Studio to pick from a set of available aggregates.</param>
        /// <param name="merger">An expression describing how to merge the grouping key and the result of the aggregation into a result payload.</param>
        /// <returns>A stream of data with result payload type <typeparamref name="TOutput"/>.</returns>
        public static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TState7, TOutput7, TState8, TOutput8, TState9, TOutput9, TState10, TOutput10, TState11, TOutput11, TState12, TOutput12, TState13, TOutput13, TState14, TOutput14, TState15, TOutput15, TOutput>(
            this IStreamable<TKey, TInput> source,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState3, TOutput3>> aggregate3,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState4, TOutput4>> aggregate4,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState5, TOutput5>> aggregate5,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState6, TOutput6>> aggregate6,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState7, TOutput7>> aggregate7,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState8, TOutput8>> aggregate8,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState9, TOutput9>> aggregate9,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState10, TOutput10>> aggregate10,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState11, TOutput11>> aggregate11,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState12, TOutput12>> aggregate12,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState13, TOutput13>> aggregate13,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState14, TOutput14>> aggregate14,
            Func<Window<TKey, TInput>, IAggregate<TInput, TState15, TOutput15>> aggregate15,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput7, TOutput8, TOutput9, TOutput10, TOutput11, TOutput12, TOutput13, TOutput14, TOutput15, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(aggregate5, nameof(aggregate5));
            Invariant.IsNotNull(aggregate6, nameof(aggregate6));
            Invariant.IsNotNull(aggregate7, nameof(aggregate7));
            Invariant.IsNotNull(aggregate8, nameof(aggregate8));
            Invariant.IsNotNull(aggregate9, nameof(aggregate9));
            Invariant.IsNotNull(aggregate10, nameof(aggregate10));
            Invariant.IsNotNull(aggregate11, nameof(aggregate11));
            Invariant.IsNotNull(aggregate12, nameof(aggregate12));
            Invariant.IsNotNull(aggregate13, nameof(aggregate13));
            Invariant.IsNotNull(aggregate14, nameof(aggregate14));
            Invariant.IsNotNull(aggregate15, nameof(aggregate15));
            Invariant.IsNotNull(merger, nameof(merger));

            var window = new Window<TKey, TInput>(source.Properties);
            var agg1 = aggregate1(window);
            var agg2 = aggregate2(window);
            var agg3 = aggregate3(window);
            var agg4 = aggregate4(window);
            var agg5 = aggregate5(window);
            var agg6 = aggregate6(window);
            var agg7 = aggregate7(window);
            var agg8 = aggregate8(window);
            var agg9 = aggregate9(window);
            var agg10 = aggregate10(window);
            var agg11 = aggregate11(window);
            var agg12 = aggregate12(window);
            var agg13 = aggregate13(window);
            var agg14 = aggregate14(window);
            var agg15 = aggregate15(window);
            var compound = AggregateFunctions.Combine(agg1, agg2, agg3, agg4, agg5, agg6, agg7, agg8, agg9, agg10, agg11, agg12, agg13, agg14, agg15, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TOutput>(source, compound);
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot semantics on the input stream.
        /// </summary>
        internal static IStreamable<TKey, TOutput> Aggregate<TKey, TInput, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TState7, TOutput7, TState8, TOutput8, TState9, TOutput9, TState10, TOutput10, TState11, TOutput11, TState12, TOutput12, TState13, TOutput13, TState14, TOutput14, TState15, TOutput15, TOutput>(
            this IStreamable<TKey, TInput> source,
            IAggregate<TInput, TState1, TOutput1> aggregate1,
            IAggregate<TInput, TState2, TOutput2> aggregate2,
            IAggregate<TInput, TState3, TOutput3> aggregate3,
            IAggregate<TInput, TState4, TOutput4> aggregate4,
            IAggregate<TInput, TState5, TOutput5> aggregate5,
            IAggregate<TInput, TState6, TOutput6> aggregate6,
            IAggregate<TInput, TState7, TOutput7> aggregate7,
            IAggregate<TInput, TState8, TOutput8> aggregate8,
            IAggregate<TInput, TState9, TOutput9> aggregate9,
            IAggregate<TInput, TState10, TOutput10> aggregate10,
            IAggregate<TInput, TState11, TOutput11> aggregate11,
            IAggregate<TInput, TState12, TOutput12> aggregate12,
            IAggregate<TInput, TState13, TOutput13> aggregate13,
            IAggregate<TInput, TState14, TOutput14> aggregate14,
            IAggregate<TInput, TState15, TOutput15> aggregate15,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput7, TOutput8, TOutput9, TOutput10, TOutput11, TOutput12, TOutput13, TOutput14, TOutput15, TOutput>> merger)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(aggregate5, nameof(aggregate5));
            Invariant.IsNotNull(aggregate6, nameof(aggregate6));
            Invariant.IsNotNull(aggregate7, nameof(aggregate7));
            Invariant.IsNotNull(aggregate8, nameof(aggregate8));
            Invariant.IsNotNull(aggregate9, nameof(aggregate9));
            Invariant.IsNotNull(aggregate10, nameof(aggregate10));
            Invariant.IsNotNull(aggregate11, nameof(aggregate11));
            Invariant.IsNotNull(aggregate12, nameof(aggregate12));
            Invariant.IsNotNull(aggregate13, nameof(aggregate13));
            Invariant.IsNotNull(aggregate14, nameof(aggregate14));
            Invariant.IsNotNull(aggregate15, nameof(aggregate15));
            Invariant.IsNotNull(merger, nameof(merger));

            var compound = AggregateFunctions.Combine(aggregate1, aggregate2, aggregate3, aggregate4, aggregate5, aggregate6, aggregate7, aggregate8, aggregate9, aggregate10, aggregate11, aggregate12, aggregate13, aggregate14, aggregate15, merger);
            return new SnapshotWindowStreamable<TKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TOutput>(source, compound);
        }
    }
}