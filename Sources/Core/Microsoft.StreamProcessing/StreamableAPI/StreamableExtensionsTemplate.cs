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
        /// Computes a time-sensitive sum aggregate over sbytes using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, sbyte> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, sbyte>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over bytes using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, byte> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, byte>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over shorts using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, short> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, short>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over ushorts using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, ushort> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ushort>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over ints using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, int> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, int>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over uints using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, uint> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, uint>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over longs using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, long> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, long>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over ulongs using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, ulong> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ulong>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over floats using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, float> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, float>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over doubles using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, double> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, double>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over decimals using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, decimal> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, decimal>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over BigIntegers using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, BigInteger> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, BigInteger>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over Complexs using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, Complex> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, Complex>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable sbytes using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, sbyte> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, sbyte?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable bytes using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, byte> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, byte?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable shorts using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, short> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, short?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable ushorts using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, ushort> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ushort?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable ints using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, int> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, int?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable uints using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, uint> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, uint?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable longs using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, long> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, long?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable ulongs using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, ulong> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ulong?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable floats using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, float> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, float?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable doubles using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, double> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, double?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable decimals using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, decimal> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, decimal?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable BigIntegers using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, BigInteger> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, BigInteger?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum aggregate over nullable Complexs using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, Complex> Sum<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, Complex?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Sum(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over sbytes using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, sbyte> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, sbyte>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over bytes using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, byte> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, byte>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over shorts using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, short> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, short>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over ushorts using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, ushort> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ushort>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over ints using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, int> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, int>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over uints using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, uint> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, uint>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over longs using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, long> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, long>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over ulongs using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, ulong> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ulong>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over floats using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, float> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, float>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over doubles using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, double> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, double>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over decimals using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, decimal> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, decimal>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over BigIntegers using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, BigInteger> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, BigInteger>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over Complexs using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, Complex> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, Complex>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over nullable sbytes using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, sbyte> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, sbyte?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over nullable bytes using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, byte> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, byte?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over nullable shorts using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, short> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, short?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over nullable ushorts using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, ushort> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ushort?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over nullable ints using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, int> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, int?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over nullable uints using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, uint> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, uint?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over nullable longs using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, long> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, long?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over nullable ulongs using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, ulong> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ulong?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over nullable floats using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, float> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, float?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over nullable doubles using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, double> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, double?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over nullable decimals using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, decimal> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, decimal?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over nullable BigIntegers using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, BigInteger> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, BigInteger?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive sum of squares aggregate over nullable Complexs using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, Complex> SumSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, Complex?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.SumSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over sbytes using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, sbyte> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, sbyte>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over bytes using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, byte> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, byte>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over shorts using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, short> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, short>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over ushorts using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, ushort> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ushort>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over ints using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, int> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, int>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over uints using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, uint> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, uint>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over longs using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, long> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, long>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over ulongs using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, ulong> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ulong>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over floats using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, float> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, float>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over doubles using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, double> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, double>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over decimals using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, decimal> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, decimal>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over BigIntegers using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, BigInteger> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, BigInteger>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over Complexs using "snapshot windows" (SI terminology).
        /// </summary>
        public static IStreamable<TKey, Complex> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, Complex>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over nullable sbytes using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, sbyte> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, sbyte?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over nullable bytes using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, byte> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, byte?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over nullable shorts using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, short> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, short?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over nullable ushorts using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, ushort> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ushort?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over nullable ints using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, int> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, int?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over nullable uints using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, uint> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, uint?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over nullable longs using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, long> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, long?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over nullable ulongs using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, ulong> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ulong?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over nullable floats using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, float> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, float?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over nullable doubles using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, double> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, double?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over nullable decimals using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, decimal> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, decimal?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over nullable BigIntegers using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, BigInteger> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, BigInteger?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive product aggregate over nullable Complexs using "snapshot windows" (SI terminology). Note that nulls have
        /// no affect on the sum.
        /// </summary>
        public static IStreamable<TKey, Complex> Product<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, Complex?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Product(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over sbytes using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a long datatype.
        /// </summary>
        public static IStreamable<TKey, double> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, sbyte>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over shorts using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a long datatype.
        /// </summary>
        public static IStreamable<TKey, double> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, short>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over ints using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a long datatype.
        /// </summary>
        public static IStreamable<TKey, double> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, int>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over longs using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a long datatype.
        /// </summary>
        public static IStreamable<TKey, double> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, long>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over bytes using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a ulong datatype.
        /// </summary>
        public static IStreamable<TKey, double> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, byte>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over ushorts using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a ulong datatype.
        /// </summary>
        public static IStreamable<TKey, double> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ushort>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over uints using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a ulong datatype.
        /// </summary>
        public static IStreamable<TKey, double> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, uint>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over ulongs using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a ulong datatype.
        /// </summary>
        public static IStreamable<TKey, double> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ulong>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over floats using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a float datatype.
        /// </summary>
        public static IStreamable<TKey, float> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, float>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over doubles using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a double datatype.
        /// </summary>
        public static IStreamable<TKey, double> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, double>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over decimals using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a decimal datatype.
        /// </summary>
        public static IStreamable<TKey, decimal> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, decimal>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over BigIntegers using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a BigInteger datatype.
        /// </summary>
        public static IStreamable<TKey, double> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, BigInteger>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over Complexs using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a Complex datatype.
        /// </summary>
        public static IStreamable<TKey, Complex> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, Complex>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable sbytes using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a long datatype and that nulls have no affect on the average.
        /// </summary>
        public static IStreamable<TKey, double?> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, sbyte?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable shorts using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a long datatype and that nulls have no affect on the average.
        /// </summary>
        public static IStreamable<TKey, double?> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, short?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable ints using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a long datatype and that nulls have no affect on the average.
        /// </summary>
        public static IStreamable<TKey, double?> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, int?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable longs using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a long datatype and that nulls have no affect on the average.
        /// </summary>
        public static IStreamable<TKey, double?> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, long?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable bytes using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a ulong datatype and that nulls have no affect on the average.
        /// </summary>
        public static IStreamable<TKey, double?> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, byte?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable ushorts using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a ulong datatype and that nulls have no affect on the average.
        /// </summary>
        public static IStreamable<TKey, double?> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ushort?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable uints using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a ulong datatype and that nulls have no affect on the average.
        /// </summary>
        public static IStreamable<TKey, double?> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, uint?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable ulongs using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a ulong datatype and that nulls have no affect on the average.
        /// </summary>
        public static IStreamable<TKey, double?> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ulong?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable floats using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a float datatype and that nulls have no affect on the average.
        /// </summary>
        public static IStreamable<TKey, float?> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, float?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable doubles using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a double datatype and that nulls have no affect on the average.
        /// </summary>
        public static IStreamable<TKey, double?> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, double?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable decimals using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a decimal datatype and that nulls have no affect on the average.
        /// </summary>
        public static IStreamable<TKey, decimal?> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, decimal?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable BigIntegers using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a BigInteger datatype and that nulls have no affect on the average.
        /// </summary>
        public static IStreamable<TKey, double?> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, BigInteger?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average aggregate over nullable Complexs using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a Complex datatype and that nulls have no affect on the average.
        /// </summary>
        public static IStreamable<TKey, Complex?> Average<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, Complex?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.Average(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over sbytes using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a long datatype.
        /// </summary>
        public static IStreamable<TKey, double> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, sbyte>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over shorts using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a long datatype.
        /// </summary>
        public static IStreamable<TKey, double> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, short>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over ints using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a long datatype.
        /// </summary>
        public static IStreamable<TKey, double> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, int>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over longs using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a long datatype.
        /// </summary>
        public static IStreamable<TKey, double> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, long>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over bytes using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a ulong datatype.
        /// </summary>
        public static IStreamable<TKey, double> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, byte>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over ushorts using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a ulong datatype.
        /// </summary>
        public static IStreamable<TKey, double> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ushort>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over uints using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a ulong datatype.
        /// </summary>
        public static IStreamable<TKey, double> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, uint>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over ulongs using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a ulong datatype.
        /// </summary>
        public static IStreamable<TKey, double> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ulong>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over floats using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a float datatype.
        /// </summary>
        public static IStreamable<TKey, float> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, float>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over doubles using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a double datatype.
        /// </summary>
        public static IStreamable<TKey, double> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, double>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over decimals using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a decimal datatype.
        /// </summary>
        public static IStreamable<TKey, decimal> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, decimal>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over BigIntegers using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a BigInteger datatype.
        /// </summary>
        public static IStreamable<TKey, double> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, BigInteger>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over Complexs using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a Complex datatype.
        /// </summary>
        public static IStreamable<TKey, Complex> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, Complex>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over nullable sbytes using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a long datatype and that nulls have no affect on the average.
        /// </summary>
        public static IStreamable<TKey, double?> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, sbyte?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over nullable shorts using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a long datatype and that nulls have no affect on the average.
        /// </summary>
        public static IStreamable<TKey, double?> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, short?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over nullable ints using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a long datatype and that nulls have no affect on the average.
        /// </summary>
        public static IStreamable<TKey, double?> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, int?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over nullable longs using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a long datatype and that nulls have no affect on the average.
        /// </summary>
        public static IStreamable<TKey, double?> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, long?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over nullable bytes using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a ulong datatype and that nulls have no affect on the average.
        /// </summary>
        public static IStreamable<TKey, double?> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, byte?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over nullable ushorts using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a ulong datatype and that nulls have no affect on the average.
        /// </summary>
        public static IStreamable<TKey, double?> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ushort?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over nullable uints using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a ulong datatype and that nulls have no affect on the average.
        /// </summary>
        public static IStreamable<TKey, double?> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, uint?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over nullable ulongs using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a ulong datatype and that nulls have no affect on the average.
        /// </summary>
        public static IStreamable<TKey, double?> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, ulong?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over nullable floats using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a float datatype and that nulls have no affect on the average.
        /// </summary>
        public static IStreamable<TKey, float?> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, float?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over nullable doubles using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a double datatype and that nulls have no affect on the average.
        /// </summary>
        public static IStreamable<TKey, double?> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, double?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over nullable decimals using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a decimal datatype and that nulls have no affect on the average.
        /// </summary>
        public static IStreamable<TKey, decimal?> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, decimal?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over nullable BigIntegers using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a BigInteger datatype and that nulls have no affect on the average.
        /// </summary>
        public static IStreamable<TKey, double?> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, BigInteger?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Computes a time-sensitive average of squares aggregate over nullable Complexs using "snapshot windows" (SI terminology). Note that the accumulator
        /// internally is a Complex datatype and that nulls have no affect on the average.
        /// </summary>
        public static IStreamable<TKey, Complex?> AverageSquares<TKey, TPayload>(this IStreamable<TKey, TPayload> source, Expression<Func<TPayload, Complex?>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));
            return source.Aggregate(w => w.AverageSquares(selector));
        }

        /// <summary>
        /// Applies multiple aggregates to snapshot windows on the input stream.
        /// </summary>
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
        /// Applies multiple aggregates to snapshot windows on the input stream relative to the selected grouping key.
        /// </summary>
        public static IStreamable<Empty, TResult> AggregateByKey<TInput, TInnerKey, TState1, TOutput1, TState2, TOutput2, TOutput, TResult>(
            this IStreamable<Empty, TInput> source,
            Expression<Func<TInput, TInnerKey>> keySelector,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Expression<Func<TOutput1, TOutput2, TOutput>> merger,
            Expression<Func<TInnerKey, TOutput, TResult>> resultSelector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(keySelector, nameof(keySelector));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(merger, nameof(merger));

            var window = new Window<CompoundGroupKey<Empty, TInnerKey>, TInput>(source.Properties.GroupNested(keySelector));
            var agg1 = aggregate1(window);
            var agg2 = aggregate2(window);
            var compound = AggregateFunctions.Combine(agg1, agg2, merger);

            return new GroupedWindowStreamable<TInnerKey, TInput, StructTuple<TState1, TState2>, TOutput, TResult>
                    (source, compound, keySelector, resultSelector);
        }

        /// <summary>
        /// Applies multiple aggregates to "snapshot windows" (SI terminology) on the input stream.
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
        /// Applies multiple aggregates to snapshot windows on the input stream relative to the selected grouping key.
        /// </summary>
        public static IStreamable<Empty, TResult> AggregateByKey<TInput, TInnerKey, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TOutput, TResult>(
            this IStreamable<Empty, TInput> source,
            Expression<Func<TInput, TInnerKey>> keySelector,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState3, TOutput3>> aggregate3,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput>> merger,
            Expression<Func<TInnerKey, TOutput, TResult>> resultSelector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(keySelector, nameof(keySelector));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(merger, nameof(merger));

            var window = new Window<CompoundGroupKey<Empty, TInnerKey>, TInput>(source.Properties.GroupNested(keySelector));
            var agg1 = aggregate1(window);
            var agg2 = aggregate2(window);
            var agg3 = aggregate3(window);
            var compound = AggregateFunctions.Combine(agg1, agg2, agg3, merger);

            return new GroupedWindowStreamable<TInnerKey, TInput, StructTuple<TState1, TState2, TState3>, TOutput, TResult>
                    (source, compound, keySelector, resultSelector);
        }

        /// <summary>
        /// Applies multiple aggregates to "snapshot windows" (SI terminology) on the input stream.
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
        /// Applies multiple aggregates to snapshot windows on the input stream relative to the selected grouping key.
        /// </summary>
        public static IStreamable<Empty, TResult> AggregateByKey<TInput, TInnerKey, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TOutput, TResult>(
            this IStreamable<Empty, TInput> source,
            Expression<Func<TInput, TInnerKey>> keySelector,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState3, TOutput3>> aggregate3,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState4, TOutput4>> aggregate4,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput>> merger,
            Expression<Func<TInnerKey, TOutput, TResult>> resultSelector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(keySelector, nameof(keySelector));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(merger, nameof(merger));

            var window = new Window<CompoundGroupKey<Empty, TInnerKey>, TInput>(source.Properties.GroupNested(keySelector));
            var agg1 = aggregate1(window);
            var agg2 = aggregate2(window);
            var agg3 = aggregate3(window);
            var agg4 = aggregate4(window);
            var compound = AggregateFunctions.Combine(agg1, agg2, agg3, agg4, merger);

            return new GroupedWindowStreamable<TInnerKey, TInput, StructTuple<TState1, TState2, TState3, TState4>, TOutput, TResult>
                    (source, compound, keySelector, resultSelector);
        }

        /// <summary>
        /// Applies multiple aggregates to "snapshot windows" (SI terminology) on the input stream.
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
        /// Applies multiple aggregates to snapshot windows on the input stream relative to the selected grouping key.
        /// </summary>
        public static IStreamable<Empty, TResult> AggregateByKey<TInput, TInnerKey, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TOutput, TResult>(
            this IStreamable<Empty, TInput> source,
            Expression<Func<TInput, TInnerKey>> keySelector,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState3, TOutput3>> aggregate3,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState4, TOutput4>> aggregate4,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState5, TOutput5>> aggregate5,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput>> merger,
            Expression<Func<TInnerKey, TOutput, TResult>> resultSelector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(keySelector, nameof(keySelector));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(aggregate5, nameof(aggregate5));
            Invariant.IsNotNull(merger, nameof(merger));

            var window = new Window<CompoundGroupKey<Empty, TInnerKey>, TInput>(source.Properties.GroupNested(keySelector));
            var agg1 = aggregate1(window);
            var agg2 = aggregate2(window);
            var agg3 = aggregate3(window);
            var agg4 = aggregate4(window);
            var agg5 = aggregate5(window);
            var compound = AggregateFunctions.Combine(agg1, agg2, agg3, agg4, agg5, merger);

            return new GroupedWindowStreamable<TInnerKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5>, TOutput, TResult>
                    (source, compound, keySelector, resultSelector);
        }

        /// <summary>
        /// Applies multiple aggregates to "snapshot windows" (SI terminology) on the input stream.
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
        /// Applies multiple aggregates to snapshot windows on the input stream relative to the selected grouping key.
        /// </summary>
        public static IStreamable<Empty, TResult> AggregateByKey<TInput, TInnerKey, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TOutput, TResult>(
            this IStreamable<Empty, TInput> source,
            Expression<Func<TInput, TInnerKey>> keySelector,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState3, TOutput3>> aggregate3,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState4, TOutput4>> aggregate4,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState5, TOutput5>> aggregate5,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState6, TOutput6>> aggregate6,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput>> merger,
            Expression<Func<TInnerKey, TOutput, TResult>> resultSelector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(keySelector, nameof(keySelector));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(aggregate5, nameof(aggregate5));
            Invariant.IsNotNull(aggregate6, nameof(aggregate6));
            Invariant.IsNotNull(merger, nameof(merger));

            var window = new Window<CompoundGroupKey<Empty, TInnerKey>, TInput>(source.Properties.GroupNested(keySelector));
            var agg1 = aggregate1(window);
            var agg2 = aggregate2(window);
            var agg3 = aggregate3(window);
            var agg4 = aggregate4(window);
            var agg5 = aggregate5(window);
            var agg6 = aggregate6(window);
            var compound = AggregateFunctions.Combine(agg1, agg2, agg3, agg4, agg5, agg6, merger);

            return new GroupedWindowStreamable<TInnerKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6>, TOutput, TResult>
                    (source, compound, keySelector, resultSelector);
        }

        /// <summary>
        /// Applies multiple aggregates to "snapshot windows" (SI terminology) on the input stream.
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
        /// Applies multiple aggregates to snapshot windows on the input stream relative to the selected grouping key.
        /// </summary>
        public static IStreamable<Empty, TResult> AggregateByKey<TInput, TInnerKey, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TState7, TOutput7, TOutput, TResult>(
            this IStreamable<Empty, TInput> source,
            Expression<Func<TInput, TInnerKey>> keySelector,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState3, TOutput3>> aggregate3,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState4, TOutput4>> aggregate4,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState5, TOutput5>> aggregate5,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState6, TOutput6>> aggregate6,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState7, TOutput7>> aggregate7,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput7, TOutput>> merger,
            Expression<Func<TInnerKey, TOutput, TResult>> resultSelector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(keySelector, nameof(keySelector));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(aggregate5, nameof(aggregate5));
            Invariant.IsNotNull(aggregate6, nameof(aggregate6));
            Invariant.IsNotNull(aggregate7, nameof(aggregate7));
            Invariant.IsNotNull(merger, nameof(merger));

            var window = new Window<CompoundGroupKey<Empty, TInnerKey>, TInput>(source.Properties.GroupNested(keySelector));
            var agg1 = aggregate1(window);
            var agg2 = aggregate2(window);
            var agg3 = aggregate3(window);
            var agg4 = aggregate4(window);
            var agg5 = aggregate5(window);
            var agg6 = aggregate6(window);
            var agg7 = aggregate7(window);
            var compound = AggregateFunctions.Combine(agg1, agg2, agg3, agg4, agg5, agg6, agg7, merger);

            return new GroupedWindowStreamable<TInnerKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7>, TOutput, TResult>
                    (source, compound, keySelector, resultSelector);
        }

        /// <summary>
        /// Applies multiple aggregates to "snapshot windows" (SI terminology) on the input stream.
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
        /// Applies multiple aggregates to snapshot windows on the input stream relative to the selected grouping key.
        /// </summary>
        public static IStreamable<Empty, TResult> AggregateByKey<TInput, TInnerKey, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TState7, TOutput7, TState8, TOutput8, TOutput, TResult>(
            this IStreamable<Empty, TInput> source,
            Expression<Func<TInput, TInnerKey>> keySelector,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState3, TOutput3>> aggregate3,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState4, TOutput4>> aggregate4,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState5, TOutput5>> aggregate5,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState6, TOutput6>> aggregate6,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState7, TOutput7>> aggregate7,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState8, TOutput8>> aggregate8,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput7, TOutput8, TOutput>> merger,
            Expression<Func<TInnerKey, TOutput, TResult>> resultSelector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(keySelector, nameof(keySelector));
            Invariant.IsNotNull(aggregate1, nameof(aggregate1));
            Invariant.IsNotNull(aggregate2, nameof(aggregate2));
            Invariant.IsNotNull(aggregate3, nameof(aggregate3));
            Invariant.IsNotNull(aggregate4, nameof(aggregate4));
            Invariant.IsNotNull(aggregate5, nameof(aggregate5));
            Invariant.IsNotNull(aggregate6, nameof(aggregate6));
            Invariant.IsNotNull(aggregate7, nameof(aggregate7));
            Invariant.IsNotNull(aggregate8, nameof(aggregate8));
            Invariant.IsNotNull(merger, nameof(merger));

            var window = new Window<CompoundGroupKey<Empty, TInnerKey>, TInput>(source.Properties.GroupNested(keySelector));
            var agg1 = aggregate1(window);
            var agg2 = aggregate2(window);
            var agg3 = aggregate3(window);
            var agg4 = aggregate4(window);
            var agg5 = aggregate5(window);
            var agg6 = aggregate6(window);
            var agg7 = aggregate7(window);
            var agg8 = aggregate8(window);
            var compound = AggregateFunctions.Combine(agg1, agg2, agg3, agg4, agg5, agg6, agg7, agg8, merger);

            return new GroupedWindowStreamable<TInnerKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8>, TOutput, TResult>
                    (source, compound, keySelector, resultSelector);
        }

        /// <summary>
        /// Applies multiple aggregates to "snapshot windows" (SI terminology) on the input stream.
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
        /// Applies multiple aggregates to snapshot windows on the input stream relative to the selected grouping key.
        /// </summary>
        public static IStreamable<Empty, TResult> AggregateByKey<TInput, TInnerKey, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TState7, TOutput7, TState8, TOutput8, TState9, TOutput9, TOutput, TResult>(
            this IStreamable<Empty, TInput> source,
            Expression<Func<TInput, TInnerKey>> keySelector,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState3, TOutput3>> aggregate3,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState4, TOutput4>> aggregate4,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState5, TOutput5>> aggregate5,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState6, TOutput6>> aggregate6,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState7, TOutput7>> aggregate7,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState8, TOutput8>> aggregate8,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState9, TOutput9>> aggregate9,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput7, TOutput8, TOutput9, TOutput>> merger,
            Expression<Func<TInnerKey, TOutput, TResult>> resultSelector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(keySelector, nameof(keySelector));
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

            var window = new Window<CompoundGroupKey<Empty, TInnerKey>, TInput>(source.Properties.GroupNested(keySelector));
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

            return new GroupedWindowStreamable<TInnerKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9>, TOutput, TResult>
                    (source, compound, keySelector, resultSelector);
        }

        /// <summary>
        /// Applies multiple aggregates to "snapshot windows" (SI terminology) on the input stream.
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
        /// Applies multiple aggregates to snapshot windows on the input stream relative to the selected grouping key.
        /// </summary>
        public static IStreamable<Empty, TResult> AggregateByKey<TInput, TInnerKey, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TState7, TOutput7, TState8, TOutput8, TState9, TOutput9, TState10, TOutput10, TOutput, TResult>(
            this IStreamable<Empty, TInput> source,
            Expression<Func<TInput, TInnerKey>> keySelector,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState3, TOutput3>> aggregate3,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState4, TOutput4>> aggregate4,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState5, TOutput5>> aggregate5,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState6, TOutput6>> aggregate6,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState7, TOutput7>> aggregate7,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState8, TOutput8>> aggregate8,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState9, TOutput9>> aggregate9,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState10, TOutput10>> aggregate10,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput7, TOutput8, TOutput9, TOutput10, TOutput>> merger,
            Expression<Func<TInnerKey, TOutput, TResult>> resultSelector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(keySelector, nameof(keySelector));
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

            var window = new Window<CompoundGroupKey<Empty, TInnerKey>, TInput>(source.Properties.GroupNested(keySelector));
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

            return new GroupedWindowStreamable<TInnerKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10>, TOutput, TResult>
                    (source, compound, keySelector, resultSelector);
        }

        /// <summary>
        /// Applies multiple aggregates to "snapshot windows" (SI terminology) on the input stream.
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
        /// Applies multiple aggregates to snapshot windows on the input stream relative to the selected grouping key.
        /// </summary>
        public static IStreamable<Empty, TResult> AggregateByKey<TInput, TInnerKey, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TState7, TOutput7, TState8, TOutput8, TState9, TOutput9, TState10, TOutput10, TState11, TOutput11, TOutput, TResult>(
            this IStreamable<Empty, TInput> source,
            Expression<Func<TInput, TInnerKey>> keySelector,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState3, TOutput3>> aggregate3,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState4, TOutput4>> aggregate4,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState5, TOutput5>> aggregate5,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState6, TOutput6>> aggregate6,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState7, TOutput7>> aggregate7,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState8, TOutput8>> aggregate8,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState9, TOutput9>> aggregate9,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState10, TOutput10>> aggregate10,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState11, TOutput11>> aggregate11,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput7, TOutput8, TOutput9, TOutput10, TOutput11, TOutput>> merger,
            Expression<Func<TInnerKey, TOutput, TResult>> resultSelector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(keySelector, nameof(keySelector));
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

            var window = new Window<CompoundGroupKey<Empty, TInnerKey>, TInput>(source.Properties.GroupNested(keySelector));
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

            return new GroupedWindowStreamable<TInnerKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11>, TOutput, TResult>
                    (source, compound, keySelector, resultSelector);
        }

        /// <summary>
        /// Applies multiple aggregates to "snapshot windows" (SI terminology) on the input stream.
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
        /// Applies multiple aggregates to snapshot windows on the input stream relative to the selected grouping key.
        /// </summary>
        public static IStreamable<Empty, TResult> AggregateByKey<TInput, TInnerKey, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TState7, TOutput7, TState8, TOutput8, TState9, TOutput9, TState10, TOutput10, TState11, TOutput11, TState12, TOutput12, TOutput, TResult>(
            this IStreamable<Empty, TInput> source,
            Expression<Func<TInput, TInnerKey>> keySelector,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState3, TOutput3>> aggregate3,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState4, TOutput4>> aggregate4,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState5, TOutput5>> aggregate5,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState6, TOutput6>> aggregate6,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState7, TOutput7>> aggregate7,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState8, TOutput8>> aggregate8,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState9, TOutput9>> aggregate9,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState10, TOutput10>> aggregate10,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState11, TOutput11>> aggregate11,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState12, TOutput12>> aggregate12,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput7, TOutput8, TOutput9, TOutput10, TOutput11, TOutput12, TOutput>> merger,
            Expression<Func<TInnerKey, TOutput, TResult>> resultSelector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(keySelector, nameof(keySelector));
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

            var window = new Window<CompoundGroupKey<Empty, TInnerKey>, TInput>(source.Properties.GroupNested(keySelector));
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

            return new GroupedWindowStreamable<TInnerKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12>, TOutput, TResult>
                    (source, compound, keySelector, resultSelector);
        }

        /// <summary>
        /// Applies multiple aggregates to "snapshot windows" (SI terminology) on the input stream.
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
        /// Applies multiple aggregates to snapshot windows on the input stream relative to the selected grouping key.
        /// </summary>
        public static IStreamable<Empty, TResult> AggregateByKey<TInput, TInnerKey, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TState7, TOutput7, TState8, TOutput8, TState9, TOutput9, TState10, TOutput10, TState11, TOutput11, TState12, TOutput12, TState13, TOutput13, TOutput, TResult>(
            this IStreamable<Empty, TInput> source,
            Expression<Func<TInput, TInnerKey>> keySelector,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState3, TOutput3>> aggregate3,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState4, TOutput4>> aggregate4,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState5, TOutput5>> aggregate5,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState6, TOutput6>> aggregate6,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState7, TOutput7>> aggregate7,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState8, TOutput8>> aggregate8,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState9, TOutput9>> aggregate9,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState10, TOutput10>> aggregate10,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState11, TOutput11>> aggregate11,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState12, TOutput12>> aggregate12,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState13, TOutput13>> aggregate13,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput7, TOutput8, TOutput9, TOutput10, TOutput11, TOutput12, TOutput13, TOutput>> merger,
            Expression<Func<TInnerKey, TOutput, TResult>> resultSelector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(keySelector, nameof(keySelector));
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

            var window = new Window<CompoundGroupKey<Empty, TInnerKey>, TInput>(source.Properties.GroupNested(keySelector));
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

            return new GroupedWindowStreamable<TInnerKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13>, TOutput, TResult>
                    (source, compound, keySelector, resultSelector);
        }

        /// <summary>
        /// Applies multiple aggregates to "snapshot windows" (SI terminology) on the input stream.
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
        /// Applies multiple aggregates to snapshot windows on the input stream relative to the selected grouping key.
        /// </summary>
        public static IStreamable<Empty, TResult> AggregateByKey<TInput, TInnerKey, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TState7, TOutput7, TState8, TOutput8, TState9, TOutput9, TState10, TOutput10, TState11, TOutput11, TState12, TOutput12, TState13, TOutput13, TState14, TOutput14, TOutput, TResult>(
            this IStreamable<Empty, TInput> source,
            Expression<Func<TInput, TInnerKey>> keySelector,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState3, TOutput3>> aggregate3,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState4, TOutput4>> aggregate4,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState5, TOutput5>> aggregate5,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState6, TOutput6>> aggregate6,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState7, TOutput7>> aggregate7,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState8, TOutput8>> aggregate8,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState9, TOutput9>> aggregate9,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState10, TOutput10>> aggregate10,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState11, TOutput11>> aggregate11,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState12, TOutput12>> aggregate12,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState13, TOutput13>> aggregate13,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState14, TOutput14>> aggregate14,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput7, TOutput8, TOutput9, TOutput10, TOutput11, TOutput12, TOutput13, TOutput14, TOutput>> merger,
            Expression<Func<TInnerKey, TOutput, TResult>> resultSelector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(keySelector, nameof(keySelector));
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

            var window = new Window<CompoundGroupKey<Empty, TInnerKey>, TInput>(source.Properties.GroupNested(keySelector));
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

            return new GroupedWindowStreamable<TInnerKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14>, TOutput, TResult>
                    (source, compound, keySelector, resultSelector);
        }

        /// <summary>
        /// Applies multiple aggregates to "snapshot windows" (SI terminology) on the input stream.
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
        /// Applies multiple aggregates to snapshot windows on the input stream relative to the selected grouping key.
        /// </summary>
        public static IStreamable<Empty, TResult> AggregateByKey<TInput, TInnerKey, TState1, TOutput1, TState2, TOutput2, TState3, TOutput3, TState4, TOutput4, TState5, TOutput5, TState6, TOutput6, TState7, TOutput7, TState8, TOutput8, TState9, TOutput9, TState10, TOutput10, TState11, TOutput11, TState12, TOutput12, TState13, TOutput13, TState14, TOutput14, TState15, TOutput15, TOutput, TResult>(
            this IStreamable<Empty, TInput> source,
            Expression<Func<TInput, TInnerKey>> keySelector,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState1, TOutput1>> aggregate1,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState2, TOutput2>> aggregate2,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState3, TOutput3>> aggregate3,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState4, TOutput4>> aggregate4,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState5, TOutput5>> aggregate5,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState6, TOutput6>> aggregate6,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState7, TOutput7>> aggregate7,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState8, TOutput8>> aggregate8,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState9, TOutput9>> aggregate9,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState10, TOutput10>> aggregate10,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState11, TOutput11>> aggregate11,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState12, TOutput12>> aggregate12,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState13, TOutput13>> aggregate13,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState14, TOutput14>> aggregate14,
            Func<Window<CompoundGroupKey<Empty, TInnerKey>, TInput>, IAggregate<TInput, TState15, TOutput15>> aggregate15,
            Expression<Func<TOutput1, TOutput2, TOutput3, TOutput4, TOutput5, TOutput6, TOutput7, TOutput8, TOutput9, TOutput10, TOutput11, TOutput12, TOutput13, TOutput14, TOutput15, TOutput>> merger,
            Expression<Func<TInnerKey, TOutput, TResult>> resultSelector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(keySelector, nameof(keySelector));
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

            var window = new Window<CompoundGroupKey<Empty, TInnerKey>, TInput>(source.Properties.GroupNested(keySelector));
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

            return new GroupedWindowStreamable<TInnerKey, TInput, StructTuple<TState1, TState2, TState3, TState4, TState5, TState6, TState7, TState8, TState9, TState10, TState11, TState12, TState13, TState14, TState15>, TOutput, TResult>
                    (source, compound, keySelector, resultSelector);
        }

        /// <summary>
        /// Applies multiple aggregates to "snapshot windows" (SI terminology) on the input stream.
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