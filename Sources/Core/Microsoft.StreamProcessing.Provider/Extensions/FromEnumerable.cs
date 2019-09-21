// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;

namespace Microsoft.StreamProcessing.Provider
{
    public static partial class QStreamableStatic
    {
        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="input"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public static IQStreamable<TResult> Min<TSource, TResult>(
            this IQStreamable<TSource> input,
            Func<TSource, TResult> selector)
            => input.Aggregate(e => e.Min(selector));

        /// <summary>
        ///
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<int> Max(
            this IQStreamable<int> input)
            => input.Aggregate(e => e.Max());

        /// <summary>
        ///
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<long> Max(
            this IQStreamable<long> input)
            => input.Aggregate(e => e.Max());

        /// <summary>
        ///
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<double> Max(
            this IQStreamable<double> input)
            => input.Aggregate(e => e.Max());

        /// <summary>
        ///
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<float> Max(
            this IQStreamable<float> input)
            => input.Aggregate(e => e.Max());

        /// <summary>
        ///
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<decimal> Max(
            this IQStreamable<decimal> input)
            => input.Aggregate(e => e.Max());

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<TSource> Max<TSource>(
            this IQStreamable<TSource> input)
            => input.Aggregate(e => e.Max());

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public static IQStreamable<int> Max<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, int> selector)
            => input.Aggregate(e => e.Max(selector));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public static IQStreamable<long> Max<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, long> selector)
            => input.Aggregate(e => e.Max(selector));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public static IQStreamable<float> Max<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, float> selector)
            => input.Aggregate(e => e.Max(selector));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public static IQStreamable<double> Max<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, double> selector)
            => input.Aggregate(e => e.Max(selector));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public static IQStreamable<decimal> Max<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, decimal> selector)
            => input.Aggregate(e => e.Max(selector));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="input"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public static IQStreamable<TResult> Max<TSource, TResult>(
            this IQStreamable<TSource> input,
            Func<TSource, TResult> selector)
            => input.Aggregate(e => e.Max(selector));

        /// <summary>
        ///
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<double> Average(
            this IQStreamable<int> input)
            => input.Aggregate(e => e.Average());

        /// <summary>
        ///
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<double> Average(
            this IQStreamable<long> input)
            => input.Aggregate(e => e.Average());

        /// <summary>
        ///
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<float> Average(
            this IQStreamable<float> input)
            => input.Aggregate(e => e.Average());

        /// <summary>
        ///
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<double> Average(
            this IQStreamable<double> input)
            => input.Aggregate(e => e.Average());

        /// <summary>
        ///
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<decimal> Average(
            this IQStreamable<decimal> input)
            => input.Aggregate(e => e.Average());

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public static IQStreamable<double> Average<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, int> selector)
            => input.Aggregate(e => e.Average(selector));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public static IQStreamable<double> Average<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, long> selector)
            => input.Aggregate(e => e.Average(selector));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public static IQStreamable<float> Average<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, float> selector)
            => input.Aggregate(e => e.Average(selector));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public static IQStreamable<double> Average<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, double> selector)
            => input.Aggregate(e => e.Average(selector));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public static IQStreamable<decimal> Average<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, decimal> selector)
            => input.Aggregate(e => e.Average(selector));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static IQStreamable<TSource> ElementAt<TSource>(
            this IQStreamable<TSource> input,
            int index)
            => input.Aggregate(e => e.ElementAt(index));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static IQStreamable<TSource> ElementAtOrDefault<TSource>(
            this IQStreamable<TSource> input,
            int index)
            => input.Aggregate(e => e.ElementAtOrDefault(index));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<bool> Any<TSource>(
            this IQStreamable<TSource> input)
            => input.Aggregate(e => e.Any());

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static IQStreamable<bool> Any<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, bool> predicate)
            => input.Aggregate(e => e.Any(predicate));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static IQStreamable<bool> All<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, bool> predicate)
            => input.Aggregate(e => e.All(predicate));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<int> Count<TSource>(
            this IQStreamable<TSource> input)
            => input.Aggregate(e => e.Count());

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static IQStreamable<int> Count<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, bool> predicate)
            => input.Aggregate(e => e.Count(predicate));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<long> LongCount<TSource>(
            this IQStreamable<TSource> input)
            => input.Aggregate(e => e.LongCount());

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static IQStreamable<long> LongCount<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, bool> predicate)
            => input.Aggregate(e => e.LongCount(predicate));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static IQStreamable<bool> Contains<TSource>(
            this IQStreamable<TSource> input,
            TSource value)
            => input.Aggregate(e => e.Contains(value));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="value"></param>
        /// <param name="comparer"></param>
        /// <returns></returns>
        public static IQStreamable<bool> Contains<TSource>(
            this IQStreamable<TSource> input,
            TSource value,
            IEqualityComparer<TSource> comparer)
            => input.Aggregate(e => e.Contains(value, comparer));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="func"></param>
        /// <returns></returns>
        public static IQStreamable<TSource> Aggregate<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, TSource, TSource> func)
            => input.Aggregate(e => e.Aggregate(func));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TAccumulate"></typeparam>
        /// <param name="input"></param>
        /// <param name="seed"></param>
        /// <param name="func"></param>
        /// <returns></returns>
        public static IQStreamable<TAccumulate> Aggregate<TSource, TAccumulate>(
            this IQStreamable<TSource> input,
            TAccumulate seed,
            Func<TAccumulate, TSource, TAccumulate> func)
            => input.Aggregate(e => e.Aggregate(seed, func));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TAccumulate"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="input"></param>
        /// <param name="seed"></param>
        /// <param name="func"></param>
        /// <param name="resultSelector"></param>
        /// <returns></returns>
        public static IQStreamable<TResult> Aggregate<TSource, TAccumulate, TResult>(
            this IQStreamable<TSource> input,
            TAccumulate seed,
            Func<TAccumulate, TSource, TAccumulate> func,
            Func<TAccumulate, TResult> resultSelector)
            => input.Aggregate(e => e.Aggregate(seed, func, resultSelector));

        /// <summary>
        ///
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<int> Sum(
            this IQStreamable<int> input)
            => input.Aggregate(e => e.Sum());

        /// <summary>
        ///
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<long> Sum(
            this IQStreamable<long> input)
            => input.Aggregate(e => e.Sum());

        /// <summary>
        ///
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<float> Sum(
            this IQStreamable<float> input)
            => input.Aggregate(e => e.Sum());

        /// <summary>
        ///
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<double> Sum(
            this IQStreamable<double> input)
            => input.Aggregate(e => e.Sum());

        /// <summary>
        ///
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<decimal> Sum(
            this IQStreamable<decimal> input)
            => input.Aggregate(e => e.Sum());

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public static IQStreamable<int> Sum<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, int> selector)
            => input.Aggregate(e => e.Sum(selector));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public static IQStreamable<long> Sum<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, long> selector)
            => input.Aggregate(e => e.Sum(selector));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public static IQStreamable<float> Sum<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, float> selector)
            => input.Aggregate(e => e.Sum(selector));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public static IQStreamable<double> Sum<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, double> selector)
            => input.Aggregate(e => e.Sum(selector));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public static IQStreamable<decimal> Sum<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, decimal> selector)
            => input.Aggregate(e => e.Sum(selector));

        /// <summary>
        ///
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<int> Min(
            this IQStreamable<int> input)
            => input.Aggregate(e => e.Min());

        /// <summary>
        ///
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<long> Min(
            this IQStreamable<long> input)
            => input.Aggregate(e => e.Min());

        /// <summary>
        ///
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<float> Min(
            this IQStreamable<float> input)
            => input.Aggregate(e => e.Min());

        /// <summary>
        ///
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<double> Min(
            this IQStreamable<double> input)
            => input.Aggregate(e => e.Min());

        /// <summary>
        ///
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<decimal> Min(
            this IQStreamable<decimal> input)
            => input.Aggregate(e => e.Min());

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<TSource> Min<TSource>(
            this IQStreamable<TSource> input)
            => input.Aggregate(e => e.Min());

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public static IQStreamable<int> Min<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, int> selector)
            => input.Aggregate(e => e.Min(selector));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public static IQStreamable<long> Min<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, long> selector)
            => input.Aggregate(e => e.Min(selector));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public static IQStreamable<float> Min<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, float> selector)
            => input.Aggregate(e => e.Min(selector));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public static IQStreamable<double> Min<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, double> selector)
            => input.Aggregate(e => e.Min(selector));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public static IQStreamable<decimal> Min<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, decimal> selector)
            => input.Aggregate(e => e.Min(selector));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<TSource> First<TSource>(
            this IQStreamable<TSource> input)
            => input.Aggregate(e => e.First());

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static IQStreamable<TSource> First<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, bool> predicate)
            => input.Aggregate(e => e.First(predicate));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<TSource> FirstOrDefault<TSource>(
            this IQStreamable<TSource> input)
            => input.Aggregate(e => e.FirstOrDefault());

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static IQStreamable<TSource> FirstOrDefault<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, bool> predicate)
            => input.Aggregate(e => e.FirstOrDefault(predicate));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<TSource> Last<TSource>(
            this IQStreamable<TSource> input)
            => input.Aggregate(e => e.Last());

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static IQStreamable<TSource> Last<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, bool> predicate)
            => input.Aggregate(e => e.Last(predicate));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<TSource> LastOrDefault<TSource>(
            this IQStreamable<TSource> input)
            => input.Aggregate(e => e.LastOrDefault());

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static IQStreamable<TSource> LastOrDefault<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, bool> predicate)
            => input.Aggregate(e => e.LastOrDefault(predicate));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<TSource> Single<TSource>(
            this IQStreamable<TSource> input)
            => input.Aggregate(e => e.Single());

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static IQStreamable<TSource> Single<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, bool> predicate)
            => input.Aggregate(e => e.Single(predicate));

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <returns></returns>
        public static IQStreamable<TSource> SingleOrDefault<TSource>(
            this IQStreamable<TSource> input)
            => input.Aggregate(e => e.SingleOrDefault());

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="input"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static IQStreamable<TSource> SingleOrDefault<TSource>(
            this IQStreamable<TSource> input,
            Func<TSource, bool> predicate)
            => input.Aggregate(e => e.SingleOrDefault(predicate));

    }
}
