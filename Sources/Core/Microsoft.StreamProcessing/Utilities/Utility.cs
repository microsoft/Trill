// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;

namespace Microsoft.StreamProcessing
{
    internal static class Utility
    {
        internal static readonly IDisposable EmptyDisposable = new NullDisposable();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long SnapToLeftBoundary(this long value, long period, long offset = 0)
            => period <= 1 ? value : value - ((value + period - (offset % period)) % period);

        internal static IEnumerable<T> Yield<T>(this T element)
        {
            yield return element;
        }

        internal static string GenerateFreshVariableName(this IEnumerable<ParameterExpression> parameters, string baseName)
        {
            var names = new HashSet<string>(parameters.Select(p => p.Name));
            var suffix = 0;
            var testName = baseName;
            while (names.Contains(testName)) testName = baseName + suffix++;
            return testName;
        }

        /// <summary>
        /// Tries to get the first element in the given sorted dictionary.
        /// </summary>
        /// <typeparam name="TKey">The key type of the sorted dictionary.</typeparam>
        /// <typeparam name="TValue">The value type of the sorted dictionary.</typeparam>
        /// <param name="source">The sorted dictionary from which to attempt drawing an item.</param>
        /// <param name="key">The key of the sorted dictionary item returned.</param>
        /// <param name="value">The value of the sorted dictionary item returned.</param>
        /// <returns>Whether a value was found in the sorted dictionary.</returns>
        public static bool TryGetFirst<TKey, TValue>(this SortedDictionary<TKey, TValue> source, out TKey key, out TValue value)
        {
            // avoids boxing in LINQ to Objects First() call
            foreach (var pair in source)
            {
                key = pair.Key;
                value = pair.Value;
                return true;
            }
            key = default;
            value = default;
            return false;
        }

        /// <summary>
        /// Tries to get the first element in the given sorted set.
        /// </summary>
        /// <typeparam name="TKey">The key type of the sorted dictionary.</typeparam>
        /// <param name="source">The sorted dictionary from which to attempt drawing an item.</param>
        /// <param name="key">The key of the sorted dictionary item returned.</param>
        /// <returns>Whether a value was found in the sorted dictionary.</returns>
        public static bool TryGetFirst<TKey>(this SortedSet<TKey> source, out TKey key)
        {
            // avoids boxing in LINQ to Objects First() call
            foreach (var entry in source)
            {
                key = entry;
                return true;
            }
            key = default;
            return false;
        }

        internal static IDisposable CreateDisposable(params IDisposable[] disposables) => new CompoundDisposable(disposables);

        internal static Dictionary<TK, TV> Clone<TK, TV>(this Dictionary<TK, TV> source) => new Dictionary<TK, TV>(source);

        /// <summary>
        /// With a dictionary of lists, add a single element to a particular list. Create a new list if none exists at that key location.
        /// </summary>
        /// <typeparam name="TKey">The key type of the dictionary to which to add a value.</typeparam>
        /// <typeparam name="TValue">The item type of the lists that serve as values in the dictionary.</typeparam>
        /// <param name="dict">The dictionary of lists to which to add an item.</param>
        /// <param name="key">The key of the list to which to add the new element.</param>
        /// <param name="value">The value to add to the given list.</param>
        public static void Add<TKey, TValue>(this IDictionary<TKey, List<TValue>> dict, TKey key, TValue value)
        {
            if (!dict.TryGetValue(key, out var list))
            {
                list = new List<TValue>();
                dict.Add(key, list);
            }
            list.Add(value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int StableHash(this string stringToHash)
        {
            const long magicno = 40343L;
            int stringLength = stringToHash.Length;
            ulong hashState = (ulong)stringLength;

            unsafe
            {
                fixed (char* str = stringToHash)
                {
                    var stringChars = str;
                    for (int i = 0; i < stringLength; i++, stringChars++)
                        hashState = magicno * hashState + *stringChars;
                }

                var rotate = magicno * hashState;
                var rotated = (rotate >> 4) | (rotate << 60);
                return (int)(rotated ^ (rotated >> 32));
            }
        }

        internal static Expression<Comparison<T2>> CreateCompoundComparer<T1, T2>(Expression<Func<T2, T1>> selector, Expression<Comparison<T1>> comparer)
        {
            var newParam1 = Expression.Parameter(selector.Parameters[0].Type, selector.Parameters[0].Name + "_1");
            var newParam2 = Expression.Parameter(selector.Parameters[0].Type, selector.Parameters[0].Name + "_2");
            var copy1 = selector.ReplaceParametersInBody(newParam1);
            var copy2 = selector.ReplaceParametersInBody(newParam2);
            var result = comparer.ReplaceParametersInBody(copy1, copy2);
            return Expression.Lambda<Comparison<T2>>(result, newParam1, newParam2);
        }

        private sealed class CompoundDisposable : IDisposable
        {
            private readonly IDisposable[] disposables;

            public CompoundDisposable(IDisposable[] disposables)
            {
                Contract.Requires(disposables != null);

                this.disposables = disposables;
            }

            #region IDisposable Members

            public void Dispose()
            {
                foreach (IDisposable disposable in this.disposables.Where(d => d != null))
                {
                    disposable.Dispose();
                }
            }

            #endregion
        }

        private sealed class NullDisposable : IDisposable
        {
            public void Dispose() { }
        }

        internal static int GetSizeOf(this Type type)
        {
            if (type.IsPrimitive)
            {
                if (type == typeof(bool)) return sizeof(bool);
                if (type == typeof(byte)) return sizeof(byte);
                if (type == typeof(sbyte)) return sizeof(sbyte);
                if (type == typeof(short)) return sizeof(ushort);
                if (type == typeof(ushort)) return sizeof(ushort);
                if (type == typeof(int)) return sizeof(int);
                if (type == typeof(uint)) return sizeof(uint);
                if (type == typeof(long)) return sizeof(long);
                if (type == typeof(ulong)) return sizeof(ulong);
                if (type == typeof(float)) return sizeof(float);
                if (type == typeof(double)) return sizeof(double);
                if (type == typeof(char)) return sizeof(char);
                throw new InvalidOperationException("Above cases meant to be exhaustive, unknown primitive type " + type.FullName);
            }

            throw new InvalidOperationException("Only primitive types supported, unknown structural type " + type.FullName);
        }
    }

    internal static class Invariant
    {
        public static T IsNotNull<T>(this T arg, string argName) where T : class
        {
            if (arg == null) throw new ArgumentNullException(argName);
            return arg;
        }

        public static int IsPositive(this int arg, string argName)
        {
            if (arg <= 0) throw new ArgumentException("Value must be positive.", argName);
            return arg;
        }

        public static long IsPositive(this long arg, string argName)
        {
            if (arg <= 0L) throw new ArgumentException("Value must be positive.", argName);
            return arg;
        }

        public static int IsNonNegative(this int arg, string argName)
        {
            if (arg < 0) throw new ArgumentException("Value must be positive or 0.", argName);
            return arg;
        }

        public static long IsNonNegative(this long arg, string argName)
        {
            if (arg < 0L) throw new ArgumentException("Value must be positive or 0.", argName);
            return arg;
        }

        public static void IsTrue(bool assertion, string message)
        {
            if (!assertion) throw new ArgumentException(message);
        }
    }
}