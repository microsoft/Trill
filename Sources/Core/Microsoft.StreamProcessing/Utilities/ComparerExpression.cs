// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// A wrapper interface for an expression to compare two values of the given type.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IComparerExpression<T>
    {
        /// <summary>
        /// Provides an expression to compare two values of the given type.
        /// </summary>
        /// <returns>An expression to compare two values of the given type.</returns>
        Expression<Comparison<T>> GetCompareExpr();
    }

    internal sealed class ComparerExpressionCache
    {
        private static readonly Dictionary<Type, object> typeComparerCache = new Dictionary<Type, object>();

        static ComparerExpressionCache()
        {
            typeComparerCache.Add(typeof(byte), new PrimitiveComparerExpression<byte>((x, y) => x < y ? -1 : x == y ? 0 : 1));
            typeComparerCache.Add(typeof(sbyte), new PrimitiveComparerExpression<sbyte>((x, y) => x < y ? -1 : x == y ? 0 : 1));
            typeComparerCache.Add(typeof(char), new PrimitiveComparerExpression<char>((x, y) => x < y ? -1 : x == y ? 0 : 1));
            typeComparerCache.Add(typeof(short), new PrimitiveComparerExpression<short>((x, y) => x < y ? -1 : x == y ? 0 : 1));
            typeComparerCache.Add(typeof(ushort), new PrimitiveComparerExpression<ushort>((x, y) => x < y ? -1 : x == y ? 0 : 1));
            typeComparerCache.Add(typeof(int), new PrimitiveComparerExpression<int>((x, y) => x < y ? -1 : x == y ? 0 : 1));
            typeComparerCache.Add(typeof(uint), new PrimitiveComparerExpression<uint>((x, y) => x < y ? -1 : x == y ? 0 : 1));
            typeComparerCache.Add(typeof(long), new PrimitiveComparerExpression<long>((x, y) => x < y ? -1 : x == y ? 0 : 1));
            typeComparerCache.Add(typeof(ulong), new PrimitiveComparerExpression<ulong>((x, y) => x < y ? -1 : x == y ? 0 : 1));
            typeComparerCache.Add(typeof(decimal), new PrimitiveComparerExpression<decimal>((x, y) => x < y ? -1 : x == y ? 0 : 1));
            typeComparerCache.Add(typeof(string), new GenericComparableExpression<string>());
            typeComparerCache.Add(typeof(TimeSpan), new GenericComparableExpression<TimeSpan>());
            typeComparerCache.Add(typeof(DateTime), new GenericComparableExpression<DateTime>());
            typeComparerCache.Add(typeof(DateTimeOffset), new GenericComparableExpression<DateTimeOffset>());
            typeComparerCache.Add(typeof(Empty), new PrimitiveComparerExpression<Empty>((x, y) => 0));
        }

        public static bool TryGetCachedComparer<T>(out IComparerExpression<T> comparer)
        {
            var t = typeof(T);
            comparer = null;
            if (typeComparerCache.TryGetValue(t, out object temp))
            {
                comparer = (IComparerExpression<T>)temp;
                return true;
            }
            return false;
        }

        public static void Add<T>(IComparerExpression<T> comparer) => typeComparerCache.Add(typeof(T), comparer);
    }

    internal class ComparerExpression<T> : IComparerExpression<T>
    {
        private static readonly object sentinel = new object();
        private readonly Expression<Comparison<T>> CompareExpr;

        public ComparerExpression(Expression<Comparison<T>> compareExpr) => this.CompareExpr = compareExpr;

        public static IComparerExpression<T> Default
        {
            get
            {
                var type = typeof(T).GetTypeInfo();

                lock (sentinel)
                {
                    if (ComparerExpressionCache.TryGetCachedComparer(out IComparerExpression<T> comparer))
                        return comparer;

                    if (type.IsCompoundGroupKey(out var t1, out var t2))
                    {
                        // equivalent to: return new CompoundGroupKeyComparer<T1, T2>(ComparerExpression<T1>.Default, ComparerExpression<T2>.Default);
                        var comparerExpressionOfT1 = typeof(ComparerExpression<>).MakeGenericType(t1);
                        var defaultPropertyForT1 = comparerExpressionOfT1.GetTypeInfo().GetProperty("Default");
                        var default1 = defaultPropertyForT1.GetValue(null);

                        var comparerExpressionOfT2 = typeof(ComparerExpression<>).MakeGenericType(t2);
                        var defaultPropertyForT2 = comparerExpressionOfT2.GetTypeInfo().GetProperty("Default");
                        var default2 = defaultPropertyForT2.GetValue(null);

                        var cgkc = typeof(CompoundGroupKeyComparer<,>);
                        var genericInstance = cgkc.MakeGenericType(t1, t2);
                        var ctor = genericInstance.GetTypeInfo().GetConstructor(new Type[] { comparerExpressionOfT1, comparerExpressionOfT2, });
                        var result = ctor.Invoke(new object[] { default1, default2, });
                        comparer = (IComparerExpression<T>)result;
                        ComparerExpressionCache.Add(comparer);
                        return comparer;
                    }

                    if (type.IsAnonymousTypeName())
                    {
                        var expr = ComparerExprForAnonymousType(type);
                        comparer = expr == null ? new GenericComparerExpression<T>() : new ComparerExpression<T>(expr);
                        ComparerExpressionCache.Add(comparer);
                        return comparer;
                    }

                    if (type.ImplementsIComparable())
                    {
                        // then fall back to using a lambda of the form:
                        // (x,y) => x.CompareTo(y)
                        var genericInstanceOfComparerExpressionForGenericIComparable = typeof(GenericComparableExpression<>).MakeGenericType(type);
                        var ctorForComparerExpressionForGenericIComparer = genericInstanceOfComparerExpressionForGenericIComparable.GetTypeInfo().GetConstructor(Array.Empty<Type>());
                        if (ctorForComparerExpressionForGenericIComparer != null)
                        {
                            comparer = (IComparerExpression<T>)ctorForComparerExpressionForGenericIComparer.Invoke(Array.Empty<object>());
                            ComparerExpressionCache.Add(comparer);
                            return comparer;
                        }
                    }

                    if (type.ImplementsIComparer())
                    {
                        // then fall back to using a lambda of the form:
                        // (x,y) => o.IComparer<T>.Compare(x,y)
                        // for an arbitrary o that is created of type T by calling its nullary ctor (if such a ctor exists)
                        var genericInstanceOfComparerExpressionForGenericIComparer = typeof(ComparerExpressionForGenericIComparer<>).MakeGenericType(type);
                        var ctorForComparerExpressionForGenericIComparer = genericInstanceOfComparerExpressionForGenericIComparer.GetTypeInfo().GetConstructor(new Type[] { type, });
                        if (ctorForComparerExpressionForGenericIComparer != null)
                        {
                            var ctorForType = type.GetConstructor(Type.EmptyTypes);
                            if (ctorForType != null)
                            {
                                var instanceOfType = ctorForType.Invoke(Array.Empty<object>());
                                if (instanceOfType != null)
                                {
                                    comparer = (IComparerExpression<T>)ctorForComparerExpressionForGenericIComparer.Invoke(new object[] { instanceOfType, });
                                    ComparerExpressionCache.Add(comparer);
                                    return comparer;
                                }
                            }
                        }
                    }

                    if (type.GetInterface("System.Collections.IComparer") != null)
                    {
                        // then fall back to using a lambda of the form:
                        // (x,y) => o.IComparer.Compare(x,y)
                        // for an arbitrary o that is created of type T by calling its nullary ctor (if such a ctor exists)
                        var genericInstanceOfComparerExpressionForNonGenericIComparer = typeof(ComparerExpressionForNonGenericIComparer<>).MakeGenericType(type);
                        var ctorForComparerExpressionForNonGenericIComparer = genericInstanceOfComparerExpressionForNonGenericIComparer.GetTypeInfo().GetConstructor(new Type[] { type, });
                        if (ctorForComparerExpressionForNonGenericIComparer != null)
                        {
                            var ctorForType = type.GetConstructor(Type.EmptyTypes);
                            if (ctorForType != null)
                            {
                                var instanceOfType = ctorForType.Invoke(Array.Empty<object>());
                                if (instanceOfType != null)
                                {
                                    comparer = (IComparerExpression<T>)ctorForComparerExpressionForNonGenericIComparer.Invoke(new object[] { instanceOfType, });
                                    ComparerExpressionCache.Add(comparer);
                                    return comparer;
                                }
                            }
                        }
                    }

                    comparer = new GenericComparerExpression<T>();
                    ComparerExpressionCache.Add(comparer);
                    return comparer;
                }
            }
        }

        /// <summary>
        /// Returns an (unfortunately) weakly-typed expression for a function
        /// that takes two values of type <paramref name="t"/> (which must be
        /// an anonymous type) and returns an int. The function is a comparer,
        /// so it returns -1 if the first argument is less than the second,
        /// +1 if the first argument is greater than the second and 0 if
        /// the two arguments are equal.
        /// The function is:
        /// (A a1, A a2) =&gt; compare_expression_for_a1.P1_and_a2.P1 == 0 ? ComparerExprForAnonymousType(new { P2, .., P_n }) : compare_expression_for_a1.P1_and_a2.P1
        /// where the compare_expression is the compare expression returned by ComparerExpression&gt;U&lt;.Default where U is the type of the property P_i.
        /// where the anonymous type had been defined as new{ P1 = ..., ...., Pn = ...}.
        /// I.e., this is using lexicographic ordering on the order the properties were defined in the
        /// new expression.
        /// If the type of any of the properties does not support the lessthan or equality operators,
        /// null is returned.
        /// </summary>
        /// <param name="t">
        /// Must be an anonymous type
        /// </param>
        /// <returns>
        /// When <paramref name="t"/> is null or not an anonymous type, then null is
        /// returned. Otherwise, an expression which is defined as above.
        /// </returns>
        private static Expression<Comparison<T>> ComparerExprForAnonymousType(Type t)
        {
            if (t == null || !t.IsAnonymousTypeName()) return null;
            var properties = t.GetTypeInfo().GetProperties(BindingFlags.Public | BindingFlags.Instance);
            if (properties.Length == 0) return null;
            var left = Expression.Parameter(t, "left");
            var right = Expression.Parameter(t, "right");
            var n = properties.Length;
            var p = properties[n - 1];
            try
            {
                Expression body = MakeComparisonExpression(left, right, p, zero);
                for (int i = n - 2; i >= 0; i--)
                {
                    p = properties[i];
                    body = MakeComparisonExpression(left, right, p, body);
                }
                return Expression.Lambda<Comparison<T>>(body, left, right);
            }
            catch (InvalidOperationException)
            {
                // this means something went wrong in creating the expression
                return null;
            }
        }
        private static readonly Expression zero = Expression.Constant(0, typeof(int));

        // Given an expression e (of type int), returns:
        //   compare_expression_for_left.P_andright.P == 0 : e : compare_expression_for_left.P_andright.P;
        // where the compare_expression is the compare expression returned by ComparerExpression<U>.Default where U is the type of the property p.
        private static ConditionalExpression MakeComparisonExpression(ParameterExpression left, ParameterExpression right, PropertyInfo p, Expression e)
        {
            var comparerTypeForPropertyType = typeof(ComparerExpression<>).MakeGenericType(p.PropertyType);
            var comparerDefaultProperty = comparerTypeForPropertyType.GetTypeInfo().GetProperty("Default");
            var getter = comparerDefaultProperty.GetMethod;
            var comparerExpressionObject = getter.Invoke(null, null);
            var comparerExpression = (LambdaExpression)comparerExpressionObject.GetType().GetTypeInfo()
                .GetMethod("GetCompareExpr").Invoke(comparerExpressionObject, null);
            var inlinedComparerExpression = comparerExpression.ReplaceParametersInBody(Expression.Property(left, p), Expression.Property(right, p));
            return Expression.Condition(Expression.Equal(inlinedComparerExpression, zero), e, inlinedComparerExpression);
        }

        public Expression<Comparison<T>> GetCompareExpr() => this.CompareExpr;

        internal static bool IsSimpleDefault(IComparerExpression<T> input)
            => input == Default && input is PrimitiveComparerExpression<T>;
    }

    internal class PrimitiveComparerExpression<T> : ComparerExpression<T>
    {
        public PrimitiveComparerExpression(Expression<Comparison<T>> compareExpr) : base(compareExpr) { }
    }

    internal sealed class GenericComparerExpression<T> : ComparerExpression<T>
    {
        public GenericComparerExpression() : base(compareExpr: (x, y) => Comparer<T>.Default.Compare(x, y)) { }
    }

    internal sealed class GenericComparableExpression<T> : ComparerExpression<T> where T : IComparable<T>
    {
        public GenericComparableExpression() : base(compareExpr: (x, y) => x.CompareTo(y)) { }
    }

    internal sealed class ComparerExpressionForGenericIComparer<T> : ComparerExpression<T> where T : IComparer<T>
    {
        public ComparerExpressionForGenericIComparer(T t) : base(compareExpr: (x, y) => t.Compare(x, y)) { }
    }

    internal sealed class ComparerExpressionForNonGenericIComparer<T> : ComparerExpression<T> where T : IComparer
    {
        public ComparerExpressionForNonGenericIComparer(T t) : base(compareExpr: (x, y) => t.Compare(x, y)) { }
    }
}