// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// An interface that provides an expression-based way
    /// to specify the same thing that <see cref="IComparer&lt;T&gt;"/>
    /// does: a function for equality and a function for computing
    /// a hash.
    /// </summary>
    /// <typeparam name="T">The type over which equality is being defined.</typeparam>
    public interface IEqualityComparerExpression<T>
    {
        /// <summary>
        /// A function encoding equality over type <typeparamref name="T"/>
        /// </summary>
        /// <returns>true iff the two values should be considered equal.</returns>
        Expression<Func<T, T, bool>> GetEqualsExpr();

        /// <summary>
        /// A function that computes a hash value over type <typeparamref name="T"/>
        /// </summary>
        /// <returns>Any integer, but it is much more useful if it returns the same
        /// number only for values of type <typeparamref name="T"/> that are considered
        /// equal.</returns>
        Expression<Func<T, int>> GetGetHashCodeExpr();
    }

    internal static class EqualityComparerExpressionCache
    {
        private static readonly Dictionary<Type, object> typeComparerCache = new Dictionary<Type, object>();
        private static readonly Dictionary<Type, object> equalsCache = new Dictionary<Type, object>();
        private static readonly Dictionary<Type, object> getHashCodeCache = new Dictionary<Type, object>();

        static EqualityComparerExpressionCache()
        {
            typeComparerCache.Add(typeof(byte), new PrimitiveEqualityComparerExpression<byte>((x, y) => x == y, (obj) => obj));
            typeComparerCache.Add(typeof(sbyte), new PrimitiveEqualityComparerExpression<sbyte>((x, y) => x == y, (obj) => obj));
            typeComparerCache.Add(typeof(char), new PrimitiveEqualityComparerExpression<char>((x, y) => x == y, (obj) => obj));
            typeComparerCache.Add(typeof(short), new PrimitiveEqualityComparerExpression<short>((x, y) => x == y, (obj) => obj));
            typeComparerCache.Add(typeof(ushort), new PrimitiveEqualityComparerExpression<ushort>((x, y) => x == y, (obj) => obj));
            typeComparerCache.Add(typeof(int), new PrimitiveEqualityComparerExpression<int>((x, y) => x == y, (obj) => obj));
            typeComparerCache.Add(typeof(uint), new PrimitiveEqualityComparerExpression<uint>((x, y) => x == y, (obj) => (int)obj));
            typeComparerCache.Add(typeof(long), new PrimitiveEqualityComparerExpression<long>((x, y) => x == y, (obj) => (int)obj));
            typeComparerCache.Add(typeof(ulong), new PrimitiveEqualityComparerExpression<ulong>((x, y) => x == y, (obj) => (int)obj));
            typeComparerCache.Add(typeof(decimal), new ComparerExpressionForIEquatable<decimal>());
            typeComparerCache.Add(typeof(string), new StringEqualityComparerExpression());
            typeComparerCache.Add(typeof(TimeSpan), new ComparerExpressionForIEquatable<TimeSpan>());
            typeComparerCache.Add(typeof(DateTime), new ComparerExpressionForIEquatable<DateTime>());
            typeComparerCache.Add(typeof(DateTimeOffset), new ComparerExpressionForIEquatable<DateTimeOffset>());
            typeComparerCache.Add(typeof(Empty), new PrimitiveEqualityComparerExpression<Empty>((x, y) => true, (obj) => 0));
        }

        public static bool TryGetCachedComparer<T>(out IEqualityComparerExpression<T> comparer)
        {
            var t = typeof(T);
            comparer = null;
            if (typeComparerCache.TryGetValue(t, out object temp))
            {
                comparer = (IEqualityComparerExpression<T>)temp;
                return true;
            }
            return false;
        }

        public static bool TryGetCachedEqualsFunction<T>(out Func<T, T, bool> equalsFunction)
        {
            var t = typeof(T);
            equalsFunction = null;
            if (equalsCache.TryGetValue(t, out object temp))
            {
                equalsFunction = (Func<T, T, bool>)temp;
                return true;
            }
            return false;
        }

        public static bool TryGetCachedGetHashCodeFunction<T>(out Func<T, int> getHashCodeFunction)
        {
            var t = typeof(T);
            if (getHashCodeCache.TryGetValue(t, out object temp))
            {
                getHashCodeFunction = (Func<T, int>)temp;
                return true;
            }
            getHashCodeFunction = null;
            return false;
        }

        public static void Add<T>(IEqualityComparerExpression<T> comparer) => typeComparerCache.Add(typeof(T), comparer);

        public static void Add<T>(Func<T, T, bool> equalsFunction) => equalsCache.Add(typeof(T), equalsFunction);

        public static void Add<T>(Func<T, int> getHashCodeFunction) => getHashCodeCache.Add(typeof(T), getHashCodeFunction);
    }

    /// <summary>
    /// Provides an implementation for wrapping two functions
    /// into an <see cref="IEqualityComparerExpression&lt;T&gt;"/>.
    /// </summary>
    /// <typeparam name="T">The type for which the equality comparers are defined.</typeparam>
    public class EqualityComparerExpression<T> : IEqualityComparerExpression<T>
    {
        private static readonly object sentinel = new object();
        private static readonly object equalsSentinel = new object();
        private static readonly object getHashCodeSentinel = new object();

        private readonly Expression<Func<T, T, bool>> EqualsExpr;
        private readonly Expression<Func<T, int>> GetHashCodeExpr;

        /// <summary>
        /// Creates an instance to be used as an argument for many of the query methods.
        /// </summary>
        /// <param name="equalsExpr">
        /// A function used to test equality on type <typeparamref name="T"/>.
        /// </param>
        /// <param name="getHashCodeExpr">
        /// A function used to compute hash values for values of type <typeparamref name="T"/>.
        /// </param>
        public EqualityComparerExpression(Expression<Func<T, T, bool>> equalsExpr, Expression<Func<T, int>> getHashCodeExpr)
        {
            this.EqualsExpr = equalsExpr;
            this.GetHashCodeExpr = getHashCodeExpr;
        }

        /// <summary>
        /// A default equality comparer.
        /// </summary>
        public static IEqualityComparerExpression<T> Default
        {
            get
            {
                var type = typeof(T).GetTypeInfo();

                lock (sentinel)
                {
                    if (EqualityComparerExpressionCache.TryGetCachedComparer(out IEqualityComparerExpression<T> comparer))
                        return comparer;

                    if (type.ImplementsIEqualityComparerExpression())
                    {
                        if (type.IsValueType)
                        {
                            comparer = (IEqualityComparerExpression<T>)default(T);
                            EqualityComparerExpressionCache.Add(comparer);
                            return comparer;
                        }
                        var ctor = type.GetConstructor(Type.EmptyTypes);
                        if (ctor != null)
                        {
                            var result = ctor.Invoke(Array.Empty<object>());
                            comparer = (IEqualityComparerExpression<T>)result;
                            EqualityComparerExpressionCache.Add(comparer);
                            return comparer;
                        }
                    }

                    if (type.ImplementsIEqualityComparer())
                    {
                        // then fall back to using lambdas of the form:
                        // (x,y) => o.IEqualityComparer<T>.Equals(x,y)
                        // (x) => o.IEqualityComparer<T>.GetHashCode(x)
                        // for an arbitrary o that is created of type T by calling its nullary ctor (if such a ctor exists)
                        var genericInstanceOfComparerExpressionForIEqualityComparer = typeof(ComparerExpressionForIEqualityComparer<>).MakeGenericType(type);
                        var ctorForComparerExpressionForIEqualityComparer = genericInstanceOfComparerExpressionForIEqualityComparer.GetTypeInfo().GetConstructor(new Type[] { type });
                        if (ctorForComparerExpressionForIEqualityComparer != null)
                        {
                            var ctorForType = type.GetConstructor(Type.EmptyTypes);
                            if (ctorForType != null)
                            {
                                var instanceOfType = ctorForType.Invoke(Array.Empty<object>());
                                if (instanceOfType != null)
                                {
                                    var result = ctorForComparerExpressionForIEqualityComparer.Invoke(new object[] { instanceOfType, });
                                    comparer = (IEqualityComparerExpression<T>)result;
                                    EqualityComparerExpressionCache.Add(comparer);
                                    return comparer;
                                }
                            }
                        }
                    }

                    if (type.ImplementsIEquatable())
                    {
                        // then fall back to using lambdas of the form:
                        // (x,y) => x.IEquatable<T>.Equals(y)
                        // (x) => x.GetHashCode()
                        var genericInstanceOfComparerExpressionForIEquatable = typeof(ComparerExpressionForIEquatable<>).MakeGenericType(type);
                        var ctorForComparerExpressionForIEquatable = genericInstanceOfComparerExpressionForIEquatable.GetTypeInfo().GetConstructor(Type.EmptyTypes);
                        var comparerExpression = ctorForComparerExpressionForIEquatable.Invoke(Array.Empty<object>());
                        comparer = (IEqualityComparerExpression<T>)comparerExpression;
                        EqualityComparerExpressionCache.Add(comparer);
                        return comparer;
                    }

                    if (type.IsCompoundGroupKey(out var t1, out var t2))
                    {
                        // equivalent to: return new CompoundGroupKeyEqualityComparer<T1, T2>(EqualityComparerExpression<T1>.Default, EqualityComparerExpression<T2>.Default);
                        var equalityComparerExpressionOfT1 = typeof(EqualityComparerExpression<>).MakeGenericType(t1);
                        var defaultPropertyForT1 = equalityComparerExpressionOfT1.GetTypeInfo().GetProperty("Default");
                        var default1 = defaultPropertyForT1.GetValue(null);

                        var equalityComparerExpressionOfT2 = typeof(EqualityComparerExpression<>).MakeGenericType(t2);
                        var defaultPropertyForT2 = equalityComparerExpressionOfT2.GetTypeInfo().GetProperty("Default");
                        var default2 = defaultPropertyForT2.GetValue(null);

                        var cgkec = typeof(CompoundGroupKeyEqualityComparer<,>);
                        var genericInstance = cgkec.MakeGenericType(t1, t2);
                        var ctor = genericInstance.GetTypeInfo().GetConstructor(new Type[] { equalityComparerExpressionOfT1, equalityComparerExpressionOfT2, });
                        var result = ctor.Invoke(new object[] { default1, default2, });
                        comparer = (IEqualityComparerExpression<T>)result;
                        EqualityComparerExpressionCache.Add(comparer);
                        return comparer;
                    }

                    if (type.IsGenericType && type.GenericTypeArguments.Length == 1 && type.GetGenericTypeDefinition() == typeof(PartitionKey<>))
                    {
                        var t = type.GenericTypeArguments[0];
                        var equalityComparerExpressionOfT = typeof(EqualityComparerExpression<>).MakeGenericType(t);
                        var defaultPropertyForT = equalityComparerExpressionOfT.GetTypeInfo().GetProperty("Default");
                        var default1 = defaultPropertyForT.GetValue(null);

                        var pkec = typeof(ComparerExpressionForPartitionKey<>);
                        var genericInstance = pkec.MakeGenericType(t);
                        var ctor = genericInstance.GetTypeInfo().GetConstructor(new Type[] { equalityComparerExpressionOfT, });
                        var result = ctor.Invoke(new object[] { default1, });
                        comparer = (IEqualityComparerExpression<T>)result;
                        EqualityComparerExpressionCache.Add(comparer);
                        return comparer;
                    }

                    if (type.IsAnonymousTypeName())
                    {
                        var tup = ExpressionsForAnonymousType(type);
                        comparer = new EqualityComparerExpression<T>(tup.Item1, tup.Item2);
                        EqualityComparerExpressionCache.Add(comparer);
                        return comparer;
                    }

                    if (IsSimpleStruct(type))
                    {
                        var tup = ExpressionsForTypeByFields(type);
                        comparer = new EqualityComparerExpression<T>(tup.Item1, tup.Item2);
                        EqualityComparerExpressionCache.Add(comparer);
                        return comparer;
                    }

                    comparer = new GenericEqualityComparerExpression<T>();
                    EqualityComparerExpressionCache.Add(comparer);
                    return comparer;
                }
            }
        }

        /// <summary>
        /// Returns an expression that computes the default equality function for the given type.
        /// </summary>
        public static Func<T, T, bool> DefaultEqualsFunction
        {
            get
            {
                Func<T, T, bool> equals;
                lock (equalsSentinel)
                {
                    if (!EqualityComparerExpressionCache.TryGetCachedEqualsFunction(out equals))
                    {
                        equals = Default.GetEqualsExpr().Compile();
                        EqualityComparerExpressionCache.Add(equals);
                    }
                }
                return equals;
            }
        }

        /// <summary>
        /// Returns an expression that computes the default hash code
        /// </summary>
        public static Func<T, int> DefaultGetHashCodeFunction
        {
            get
            {
                Func<T, int> getHashCode;
                lock (getHashCodeSentinel)
                {
                    if (!EqualityComparerExpressionCache.TryGetCachedGetHashCodeFunction(out getHashCode))
                    {
                        getHashCode = Default.GetGetHashCodeExpr().Compile();
                        EqualityComparerExpressionCache.Add(getHashCode);
                    }
                }
                return getHashCode;
            }
        }

        /// <summary>
        /// Determines if <paramref name="type"/> is just a struct that has nothing
        /// but public fields, i.e., a true "record". It may have an override for
        /// ToString, but not overrides for GetHashCode or Equals. It also may not have
        /// any properties at all.
        /// </summary>
        private static bool IsSimpleStruct(TypeInfo type)
            => type.IsValueType
            && !type.IsPrimitive
            && !Recursive(type)
            && type.GetMethod("GetHashCode").DeclaringType == typeof(ValueType)
            && type.GetMethod("Equals").DeclaringType == typeof(ValueType)
            && type.GetProperties().Length == 0
            && type.GetFields().All(f => f.IsPublic);

        private static bool Recursive(TypeInfo type)
        {
            var hashSet = new HashSet<TypeInfo> { type };
            return RecursiveHelper(type, hashSet);
        }

        private static bool RecursiveHelper(TypeInfo type, HashSet<TypeInfo> hashSet)
        {
            var fields = type.GetFields(BindingFlags.Public | BindingFlags.Instance);
            foreach (var field in fields)
            {
                var t = field.FieldType.GetTypeInfo();
                if (!t.IsValueType || t.IsPrimitive) continue;
                if (hashSet.Contains(t)) return true;
                hashSet.Add(t);
                var b = RecursiveHelper(t, hashSet);
                if (b) return true;
                hashSet.Remove(t);
            }
            return false;
        }

        /// <summary>
        /// An accessor for the equals function.
        /// </summary>
        /// <returns>The function used for equality tests.</returns>
        public Expression<Func<T, T, bool>> GetEqualsExpr() => this.EqualsExpr;

        /// <summary>
        /// An accessor for the hash function.
        /// </summary>
        /// <returns>The function used for computing hashes.</returns>
        public Expression<Func<T, int>> GetGetHashCodeExpr() => this.GetHashCodeExpr;

        /// <summary>
        /// Returns (unfortunately) weakly-typed expressions for the two functions
        /// for checking equality and getting the hash code for the anonymous
        /// type <paramref name="t"/>.
        /// The body of the equals function is the conjunction of calling "equals" on each property contained in the anonymous type.
        /// That is, the first one (for equality) looks like this:
        /// (A a1, A a2) =&gt; "equals"(a1.P1, a2.P1) &amp;&amp; "equals"(a1.P2, a2.P2) &amp;&amp; ... "equals"(a1.Pn, a2.Pn)
        /// The body of the second one (for hash code) is just the xor of calling "getHashCode" on each property contained in the anonymous type.
        /// That is, it looks like this:
        /// (A a) =&gt; "getHashCode"(a.P1) ^ "getHashCode"(a.P2) &amp;&amp; ... "getHashCode"(a.Pn)
        /// where
        ///     - the anonymous type had been defined as new{ P1 = ..., ...., Pn = ...}.
        ///     - "equals" is the expression returned from EqualityComparerExpression&lt;U&gt;.Default.GetEqualsExpr()
        ///     - "getHashCode" is the expression returned EqualityComparerExpression&lt;U&gt;.Default.GetGetHashCodeExpr()
        ///     - U is the type of the corresponding property
        /// So really they are not function calls, but instead expressions
        /// </summary>
        /// <param name="t">
        /// Must be an anonymous type
        /// </param>
        /// <returns>
        /// When <paramref name="t"/> is null or not an anonymous type, then null is
        /// returned. Otherwise, an expression which is defined as above.
        /// </returns>
        private static Tuple<Expression<Func<T, T, bool>>, Expression<Func<T, int>>> ExpressionsForAnonymousType(Type t)
        {
            if (t == null || !t.IsAnonymousTypeName()) return null;
            var properties = t.GetTypeInfo().GetProperties(BindingFlags.Public | BindingFlags.Instance);
            var left = Expression.Parameter(t, "left");
            var right = Expression.Parameter(t, "right");
            var a = Expression.Parameter(t, "a");
            var clauses = properties.Select(p => MakeEqualityAndHashCodeExpressions(left, right, a, p.PropertyType, p.Name));
            var equalsClauses = clauses.Select(c => c.Item1);
            var equalsLambdaBody = equalsClauses.Aggregate((e1, e2) => Expression.AndAlso(e1, e2));
            var hashCodeClauses = clauses.Select(c => c.Item2);
            var hashCodeLambdaBody = hashCodeClauses.Aggregate((e1, e2) => Expression.ExclusiveOr(e1, e2));
            var equalsLambda = Expression.Lambda(equalsLambdaBody, left, right) as Expression<Func<T, T, bool>>;
            var hashCodeLambda = Expression.Lambda(hashCodeLambdaBody, a) as Expression<Func<T, int>>;
            return Tuple.Create(equalsLambda, hashCodeLambda);
        }

        private static Tuple<Expression<Func<T, T, bool>>, Expression<Func<T, int>>> ExpressionsForTypeByFields(Type t)
        {
            var fields = t.GetTypeInfo().GetFields(BindingFlags.Public | BindingFlags.Instance);
            var left = Expression.Parameter(t, "left");
            var right = Expression.Parameter(t, "right");
            var a = Expression.Parameter(t, "a");
            if (fields.Length == 0)
            {
                return Tuple.Create(

                    // (l,r) => true
                    Expression.Lambda(Expression.Constant(true, typeof(bool)), left, right) as Expression<Func<T, T, bool>>,

                    // a => 0
                    Expression.Lambda(Expression.Constant(0, typeof(int)), a) as Expression<Func<T, int>>);
            }
            var clauses = fields.Select(f => MakeEqualityAndHashCodeExpressions(left, right, a, f.FieldType, f.Name));
            var equalsClauses = clauses.Select(c => c.Item1);
            var equalsLambdaBody = equalsClauses.Aggregate((e1, e2) => Expression.AndAlso(e1, e2));
            var hashCodeClauses = clauses.Select(c => c.Item2);
            var hashCodeLambdaBody = hashCodeClauses.Aggregate((e1, e2) => Expression.ExclusiveOr(e1, e2));
            var equalsLambda = Expression.Lambda(equalsLambdaBody, left, right) as Expression<Func<T, T, bool>>;
            var hashCodeLambda = Expression.Lambda(hashCodeLambdaBody, a) as Expression<Func<T, int>>;
            return Tuple.Create(equalsLambda, hashCodeLambda);
        }

        private static Tuple<Expression, Expression> MakeEqualityAndHashCodeExpressions(ParameterExpression left, ParameterExpression right, ParameterExpression a, Type pType, string pName)
        {
            var equalityComparerTypeForPropertyType = typeof(EqualityComparerExpression<>).MakeGenericType(pType);
            var equalityComparerDefaultProperty = equalityComparerTypeForPropertyType.GetTypeInfo().GetProperty("Default");
            var getter = equalityComparerDefaultProperty.GetMethod;
            var equalityComparerExpressionObject = getter.Invoke(null, null);
            var equalityComparerExpressionObjectType = equalityComparerExpressionObject.GetType();
            var equalityComparerExpression = (LambdaExpression)equalityComparerExpressionObjectType.GetTypeInfo()
                .GetMethod("GetEqualsExpr").Invoke(equalityComparerExpressionObject, null);
            var hashCodeExpression = (LambdaExpression)equalityComparerExpressionObjectType.GetTypeInfo()
                .GetMethod("GetGetHashCodeExpr").Invoke(equalityComparerExpressionObject, null);
            var inlinedEqualityExpression = equalityComparerExpression.ReplaceParametersInBody(Expression.PropertyOrField(left, pName), Expression.PropertyOrField(right, pName));
            var inlinedHashCodeExpression = hashCodeExpression.ReplaceParametersInBody(Expression.PropertyOrField(a, pName));
            return Tuple.Create(inlinedEqualityExpression, inlinedHashCodeExpression);
        }

        internal static bool IsSimpleDefault(IEqualityComparerExpression<T> input)
            => input == Default && input is PrimitiveEqualityComparerExpression<T>;
    }

    internal sealed class GenericEqualityComparerExpression<T> : EqualityComparerExpression<T>
    {
        public GenericEqualityComparerExpression()
            : base(
                equalsExpr: (x, y) => EqualityComparer<T>.Default.Equals(x, y),
                getHashCodeExpr: ComputeGetHashCodeExpr())
        { }

        private static Expression<Func<T, int>> ComputeGetHashCodeExpr()
        {
            var type = typeof(T);
            if (type.GetTypeInfo().IsGenericType)
            {
                var def = type.GetGenericTypeDefinition();
                if (def.Equals(typeof(Nullable<>)))
                {
                    var args = type.GetTypeInfo().GetGenericArguments();
                    Contract.Assume(args.Length == 1);
                    if (ShouldUseCastToInt(args[0]))
                    {
                        // (a) => a == null ? 0 : (int)a;
                        var a = Expression.Parameter(type, "a");
                        var lambda = Expression.Lambda(
                            Expression.Condition(
                                Expression.Equal(a, Expression.Constant(null, typeof(T))),
                                Expression.Constant(0, typeof(int)),
                                Expression.Convert(a, typeof(int))), a) as Expression<Func<T, int>>;
                        return lambda;
                    }
                }
            }
            return (obj) => EqualityComparer<T>.Default.GetHashCode(obj);
        }

        private static bool ShouldUseCastToInt(Type t)
        {
            if (t == typeof(byte)) return true;
            if (t == typeof(short)) return true;
            if (t == typeof(int)) return true;
            if (t == typeof(long)) return true;
            if (t.GetTypeInfo().IsGenericType)
            {
                var def = t.GetGenericTypeDefinition();
                if (def.Equals(typeof(Nullable<>)))
                {
                    var args = t.GetTypeInfo().GetGenericArguments();
                    Contract.Assume(args.Length == 1);
                    return ShouldUseCastToInt(args[0]);
                }
            }
            return false;
        }
    }

    internal class PrimitiveEqualityComparerExpression<T> : EqualityComparerExpression<T>
    {
        public PrimitiveEqualityComparerExpression(Expression<Func<T, T, bool>> equalsExpr, Expression<Func<T, int>> getHashCodeExpr)
            : base(equalsExpr, getHashCodeExpr) { }
    }

    internal sealed class StringEqualityComparerExpression : PrimitiveEqualityComparerExpression<string>
    {
        public StringEqualityComparerExpression()
            : base(
                  equalsExpr: (x, y) => x == y,
                  getHashCodeExpr: obj => obj == null ? 0 : obj.StableHash())
        { }
    }

    internal sealed class ComparerExpressionForIEqualityComparer<T> : EqualityComparerExpression<T> where T : IEqualityComparer<T>
    {
        public ComparerExpressionForIEqualityComparer(T t)
            : base(
                equalsExpr: (x, y) => t.Equals(x, y),
                getHashCodeExpr: (obj) => t.GetHashCode(obj))
        { }
    }

    internal sealed class ComparerExpressionForIEquatable<T> : PrimitiveEqualityComparerExpression<T> where T : IEquatable<T>
    {
        public ComparerExpressionForIEquatable()
            : base(
                equalsExpr: (x, y) => x.Equals(y),
                getHashCodeExpr: (obj) => obj.GetHashCode())
        { }
    }

    internal sealed class ComparerExpressionForPartitionKey<T> : IEqualityComparerExpression<PartitionKey<T>>
    {
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Comparer is a function and thus immutable")]
        public readonly IEqualityComparerExpression<T> baseComparer;

        public ComparerExpressionForPartitionKey(IEqualityComparerExpression<T> comparer) => this.baseComparer = comparer;

        public Expression<Func<PartitionKey<T>, PartitionKey<T>, bool>> GetEqualsExpr()
        {
            Expression<Func<PartitionKey<T>, T>> keyExtractor1 = o => o.Key;
            Expression<Func<PartitionKey<T>, T>> keyExtractor2 = o => o.Key;
            var baseGetEqualsExpr = this.baseComparer.GetEqualsExpr();
            var newExpressionBody = baseGetEqualsExpr.ReplaceParametersInBody(keyExtractor1.Body, keyExtractor2.Body);
            return Expression.Lambda<Func<PartitionKey<T>, PartitionKey<T>, bool>>(
                newExpressionBody,
                keyExtractor1.Parameters[0],
                keyExtractor2.Parameters[0]);
        }

        public Expression<Func<PartitionKey<T>, int>> GetGetHashCodeExpr()
        {
            Expression<Func<PartitionKey<T>, T>> keyExtractor = o => o.Key;
            var baseGetEqualsExpr = this.baseComparer.GetGetHashCodeExpr();
            var newExpressionBody = baseGetEqualsExpr.ReplaceParametersInBody(keyExtractor.Body);
            return Expression.Lambda<Func<PartitionKey<T>, int>>(
                newExpressionBody,
                keyExtractor.Parameters[0]);
        }
    }
}