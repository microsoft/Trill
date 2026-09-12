// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Text;

namespace Microsoft.StreamProcessing
{
    internal static class TypeExtensions
    {
        static TypeExtensions()
        {
            OperatorNameLookup["+"] = "op_Addition";
            OperatorNameLookup["-"] = "op_Subtraction";
            OperatorNameLookup["*"] = "op_Multiply";
            OperatorNameLookup["*d"] = "op_Multiply";
            OperatorNameLookup["/"] = "op_Division";
            OperatorNameLookup["/d"] = "op_Division";
            OperatorNameLookup["%"] = "op_Modulus";
            OperatorNameLookup["%d"] = "op_Modulus";

            foreach (var pair in OperatorNameLookup)
            {
                KnownSupportedOperators.TryAdd(
                    pair.Key, [typeof(long), typeof(ulong), typeof(int), typeof(uint), typeof(short), typeof(ushort), typeof(double), typeof(float), typeof(decimal), typeof(byte), typeof(sbyte)]);
            }
        }

        public static PropertyInfo GetPropertyByName(this Type type, string name)
            => type.GetProperty(name, BindingFlags.Public | BindingFlags.Instance);

        public static MethodInfo GetMethodByName(this Type type, string shortName, params Type[] arguments)
        {
            var result = type
                .GetMethods(BindingFlags.Instance | BindingFlags.Public)
                .SingleOrDefault(m => m.Name == shortName && m.GetParameters().Select(p => p.ParameterType).SequenceEqual(arguments));

            return result ?? type
                                 .GetMethods(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)
                                 .FirstOrDefault(m => (m.Name.EndsWith(shortName, StringComparison.Ordinal) ||
                                      m.Name.EndsWith("." + shortName, StringComparison.Ordinal))
                                   && m.GetParameters().Select(p => p.ParameterType).SequenceEqual(arguments));
        }

        public static bool CanContainNull(this Type type)
            => !type.IsValueType || Nullable.GetUnderlyingType(type) != null;

        public static bool CanBeKnownTypeOf(this Type type, Type baseType)
        {
            return !type.IsAbstract
                && !type.IsUnsupported()
                && (type.IsSubclassOf(baseType)
                    || type == baseType
                    || (baseType.IsInterface && baseType.IsAssignableFrom(type))
                    || (baseType.IsGenericType && baseType.IsInterface && baseType.GenericIsAssignable(type)
                        && type.GetGenericArguments()
                                   .Zip(baseType.GetGenericArguments(), (type1, type2) => new Tuple<Type, Type>(type1, type2))
                                   .ToList()
                                   .TrueForAll(tuple => CanBeKnownTypeOf(tuple.Item1, tuple.Item2))));
        }

        private static bool GenericIsAssignable(this Type type, Type instanceType)
        {
            if (!type.IsGenericType || !instanceType.IsGenericType) return false;

            var args = type.GetGenericArguments();
            var typeDefinition = instanceType.GetGenericTypeDefinition();
            var args2 = typeDefinition.GetGenericArguments();
            return args.Length != 0 && args.Length == args2.Length && type.IsAssignableFrom(typeDefinition.MakeGenericType(args));
        }

        public static IEnumerable<Type> GetAllKnownTypes(this Type t)
        {
            var types = t.GetCustomAttributes(true)
                .OfType<KnownTypeAttribute>()
                .SelectMany(a =>
                    a.Type != null
                    ? new Type[] { a.Type }
                    : (IEnumerable<Type>)t.GetMethod(a.MethodName, BindingFlags.NonPublic | BindingFlags.Static).Invoke(null, []));
            if (t.BaseType != null) types = types.Concat(GetAllKnownTypes(t.BaseType));
            return types;
        }

        public static string GetValidIdentifier(this Type t) => t.GetCSharpSourceSyntax().CleanUpIdentifierName();

        public static int ReadAllRequiredBytes(this Stream stream, byte[] buffer, int offset, int count)
        {
            return ReadAllRequiredBytes(stream, buffer.AsSpan(offset, count));
        }

        /// <summary>
        /// Reads up to <paramref name="buffer"/>.Length bytes into <paramref name="buffer"/>; returns total bytes read.
        /// </summary>
        public static int ReadAllRequiredBytes(this Stream stream, Span<byte> buffer)
        {
            int totalRead = 0;
            while (totalRead < buffer.Length)
            {
                int n = stream.Read(buffer[totalRead..]);
                if (n == 0)
                    break;
                totalRead += n;
            }

            return totalRead;
        }

        public static Tuple<IEnumerable<MyFieldInfo>, bool> GetAnnotatedFields(this Type t)
        {
            var fields = t.ResolveMembers();
            return fields.Any()
                ? Tuple.Create(fields, false)
                : Tuple.Create(new MyFieldInfo(t).Yield(), true);
        }

        private static readonly ConcurrentDictionary<string, string> OperatorNameLookup = new();

        private static readonly ConcurrentDictionary<string, HashSet<Type>> KnownSupportedOperators = new();

        public static bool SupportsOperator(this Type t, string @operator)
        {
            if (OperatorNameLookup.TryGetValue(@operator, out string methodName))
            {
                if (KnownSupportedOperators[@operator].Contains(t)) return true;

                // Get all operator methods that have the correct name for the given operator and have the given type as the first parameter and return type.
                var secondParameter = @operator.EndsWith('d') ? typeof(double) : t;
                var operatorMethods = t.GetMethods(BindingFlags.Static | BindingFlags.Public)
                    .Where(o => o.CallingConvention == CallingConventions.Standard
                        && o.IsSpecialName
                        && o.Name == methodName
                        && o.ReturnType == t)
                    .Select(o => o.GetParameters())
                    .Where(o => o.Length == 2
                        && o[0].ParameterType == t
                        && o[1].ParameterType == secondParameter);
                if (operatorMethods.Any())
                {
                    KnownSupportedOperators[@operator].Add(t);
                    return true;
                }
                return false;

            }
            throw new InvalidOperationException("Unknown operator: " + @operator);
        }

        public static Type InstantiateAsNecessary(this Type type, params Type[] types)
        {
            ArgumentNullException.ThrowIfNull(type);
            ArgumentNullException.ThrowIfNull(types);

            if (type == null) throw new NullReferenceException(nameof(type));
            var genericArgs = types
                .Distinct()
                .Where(IsAnonymousType);
            return !genericArgs.Any()
                ? type
                : type.MakeGenericType(genericArgs.ToArray());
        }

        public static bool IsAnonymousTypeName(this Type type)
        {
            ArgumentNullException.ThrowIfNull(type);

            return type.IsClass
                && type.IsDefined(typeof(CompilerGeneratedAttribute))
                && !type.IsNested
                && type.Name.StartsWith("<>", StringComparison.Ordinal)
                && type.Name.Contains("__Anonymous", StringComparison.Ordinal);
        }

        /// <summary>
        /// Returns true if <paramref name="type"/> is an anonymous type or is a generic type
        /// with an anonymous type somewhere in the type tree(s) of its type arguments.
        /// REVIEW: Is there a better way to tell if a type represents an anonymous type?
        /// </summary>
        [Pure]
        public static bool IsAnonymousType(this Type type)
        {
            ArgumentNullException.ThrowIfNull(type);

            return type.IsAnonymousTypeName()
                || type.Assembly.IsDynamic
                || (type.IsGenericType && type.GenericTypeArguments.Any(IsAnonymousType));
        }

        public static bool HasSupportedParameterizedConstructor(this Type type)
        {
            // There are certain built-in types that have no parameterless constructor but can still be supported.
            // This method enumerates those cases.
            // Pattern: Constructor whose types and cardinality match the names and number of read-only properties.

            // Case 1: Anonymous types
            if (type.IsAnonymousTypeName()) return true;

            // Case 2: Key-Value pairs
            if (type.IsGenericType
                && type.GetGenericTypeDefinition() == typeof(KeyValuePair<,>)) return true;

            // Case 3: Tuples
            if (type.IsGenericType)
            {
                var baseType = type.GetGenericTypeDefinition();
                if (
                    baseType == typeof(Tuple<>) ||
                    baseType == typeof(Tuple<,>) ||
                    baseType == typeof(Tuple<,,>) ||
                    baseType == typeof(Tuple<,,,>) ||
                    baseType == typeof(Tuple<,,,,>) ||
                    baseType == typeof(Tuple<,,,,,>) ||
                    baseType == typeof(Tuple<,,,,,,>) ||
                    baseType == typeof(Tuple<,,,,,,,>)) return true;
            }

            // Case 4: StreamEvent and PartitionedStreamEvent — readonly structs deserialized via constructor
            if (type.IsGenericType)
            {
                var baseType = type.GetGenericTypeDefinition();
                if (baseType == typeof(StreamEvent<>) ||
                    baseType == typeof(PartitionedStreamEvent<,>)) return true;
            }

            return false;
        }

        /// <summary>
        /// Gets all fields of the type.
        /// </summary>
        /// <param name="t">The type.</param>
        /// <returns>Collection of fields.</returns>
        public static IEnumerable<FieldInfo> GetAllFields(this Type t)
        {
            if (t == null) return [];

            const BindingFlags Flags =
                BindingFlags.Public |
                BindingFlags.NonPublic |
                BindingFlags.Instance |
                BindingFlags.DeclaredOnly;
            var returnValue = t
                
                .GetFields(Flags)
                .Where(f => !f.IsDefined(typeof(CompilerGeneratedAttribute), false))
                .Concat(GetAllFields(t.BaseType));
            if (!t.HasSupportedParameterizedConstructor()) returnValue = returnValue.OrderBy(o => o.Name);
            return returnValue;
        }

        /// <summary>
        /// Gets all properties of the type.
        /// </summary>
        /// <param name="t">The type.</param>
        /// <returns>Collection of properties.</returns>
        public static IEnumerable<PropertyInfo> GetAllProperties(this Type t)
        {
            if (t == null) return [];

            const BindingFlags Flags =
                BindingFlags.Public |
                BindingFlags.NonPublic |
                BindingFlags.Instance |
                BindingFlags.DeclaredOnly;
            var returnValue = t
                
                .GetProperties(Flags)
                .Where(p => !p.IsDefined(typeof(CompilerGeneratedAttribute), false)
                            && p.GetIndexParameters().Length == 0
                            && !p.IsSpecialName
                            && p.CanRead && p.CanWrite)
                .Concat(GetAllProperties(t.BaseType));
            if (!t.HasSupportedParameterizedConstructor()) returnValue = returnValue.OrderBy(o => o.Name);
            return returnValue;
        }

        private static void GetAnonymousTypes(Type t, List<Type> partialList)
        {
            if (t.IsAnonymousTypeName()) partialList.Add(t);
            else if (t.IsGenericType)
            {
                foreach (var genericArgument in t.GenericTypeArguments)
                {
                    GetAnonymousTypes(genericArgument, partialList);
                }
            }
        }

        public static List<Type> GetAnonymousTypes(this Type t)
        {
            ArgumentNullException.ThrowIfNull(t);

            var list = new List<Type>();
            GetAnonymousTypes(t, list);
            return list;
        }

        public static bool MemoryPoolHasGetMethodFor(this Type type) => type == typeof(int) || type == typeof(long) || type == typeof(string);

        /// <summary>
        /// Returns true iff <paramref name="keyType"/> is a type for which
        /// a generated memory pool is needed. That is needed when the
        /// <paramref name="keyType"/> is either an atomic type for which
        /// the memory pool does not have a Get method for or is a CompoundGroupKey
        /// whose TOuterKey or TInnerKey requires a generated memory pool.
        /// </summary>
        /// <param name="keyType"></param>
        /// <returns></returns>
        public static bool KeyTypeNeedsGeneratedMemoryPool(this Type keyType)
        {
            if (keyType == typeof(Empty)) return false;
            if (keyType.IsGenericType)
            {
                if (keyType.GetGenericTypeDefinition() != typeof(CompoundGroupKey<,>)) return true;
                else
                {
                    var outerKeyType = keyType.GetField("outerGroup").FieldType;
                    var innerKeyType = keyType.GetField("innerGroup").FieldType;
                    return outerKeyType.KeyTypeNeedsGeneratedMemoryPool() || innerKeyType.KeyTypeNeedsGeneratedMemoryPool();
                }
            }
            return keyType.NeedGeneratedMemoryPool();
        }

        /// <summary>
        /// Returns true iff <paramref name="type"/> is a type for which,
        /// if it is decomposed into its columns, has a type that is not
        /// handled by the base MemoryPool type (i.e., something that is
        /// not a long, int, or string).
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        private static bool NeedGeneratedMemoryPool(this Type type)
        {
            ArgumentNullException.ThrowIfNull(type);

            if (type.MemoryPoolHasGetMethodFor()) return false;

            if (type.HasSupportedParameterizedConstructor())
            {
                foreach (var p in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                {
                    var t = p.PropertyType;
                    if (t != typeof(int) && t != typeof(long) && t != typeof(string))
                        return true;
                }
            }
            else
            {
                foreach (var f in type.GetFields(BindingFlags.Public | BindingFlags.Instance))
                {
                    var t = f.FieldType;
                    if (t != typeof(int) && t != typeof(long) && t != typeof(string))
                        return true;
                }
            }

            return false; // didn't find a type for which the base MemoryPool can't handle
        }

        /// <summary>
        /// Returns true iff <paramref name="type"/> can be used as a *non* key
        /// type for code gen. (All types can be used as key types.)
        /// </summary>
        public static bool CanRepresentAsColumnar(this Type type)
        {
            ArgumentNullException.ThrowIfNull(type);

            // If any public instance fields are anonymous types, then they
            // cannot be represented as columns.
            var fields = type.GetFields(BindingFlags.Public | BindingFlags.Instance);
            if (fields.Any(f => f.FieldType.IsAnonymousTypeName())) return false;

            // However, an anonymous type can be decomposed into its "fields" (NOTE: anonymous types
            // have public properties, not fields) as long as they themselves are not anonymous types.
            if (type.IsAnonymousTypeName())
            {
                var props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
                return props.All(p => !p.PropertyType.IsAnonymousTypeName());
            }

            // If the type isn't visible outside of its assembly, then code gen can't compile
            // since it has to have a reference to the type in the code.
            // Need to check this after the anonymous test because anonymous
            // types say they are not visible.
            if (!type.IsVisible) return false;

            // The type must be an "open" container: that is, its state is just the value
            // of all of its fields and autoprops. Otherwise we cannot guarantee that we can
            // reconstitute a value from its columnar representation.
            if (!type.IsPrimitive)
            {
                var allFields = type
                    .GetFields(BindingFlags.NonPublic | BindingFlags.Instance)
                    .Where(f => !(f.Name.StartsWith('<') && f.Name.EndsWith(">k__BackingField", StringComparison.Ordinal))) // ignore backing fields for autoprops
                    ;
                if (allFields.Any()) return false;

                // A public instance field only works if columnar reconstitution can assign it
                // directly; a readonly field (as in a "readonly struct" or a hand-written
                // immutable type) can only be set from a constructor, which we do not generate.
                if (fields.Any(f => f.IsInitOnly)) return false;

                // Check only the autoprops: any non-autoprop will not be able to depend on non-public fields
                // due to the check for non-public fields.
                var allProperties = type.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
                foreach (var p in allProperties)
                {
                    var getMethod = p.GetMethod;
                    if (getMethod == null) continue;
                    if (!getMethod.IsDefined(typeof(CompilerGeneratedAttribute))) continue;
                    var setMethod = p.SetMethod;
                    if (setMethod == null)
                    {
                        // A get-only autoprop (no setter at all) cannot be assigned during
                        // columnar reconstitution, which sets each property via a plain
                        // assignment - UNLESS its getter is non-public, in which case this isn't
                        // user-facing row state at all: every `record` (but not `record struct`)
                        // synthesizes a `protected virtual Type EqualityContract => typeof(T);`
                        // exactly like this (compiler-generated, get-only, non-public). It has no
                        // independent storage - it is always re-derivable from the runtime type -
                        // so there is nothing for columnar reconstitution to lose by not touching it.
                        if (!getMethod.IsPublic) continue;
                        return false;
                    }
                    if (!setMethod.IsDefined(typeof(CompilerGeneratedAttribute))) continue;

                    // p is definitely an autoprop. Cannot columnarize if the property is not visible.
                    if (!(getMethod.IsPublic && setMethod.IsPublic)) return false;

                    // An init-only setter (`{ get; init; }`) has IsExternalInit among its return
                    // parameter's required custom modifiers. It is public and compiler-generated
                    // like a normal autoprop setter, but callable only from a constructor or an
                    // object initializer - not from the plain assignment columnar reconstitution
                    // generates - so it must be excluded explicitly.
                    if (setMethod.ReturnParameter.GetRequiredCustomModifiers().Contains(typeof(IsExternalInit)))
                        return false;
                }
            }

            if (type.IsValueType) // we can always create instances of a struct
                return true;

            if (type.IsAbstract || type.IsInterface) // until we determine how we do columnar over a polymorphic set, we cannot do columnar over an abstract type or an interface
                return false;

            // otherwise it must have a nullary ctor that can be called
            // when a query cannot be transformed to a columnar representation
            // (and so instances of the type must be dynamically generated
            // as part of the query processing)
            var ctor = type.GetConstructor(Type.EmptyTypes);
            return ctor != null;
        }

        public static Type GetPartitionType(this Type type)
        {
            if (type.IsConstructedGenericType)
            {
                if (type.GetGenericTypeDefinition() == typeof(CompoundGroupKey<,>))
                    return GetPartitionType(type.GenericTypeArguments[0]);
                if (type.GetGenericTypeDefinition() == typeof(PartitionKey<>))
                    return type.GenericTypeArguments[0];
            }
            return null;
        }

        private static string TurnTypeIntoCSharpSource(Type t, ref List<string> introducedGenericTypeParameters)
        {
            ArgumentNullException.ThrowIfNull(t);
            ArgumentNullException.ThrowIfNull(introducedGenericTypeParameters);

            var typeName = t.FullName.Replace('#', '_').Replace('+', '.');
            if (t.IsAnonymousTypeName())
            {
                var newGenericTypeParameter = t.FullName.CleanUpIdentifierName(); // "A" + introducedGenericTypeParameters.Count.ToString(CultureInfo.InvariantCulture);
                introducedGenericTypeParameters.Add(newGenericTypeParameter);
                return newGenericTypeParameter;
            }
            if (!t.IsGenericType) // need to test after anonymous because deserialized anonymous types are *not* generic (but unserialized anonymous types *are* generic)
                return typeName;
            var isDynamic = t.Assembly.IsDynamic;
            var sb = new StringBuilder();
            if (!string.IsNullOrWhiteSpace(t.Namespace))
            {
                sb.Append(t.Namespace);
                sb.Append('.');
            }
            var genericArgs = new List<string>();
            foreach (var genericArgument in t.GenericTypeArguments)
            {
                string name = TurnTypeIntoCSharpSource(genericArgument, ref introducedGenericTypeParameters);
                genericArgs.Add(name);
            }

            // Need to handle nested types, e.g., T1<X,Y>.T2<A,B,C> in C#
            // The names look like this: T1`2.T2`3 where there are 5 generic arguments
            // The generic arguments go into different places in the source version of the type name.
            var indexIntoGenericArguments = 0;
            var listOfTypes = new List<Type>();
            var t2 = t;
            while (t2 != null)
            {
                listOfTypes.Add(t2);
                t2 = t2.DeclaringType;
            }
            listOfTypes.Reverse(); // start at the root of the nested type chain
            for (int typeIndex = 0; typeIndex < listOfTypes.Count; typeIndex++)
            {
                var currentType = listOfTypes[typeIndex];
                var currentName = currentType.Name;
                var indexOfBackTick = currentType.Name.IndexOf('`');

                if (typeIndex > 0) sb.Append('.');
                sb.Append(indexOfBackTick > 0 ? currentName[..indexOfBackTick] : currentName);
                if (indexOfBackTick > 0)
                {
                    var j = indexOfBackTick + 1;
                    while (j < currentName.Length && char.IsDigit(currentName[j])) j++;
                    var numberOfGenerics = int.Parse(currentName[(indexOfBackTick + 1)..j]);
                    sb.Append('<');
                    if (!isDynamic)
                    {
                        for (int i = 0; i < numberOfGenerics; i++)
                        {
                            if (i > 0) { sb.Append(", "); }
                            sb.Append(genericArgs[indexIntoGenericArguments]);
                            indexIntoGenericArguments++;
                        }
                    }
                    else indexIntoGenericArguments += numberOfGenerics;
                    sb.Append('>');
                }
            }

            typeName = sb.ToString();
            return typeName;
        }

        public static string GetCSharpSourceSyntax(this Type t)
        {
            var list = new List<string>();
            string ret = TurnTypeIntoCSharpSource(t, ref list);
            return ret;
        }

        public static string GetCSharpSourceSyntax(this Type t, ref List<string> introducedGenericTypeParameters)
        {
            if (introducedGenericTypeParameters == null) introducedGenericTypeParameters = [];
            string ret = TurnTypeIntoCSharpSource(t, ref introducedGenericTypeParameters);
            return ret;
        }

        /// <summary>
        /// http://msdn.microsoft.com/en-us/library/aa664771(v=vs.71).aspx
        /// </summary>
        public static bool CanBeFixed(this Type t)
        {
            var b = !IsManagedType(t);
            return b;
        }

        public static bool IsCompoundGroupKey(this Type t, out Type outerType, out Type innerType)
        {
            if (t.IsGenericType && t.GenericTypeArguments.Length == 2 && t.GetGenericTypeDefinition() == typeof(CompoundGroupKey<,>))
            {
                outerType = t.GenericTypeArguments[0];
                innerType = t.GenericTypeArguments[1];
                return true;
            }
            else
            {
                outerType = default;
                innerType = default;
                return false;
            }
        }

        public static bool ImplementsIEqualityComparerExpression(this Type t)
            => t.ContainsGenericParameters
                ? t.GetInterfaces()
                    .Any(i => i.Namespace.Equals("Microsoft.StreamProcessing") && i.Name.Equals("IEqualityComparerExpression`1") && i.GetGenericArguments().Length == 1 && i.GetGenericArguments()[0] == t)
                : typeof(IEqualityComparerExpression<>).MakeGenericType(t).IsAssignableFrom(t);

        public static bool ImplementsIEqualityComparer(this Type t)
            => t.ContainsGenericParameters
                ? t.GetInterfaces()
                    .Any(i => i.Namespace.Equals("System.Collections.Generic") && i.Name.Equals("IEqualityComparer`1") && i.GetGenericArguments().Length == 1 && i.GetGenericArguments()[0] == t)
                : typeof(IEqualityComparer<>).MakeGenericType(t).IsAssignableFrom(t);

        public static bool ImplementsIComparer(this Type t)
            => t.ContainsGenericParameters
                ? t.GetInterfaces()
                    .Any(i => i.Namespace.Equals("System.Collections.Generic") && i.Name.Equals("IComparer`1") && i.GetGenericArguments().Length == 1 && i.GetGenericArguments()[0] == t)
                : typeof(IComparer<>).MakeGenericType(t).IsAssignableFrom(t);

        public static bool ImplementsIEquatable(this Type t)
            => t.ContainsGenericParameters
                ? t.GetInterfaces()
                    .Any(i => i.Namespace.Equals("System") && i.Name.Equals("IEquatable`1") && i.GetGenericArguments().Length == 1 && i.GetGenericArguments()[0] == t)
                : typeof(IEquatable<>).MakeGenericType(t).IsAssignableFrom(t);

        public static bool ImplementsIComparable(this Type t)
            => t.ContainsGenericParameters
                ? t.GetInterfaces()
                    .Any(i => i.Namespace.Equals("System") && i.Name.Equals("IComparable`1") && i.GetGenericArguments().Length == 1 && i.GetGenericArguments()[0] == t)
                : typeof(IComparable<>).MakeGenericType(t).IsAssignableFrom(t);

        #region Borrowed from Roslyn

        /// <summary>
        /// IsManagedType is simple for most named types:
        ///     enums are not managed;
        ///     non-enum, non-struct named types are managed;
        ///     generic types and their nested types are managed;
        ///     type parameters are managed;
        ///     all special types have spec'd values (basically, (non-string) primitives) are not managed;
        ///
        /// Only structs are complicated, because the definition is recursive.  A struct type is managed
        /// if one of its instance fields is managed.  Unfortunately, this can result in infinite recursion.
        /// If the closure is finite, and we don't find anything definitely managed, then we return true.
        /// If the closure is infinite, we disregard all but a representative of any expanding cycle.
        ///
        /// Intuitively, this will only return true if there's a specific type we can point to that is would
        /// be managed even if it had no fields.  e.g. struct S { S s; } is not managed, but struct S { S s; object o; }
        /// is because we can point to object.
        /// </summary>
        public static bool IsManagedType(Type type)
        {
            // If this is a type with an obvious answer, return quickly.
            var b = IsManagedTypeHelper(type);
            if (b.HasValue) return b.Value;

            // Otherwise, we have to build and inspect the closure of depended-upon types.
            var closure = new HashSet<Type>();
            bool result = DependsOnDefinitelyManagedType(type, closure);
            closure.Clear();
            return result;
        }

        private static bool DependsOnDefinitelyManagedType(Type type, HashSet<Type> partialClosure)
        {
            ArgumentNullException.ThrowIfNull(type);

            // NOTE: unlike in StructDependsClosure, we don't have to check for expanding cycles,
            // because as soon as we see something with non-zero arity we kick out (generic => managed).
            if (partialClosure.Add(type))
            {
                foreach (var field in type.GetFields(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance))
                {
                    // Only instance fields (including field-like events) affect the outcome.
                    if (field.IsStatic) continue;

                    var fieldType = field.FieldType;
                    switch (IsManagedTypeHelper(fieldType))
                    {
                        case true:
                            return true;
                        case false:
                            continue;
                        case null:
                            if (DependsOnDefinitelyManagedType(fieldType, partialClosure)) return true;
                            continue;
                    }
                }
            }

            return false;
        }

        /// <summary>
        /// Returns a boolean value if we can determine whether the type is managed
        /// without looking at its fields and Unset otherwise.
        /// </summary>
        private static bool? IsManagedTypeHelper(Type type)
        {
            // To match dev10, we treat enums as their underlying types.
            if (type.IsEnum) type = Enum.GetUnderlyingType(type);

            if (type.IsEnum) return false;
            if (type.IsPrimitive) return false;
            if (type.IsGenericType) return true;
            if (type.IsValueType) return null;

            return true;
        }
        #endregion

        /// <summary>
        ///     Checks if type t has a public parameter-less constructor.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>True if type t has a public parameter-less constructor, false otherwise.</returns>
        private static bool HasParameterlessConstructor(this Type type)
            => type.GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic).Any(c => c.GetParameters().Length == 0);

        /// <summary>
        ///     Determines whether the type is definitely unsupported for schema generation.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>
        ///     <c>true</c> if the type is unsupported; otherwise, <c>false</c>.
        /// </returns>
        public static bool IsUnsupported(this Type type)
            => type == typeof(IntPtr)
            || type == typeof(UIntPtr)
            || type == typeof(object)
            || type.ContainsGenericParameters
            || (!type.IsArray
                && !type.IsValueType
                && !type.HasSupportedParameterizedConstructor()
                && !type.HasParameterlessConstructor()
                && type != typeof(string)
                && type != typeof(Uri)
                && !type.IsAbstract
                && !type.IsInterface
                && !(type.IsGenericType && SupportedInterfaces.Contains(type.GetGenericTypeDefinition())));

        private static readonly HashSet<Type> SupportedInterfaces =
        [
            typeof(IList<>),
            typeof(IDictionary<,>)
        ];

        /// <summary>
        /// Validates that a type can be serialized.
        /// </summary>
        /// <param name="type">The type to validate.</param>
        /// <returns>
        /// Returns the input back again if it is valid.
        /// </returns>
        public static Type ValidateTypeForSerializer(this Type type)
        {
            ArgumentNullException.ThrowIfNull(type);
            Contract.EndContractBlock();

            if (type.IsUnsupported())
                throw new SerializationException($"Type '{type}' is not supported by the resolver.");

            return type;
        }

        /// <summary>
        /// Gets the serialization information about the type members.
        /// </summary>
        /// <param name="type">Type containing members which should be serialized/columnarized.</param>
        /// <returns>
        /// Serialization information about the fields/properties.
        /// </returns>
        public static IEnumerable<MyFieldInfo> ResolveMembers(this Type type)
        {
            ArgumentNullException.ThrowIfNull(type);
            Contract.EndContractBlock();

            if (type.IsPrimitive) return [];
            else if (type.IsDefined(typeof(DataContractAttribute)))
            {
                // In DataContract context, return all fields and properties marked with DataMember.
                // This takes priority over HasSupportedParameterizedConstructor so that types like
                // StreamEvent<T> (readonly struct with [DataContract]) use their [DataMember] fields
                // as constructor arguments rather than their unrelated public getter properties.
                var fields = type.GetAllFields().Where(m => m.IsDefined(typeof(DataMemberAttribute))).Select(o => new MyFieldInfo(o));
                var properties = type.GetAllProperties().Where(m => m.IsDefined(typeof(DataMemberAttribute))).Select(o => new MyFieldInfo(o));
                return fields.Concat(properties);
            }
            else if (type.HasSupportedParameterizedConstructor())
            {
                return type.GetProperties().Where(p => p.GetIndexParameters().Length == 0).Select(o => new MyFieldInfo(o));
            }
            else
            {
                // Otherwise, return all fields, as well as all autoproperties
                var fields = type.GetAllFields().Select(o => new MyFieldInfo(o));
                var properties = type.GetAllProperties().Where(m => m.IsFieldOrAutoProp()).Select(o => new MyFieldInfo(o));
                return fields.Concat(properties);
            }
        }
    }
}
