// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing.Internal
{
    internal static class FastDictionaryGenerator
    {
        private const string Prefix = "GeneratedFastDictionary_";
        private static readonly object sentinel = new object();
        private static int classCounter = 0;
        private static readonly Dictionary<Tuple<string, Type, Type>, Type> generatorCache = new Dictionary<Tuple<string, Type, Type>, Type>();

        public static Func<FastDictionary<TKey, TValue>> CreateFastDictionaryGenerator<TKey, TValue>(
            this IEqualityComparerExpression<TKey> comparerExp, int capacity, Func<TKey, TKey, bool> equalsFunc, Func<TKey, int> getHashCodeFunc, QueryContainer container)
        {
            if (EqualityComparerExpression<TKey>.IsSimpleDefault(comparerExp)) return () => new FastDictionary<TKey, TValue>();
            if (container == null) return () => new FastDictionary<TKey, TValue>(capacity, equalsFunc, getHashCodeFunc);

            var equalsExp = comparerExp.GetEqualsExpr();
            var getHashCodeExp = comparerExp.GetGetHashCodeExpr();
            var vars = VariableFinder.Find(equalsExp).Select(o => o.GetHashCode()).ToList();
            if (!vars.Any()) vars.Add(string.Empty.StableHash());
            var hashvars = VariableFinder.Find(getHashCodeExp).Select(o => o.GetHashCode()).ToList();
            if (!hashvars.Any()) hashvars.Add(string.Empty.StableHash());
            var key =
                Tuple.Create(
                    equalsExp.ToString() + getHashCodeExp.ToString() + string.Concat(vars.Aggregate((a, i) => a ^ i)) + string.Concat(hashvars.Aggregate((a, i) => a ^ i)),
                    typeof(TKey), typeof(TValue));

            Type temp;
            lock (sentinel)
            {
                if (!generatorCache.TryGetValue(key, out temp))
                {
                    string typeName = Prefix + classCounter++;
                    var builderCode = new GeneratedFastDictionary(typeName, string.Empty).TransformText();
                    var a = Transformer.CompileSourceCode(builderCode, Array.Empty<Assembly>(), out string errorMessages);

                    temp = a.GetType(typeName + "`2");
                    temp = temp.MakeGenericType(typeof(TKey), typeof(TValue));
                    MethodInfo init = temp.GetTypeInfo().GetMethod("Initialize", BindingFlags.Static | BindingFlags.Public);
                    init.Invoke(null, new object[] { equalsFunc, getHashCodeFunc, capacity });
                    generatorCache.Add(key, temp);
                }
                if (!container.TryGetFastDictionaryType(key, out Type other))
                    container.RegisterFastDictionaryType(key, temp);
           }

            return () => (FastDictionary<TKey, TValue>)Activator.CreateInstance(temp);
        }
    }
    internal static class FastDictionaryGenerator2
    {
        private const string Prefix = "GeneratedFastDictionary2_";
        private static readonly object sentinel = new object();
        private static int classCounter = 0;
        private static readonly Dictionary<Tuple<string, Type, Type>, Type> generatorCache = new Dictionary<Tuple<string, Type, Type>, Type>();

        public static Func<FastDictionary2<TKey, TValue>> CreateFastDictionary2Generator<TKey, TValue>(
            this IEqualityComparerExpression<TKey> comparerExp, int capacity, Func<TKey, TKey, bool> equalsFunc, Func<TKey, int> getHashCodeFunc, QueryContainer container)
        {
            if (EqualityComparerExpression<TKey>.IsSimpleDefault(comparerExp)) return () => new FastDictionary2<TKey, TValue>();
            if (container == null) return () => new FastDictionary2<TKey, TValue>(capacity, equalsFunc, getHashCodeFunc);

            var equalsExp = comparerExp.GetEqualsExpr();
            var getHashCodeExp = comparerExp.GetGetHashCodeExpr();
            var vars = VariableFinder.Find(equalsExp).Select(o => o.GetHashCode()).ToList();
            if (!vars.Any()) vars.Add(string.Empty.StableHash());
            var hashvars = VariableFinder.Find(getHashCodeExp).Select(o => o.GetHashCode()).ToList();
            if (!hashvars.Any()) hashvars.Add(string.Empty.StableHash());
            var key =
                Tuple.Create(
                    equalsExp.ToString() + getHashCodeExp.ToString() + string.Concat(vars.Aggregate((a, i) => a ^ i)) + string.Concat(hashvars.Aggregate((a, i) => a ^ i)),
                    typeof(TKey), typeof(TValue));

            Type temp;
            lock (sentinel)
            {
                if (!generatorCache.TryGetValue(key, out temp))
                {
                    string typeName = Prefix + classCounter++;
                    var builderCode = new GeneratedFastDictionary(typeName, "2").TransformText();
                    var a = Transformer.CompileSourceCode(builderCode, Array.Empty<Assembly>(), out string errorMessages);

                    temp = a.GetType(typeName + "`2");
                    temp = temp.MakeGenericType(typeof(TKey), typeof(TValue));
                    MethodInfo init = temp.GetTypeInfo().GetMethod("Initialize", BindingFlags.Static | BindingFlags.Public);
                    init.Invoke(null, new object[] { equalsFunc, getHashCodeFunc, capacity });
                    generatorCache.Add(key, temp);
                }
                if (!container.TryGetFastDictionary2Type(key, out Type other))
                    container.RegisterFastDictionary2Type(key, temp);
           }

            return () => (FastDictionary2<TKey, TValue>)Activator.CreateInstance(temp);
        }
    }
    internal static class FastDictionaryGenerator3
    {
        private const string Prefix = "GeneratedFastDictionary3_";
        private static readonly object sentinel = new object();
        private static int classCounter = 0;
        private static readonly Dictionary<Tuple<string, Type, Type>, Type> generatorCache = new Dictionary<Tuple<string, Type, Type>, Type>();

        public static Func<FastDictionary3<TKey, TValue>> CreateFastDictionary3Generator<TKey, TValue>(
            this IEqualityComparerExpression<TKey> comparerExp, int capacity, Func<TKey, TKey, bool> equalsFunc, Func<TKey, int> getHashCodeFunc, QueryContainer container)
        {
            if (EqualityComparerExpression<TKey>.IsSimpleDefault(comparerExp)) return () => new FastDictionary3<TKey, TValue>();
            if (container == null) return () => new FastDictionary3<TKey, TValue>(capacity, equalsFunc, getHashCodeFunc);

            var equalsExp = comparerExp.GetEqualsExpr();
            var getHashCodeExp = comparerExp.GetGetHashCodeExpr();
            var vars = VariableFinder.Find(equalsExp).Select(o => o.GetHashCode()).ToList();
            if (!vars.Any()) vars.Add(string.Empty.StableHash());
            var hashvars = VariableFinder.Find(getHashCodeExp).Select(o => o.GetHashCode()).ToList();
            if (!hashvars.Any()) hashvars.Add(string.Empty.StableHash());
            var key =
                Tuple.Create(
                    equalsExp.ToString() + getHashCodeExp.ToString() + string.Concat(vars.Aggregate((a, i) => a ^ i)) + string.Concat(hashvars.Aggregate((a, i) => a ^ i)),
                    typeof(TKey), typeof(TValue));

            Type temp;
            lock (sentinel)
            {
                if (!generatorCache.TryGetValue(key, out temp))
                {
                    string typeName = Prefix + classCounter++;
                    var builderCode = new GeneratedFastDictionary(typeName, "3").TransformText();
                    var a = Transformer.CompileSourceCode(builderCode, Array.Empty<Assembly>(), out string errorMessages);

                    temp = a.GetType(typeName + "`2");
                    temp = temp.MakeGenericType(typeof(TKey), typeof(TValue));
                    MethodInfo init = temp.GetTypeInfo().GetMethod("Initialize", BindingFlags.Static | BindingFlags.Public);
                    init.Invoke(null, new object[] { equalsFunc, getHashCodeFunc, capacity });
                    generatorCache.Add(key, temp);
                }
                if (!container.TryGetFastDictionary3Type(key, out Type other))
                    container.RegisterFastDictionary3Type(key, temp);
           }

            return () => (FastDictionary3<TKey, TValue>)Activator.CreateInstance(temp);
        }
    }
    internal partial class GeneratedFastDictionary
    {
        private readonly string classname;
        private readonly string dictType;
        public GeneratedFastDictionary(string name, string dictType)
        {
            this.classname = name;
            this.dictType = dictType;
        }
    }
}
