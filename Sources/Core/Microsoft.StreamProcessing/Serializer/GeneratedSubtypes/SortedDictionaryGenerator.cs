// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Microsoft.StreamProcessing.Internal
{
    internal static class SortedDictionaryGenerator
    {
        private const string Prefix = "GeneratedSortedDictionary";
        private static readonly object sentinel = new object();
        private static readonly Dictionary<Tuple<string, Type, Type>, Type> DictionaryTypes = new Dictionary<Tuple<string, Type, Type>, Type>();

        public static Expression<Func<SortedDictionary<TKey, TValue>>> CreateSortedDictionaryGenerator<TKey, TValue>(this IComparerExpression<TKey> comparerExp, QueryContainer container)
        {
            if (ComparerExpression<TKey>.IsSimpleDefault(comparerExp)) return () => new SortedDictionary<TKey, TValue>();

            var expr = comparerExp.GetCompareExpr();
            if (container == null)
            {
                Expression<Func<Comparison<TKey>, SortedDictionary<TKey, TValue>>> template
                    = (c) => new SortedDictionary<TKey, TValue>(Comparer<TKey>.Create(c));
                var replaced = template.ReplaceParametersInBody(expr);
                return Expression.Lambda<Func<SortedDictionary<TKey, TValue>>>(replaced);
            }

            var expression = expr.ToString();
            var vars = VariableFinder.Find(expr).Select(o => o.GetHashCode());
            var captures = vars.Aggregate(expression.StableHash(), (a, i) => a ^ i);
            var key = Tuple.Create(expression + string.Concat(vars.Select(o => o.ToString(CultureInfo.InvariantCulture))), typeof(TKey), typeof(TValue));

            Type temp;
            lock (sentinel)
            {
                if (!DictionaryTypes.TryGetValue(key, out temp))
                {
                    string typeName = Prefix
                        + captures.ToString(CultureInfo.InvariantCulture)
                        + typeof(TKey).ToString().StableHash().ToString(CultureInfo.InvariantCulture)
                        + typeof(TValue).ToString().StableHash().ToString(CultureInfo.InvariantCulture);
                    typeName = typeName.Replace("-", "_");
                    var builderCode = new GeneratedSortedDictionary(typeName).TransformText();
                    var assemblyReferences = Transformer.AssemblyReferencesNeededFor(typeof(SortedDictionary<,>));
                    var a = Transformer.CompileSourceCode(builderCode, assemblyReferences, out string errorMessages);

                    temp = a.GetType(typeName + "`2");
                    temp = temp.MakeGenericType(typeof(TKey), typeof(TValue));
                    var init = temp.GetTypeInfo().GetMethod("Initialize", BindingFlags.Static | BindingFlags.Public);
                    init.Invoke(null, new object[] { Comparer<TKey>.Create(expr.Compile()) });
                    DictionaryTypes.Add(key, temp);
                }
                if (!container.TryGetSortedDictionaryType(key, out var other))
                    container.RegisterSortedDictionaryType(key, temp);
            }
            return Expression.Lambda<Func<SortedDictionary<TKey, TValue>>>(Expression.New(temp));
        }
    }

    internal partial class GeneratedSortedDictionary
    {
        private readonly string name;

        public GeneratedSortedDictionary(string name) => this.name = name;
    }
}
