// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace Microsoft.StreamProcessing
{
    internal static class Extensions
    {
        public static string CleanUpIdentifierName(this string s)
        {
            Contract.Requires(s != null);

            return s.Replace('`', '_').Replace('.', '_').Replace('<', '_').Replace('>', '_').Replace(',', '_').Replace(' ', '_').Replace('`', '_').Replace('[', '_').Replace(']', '_').Replace('=', '_').Replace('+', '_');
        }

        public static string AddNumberOfNecessaryGenericArguments(this string s, params Type[] types)
        {
            Contract.Requires(types != null);
            var i = types.Count(t => t.IsAnonymousType());
            return i > 0
                ? s + "`" + i.ToString(CultureInfo.InvariantCulture)
                : s;
        }

        public static bool OptimizeString(this MyFieldInfo field) => Config.UseMultiString && field.Type == typeof(string);

        /// <summary>
        /// Returns true iff the equality and hashcode functions defined in
        /// <paramref name="payloadEqualityComparer"/> can be used in columnar codegen.
        /// </summary>
        /// <typeparam name="T">The type of the payload</typeparam>
        /// <param name="payloadEqualityComparer">The equality and hashcode functions
        /// represented as lambda expressions.</param>
        /// <returns>True iff both the equality and hashcode functions can be used in columnar
        /// codegen operators.</returns>
        public static bool CanUsePayloadEquality<T>(this IEqualityComparerExpression<T> payloadEqualityComparer)
        {
            var typeofT = typeof(T);

            // If T is a struct, then even if the user defined equality function calls a method on the struct,
            // it can still be used because the active events will have a field of type T.
            if (typeofT.GetTypeInfo().IsValueType) return true;

            // If T is a type for which the columnar representation is just a pseudo-field, e.g., string,
            // then the payload is not really represented as a set of its fields, but is an instance of T
            if (new ColumnarRepresentation(typeofT).noFields) return true;

            // If T is a reference type, then codegen is going to define a structurally-equivalent type T'
            // and the active events will have a field of type T'. So if the equality or hash functions ever
            // use a value of type T (other than to dereference it for a field/property) then it cannot be used.
            var equalsIsBad = ParameterInstanceFinder.FoundInstance(payloadEqualityComparer.GetEqualsExpr());
            var hashIsBad = ParameterInstanceFinder.FoundInstance(payloadEqualityComparer.GetGetHashCodeExpr());
            return !equalsIsBad && !hashIsBad;
        }

        public static bool IsFieldOrAutoProp(this MemberInfo member)
            => member is FieldInfo
            || (member is PropertyInfo p
                && p.GetMethod != null
                && p.GetMethod.IsDefined(typeof(CompilerGeneratedAttribute))
                && p.SetMethod != null
                && p.SetMethod.IsDefined(typeof(CompilerGeneratedAttribute)));

        /// <summary>
        /// Transforms a unary function f, with parameter e of type <typeparamref name="TPayload"/>, into
        /// a columnar representation where all occurrences of "e.f" have been replaced with "f_col[i]"
        /// "f_col" is a new new variable that the caller must define to point to the "col" array within
        /// the ColumnBatch&lt;T&gt; (where T is the type of the field "f") that represents the field f from
        /// the type <typeparamref name="TPayload"/>.
        /// The name "i" is used as the index variable for the column batch, so the caller must also have defined
        /// that variable.
        /// </summary>
        /// <typeparam name="TKey">
        /// The key type of the trill message whose payloads are used for arguments to <paramref name="f"/>
        /// </typeparam>
        /// <typeparam name="TPayload">
        /// The type of the parameter of <paramref name="f"/>
        /// </typeparam>
        /// <param name="f"></param>
        /// <returns>Null if the columnar transformation of f's body fails.</returns>
        public static LambdaExpression/*?*/ TransformUnaryFunction<TKey, TPayload>(LambdaExpression f)
            => TransformFunction<TKey, TPayload>(f, 0);

        /// <summary>
        /// Transforms an n-ary function f, with parameters e_i, into a columnar representation.
        /// One parameter, at position <paramref name="j"/>, is e_j and its type is <typeparamref name="TPayload"/>.
        /// In the resulting lambda, all occurrences of "e_j.f" have been replaced with "x_f_col[i]"
        /// "x_f_col" is a new (array) variable that the caller must define to point to the "col" array within
        /// the ColumnBatch&lt;T&gt; (where T is the type of the field "f") that represents the field f from
        /// the type <typeparamref name="TPayload"/>.
        /// The name "i" is used as the index variable for the column batch, so the caller must also have defined
        /// that variable.
        /// The resulting lambda has all of the same parameter as <paramref name="f"/> *except* for e_j.
        /// </summary>
        /// <typeparam name="TKey">
        /// The key type of the trill message whose payloads are used for arguments to <paramref name="f"/>
        /// </typeparam>
        /// <typeparam name="TPayload">
        /// The type of the parameter of <paramref name="f"/>
        /// </typeparam>
        /// <param name="f"></param>
        /// <param name="j">The index of the parameter e that is to be transformed.</param>
        /// <param name="batchVariableName">
        /// The name to use for "x" in the new variable name. The default is the empty string, in which case
        /// the name does *not* have a leading underscore, but is just "f_col".
        /// </param>
        /// <param name="indexVariableName">
        /// The name to use for the index variable used to access the array. The default is "i".
        /// </param>
        /// <returns>Null if the columnar transformation of f's body fails.</returns>
        public static LambdaExpression/*?*/ TransformFunction<TKey, TPayload>(LambdaExpression f, int j, string batchVariableName = "", string indexVariableName = "i")
        {
            Contract.Requires(j >= 0 && j < f.Parameters.Count);

            var l_prime = MakeOneParameterColumnar<TKey, TPayload>(f, j, indexVariableName);
            var batchNameDictionary = new Dictionary<ParameterExpression, string>
            {
                { l_prime.Parameters[j], batchVariableName }
            };
            var newLambda = IntroduceArrayVariables.Transform(l_prime, batchNameDictionary);
            var n = f.Parameters.Count;
            var remainingParameters = new List<ParameterExpression>();
            for (int i = 0; i < n; i++)
            {
                if (i != j) remainingParameters.Add(f.Parameters[i]);
            }
            return Expression.Lambda(newLambda.Body, remainingParameters);
        }

        /// <summary>
        /// Transforms an n-ary function f, with parameters e_i, into a columnar representation.
        /// The first parameter, at position 0, is e_0 and its type is <typeparamref name="TPayload"/>.
        /// In the resulting lambda, all occurrences of "e_0.f" have been replaced with "b_0.f.col[i]"
        /// "b_0" is a new parameter of type G where G is the generated batch type for a
        /// StreamMessage&lt;<typeparamref name="TKey"/>, <typeparamref name="TPayload"/>&gt;.
        /// "f" is the column batch representing the field f in type TPayload.
        /// The name "i" is used as the index variable for the column batch, so the caller must also have defined
        /// that variable.
        /// The resulting lambda has all of the same parameter as <paramref name="f"/> *except* for e_0.
        /// That parameter has been replaced by b_0.
        /// </summary>
        /// <typeparam name="TKey">
        /// The key type of the trill message whose payloads are used for arguments to <paramref name="f"/>
        /// </typeparam>
        /// <typeparam name="TPayload">
        /// The type of the parameter of <paramref name="f"/>
        /// </typeparam>
        /// <param name="f"></param>
        /// <param name="indexVariableName">
        /// The name to use for the index variable used to access the array.
        /// </param>
        /// <param name="parameterIndex"></param>
        /// <returns>Null if the columnar transformation of f's body fails.</returns>
        public static LambdaExpression/*?*/ TransformFunction<TKey, TPayload>(LambdaExpression f, string indexVariableName, int parameterIndex = 0)
        {
            Contract.Requires(parameterIndex < f.Parameters.Count);
            return MakeOneParameterColumnar<TKey, TPayload>(f, parameterIndex, indexVariableName);
        }

        private static LambdaExpression/*?*/ MakeOneParameterColumnar<TKey, TPayload>(LambdaExpression f, int index, string indexVariableName)
        {
            Contract.Requires(index >= 0 && index < f.Parameters.Count);

            var batchType = StreamMessageManager.GetStreamMessageType<TKey, TPayload>();
            var d = new Dictionary<ParameterExpression, ColumnOriented.SubstitutionInformation>();
            var payloadRepresentation = new ColumnarRepresentation(typeof(TPayload));
            d.Add(
                f.Parameters[index],
                new ColumnOriented.SubstitutionInformation
                {
                    columnarRepresentation = payloadRepresentation,
                    nameForIndexVariable = indexVariableName,
                    typeOfBatchVariable = batchType,
                });
            return ColumnOriented.Transform(f, d);
        }

        public static string AccessExpressionForRowValue(this MyFieldInfo f, string batchVariableName, string indexVariableName)
            => string.Format(CultureInfo.InvariantCulture, "{0}.{1}{2}[{3}]", batchVariableName, f.Name, f.OptimizeString() ? string.Empty : ".col", indexVariableName);

        public static bool IsSimpleFieldOrPropertyAccess(this LambdaExpression function)
            => function.Parameters.Count == 1
            && function.Body is MemberExpression me
            && me.Expression is ParameterExpression param
            && param.Equals(function.Parameters[0])
            && (me.Member is FieldInfo || me.Member is PropertyInfo);

        public static string BracketedCommaSeparatedString(this IEnumerable<string> strings)
        {
            if (!strings.Any()) return string.Empty;
            var commaSeparatedList = string.Join(",", strings);
            return "<" + commaSeparatedList + ">";
        }
    }
}
