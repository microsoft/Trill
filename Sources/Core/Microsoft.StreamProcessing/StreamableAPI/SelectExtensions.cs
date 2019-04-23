// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Static class for transformations to Streamables
    /// </summary>
    public static partial class Streamable
    {
        /// <summary>
        /// Performs a project over a streamable.
        /// </summary>
        /// <param name="source">Source streamable for the operation.</param>
        /// <param name="selector">Expression over Payload that returns the new Payload.</param>
        public static IStreamable<TKey, TResult> Select<TKey, TPayload, TResult>(
            this IStreamable<TKey, TPayload> source,
            Expression<Func<TPayload, TResult>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));

            return source is IFusibleStreamable<TKey, TPayload> s && s.CanFuseSelect(selector, false, false)
                ? s.FuseSelect(selector)
                : (IStreamable<TKey, TResult>)new SelectStreamable<TKey, TPayload, TResult>(source, selector);
        }

        /// <summary>
        /// Performs a project over a streamable.
        /// </summary>
        /// <param name="source">Source streamable for the operation.</param>
        /// <param name="selector">Expression over StartTime and Payload that returns the new Payload.</param>
        public static IStreamable<TKey, TResult> Select<TKey, TPayload, TResult>(
            this IStreamable<TKey, TPayload> source,
            Expression<Func<long, TPayload, TResult>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));

            return source is IFusibleStreamable<TKey, TPayload> s && s.CanFuseSelect(selector, true, false)
                ? s.FuseSelect(selector)
                : (IStreamable<TKey, TResult>)new SelectStreamable<TKey, TPayload, TResult>(source, selector, hasStartEdge: true);
        }

        /// <summary>
        /// Performs a project over a streamable, relative to the grouping key.
        /// </summary>
        /// <param name="source">Source streamable for the operation.</param>
        /// <param name="selector">Expression over Key and Payload, that returns the new Payload.</param>
        public static IStreamable<TKey, TResult> SelectByKey<TKey, TPayload, TResult>(
            this IStreamable<TKey, TPayload> source,
            Expression<Func<TKey, TPayload, TResult>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));

            return source is IFusibleStreamable<TKey, TPayload> s && s.CanFuseSelect(selector, false, true)
                ? s.FuseSelectWithKey(selector)
                : (IStreamable<TKey, TResult>)new SelectStreamable<TKey, TPayload, TResult>(source, selector, hasKey: true);
        }

        /// <summary>
        /// Performs a project over a streamable, relative to the grouping key.
        /// </summary>
        /// <param name="source">Source streamable for the operation.</param>
        /// <param name="selector">Expression over StartTime, Key, and Payload, that returns the new Payload.</param>
        public static IStreamable<TKey, TResult> SelectByKey<TKey, TPayload, TResult>(
            this IStreamable<TKey, TPayload> source,
            Expression<Func<long, TKey, TPayload, TResult>> selector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(selector, nameof(selector));

            return source is IFusibleStreamable<TKey, TPayload> s && s.CanFuseSelect(selector, true, true)
                ? s.FuseSelectWithKey(selector)
                : (IStreamable<TKey, TResult>)new SelectStreamable<TKey, TPayload, TResult>(source, selector, hasStartEdge: true, hasKey: true);
        }

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TOld"></typeparam>
        /// <typeparam name="TNew"></typeparam>
        /// <param name="source"></param>
        /// <param name="initializer"></param>
        /// <param name="newColumnFormulas"></param>
        /// <returns></returns>
        public static IStreamable<TKey, TNew> Select<TKey, TOld, TNew>(
            this IStreamable<TKey, TOld> source,
            Expression<Func<TNew>> initializer,
            IDictionary<string, Expression<Func<TOld, object>>> newColumnFormulas = null) where TNew : new()
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(initializer, nameof(initializer));

            // Validate that the dictionary parameter references proper fields
            if (newColumnFormulas == null) newColumnFormulas = new Dictionary<string, Expression<Func<TOld, object>>>();
            Type newType = typeof(TNew);
            foreach (var pair in newColumnFormulas)
            {
                if (newType.GetTypeInfo().GetMember(pair.Key).Length == 0) throw new ArgumentException("Dictionary keys must refer to valid members of the destination type", nameof(newColumnFormulas));
            }

            if (!(initializer.Body is NewExpression newExpression)) throw new ArgumentException("Initializer must return a constructor expression.", nameof(initializer));

            return source.Select(CreateAdjustColumnsExpression<TOld, TNew>(
                newColumnFormulas.ToDictionary(o => o.Key, o => (LambdaExpression)o.Value),
                newExpression));
        }

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TOld"></typeparam>
        /// <typeparam name="TNew"></typeparam>
        /// <typeparam name="TField1"></typeparam>
        /// <param name="source"></param>
        /// <param name="initializer"></param>
        /// <param name="fieldSelector1"></param>
        /// <param name="fieldInitializer1"></param>
        /// <returns></returns>
        public static IStreamable<TKey, TNew> Select<TKey, TOld, TNew, TField1>(
            this IStreamable<TKey, TOld> source,
            Expression<Func<TNew>> initializer,
            Expression<Func<TNew, TField1>> fieldSelector1,
            Expression<Func<TOld, TField1>> fieldInitializer1) where TNew : new()
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(initializer, nameof(initializer));

            // Validate that the field selector formulas are actually selector expressions
            if (fieldSelector1.Body.NodeType != ExpressionType.MemberAccess)
                throw new ArgumentException("Unable to determine destination field name from selector expression - expected a member selection expression.", nameof(fieldSelector1));

            if (!(initializer.Body is NewExpression newExpression)) throw new ArgumentException("Initializer must return a constructor expression.", nameof(initializer));

            return source.Select(CreateAdjustColumnsExpression<TOld, TNew>(
                new Dictionary<string, LambdaExpression>
                {
                    { ((MemberExpression)(fieldSelector1.Body)).Member.Name, fieldInitializer1 },
                },
                newExpression));
        }

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TOld"></typeparam>
        /// <typeparam name="TNew"></typeparam>
        /// <typeparam name="TField1"></typeparam>
        /// <typeparam name="TField2"></typeparam>
        /// <param name="source"></param>
        /// <param name="initializer"></param>
        /// <param name="fieldSelector1"></param>
        /// <param name="fieldInitializer1"></param>
        /// <param name="fieldSelector2"></param>
        /// <param name="fieldInitializer2"></param>
        /// <returns></returns>
        public static IStreamable<TKey, TNew> Select<TKey, TOld, TNew, TField1, TField2>(
            this IStreamable<TKey, TOld> source,
            Expression<Func<TNew>> initializer,
            Expression<Func<TNew, TField1>> fieldSelector1,
            Expression<Func<TOld, TField1>> fieldInitializer1,
            Expression<Func<TNew, TField2>> fieldSelector2,
            Expression<Func<TOld, TField2>> fieldInitializer2) where TNew : new()
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(initializer, nameof(initializer));

            // Validate that the field selector formulas are actually selector expressions
            if (fieldSelector1.Body.NodeType != ExpressionType.MemberAccess)
                throw new ArgumentException("Unable to determine destination field name from selector expression - expected a member selection expression.", nameof(fieldSelector1));
            if (fieldSelector2.Body.NodeType != ExpressionType.MemberAccess)
                throw new ArgumentException("Unable to determine destination field name from selector expression - expected a member selection expression.", nameof(fieldSelector2));

            if (!(initializer.Body is NewExpression newExpression)) throw new ArgumentException("Initializer must return a constructor expression.", nameof(initializer));

            return source.Select(CreateAdjustColumnsExpression<TOld, TNew>(
                new Dictionary<string, LambdaExpression>
                {
                    { ((MemberExpression)(fieldSelector1.Body)).Member.Name, fieldInitializer1 },
                    { ((MemberExpression)(fieldSelector2.Body)).Member.Name, fieldInitializer2 },
                },
                newExpression));
        }

        private static Expression<Func<TOld, TNew>> CreateAdjustColumnsExpression<TOld, TNew>(
            IDictionary<string, LambdaExpression> newColumnFormulas,
            NewExpression newExpression) where TNew : new()
        {
            Type oldType = typeof(TOld);
            Type newType = typeof(TNew);

            var inputParameter = Expression.Parameter(oldType, "input");

            var oldPublicFields =
                oldType.GetTypeInfo().GetFields(BindingFlags.Public | BindingFlags.Instance).Select(o => o.Name)
                .Union(
                oldType.GetTypeInfo().GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.GetIndexParameters().Length == 0).Select(o => o.Name));

            var newPublicFields =
                newType.GetTypeInfo().GetFields(BindingFlags.Public | BindingFlags.Instance).Select(o => o.Name)
                .Union(
                newType.GetTypeInfo().GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.GetIndexParameters().Length == 0).Select(o => o.Name));

            var fieldsInCommon = oldPublicFields.Intersect(newPublicFields).Except(newColumnFormulas.Keys);

            var commonFieldAssignments = fieldsInCommon.Select(
                o => Expression.Bind(newType.GetTypeInfo().GetMember(o).Single(), Expression.PropertyOrField(inputParameter, o)));

            var newFieldAssignments = newColumnFormulas.Select(
                o => Expression.Bind(newType.GetTypeInfo().GetMember(o.Key).Single(), o.Value.RemoveCastToObject().ReplaceParametersInBody(inputParameter)));

            var member = Expression.MemberInit(newExpression, commonFieldAssignments.Concat(newFieldAssignments).ToArray());
            var lambda = Expression.Lambda<Func<TOld, TNew>>(member, new ParameterExpression[] { inputParameter });
            return lambda;
        }
    }
}