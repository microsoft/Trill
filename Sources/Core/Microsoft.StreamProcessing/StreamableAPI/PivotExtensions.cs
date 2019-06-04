// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.StreamProcessing.Aggregates;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Static class for transformations to Streamables
    /// </summary>
    public static partial class Streamable
    {
        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TInput"></typeparam>
        /// <typeparam name="TOutput"></typeparam>
        /// <typeparam name="TGroupKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <typeparam name="TAggValue"></typeparam>
        /// <typeparam name="TState"></typeparam>
        /// <param name="inputStreamable"></param>
        /// <param name="initializer"></param>
        /// <param name="keySelector"></param>
        /// <param name="attributeSelector"></param>
        /// <param name="valueSelector"></param>
        /// <param name="aggregate"></param>
        /// <returns></returns>
        public static IStreamable<TKey, TOutput> Pivot<TKey, TInput, TOutput, TGroupKey, TValue, TAggValue, TState>(
            this IStreamable<TKey, TInput> inputStreamable,
            Expression<Func<TOutput>> initializer,
            Expression<Func<TInput, TGroupKey>> keySelector,
            Expression<Func<TInput, string>> attributeSelector,
            Expression<Func<TInput, TValue>> valueSelector,
            Func<Window<CompoundGroupKey<TKey, TGroupKey>, TValue>, IAggregate<TValue, TState, TAggValue>> aggregate) where TOutput : new()
        {
            bool sourceHasNullableValues = IsNullable(typeof(TValue));
            if (initializer == null) throw new ArgumentNullException(nameof(initializer));

            var window = new Window<CompoundGroupKey<TKey, TGroupKey>, TValue>(inputStreamable.Properties.GroupNested(keySelector).Select<TValue>(valueSelector, false, false));
            var agg = aggregate(window);
            var valueAgg = agg.TransformInput(valueSelector);
            var outputPublicFields =
                typeof(TOutput).GetTypeInfo()
                .GetFields(BindingFlags.Public | BindingFlags.Instance)
                .Select(f => Tuple.Create(f.Name, f.FieldType, (MemberInfo)f, !sourceHasNullableValues && IsNullable(f.FieldType)))
                .Concat(
                typeof(TOutput).GetTypeInfo().GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.GetIndexParameters().Length == 0)
                .Select(f => Tuple.Create(f.Name, f.PropertyType, (MemberInfo)f, !sourceHasNullableValues && IsNullable(f.PropertyType))))
                .Where(m => m.Item2.GetTypeInfo().IsAssignableFrom(typeof(TAggValue))).ToArray();
            var aggArray = outputPublicFields.Select(
                m => valueAgg.ApplyFilter(
                    Expression.Lambda<Func<TInput, bool>>(
                        Expression.Equal(attributeSelector.Body, Expression.Constant(m.Item1)),
                        attributeSelector.Parameters)));

            var initialState = Expression.Lambda<Func<TState[]>>(Expression.NewArrayInit(typeof(TState), aggArray.Select(a => a.InitialState().Body)));

            var state = Expression.Parameter(typeof(TState[]), "state");
            var time = Expression.Parameter(typeof(long), "time");
            var input = Expression.Parameter(typeof(TInput), "input");
            var accumulator = Expression.Lambda<Func<TState[], long, TInput, TState[]>>(
                Expression.NewArrayInit(
                    typeof(TState),
                    aggArray.Select((a, i) => a.Accumulate().ReplaceParametersInBody(Expression.ArrayIndex(state, Expression.Constant(i)), time, input))), state, time, input);
            var deaccumulator = Expression.Lambda<Func<TState[], long, TInput, TState[]>>(
                Expression.NewArrayInit(
                    typeof(TState),
                    aggArray.Select((a, i) => a.Deaccumulate().ReplaceParametersInBody(Expression.ArrayIndex(state, Expression.Constant(i)), time, input))), state, time, input);

            var state1 = Expression.Parameter(typeof(TState[]), "state1");
            var state2 = Expression.Parameter(typeof(TState[]), "state2");
            var difference = Expression.Lambda<Func<TState[], TState[], TState[]>>(
                Expression.NewArrayInit(
                    typeof(TState),
                    aggArray.Select((a, i) => a.Difference().ReplaceParametersInBody(Expression.ArrayIndex(state1, Expression.Constant(i)), Expression.ArrayIndex(state1, Expression.Constant(i))))), state1, state2);

            var resultSelector = Expression.Lambda<Func<TState[], TAggValue[]>>(
                Expression.NewArrayInit(
                    typeof(TAggValue),
                    aggArray.Select((a, i) => a.ComputeResult().ReplaceParametersInBody(Expression.ArrayIndex(state1, Expression.Constant(i))))), state1);
            var newAgg = GeneratedAggregate.Create(initialState, accumulator, deaccumulator, difference, resultSelector);

            if (initializer.Body as NewExpression == null) throw new ArgumentException("Initializer must return a constructor expression.", nameof(initializer));

            var groupkey = Expression.Parameter(typeof(GroupSelectorInput<TGroupKey>), "groupkey");
            var aggvalues = Expression.Parameter(typeof(TAggValue[]), "aggvalue");
            var keyAssignments = new List<MemberAssignment>();

            // Key selector magic!
            switch (keySelector.Body.NodeType)
            {
                case ExpressionType.MemberAccess:
                {
                    var selector = keySelector.Body as MemberExpression;
                    if (selector.Expression.NodeType == ExpressionType.Parameter)
                    {
                        keyAssignments.Add(Expression.Bind(
                            typeof(TOutput).GetTypeInfo().GetMember(selector.Member.Name).Single(),
                            Expression.PropertyOrField(groupkey, "Key")));
                    }
                    else throw new NotImplementedException();
                    break;
                }
                case ExpressionType.MemberInit:
                {
                    var memberInit = keySelector.Body as MemberInitExpression;
                    foreach (var init in memberInit.Bindings)
                    {
                        if (init.BindingType != MemberBindingType.Assignment) throw new NotImplementedException();
                        var assign = init as MemberAssignment;
                            if (assign.Expression is MemberExpression rightSide && rightSide.Expression.NodeType == ExpressionType.Parameter)
                            {
                                keyAssignments.Add(Expression.Bind(
                                    typeof(TOutput).GetTypeInfo().GetMember(assign.Member.Name).Single(),
                                    Expression.PropertyOrField(Expression.PropertyOrField(groupkey, "Key"), rightSide.Member.Name)));
                            }
                            else throw new NotImplementedException();
                        }
                    break;
                }
                case ExpressionType.New:
                {
                    var newInit = keySelector.Body as NewExpression;
                    if (newInit.Type.IsAnonymousTypeName())
                    {
                        for (int i = 0; i < newInit.Arguments.Count; i++)
                        {
                            var leftSide = newInit.Members[i];
                            if (newInit.Arguments[i] is MemberExpression rightSide)
                            {
                                keyAssignments.Add(Expression.Bind(
                                    typeof(TOutput).GetTypeInfo().GetMember(leftSide.Name).Single(),
                                    Expression.PropertyOrField(Expression.PropertyOrField(groupkey, "Key"), rightSide.Member.Name)));
                            }
                            else throw new NotImplementedException();
                        }
                    }
                    else throw new NotImplementedException();
                    break;
                }
                default:
                    throw new NotImplementedException();
            }

            var constructor = Expression.MemberInit(
                initializer.Body as NewExpression,
                keyAssignments.Concat(
                    outputPublicFields.Select(
                        (a, i) => Expression.Bind(
                            a.Item3,
                            a.Item4
                                ? (Expression)Expression.New(typeof(Nullable<>).MakeGenericType(typeof(TValue)).GetTypeInfo().GetConstructor(new[] { typeof(TValue) }), Expression.ArrayIndex(aggvalues, Expression.Constant(i)))
                                : Expression.ArrayIndex(aggvalues, Expression.Constant(i))))));
            var resultConstructor = Expression.Lambda<Func<GroupSelectorInput<TGroupKey>, TAggValue[], TOutput>>(
                constructor, groupkey, aggvalues);

            return inputStreamable.GroupAggregate(keySelector, newAgg, resultConstructor);
        }

        /// <summary>
        /// Rotates a stream of payload objects into a stream of key-attribute-value triples corresponding to the data from the input payloads.
        /// </summary>
        /// <typeparam name="TKey">The type of the grouping key of the input streamable.</typeparam>
        /// <typeparam name="TInput">The type of the payload of the input streamable.</typeparam>
        /// <typeparam name="TPivotKey">The type of the unpivot key.</typeparam>
        /// <typeparam name="TValue">The type of the value component of the unpivot operation.</typeparam>
        /// <typeparam name="TResult">The type of the result.</typeparam>
        /// <param name="inputStreamable">The source streamable to unpivot.</param>
        /// <param name="initializer">A constructor describing how to create output values.</param>
        /// <param name="keySelector">A selector function returning the unpivot key.</param>
        /// <param name="attributeSelector">A selector function stating what field in the return type holds desired unpivoted attributes.</param>
        /// <param name="valueSelector">A selector function stating what field in the return type holds desired unpivoted values.</param>
        /// <returns>A stream of key-attribute-value triples corresponding to the original data stream.</returns>
        public static IStreamable<TKey, TResult> Unpivot<TKey, TInput, TPivotKey, TValue, TResult>(
            this IStreamable<TKey, TInput> inputStreamable,
            Expression<Func<TResult>> initializer,
            Expression<Func<TInput, TPivotKey>> keySelector,
            Expression<Func<TResult, string>> attributeSelector,
            Expression<Func<TResult, TValue>> valueSelector) where TResult : new()
        {
            Invariant.IsNotNull(inputStreamable, nameof(inputStreamable));
            Invariant.IsNotNull(initializer, nameof(initializer));
            Invariant.IsNotNull(keySelector, nameof(keySelector));
            Invariant.IsNotNull(attributeSelector, nameof(attributeSelector));
            Invariant.IsNotNull(valueSelector, nameof(valueSelector));

            if (!(initializer.Body is NewExpression newExpression)) throw new ArgumentException("Initializer must return a constructor expression.", nameof(initializer));
            if (!(attributeSelector.Body is MemberExpression attributeField)) throw new ArgumentException("Attribute selector expression must refer to a single field in the return type.");
            if (!(valueSelector.Body is MemberExpression valueField)) throw new ArgumentException("Value selector expression must refer to a single field in the return type.");

            Func<TInput, IEnumerable<TResult>> selector = new UnpivotEnumerable<TInput, TPivotKey, TValue, TResult>(keySelector, newExpression, attributeField, valueField).GetEnumerable;
            var selectMany = inputStreamable.SelectMany(e => selector(e));
            return selectMany;
        }

        private sealed class UnpivotEnumerable<TInput, TPivotKey, TValue, TResult>
        {
            private readonly Dictionary<string, Func<TInput, string, TResult>> fields = new Dictionary<string, Func<TInput, string, TResult>>();
            private readonly Dictionary<string, Func<TInput, bool>> isNull = new Dictionary<string, Func<TInput, bool>>();

            public UnpivotEnumerable(Expression<Func<TInput, TPivotKey>> keySelector, NewExpression newExpression, MemberExpression attributeField, MemberExpression valueField)
            {
                var input = Expression.Parameter(typeof(TInput), "input");
                var attribute = Expression.Parameter(typeof(string), "attribute");
                var keyAssignments = new List<MemberAssignment>();

                var keyFields = new HashSet<string>();

                // Key selector magic!
                switch (keySelector.Body.NodeType)
                {
                    case ExpressionType.MemberAccess:
                        {
                            var selector = keySelector.Body as MemberExpression;
                            keyAssignments.Add(Expression.Bind(
                                typeof(TResult).GetTypeInfo().GetMember(selector.Member.Name).Single(),
                                Expression.PropertyOrField(input, selector.Member.Name)));
                            keyFields.Add(selector.Member.Name);
                            break;
                        }
                    case ExpressionType.MemberInit:
                        {
                            var memberInit = keySelector.Body as MemberInitExpression;
                            foreach (var init in memberInit.Bindings)
                            {
                                if (init.BindingType != MemberBindingType.Assignment) throw new NotSupportedException("Unpivot operation currently requires key member assignments to be simple field expressions.");
                                var singleInit = init as MemberAssignment;

                                if (singleInit.Expression is MemberExpression rightSide)
                                {
                                    keyAssignments.Add(Expression.Bind(
                                        typeof(TResult).GetTypeInfo().GetMember(singleInit.Member.Name).Single(),
                                        Expression.PropertyOrField(input, rightSide.Member.Name)));
                                    keyFields.Add(rightSide.Member.Name);
                                }
                                else
                                {
                                    keyAssignments.Add(Expression.Bind(
                                        typeof(TResult).GetTypeInfo().GetMember(singleInit.Member.Name).Single(), ParameterSubstituter.Replace(keySelector.Parameters[0], input, singleInit.Expression)));
                                }
                            }
                            break;
                        }
                    case ExpressionType.New:
                        {
                            var newInit = keySelector.Body as NewExpression;
                            if (newInit.Type.IsAnonymousTypeName())
                            {
                                for (int i = 0; i < newInit.Arguments.Count; i++)
                                {
                                    var leftSide = newInit.Members[i];
                                    if (newInit.Arguments[i] is MemberExpression rightSide)
                                    {
                                        keyAssignments.Add(Expression.Bind(
                                            typeof(TResult).GetTypeInfo().GetMember(leftSide.Name).Single(),
                                            Expression.PropertyOrField(input, rightSide.Member.Name)));
                                        keyFields.Add(rightSide.Member.Name);
                                    }
                                    else
                                    {
                                        keyAssignments.Add(Expression.Bind(
                                            typeof(TResult).GetTypeInfo().GetMember(leftSide.Name).Single(), ParameterSubstituter.Replace(keySelector.Parameters[0], input, newInit.Arguments[i])));
                                    }
                                }
                            }
                            else
                            {
                                // Currently do the same thing as the default case, but we might be able to be smarter here.
                                // If we have an arbitrary expression, assume we have only one remaining field that is not the attribute or value fields and assign to it.
                                var outputFields = typeof(TResult).GetTypeInfo().GetFields(BindingFlags.Public | BindingFlags.Instance).OrderBy(o => o.Name).Select(o => o.Name)
                                    .Concat(
                                        typeof(TResult).GetTypeInfo().GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.GetIndexParameters().Length == 0).OrderBy(o => o.Name).Select(o => o.Name))
                                    .Where(o => o != attributeField.Member.Name && o != valueField.Member.Name).ToList();
                                if (outputFields.Count != 1) throw new NotSupportedException("Unpivot operation could not determine a unique field to which to assign key values.");

                                keyAssignments.Add(Expression.Bind(
                                    typeof(TResult).GetTypeInfo().GetMember(outputFields.Single()).Single(), keySelector.ReplaceParametersInBody(input)));
                            }
                            break;
                        }
                    default:
                        {
                            // If we have an arbitrary expression, assume we have only one remaining field that is not the attribute or value fields and assign to it.
                            var outputFields = typeof(TResult).GetTypeInfo().GetFields(BindingFlags.Public | BindingFlags.Instance).OrderBy(o => o.Name).Select(o => o.Name)
                                .Concat(
                                    typeof(TResult).GetTypeInfo().GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.GetIndexParameters().Length == 0).OrderBy(o => o.Name).Select(o => o.Name))
                                .Where(o => o != attributeField.Member.Name && o != valueField.Member.Name).ToList();
                            if (outputFields.Count != 1) throw new NotSupportedException("Unpivot operation could not determine a unique field to which to assign key values.");

                            keyAssignments.Add(Expression.Bind(
                                typeof(TResult).GetTypeInfo().GetMember(outputFields.Single()).Single(), keySelector.ReplaceParametersInBody(input)));
                            break;
                        }
                }

                keyAssignments.Add(Expression.Bind(attributeField.Member, attribute));
                foreach (var field in typeof(TInput).GetTypeInfo().GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.GetIndexParameters().Length == 0).Where(o => !keyFields.Contains(o.Name)).OrderBy(o => o.Name))
                {
                    if (IsNullable(field.PropertyType))
                    {
                        if (!typeof(TValue).GetTypeInfo().IsAssignableFrom(field.PropertyType.GetTypeInfo().GetGenericArguments()[0])) continue;
                        this.isNull.Add(
                            field.Name, Expression.Lambda<Func<TInput, bool>>(
                                Expression.IsTrue(Expression.PropertyOrField(Expression.PropertyOrField(input, field.Name), "HasValue")),
                                new[] { input }).Compile());
                    }
                    else if (valueField.Type.GetTypeInfo().IsClass)
                    {
                        if (!typeof(TValue).GetTypeInfo().IsAssignableFrom(field.PropertyType)) continue;
                        this.isNull.Add(
                            field.Name, Expression.Lambda<Func<TInput, bool>>(
                                Expression.Equal(Expression.PropertyOrField(input, field.Name), Expression.Constant(null)),
                                new[] { input }).Compile());
                    }
                    else
                    {
                        if (!typeof(TValue).GetTypeInfo().IsAssignableFrom(field.PropertyType)) continue;
                        this.isNull.Add(field.Name, (TInput o) => true);
                    }

                    var fieldAssignment = new[]
                    {
                        Expression.Bind(
                            valueField.Member,
                            IsNullable(field.PropertyType)
                                ? Expression.PropertyOrField(Expression.PropertyOrField(input, field.Name), "Value")
                                : Expression.PropertyOrField(input, field.Name))
                    };
                    var constructor = Expression.MemberInit(newExpression, keyAssignments.Concat(fieldAssignment));
                    var fieldResult = Expression.Lambda<Func<TInput, string, TResult>>(constructor, input, attribute);
                    this.fields.Add(field.Name, fieldResult.Compile());
                }
                foreach (var field in typeof(TInput).GetTypeInfo().GetFields(BindingFlags.Public | BindingFlags.Instance).Where(o => !keyFields.Contains(o.Name)).OrderBy(o => o.Name))
                {
                    if (IsNullable(field.FieldType))
                    {
                        if (!typeof(TValue).GetTypeInfo().IsAssignableFrom(field.FieldType.GetTypeInfo().GetGenericArguments()[0])) continue;
                        this.isNull.Add(
                            field.Name, Expression.Lambda<Func<TInput, bool>>(
                                Expression.IsFalse(Expression.PropertyOrField(Expression.PropertyOrField(input, field.Name), "HasValue")),
                                new[] { input }).Compile());
                    }
                    else if (valueField.Type.GetTypeInfo().IsClass)
                    {
                        if (!typeof(TValue).GetTypeInfo().IsAssignableFrom(field.FieldType)) continue;
                        this.isNull.Add(
                            field.Name, Expression.Lambda<Func<TInput, bool>>(
                                Expression.Equal(Expression.PropertyOrField(input, field.Name), Expression.Constant(null)),
                                new[] { input }).Compile());
                    }
                    else
                    {
                        if (!typeof(TValue).GetTypeInfo().IsAssignableFrom(field.FieldType)) continue;
                        this.isNull.Add(field.Name, (TInput o) => true);
                    }

                    var fieldAssignment = new[]
                    {
                        Expression.Bind(
                            valueField.Member,
                            IsNullable(field.FieldType)
                                ? Expression.PropertyOrField(Expression.PropertyOrField(input, field.Name), "Value")
                                : Expression.PropertyOrField(input, field.Name))
                    };
                    var constructor = Expression.MemberInit(newExpression, keyAssignments.Concat(fieldAssignment));
                    var fieldResult = Expression.Lambda<Func<TInput, string, TResult>>(constructor, input, attribute);
                    this.fields.Add(field.Name, fieldResult.Compile());
                }
            }

            public IEnumerable<TResult> GetEnumerable(TInput obj)
            {
                foreach (var att in this.fields) if (!this.isNull[att.Key](obj)) yield return att.Value(obj, att.Key);
            }
        }

        private static bool IsNullable(Type type) => type.GetTypeInfo().IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
    }
}