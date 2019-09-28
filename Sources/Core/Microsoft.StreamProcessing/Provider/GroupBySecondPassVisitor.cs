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

namespace Microsoft.StreamProcessing.Provider
{
    /// <summary>
    /// Visitor to identify whether the result constructor can be entirely expressed using aggregates,
    /// and if so, returns said aggregates and associated type information
    /// - References to the grouping key of IGrouping(T,K) become references to a grouping key for a Window
    /// - All other references are altered to be from the state variable
    /// </summary>
    internal sealed partial class GroupBySecondPassVisitor : ExpressionVisitor
    {
        private static readonly Lazy<Dictionary<MethodInfo, Expression>> ZeroParameterAggregates
            = new Lazy<Dictionary<MethodInfo, Expression>>(() => new Dictionary<MethodInfo, Expression>
                {
                    { GetMethodInfoForSumInt(), ExpressionForSumIntAggregate },
                    { GetMethodInfoForSumLong(), ExpressionForSumLongAggregate },
                    { GetMethodInfoForSumFloat(), ExpressionForSumFloatAggregate },
                    { GetMethodInfoForSumDouble(), ExpressionForSumDoubleAggregate },
                    { GetMethodInfoForSumDecimal(), ExpressionForSumDecimalAggregate },
                    { GetMethodInfoForNullableSumInt(), ExpressionForNullableSumIntAggregate },
                    { GetMethodInfoForNullableSumLong(), ExpressionForNullableSumLongAggregate },
                    { GetMethodInfoForNullableSumFloat(), ExpressionForNullableSumFloatAggregate },
                    { GetMethodInfoForNullableSumDouble(), ExpressionForNullableSumDoubleAggregate },
                    { GetMethodInfoForNullableSumDecimal(), ExpressionForNullableSumDecimalAggregate },
                    { GetMethodInfoForAverageInt(), ExpressionForAverageIntAggregate },
                    { GetMethodInfoForAverageLong(), ExpressionForAverageLongAggregate },
                    { GetMethodInfoForAverageFloat(), ExpressionForAverageFloatAggregate },
                    { GetMethodInfoForAverageDouble(), ExpressionForAverageDoubleAggregate },
                    { GetMethodInfoForAverageDecimal(), ExpressionForAverageDecimalAggregate },
                    { GetMethodInfoForNullableAverageInt(), ExpressionForNullableAverageIntAggregate },
                    { GetMethodInfoForNullableAverageLong(), ExpressionForNullableAverageLongAggregate },
                    { GetMethodInfoForNullableAverageFloat(), ExpressionForNullableAverageFloatAggregate },
                    { GetMethodInfoForNullableAverageDouble(), ExpressionForNullableAverageDoubleAggregate },
                    { GetMethodInfoForNullableAverageDecimal(), ExpressionForNullableAverageDecimalAggregate },
                });
        private static readonly Lazy<Dictionary<MethodInfo, Func<Type, Expression>>> OneParameterAggregates
            = new Lazy<Dictionary<MethodInfo, Func<Type, Expression>>>(() => new Dictionary<MethodInfo, Func<Type, Expression>>
            {
                { GetMethodInfoForCount(), GetCreateMethodForType(nameof(CreateExpressionForCountAggregate)) },
                { GetMethodInfoForLongCount(), GetCreateMethodForType(nameof(CreateExpressionForLongCountAggregate)) },
            });

        private readonly ParameterExpression stateParameter;
        private readonly ParameterExpression windowParameter;
        private readonly List<Expression> seenAggregates = new List<Expression>();
        private readonly List<ParameterExpression> createdParameters = new List<ParameterExpression>();
        private readonly List<Expression> createdAggregates = new List<Expression>();
        private readonly List<Type> aggregateStateTypes = new List<Type>();
        private readonly List<Type> aggregateResultTypes = new List<Type>();
        private readonly QueryContainer container;

        private bool foundUnknown = false;

        public static LambdaExpression CreateAggregateProfile(
            QueryContainer container,
            LambdaExpression constructorExpression,
            out List<Expression> createdAggregates,
            out List<Type> aggregateStateTypes,
            out List<Type> aggregateResultTypes)
        {
            createdAggregates = new List<Expression>();
            aggregateStateTypes = new List<Type>();
            aggregateResultTypes = new List<Type>();

            // Expecting input like (GroupSelectorInput(K) key, IEnumerable(V) list) => f(key, list)
            // Producing input like (GroupSelectorInput(K) key, state1 s1, state2 s2, ...) => f(key, s1, s2, ...)
            var visitor = new GroupBySecondPassVisitor(
                container, constructorExpression.Parameters[0], constructorExpression.Parameters[1]);
            var output = visitor.Visit(constructorExpression.Body);

            // If we have found any unsupported state references, return the original function
            if (visitor.foundUnknown || visitor.createdAggregates.Count == 0) return constructorExpression;

            // If we have found more than 15 distinct aggregates, we do not support it (yet)
            if (visitor.createdAggregates.Count > 15) return constructorExpression;

            createdAggregates = visitor.createdAggregates;
            aggregateStateTypes = visitor.aggregateStateTypes;
            aggregateResultTypes = visitor.aggregateResultTypes;
            return Expression.Lambda(output, visitor.windowParameter.Yield().Concat(visitor.createdParameters).ToArray());
        }

        private GroupBySecondPassVisitor(QueryContainer container, ParameterExpression windowParameter, ParameterExpression stateParameter)
        {
            this.container = container;
            this.windowParameter = windowParameter;
            this.stateParameter = stateParameter;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            for (int i = 0; i < this.seenAggregates.Count; i++)
            {
                if (this.seenAggregates[i].ExpressionEquals(node)) return this.createdParameters[i];
            }

            // We are only looking at method call replacements on extension methods on the state parameter
            if (node.Object == null && node.Arguments[0] == stateParameter)
            {
                var method = node.Method;
                Type aggregateInterface = null;
                Expression aggregateExpression = null;

                // Case 1: Method has a user-defined aggregate defined as an attribute
                if (method.IsDefined(typeof(AggregateAttribute), false))
                {
                    var aggType = ((AggregateAttribute)method.GetCustomAttributes(typeof(AggregateAttribute), false)[0]).Aggregate;
                    aggregateInterface = aggType.GetInterfaces().FirstOrDefault(
                        e => e.IsGenericType && e.GetGenericTypeDefinition() == typeof(IAggregate<,,>));
                    if (aggregateInterface == null) throw new InvalidOperationException("AggregateAttribute constructor should have verified that aggregate class must implement IAggregate interface.");

                    // Type test: input type to aggregate must be the same as the element type of the enumerable,
                    // and the output type of the aggregate must be the same as the output of the enumerable method
                    if (aggregateInterface.GenericTypeArguments[0] == stateParameter.Type.GenericTypeArguments[0]
                        && aggregateInterface.GenericTypeArguments[2] == method.ReturnType)
                    {
                        aggregateExpression = Expression.New(aggType);
                    }

                    // One fallback alternative: if the input and output are both nullable, and their base types
                    // match the types as stated above, we can create the nullable version of the aggregate
                    else if (method.GetParameters()[0].ParameterType.GenericTypeArguments[0].IsGenericType
                        && method.GetParameters()[0].ParameterType.GenericTypeArguments[0].GetGenericTypeDefinition() == typeof(Nullable<>)
                        && method.ReturnType.IsGenericType
                        && method.ReturnType.GetGenericTypeDefinition() == typeof(Nullable<>)
                        && aggregateInterface.GenericTypeArguments[0] == method.GetParameters()[0].ParameterType.GenericTypeArguments[0].GenericTypeArguments[0]
                        && aggregateInterface.GenericTypeArguments[2] == method.ReturnType.GenericTypeArguments[0])
                    {
                        aggregateExpression = CreateNullableAggregateLambda(aggregateInterface.GenericTypeArguments)
                            .ReplaceParametersInBody(Expression.New(aggType));
                        aggregateInterface = aggregateExpression.Type;
                    }

                    // Otherwise, we need to alert the user to the issue
                    else
                    {
                        throw new InvalidOperationException(
                            string.Format(
                                "The type signature of the aggregate does not match the input and output types for the attached method." + Environment.NewLine
                                + "Method Name: {0}" + Environment.NewLine
                                + "Aggregate Type: {1}" + Environment.NewLine
                                + "Input Type Expected: {2}" + Environment.NewLine
                                + "Input Type of Aggregate: {3}" + Environment.NewLine
                                + "Output Type Expected: {4}" + Environment.NewLine
                                + "Output Type of Aggregage: {5}",
                                method.Name,
                                aggType.FullName,
                                method.GetParameters()[0].ParameterType.FullName,
                                aggregateInterface.GenericTypeArguments[0].FullName,
                                method.ReturnType.FullName,
                                aggregateInterface.GenericTypeArguments[2].FullName));
                    }
                }

                // Case 2: Method is a supported built-in extension method on IEnumerable with a type argument
                else if (method.IsGenericMethod && OneParameterAggregates.Value.TryGetValue(
                    method.GetGenericMethodDefinition(),
                    out var create))
                {
                    aggregateExpression = create(method.GetGenericArguments()[0]);
                    aggregateInterface = aggregateExpression.Type;
                }

                // Case 3: Method is a supported built-in extension method on IEnumerable without a type argument
                else if (!method.IsGenericMethod && ZeroParameterAggregates.Value.TryGetValue(
                    method,
                    out aggregateExpression))
                {
                    aggregateInterface = aggregateExpression.Type.GetInterfaces()
                        .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IAggregate<,,>));
                }

                if (aggregateInterface != null)
                {
                    this.seenAggregates.Add(node);
                    var genericArguments = aggregateInterface.GetGenericArguments();
                    var newParameter = Expression.Parameter(genericArguments[2]);
                    this.createdAggregates.Add(aggregateExpression);
                    this.createdParameters.Add(newParameter);
                    this.aggregateStateTypes.Add(genericArguments[1]);
                    this.aggregateResultTypes.Add(genericArguments[2]);
                    return newParameter;
                }
            }

            return base.VisitMethodCall(node);
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node == stateParameter) this.foundUnknown = true;
            return base.VisitParameter(node);
        }

        private static LambdaExpression CreateNullableAggregateLambda(Type[] aggregateTypes)
            => (LambdaExpression)typeof(GroupBySecondPassVisitor)
            .GetMethod(nameof(CreateNullableAggregate), BindingFlags.Static | BindingFlags.NonPublic)
            .MakeGenericMethod(aggregateTypes)
            .Invoke(null, Array.Empty<object>());

        private static Expression<Func<IAggregate<TInput, TState, TOutput>, IAggregate<TInput?, NullOutputWrapper<TState>, TOutput?>>> CreateNullableAggregate<TInput, TState, TOutput>()
            where TInput : struct
            where TOutput : struct
            => aggregate => aggregate.MakeInputNullableAndSkipNulls().MakeOutputNullableAndOutputNullWhenEmpty();

        private static Func<Type, Expression> GetCreateMethodForType(string name)
            => type => (Expression)typeof(GroupBySecondPassVisitor)
            .GetMethod(name, BindingFlags.Static | BindingFlags.NonPublic)
            .MakeGenericMethod(type)
            .Invoke(null, Array.Empty<object>());

        private static MethodInfo GetMethodInfoForCount()
        {
            Expression<Func<IEnumerable<object>, int>> expression = (e) => e.Count();
            return ((MethodCallExpression)expression.Body).Method.GetGenericMethodDefinition();
        }

        private static Expression CreateExpressionForCountAggregate<T>()
        {
            Expression<Func<IAggregate<T, ulong, int>>> expression = () => new CountAggregate<T>().TransformOutput(o => (int)o);
            return expression.Body;
        }

        private static MethodInfo GetMethodInfoForLongCount()
        {
            Expression<Func<IEnumerable<object>, long>> expression = (e) => e.LongCount();
            return ((MethodCallExpression)expression.Body).Method.GetGenericMethodDefinition();
        }

        private static Expression CreateExpressionForLongCountAggregate<T>()
        {
            Expression<Func<IAggregate<T, ulong, long>>> expression = () => new CountAggregate<T>().TransformOutput(o => (long)o);
            return expression.Body;
        }
    }
}
