// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.StreamProcessing.Aggregates;

namespace Microsoft.StreamProcessing.Provider
{
    /// <summary>
    /// Visitor to identify whether the result constructor can be entirely expressed using aggregates,
    /// and if so, returns said aggregates and associated type information
    /// - References to the grouping key of IGrouping(T,K) become references to a grouping key for a Window
    /// - All other references are altered to be from the state variable
    /// </summary>
    internal sealed class GroupBySecondPassVisitor : ExpressionVisitor
    {
        private readonly ParameterExpression stateParameter;
        private readonly ParameterExpression windowParameter;
        private readonly List<Expression> seenAggregates = new List<Expression>();
        private readonly List<ParameterExpression> createdParameters = new List<ParameterExpression>();
        private readonly List<Expression> createdAggregates = new List<Expression>();
        private readonly List<Type> aggregateStateTypes = new List<Type>();
        private readonly List<Type> aggregateResultTypes = new List<Type>();
        private bool foundUnknown = false;

        public static LambdaExpression CreateAggregateProfile(
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
                constructorExpression.Parameters[0], constructorExpression.Parameters[1]);
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

        private GroupBySecondPassVisitor(ParameterExpression windowParameter, ParameterExpression stateParameter)
        {
            this.windowParameter = windowParameter;
            this.stateParameter = stateParameter;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Object == null && node.Arguments[0] == stateParameter && node.Method.IsDefined(typeof(AggregateAttribute), false))
            {
                for (int i = 0; i < this.seenAggregates.Count; i++)
                {
                    if (this.seenAggregates[i].ExpressionEquals(node)) return this.createdParameters[i];
                }

                var aggType = ((AggregateAttribute)node.Method.GetCustomAttributes(typeof(AggregateAttribute), false)[0]).Aggregate;
                var aggregateInterface = aggType.GetInterfaces().FirstOrDefault(
                    e => e.IsGenericType && e.GetGenericTypeDefinition() == typeof(IAggregate<,,>));
                if (aggregateInterface == null) throw new InvalidOperationException("AggregateAttribute constructor should have verified that aggregate class must implement IAggregate interface.");

                this.seenAggregates.Add(node);
                var genericArguments = aggregateInterface.GetGenericArguments();
                var newParameter = Expression.Parameter(genericArguments[2]);
                this.createdAggregates.Add(Expression.New(aggType));
                this.createdParameters.Add(newParameter);
                this.aggregateStateTypes.Add(genericArguments[1]);
                this.aggregateResultTypes.Add(genericArguments[2]);
                return newParameter;
            }
            return base.VisitMethodCall(node);
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node == stateParameter) this.foundUnknown = true;
            return base.VisitParameter(node);
        }
    }
}
