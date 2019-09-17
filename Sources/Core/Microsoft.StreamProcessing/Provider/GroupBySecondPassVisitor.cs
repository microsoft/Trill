// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace Microsoft.StreamProcessing.Provider
{
    /// <summary>
    /// Visitor to translate a Select expression into a valid result constructor for a GroupAggregate call
    /// - References to the grouping key of IGrouping(T,K) become references to a grouping key for a Window
    /// - All other references are altered to be from the state variable
    /// </summary>
    internal sealed class GroupBySecondPassVisitor : ExpressionVisitor
    {
        private readonly ParameterExpression inputVariable;
        private readonly ParameterExpression stateParameter;
        private readonly ParameterExpression windowParameter;

        public static LambdaExpression CreateConstructorFromSelect(LambdaExpression selectExpression)
        {
            // Expecting input like (IGrouping(K, V) param) => f(param)
            // Producing input like (GroupSelectorInput(K) key, IEnumerable(V) list) => f(key, list)
            var groupingType = selectExpression.Parameters[0].Type;
            var visitor = new GroupBySecondPassVisitor(
                selectExpression.Parameters[0], groupingType.GenericTypeArguments[0], groupingType.GenericTypeArguments[1]);
            return Expression.Lambda(visitor.Visit(selectExpression.Body), visitor.windowParameter, visitor.stateParameter);
        }

        private GroupBySecondPassVisitor(ParameterExpression inputVariable, Type groupingKeyType, Type elementType)
        {
            this.windowParameter = Expression.Parameter(typeof(GroupSelectorInput<>).MakeGenericType(groupingKeyType));
            this.stateParameter = Expression.Parameter(typeof(IEnumerable<>).MakeGenericType(elementType));
            this.inputVariable = inputVariable;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            return base.VisitMethodCall(node);
        }

        protected override Expression VisitParameter(ParameterExpression node)
            => node == this.inputVariable ? this.stateParameter : base.VisitParameter(node);
    }
}
