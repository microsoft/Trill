// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Provider
{
    /// <summary>
    /// Visitor to translate a Select expression into a valid result constructor for a GroupAggregate call
    /// - References to the grouping key of IGrouping(T,K) become references to a grouping key for a Window
    /// - All other references are altered to be from the state variable
    /// </summary>
    internal sealed class JoinFirstPassVisitor : ExpressionVisitor
    {
        private readonly ParameterExpression inputVariable;
        private readonly ParameterExpression leftParameter;
        private readonly ParameterExpression rightParameter;
        private bool foundBareReference = false;

        public static bool TryCreateConstructorFromSelect(LambdaExpression selectExpression, out LambdaExpression resultExpression)
        {
            // Expecting input like (ValueTuple<T1, T2> param) => f(param)
            // Producing input like (T1 left, T2 right) => f(left, right)
            var valueType = selectExpression.Parameters[0].Type;
            var visitor = new JoinFirstPassVisitor(
                selectExpression.Parameters[0], valueType.GenericTypeArguments[0], valueType.GenericTypeArguments[1]);
            resultExpression = Expression.Lambda(visitor.Visit(selectExpression.Body), visitor.leftParameter, visitor.rightParameter);
            return !visitor.foundBareReference;
        }

        private JoinFirstPassVisitor(ParameterExpression inputVariable, Type leftType, Type rightType)
        {
            this.leftParameter = Expression.Parameter(leftType);
            this.rightParameter = Expression.Parameter(rightType);
            this.inputVariable = inputVariable;
        }

        protected override Expression VisitMember(MemberExpression node)
            => node.Expression == this.inputVariable && node.Member.Name == "Item1"
                ? leftParameter
                : node.Expression == this.inputVariable && node.Member.Name == "Item2"
                    ? rightParameter
                    : base.VisitMember(node);

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node == this.inputVariable) this.foundBareReference = true;
            return base.VisitParameter(node);
        }
    }
}
