// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    internal static class ExpressionExtensions
    {
        public static Expression ReplaceParametersInBody(this LambdaExpression lambda, params Expression[] expressions)
            => ParameterSubstituter.Replace(lambda.Parameters, lambda.Body, expressions);
    }

    internal sealed class ParameterSubstituter : ExpressionVisitor
    {
        private readonly Dictionary<ParameterExpression, Expression> arguments;

        public static Expression Replace(ReadOnlyCollection<ParameterExpression> parameters, Expression expression, params Expression[] substitutes)
        {
            var dictionary = new Dictionary<ParameterExpression, Expression>();
            for (int i = 0; i < Math.Min(parameters.Count, substitutes.Length); i++) dictionary.Add(parameters[i], substitutes[i]);
            return new ParameterSubstituter(dictionary).Visit(expression);
        }

        private ParameterSubstituter(Dictionary<ParameterExpression, Expression> arguments)
            => this.arguments = new Dictionary<ParameterExpression, Expression>(arguments);

        protected override Expression VisitParameter(ParameterExpression node)
            => this.arguments.TryGetValue(node, out var replacement)
                ? replacement
                : base.VisitParameter(node);
    }

}
