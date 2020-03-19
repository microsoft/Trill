// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    internal enum ExpressionCategory { Select, SelectMany, Where }

    internal sealed class ExpressionProfile
    {
        public ExpressionCategory category;
        public Type keyType;
        public Type inputType;
        public Type outputType;
        public bool hasKey;
        public bool hasStartEdge;
        public LambdaExpression expression;

        public ExpressionProfile Clone()
        {
            var p = new ExpressionProfile
            {
                category = this.category,
                keyType = this.keyType,
                inputType = this.inputType,
                outputType = this.outputType,
                hasKey = this.hasKey,
                hasStartEdge = this.hasStartEdge,
                expression = this.expression
            };
            return p;
        }
    }
}
