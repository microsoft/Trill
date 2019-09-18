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
    internal sealed partial class InOrderVisitor : QStreamableVisitor
    {
        private static MethodInfo GenerateWrapMethodInfo(Type newInputType, Type initialInputType, Type stateType, Type resultType)
        {
            Expression<Func<IAggregate<int, int, int>, Expression<Func<int, int>>, IAggregate<int, int, int>>> wrap
                = (a, f) => a.Wrap(f);
            return ((MethodCallExpression)wrap.Body).Method
                .GetGenericMethodDefinition()
                .MakeGenericMethod(newInputType, initialInputType, stateType, resultType);
        }

        private static MethodInfo GenerateMethodInfoForGroupAggregate1(
            Type inputGroupType, Type inputPayloadType, Type newGroupType,
            Type state1Type, Type result1Type,
            Type overallResultType)
        {
            Expression<Func<IStreamable<Empty, int>, IStreamable<Empty, int>>> groupAgg =
                o => o.GroupAggregate(
                    i => i,
                    new CountAggregate<int>(),
                    (g, c1) => g.Key);
            return ((MethodCallExpression)groupAgg.Body).Method.GetGenericMethodDefinition()
                .MakeGenericMethod(
                    inputGroupType, inputPayloadType, newGroupType,
                    state1Type, result1Type,
                    overallResultType);
        }

        private static MethodInfo GenerateMethodInfoForGroupAggregate2(
            Type inputGroupType, Type inputPayloadType, Type newGroupType,
            Type state1Type, Type result1Type,
            Type state2Type, Type result2Type,
            Type overallResultType)
        {
            Expression<Func<IStreamable<Empty, int>, IStreamable<Empty, int>>> groupAgg =
                o => o.GroupAggregate(
                    i => i,
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    (g, c1, c2) => g.Key);
            var oldMethod = ((MethodCallExpression)groupAgg.Body).Method;
            return ((MethodCallExpression)groupAgg.Body).Method.GetGenericMethodDefinition()
                .MakeGenericMethod(
                    inputGroupType, inputPayloadType, newGroupType,
                    state1Type, result1Type,
                    state2Type, result2Type,
                    overallResultType);
        }

        private static MethodInfo GenerateMethodInfoForGroupAggregate3(
            Type inputGroupType, Type inputPayloadType, Type newGroupType,
            Type state1Type, Type result1Type,
            Type state2Type, Type result2Type,
            Type state3Type, Type result3Type,
            Type overallResultType)
        {
            Expression<Func<IStreamable<Empty, int>, IStreamable<Empty, int>>> groupAgg =
                o => o.GroupAggregate(
                    i => i,
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    (g, c1, c2, c3) => g.Key);
            return ((MethodCallExpression)groupAgg.Body).Method.GetGenericMethodDefinition()
                .MakeGenericMethod(
                    inputGroupType, inputPayloadType, newGroupType,
                    state1Type, result1Type,
                    state2Type, result2Type,
                    state3Type, result3Type,
                    overallResultType);
        }
    }
}
