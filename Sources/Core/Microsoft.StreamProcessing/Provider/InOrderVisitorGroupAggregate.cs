// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
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

        private static MethodInfo GenerateMethodInfoForGroupAggregate4(
            Type inputGroupType, Type inputPayloadType, Type newGroupType,
            Type state1Type, Type result1Type,
            Type state2Type, Type result2Type,
            Type state3Type, Type result3Type,
            Type state4Type, Type result4Type,
            Type overallResultType)
        {
            Expression<Func<IStreamable<Empty, int>, IStreamable<Empty, int>>> groupAgg =
                o => o.GroupAggregate(
                    i => i,
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    (g, c1, c2, c3, c4) => g.Key);
            return ((MethodCallExpression)groupAgg.Body).Method.GetGenericMethodDefinition()
                .MakeGenericMethod(
                    inputGroupType, inputPayloadType, newGroupType,
                    state1Type, result1Type,
                    state2Type, result2Type,
                    state3Type, result3Type,
                    state4Type, result4Type,
                    overallResultType);
        }

        private static MethodInfo GenerateMethodInfoForGroupAggregate5(
            Type inputGroupType, Type inputPayloadType, Type newGroupType,
            Type state1Type, Type result1Type,
            Type state2Type, Type result2Type,
            Type state3Type, Type result3Type,
            Type state4Type, Type result4Type,
            Type state5Type, Type result5Type,
            Type overallResultType)
        {
            Expression<Func<IStreamable<Empty, int>, IStreamable<Empty, int>>> groupAgg =
                o => o.GroupAggregate(
                    i => i,
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    (g, c1, c2, c3, c4, c5) => g.Key);
            return ((MethodCallExpression)groupAgg.Body).Method.GetGenericMethodDefinition()
                .MakeGenericMethod(
                    inputGroupType, inputPayloadType, newGroupType,
                    state1Type, result1Type,
                    state2Type, result2Type,
                    state3Type, result3Type,
                    state4Type, result4Type,
                    state5Type, result5Type,
                    overallResultType);
        }

        private static MethodInfo GenerateMethodInfoForGroupAggregate6(
            Type inputGroupType, Type inputPayloadType, Type newGroupType,
            Type state1Type, Type result1Type,
            Type state2Type, Type result2Type,
            Type state3Type, Type result3Type,
            Type state4Type, Type result4Type,
            Type state5Type, Type result5Type,
            Type state6Type, Type result6Type,
            Type overallResultType)
        {
            Expression<Func<IStreamable<Empty, int>, IStreamable<Empty, int>>> groupAgg =
                o => o.GroupAggregate(
                    i => i,
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    (g, c1, c2, c3, c4, c5, c6) => g.Key);
            return ((MethodCallExpression)groupAgg.Body).Method.GetGenericMethodDefinition()
                .MakeGenericMethod(
                    inputGroupType, inputPayloadType, newGroupType,
                    state1Type, result1Type,
                    state2Type, result2Type,
                    state3Type, result3Type,
                    state4Type, result4Type,
                    state5Type, result5Type,
                    state6Type, result6Type,
                    overallResultType);
        }

        private static MethodInfo GenerateMethodInfoForGroupAggregate7(
            Type inputGroupType, Type inputPayloadType, Type newGroupType,
            Type state1Type, Type result1Type,
            Type state2Type, Type result2Type,
            Type state3Type, Type result3Type,
            Type state4Type, Type result4Type,
            Type state5Type, Type result5Type,
            Type state6Type, Type result6Type,
            Type state7Type, Type result7Type,
            Type overallResultType)
        {
            Expression<Func<IStreamable<Empty, int>, IStreamable<Empty, int>>> groupAgg =
                o => o.GroupAggregate(
                    i => i,
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    (g, c1, c2, c3, c4, c5, c6, c7) => g.Key);
            return ((MethodCallExpression)groupAgg.Body).Method.GetGenericMethodDefinition()
                .MakeGenericMethod(
                    inputGroupType, inputPayloadType, newGroupType,
                    state1Type, result1Type,
                    state2Type, result2Type,
                    state3Type, result3Type,
                    state4Type, result4Type,
                    state5Type, result5Type,
                    state6Type, result6Type,
                    state7Type, result7Type,
                    overallResultType);
        }

        private static MethodInfo GenerateMethodInfoForGroupAggregate8(
            Type inputGroupType, Type inputPayloadType, Type newGroupType,
            Type state1Type, Type result1Type,
            Type state2Type, Type result2Type,
            Type state3Type, Type result3Type,
            Type state4Type, Type result4Type,
            Type state5Type, Type result5Type,
            Type state6Type, Type result6Type,
            Type state7Type, Type result7Type,
            Type state8Type, Type result8Type,
            Type overallResultType)
        {
            Expression<Func<IStreamable<Empty, int>, IStreamable<Empty, int>>> groupAgg =
                o => o.GroupAggregate(
                    i => i,
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    (g, c1, c2, c3, c4, c5, c6, c7, c8) => g.Key);
            return ((MethodCallExpression)groupAgg.Body).Method.GetGenericMethodDefinition()
                .MakeGenericMethod(
                    inputGroupType, inputPayloadType, newGroupType,
                    state1Type, result1Type,
                    state2Type, result2Type,
                    state3Type, result3Type,
                    state4Type, result4Type,
                    state5Type, result5Type,
                    state6Type, result6Type,
                    state7Type, result7Type,
                    state8Type, result8Type,
                    overallResultType);
        }

        private static MethodInfo GenerateMethodInfoForGroupAggregate9(
            Type inputGroupType, Type inputPayloadType, Type newGroupType,
            Type state1Type, Type result1Type,
            Type state2Type, Type result2Type,
            Type state3Type, Type result3Type,
            Type state4Type, Type result4Type,
            Type state5Type, Type result5Type,
            Type state6Type, Type result6Type,
            Type state7Type, Type result7Type,
            Type state8Type, Type result8Type,
            Type state9Type, Type result9Type,
            Type overallResultType)
        {
            Expression<Func<IStreamable<Empty, int>, IStreamable<Empty, int>>> groupAgg =
                o => o.GroupAggregate(
                    i => i,
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    (g, c1, c2, c3, c4, c5, c6, c7, c8, c9) => g.Key);
            return ((MethodCallExpression)groupAgg.Body).Method.GetGenericMethodDefinition()
                .MakeGenericMethod(
                    inputGroupType, inputPayloadType, newGroupType,
                    state1Type, result1Type,
                    state2Type, result2Type,
                    state3Type, result3Type,
                    state4Type, result4Type,
                    state5Type, result5Type,
                    state6Type, result6Type,
                    state7Type, result7Type,
                    state8Type, result8Type,
                    state9Type, result9Type,
                    overallResultType);
        }

        private static MethodInfo GenerateMethodInfoForGroupAggregate10(
            Type inputGroupType, Type inputPayloadType, Type newGroupType,
            Type state1Type, Type result1Type,
            Type state2Type, Type result2Type,
            Type state3Type, Type result3Type,
            Type state4Type, Type result4Type,
            Type state5Type, Type result5Type,
            Type state6Type, Type result6Type,
            Type state7Type, Type result7Type,
            Type state8Type, Type result8Type,
            Type state9Type, Type result9Type,
            Type state10Type, Type result10Type,
            Type overallResultType)
        {
            Expression<Func<IStreamable<Empty, int>, IStreamable<Empty, int>>> groupAgg =
                o => o.GroupAggregate(
                    i => i,
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    (g, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10) => g.Key);
            return ((MethodCallExpression)groupAgg.Body).Method.GetGenericMethodDefinition()
                .MakeGenericMethod(
                    inputGroupType, inputPayloadType, newGroupType,
                    state1Type, result1Type,
                    state2Type, result2Type,
                    state3Type, result3Type,
                    state4Type, result4Type,
                    state5Type, result5Type,
                    state6Type, result6Type,
                    state7Type, result7Type,
                    state8Type, result8Type,
                    state9Type, result9Type,
                    state10Type, result10Type,
                    overallResultType);
        }

        private static MethodInfo GenerateMethodInfoForGroupAggregate11(
            Type inputGroupType, Type inputPayloadType, Type newGroupType,
            Type state1Type, Type result1Type,
            Type state2Type, Type result2Type,
            Type state3Type, Type result3Type,
            Type state4Type, Type result4Type,
            Type state5Type, Type result5Type,
            Type state6Type, Type result6Type,
            Type state7Type, Type result7Type,
            Type state8Type, Type result8Type,
            Type state9Type, Type result9Type,
            Type state10Type, Type result10Type,
            Type state11Type, Type result11Type,
            Type overallResultType)
        {
            Expression<Func<IStreamable<Empty, int>, IStreamable<Empty, int>>> groupAgg =
                o => o.GroupAggregate(
                    i => i,
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    (g, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11) => g.Key);
            return ((MethodCallExpression)groupAgg.Body).Method.GetGenericMethodDefinition()
                .MakeGenericMethod(
                    inputGroupType, inputPayloadType, newGroupType,
                    state1Type, result1Type,
                    state2Type, result2Type,
                    state3Type, result3Type,
                    state4Type, result4Type,
                    state5Type, result5Type,
                    state6Type, result6Type,
                    state7Type, result7Type,
                    state8Type, result8Type,
                    state9Type, result9Type,
                    state10Type, result10Type,
                    state11Type, result11Type,
                    overallResultType);
        }

        private static MethodInfo GenerateMethodInfoForGroupAggregate12(
            Type inputGroupType, Type inputPayloadType, Type newGroupType,
            Type state1Type, Type result1Type,
            Type state2Type, Type result2Type,
            Type state3Type, Type result3Type,
            Type state4Type, Type result4Type,
            Type state5Type, Type result5Type,
            Type state6Type, Type result6Type,
            Type state7Type, Type result7Type,
            Type state8Type, Type result8Type,
            Type state9Type, Type result9Type,
            Type state10Type, Type result10Type,
            Type state11Type, Type result11Type,
            Type state12Type, Type result12Type,
            Type overallResultType)
        {
            Expression<Func<IStreamable<Empty, int>, IStreamable<Empty, int>>> groupAgg =
                o => o.GroupAggregate(
                    i => i,
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    (g, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12) => g.Key);
            return ((MethodCallExpression)groupAgg.Body).Method.GetGenericMethodDefinition()
                .MakeGenericMethod(
                    inputGroupType, inputPayloadType, newGroupType,
                    state1Type, result1Type,
                    state2Type, result2Type,
                    state3Type, result3Type,
                    state4Type, result4Type,
                    state5Type, result5Type,
                    state6Type, result6Type,
                    state7Type, result7Type,
                    state8Type, result8Type,
                    state9Type, result9Type,
                    state10Type, result10Type,
                    state11Type, result11Type,
                    state12Type, result12Type,
                    overallResultType);
        }

        private static MethodInfo GenerateMethodInfoForGroupAggregate13(
            Type inputGroupType, Type inputPayloadType, Type newGroupType,
            Type state1Type, Type result1Type,
            Type state2Type, Type result2Type,
            Type state3Type, Type result3Type,
            Type state4Type, Type result4Type,
            Type state5Type, Type result5Type,
            Type state6Type, Type result6Type,
            Type state7Type, Type result7Type,
            Type state8Type, Type result8Type,
            Type state9Type, Type result9Type,
            Type state10Type, Type result10Type,
            Type state11Type, Type result11Type,
            Type state12Type, Type result12Type,
            Type state13Type, Type result13Type,
            Type overallResultType)
        {
            Expression<Func<IStreamable<Empty, int>, IStreamable<Empty, int>>> groupAgg =
                o => o.GroupAggregate(
                    i => i,
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    (g, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13) => g.Key);
            return ((MethodCallExpression)groupAgg.Body).Method.GetGenericMethodDefinition()
                .MakeGenericMethod(
                    inputGroupType, inputPayloadType, newGroupType,
                    state1Type, result1Type,
                    state2Type, result2Type,
                    state3Type, result3Type,
                    state4Type, result4Type,
                    state5Type, result5Type,
                    state6Type, result6Type,
                    state7Type, result7Type,
                    state8Type, result8Type,
                    state9Type, result9Type,
                    state10Type, result10Type,
                    state11Type, result11Type,
                    state12Type, result12Type,
                    state13Type, result13Type,
                    overallResultType);
        }

        private static MethodInfo GenerateMethodInfoForGroupAggregate14(
            Type inputGroupType, Type inputPayloadType, Type newGroupType,
            Type state1Type, Type result1Type,
            Type state2Type, Type result2Type,
            Type state3Type, Type result3Type,
            Type state4Type, Type result4Type,
            Type state5Type, Type result5Type,
            Type state6Type, Type result6Type,
            Type state7Type, Type result7Type,
            Type state8Type, Type result8Type,
            Type state9Type, Type result9Type,
            Type state10Type, Type result10Type,
            Type state11Type, Type result11Type,
            Type state12Type, Type result12Type,
            Type state13Type, Type result13Type,
            Type state14Type, Type result14Type,
            Type overallResultType)
        {
            Expression<Func<IStreamable<Empty, int>, IStreamable<Empty, int>>> groupAgg =
                o => o.GroupAggregate(
                    i => i,
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    (g, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14) => g.Key);
            return ((MethodCallExpression)groupAgg.Body).Method.GetGenericMethodDefinition()
                .MakeGenericMethod(
                    inputGroupType, inputPayloadType, newGroupType,
                    state1Type, result1Type,
                    state2Type, result2Type,
                    state3Type, result3Type,
                    state4Type, result4Type,
                    state5Type, result5Type,
                    state6Type, result6Type,
                    state7Type, result7Type,
                    state8Type, result8Type,
                    state9Type, result9Type,
                    state10Type, result10Type,
                    state11Type, result11Type,
                    state12Type, result12Type,
                    state13Type, result13Type,
                    state14Type, result14Type,
                    overallResultType);
        }

        private static MethodInfo GenerateMethodInfoForGroupAggregate15(
            Type inputGroupType, Type inputPayloadType, Type newGroupType,
            Type state1Type, Type result1Type,
            Type state2Type, Type result2Type,
            Type state3Type, Type result3Type,
            Type state4Type, Type result4Type,
            Type state5Type, Type result5Type,
            Type state6Type, Type result6Type,
            Type state7Type, Type result7Type,
            Type state8Type, Type result8Type,
            Type state9Type, Type result9Type,
            Type state10Type, Type result10Type,
            Type state11Type, Type result11Type,
            Type state12Type, Type result12Type,
            Type state13Type, Type result13Type,
            Type state14Type, Type result14Type,
            Type state15Type, Type result15Type,
            Type overallResultType)
        {
            Expression<Func<IStreamable<Empty, int>, IStreamable<Empty, int>>> groupAgg =
                o => o.GroupAggregate(
                    i => i,
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    new CountAggregate<int>(),
                    (g, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15) => g.Key);
            return ((MethodCallExpression)groupAgg.Body).Method.GetGenericMethodDefinition()
                .MakeGenericMethod(
                    inputGroupType, inputPayloadType, newGroupType,
                    state1Type, result1Type,
                    state2Type, result2Type,
                    state3Type, result3Type,
                    state4Type, result4Type,
                    state5Type, result5Type,
                    state6Type, result6Type,
                    state7Type, result7Type,
                    state8Type, result8Type,
                    state9Type, result9Type,
                    state10Type, result10Type,
                    state11Type, result11Type,
                    state12Type, result12Type,
                    state13Type, result13Type,
                    state14Type, result14Type,
                    state15Type, result15Type,
                    overallResultType);
        }

        private Expression VisitSelectCallForGroupBy(Expression argument, Type outputElementType, LambdaExpression selectExpression)
        {
            var methodExpression = (MethodCallExpression)argument;
            var constructor = GroupByFirstPassVisitor.CreateConstructorFromSelect(selectExpression);
            var rewritten = GroupBySecondPassVisitor.CreateAggregateProfile(
                constructor,
                out var createdAggregates,
                out var stateTypes,
                out var resultTypes);

            if (createdAggregates.Count > 0)
            {
                var baseExpressionConstant = (ConstantExpression)methodExpression.Arguments[0];
                var keySelector = (ConstantExpression)methodExpression.Arguments[1];
                var elementSelector = (ConstantExpression)methodExpression.Arguments[2];

                // Need to apply the element selector to each aggregate via the Wrap method
                // Wrap has four generic type arguments: new agg's input, old agg's input, state, result
                var keySelectorExpression = (LambdaExpression)keySelector.Value;
                var elementSelectorExpression = (LambdaExpression)elementSelector.Value;

                if (!elementSelectorExpression.Body.ExpressionEquals(elementSelectorExpression.Parameters[0]))
                {
                    // We have an non-identity function for the element selector, so we need to wrap inputs
                    for (int i = 0; i < createdAggregates.Count; i++)
                    {
                        createdAggregates[i] = Expression.Call(
                            null, // static extension method
                            GenerateWrapMethodInfo(
                                elementSelectorExpression.Parameters[0].Type,
                                elementSelectorExpression.Body.Type,
                                stateTypes[i],
                                resultTypes[i]),
                            createdAggregates[i],
                            elementSelector);
                    }
                }

                switch (createdAggregates.Count)
                {
                    case 1:
                        return Expression.Call(
                            null, // static extension method
                            GenerateMethodInfoForGroupAggregate1(
                                typeof(Empty), elementSelectorExpression.Parameters[0].Type, keySelectorExpression.Body.Type,
                                stateTypes[0], resultTypes[0],
                                outputElementType),
                            Visit(baseExpressionConstant).Yield().Concat(
                                keySelector.Yield()).Concat(
                                createdAggregates).Concat(
                                rewritten.Yield()).ToArray());
                    case 2:
                        return Expression.Call(
                            null, // static extension method
                            GenerateMethodInfoForGroupAggregate2(
                                typeof(Empty), elementSelectorExpression.Parameters[0].Type, keySelectorExpression.Body.Type,
                                stateTypes[0], resultTypes[0],
                                stateTypes[1], resultTypes[1],
                                outputElementType),
                            Visit(baseExpressionConstant).Yield().Concat(
                                keySelector.Yield()).Concat(
                                createdAggregates).Concat(
                                rewritten.Yield()).ToArray());
                    case 3:
                        return Expression.Call(
                            null, // static extension method
                            GenerateMethodInfoForGroupAggregate3(
                                typeof(Empty), elementSelectorExpression.Parameters[0].Type, keySelectorExpression.Body.Type,
                                stateTypes[0], resultTypes[0],
                                stateTypes[1], resultTypes[1],
                                stateTypes[2], resultTypes[2],
                                outputElementType),
                            Visit(baseExpressionConstant).Yield().Concat(
                                keySelector.Yield()).Concat(
                                createdAggregates).Concat(
                                rewritten.Yield()).ToArray());
                    case 4:
                        return Expression.Call(
                            null, // static extension method
                            GenerateMethodInfoForGroupAggregate4(
                                typeof(Empty), elementSelectorExpression.Parameters[0].Type, keySelectorExpression.Body.Type,
                                stateTypes[0], resultTypes[0],
                                stateTypes[1], resultTypes[1],
                                stateTypes[2], resultTypes[2],
                                stateTypes[3], resultTypes[3],
                                outputElementType),
                            Visit(baseExpressionConstant).Yield().Concat(
                                keySelector.Yield()).Concat(
                                createdAggregates).Concat(
                                rewritten.Yield()).ToArray());
                    case 5:
                        return Expression.Call(
                            null, // static extension method
                            GenerateMethodInfoForGroupAggregate5(
                                typeof(Empty), elementSelectorExpression.Parameters[0].Type, keySelectorExpression.Body.Type,
                                stateTypes[0], resultTypes[0],
                                stateTypes[1], resultTypes[1],
                                stateTypes[2], resultTypes[2],
                                stateTypes[3], resultTypes[3],
                                stateTypes[4], resultTypes[4],
                                outputElementType),
                            Visit(baseExpressionConstant).Yield().Concat(
                                keySelector.Yield()).Concat(
                                createdAggregates).Concat(
                                rewritten.Yield()).ToArray());
                    case 6:
                        return Expression.Call(
                            null, // static extension method
                            GenerateMethodInfoForGroupAggregate6(
                                typeof(Empty), elementSelectorExpression.Parameters[0].Type, keySelectorExpression.Body.Type,
                                stateTypes[0], resultTypes[0],
                                stateTypes[1], resultTypes[1],
                                stateTypes[2], resultTypes[2],
                                stateTypes[3], resultTypes[3],
                                stateTypes[4], resultTypes[4],
                                stateTypes[5], resultTypes[5],
                                outputElementType),
                            Visit(baseExpressionConstant).Yield().Concat(
                                keySelector.Yield()).Concat(
                                createdAggregates).Concat(
                                rewritten.Yield()).ToArray());
                    case 7:
                        return Expression.Call(
                            null, // static extension method
                            GenerateMethodInfoForGroupAggregate7(
                                typeof(Empty), elementSelectorExpression.Parameters[0].Type, keySelectorExpression.Body.Type,
                                stateTypes[0], resultTypes[0],
                                stateTypes[1], resultTypes[1],
                                stateTypes[2], resultTypes[2],
                                stateTypes[3], resultTypes[3],
                                stateTypes[4], resultTypes[4],
                                stateTypes[5], resultTypes[5],
                                stateTypes[6], resultTypes[6],
                                outputElementType),
                            Visit(baseExpressionConstant).Yield().Concat(
                                keySelector.Yield()).Concat(
                                createdAggregates).Concat(
                                rewritten.Yield()).ToArray());
                    case 8:
                        return Expression.Call(
                            null, // static extension method
                            GenerateMethodInfoForGroupAggregate8(
                                typeof(Empty), elementSelectorExpression.Parameters[0].Type, keySelectorExpression.Body.Type,
                                stateTypes[0], resultTypes[0],
                                stateTypes[1], resultTypes[1],
                                stateTypes[2], resultTypes[2],
                                stateTypes[3], resultTypes[3],
                                stateTypes[4], resultTypes[4],
                                stateTypes[5], resultTypes[5],
                                stateTypes[6], resultTypes[6],
                                stateTypes[7], resultTypes[7],
                                outputElementType),
                            Visit(baseExpressionConstant).Yield().Concat(
                                keySelector.Yield()).Concat(
                                createdAggregates).Concat(
                                rewritten.Yield()).ToArray());
                    case 9:
                        return Expression.Call(
                            null, // static extension method
                            GenerateMethodInfoForGroupAggregate9(
                                typeof(Empty), elementSelectorExpression.Parameters[0].Type, keySelectorExpression.Body.Type,
                                stateTypes[0], resultTypes[0],
                                stateTypes[1], resultTypes[1],
                                stateTypes[2], resultTypes[2],
                                stateTypes[3], resultTypes[3],
                                stateTypes[4], resultTypes[4],
                                stateTypes[5], resultTypes[5],
                                stateTypes[6], resultTypes[6],
                                stateTypes[7], resultTypes[7],
                                stateTypes[8], resultTypes[8],
                                outputElementType),
                            Visit(baseExpressionConstant).Yield().Concat(
                                keySelector.Yield()).Concat(
                                createdAggregates).Concat(
                                rewritten.Yield()).ToArray());
                    case 10:
                        return Expression.Call(
                            null, // static extension method
                            GenerateMethodInfoForGroupAggregate10(
                                typeof(Empty), elementSelectorExpression.Parameters[0].Type, keySelectorExpression.Body.Type,
                                stateTypes[0], resultTypes[0],
                                stateTypes[1], resultTypes[1],
                                stateTypes[2], resultTypes[2],
                                stateTypes[3], resultTypes[3],
                                stateTypes[4], resultTypes[4],
                                stateTypes[5], resultTypes[5],
                                stateTypes[6], resultTypes[6],
                                stateTypes[7], resultTypes[7],
                                stateTypes[8], resultTypes[8],
                                stateTypes[9], resultTypes[9],
                                outputElementType),
                            Visit(baseExpressionConstant).Yield().Concat(
                                keySelector.Yield()).Concat(
                                createdAggregates).Concat(
                                rewritten.Yield()).ToArray());
                    case 11:
                        return Expression.Call(
                            null, // static extension method
                            GenerateMethodInfoForGroupAggregate11(
                                typeof(Empty), elementSelectorExpression.Parameters[0].Type, keySelectorExpression.Body.Type,
                                stateTypes[0], resultTypes[0],
                                stateTypes[1], resultTypes[1],
                                stateTypes[2], resultTypes[2],
                                stateTypes[3], resultTypes[3],
                                stateTypes[4], resultTypes[4],
                                stateTypes[5], resultTypes[5],
                                stateTypes[6], resultTypes[6],
                                stateTypes[7], resultTypes[7],
                                stateTypes[8], resultTypes[8],
                                stateTypes[9], resultTypes[9],
                                stateTypes[10], resultTypes[10],
                                outputElementType),
                            Visit(baseExpressionConstant).Yield().Concat(
                                keySelector.Yield()).Concat(
                                createdAggregates).Concat(
                                rewritten.Yield()).ToArray());
                    case 12:
                        return Expression.Call(
                            null, // static extension method
                            GenerateMethodInfoForGroupAggregate12(
                                typeof(Empty), elementSelectorExpression.Parameters[0].Type, keySelectorExpression.Body.Type,
                                stateTypes[0], resultTypes[0],
                                stateTypes[1], resultTypes[1],
                                stateTypes[2], resultTypes[2],
                                stateTypes[3], resultTypes[3],
                                stateTypes[4], resultTypes[4],
                                stateTypes[5], resultTypes[5],
                                stateTypes[6], resultTypes[6],
                                stateTypes[7], resultTypes[7],
                                stateTypes[8], resultTypes[8],
                                stateTypes[9], resultTypes[9],
                                stateTypes[10], resultTypes[10],
                                stateTypes[11], resultTypes[11],
                                outputElementType),
                            Visit(baseExpressionConstant).Yield().Concat(
                                keySelector.Yield()).Concat(
                                createdAggregates).Concat(
                                rewritten.Yield()).ToArray());
                    case 13:
                        return Expression.Call(
                            null, // static extension method
                            GenerateMethodInfoForGroupAggregate13(
                                typeof(Empty), elementSelectorExpression.Parameters[0].Type, keySelectorExpression.Body.Type,
                                stateTypes[0], resultTypes[0],
                                stateTypes[1], resultTypes[1],
                                stateTypes[2], resultTypes[2],
                                stateTypes[3], resultTypes[3],
                                stateTypes[4], resultTypes[4],
                                stateTypes[5], resultTypes[5],
                                stateTypes[6], resultTypes[6],
                                stateTypes[7], resultTypes[7],
                                stateTypes[8], resultTypes[8],
                                stateTypes[9], resultTypes[9],
                                stateTypes[10], resultTypes[10],
                                stateTypes[11], resultTypes[11],
                                stateTypes[12], resultTypes[12],
                                outputElementType),
                            Visit(baseExpressionConstant).Yield().Concat(
                                keySelector.Yield()).Concat(
                                createdAggregates).Concat(
                                rewritten.Yield()).ToArray());
                    case 14:
                        return Expression.Call(
                            null, // static extension method
                            GenerateMethodInfoForGroupAggregate14(
                                typeof(Empty), elementSelectorExpression.Parameters[0].Type, keySelectorExpression.Body.Type,
                                stateTypes[0], resultTypes[0],
                                stateTypes[1], resultTypes[1],
                                stateTypes[2], resultTypes[2],
                                stateTypes[3], resultTypes[3],
                                stateTypes[4], resultTypes[4],
                                stateTypes[5], resultTypes[5],
                                stateTypes[6], resultTypes[6],
                                stateTypes[7], resultTypes[7],
                                stateTypes[8], resultTypes[8],
                                stateTypes[9], resultTypes[9],
                                stateTypes[10], resultTypes[10],
                                stateTypes[11], resultTypes[11],
                                stateTypes[12], resultTypes[12],
                                stateTypes[13], resultTypes[13],
                                outputElementType),
                            Visit(baseExpressionConstant).Yield().Concat(
                                keySelector.Yield()).Concat(
                                createdAggregates).Concat(
                                rewritten.Yield()).ToArray());
                    case 15:
                        return Expression.Call(
                            null, // static extension method
                            GenerateMethodInfoForGroupAggregate15(
                                typeof(Empty), elementSelectorExpression.Parameters[0].Type, keySelectorExpression.Body.Type,
                                stateTypes[0], resultTypes[0],
                                stateTypes[1], resultTypes[1],
                                stateTypes[2], resultTypes[2],
                                stateTypes[3], resultTypes[3],
                                stateTypes[4], resultTypes[4],
                                stateTypes[5], resultTypes[5],
                                stateTypes[6], resultTypes[6],
                                stateTypes[7], resultTypes[7],
                                stateTypes[8], resultTypes[8],
                                stateTypes[9], resultTypes[9],
                                stateTypes[10], resultTypes[10],
                                stateTypes[11], resultTypes[11],
                                stateTypes[12], resultTypes[12],
                                stateTypes[13], resultTypes[13],
                                stateTypes[14], resultTypes[14],
                                outputElementType),
                            Visit(baseExpressionConstant).Yield().Concat(
                                keySelector.Yield()).Concat(
                                createdAggregates).Concat(
                                rewritten.Yield()).ToArray());
                }
            }

            // Fallback: Simply fuse the select operation into the GroupAggregate result constructor
            var groupAggregateExpression = (MethodCallExpression)Visit(argument);
            var genericMethodArguments = groupAggregateExpression.Method.GetGenericArguments();
            genericMethodArguments[genericMethodArguments.Length - 1] = outputElementType;
            var newGroupAggregateMethod = groupAggregateExpression.Method
                .GetGenericMethodDefinition().MakeGenericMethod(genericMethodArguments);

            return Expression.Call(
                groupAggregateExpression.Object,
                newGroupAggregateMethod,
                groupAggregateExpression.Arguments[0],
                groupAggregateExpression.Arguments[1],
                groupAggregateExpression.Arguments[2],
                Expression.Constant(constructor));
        }
    }
}