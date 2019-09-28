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
    internal sealed partial class GroupBySecondPassVisitor : ExpressionVisitor
    {
        private static MethodInfo GetMethodInfoForSumInt()
        {
            Expression<Func<IEnumerable<int>, int>> expression = (e) => e.Sum();
            return ((MethodCallExpression)expression.Body).Method;
        }

        private static Expression ExpressionForSumIntAggregate
        {
            get
            {
                Expression<Func<IAggregate<int, int, int>>> expression = () => new SumIntAggregate();
                return expression.Body;
            }
        }

        private static MethodInfo GetMethodInfoForNullableSumInt()
        {
            Expression<Func<IEnumerable<int?>, int?>> expression = (e) => e.Sum();
            return ((MethodCallExpression)expression.Body).Method;
        }

        private static Expression ExpressionForNullableSumIntAggregate
        {
            get
            {
                Expression<Func<IAggregate<int?, NullOutputWrapper<int>, int?>>> expression
                    = () => new SumIntAggregate().MakeInputNullableAndSkipNulls().MakeOutputNullableAndOutputNullWhenEmpty();
                return expression.Body;
            }
        }

        private static MethodInfo GetMethodInfoForAverageInt()
        {
            Expression<Func<IEnumerable<int>, double>> expression = (e) => e.Average();
            return ((MethodCallExpression)expression.Body).Method;
        }

        private static Expression ExpressionForAverageIntAggregate
        {
            get
            {
                Expression<Func<IAggregate<int, AverageState<long>, double>>> expression = () => new AverageIntAggregate();
                return expression.Body;
            }
        }

        private static MethodInfo GetMethodInfoForNullableAverageInt()
        {
            Expression<Func<IEnumerable<int?>, double?>> expression = (e) => e.Average();
            return ((MethodCallExpression)expression.Body).Method;
        }

        private static Expression ExpressionForNullableAverageIntAggregate
        {
            get
            {
                Expression<Func<IAggregate<int?, AverageState<long>, double?>>> expression
                    = () => new AverageNullableIntAggregate();
                return expression.Body;
            }
        }

        private static MethodInfo GetMethodInfoForSumLong()
        {
            Expression<Func<IEnumerable<long>, long>> expression = (e) => e.Sum();
            return ((MethodCallExpression)expression.Body).Method;
        }

        private static Expression ExpressionForSumLongAggregate
        {
            get
            {
                Expression<Func<IAggregate<long, long, long>>> expression = () => new SumLongAggregate();
                return expression.Body;
            }
        }

        private static MethodInfo GetMethodInfoForNullableSumLong()
        {
            Expression<Func<IEnumerable<long?>, long?>> expression = (e) => e.Sum();
            return ((MethodCallExpression)expression.Body).Method;
        }

        private static Expression ExpressionForNullableSumLongAggregate
        {
            get
            {
                Expression<Func<IAggregate<long?, NullOutputWrapper<long>, long?>>> expression
                    = () => new SumLongAggregate().MakeInputNullableAndSkipNulls().MakeOutputNullableAndOutputNullWhenEmpty();
                return expression.Body;
            }
        }

        private static MethodInfo GetMethodInfoForAverageLong()
        {
            Expression<Func<IEnumerable<long>, double>> expression = (e) => e.Average();
            return ((MethodCallExpression)expression.Body).Method;
        }

        private static Expression ExpressionForAverageLongAggregate
        {
            get
            {
                Expression<Func<IAggregate<long, AverageState<long>, double>>> expression = () => new AverageLongAggregate();
                return expression.Body;
            }
        }

        private static MethodInfo GetMethodInfoForNullableAverageLong()
        {
            Expression<Func<IEnumerable<long?>, double?>> expression = (e) => e.Average();
            return ((MethodCallExpression)expression.Body).Method;
        }

        private static Expression ExpressionForNullableAverageLongAggregate
        {
            get
            {
                Expression<Func<IAggregate<long?, AverageState<long>, double?>>> expression
                    = () => new AverageNullableLongAggregate();
                return expression.Body;
            }
        }

        private static MethodInfo GetMethodInfoForSumFloat()
        {
            Expression<Func<IEnumerable<float>, float>> expression = (e) => e.Sum();
            return ((MethodCallExpression)expression.Body).Method;
        }

        private static Expression ExpressionForSumFloatAggregate
        {
            get
            {
                Expression<Func<IAggregate<float, float, float>>> expression = () => new SumFloatAggregate();
                return expression.Body;
            }
        }

        private static MethodInfo GetMethodInfoForNullableSumFloat()
        {
            Expression<Func<IEnumerable<float?>, float?>> expression = (e) => e.Sum();
            return ((MethodCallExpression)expression.Body).Method;
        }

        private static Expression ExpressionForNullableSumFloatAggregate
        {
            get
            {
                Expression<Func<IAggregate<float?, NullOutputWrapper<float>, float?>>> expression
                    = () => new SumFloatAggregate().MakeInputNullableAndSkipNulls().MakeOutputNullableAndOutputNullWhenEmpty();
                return expression.Body;
            }
        }

        private static MethodInfo GetMethodInfoForAverageFloat()
        {
            Expression<Func<IEnumerable<float>, float>> expression = (e) => e.Average();
            return ((MethodCallExpression)expression.Body).Method;
        }

        private static Expression ExpressionForAverageFloatAggregate
        {
            get
            {
                Expression<Func<IAggregate<float, AverageState<float>, float>>> expression = () => new AverageFloatAggregate();
                return expression.Body;
            }
        }

        private static MethodInfo GetMethodInfoForNullableAverageFloat()
        {
            Expression<Func<IEnumerable<float?>, float?>> expression = (e) => e.Average();
            return ((MethodCallExpression)expression.Body).Method;
        }

        private static Expression ExpressionForNullableAverageFloatAggregate
        {
            get
            {
                Expression<Func<IAggregate<float?, AverageState<float>, float?>>> expression
                    = () => new AverageNullableFloatAggregate();
                return expression.Body;
            }
        }

        private static MethodInfo GetMethodInfoForSumDouble()
        {
            Expression<Func<IEnumerable<double>, double>> expression = (e) => e.Sum();
            return ((MethodCallExpression)expression.Body).Method;
        }

        private static Expression ExpressionForSumDoubleAggregate
        {
            get
            {
                Expression<Func<IAggregate<double, double, double>>> expression = () => new SumDoubleAggregate();
                return expression.Body;
            }
        }

        private static MethodInfo GetMethodInfoForNullableSumDouble()
        {
            Expression<Func<IEnumerable<double?>, double?>> expression = (e) => e.Sum();
            return ((MethodCallExpression)expression.Body).Method;
        }

        private static Expression ExpressionForNullableSumDoubleAggregate
        {
            get
            {
                Expression<Func<IAggregate<double?, NullOutputWrapper<double>, double?>>> expression
                    = () => new SumDoubleAggregate().MakeInputNullableAndSkipNulls().MakeOutputNullableAndOutputNullWhenEmpty();
                return expression.Body;
            }
        }

        private static MethodInfo GetMethodInfoForAverageDouble()
        {
            Expression<Func<IEnumerable<double>, double>> expression = (e) => e.Average();
            return ((MethodCallExpression)expression.Body).Method;
        }

        private static Expression ExpressionForAverageDoubleAggregate
        {
            get
            {
                Expression<Func<IAggregate<double, AverageState<double>, double>>> expression = () => new AverageDoubleAggregate();
                return expression.Body;
            }
        }

        private static MethodInfo GetMethodInfoForNullableAverageDouble()
        {
            Expression<Func<IEnumerable<double?>, double?>> expression = (e) => e.Average();
            return ((MethodCallExpression)expression.Body).Method;
        }

        private static Expression ExpressionForNullableAverageDoubleAggregate
        {
            get
            {
                Expression<Func<IAggregate<double?, AverageState<double>, double?>>> expression
                    = () => new AverageNullableDoubleAggregate();
                return expression.Body;
            }
        }

        private static MethodInfo GetMethodInfoForSumDecimal()
        {
            Expression<Func<IEnumerable<decimal>, decimal>> expression = (e) => e.Sum();
            return ((MethodCallExpression)expression.Body).Method;
        }

        private static Expression ExpressionForSumDecimalAggregate
        {
            get
            {
                Expression<Func<IAggregate<decimal, decimal, decimal>>> expression = () => new SumDecimalAggregate();
                return expression.Body;
            }
        }

        private static MethodInfo GetMethodInfoForNullableSumDecimal()
        {
            Expression<Func<IEnumerable<decimal?>, decimal?>> expression = (e) => e.Sum();
            return ((MethodCallExpression)expression.Body).Method;
        }

        private static Expression ExpressionForNullableSumDecimalAggregate
        {
            get
            {
                Expression<Func<IAggregate<decimal?, NullOutputWrapper<decimal>, decimal?>>> expression
                    = () => new SumDecimalAggregate().MakeInputNullableAndSkipNulls().MakeOutputNullableAndOutputNullWhenEmpty();
                return expression.Body;
            }
        }

        private static MethodInfo GetMethodInfoForAverageDecimal()
        {
            Expression<Func<IEnumerable<decimal>, decimal>> expression = (e) => e.Average();
            return ((MethodCallExpression)expression.Body).Method;
        }

        private static Expression ExpressionForAverageDecimalAggregate
        {
            get
            {
                Expression<Func<IAggregate<decimal, AverageState<decimal>, decimal>>> expression = () => new AverageDecimalAggregate();
                return expression.Body;
            }
        }

        private static MethodInfo GetMethodInfoForNullableAverageDecimal()
        {
            Expression<Func<IEnumerable<decimal?>, decimal?>> expression = (e) => e.Average();
            return ((MethodCallExpression)expression.Body).Method;
        }

        private static Expression ExpressionForNullableAverageDecimalAggregate
        {
            get
            {
                Expression<Func<IAggregate<decimal?, AverageState<decimal>, decimal?>>> expression
                    = () => new AverageNullableDecimalAggregate();
                return expression.Body;
            }
        }

    }
}
