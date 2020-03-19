// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;
using System.Reflection;

namespace Microsoft.StreamProcessing
{
    internal partial class FixedIntervalEquiJoinTemplate
    {
        private static int EquiJoinSequenceNumber = 0;
        private string BatchGeneratedFrom_TKey_TResult;
        private string TKeyTResultGenericParameters = string.Empty;
        private string genericParameters;
        private IEnumerable<MyFieldInfo> leftFields;
        private IEnumerable<MyFieldInfo> rightFields;
        private IEnumerable<MyFieldInfo> resultFields;
        private Func<string, string, string> keyComparerEquals;
        private Func<string, string, string, string> leftBatchSelector;
        private Func<string, string, string, string> rightBatchSelector;
        private Func<string, string, string> activeSelector;
        private string LeftBatchType;
        private string RightBatchType;
        private ColumnarRepresentation leftMessageRepresentation;
        private ColumnarRepresentation rightMessageRepresentation;
        private string ActiveEventTypeLeft;
        private string ActiveEventTypeRight;
        private long leftDuration;
        private long rightDuration;

        private FixedIntervalEquiJoinTemplate(string className, Type keyType, Type leftType, Type rightType, Type resultType)
            : base(className, keyType, leftType, rightType, resultType) { }

    internal static Tuple<Type, string> Generate<TKey, TLeft, TRight, TResult>(
            BinaryStreamable<TKey, TLeft, TRight, TResult> stream,
            Expression<Func<TLeft, TRight, TResult>> selector)
        {
            Contract.Requires(stream != null);
            Contract.Ensures(Contract.Result<Tuple<Type, string>>() == null || typeof(BinaryPipe<TKey, TLeft, TRight, TResult>).GetTypeInfo().IsAssignableFrom(Contract.Result<Tuple<Type, string>>().Item1));

            string errorMessages = null;
            try
            {
                var template = new FixedIntervalEquiJoinTemplate($"GeneratedFixedIntervalEquiJoin_{EquiJoinSequenceNumber++}", typeof(TKey), typeof(TLeft), typeof(TRight), typeof(TResult))
                {
                    leftDuration = stream.Left.Properties.ConstantDurationLength.Value,
                    rightDuration = stream.Right.Properties.ConstantDurationLength.Value
                };

                var keyAndLeftGenericParameters = template.tm.GenericTypeVariables(template.keyType, template.leftType).BracketedCommaSeparatedString();
                var keyAndRightGenericParameters = template.tm.GenericTypeVariables(template.keyType, template.rightType).BracketedCommaSeparatedString();
                template.TKeyTResultGenericParameters = template.tm.GenericTypeVariables(template.keyType, template.resultType).BracketedCommaSeparatedString();
                template.genericParameters = template.tm.GenericTypeVariables(template.keyType, template.leftType, template.rightType, template.resultType).BracketedCommaSeparatedString();

                template.leftMessageRepresentation = new ColumnarRepresentation(template.leftType);
                template.rightMessageRepresentation = new ColumnarRepresentation(template.rightType);
                var resultMessageRepresentation = new ColumnarRepresentation(template.resultType);

                var batchGeneratedFrom_TKey_TLeft = Transformer.GetBatchClassName(template.keyType, template.leftType);
                var batchGeneratedFrom_TKey_TRight = Transformer.GetBatchClassName(template.keyType, template.rightType);
                template.BatchGeneratedFrom_TKey_TResult = Transformer.GetBatchClassName(template.keyType, template.resultType);

                template.LeftBatchType = batchGeneratedFrom_TKey_TLeft + keyAndLeftGenericParameters;
                template.RightBatchType = batchGeneratedFrom_TKey_TRight + keyAndRightGenericParameters;

                template.leftFields = template.leftMessageRepresentation.AllFields;
                template.rightFields = template.rightMessageRepresentation.AllFields;
                template.resultFields = resultMessageRepresentation.AllFields;

                template.ActiveEventTypeLeft = template.leftType.GetTypeInfo().IsValueType ? template.TLeft : "Active_Event_Left";
                template.ActiveEventTypeRight = template.rightType.GetTypeInfo().IsValueType ? template.TRight : "Active_Event_Right";

                #region Key Equals
                var keyComparer = stream.Properties.KeyEqualityComparer.GetEqualsExpr();
                template.keyComparerEquals =
                    (left, right) =>
                        keyComparer.Inline(left, right);
                if (template.keyType.IsAnonymousType())
                {
                    template.keyComparerEquals =
                        (left, right) => $"keyComparerEquals({left}, {right})";
                }
                #endregion

                #region Result Selector
                {
                    var leftMessageType = StreamMessageManager.GetStreamMessageType<TKey, TLeft>();
                    var rightMessageType = StreamMessageManager.GetStreamMessageType<TKey, TRight>();

                    if (!ConstantExpressionFinder.IsClosedExpression(selector))
                    {
                        errorMessages = "result selector is not a closed expression";
                        throw new InvalidOperationException();
                    }

                    #region LeftBatchSelector
                    {
                        var leftBatchIndexVariable = selector.Parameters.GenerateFreshVariableName("i");
                        var parameterSubsitutions = new List<Tuple<ParameterExpression, SelectParameterInformation>>()
                        {
                            Tuple.Create(selector.Parameters[0], new SelectParameterInformation() { BatchName = "leftBatch", BatchType = leftMessageType, IndexVariableName = leftBatchIndexVariable, parameterRepresentation = template.leftMessageRepresentation, }),
                        };
                        var projectionResult = SelectTransformer.Transform(selector, parameterSubsitutions, resultMessageRepresentation, true);
                        if (projectionResult.Error)
                        {
                            errorMessages = "error while transforming the result selector";
                            throw new InvalidOperationException();
                        }
                        template.leftBatchSelector = (leftBatch, leftIndex, rightEvent) =>
                        {
                            var parameterMap = new Dictionary<ParameterExpression, string>
                            {
                                { Expression.Variable(leftMessageType, "leftBatch"), leftBatch },
                                { Expression.Variable(typeof(int), leftBatchIndexVariable), leftIndex },
                                { selector.Parameters[1], rightEvent }
                            };
                            if (projectionResult.ProjectionReturningResultInstance != null)
                            {
                                return $"this.output[index] = {projectionResult.ProjectionReturningResultInstance.ExpressionToCSharpStringWithParameterSubstitution(parameterMap)};";
                            }
                            else
                            {
                                var sb = new System.Text.StringBuilder();
                                sb.AppendLine("{");
                                foreach (var kv in projectionResult.ComputedFields)
                                {
                                    var f = kv.Key;
                                    var e = kv.Value;
                                    if (f.OptimizeString())
                                    {
                                        sb.AppendLine($"this.output.{f.Name}.AddString({e.ExpressionToCSharpStringWithParameterSubstitution(parameterMap)});");
                                    }
                                    else
                                    {
                                        sb.AppendLine($"this.output.{f.Name}.col[index] = {e.ExpressionToCSharpStringWithParameterSubstitution(parameterMap)};");
                                    }
                                }
                                sb.AppendLine("}");
                                return sb.ToString();
                            }
                        };
                    }
                    #endregion
                    #region RightBatchSelector
                    {
                        var rightBatchIndexVariable = selector.Parameters.GenerateFreshVariableName("j");
                        var parameterSubsitutions = new List<Tuple<ParameterExpression, SelectParameterInformation>>()
                        {
                            Tuple.Create(selector.Parameters[1], new SelectParameterInformation() { BatchName = "rightBatch", BatchType = rightMessageType, IndexVariableName = rightBatchIndexVariable, parameterRepresentation = template.rightMessageRepresentation, }),
                        };
                        var projectionResult = SelectTransformer.Transform(selector, parameterSubsitutions, resultMessageRepresentation, true);
                        if (projectionResult.Error)
                        {
                            errorMessages = "error while transforming the result selector";
                            throw new InvalidOperationException();
                        }
                        template.rightBatchSelector = (leftEvent, rightBatch, rightIndex) =>
                        {
                            var parameterMap = new Dictionary<ParameterExpression, string>
                            {
                                { selector.Parameters[0], leftEvent },
                                { Expression.Variable(rightMessageType, "rightBatch"), rightBatch },
                                { Expression.Variable(typeof(int), rightBatchIndexVariable), rightIndex }
                            };
                            if (projectionResult.ProjectionReturningResultInstance != null)
                            {
                                return $"this.output[index] = {projectionResult.ProjectionReturningResultInstance.ExpressionToCSharpStringWithParameterSubstitution(parameterMap)};";
                            }
                            else
                            {
                                var sb = new System.Text.StringBuilder();
                                sb.AppendLine("{");
                                foreach (var kv in projectionResult.ComputedFields)
                                {
                                    var f = kv.Key;
                                    var e = kv.Value;
                                    if (f.OptimizeString())
                                    {
                                        sb.AppendLine($"this.output.{f.Name}.AddString({e.ExpressionToCSharpStringWithParameterSubstitution(parameterMap)});");
                                    }
                                    else
                                    {
                                        sb.AppendLine($"this.output.{f.Name}.col[index] = {e.ExpressionToCSharpStringWithParameterSubstitution(parameterMap)};");
                                    }
                                }
                                sb.AppendLine("}");
                                return sb.ToString();
                            }
                        };
                    }
                    #endregion
                    #region ActiveSelector
                    {
                        var parameterSubsitutions = new List<Tuple<ParameterExpression, SelectParameterInformation>>();
                        var projectionResult = SelectTransformer.Transform(selector, parameterSubsitutions, resultMessageRepresentation, true);
                        if (projectionResult.Error)
                        {
                            errorMessages = "error while transforming the result selector";
                            throw new InvalidOperationException();
                        }
                        template.activeSelector = (leftEvent, rightEvent) =>
                        {
                            var parameterMap = new Dictionary<ParameterExpression, string>
                            {
                                { selector.Parameters[0], leftEvent },
                                { selector.Parameters[1], rightEvent }
                            };
                            if (projectionResult.ProjectionReturningResultInstance != null)
                            {
                                return $"this.output[index] = {projectionResult.ProjectionReturningResultInstance.ExpressionToCSharpStringWithParameterSubstitution(parameterMap)};";
                            }
                            else
                            {
                                var sb = new System.Text.StringBuilder();
                                sb.AppendLine("{");
                                foreach (var kv in projectionResult.ComputedFields)
                                {
                                    var f = kv.Key;
                                    var e = kv.Value;
                                    if (f.OptimizeString())
                                    {
                                        sb.AppendLine($"this.output.{f.Name}.AddString({e.ExpressionToCSharpStringWithParameterSubstitution(parameterMap)});");
                                    }
                                    else
                                    {
                                        sb.AppendLine($"this.output.{f.Name}.col[index] = {e.ExpressionToCSharpStringWithParameterSubstitution(parameterMap)};");
                                    }
                                }
                                sb.AppendLine("}");
                                return sb.ToString();
                            }
                        };
                    }
                    #endregion
                }
                #endregion

                return template.Generate<TKey, TLeft, TRight, TResult>(selector);
            }
            catch
            {
                if (Config.CodegenOptions.DontFallBackToRowBasedExecution)
                {
                    throw new InvalidOperationException("Code Generation failed when it wasn't supposed to!");
                }
                return Tuple.Create((Type)null, errorMessages);
            }
        }

    }
}
