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
    internal partial class StartEdgeEquiJoinTemplate
    {
        private static int StartEdgeEquiJoinSequenceNumber = 0;
        private string genericParameters = string.Empty;
        private string BatchGeneratedFrom_TKey_TLeft;
        private string TKeyTLeftGenericParameters;
        private string BatchGeneratedFrom_TKey_TRight;
        private string TKeyTRightGenericParameters;
        private string BatchGeneratedFrom_TKey_TResult;
        private string TKeyTResultGenericParameters;
        private IEnumerable<MyFieldInfo> outputFields;
        private Func<string, string, string> keyComparer;
        private Func<string, string, string, string> leftBatchSelector;
        private Func<string, string, string, string> rightBatchSelector;
        private IEnumerable<MyFieldInfo> leftFields;
        private IEnumerable<MyFieldInfo> rightFields;
        private ColumnarRepresentation leftMessageRepresentation;
        private ColumnarRepresentation rightMessageRepresentation;

        private StartEdgeEquiJoinTemplate(string className, Type keyType, Type leftType, Type rightType, Type resultType)
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
                var template = new StartEdgeEquiJoinTemplate($"GeneratedStartEdgeEquiJoin_{StartEdgeEquiJoinSequenceNumber++}", typeof(TKey), typeof(TLeft), typeof(TRight), typeof(TResult));

                var gps = template.tm.GenericTypeVariables(template.keyType, template.leftType, template.rightType, template.resultType);
                template.genericParameters = gps.BracketedCommaSeparatedString();

                template.leftMessageRepresentation = new ColumnarRepresentation(template.leftType);
                template.leftFields = template.leftMessageRepresentation.AllFields;
                template.rightMessageRepresentation = new ColumnarRepresentation(template.rightType);
                template.rightFields = template.rightMessageRepresentation.AllFields;
                var resultRepresentation = new ColumnarRepresentation(template.resultType);

                #region Key Comparer
                var keyComparer = stream.Properties.KeyEqualityComparer.GetEqualsExpr();
                template.keyComparer =
                    (left, right) =>
                        keyComparer.Inline(left, right);
                #endregion

                template.BatchGeneratedFrom_TKey_TLeft = Transformer.GetBatchClassName(template.keyType, template.leftType);
                template.TKeyTLeftGenericParameters = template.tm.GenericTypeVariables(template.keyType, template.leftType).BracketedCommaSeparatedString();

                template.BatchGeneratedFrom_TKey_TRight = Transformer.GetBatchClassName(template.keyType, template.rightType);
                template.TKeyTRightGenericParameters = template.tm.GenericTypeVariables(template.keyType, template.rightType).BracketedCommaSeparatedString();

                template.BatchGeneratedFrom_TKey_TResult = Transformer.GetBatchClassName(template.keyType, template.resultType);
                template.TKeyTResultGenericParameters = template.tm.GenericTypeVariables(template.keyType, template.resultType).BracketedCommaSeparatedString();

                template.outputFields = resultRepresentation.AllFields;

                var leftMessageType = StreamMessageManager.GetStreamMessageType<TKey, TLeft>();
                var rightMessageType = StreamMessageManager.GetStreamMessageType<TKey, TRight>();
                #region LeftBatchSelector
                {
                    var leftBatchIndexVariable = selector.Parameters.GenerateFreshVariableName("i");
                    var parameterSubsitutions = new List<Tuple<ParameterExpression, SelectParameterInformation>>()
                        {
                            Tuple.Create(selector.Parameters[0], new SelectParameterInformation() { BatchName = "leftBatch", BatchType = leftMessageType, IndexVariableName = leftBatchIndexVariable, parameterRepresentation = template.leftMessageRepresentation, }),
                        };
                    var projectionResult = SelectTransformer.Transform(selector, parameterSubsitutions, resultRepresentation, true);
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
                    var projectionResult = SelectTransformer.Transform(selector, parameterSubsitutions, resultRepresentation, true);
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
