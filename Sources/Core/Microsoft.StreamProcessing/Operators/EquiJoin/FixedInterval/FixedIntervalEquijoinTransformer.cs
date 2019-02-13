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
        private Type keyType;
        private Type leftType;
        private Type rightType;
        private Type resultType;
        private string TKey;
        private string TLeft;
        private string TRight;
        private string TResult;
        private string className;
        private string BatchGeneratedFrom_TKey_TResult;
        private string TKeyTResultGenericParameters = string.Empty;
        private string genericParameters;
        private IEnumerable<MyFieldInfo> leftFields;
        private IEnumerable<MyFieldInfo> rightFields;
        private IEnumerable<MyFieldInfo> resultFields;
        private string staticCtor;
        private Func<string, string, string> keyComparerEquals;
        private Func<string, string, string> leftComparerEquals;
        private Func<string, string, string> rightComparerEquals;
        private Func<string, string, string, string> leftBatchSelector;
        private Func<string, string, string, string> rightBatchSelector;
        private Func<string, string, string> activeSelector;
        private string LeftBatchType;
        private string RightBatchType;
        private ColumnarRepresentation leftMessageRepresentation;
        private ColumnarRepresentation rightMessageRepresentation;
        private string ActiveEventTypeLeft;
        private string ActiveEventTypeRight;

        private FixedIntervalEquiJoinTemplate() { }

        internal static Tuple<Type, string> Generate<TKey, TLeft, TRight, TResult>(
            BinaryStreamable<TKey, TLeft, TRight, TResult> stream,
            Expression<Func<TLeft, TRight, TResult>> selector)
        {
            Contract.Requires(stream != null);
            Contract.Ensures(Contract.Result<Tuple<Type, string>>() == null || typeof(BinaryPipe<TKey, TLeft, TRight, TResult>).GetTypeInfo().IsAssignableFrom(Contract.Result<Tuple<Type, string>>().Item1));

            string errorMessages = null;
            try
            {
                var template = new FixedIntervalEquiJoinTemplate();

                var keyType = template.keyType = typeof(TKey);
                var leftType = template.leftType = typeof(TLeft);
                var rightType = template.rightType = typeof(TRight);
                var resultType = template.resultType = typeof(TResult);

                var tm = new TypeMapper(keyType, leftType, rightType, resultType);
                template.TKey = tm.CSharpNameFor(keyType);
                template.TLeft = tm.CSharpNameFor(leftType);
                template.TRight = tm.CSharpNameFor(rightType);
                template.TResult = tm.CSharpNameFor(resultType);
                var keyAndLeftGenericParameters = tm.GenericTypeVariables(keyType, leftType).BracketedCommaSeparatedString();
                var keyAndRightGenericParameters = tm.GenericTypeVariables(keyType, rightType).BracketedCommaSeparatedString();
                template.TKeyTResultGenericParameters = tm.GenericTypeVariables(keyType, resultType).BracketedCommaSeparatedString();
                template.genericParameters = tm.GenericTypeVariables(keyType, leftType, rightType, resultType).BracketedCommaSeparatedString();

                template.className = string.Format("GeneratedFixedIntervalEquiJoin_{0}", EquiJoinSequenceNumber++);

                template.leftMessageRepresentation = new ColumnarRepresentation(leftType);
                template.rightMessageRepresentation = new ColumnarRepresentation(rightType);
                var resultMessageRepresentation = new ColumnarRepresentation(resultType);

                var batchGeneratedFrom_TKey_TLeft = Transformer.GetBatchClassName(keyType, leftType);
                var batchGeneratedFrom_TKey_TRight = Transformer.GetBatchClassName(keyType, rightType);
                template.BatchGeneratedFrom_TKey_TResult = Transformer.GetBatchClassName(keyType, resultType);

                template.LeftBatchType = batchGeneratedFrom_TKey_TLeft + keyAndLeftGenericParameters;
                template.RightBatchType = batchGeneratedFrom_TKey_TRight + keyAndRightGenericParameters;

                template.leftFields = template.leftMessageRepresentation.AllFields;
                template.rightFields = template.rightMessageRepresentation.AllFields;
                template.resultFields = resultMessageRepresentation.AllFields;

                template.ActiveEventTypeLeft = leftType.GetTypeInfo().IsValueType ? template.TLeft : "Active_Event_Left";
                template.ActiveEventTypeRight = rightType.GetTypeInfo().IsValueType ? template.TRight : "Active_Event_Right";

                #region Key Equals
                var keyComparer = stream.Properties.KeyEqualityComparer.GetEqualsExpr();
                template.keyComparerEquals =
                    (left, right) =>
                        keyComparer.Inline(left, right);
                if (keyType.IsAnonymousType())
                {
                    template.keyComparerEquals =
                        (left, right) => string.Format("keyComparerEquals({0}, {1})", left, right);
                }
                #endregion

                #region Left Payload Equals
                {
                    var leftPayloadComparer = stream.Left.Properties.PayloadEqualityComparer.GetEqualsExpr();
                    var newLambda = Extensions.TransformFunction<TKey, TLeft>(leftPayloadComparer, "leftIndex", 0);
                    template.leftComparerEquals = (left, right) => newLambda.Inline(left, right);
                }
                #endregion

                #region Right Payload Equals
                {
                    var rightPayloadComparer = stream.Right.Properties.PayloadEqualityComparer.GetEqualsExpr();
                    var newLambda = Extensions.TransformFunction<TKey, TRight>(rightPayloadComparer, "rightIndex", 0);
                    template.rightComparerEquals = (left, right) => newLambda.Inline(left, right);
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
                        var parameterSubsitutions = new List<Tuple<ParameterExpression, SelectParameterInformation>>()
                        {
                            Tuple.Create(selector.Parameters[0], new SelectParameterInformation() { BatchName = "leftBatch", BatchType = leftMessageType, IndexVariableName = "i", parameterRepresentation = template.leftMessageRepresentation, }),
                        };
                        var projectionResult = SelectTransformer.Transform(selector, parameterSubsitutions, resultMessageRepresentation, true);
                        if (projectionResult.Error)
                        {
                            errorMessages = "error while transforming the result selector";
                            throw new InvalidOperationException();
                        }
                        template.leftBatchSelector = (leftBatch, leftIndex, rightEvent) =>
                        {
                            var d = new Dictionary<ParameterExpression, string>
                            {
                                { Expression.Variable(leftMessageType, "leftBatch"), leftBatch },
                                { Expression.Variable(typeof(int), "i"), leftIndex },
                                { selector.Parameters[1], rightEvent }
                            };
                            var sb = new System.Text.StringBuilder();
                            sb.AppendLine("{");
                            foreach (var kv in projectionResult.ComputedFields)
                            {
                                var f = kv.Key;
                                var e = kv.Value;
                                if (f.OptimizeString())
                                {
                                    sb.AppendFormat(
                                        "output.{0}.AddString({1});\n",
                                        f.Name,
                                        e.ExpressionToCSharpStringWithParameterSubstitution(d));
                                }
                                else
                                {
                                    sb.AppendFormat(
                                        "output.{0}.col[index] = {1};\n",
                                        f.Name,
                                        e.ExpressionToCSharpStringWithParameterSubstitution(d));
                                }
                            }
                            sb.AppendLine("}");
                            return sb.ToString();
                        };
                    }
                    #endregion
                    #region RightBatchSelector
                    {
                        var parameterSubsitutions = new List<Tuple<ParameterExpression, SelectParameterInformation>>()
                        {
                            Tuple.Create(selector.Parameters[1], new SelectParameterInformation() { BatchName = "rightBatch", BatchType = rightMessageType, IndexVariableName = "j", parameterRepresentation = template.rightMessageRepresentation, }),
                        };
                        var projectionResult = SelectTransformer.Transform(selector, parameterSubsitutions, resultMessageRepresentation, true);
                        if (projectionResult.Error)
                        {
                            errorMessages = "error while transforming the result selector";
                            throw new InvalidOperationException();
                        }
                        template.rightBatchSelector = (leftEvent, rightBatch, rightIndex) =>
                        {
                            var d = new Dictionary<ParameterExpression, string>
                            {
                                { selector.Parameters[0], leftEvent },
                                { Expression.Variable(rightMessageType, "rightBatch"), rightBatch },
                                { Expression.Variable(typeof(int), "j"), rightIndex }
                            };
                            var sb = new System.Text.StringBuilder();
                            sb.AppendLine("{");
                            foreach (var kv in projectionResult.ComputedFields)
                            {
                                var f = kv.Key;
                                var e = kv.Value;
                                if (f.OptimizeString())
                                {
                                    sb.AppendFormat(
                                        "output.{0}.AddString({1});\n",
                                        f.Name,
                                        e.ExpressionToCSharpStringWithParameterSubstitution(d));
                                }
                                else
                                {
                                    sb.AppendFormat(
                                        "output.{0}.col[index] = {1};\n",
                                        f.Name,
                                        e.ExpressionToCSharpStringWithParameterSubstitution(d));
                                }
                            }
                            sb.AppendLine("}");
                            return sb.ToString();
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
                            var d = new Dictionary<ParameterExpression, string>
                            {
                                { selector.Parameters[0], leftEvent },
                                { selector.Parameters[1], rightEvent }
                            };
                            var sb = new System.Text.StringBuilder();
                            sb.AppendLine("{");
                            foreach (var kv in projectionResult.ComputedFields)
                            {
                                var f = kv.Key;
                                var e = kv.Value;
                                if (f.OptimizeString())
                                {
                                    sb.AppendFormat(
                                        "output.{0}.AddString({1});\n",
                                        f.Name,
                                        e.ExpressionToCSharpStringWithParameterSubstitution(d));
                                }
                                else
                                {
                                    sb.AppendFormat(
                                        "output.{0}.col[index] = {1};\n",
                                        f.Name,
                                        e.ExpressionToCSharpStringWithParameterSubstitution(d));
                                }
                            }
                            sb.AppendLine("}");
                            return sb.ToString();
                        };
                    }
                    #endregion
                }
                #endregion

                template.staticCtor = Transformer.StaticCtor(template.className);
                var expandedCode = template.TransformText();

                var assemblyReferences = Transformer.AssemblyReferencesNeededFor(keyType, leftType, rightType, resultType);
                assemblyReferences.Add(typeof(IStreamable<,>).GetTypeInfo().Assembly);
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TLeft>());
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TRight>());
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TResult>());

                var a = Transformer.CompileSourceCode(expandedCode, assemblyReferences, out errorMessages);
                if (keyType.IsAnonymousType())
                {
                    if (errorMessages == null) errorMessages = string.Empty;
                    errorMessages += "\nCodegen Warning: The key type for an equi-join is an anonymous type (or contains an anonymous type), preventing the inlining of the key equality and hashcode functions. This may lead to poor performance.\n";
                }
                var realClassName = template.className.AddNumberOfNecessaryGenericArguments(keyType, leftType, rightType, resultType);
                var t = a.GetType(realClassName);
                if (t.GetTypeInfo().IsGenericType)
                {
                    var list = keyType.GetAnonymousTypes();
                    list.AddRange(leftType.GetAnonymousTypes());
                    list.AddRange(rightType.GetAnonymousTypes());
                    list.AddRange(resultType.GetAnonymousTypes());
                    return Tuple.Create(t.MakeGenericType(list.ToArray()), errorMessages);
                }
                else
                {
                    return Tuple.Create(t, errorMessages);
                }
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
