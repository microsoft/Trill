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
        private Type keyType;
        private Type leftType;
        private Type rightType;
        private Type resultType;
        private string TKey;
        private string TLeft;
        private string TRight;
        private string TResult;
        private string className;
        private string genericParameters = string.Empty;
        private string BatchGeneratedFrom_TKey_TLeft;
        private string TKeyTLeftGenericParameters;
        private string BatchGeneratedFrom_TKey_TRight;
        private string TKeyTRightGenericParameters;
        private string BatchGeneratedFrom_TKey_TResult;
        private string TKeyTResultGenericParameters;
        private IEnumerable<MyFieldInfo> outputFields;
        private Func<string, string, string> keyComparer;
        private string staticCtor;
        private Func<string, string, string, string> leftBatchSelector;
        private Func<string, string, string, string> rightBatchSelector;
        private IEnumerable<MyFieldInfo> leftFields;
        private IEnumerable<MyFieldInfo> rightFields;
        private ColumnarRepresentation leftMessageRepresentation;
        private ColumnarRepresentation rightMessageRepresentation;

        private StartEdgeEquiJoinTemplate() { }

        internal static Tuple<Type, string> Generate<TKey, TLeft, TRight, TResult>(
            BinaryStreamable<TKey, TLeft, TRight, TResult> stream,
            Expression<Func<TLeft, TRight, TResult>> selector)
        {
            Contract.Requires(stream != null);
            Contract.Ensures(Contract.Result<Tuple<Type, string>>() == null || typeof(BinaryPipe<TKey, TLeft, TRight, TResult>).GetTypeInfo().IsAssignableFrom(Contract.Result<Tuple<Type, string>>().Item1));

            string errorMessages = null;
            try
            {
                var template = new StartEdgeEquiJoinTemplate();

                var keyType = template.keyType = typeof(TKey);
                var leftType = template.leftType = typeof(TLeft);
                var rightType = template.rightType = typeof(TRight);
                var resultType = template.resultType = typeof(TResult);

                var tm = new TypeMapper(keyType, leftType, rightType, resultType);
                template.TKey = tm.CSharpNameFor(keyType);
                template.TLeft = tm.CSharpNameFor(leftType);
                template.TRight = tm.CSharpNameFor(rightType);
                template.TResult = tm.CSharpNameFor(resultType);
                var gps = tm.GenericTypeVariables(keyType, leftType, rightType, resultType);
                template.genericParameters = gps.BracketedCommaSeparatedString();

                template.className = string.Format("GeneratedStartEdgeEquiJoin_{0}", StartEdgeEquiJoinSequenceNumber++);

                template.leftMessageRepresentation = new ColumnarRepresentation(leftType);
                template.leftFields = template.leftMessageRepresentation.AllFields;
                template.rightMessageRepresentation = new ColumnarRepresentation(rightType);
                template.rightFields = template.rightMessageRepresentation.AllFields;
                var outputMessageRepresentation = new ColumnarRepresentation(resultType);

                var resultRepresentation = outputMessageRepresentation;

                #region Key Comparer
                var keyComparer = stream.Properties.KeyEqualityComparer.GetEqualsExpr();
                template.keyComparer =
                    (left, right) =>
                        keyComparer.Inline(left, right);
                #endregion

                template.BatchGeneratedFrom_TKey_TLeft = Transformer.GetBatchClassName(keyType, leftType);
                template.TKeyTLeftGenericParameters = tm.GenericTypeVariables(keyType, leftType).BracketedCommaSeparatedString();

                template.BatchGeneratedFrom_TKey_TRight = Transformer.GetBatchClassName(keyType, rightType);
                template.TKeyTRightGenericParameters = tm.GenericTypeVariables(keyType, rightType).BracketedCommaSeparatedString();

                template.BatchGeneratedFrom_TKey_TResult = Transformer.GetBatchClassName(keyType, resultType);
                template.TKeyTResultGenericParameters = tm.GenericTypeVariables(keyType, resultType).BracketedCommaSeparatedString();

                template.outputFields = resultRepresentation.AllFields;

                var leftMessageType = StreamMessageManager.GetStreamMessageType<TKey, TLeft>();
                var rightMessageType = StreamMessageManager.GetStreamMessageType<TKey, TRight>();
                #region LeftBatchSelector
                {
                    var parameterSubsitutions = new List<Tuple<ParameterExpression, SelectParameterInformation>>()
                        {
                            Tuple.Create(selector.Parameters[0], new SelectParameterInformation() { BatchName = "leftBatch", BatchType = leftMessageType, IndexVariableName = "i", parameterRepresentation = template.leftMessageRepresentation, }),
                        };
                    var projectionResult = SelectTransformer.Transform(selector, parameterSubsitutions, resultRepresentation, true);
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
                    var projectionResult = SelectTransformer.Transform(selector, parameterSubsitutions, resultRepresentation, true);
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

                template.staticCtor = Transformer.StaticCtor(template.className);
                var expandedCode = template.TransformText();

                var assemblyReferences = Transformer.AssemblyReferencesNeededFor(typeof(TKey), typeof(TLeft), typeof(TRight), typeof(TResult));
                assemblyReferences.Add(typeof(IStreamable<,>).GetTypeInfo().Assembly);
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TLeft>());
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TRight>());
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TResult>());
                assemblyReferences.Add(Transformer.GeneratedMemoryPoolAssembly<TKey, TResult>());
                assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(selector));

                var a = Transformer.CompileSourceCode(expandedCode, assemblyReferences, out errorMessages);
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
