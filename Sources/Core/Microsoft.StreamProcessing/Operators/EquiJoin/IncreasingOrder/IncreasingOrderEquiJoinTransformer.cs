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
    internal partial class IncreasingOrderEquiJoinTemplate
    {
        private static int IOOEJSequenceNumber = 0;
        private Type keyType;
        private Type leftType;
        private Type rightType;
        private Type resultType;
        private string TKey;
        private string TLeft;
        private string TRight;
        private string TResult;
        private string className;
        private string BatchGeneratedFrom_TKey_TLeft;
        private string TKeyTLeftGenericParameters;
        private string BatchGeneratedFrom_TKey_TRight;
        private string TKeyTRightGenericParameters;
        private string BatchGeneratedFrom_TKey_TResult;
        private string TKeyTResultGenericParameters;
        private IEnumerable<MyFieldInfo> outputFields;

        private Func<string, string, string, string> leftBatchSelector;
        private Func<string, string, string, string> rightBatchSelector;

        private Func<string, string, string> joinKeyOrderComparer;

        private string genericParameters = string.Empty; // BUGBUG
        private string getOutputBatch;
        private string staticCtor;
        private IEnumerable<MyFieldInfo> leftFields;
        private IEnumerable<MyFieldInfo> rightFields;
        private ColumnarRepresentation leftMessageRepresentation;
        private ColumnarRepresentation rightMessageRepresentation;

        private IncreasingOrderEquiJoinTemplate() { }

        /// <summary>
        /// Generate a batch class definition to be used as StartEdgeEquiJoin operator.
        /// Compile the definition, dynamically load the assembly containing it, and return the Type representing the
        /// aggregate class.
        /// </summary>
        /// <typeparam name="TKey">The key type for both sides.</typeparam>
        /// <typeparam name="TLeft">The payload type for the left side.</typeparam>
        /// <typeparam name="TRight">The payload type for the right side.</typeparam>
        /// <typeparam name="TResult">The payload type for the resulting stream.</typeparam>
        /// <returns>
        /// A type that is defined to be a subtype of BinaryPipe&lt;<typeparamref name="TKey"/>,<typeparamref name="TLeft"/>, <typeparamref name="TRight"/>, <typeparamref name="TKey"/>, <typeparamref name="TResult"/>&gt;.
        /// </returns>
        internal static Tuple<Type, string> Generate<TKey, TLeft, TRight, TResult>(
            BinaryStreamable<TKey, TLeft, TRight, TResult> stream,
            Expression<Func<TLeft, TRight, TResult>> selector)
        {
            Contract.Requires(stream != null);
            Contract.Ensures(Contract.Result<Tuple<Type, string>>() == null || typeof(BinaryPipe<TKey, TLeft, TRight, TResult>).GetTypeInfo().IsAssignableFrom(Contract.Result<Tuple<Type, string>>().Item1));

            string errorMessages = null;
            try
            {
                var template = new IncreasingOrderEquiJoinTemplate();

                var keyType = template.keyType = typeof(TKey);
                var leftType = template.leftType = typeof(TLeft);
                var rightType = template.rightType = typeof(TRight);
                var resultType = template.resultType = typeof(TResult);

                template.TKey = keyType.GetCSharpSourceSyntax();
                template.TLeft = leftType.GetCSharpSourceSyntax();
                template.TRight = rightType.GetCSharpSourceSyntax();
                template.TResult = resultType.GetCSharpSourceSyntax(); // BUGBUG: need to get any generic parameters needed

                template.className = string.Format("GeneratedIncreasingOrderEquiJoin_{0}", IOOEJSequenceNumber++);

                template.leftMessageRepresentation = new ColumnarRepresentation(leftType);
                template.leftFields = template.leftMessageRepresentation.AllFields;
                template.rightMessageRepresentation = new ColumnarRepresentation(rightType);
                template.rightFields = template.rightMessageRepresentation.AllFields;
                var outputMessageRepresentation = new ColumnarRepresentation(resultType);

                var leftMessageType = StreamMessageManager.GetStreamMessageType<TKey, TLeft>();
                var rightMessageType = StreamMessageManager.GetStreamMessageType<TKey, TRight>();

                var resultRepresentation = outputMessageRepresentation;

                #region Key Comparer
                var keyComparer = stream.Left.Properties.KeyComparer.GetCompareExpr();
                if (!ConstantExpressionFinder.IsClosedExpression(keyComparer))
                    return null;
                template.joinKeyOrderComparer =
                    (left, right) =>
                        keyComparer.Inline(left, right);
                #endregion

                template.BatchGeneratedFrom_TKey_TLeft = Transformer.GetBatchClassName(keyType, leftType);
                template.TKeyTLeftGenericParameters = string.Empty; // BUGBUG

                template.BatchGeneratedFrom_TKey_TRight = Transformer.GetBatchClassName(keyType, rightType);
                template.TKeyTRightGenericParameters = string.Empty; // BUGBUG

                template.BatchGeneratedFrom_TKey_TResult = Transformer.GetBatchClassName(keyType, resultType);
                template.TKeyTResultGenericParameters = string.Empty; // BUGBUG

                template.outputFields = resultRepresentation.AllFields;

                if (!ConstantExpressionFinder.IsClosedExpression(selector))
                    return null;
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

                template.getOutputBatch = string.Format(
                    "pool.Get(out genericOutputBatch); output = ({0}{1})genericOutputBatch;",
                    Transformer.GetBatchClassName(keyType, resultType),
                    template.TKeyTResultGenericParameters);

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
                var t = a.GetType(template.className);
                if (t.GetTypeInfo().IsGenericType)
                {
                    var list = keyType.GetAnonymousTypes();
                    list.AddRange(leftType.GetAnonymousTypes());
                    list.AddRange(rightType.GetAnonymousTypes());
                    list.AddRange(resultType.GetAnonymousTypes());
                    t = t.MakeGenericType(list.ToArray());
                }
                return Tuple.Create(t, errorMessages);
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
