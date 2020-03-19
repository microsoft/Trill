// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Microsoft.StreamProcessing
{
    internal partial class SelectManyTemplate
    {
        private static int sequenceNumber = 0;

        /// <summary>
        /// All public instance fields of type TResult.
        /// Equal to the union of computedFields and swingingFields.
        /// REVIEW: Is this true? Can there be public fields that just aren't mentioned in the projection?
        /// </summary>
        private IEnumerable<MyFieldInfo> resultFields;
        private ColumnarRepresentation resultPayloadRepresentation;

        private string transformedSelectorAsSource;

        private string StartEdgeParameterName;
        private bool hasKey;

        /// <summary>
        /// Subset of resultFields that are computed a row at a time.
        /// For each computed field, f, the table maps f to the the C# source for the value that is assigned to f for each row.
        /// </summary>
        private IDictionary<MyFieldInfo, Expression> computedFields = new Dictionary<MyFieldInfo, Expression>();

        /// <summary>
        /// Needed when the projection function is optimized to a single select, but cannot be split into separate assignments to the columns
        /// of the result batch.
        /// </summary>
        private Expression projectionReturningResultInstance = null;

        private bool useEnumerator;
        private string loopCounter;
        private bool enumerableRepeatSelector;
        private string genericParameters;
        private int numberOfGenericParameters;
        private string TKeyTResultGenericParameters;
        private string MemoryPoolGenericParameters;
        private string keyParameterName;

        private SelectManyTemplate(string className, Type keyType, Type payloadType, Type resultType)
            : base(className, keyType, payloadType, resultType) { }

        public static Tuple<Type, string> Generate<TKey, TPayload, TResult>(SelectManyStreamable<TKey, TPayload, TResult> stream)
        {
            Contract.Ensures(Contract.Result<Tuple<Type, string>>() == null || typeof(UnaryPipe<TKey, TPayload, TResult>).GetTypeInfo().IsAssignableFrom(Contract.Result<Tuple<Type, string>>().Item1));

            string generatedClassName;
            string expandedCode;
            string errorMessages = null;
            try
            {
                generatedClassName = $"SelectMany_{sequenceNumber++}";
                var keyType = typeof(TKey);
                var payloadType = typeof(TPayload);
                var resultType = typeof(TResult);
                var template = new SelectManyTemplate(generatedClassName, keyType, payloadType, resultType);

                var tm = new TypeMapper(keyType, payloadType, resultType);
                var gps = tm.GenericTypeVariables(keyType, payloadType, resultType);
                template.genericParameters = gps.BracketedCommaSeparatedString();
                template.numberOfGenericParameters = gps.Count();
                template.TKeyTResultGenericParameters = tm.GenericTypeVariables(keyType, resultType).BracketedCommaSeparatedString();
                template.MemoryPoolGenericParameters = $"<{template.TKey}, {template.TResult}>";
                if (resultType == typeof(int) || resultType == typeof(long) || resultType == typeof(string))
                    template.MemoryPoolGenericParameters = string.Empty;

                var payloadParameterIndex = 0;
                if (stream.HasKey && stream.HasStartEdge) payloadParameterIndex = 2;
                else if (stream.HasKey || stream.HasStartEdge) payloadParameterIndex = 1;

                var selector = stream.Selector;
                var payloadParameter = selector.Parameters.ElementAt(payloadParameterIndex);

                template.resultPayloadRepresentation = new ColumnarRepresentation(resultType);
                template.resultFields = template.resultPayloadRepresentation.AllFields;

                if (template.numberOfGenericParameters > 0)
                    generatedClassName = generatedClassName + "`" + template.numberOfGenericParameters.ToString(CultureInfo.InvariantCulture);

                expandedCode = string.Empty;

                Expression transformedSelector = selector;

                // No substitutions are made for the start edge parameter or key parameter. Both just remain in the
                // body of the result selector and are set as local variables in the generated code.
                var keyParameterIndex = stream.HasStartEdge ? 1 : 0;

                var tuple = OptimizeSelectMany(selector.Body);
                if (tuple != null)
                {
                    template.enumerableRepeatSelector = true;
                    var resultSelector = stream.Selector;
                    var sourceMessageType = StreamMessageManager.GetStreamMessageType<TKey, TPayload>();
                    var pseudoLambdaParameters = new ParameterExpression[stream.HasKey ? 2 : 1];
                    var pseudoLambdaIndex = 0;
                    if (stream.HasKey) pseudoLambdaParameters[pseudoLambdaIndex++] = selector.Parameters[keyParameterIndex];
                    pseudoLambdaParameters[pseudoLambdaIndex] = payloadParameter;
                    var pseudoLambda = Expression.Lambda(tuple.Item1, pseudoLambdaParameters);

                    var parameterSubstitutions = new List<Tuple<ParameterExpression, SelectParameterInformation>>
                    {
                        Tuple.Create(payloadParameter, new SelectParameterInformation() { BatchName = "batch", BatchType = sourceMessageType, IndexVariableName = "i", parameterRepresentation = new ColumnarRepresentation(payloadType) })
                    };
                    var projectionResult = SelectTransformer.Transform(pseudoLambda, parameterSubstitutions, template.resultPayloadRepresentation, true, stream.HasStartEdge);
                    template.computedFields = projectionResult.ComputedFields;
                    template.projectionReturningResultInstance = projectionResult.ProjectionReturningResultInstance;
                    template.useEnumerator = false;
                    var loopCounter = tuple.Item2;
                    var newParameters = new ParameterExpression[stream.HasKey ? 2 : 1];
                    newParameters[0] = payloadParameter;
                    if (stream.HasKey) newParameters[1] = selector.Parameters[keyParameterIndex];

                    var loopCounterLambda = Expression.Lambda(loopCounter, payloadParameter);
                    var transformedLoopCounter = Extensions.TransformFunction<TKey, TPayload>(loopCounterLambda, 0);
                    template.loopCounter = transformedLoopCounter.Body.ExpressionToCSharp();

                    // REVIEW: Alternative: use Inline to replace occurrences of the key parameter
                    // with "batch.key.col[i]".
                    if (stream.HasKey)
                    {
                        template.keyParameterName = selector.Parameters[keyParameterIndex].Name;
                    }
                }
                else
                {
                    transformedSelector = Extensions.TransformFunction<TKey, TPayload>(stream.Selector, payloadParameterIndex).Body;

                    if (transformedSelector == null)
                    {
                        template.useEnumerator = true;
                        template.transformedSelectorAsSource = stream.Selector.ExpressionToCSharp();
                    }
                    else
                    {
                        var tuple2 = OptimizeSelectMany(transformedSelector);
                        if (tuple2 != null)
                        {
                            template.useEnumerator = false;
                            template.loopCounter = tuple2.Item2.ExpressionToCSharp();
                            template.transformedSelectorAsSource = tuple2.Item1.ExpressionToCSharp();
                        }
                        else
                        {
                            template.useEnumerator = true;
                            template.transformedSelectorAsSource = transformedSelector.ExpressionToCSharp();
                        }
                    }
                }

                template.StartEdgeParameterName = stream.HasStartEdge ? selector.Parameters.ElementAt(0).Name : null;
                template.hasKey = stream.HasKey;
                expandedCode = template.TransformText();

                var assemblyReferences = Transformer.AssemblyReferencesNeededFor(typeof(TKey), typeof(TPayload), typeof(TResult));
                assemblyReferences.Add(typeof(IStreamable<,>).GetTypeInfo().Assembly);
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TPayload>());
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TResult>());
                assemblyReferences.Add(Transformer.GeneratedMemoryPoolAssembly<TKey, TResult>());
                assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(stream.Selector));

                var a = Transformer.CompileSourceCode(expandedCode, assemblyReferences, out errorMessages);
                var t = a.GetType(generatedClassName);
                t = t.InstantiateAsNecessary(typeof(TKey), typeof(TPayload), typeof(TResult));
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

        private static Tuple<Expression, Expression> OptimizeSelectMany(Expression e)
        {
            if (!(e is MethodCallExpression methodCall)) return null;
            var method = methodCall.Method;
            if (!method.Name.Equals("Repeat") || !method.DeclaringType.Equals(typeof(Enumerable))) return null;
            return Tuple.Create(methodCall.Arguments[0], methodCall.Arguments[1]);
        }
    }
}
