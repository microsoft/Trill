// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.StreamProcessing.Internal;

namespace Microsoft.StreamProcessing
{
    internal partial class GroupedWindowTemplate
    {
        private static int GroupedAggregateSequenceNumber = 0;
        private Type keyType;
        private Type inputType;
        private Type stateType;
        private Type outputType;
        private Type resultType;
        private string BatchGeneratedFrom_Unit_TInput;
        private string UnitTInputGenericParameters;
        protected Func<string, string, string> finalResultSelector;
        protected Func<string, string, string> keyComparerEquals;
        protected Func<string, string> keyComparerGetHashCode;
        private string TResult;
        private string UnitTResultGenericParameters;
        private string keySelector;

        private IEnumerable<MyFieldInfo> inputFields;
        private IEnumerable<MyFieldInfo> outputFields;

        private string initialState;
        private Func<string, string, string> accumulate;
        private Func<string, string, string> deaccumulate;
        private Func<string, string, string> difference;
        private Func<string, string> computeResult;
        private bool useCompiledInitialState;
        private bool useCompiledComputeResult;
        private bool useCompiledAccumulate;
        private bool useCompiledDeaccumulate;
        private bool useCompiledDifference;
        private bool isUngrouped;

        private GroupedWindowTemplate(string className) : base(className) { }

        /// <summary>
        /// Generate a batch class definition to be used as an aggregate definition.
        /// Compile the definition, dynamically load the assembly containing it, and return the Type representing the
        /// aggregate class.
        /// </summary>
        /// <typeparam name="TKey">The key type for the aggregate.</typeparam>
        /// <typeparam name="TInput">The input type for the aggregate.</typeparam>
        /// <typeparam name="TOutput">The output type for the aggregate.</typeparam>
        /// <typeparam name="TState">The type for the accumulated state held by the aggregate.</typeparam>
        /// <typeparam name="TResult">The type of the result.</typeparam>
        /// <returns>
        /// A type that is defined to be a subtype of UnaryPipe&lt;<typeparamref name="TKey"/>,<typeparamref name="TInput"/>&gt;.
        /// </returns>
        internal static Tuple<Type, string> Generate<TKey, TInput, TState, TOutput, TResult>(
            GroupedWindowStreamable<TKey, TInput, TState, TOutput, TResult> stream)
        {
            Contract.Requires(stream != null);
            Contract.Ensures(Contract.Result<Tuple<Type, string>>() == null || typeof(IStreamObserver<Empty, TInput>).GetTypeInfo().IsAssignableFrom(Contract.Result<Tuple<Type, string>>().Item1));

            string errorMessages = null;
            try
            {
                string expandedCode;

                var template = new GroupedWindowTemplate($"GeneratedGroupedAggregate_{GroupedAggregateSequenceNumber++}");

                var keyType = template.keyType = typeof(TKey);
                var inputType = template.inputType = typeof(TInput);
                var stateType = template.stateType = typeof(TState);
                var outputType = template.outputType = typeof(TOutput);
                var resultType = template.resultType = typeof(TResult);

                template.TResult = resultType.GetCSharpSourceSyntax(); // BUGBUG: need to get any generic parameters needed
                template.isUngrouped = (keyType == typeof(Empty));

                var inputMessageRepresentation = new ColumnarRepresentation(inputType);

                var resultRepresentation = new ColumnarRepresentation(resultType);

                var assemblyReferences = new List<Assembly>();

                #region Key Selector
                var keySelector = stream.KeySelector;
                string transformedKeySelectorAsString;
                if (keyType.IsAnonymousTypeName())
                {
                    Contract.Assume(keySelector.Body is NewExpression);
                    var transformedFunction = Extensions.TransformUnaryFunction<TKey, TInput>(keySelector);
                    var newBody = (NewExpression)transformedFunction.Body;
                    transformedKeySelectorAsString = string.Join(",", newBody.Arguments);
                }
                else
                {
                    var transformedFunction = Extensions.TransformUnaryFunction<Empty, TInput>(keySelector).Body;
                    if (transformedFunction == null) return null;
                    transformedKeySelectorAsString = transformedFunction.ExpressionToCSharp();
                }
                template.keySelector = transformedKeySelectorAsString;
                assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(keySelector));
                #endregion

                #region Key Comparer and HashCode
                var keyComparer = EqualityComparerExpression<TKey>.Default;
                template.keyComparerEquals =
                    (left, right) =>
                        keyComparer.GetEqualsExpr().Inline(left, right);
                template.keyComparerGetHashCode =
                    (x) =>
                        keyComparer.GetGetHashCodeExpr().Inline(x);
                assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(keyComparer.GetEqualsExpr()));
                assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(keyComparer.GetGetHashCodeExpr()));
                #endregion

                #region Aggregate functions
                var initialStateLambda = stream.Aggregate.InitialState();
                if (ConstantExpressionFinder.IsClosedExpression(initialStateLambda))
                {
                    template.initialState = initialStateLambda.Body.ExpressionToCSharp();
                    assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(initialStateLambda));
                }
                else
                {
                    if (Config.CodegenOptions.SuperStrictColumnar)
                    {
                        errorMessages = "Code Generation for GroupedWindow: couldn't inline the initial state lambda!";
                        throw new InvalidOperationException(errorMessages);
                    }
                    else
                    {
                        template.useCompiledInitialState = true;
                        template.initialState = "initialState()";
                    }
                }

                var accumulateLambda = stream.Aggregate.Accumulate();
                if (ConstantExpressionFinder.IsClosedExpression(accumulateLambda))
                {
                    var accTransformedLambda = Extensions.TransformFunction<TKey, TInput>(accumulateLambda, 2);
                    template.accumulate = (stateArg, longArg) => accTransformedLambda.Inline(stateArg, longArg);
                    assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(accumulateLambda));
                }
                else
                {
                    if (Config.CodegenOptions.SuperStrictColumnar)
                    {
                        errorMessages = "Code Generation for GroupedWindow: couldn't inline the accumulate lambda!";
                        throw new InvalidOperationException(errorMessages);
                    }
                    else
                    {
                        template.useCompiledAccumulate = true;
                        template.accumulate = (s1, s2) => $"accumulate({s1}, {s2}, batch[i]);";
                    }
                }

                var deaccumulateLambda = stream.Aggregate.Deaccumulate();
                if (ConstantExpressionFinder.IsClosedExpression(deaccumulateLambda))
                {
                    var deaccumulateTransformedLambda = Extensions.TransformFunction<TKey, TInput>(deaccumulateLambda, 2);
                    template.deaccumulate = (stateArg, longArg) => deaccumulateTransformedLambda.Inline(stateArg, longArg);
                    assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(deaccumulateLambda));
                }
                else
                {
                    if (Config.CodegenOptions.SuperStrictColumnar)
                    {
                        throw new InvalidOperationException("Code Generation couldn't inline a lambda!");
                    }
                    else
                    {
                        template.useCompiledDeaccumulate = true;
                        template.deaccumulate = (s1, s2) => $"deaccumulate({s1}, {s2}, batch[i]);";
                    }
                }

                var differenceLambda = stream.Aggregate.Difference();
                if (ConstantExpressionFinder.IsClosedExpression(differenceLambda))
                {
                    template.difference = (stateArg1, stateArg2) => differenceLambda.Inline(stateArg1, stateArg2);
                    assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(differenceLambda));
                }
                else
                {
                    if (Config.CodegenOptions.SuperStrictColumnar)
                    {
                        errorMessages = "Code Generation for GroupedWindow: couldn't inline the deaccumulate lambda!";
                        throw new InvalidOperationException(errorMessages);
                    }
                    else
                    {
                        template.useCompiledDifference = true;
                        template.deaccumulate = (s1, s2) => $"difference({s1}, {s2});";
                    }
                }

                var computeResultLambda = stream.Aggregate.ComputeResult();
                if (ConstantExpressionFinder.IsClosedExpression(computeResultLambda))
                {
                    if (outputType.IsAnonymousType())
                    {
                        if (computeResultLambda.Body is NewExpression newExpression)
                        {
                            errorMessages = "Code Generation for GroupedWindow: result selector must be a new expression for anonymous types";
                            throw new NotImplementedException(errorMessages);
                        }
                        else
                        {
                            template.computeResult = (stateArg) => computeResultLambda.Inline(stateArg);
                        }
                    }
                    else
                    {
                        template.computeResult = (stateArg) => computeResultLambda.Inline(stateArg);
                    }
                    assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(computeResultLambda));
                }
                else
                {
                    if (Config.CodegenOptions.SuperStrictColumnar)
                    {
                        errorMessages = "Code Generation for GroupedWindow: couldn't inline the result selector lambda!";
                        throw new InvalidOperationException(errorMessages);
                    }
                    else
                    {
                        template.useCompiledComputeResult = true;
                        template.computeResult = (stateArg) => $"computeResult({stateArg})";
                    }
                }
                #endregion

                template.BatchGeneratedFrom_Unit_TInput = Transformer.GetBatchClassName(typeof(Empty), inputType);
                template.UnitTInputGenericParameters = string.Empty; // BUGBUG
                template.UnitTResultGenericParameters = string.Empty; // BUGBUG
                template.inputFields = inputMessageRepresentation.AllFields;
                template.outputFields = resultRepresentation.AllFields;

                var resultSelector = stream.ResultSelector;
                var parameterSubstitutions = new List<Tuple<ParameterExpression, SelectParameterInformation>>(); // dont want the parameters substituted for at all
                var projectionResult = SelectTransformer.Transform(resultSelector, parameterSubstitutions, resultRepresentation, true);
                if (projectionResult.Error)
                {
                    return null;
                }
                template.finalResultSelector =
                    (key, aggregateResult) =>
                    {
                        var parameters = new Dictionary<ParameterExpression, string>
                        {
                            { resultSelector.Parameters.ElementAt(0), key }
                        };
                        var sb = new System.Text.StringBuilder();
                        sb.AppendLine("{");
                        sb.AppendLine($"var {resultSelector.Parameters.ElementAt(1).Name} = {aggregateResult};\n");
                        if (projectionResult.ProjectionReturningResultInstance != null)
                        {
                            sb.AppendLine($"this.batch[_c] = {projectionResult.ProjectionReturningResultInstance.ExpressionToCSharp()};");
                        }
                        else
                        {
                            foreach (var kv in projectionResult.ComputedFields)
                            {
                                var f = kv.Key;
                                var e = kv.Value;
                                sb.AppendLine($"this.batch.{f.Name}.col[_c] = {e.ExpressionToCSharpStringWithParameterSubstitution(parameters)};");
                            }
                        }

                        sb.AppendLine("}");
                        return sb.ToString();
                    };
                assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(resultSelector));

                expandedCode = template.TransformText();

                assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(typeof(Empty), typeof(TKey), typeof(TInput), typeof(TState), typeof(TOutput), typeof(FastDictionaryGenerator3)));
                assemblyReferences.Add(typeof(IStreamable<,>).GetTypeInfo().Assembly);
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<Empty, TInput>());
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<Empty, TResult>());
                assemblyReferences.Add(Transformer.GeneratedMemoryPoolAssembly<Empty, TResult>());

                var assembly = Transformer.CompileSourceCode(expandedCode, assemblyReferences, out errorMessages);
                var t = assembly.GetType(template.className);
                if (t.GetTypeInfo().IsGenericType)
                {
                    var list = typeof(TKey).GetAnonymousTypes();
                    list.AddRange(typeof(TInput).GetAnonymousTypes());
                    list.AddRange(typeof(TState).GetAnonymousTypes());
                    list.AddRange(typeof(TOutput).GetAnonymousTypes());
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
