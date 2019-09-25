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
    internal partial class UngroupTemplate
    {
        private readonly Type outerKeyType;
        private readonly Type innerKeyType;
        private readonly Type innerResultType;
        private readonly Type resultType;
        private readonly bool ungroupingToUnit;
        private readonly bool ungroupingFromCompound;

        private UngroupTemplate(
            string className,
            bool ungroupingFromCompound,
            Type outerKeyType,
            Type innerKeyType,
            Type innerResultType,
            Type resultType) : base(className)
        {
            Contract.Requires(className != null);
            Contract.Requires(outerKeyType != null);
            Contract.Requires(innerKeyType != null);
            Contract.Requires(innerResultType != null);
            Contract.Requires(resultType != null);

            this.outerKeyType = outerKeyType;
            this.innerKeyType = innerKeyType;
            this.innerResultType = innerResultType;
            this.resultType = resultType;
            this.ungroupingToUnit = outerKeyType == typeof(Empty);
            this.ungroupingFromCompound = ungroupingFromCompound;

            var tm = new TypeMapper(outerKeyType, innerKeyType, innerResultType, resultType);
            this.TOuterKey = tm.CSharpNameFor(outerKeyType);
            this.TInnerKey = tm.CSharpNameFor(innerKeyType);
            this.TInnerResult = tm.CSharpNameFor(innerResultType);
            this.TResult = tm.CSharpNameFor(resultType);

            var gps = tm.GenericTypeVariables(outerKeyType, innerKeyType, innerResultType, resultType);
            this.genericParameters = gps.BracketedCommaSeparatedString();
            this.numberOfGenericParameters = gps.Count();

            this.inputBatchClassType = ungroupingFromCompound
                ? Transformer.GetBatchClassName(typeof(CompoundGroupKey<,>).MakeGenericType(outerKeyType, innerKeyType), innerResultType)
                : Transformer.GetBatchClassName(innerKeyType, innerResultType);
            this.inputBatchGenericParameters = ungroupingFromCompound
                ? tm.GenericTypeVariables(outerKeyType, innerKeyType, innerResultType).BracketedCommaSeparatedString()
                : tm.GenericTypeVariables(innerKeyType, innerResultType).BracketedCommaSeparatedString();

            this.resultBatchClassType = Transformer.GetBatchClassName(outerKeyType, resultType);
            this.resultBatchGenericParameters = tm.GenericTypeVariables(outerKeyType, resultType).BracketedCommaSeparatedString();
        }

        private static int UngroupSequenceNumber = 0;
        private ColumnarRepresentation innerResultRepresentation;
        private IEnumerable<Tuple<MyFieldInfo, MyFieldInfo>> swingingFields;
        private IDictionary<MyFieldInfo, Expression> computedFields;

        /// <summary>
        /// Needed when the projection function cannot be split into separate assignments to the columns
        /// of the result batch.
        /// </summary>
        private Expression ProjectionReturningResultInstance = null;

        private ParameterExpression keyParameter;
        private string TOuterKey;
        private string TInnerKey;
        private string TInnerResult;
        private string TResult;
        private string resultBatchClassType;
        private string inputBatchClassType;
        private string inputBatchGenericParameters;
        private string resultBatchGenericParameters;
        private string genericParameters;
        private int numberOfGenericParameters;
        private IEnumerable<MyFieldInfo> unassignedFields;

        internal static Tuple<Type, string> Generate<TOuterKey, TInnerKey, TInnerResult, TResult>(Expression<Func<TInnerKey, TInnerResult, TResult>> resultSelector)
            => GenerateInternal<TOuterKey, TInnerKey, TInnerResult, TResult>(resultSelector, false);

        internal static Tuple<Type, string> Generate<TInnerKey, TInnerResult, TResult>(Expression<Func<TInnerKey, TInnerResult, TResult>> resultSelector)
            => GenerateInternal<Empty, TInnerKey, TInnerResult, TResult>(resultSelector, true);

        private static Tuple<Type, string> GenerateInternal<TOuterKey, TInnerKey, TInnerResult, TResult>(Expression<Func<TInnerKey, TInnerResult, TResult>> resultSelector, bool isFirstLevelGroup)
        {
            Contract.Ensures(Contract.Result<Tuple<Type, string>>() != null);
            Contract.Ensures(typeof(Pipe<TOuterKey, TResult>).GetTypeInfo().IsAssignableFrom(Contract.Result<Tuple<Type, string>>().Item1));

            string errorMessages = null;
            try
            {
                var typeOfTOuterKey = typeof(TOuterKey);
                var typeOfTInnerKey = typeof(TInnerKey);
                var typeofTInnerResult = typeof(TInnerResult);
                var typeofTResult = typeof(TResult);

                string expandedCode;
                List<Assembly> assemblyReferences;
                int numberOfGenericParameters;

                // inline:
                // var ok = UngroupTemplate.GetGeneratedCode<TOuterKey, TInnerKey, TInnerResult, TResult>(
                //      resultSelector,
                //      false,
                //      out generatedClassName,
                //      out expandedCode,
                //      out assemblyReferences,
                //      out numberOfGenericParameters);
                var generatedClassName = string.Format(
                    "UngroupPipeGeneratedFrom_{0}_{1}_{2}_{3}_{4}",
                    typeOfTOuterKey.GetValidIdentifier(),
                    typeOfTInnerKey.GetValidIdentifier(),
                    typeofTInnerResult.GetValidIdentifier(),
                    typeofTResult.GetValidIdentifier(),
                    UngroupSequenceNumber++);

                var inputMessageType = StreamMessageManager.GetStreamMessageType<CompoundGroupKey<TOuterKey, TInnerKey>, TInnerResult>();

                var innerResultRepresentation = new ColumnarRepresentation(typeofTInnerResult);
                var resultRepresentation = new ColumnarRepresentation(typeofTResult);

                var parameterSubstitutions = new List<Tuple<ParameterExpression, SelectParameterInformation>>
                {
                    // Leave the key parameter to the selector unchanged, so no substitution for parameters[0]
                    Tuple.Create(resultSelector.Parameters[1], new SelectParameterInformation() { BatchName = "inputBatch", BatchType = inputMessageType, IndexVariableName = "i", parameterRepresentation = innerResultRepresentation, })
                };
                var result = SelectTransformer.Transform(resultSelector, parameterSubstitutions, resultRepresentation);
                if (result.Error) return Tuple.Create((Type)null, errorMessages);

                var template = new UngroupTemplate(generatedClassName, !isFirstLevelGroup, typeOfTOuterKey, typeOfTInnerKey, typeofTInnerResult, typeofTResult)
                {
                    innerResultRepresentation = innerResultRepresentation,
                    swingingFields = result.SwingingFields,
                    computedFields = result.ComputedFields,
                    unassignedFields = result.UnmentionedFields,
                    ProjectionReturningResultInstance = result.ProjectionReturningResultInstance,
                    keyParameter = resultSelector.Parameters.First()
                };

                expandedCode = template.TransformText();

                assemblyReferences = Transformer.AssemblyReferencesNeededFor(typeOfTOuterKey, typeOfTInnerKey, typeofTInnerResult, typeofTResult);
                assemblyReferences.Add(typeof(IStreamable<,>).GetTypeInfo().Assembly);

                // input messages
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<CompoundGroupKey<TOuterKey, TInnerKey>, TInnerResult>());

                // output messages
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TOuterKey, TResult>());

                // memory pool
                assemblyReferences.Add(Transformer.GeneratedMemoryPoolAssembly<TOuterKey, TResult>());
                assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(resultSelector));

                numberOfGenericParameters = template.numberOfGenericParameters;

                var a = Transformer.CompileSourceCode(expandedCode, assemblyReferences, out errorMessages);

                if (numberOfGenericParameters > 0)
                    generatedClassName = generatedClassName + "`" + numberOfGenericParameters.ToString(CultureInfo.InvariantCulture);
                var t = a.GetType(generatedClassName);
                t = t.InstantiateAsNecessary(typeOfTOuterKey, typeOfTInnerKey, typeofTInnerResult, typeofTResult);

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
