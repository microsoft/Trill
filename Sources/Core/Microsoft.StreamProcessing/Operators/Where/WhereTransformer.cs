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

namespace Microsoft.StreamProcessing
{
    internal partial class WhereTemplate
    {
        public readonly bool noTransformation;
        public readonly string PARAMETER;
        public readonly string PREDICATE;

        private static int sequenceNumber = 0;
        private readonly string multiStringInit;
        private readonly string multiStringReturns;
        private string multiStringWrapperInit;
        private string vectorOperations;

        private WhereTemplate(
            string className, Type keyType, Type payloadType, string predicate, bool noTransformation, string parameterName, string multiStringInit, string multiStringReturns)
            : base(className, keyType, payloadType, payloadType)
        {
            this.fields = payloadType.GetAnnotatedFields().Item1; // Do we need this special behavior?
            this.PREDICATE = predicate;
            this.noTransformation = noTransformation;
            this.PARAMETER = parameterName;
            this.multiStringInit = multiStringInit;
            this.multiStringReturns = multiStringReturns;
        }

        internal static Tuple<Type, string> Generate<TKey, TPayload>(WhereStreamable<TKey, TPayload> stream)
        {
            string generatedClassName;
            string expandedCode;
            var assemblyReferences = new List<Assembly>();
            string errorMessages = null;

            try
            {
                var keyType = typeof(TKey);
                assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(keyType));

                var sourceType = typeof(TPayload);
                assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(sourceType));
                assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(stream.Predicate));

                generatedClassName = string.Format("WhereUnaryPipeGeneratedFrom_{0}_{1}_{2}", keyType.GetValidIdentifier(), sourceType.GetValidIdentifier(), sequenceNumber++);

                var noTransformation = false;

                var p = stream.Predicate.Body;

                var transformedPredicate = p;
                var multiStringInit = string.Empty;
                var multiStringReturns = string.Empty;
                var multiStringWrapperInit = string.Empty;
                string vectorOperations = null;

                if (Config.UseMultiString && Config.MultiStringTransforms != Config.CodegenOptions.MultiStringFlags.None)
                {
                    var result = MultiStringTransformer.Transform(sourceType, transformedPredicate);
                    if (result.transformedExpression != null)
                        transformedPredicate = result.transformedExpression;

                    multiStringInit = string.Join("\n", result.wrapperTable.Select(e => string.Format(
                        "var {0} = new MultiString.MultiStringWrapper({1}{2}_col);",
                        e.Value.Name, Transformer.ColumnFieldPrefix, e.Key.Name)));
                    multiStringWrapperInit = string.Join("\n", result.wrapperTable.Select(e => string.Format("{0}.rowIndex = i;", e.Value.Name)));

                    vectorOperations = result.vectorOperation;
                }

                Contract.Assume(stream.Predicate.Parameters.Count() == 1);
                var parameter = stream.Predicate.Parameters.First();

                var x = Extensions.TransformUnaryFunction<TKey, TPayload>(Expression.Lambda(transformedPredicate, parameter));
                noTransformation = x == null;
                transformedPredicate = noTransformation ? p : x.Body;

                var template = new WhereTemplate(
                    generatedClassName,
                    keyType,
                    sourceType,
                    transformedPredicate.ExpressionToCSharp(),
                    noTransformation,
                    stream.Predicate.Parameters[0].ToString(),
                    multiStringInit,
                    multiStringReturns)
                {
                    multiStringWrapperInit = multiStringWrapperInit,
                    vectorOperations = vectorOperations
                };

                generatedClassName = generatedClassName.AddNumberOfNecessaryGenericArguments(keyType, sourceType);
                expandedCode = template.TransformText();

                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TPayload>());
                assemblyReferences.Add(typeof(IStreamable<,>).GetTypeInfo().Assembly);

                var a = Transformer.CompileSourceCode(expandedCode, assemblyReferences, out errorMessages);
                var t = a.GetType(generatedClassName);
                t = t.InstantiateAsNecessary(typeof(TKey), typeof(TPayload));
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
