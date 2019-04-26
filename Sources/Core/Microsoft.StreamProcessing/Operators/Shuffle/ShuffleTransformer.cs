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
    internal partial class ShuffleTemplate
    {
        public IEnumerable<MyFieldInfo> fields;
        public string inlinedHashCodeComputation;
        public string transformedKeySelectorAsString = string.Empty;
        private readonly string TOuterKeyTSourceGenericParameters;
        private readonly string TOuterKey;
        private readonly string TSource;
        public string TInnerKey;
        private readonly string genericParameters;
        public bool innerKeyIsAnonymous;
        private readonly string resultBatchClassType;
        private readonly string sourceBatchClassType;
        private readonly string resultBatchGenericParameters;
        private readonly bool isFirstLevelGroup;
        private readonly bool powerOf2;
        public string vectorHashCodeInitialization = string.Empty;

        private static int shuffleCounter = 0;

        public ShuffleTemplate(
            string className,
            Type outerKeyType,
            Type sourceType,
            Type innerKeyType,
            string inlinedHashCodeComputation,
            bool nested, bool powerOf2) : base(className)
        {
            Contract.Requires(className != null);
            Contract.Requires(outerKeyType != null);
            Contract.Requires(sourceType != null);
            Contract.Requires(innerKeyType != null);

            this.inlinedHashCodeComputation = inlinedHashCodeComputation;
            this.isFirstLevelGroup = !nested;
            this.powerOf2 = powerOf2;

            var tm = new TypeMapper(outerKeyType, sourceType, innerKeyType);
            this.TOuterKey = tm.CSharpNameFor(outerKeyType);
            this.TSource = tm.CSharpNameFor(sourceType);
            this.TInnerKey = tm.CSharpNameFor(innerKeyType);

            this.genericParameters = tm.GenericTypeVariables(outerKeyType, sourceType, innerKeyType).BracketedCommaSeparatedString();

            this.resultBatchClassType = this.isFirstLevelGroup
                ? Transformer.GetBatchClassName(innerKeyType, sourceType)
                : Transformer.GetBatchClassName(typeof(CompoundGroupKey<,>).MakeGenericType(outerKeyType, innerKeyType), sourceType);
            this.resultBatchGenericParameters = this.genericParameters;

            this.sourceBatchClassType = Transformer.GetBatchClassName(outerKeyType, sourceType);
            this.TOuterKeyTSourceGenericParameters = tm.GenericTypeVariables(outerKeyType, sourceType).BracketedCommaSeparatedString();
        }

        public static Tuple<Type, string> Generate<TOuterKey, TSource, TInnerKey>(
            Expression<Func<TSource, TInnerKey>> keySelector,
            string inlinedHashCodeComputation,
            bool nested,
            bool powerOf2)
        {
            string errorMessages = null;
            try
            {
                var typeOfTOuterKey = typeof(TOuterKey);
                var typeOfTSource = typeof(TSource);
                var typeOfTInnerKey = typeof(TInnerKey);
                var generatedClassName = string.Format(CultureInfo.InvariantCulture, "ShuffleStreamablePipeGeneratedFrom_{0}_{1}_{2}_{3}", typeOfTOuterKey.GetValidIdentifier(), typeOfTSource.GetValidIdentifier(), typeOfTInnerKey.GetValidIdentifier(), shuffleCounter++);

                var inputMessageRepresentation = new ColumnarRepresentation(typeOfTSource);

                var template = new ShuffleTemplate(
                    generatedClassName,
                    typeOfTOuterKey,
                    typeOfTSource,
                    typeOfTInnerKey,
                    inlinedHashCodeComputation,
                    nested, powerOf2)
                {
                    fields = inputMessageRepresentation.AllFields
                };

                var innerKeyIsAnonymous = typeOfTInnerKey.IsAnonymousTypeName();

                if (keySelector != null)
                {
                    var transformedKeySelector = Extensions.TransformUnaryFunction<TOuterKey, TSource>(keySelector);
                    if (innerKeyIsAnonymous && keySelector.Body is NewExpression keySelectorAsNewExpression)
                    {
                        var newPrime = (NewExpression)transformedKeySelector.Body;
                        template.transformedKeySelectorAsString = string.Format(CultureInfo.InvariantCulture, "({0})Activator.CreateInstance(typeof({0}), {1})",
                            template.TInnerKey,
                            string.Join(",", newPrime.Arguments.Select(m => m.ExpressionToCSharp())));
                    }
                    else
                    {
                        template.transformedKeySelectorAsString = transformedKeySelector.Body.ExpressionToCSharp();
                        if (Config.UseMultiString &&
                            typeOfTInnerKey.Equals(typeof(string)) &&
                            keySelector.IsSimpleFieldOrPropertyAccess())
                        {
                            template.inlinedHashCodeComputation = "hashCodeVector.col[i]";
                            var fieldName = ((MemberExpression)keySelector.Body).Member.Name;
                            template.vectorHashCodeInitialization = $"var hashCodeVector = {Transformer.ColumnFieldPrefix}{fieldName}_col.GetHashCode(batch.bitvector);";
                        }
                    }
                }
                else
                {
                    template.transformedKeySelectorAsString = string.Empty;
                }

                template.innerKeyIsAnonymous = innerKeyIsAnonymous;
                var expandedCode = template.TransformText();

                var assemblyReferences = Transformer.AssemblyReferencesNeededFor(typeOfTOuterKey, typeOfTSource, typeOfTInnerKey);
                assemblyReferences.Add(typeof(IStreamable<,>).GetTypeInfo().Assembly);
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TOuterKey, TSource>());
                if (nested)
                {
                    assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<CompoundGroupKey<TOuterKey, TInnerKey>, TSource>());
                    assemblyReferences.Add(Transformer.GeneratedMemoryPoolAssembly<CompoundGroupKey<TOuterKey, TInnerKey>, TSource>());
                }
                else
                {
                    assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TInnerKey, TSource>());
                    assemblyReferences.Add(Transformer.GeneratedMemoryPoolAssembly<TInnerKey, TSource>());
                }
                assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(keySelector));

                var a = Transformer.CompileSourceCode(expandedCode, assemblyReferences, out errorMessages);
                if (typeOfTInnerKey.IsAnonymousTypeName())
                {
                    if (errorMessages == null) errorMessages = string.Empty;
                    errorMessages += "\nCodegen Warning: The inner key type for Shuffle is anonymous, causing the use of Activator.CreateInstance in an inner loop. This will lead to poor performance.\n";
                }

                generatedClassName = generatedClassName.AddNumberOfNecessaryGenericArguments(typeOfTOuterKey, typeOfTSource, typeOfTInnerKey);
                var t = a.GetType(generatedClassName);
                return Tuple.Create(t.InstantiateAsNecessary(typeOfTOuterKey, typeOfTSource, typeOfTInnerKey), errorMessages);
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
