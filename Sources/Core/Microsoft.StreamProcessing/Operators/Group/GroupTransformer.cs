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
    internal partial class GroupTemplate
    {
        private static int GroupStreamableSequenceNumber = 0;
        private readonly Type outerKeyType;
        private readonly Type sourceType;
        private readonly Type innerKeyType;
        public IEnumerable<MyFieldInfo> fields;
        private string transformedKeySelectorAsString;
        private string inlinedHashCodeComputation;
        private readonly bool isFirstLevelGroup;
        private MyFieldInfo swingingField;
        public string vectorHashCodeInitialization = string.Empty;
        private bool swingingHashColumn;
        private bool payloadMightBeNull;

        private GroupTemplate(
            string className,
            Type outerKeyType,
            Type sourceType,
            Type innerKeyType,
            string transformedKeySelectorAsString,
            string inlinedHashCodeComputation,
            bool nested) : base(className)
        {
            Contract.Requires(className != null);
            Contract.Requires(outerKeyType != null);
            Contract.Requires(sourceType != null);
            Contract.Requires(innerKeyType != null);

            this.outerKeyType = outerKeyType;
            this.sourceType = sourceType;
            this.innerKeyType = innerKeyType;
            this.transformedKeySelectorAsString = transformedKeySelectorAsString;
            this.inlinedHashCodeComputation = inlinedHashCodeComputation;
            this.isFirstLevelGroup = !nested;
        }

        public static Tuple<Type, string> Generate<TOuterKey, TSource, TInnerKey>(
            Expression<Func<TInnerKey, int>> hashComparer,
            Expression<Func<TSource, TInnerKey>> keySelector,
            bool nested)
        {
            var typeOfTOuterKey = typeof(TOuterKey);
            var typeOfTSource = typeof(TSource);
            var typeOfTInnerKey = typeof(TInnerKey);
            string expandedCode;
            List<Assembly> assemblyReferences;
            string errorMessages = null;
            try
            {
                string generatedClassName = $"GeneratedGroupStreamable_{GroupStreamableSequenceNumber++}";

                string transformedKeySelectorAsString;
                MyFieldInfo swingingField = default;
                if (typeOfTInnerKey.IsAnonymousTypeName())
                {
                    Contract.Assume(keySelector.Body is NewExpression);
                    var transformedFunction = Extensions.TransformUnaryFunction<TOuterKey, TSource>(keySelector);
                    var newBody = (NewExpression)transformedFunction.Body;
                    transformedKeySelectorAsString = string.Join(",", newBody.Arguments.Select(arg => arg.ExpressionToCSharp()));
                }
                else
                {
                    var body = keySelector.Body;

                    if (!nested && body is MemberExpression singleFieldProjection)
                    {
                        if (singleFieldProjection.Expression is ParameterExpression dereferencedObject)
                        {
                            // then can just swing a pointer to set the key of the result message
                            Contract.Assume(dereferencedObject == keySelector.Parameters.ElementAt(0));
                            var f = singleFieldProjection.Member;
                            var sourceMessageRepresentation = new ColumnarRepresentation(typeOfTSource);
                            var fieldToSwingFrom = sourceMessageRepresentation.Fields[f.Name];
                            swingingField = fieldToSwingFrom;
                            transformedKeySelectorAsString = swingingField.Name + "_col[i]";
                        }
                        else
                        {
                            var transformedPredicate = Extensions.TransformUnaryFunction<TOuterKey, TSource>(keySelector).Body;
                            if (transformedPredicate == null) return Tuple.Create((Type)null, errorMessages);
                            transformedKeySelectorAsString = transformedPredicate.ExpressionToCSharp();
                        }

                    }
                    else
                    {
                        var transformedPredicate = Extensions.TransformUnaryFunction<TOuterKey, TSource>(keySelector).Body;

                        if (transformedPredicate == null) return Tuple.Create((Type)null, errorMessages);
                        transformedKeySelectorAsString = transformedPredicate.ExpressionToCSharp();
                    }
                }

                var inlinedHashCodeComputation = hashComparer.Inline("key");

                var template = new GroupTemplate(
                  generatedClassName,
                  typeOfTOuterKey,
                  typeOfTSource,
                  typeOfTInnerKey,
                  transformedKeySelectorAsString,
                  inlinedHashCodeComputation,
                  nested)
                {
                    swingingField = swingingField,
                    payloadMightBeNull = typeOfTSource.CanContainNull()
                };

                if (!nested && Config.UseMultiString &&
                    typeOfTInnerKey.Equals(typeof(string)) &&
                    keySelector.IsSimpleFieldOrPropertyAccess())
                {
                    var transformedPredicate = Extensions.TransformUnaryFunction<TOuterKey, TSource>(keySelector).Body;
                    template.transformedKeySelectorAsString = transformedPredicate.ExpressionToCSharp();
                    template.inlinedHashCodeComputation = "hashCodeVector.col[i]";
                    var fieldName = ((MemberExpression)keySelector.Body).Member.Name;
                    template.vectorHashCodeInitialization = $"resultBatch.hash = {Transformer.ColumnFieldPrefix}{fieldName}_col.GetHashCode(batch.bitvector);";
                    template.swingingHashColumn = true;
                }

                template.fields = new ColumnarRepresentation(typeOfTSource).AllFields;

                expandedCode = template.TransformText();

                assemblyReferences = Transformer.AssemblyReferencesNeededFor(typeOfTOuterKey, typeOfTSource, typeOfTInnerKey);
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
                    errorMessages += "\nCodegen Warning: The inner key type for Group is anonymous, causing the use of Activator.CreateInstance in an inner loop. This will lead to poor performance.\n";
                }

                generatedClassName = generatedClassName.AddNumberOfNecessaryGenericArguments(typeOfTOuterKey, typeOfTSource, typeOfTInnerKey);
                var t = a.GetType(generatedClassName);
                t = t.InstantiateAsNecessary(typeOfTOuterKey, typeOfTSource, typeOfTInnerKey);

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
