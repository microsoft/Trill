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
    internal partial class SelectTemplate
    {
        private static int sequenceNumber = 0;
        private string PARAMETER;

        /// <summary>
        /// All public instance fields of type TResult.
        /// Equal to the union of computedFields and swingingFields.
        /// REVIEW: Is this true? Can there be public fields that just aren't mentioned in the projection?
        /// </summary>
        private IEnumerable<MyFieldInfo> destinationFields;

        /// <summary>
        /// Subset of destinationFields that are computed a row at a time.
        /// For each computed field, f, the table maps f to the the C# source for the value that is assigned to f for each row.
        /// </summary>
        private IDictionary<MyFieldInfo, Expression> computedFields = new Dictionary<MyFieldInfo, Expression>();

        /// <summary>
        /// Pairs of result type fields and source type fields where the result field
        /// is just assigned from a source field.
        /// </summary>
        private IEnumerable<Tuple<MyFieldInfo, MyFieldInfo>> swingingFields = new List<Tuple<MyFieldInfo, MyFieldInfo>>();

        /// <summary>
        /// Subset of sourceFields that are *not* just assigned to a result field.
        /// </summary>
        private IEnumerable<MyFieldInfo> nonSwingingFields;

        /// <summary>
        /// Needed when the projection function cannot be split into separate assignments to the columns
        /// of the result batch.
        /// </summary>
        private string ProjectionReturningResultInstance = null;

        /// <summary>
        /// Needed when the projection function references the parameter (i.e., source instance) directly.
        /// </summary>
        private bool needSourceInstance = false;

        private ColumnarRepresentation resultPayloadRepresentation;
        private IEnumerable<string> multiStringOperations;
        private IEnumerable<MyFieldInfo> unassignedFields;
        private Dictionary<MyFieldInfo, int> swungFieldsCount;
        private string genericParameters;
        private int numberOfGenericParameters;
        private string TKeyTResultGenericParameters;
        private string MemoryPoolGenericParameters;
        private string StartEdgeParameterName;

        private SelectTemplate(string className, Type keyType, Type payloadType, Type resultType)
            : base(className, keyType, payloadType, resultType) { }

        public static Tuple<Type, string> Generate<TKey, TPayload, TResult>(SelectStreamable<TKey, TPayload, TResult> stream)
        {
            Contract.Ensures(Contract.Result<Tuple<Type, string>>() == null || typeof(UnaryPipe<TKey, TPayload, TResult>).GetTypeInfo().IsAssignableFrom(Contract.Result<Tuple<Type, string>>().Item1));

            string generatedClassName;
            string errorMessages = null;
            try
            {
                var keyType = typeof(TKey);
                var payloadType = typeof(TPayload);
                var resultType = typeof(TResult);
                generatedClassName = string.Format("Select_{0}_{1}_{2}_{3}", keyType.GetValidIdentifier(), payloadType.GetValidIdentifier(), resultType.GetValidIdentifier(), sequenceNumber++);
                var template = new SelectTemplate(generatedClassName, keyType, payloadType, resultType);

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

                template.PARAMETER = stream.Selector.Parameters[payloadParameterIndex].Name;
                template.resultPayloadRepresentation = new ColumnarRepresentation(resultType);
                template.destinationFields = template.resultPayloadRepresentation.AllFields;

                if (template.numberOfGenericParameters > 0)
                    generatedClassName = generatedClassName + "`" + template.numberOfGenericParameters.ToString(CultureInfo.InvariantCulture);

                var resultSelector = stream.Selector;
                var sourceMessageType = StreamMessageManager.GetStreamMessageType<TKey, TPayload>();
                var parameterSubsitutions = new List<Tuple<ParameterExpression, SelectParameterInformation>>();

                // Don't create a parameter substitution for the start edge parameter. That will just remain in the
                // body of the result selector and will be set as a local variable in the generated code.
                if (stream.HasKey)
                {
                    var keyRepresentation = new ColumnarRepresentation(keyType, "key");
                    var keyParameterIndex = stream.HasStartEdge ? 1 : 0;
                    parameterSubsitutions.Add(Tuple.Create(
                        resultSelector.Parameters.ElementAt(keyParameterIndex),
                        new SelectParameterInformation() { BatchName = "sourceBatch", BatchType = sourceMessageType, IndexVariableName = "i", parameterRepresentation = keyRepresentation, }));
                }
                parameterSubsitutions.Add(Tuple.Create(
                    resultSelector.Parameters.ElementAt(payloadParameterIndex),
                    new SelectParameterInformation() { BatchName = "sourceBatch", BatchType = sourceMessageType, IndexVariableName = "i", parameterRepresentation = new ColumnarRepresentation(payloadType) }));
                var projectionResult = SelectTransformer.Transform(resultSelector, parameterSubsitutions, template.resultPayloadRepresentation, false, stream.HasStartEdge);
                if (projectionResult.Error)
                {
                    if (Config.CodegenOptions.SuperStrictColumnar)
                    {
                        throw new InvalidOperationException("Code Generation couldn't transform a selector!");
                    }
                    return Tuple.Create((Type)null, errorMessages);
                }
                template.StartEdgeParameterName = stream.HasStartEdge ? resultSelector.Parameters.ElementAt(0).Name : null;
                template.computedFields = projectionResult.ComputedFields;
                template.swingingFields = projectionResult.SwingingFields;
                template.unassignedFields = projectionResult.UnmentionedFields;
                template.ProjectionReturningResultInstance = projectionResult.ProjectionReturningResultInstance?.ExpressionToCSharp();
                template.multiStringOperations = projectionResult.MultiStringOperations;
                template.needSourceInstance = projectionResult.NeedsSourceInstance;

                var d = new Dictionary<MyFieldInfo, int>();
                foreach (var f in template.swingingFields)
                {
                    var target = f.Item1;
                    var source = f.Item2;
                    if (!d.ContainsKey(source))
                    {
                        d.Add(source, 0);
                    }
                    d[source] = d[source] + 1;
                }
                template.swungFieldsCount = d;

                template.nonSwingingFields = template.fields.Where(sf => !template.swingingFields.Any(swingingField => swingingField.Item2.Equals(sf)));
                var expandedCode = template.TransformText();

                var assemblyReferences = Transformer.AssemblyReferencesNeededFor(
                    typeof(TKey), typeof(TPayload), typeof(TResult), typeof(IStreamable<,>));
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
                    throw new InvalidOperationException("Code Generation failed when it wasn't supposed to!");

                return Tuple.Create((Type)null, errorMessages);
            }
        }
    }
}