// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace Microsoft.StreamProcessing
{
    internal partial class TemporalEgressTemplate
    {
        private static int TemporalEgressSequenceNumber = 0;

        private readonly string TKey;
        private readonly string TPayload;
        private readonly string TResult;

        private Func<string, string, string> startEdgeFunction;
        private Func<string, string, string, string> intervalFunction;

        private readonly string partitionString;
        private readonly string ingressType;

        private readonly string genericArguments;
        private readonly string egress;
        private readonly string inputKey;
        private readonly string partitionKeyArgument;

        private readonly string BatchGeneratedFrom_TKey_TPayload;
        private readonly string TKeyTPayloadGenericParameters;
        private readonly IEnumerable<MyFieldInfo> fields;
        private readonly ColumnarRepresentation payloadRepresentation;
        private readonly bool isColumnar;

        private TemporalEgressTemplate(Type tKey, Type tPayload, Type tResult, string partitionString, string ingressType, bool isColumnar)
            : base($"GeneratedTemporalEgress_{TemporalEgressSequenceNumber++}")
        {
            var tm = new TypeMapper(tKey, tPayload, tResult);

            this.payloadRepresentation = new ColumnarRepresentation(tPayload);

            this.BatchGeneratedFrom_TKey_TPayload = Transformer.GetBatchClassName(
                tKey == typeof(Empty)
                ? tKey
                : typeof(PartitionKey<>).MakeGenericType(tKey), tPayload);
            this.TKeyTPayloadGenericParameters = tm.GenericTypeVariables(tKey, tPayload).BracketedCommaSeparatedString();

            this.fields = this.payloadRepresentation.AllFields;

            this.TKey = tm.CSharpNameFor(tKey);
            this.TPayload = tm.CSharpNameFor(tPayload);
            this.TResult = tm.CSharpNameFor(tResult);

            this.partitionString = partitionString;
            this.ingressType = ingressType;

            this.partitionKeyArgument = !string.IsNullOrEmpty(partitionString) ? "colkey[i].Key, " : string.Empty;
            this.genericArguments = string.IsNullOrEmpty(partitionString) ? this.TPayload : this.TKey + ", " + this.TPayload;
            this.egress = (ingressType != "StreamEvent")
                ? this.TResult
                : partitionString + "StreamEvent<" + this.genericArguments + ">";
            this.inputKey = string.IsNullOrEmpty(partitionString) ? this.TKey : "PartitionKey<" + this.TKey + ">";
            this.isColumnar = isColumnar;
        }

        internal static Tuple<Type, string> Generate<TPayload>(StreamEventObservable<TPayload> streamEventObservable)
        {
            string errorMessages = null;
            try
            {
                var template = new TemporalEgressTemplate(typeof(Empty), typeof(TPayload), typeof(TPayload), string.Empty, "StreamEvent", streamEventObservable.source.Properties.IsColumnar);
                var keyType = typeof(Empty);
                var expandedCode = template.TransformText();

                var assemblyReferences = Transformer.AssemblyReferencesNeededFor(keyType, typeof(TPayload));
                assemblyReferences.Add(typeof(IStreamable<,>).GetTypeInfo().Assembly);
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<Empty, TPayload>());

                var a = Transformer.CompileSourceCode(expandedCode, assemblyReferences, out errorMessages);
                var t = a.GetType(template.className);
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

        internal static Tuple<Type, string> Generate<TPayload, TResult>(StartEdgeObservable<TPayload, TResult> startEdgeObservable)
        {
            string errorMessages = null;
            try
            {
                var template = new TemporalEgressTemplate(typeof(Empty), typeof(TPayload), typeof(TResult), string.Empty, "StartEdge", startEdgeObservable.source.Properties.IsColumnar);
                if (startEdgeObservable.constructor != null)
                {
                    template.startEdgeFunction = (x, y) =>
                        startEdgeObservable.constructor.Body.ExpressionToCSharpStringWithParameterSubstitution(
                            new Dictionary<ParameterExpression, string>
                            {
                                { startEdgeObservable.constructor.Parameters[0], x },
                                { startEdgeObservable.constructor.Parameters[1], y },
                            });
                }
                var keyType = typeof(Empty);
                var expandedCode = template.TransformText();

                var assemblyReferences = Transformer.AssemblyReferencesNeededFor(keyType, typeof(TPayload), typeof(TResult));
                assemblyReferences.Add(typeof(IStreamable<,>).GetTypeInfo().Assembly);
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<Empty, TPayload>());

                var a = Transformer.CompileSourceCode(expandedCode, assemblyReferences, out errorMessages);
                var t = a.GetType(template.className);
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

        internal static Tuple<Type, string> Generate<TPayload, TResult>(IntervalObservable<TPayload, TResult> intervalObservable)
        {
            string errorMessages = null;
            try
            {
                var template = new TemporalEgressTemplate(typeof(Empty), typeof(TPayload), typeof(TResult), string.Empty, "Interval", intervalObservable.source.Properties.IsColumnar);
                if (intervalObservable.constructor != null)
                {
                    template.intervalFunction = (x, y, z) =>
                        intervalObservable.constructor.Body.ExpressionToCSharpStringWithParameterSubstitution(
                        new Dictionary<ParameterExpression, string>
                        {
                            { intervalObservable.constructor.Parameters[0], x },
                            { intervalObservable.constructor.Parameters[1], y },
                            { intervalObservable.constructor.Parameters[2], z },
                        });
                }
                var keyType = typeof(Empty);
                var expandedCode = template.TransformText();

                var assemblyReferences = Transformer.AssemblyReferencesNeededFor(keyType, typeof(TPayload), typeof(TResult));
                assemblyReferences.Add(typeof(IStreamable<,>).GetTypeInfo().Assembly);
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<Empty, TPayload>());

                var a = Transformer.CompileSourceCode(expandedCode, assemblyReferences, out errorMessages);
                var t = a.GetType(template.className);
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

        internal static Tuple<Type, string> Generate<TKey, TPayload>(PartitionedStreamEventObservable<TKey, TPayload> partitionedStreamEventObservable)
        {
            string errorMessages = null;
            try
            {
                var template = new TemporalEgressTemplate(typeof(TKey), typeof(TPayload), typeof(TPayload), "Partitioned", "StreamEvent", partitionedStreamEventObservable.source.Properties.IsColumnar);

                var keyType = typeof(PartitionKey<>).MakeGenericType(typeof(TKey));
                var expandedCode = template.TransformText();

                var assemblyReferences = Transformer.AssemblyReferencesNeededFor(keyType, typeof(TPayload));
                assemblyReferences.Add(typeof(IStreamable<,>).GetTypeInfo().Assembly);
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<PartitionKey<TKey>, TPayload>());

                var a = Transformer.CompileSourceCode(expandedCode, assemblyReferences, out errorMessages);
                var t = a.GetType(template.className);
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

        internal static Tuple<Type, string> Generate<TKey, TPayload, TResult>(PartitionedStartEdgeObservable<TKey, TPayload, TResult> partitionedStartEdgeObservable)
        {
            string errorMessages = null;
            try
            {
                var template = new TemporalEgressTemplate(typeof(TKey), typeof(TPayload), typeof(TResult), "Partitioned", "StartEdge", partitionedStartEdgeObservable.source.Properties.IsColumnar);
                if (partitionedStartEdgeObservable.constructor != null)
                {
                    template.startEdgeFunction = (x, y) =>
                        partitionedStartEdgeObservable.constructor.Body.ExpressionToCSharpStringWithParameterSubstitution(
                        new Dictionary<ParameterExpression, string>
                        {
                            { partitionedStartEdgeObservable.constructor.Parameters[0], "colkey[i].Key" },
                            { partitionedStartEdgeObservable.constructor.Parameters[0], x },
                            { partitionedStartEdgeObservable.constructor.Parameters[1], y },
                        });
                }
                var keyType = typeof(PartitionKey<>).MakeGenericType(typeof(TKey));
                var expandedCode = template.TransformText();

                var assemblyReferences = Transformer.AssemblyReferencesNeededFor(keyType, typeof(TPayload), typeof(TResult));
                assemblyReferences.Add(typeof(IStreamable<,>).GetTypeInfo().Assembly);
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<PartitionKey<TKey>, TPayload>());

                var a = Transformer.CompileSourceCode(expandedCode, assemblyReferences, out errorMessages);
                var t = a.GetType(template.className);
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

        internal static Tuple<Type, string> Generate<TKey, TPayload, TResult>(PartitionedIntervalObservable<TKey, TPayload, TResult> partitionedIntervalObservable)
        {
            string errorMessages = null;
            try
            {
                var template = new TemporalEgressTemplate(typeof(TKey), typeof(TPayload), typeof(TResult), "Partitioned", "Interval", partitionedIntervalObservable.source.Properties.IsColumnar);
                if (partitionedIntervalObservable.constructor != null)
                {
                    template.intervalFunction = (x, y, z) =>
                        partitionedIntervalObservable.constructor.Body.ExpressionToCSharpStringWithParameterSubstitution(
                        new Dictionary<ParameterExpression, string>
                        {
                            { partitionedIntervalObservable.constructor.Parameters[0], "colkey[i].Key" },
                            { partitionedIntervalObservable.constructor.Parameters[0], x },
                            { partitionedIntervalObservable.constructor.Parameters[1], y },
                            { partitionedIntervalObservable.constructor.Parameters[2], z },
                        });
                }
                var keyType = typeof(PartitionKey<>).MakeGenericType(typeof(TKey));
                var expandedCode = template.TransformText();

                var assemblyReferences = Transformer.AssemblyReferencesNeededFor(keyType, typeof(TPayload), typeof(TResult));
                assemblyReferences.Add(typeof(IStreamable<,>).GetTypeInfo().Assembly);
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<PartitionKey<TKey>, TPayload>());

                var a = Transformer.CompileSourceCode(expandedCode, assemblyReferences, out errorMessages);
                var t = a.GetType(template.className);
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
