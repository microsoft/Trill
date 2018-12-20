// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Reflection;

namespace Microsoft.StreamProcessing
{
    internal partial class GroupedAfaTemplate : AfaTemplate
    {
        private Func<string, string, string> keyEqualityComparer;

        private GroupedAfaTemplate(string className, Type keyType, Type payloadType, Type registerType, Type accumulatorType)
            : base(className, keyType, payloadType, registerType, accumulatorType)
        {
            if (Config.ForceRowBasedExecution)
            {
                this.sourceBatchTypeName = string.Format("Microsoft.StreamProcessing.StreamMessage<{0}, {1}>", this.TKey, this.TPayload);
                this.resultBatchTypeName = string.Format("Microsoft.StreamProcessing.StreamMessage<{0}, {1}>", this.TKey, this.TRegister);
            }
            else
            {
                this.sourceBatchTypeName = Transformer.GetBatchClassName(keyType, payloadType);
                this.resultBatchTypeName = Transformer.GetBatchClassName(keyType, registerType);
            }
        }

        internal static Tuple<Type, string> GenerateAFA<TKey, TPayload, TRegister, TAccumulator>(
            AfaStreamable<TKey, TPayload, TRegister, TAccumulator> stream)
        {
            Contract.Requires(stream != null);
            Contract.Ensures(Contract.Result<Tuple<Type, string>>() == null || typeof(UnaryPipe<TKey, TPayload, TRegister>).GetTypeInfo().IsAssignableFrom(Contract.Result<Tuple<Type, string>>().Item1));

            string errorMessages = null;
            try
            {
                var className = string.Format("GeneratedGroupedAfa_{0}", AFASequenceNumber++);
                var template = new GroupedAfaTemplate(className, typeof(TKey), typeof(TPayload), typeof(TRegister), typeof(TAccumulator))
                {
                    TKey = typeof(TKey).GetCSharpSourceSyntax()
                };

                template.isFinal = stream.afa.isFinal;
                template.hasOutgoingArcs = stream.afa.hasOutgoingArcs;
                template.startStates = stream.afa.startStates;
                template.AllowOverlappingInstances = stream.afa.uncompiledAfa.AllowOverlappingInstances;

                var d1 = stream.afa.uncompiledAfa.transitionInfo;
                var orderedKeys = d1.Keys.OrderBy(e => e).ToArray();
                for (int i = 0; i < orderedKeys.Length; i++)
                {
                    var sourceNodeNumber = orderedKeys[i];
                    var outgoingEdgesDictionary = d1[sourceNodeNumber];
                    var orderedTargetNodes = outgoingEdgesDictionary.Keys.OrderBy(e => e).ToArray();
                    var edgeList1 = new List<EdgeInfo>();
                    for (int j = 0; j < orderedTargetNodes.Length; j++)
                    {
                        var targetNodeNumber = orderedTargetNodes[j];
                        var edge = outgoingEdgesDictionary[targetNodeNumber];
                        if (edge is SingleElementArc<TPayload, TRegister> searc)
                        {
                            var edgeInfo = CreateSingleEdgeInfo(stream, targetNodeNumber, searc, "i");
                            edgeList1.Add(edgeInfo);
                        }

                    }
                    template.currentlyActiveInfo.Add(Tuple.Create(sourceNodeNumber, edgeList1));
                }
                for (int i = 0; i < stream.afa.startStates.Length; i++)
                {
                    var startState = stream.afa.startStates[i];
                    var edgeList2 = new List<EdgeInfo>();
                    var outgoingEdgeDictionary = stream.afa.uncompiledAfa.transitionInfo[startState];
                    foreach (var edge in outgoingEdgeDictionary)
                    {
                        var targetNode = edge.Key;
                        var arc = edge.Value;
                        if (arc is SingleElementArc<TPayload, TRegister> searc)
                        {
                            var edgeInfo = CreateSingleEdgeInfo(stream, targetNode, searc, "i");
                            edgeList2.Add(edgeInfo);
                        }
                    }

                    template.newActivationInfo.Add(Tuple.Create(startState, edgeList2));
                }
                template.isSyncTimeSimultaneityFree = stream.Properties.IsSyncTimeSimultaneityFree;
                template.keyEqualityComparer =
                    (left, right) =>
                        stream.Properties.KeyEqualityComparer.GetEqualsExpr().Inline(left, right);

                var expandedCode = template.TransformText();

                var assemblyReferences = Transformer.AssemblyReferencesNeededFor(typeof(TKey), typeof(TPayload), typeof(TRegister));
                assemblyReferences.Add(typeof(IStreamable<,>).GetTypeInfo().Assembly);
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TPayload>());
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TRegister>());
                assemblyReferences.Add(Transformer.GeneratedMemoryPoolAssembly<TKey, TRegister>());

                var a = Transformer.CompileSourceCode(expandedCode, assemblyReferences, out errorMessages);
                var t = a.GetType(template.className);
                if (t.GetTypeInfo().IsGenericType)
                {
                    var list = typeof(TKey).GetAnonymousTypes();
                    list.AddRange(template.payloadType.GetAnonymousTypes());
                    list.AddRange(template.registerType.GetAnonymousTypes());
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
