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
    internal partial class UngroupedAfaTemplate : AfaTemplate
    {
        private UngroupedAfaTemplate(string className, Type payloadType, Type registerType, Type accumulatorType)
            : base(className, typeof(Empty), payloadType, registerType, accumulatorType)
        {
            if (Config.ForceRowBasedExecution)
            {
                this.sourceBatchTypeName = $"Microsoft.StreamProcessing.StreamMessage<Microsoft.StreamProcessing.Empty, {this.TPayload}>";
                this.resultBatchTypeName = $"Microsoft.StreamProcessing.StreamMessage<Microsoft.StreamProcessing.Empty, {this.TRegister}>";
            }
            else
            {
                this.sourceBatchTypeName = Transformer.GetBatchClassName(typeof(Empty), payloadType);
                this.resultBatchTypeName = Transformer.GetBatchClassName(typeof(Empty), registerType);
            }
        }

        internal static Tuple<Type, string> GenerateAFA<TPayload, TRegister, TAccumulator>(
            AfaStreamable<Empty, TPayload, TRegister, TAccumulator> stream)
        {
            Contract.Requires(stream != null);
            Contract.Ensures(Contract.Result<Tuple<Type, string>>() == null || typeof(UnaryPipe<Empty, TPayload, TRegister>).GetTypeInfo().IsAssignableFrom(Contract.Result<Tuple<Type, string>>().Item1));

            var className = $"GeneratedUngroupedAfa_{AFASequenceNumber++}";
            var template = new UngroupedAfaTemplate(className, typeof(TPayload), typeof(TRegister), typeof(TAccumulator))
            {
                isFinal = stream.afa.isFinal,
                hasOutgoingArcs = stream.afa.hasOutgoingArcs,
                startStates = stream.afa.startStates,
                AllowOverlappingInstances = stream.afa.uncompiledAfa.AllowOverlappingInstances
            };

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
                        var edgeInfo = CreateSingleEdgeInfo(stream, targetNodeNumber, searc);
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
                        var edgeInfo = CreateSingleEdgeInfo(stream, targetNode, searc);
                        edgeList2.Add(edgeInfo);
                    }
                }

                template.newActivationInfo.Add(Tuple.Create(startState, edgeList2));
            }
            template.isSyncTimeSimultaneityFree = stream.Properties.IsSyncTimeSimultaneityFree;

            return template.Generate<Empty, TPayload, TRegister, TAccumulator>();
        }
    }
}
