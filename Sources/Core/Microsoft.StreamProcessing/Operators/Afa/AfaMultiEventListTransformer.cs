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
    internal partial class AfaMultiEventListTemplate : AfaTemplate
    {
        private Func<string, string, string> keyEqualityComparer;
        protected readonly List<Tuple<int, List<EdgeInfo>>> edgeInfos = new List<Tuple<int, List<EdgeInfo>>>();
        protected readonly List<Tuple<int, List<EdgeInfo>>> startEdgeInfos = new List<Tuple<int, List<EdgeInfo>>>();
        private bool payloadIsAnon;
        private bool payloadHasNoFields;

        private AfaMultiEventListTemplate(string className, Type keyType, Type payloadType, Type registerType, Type accumulatorType)
            : base(className, keyType, payloadType, registerType, accumulatorType)
        {
            if (Config.ForceRowBasedExecution)
            {
                this.sourceBatchTypeName = $"Microsoft.StreamProcessing.StreamMessage<{this.TKey}, {this.TPayload}>";
                this.resultBatchTypeName = $"Microsoft.StreamProcessing.StreamMessage<{this.TKey}, {this.TPayload}>";
            }
            else
            {
                this.sourceBatchTypeName = Transformer.GetBatchClassName(keyType, payloadType);
                this.resultBatchTypeName = Transformer.GetBatchClassName(keyType, registerType);
            }

            this.TAccumulator = accumulatorType.GetCSharpSourceSyntax();
        }

        internal static Tuple<Type, string> GenerateAFA<TKey, TPayload, TRegister, TAccumulator>(
            AfaStreamable<TKey, TPayload, TRegister, TAccumulator> stream)
        {
            Contract.Requires(stream != null);
            Contract.Ensures(Contract.Result<Tuple<Type, string>>() == null || typeof(UnaryPipe<TKey, TPayload, TRegister>).GetTypeInfo().IsAssignableFrom(Contract.Result<Tuple<Type, string>>().Item1));

            var className = string.Format("Generated{1}AfaMultiEventList_{0}", AFASequenceNumber++, typeof(TKey).Equals(typeof(Empty)) ? string.Empty : "Grouped");
            var template = new AfaMultiEventListTemplate(className, typeof(TKey), typeof(TPayload), typeof(TRegister), typeof(TAccumulator))
            {
                TKey = typeof(TKey).GetCSharpSourceSyntax()
            };
            var payloadRepresentation = new ColumnarRepresentation(typeof(TPayload));

            template.isFinal = stream.afa.isFinal;
            template.hasOutgoingArcs = stream.afa.hasOutgoingArcs;
            template.startStates = stream.afa.startStates;
            template.AllowOverlappingInstances = stream.afa.uncompiledAfa.AllowOverlappingInstances;
            template.payloadIsAnon = typeof(TPayload).IsAnonymousTypeName();
            template.payloadHasNoFields = payloadRepresentation.noFields;

            var d1 = stream.afa.uncompiledAfa.transitionInfo;
            var orderedKeys = d1.Keys.OrderBy(e => e).ToArray();
            for (int i = 0; i < orderedKeys.Length; i++)
            {
                var sourceNodeNumber = orderedKeys[i];
                var outgoingEdgesDictionary = d1[sourceNodeNumber];
                var orderedTargetNodes = outgoingEdgesDictionary.Keys.OrderBy(e => e).ToArray();
                var edgeList = new List<EdgeInfo>();
                for (int j = 0; j < orderedTargetNodes.Length; j++)
                {
                    var targetNodeNumber = orderedTargetNodes[j];
                    var edge = outgoingEdgesDictionary[targetNodeNumber];
                    if (edge is MultiElementArc<TPayload, TRegister, TAccumulator> multiArc)
                    {
                        var multiEdgeInfo = new MultiEdgeInfo()
                        {
                            SourceNode = sourceNodeNumber,
                            TargetNode = targetNodeNumber,
                            fromStartState = stream.afa.startStates.Any(n => n == sourceNodeNumber),
                            EpsilonReachableNodes = EpsilonClosure(stream.afa, targetNodeNumber),
                            Initialize = (ts, reg) => multiArc.Initialize.Inline(ts, reg),
                            Accumulate = (ts, ev, reg, acc) => multiArc.Accumulate.Inline(ts, ev, reg, acc),
                            Fence = (ts, acc, reg) => multiArc.Fence.Inline(ts, acc, reg),
                        };

                        multiEdgeInfo.Transfer = multiArc.Transfer == null
                            ? (Func<string, string, string, string>)null
                            : ((ts, acc, reg) => multiArc.Transfer.Inline(ts, acc, reg));

                        if (multiArc.Dispose == null)
                        {
                            multiEdgeInfo.Dispose = (acc) => "// no dispose function";
                        }
                        else
                        {
                            multiEdgeInfo.Dispose = (acc) => multiArc.Dispose.Inline(acc);
                        }

                        multiEdgeInfo.SkipToEnd = multiArc.SkipToEnd == null
                            ? (Func<string, string, string, string>)null
                            : ((ts, ev, acc) => multiArc.SkipToEnd.Inline(ts, ev, acc));

                        edgeList.Add(multiEdgeInfo);
                        continue;
                    }
                    if (edge is SingleElementArc<TPayload, TRegister> singleArc)
                    {
                        var edgeInfo = CreateSingleEdgeInfo(stream, targetNodeNumber, singleArc);
                        edgeList.Add(edgeInfo);
                        continue;
                    }
                    if (edge is ListElementArc<TPayload, TRegister> listArc)
                    {
                        var edgeInfo = CreateListEdgeInfo(stream, targetNodeNumber, listArc);
                        edgeList.Add(edgeInfo);
                        continue;
                    }

                }
                template.edgeInfos.Add(Tuple.Create(sourceNodeNumber, edgeList));
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
                    if (arc is MultiElementArc<TPayload, TRegister, TAccumulator> multiArc)
                    {
                        var eps = EpsilonClosure(stream.afa, targetNode);

                        var multiEdgeInfo = new MultiEdgeInfo()
                        {
                            SourceNode = startState,
                            TargetNode = targetNode,
                            fromStartState = true,
                            EpsilonReachableNodes = EpsilonClosure(stream.afa, targetNode),
                            Initialize = (ts, reg) => multiArc.Initialize.Inline(ts, reg),
                            Accumulate = (ts, ev, reg, acc) => multiArc.Accumulate.Inline(ts, ev, reg, acc),
                            Fence = (ts, acc, reg) => multiArc.Fence.Inline(ts, acc, reg),
                        };

                        multiEdgeInfo.Transfer = multiArc.Transfer == null
                            ? (Func<string, string, string, string>)null
                            : ((ts, acc, reg) => multiArc.Transfer.Inline(ts, acc, reg));

                        if (multiArc.Dispose == null)
                        {
                            multiEdgeInfo.Dispose = (acc) => "// no dispose function";
                        }
                        else
                        {
                            multiEdgeInfo.Dispose = (acc) => multiArc.Dispose.Inline(acc);
                        }

                        multiEdgeInfo.SkipToEnd = multiArc.SkipToEnd == null
                            ? (Func<string, string, string, string>)null
                            : ((ts, ev, acc) => multiArc.SkipToEnd.Inline(ts, ev, acc));

                        edgeList2.Add(multiEdgeInfo);
                        continue;
                    }
                    if (arc is SingleElementArc<TPayload, TRegister> singleArc)
                    {
                        var edgeInfo = CreateSingleEdgeInfo(stream, targetNode, singleArc);
                        edgeList2.Add(edgeInfo);
                        continue;
                    }
                    if (arc is ListElementArc<TPayload, TRegister> listArc)
                    {
                        var edgeInfo = CreateListEdgeInfo(stream, targetNode, listArc);
                        edgeList2.Add(edgeInfo);
                        continue;
                    }
                }

                template.startEdgeInfos.Add(Tuple.Create(startState, edgeList2));
            }

            template.isSyncTimeSimultaneityFree = true; // The handwritten version doesn't make a distinction.
            template.keyEqualityComparer =
                (left, right) =>
                    stream.Properties.KeyEqualityComparer.GetEqualsExpr().Inline(left, right);

            return template.Generate<TKey, TPayload, TRegister, TAccumulator>();
        }
    }
}
