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
    internal partial class GroupedAfaMultiEventTemplate : AfaTemplate
    {
        private Func<string, string, string> keyEqualityComparer;
        protected readonly List<Tuple<int, List<MultiEdgeInfo>>> edgeInfos = new List<Tuple<int, List<MultiEdgeInfo>>>();
        protected readonly List<Tuple<int, List<MultiEdgeInfo>>> startEdgeInfos = new List<Tuple<int, List<MultiEdgeInfo>>>();

        private GroupedAfaMultiEventTemplate(string className, Type keyType, Type payloadType, Type registerType, Type accumulatorType)
            : base(className, keyType, payloadType, registerType, accumulatorType)
        {
            this.TKey = keyType.GetCSharpSourceSyntax();

            if (Config.ForceRowBasedExecution)
            {
                this.sourceBatchTypeName = $"Microsoft.StreamProcessing.StreamMessage<{this.TKey}, {this.TPayload}>";
                this.resultBatchTypeName = $"Microsoft.StreamProcessing.StreamMessage<{this.TKey}, {this.TRegister}>";
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

            var className = $"GeneratedGroupedAfaMultiEvent_{AFASequenceNumber++}";
            var template = new GroupedAfaMultiEventTemplate(className, typeof(TKey), typeof(TPayload), typeof(TRegister), typeof(TAccumulator))
            {
                isFinal = stream.afa.isFinal,
                hasOutgoingArcs = stream.afa.hasOutgoingArcs,
                startStates = stream.afa.startStates,
                AllowOverlappingInstances = stream.afa.uncompiledAfa.AllowOverlappingInstances,
                isSyncTimeSimultaneityFree = true, // The handwritten version doesn't make a distinction.
                keyEqualityComparer =
                    (left, right) =>
                        stream.Properties.KeyEqualityComparer.GetEqualsExpr().Inline(left, right),
            };

            var d1 = stream.afa.uncompiledAfa.transitionInfo;
            var orderedKeys = d1.Keys.OrderBy(e => e).ToArray();
            for (int i = 0; i < orderedKeys.Length; i++)
            {
                var sourceNodeNumber = orderedKeys[i];
                var outgoingEdgesDictionary = d1[sourceNodeNumber];
                var orderedTargetNodes = outgoingEdgesDictionary.Keys.OrderBy(e => e).ToArray();
                var edgeList = new List<MultiEdgeInfo>();
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
                            Accumulate = (ts, ev, reg, acc) =>
                            {
                                var transformedAccumulate = Extensions.TransformFunction<TKey, TPayload>(multiArc.Accumulate, 1, batchVariableName: "sourceBatch");
                                return transformedAccumulate.Inline(ts, reg, acc);
                            },
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
                    }
                }
                template.edgeInfos.Add(Tuple.Create(sourceNodeNumber, edgeList));
            }
            for (int i = 0; i < stream.afa.startStates.Length; i++)
            {
                var startState = stream.afa.startStates[i];
                var edgeList2 = new List<MultiEdgeInfo>();
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
                            Accumulate = (ts, ev, reg, acc) =>
                            {
                                var transformedAccumulate = Extensions.TransformFunction<TKey, TPayload>(multiArc.Accumulate, 1, batchVariableName: "sourceBatch");
                                return transformedAccumulate.Inline(ts, reg, acc);
                            },
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
                    }
                }

                template.startEdgeInfos.Add(Tuple.Create(startState, edgeList2));
            }

            return template.Generate<TKey, TPayload, TRegister, TAccumulator>();
        }
    }
}
