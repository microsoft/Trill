// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Reflection;

namespace Microsoft.StreamProcessing
{
    internal partial class AfaTemplate
    {
        protected static int AFASequenceNumber = 0;
        protected Type keyType;
        protected Type payloadType;
        protected Type registerType;
        protected Type accumulatorType;
        protected bool hasRegister;
        protected bool isSyncTimeSimultaneityFree;
        protected readonly List<Tuple<int, List<EdgeInfo>>> currentlyActiveInfo = new List<Tuple<int, List<EdgeInfo>>>();
        protected readonly List<Tuple<int, List<EdgeInfo>>> newActivationInfo = new List<Tuple<int, List<EdgeInfo>>>();
        protected string TKey;
        protected string TPayload;
        protected string TRegister;
        protected string TAccumulator;
        protected bool[] isFinal;
        protected bool[] hasOutgoingArcs;
        protected int[] startStates;
        protected IEnumerable<MyFieldInfo> sourceFields;
        protected IEnumerable<MyFieldInfo> resultFields;
        protected string sourceBatchTypeName;
        protected string resultBatchTypeName;
        protected bool noPublicResultFields;
        protected bool AllowOverlappingInstances;

        internal AfaTemplate(string className, Type keyType, Type payloadType, Type registerType, Type accumulatorType)
            : base(className)
        {
            this.keyType = keyType;
            this.payloadType = payloadType;
            this.registerType = registerType;
            this.accumulatorType = accumulatorType;
            this.hasRegister = !registerType.Equals(typeof(Empty));
            this.TKey = keyType.GetCSharpSourceSyntax();
            this.TPayload = payloadType.GetCSharpSourceSyntax();
            this.TRegister = registerType.GetCSharpSourceSyntax();
            this.TAccumulator = accumulatorType.GetCSharpSourceSyntax();
            if (Config.ForceRowBasedExecution)
            {
                // then need to use the field "payload" that is defined on the generic StreamMessage
                this.sourceFields = new MyFieldInfo[] { new MyFieldInfo(payloadType, "payload") };
            }
            else
            {
                this.sourceFields = payloadType.GetAnnotatedFields().Item1;
            }
            var resultFieldInfo = registerType.GetAnnotatedFields();
            this.resultFields = resultFieldInfo.Item1;
            this.noPublicResultFields = resultFieldInfo.Item2;
        }

        protected Tuple<Type, string> Generate<TKey, TPayload, TRegister, TAccumulator>()
        {
            string errorMessages = null;
            try
            {
                var expandedCode = TransformText();

                var assemblyReferences = Transformer.AssemblyReferencesNeededFor(
                    typeof(TKey), typeof(TPayload), typeof(TRegister), typeof(Stack<>), typeof(IStreamable<,>));
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TPayload>());
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TRegister>());
                assemblyReferences.Add(Transformer.GeneratedMemoryPoolAssembly<TKey, TRegister>());

                var a = Transformer.CompileSourceCode(expandedCode, assemblyReferences, out errorMessages);
                var t = a.GetType(this.className);
                if (t.GetTypeInfo().IsGenericType)
                {
                    var list = typeof(TKey).GetAnonymousTypes();
                    list.AddRange(this.payloadType.GetAnonymousTypes());
                    list.AddRange(this.registerType.GetAnonymousTypes());
                    t = t.MakeGenericType(list.ToArray());
                }
                return Tuple.Create(t, errorMessages);
            }
            catch (Exception e)
            {
                if (Config.CodegenOptions.DontFallBackToRowBasedExecution)
                    throw new InvalidOperationException("Code Generation failed when it wasn't supposed to!", e);

                return Tuple.Create((Type)null, errorMessages);
            }
        }

        internal class EdgeInfo
        {
            public enum EdgeType { Single, List, Multi, }
            public EdgeType Type;
            public int SourceNode;
            public Func<string, string, string, string> Fence;
            public Func<string, string, string, string> Transfer;
            public List<int> EpsilonReachableNodes;

        }

        internal sealed class MultiEdgeInfo : EdgeInfo
        {
            public int TargetNode;
            public bool fromStartState = false;

            public Func<string, string, string> Initialize;
            public Func<string, string, string, string, string> Accumulate;
            public Func<string, string> Dispose;
            public Func<string, string, string, string> SkipToEnd;

            public MultiEdgeInfo() => this.Type = EdgeType.Multi;
        }

        protected static List<int> EpsilonClosure<TPayload, TRegister, TAccumulator>(CompiledAfa<TPayload, TRegister, TAccumulator> afa, int node)
        {
            var result = new List<int> { node, };
            if (afa.epsilonStateMap != null)
            {
                EpsilonClosureHelper(afa, node, result);
            }
            return result;
        }

        protected static EdgeInfo CreateSingleEdgeInfo<TKey, TPayload, TRegister, TAccumulator>(AfaStreamable<TKey, TPayload, TRegister, TAccumulator> stream, int targetNodeNumber, SingleElementArc<TPayload, TRegister> searc)
        {
            var edgeInfo = new EdgeInfo()
            {
                Type = EdgeInfo.EdgeType.Single,
                EpsilonReachableNodes = EpsilonClosure(stream.afa, targetNodeNumber),
                SourceNode = targetNodeNumber,
                Fence = (ts, ev, reg) => searc.Fence.Inline(ts, ev, reg),
            };
            edgeInfo.Transfer = searc.Transfer == null
                ? (Func<string, string, string, string>)null
                : ((ts, ev, reg) => searc.Transfer.Inline(ts, ev, reg));
            return edgeInfo;
        }

        protected static EdgeInfo CreateListEdgeInfo<TKey, TPayload, TRegister, TAccumulator>(AfaStreamable<TKey, TPayload, TRegister, TAccumulator> stream, int targetNodeNumber, ListElementArc<TPayload, TRegister> edge)
        {
            var edgeInfo = new EdgeInfo()
            {
                Type = EdgeInfo.EdgeType.List,
                EpsilonReachableNodes = EpsilonClosure(stream.afa, targetNodeNumber),
                SourceNode = targetNodeNumber,
                Fence = (ts, evs, reg) => edge.Fence.Inline(ts, evs, reg),
                Transfer = edge.Transfer == null ? ((Func<string, string, string, string>)null) : (ts, evs, reg) => edge.Transfer.Inline(ts, evs, reg),
            };
            return edgeInfo;
        }

        protected void UpdateRegisterValue(EdgeInfo e, string defaultRegisterValue, string ts, string payloadList, string reg)
        {
            var newRegisterValue = !this.hasRegister || e.Transfer == null
                ? defaultRegisterValue
                : e.Transfer(ts, payloadList, reg);
            WriteLine("{0}var newReg = {1};", this.CurrentIndent, newRegisterValue);
            return;
        }

        protected void UpdateRegisterValue(MultiEdgeInfo e, string defaultRegisterValue, string ts, string acc, string reg)
        {
            var newRegisterValue = e.Transfer == null
                ? defaultRegisterValue
                : e.Transfer(ts, acc, reg);
            WriteLine("{0}var newReg = {1};", this.CurrentIndent, newRegisterValue);
            return;
        }

        private static void EpsilonClosureHelper<TPayload, TRegister, TAccumulator>(CompiledAfa<TPayload, TRegister, TAccumulator> afa, int node, List<int> accumulator)
        {
            if (afa.epsilonStateMap[node] != null)
            {
                for (int i = 0; i < afa.epsilonStateMap[node].Length; i++)
                {
                    var epsilonReachableNode = afa.epsilonStateMap[node][i];
                    accumulator.Add(epsilonReachableNode);
                    EpsilonClosureHelper(afa, epsilonReachableNode, accumulator);
                }
            }
            return;
        }

        protected static string BeginColumnPointerDeclaration(MyFieldInfo f, string batchName)
            => f.canBeFixed
                ? string.Format("fixed ({0}* {2}_{1}_col = {2}.{1}.col) {{", f.TypeName, f.Name, batchName)
                : f.OptimizeString()
                    ? string.Format("var {1}_{0}_col = {1}.{0};", f.Name, batchName)
                    : string.Format("var {1}_{0}_col = {1}.{0}.col;", f.Name, batchName);

        protected static string EndColumnPointerDeclaration(MyFieldInfo f) => f.canBeFixed ? "}" : string.Empty;

        protected static string ColumnPointerFieldDeclaration(MyFieldInfo f, string batchName)
            => f.OptimizeString()
                ? string.Format("Microsoft.StreamProcessing.Internal.Collections.Multistring {1}_{0}_col;", f.Name, batchName)
                : string.Format("{2} {1}_{0}_col;", f.Name, batchName, f.Type.MakeArrayType().GetCSharpSourceSyntax());

        protected static string ColumnPointerFieldAssignment(MyFieldInfo f, string batchName)
            => f.OptimizeString()
                ? string.Format("this.{1}_{0}_col = {1}.{0};", f.Name, batchName)
                : string.Format("this.{1}_{0}_col = {1}.{0}.col;", f.Name, batchName);
    }
}
