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
using Microsoft.StreamProcessing.Internal;

namespace Microsoft.StreamProcessing
{
    internal partial class AggregateTemplate
    {
        protected Type keyType;
        protected Type inputType;
        protected Type stateType;
        protected Type outputType;
        private static int sequenceNumber = 0;

        protected IEnumerable<MyFieldInfo> inputFields;
        protected IEnumerable<MyFieldInfo> outputFields;

        protected Func<string, string, string> inlinedKeyComparerEquals;
        protected Func<string, string> inlinedKeyComparerGetHashCode;

        protected string initialState;
        protected Func<string, string, string> accumulate;
        protected Func<string, string, string> deaccumulate;
        protected Func<string, string, string> difference;
        protected Func<string, string> computeResult;
        protected bool useCompiledInitialState;
        protected bool useCompiledComputeResult;
        protected bool useCompiledAccumulate;
        protected bool useCompiledDeaccumulate;
        protected bool useCompiledDifference;
        protected bool isUngrouped;

        protected AggregateTemplate(string className) : base(className) { }

        public static Tuple<Type, string> Generate<TKey, TInput, TState, TOutput>(SnapshotWindowStreamable<TKey, TInput, TState, TOutput> stream, AggregatePipeType pipeType)
        {
            Contract.Requires(stream != null);
            Contract.Ensures(Contract.Result<Tuple<Type, string>>() == null || typeof(IStreamObserver<TKey, TInput>).GetTypeInfo().IsAssignableFrom(Contract.Result<Tuple<Type, string>>().Item1));

            var container = stream.Properties.QueryContainer;
            string generatedClassName = string.Format("Aggregate_{0}", sequenceNumber++);
            string expandedCode;
            List<Assembly> assemblyReferences;
            string errorMessages = null;

            try
            {
                AggregateTemplate template;
                switch (pipeType)
                {
                    case AggregatePipeType.StartEdge:
                        template = new SnapshotWindowStartEdgeTemplate(generatedClassName);
                        break;
                    case AggregatePipeType.PriorityQueue:
                        template = new SnapshotWindowPriorityQueueTemplate(generatedClassName);
                        break;
                    case AggregatePipeType.Tumbling:
                        template = new SnapshotWindowTumblingTemplate(generatedClassName);
                        break;
                    case AggregatePipeType.Sliding:
                        template = new SnapshotWindowSlidingTemplate(generatedClassName);
                        break;
                    case AggregatePipeType.Hopping:
                        template = new SnapshotWindowHoppingTemplate(
                            generatedClassName,
                            ((int)(stream.Source.Properties.ConstantDurationLength.Value / stream.Source.Properties.ConstantHopLength) + 1).ToString());
                        break;
                    default:
                        Contract.Assert(false, "case meant to be exhaustive");
                        throw new InvalidOperationException("case meant to be exhaustive");
                }

                template.isUngrouped = typeof(TKey) == typeof(Empty);
                var keyType = template.keyType = typeof(TKey);
                var inputType = template.inputType = typeof(TInput);
                var stateType = template.stateType = typeof(TState);
                var outputType = template.outputType = typeof(TOutput);

                template.inputFields = new ColumnarRepresentation(inputType).AllFields;

                template.outputFields = new ColumnarRepresentation(outputType).AllFields;

                assemblyReferences = new List<Assembly>();

                #region Key Comparer
                IEqualityComparerExpression<TKey> keyComparer;
                keyComparer = stream.Properties.KeyEqualityComparer;
                var equalsExpression = keyComparer.GetEqualsExpr();
                var getHashcodeExpression = keyComparer.GetGetHashCodeExpr();
                template.inlinedKeyComparerEquals =
                    (left, right) =>
                        $"({equalsExpression.Inline(left, right)})";
                template.inlinedKeyComparerGetHashCode =
                    (x) =>
                        $"({getHashcodeExpression.Inline(x)}/* inlined GetHashCode */)";
                if (keyType.IsAnonymousType())
                {
                    template.inlinedKeyComparerEquals =
                        (left, right) => $"keyComparerEquals({left}, {right})";
                    template.inlinedKeyComparerGetHashCode =
                        (x) => $"keyComparerGetHashCode({x})";
                }
                assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(equalsExpression));
                assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(getHashcodeExpression));
                #endregion

                #region Aggregate Functions
                var initialStateLambda = stream.Aggregate.InitialState();
                if (ConstantExpressionFinder.IsClosedExpression(initialStateLambda))
                {
                    template.initialState = initialStateLambda.Body.ExpressionToCSharp();
                    assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(initialStateLambda));
                }
                else
                {
                    if (Config.CodegenOptions.SuperStrictColumnar)
                    {
                        errorMessages = "Code Generation for Aggregate: couldn't inline the initial state lambda!";
                        throw new InvalidOperationException(errorMessages);
                    }
                    else
                    {
                        template.useCompiledInitialState = true;
                        template.initialState = "initialState()";
                    }
                }

                var accumulateLambda = stream.Aggregate.Accumulate();
                if (ConstantExpressionFinder.IsClosedExpression(accumulateLambda))
                {
                    var accTransformedLambda = Extensions.TransformFunction<TKey, TInput>(accumulateLambda, 2);
                    template.accumulate = (stateArg, longArg) => accTransformedLambda.Inline(stateArg, longArg);
                    assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(accumulateLambda));
                }
                else
                {
                    if (Config.CodegenOptions.SuperStrictColumnar)
                    {
                        errorMessages = "Code Generation for Aggregate: couldn't inline the accumulate lambda!";
                        throw new InvalidOperationException(errorMessages);
                    }
                    else
                    {
                        template.useCompiledAccumulate = true;
                        template.accumulate = (s1, s2) => $"accumulate({s1}, {s2}, inputBatch[i]);";
                    }
                }

                var deaccumulateLambda = stream.Aggregate.Deaccumulate();
                if (ConstantExpressionFinder.IsClosedExpression(deaccumulateLambda))
                {
                    var deaccumulateTransformedLambda = Extensions.TransformFunction<TKey, TInput>(deaccumulateLambda, 2);
                    template.deaccumulate = (stateArg, longArg) => deaccumulateTransformedLambda.Inline(stateArg, longArg);
                    assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(deaccumulateLambda));
                }
                else
                {
                    if (Config.CodegenOptions.SuperStrictColumnar)
                    {
                        errorMessages = "Code Generation for Aggregate: couldn't inline the deaccumulate lambda!";
                        throw new InvalidOperationException(errorMessages);
                    }
                    else
                    {
                        template.useCompiledDeaccumulate = true;
                        template.deaccumulate = (s1, s2) => $"deaccumulate({s1}, {s2}, inputBatch[i]);";
                    }
                }

                var differenceLambda = stream.Aggregate.Difference();
                if (ConstantExpressionFinder.IsClosedExpression(differenceLambda))
                {
                    template.difference = (stateArg1, stateArg2) => differenceLambda.Inline(stateArg1, stateArg2);
                    assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(differenceLambda));
                }
                else
                {
                    if (Config.CodegenOptions.SuperStrictColumnar)
                    {
                        errorMessages = "Code Generation for Aggregate: couldn't inline the difference lambda!";
                        throw new InvalidOperationException(errorMessages);
                    }
                    else
                    {
                        template.useCompiledDifference = true;
                        template.difference = (s1, s2) => $"difference({s1}, {s2});";
                    }
                }

                var computeResultLambda = stream.Aggregate.ComputeResult();
                if (ConstantExpressionFinder.IsClosedExpression(computeResultLambda))
                {
                    if (outputType.IsAnonymousType())
                    {
                        if (computeResultLambda.Body is NewExpression newExpression)
                        {
                            var outputBatchType = StreamMessageManager.GetStreamMessageType<TKey, TOutput>();
                            var foo = Transform(newExpression, outputBatchType);
                            template.computeResult = (stateArg) => Expression.Lambda(foo, computeResultLambda.Parameters.ToArray()).Inline(stateArg);
                        }
                        else
                        {
                            template.computeResult = (stateArg) => computeResultLambda.Inline(stateArg);
                        }
                    }
                    else
                    {
                        template.computeResult = (stateArg) => computeResultLambda.Inline(stateArg);
                    }
                    assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(computeResultLambda));
                }
                else
                {
                    if (Config.CodegenOptions.SuperStrictColumnar || outputType.IsAnonymousType())
                    {
                        // second disjunct is because if we aren't inlining the computeResult function and
                        // the output type is anonymous, calling the compiled computeResult function returns
                        // a value of the anonymous type and since the generated operator represents the anonymous
                        // type as a generic parameter, it can't use the "field" (i.e., property) names to
                        // get the individual pieces to assign to each column of the output message.
                        errorMessages = "Code Generation for Aggregate: couldn't inline the compute result lambda!";
                        throw new InvalidOperationException(errorMessages);
                    }
                    else
                    {
                        template.useCompiledComputeResult = true;
                        template.computeResult = (stateArg) => $"computeResult({stateArg});";
                    }
                }
                #endregion

                generatedClassName = generatedClassName.AddNumberOfNecessaryGenericArguments(keyType, inputType, stateType, outputType);
                expandedCode = template.TransformText();

                assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(typeof(TKey), typeof(TInput), typeof(TState), typeof(TOutput), typeof(FastDictionaryGenerator), typeof(SortedDictionary<,>)));
                assemblyReferences.Add(typeof(IStreamable<,>).GetTypeInfo().Assembly);
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TInput>());
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TOutput>());
                assemblyReferences.Add(Transformer.GeneratedMemoryPoolAssembly<TKey, TOutput>());
                if (container != null) assemblyReferences.AddRange(container.CollectedGeneratedTypes.Select(o => o.GetTypeInfo().Assembly));

                var a = Transformer.CompileSourceCode(expandedCode, assemblyReferences, out errorMessages);
                if (keyType.IsAnonymousType())
                {
                    if (errorMessages == null) errorMessages = string.Empty;
                    errorMessages += "\nCodegen Warning: The key type for an aggregate is an anonymous type (or contains an anonymous type), preventing the inlining of the key equality and hashcode functions. This may lead to poor performance.\n";
                }
                var t = a.GetType(generatedClassName);
                if (t.GetTypeInfo().IsGenericType)
                {
                    var list = typeof(TKey).GetAnonymousTypes();
                    list.AddRange(typeof(TInput).GetAnonymousTypes());
                    list.AddRange(typeof(TState).GetAnonymousTypes());
                    list.AddRange(typeof(TOutput).GetAnonymousTypes());
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

        /// <summary>
        /// The compute result function is e => new { f1 = e1, f2 = e2, ...}), i.e., creating an anonymous type
        /// Transform it into { dest_f1[c] := e1; dest_f2[c] := e2; ... }
        /// </summary>
        private static Expression/*?*/ Transform(NewExpression newExpression, Type outputBatchType)
        {
            Contract.Requires(newExpression != null);

            Contract.Assume(newExpression != null && newExpression.Type.IsAnonymousType());
            Contract.Assume(newExpression.Arguments != null);
            Contract.Assume(newExpression.Members != null);
            Contract.Assume(newExpression.Arguments.Count == newExpression.Members.Count);

            var assignments = new List<Expression>();
            var indexVariable = Expression.Variable(typeof(int), "c");
            var batch = Expression.Variable(outputBatchType, "batch");
            var outputFields = outputBatchType.GetTypeInfo().GetFields();
            if (outputFields == null || outputFields.Length == 0) return null; // can this really happen?
            for (int i = 0; i < newExpression.Arguments.Count; i++)
            {
                var member = newExpression.Members[i];
                var destinationField = member as PropertyInfo; // assignments to "fields" of an anonymous type are really to its properties
                if (destinationField == null) return null; // can this really happen? what else could it be?
                var argument = newExpression.Arguments[i];
                var columnBatchField = outputFields.Where(e => e.Name == Transformer.ColumnFieldPrefix + destinationField.Name).SingleOrDefault();
                if (columnBatchField == null) return null; // this also should be an error, shouldn't be able to happen.
                var columnBatch = Expression.MakeMemberAccess(batch, columnBatchField);
                var columnBatchType = typeof(ColumnBatch<>).MakeGenericType(destinationField.PropertyType);
                var arrayInColumnBatch = Expression.MakeMemberAccess(columnBatch, columnBatchType.GetTypeInfo().GetField("col"));
                var lhs = Expression.ArrayAccess(arrayInColumnBatch, indexVariable);
                var assign = Expression.Assign(lhs, argument);
                assignments.Add(assign);
            }
            var be = Expression.Block(assignments);
            return be;
        }
    }

    internal partial class SnapshotWindowStartEdgeTemplate
    {
        public SnapshotWindowStartEdgeTemplate(string className) : base(className) { }
    }

    internal partial class SnapshotWindowPriorityQueueTemplate
    {
        public SnapshotWindowPriorityQueueTemplate(string className) : base(className) { }
    }

    internal partial class SnapshotWindowTumblingTemplate
    {
        public SnapshotWindowTumblingTemplate(string className) : base(className) { }
    }

    internal partial class SnapshotWindowSlidingTemplate
    {
        public SnapshotWindowSlidingTemplate(string className) : base(className) { }
    }

    internal partial class SnapshotWindowHoppingTemplate
    {
        private readonly string hopsPerDuration;

        public SnapshotWindowHoppingTemplate(string className, string hopsPerDuration) : base(className)
            => this.hopsPerDuration = hopsPerDuration;
    }
}
