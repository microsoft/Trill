// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    internal partial class TemporalIngressTemplate
    {
        private static int TemporalIngressSequenceNumber = 0;
        private string genericParameters = string.Empty;
        private string GeneratedBatchName;
        private string payloadOrResult;
        private string keyOrNothing;
        private string emptyOrPartition;
        private string genericArguments;
        private string adjustedGenericArgs;
        private string partitionString;
        private string inheritBase;
        private string ingressType;
        private string latencyOption;
        private string baseStructure;
        private bool needsStreamEvent;
        private string diagnosticOption;
        private string fusionOption;
        private string leadingText;
        private string trailingText;
        private string valueString = "value.Payload";
        private string generatedEndTimeVariable = "value.OtherTime";
        private bool resultMightBeNull;
        private Func<string, string> partitionFunction;
        private Func<string, string> startEdgeFunction;
        private Func<string, string> endEdgeFunction;

        private TemporalIngressTemplate(string className, Type keyType, Type payloadType, Type resultType)
            : base(className, keyType, payloadType, resultType, true) { }

        internal static Tuple<Type, string> Generate<TSource>(
            Expression<Func<TSource, long>> startEdgeExpression,
            Expression<Func<TSource, long>> endEdgeExpression,
            string latencyOption,
            string diagnosticOption,
            FuseModule fuseModule)
            => Generate<Empty, TSource, TSource>("Interval", string.Empty, null, startEdgeExpression, endEdgeExpression,
                latencyOption, diagnosticOption, fuseModule);

        internal static Tuple<Type, string> GenerateFused<TSource, TResult>(
            Expression<Func<TSource, long>> startEdgeExpression,
            Expression<Func<TSource, long>> endEdgeExpression,
            string latencyOption,
            string diagnosticOption,
            FuseModule fuseModule)
            => Generate<Empty, TSource, TResult>("Interval", string.Empty, null, startEdgeExpression, endEdgeExpression,
                latencyOption, diagnosticOption, fuseModule);

        internal static Tuple<Type, string> Generate<TKey, TSource>(
            Expression<Func<TSource, TKey>> partitionExpression,
            Expression<Func<TSource, long>> startEdgeExpression,
            Expression<Func<TSource, long>> endEdgeExpression,
            string latencyOption,
            string diagnosticOption,
            FuseModule fuseModule)
            => Generate<TKey, TSource, TSource>("Interval", "Partitioned", partitionExpression, startEdgeExpression, endEdgeExpression,
                latencyOption, diagnosticOption, fuseModule);

        internal static Tuple<Type, string> GenerateFused<TKey, TSource, TResult>(
            Expression<Func<TSource, TKey>> partitionExpression,
            Expression<Func<TSource, long>> startEdgeExpression,
            Expression<Func<TSource, long>> endEdgeExpression,
            string latencyOption,
            string diagnosticOption,
            FuseModule fuseModule)
            => Generate<TKey, TSource, TResult>("Interval", "Partitioned", partitionExpression, startEdgeExpression, endEdgeExpression,
                latencyOption, diagnosticOption, fuseModule);

        internal static Tuple<Type, string> Generate<TSource>(
            string latencyOption,
            string diagnosticOption,
            FuseModule fuseModule)
            => Generate<Empty, TSource, TSource>("StreamEvent", string.Empty, null, null, null,
                latencyOption, diagnosticOption, fuseModule);

        internal static Tuple<Type, string> GenerateFused<TSource, TResult>(
            string latencyOption,
            string diagnosticOption,
            FuseModule fuseModule)
            => Generate<Empty, TSource, TResult>("StreamEvent", string.Empty, null, null, null,
                latencyOption, diagnosticOption, fuseModule);

        internal static Tuple<Type, string> Generate<TKey, TSource>(
            string latencyOption,
            string diagnosticOption,
            FuseModule fuseModule)
            => Generate<TKey, TSource, TSource>("StreamEvent", "Partitioned", null, null, null,
                latencyOption, diagnosticOption, fuseModule);

        internal static Tuple<Type, string> GenerateFused<TKey, TSource, TResult>(
            string latencyOption,
            string diagnosticOption,
            FuseModule fuseModule)
            => Generate<TKey, TSource, TResult>("StreamEvent", "Partitioned", null, null, null,
                latencyOption, diagnosticOption, fuseModule);

        private static Tuple<Type, string> Generate<TKey, TSource, TResult>(
            string ingressType,
            string partitionString,
            Expression<Func<TSource, TKey>> partitionExpression,
            Expression<Func<TSource, long>> startEdgeExpression,
            Expression<Func<TSource, long>> endEdgeExpression,
            string latencyOption,
            string diagnosticOption,
            FuseModule fuseModule)
        {
            var template = new TemporalIngressTemplate(
                $"GeneratedTemporalIngress_{TemporalIngressSequenceNumber++}",
                typeof(TKey), typeof(TSource), fuseModule?.OutputType ?? typeof(TResult));

            var tm = new TypeMapper(template.keyType, template.payloadType, template.resultType);
            var gps = tm.GenericTypeVariables(template.keyType, template.payloadType);
            template.genericParameters = gps.BracketedCommaSeparatedString();
            template.payloadOrResult = tm.CSharpNameFor(template.resultRepresentation.RepresentationFor);

            var batchGeneratedFrom_TKey_TPayload = string.IsNullOrEmpty(partitionString)
                ? Transformer.GetBatchClassName(template.keyType, template.resultType)
                : Transformer.GetBatchClassName(typeof(PartitionKey<TKey>), template.resultType);
            var keyAndPayloadGenericParameters = tm.GenericTypeVariables(template.keyType, template.resultType).BracketedCommaSeparatedString();
            template.GeneratedBatchName = batchGeneratedFrom_TKey_TPayload + keyAndPayloadGenericParameters;

            template.keyOrNothing = string.IsNullOrEmpty(partitionString) ? string.Empty : template.TKey + ", ";
            template.diagnosticOption = diagnosticOption;
            template.needsStreamEvent = ingressType == "StreamEvent";
            template.genericArguments = (string.IsNullOrEmpty(partitionString) ? string.Empty : template.TKey + ", ") + template.TPayload;
            template.adjustedGenericArgs = (string.IsNullOrEmpty(partitionString) ? string.Empty : template.TKey + ", ") + (!fuseModule.IsEmpty && Config.AllowFloatingReorderPolicy ? template.TResult : template.TPayload);
            template.emptyOrPartition = string.IsNullOrEmpty(partitionString) ? "Microsoft.StreamProcessing.Empty.Default" : "new PartitionKey<" + template.TKey + ">(value.PartitionKey)";
            template.partitionString = partitionString;
            template.baseStructure = partitionString + "StreamEvent<" + template.genericArguments + ">";
            template.inheritBase = (ingressType != "StreamEvent") ? template.TPayload : template.baseStructure;
            template.ingressType = ingressType;
            template.latencyOption = latencyOption;

            template.partitionFunction = x => partitionExpression == null ?
                    string.Empty : partitionExpression.Body.ExpressionToCSharpStringWithParameterSubstitution(
                                        new Dictionary<ParameterExpression, string>
                                                    {
                                                        { partitionExpression.Parameters.Single(), x }
                                                    });
            template.startEdgeFunction = x => startEdgeExpression == null ?
                    string.Empty : startEdgeExpression.Body.ExpressionToCSharpStringWithParameterSubstitution(
                                        new Dictionary<ParameterExpression, string>
                                                    {
                                                        { startEdgeExpression.Parameters.Single(), x }
                                                    });
            template.endEdgeFunction = x => endEdgeExpression == null ?
                    "StreamEvent.InfinitySyncTime" :
                endEdgeExpression.Body.ExpressionToCSharpStringWithParameterSubstitution(
                                        new Dictionary<ParameterExpression, string>
                                                    {
                                                        { endEdgeExpression.Parameters.Single(), x }
                                                    });

            template.fusionOption = fuseModule.IsEmpty ? "Simple" :
                (Config.AllowFloatingReorderPolicy ? "Disordered" : "Fused");
            template.resultMightBeNull = template.resultType.CanContainNull();

            Expression[] expressions = null;
            if (!fuseModule.IsEmpty)
            {
                template.valueString = fuseModule.Coalesce<TSource, TResult, TKey>("value.SyncTime", "value.OtherTime", "value.Payload", template.emptyOrPartition, out template.leadingText, out template.trailingText);
                template.generatedEndTimeVariable = "generatedEndTimeVariable";
                if (Config.AllowFloatingReorderPolicy)
                {
                    template.valueString = "value.Payload";
                    template.generatedEndTimeVariable = "value.OtherTime";
                }

                expressions = fuseModule.GetCodeGenExpressions();
            }

            return template.Generate<TKey, TSource, TResult>(new Type[] { typeof(IStreamable<,>) }, expressions);
        }
    }
}