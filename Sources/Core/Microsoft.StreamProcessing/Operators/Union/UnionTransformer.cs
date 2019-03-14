// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Reflection;

namespace Microsoft.StreamProcessing
{
    internal partial class UnionTemplate
    {
        private static int UnionSequenceNumber = 0;
        private string genericParameters = string.Empty;
        private string GeneratedBatchName;
        private IEnumerable<MyFieldInfo> fields;

        private UnionTemplate(string className, Type keyType, Type payloadType)
            : base(className, keyType, payloadType, payloadType, payloadType) { }

        /// <summary>
        /// Generate a batch class definition to be used as a Union pipe.
        /// Compile the definition, dynamically load the assembly containing it, and return the Type representing the
        /// union pipe class.
        /// </summary>
        /// <typeparam name="TKey">The key type for both sides.</typeparam>
        /// <typeparam name="TPayload">The payload type.</typeparam>
        /// <returns>
        /// A type that is defined to be a subtype of BinaryPipe&lt;<typeparamref name="TKey"/>,<typeparamref name="TPayload"/>, <typeparamref name="TPayload"/>, <typeparamref name="TKey"/>, <typeparamref name="TPayload"/>&gt;.
        /// </returns>
        internal static Tuple<Type, string> GenerateUnionPipeClass<TKey, TPayload>(
            UnionStreamable<TKey, TPayload> stream)
        {
            Contract.Requires(stream != null);
            Contract.Ensures(Contract.Result<Tuple<Type, string>>() == null || typeof(BinaryPipe<TKey, TPayload, TPayload, TPayload>).GetTypeInfo().IsAssignableFrom(Contract.Result<Tuple<Type, string>>().Item1));

#if CODEGEN_TIMING
          Stopwatch sw = new Stopwatch();
          sw.Start();
#endif
            var template = new UnionTemplate($"GeneratedUnion_{UnionSequenceNumber++}", typeof(TKey), typeof(TPayload));

            var gps = template.tm.GenericTypeVariables(template.keyType, template.resultType);
            template.genericParameters = gps.BracketedCommaSeparatedString();

            var resultRepresentation = new ColumnarRepresentation(template.resultType);

            var batchGeneratedFrom_TKey_TPayload = Transformer.GetBatchClassName(template.keyType, template.resultType);
            var keyAndPayloadGenericParameters = template.tm.GenericTypeVariables(template.keyType, template.resultType).BracketedCommaSeparatedString();
            template.GeneratedBatchName = batchGeneratedFrom_TKey_TPayload + keyAndPayloadGenericParameters;

            template.fields = resultRepresentation.AllFields;

            return template.Generate<TKey, TPayload>();
        }
    }
}
