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
        private Type keyType;
        private Type payloadType;
        private string TKey;
        private string TPayload;
        private string className;
        private string genericParameters = string.Empty;
        private string GeneratedBatchName;
        private IEnumerable<MyFieldInfo> fields;
        private string staticCtor;

        private UnionTemplate() { }

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
            string errorMessages = null;
            try
            {
                var template = new UnionTemplate();

                var keyType = template.keyType = typeof(TKey);
                var payloadType = template.payloadType = typeof(TPayload);

                var tm = new TypeMapper(keyType, payloadType);
                template.TKey = tm.CSharpNameFor(keyType);
                template.TPayload = tm.CSharpNameFor(payloadType);
                var gps = tm.GenericTypeVariables(keyType, payloadType);
                template.genericParameters = gps.BracketedCommaSeparatedString();

                template.className = string.Format("GeneratedUnion_{0}", UnionSequenceNumber++);

                var resultRepresentation = new ColumnarRepresentation(payloadType);

                var batchGeneratedFrom_TKey_TPayload = Transformer.GetBatchClassName(keyType, payloadType);
                var keyAndPayloadGenericParameters = tm.GenericTypeVariables(keyType, payloadType).BracketedCommaSeparatedString();
                template.GeneratedBatchName = batchGeneratedFrom_TKey_TPayload + keyAndPayloadGenericParameters;

                template.fields = resultRepresentation.AllFields;

                template.staticCtor = Transformer.StaticCtor(template.className);
                var expandedCode = template.TransformText();

                var assemblyReferences = Transformer.AssemblyReferencesNeededFor(typeof(TKey), typeof(TPayload));
                assemblyReferences.Add(typeof(IStreamable<,>).GetTypeInfo().Assembly);
                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TPayload>());

                var a = Transformer.CompileSourceCode(expandedCode, assemblyReferences, out errorMessages);
                var realClassName = template.className.AddNumberOfNecessaryGenericArguments(keyType, payloadType);
                var t = a.GetType(realClassName);
                if (t.GetTypeInfo().IsGenericType)
                {
                    var list = keyType.GetAnonymousTypes();
                    list.AddRange(payloadType.GetAnonymousTypes());
                    return Tuple.Create(t.MakeGenericType(list.ToArray()), errorMessages);
                }
                else
                {
                    return Tuple.Create(t, errorMessages);
                }
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
