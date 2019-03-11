// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Reflection;

namespace Microsoft.StreamProcessing
{
    internal partial class RowToColumnTemplate
    {
        private static int RowToColumnSequenceNumber = 0;
        private readonly bool rowMajor = true;

        public RowToColumnTemplate(string className, Type keyType, Type payloadType)
            : base(className, keyType, payloadType, payloadType)
            => this.fields = payloadType.GetAnnotatedFields().Item1; // Do we need this special case?

        internal static Tuple<Type, string> Generate<TKey, TPayload>(RowToColumnStreamable<TKey, TPayload> stream)
        {
            Contract.Requires(stream != null);
            Contract.Ensures(Contract.Result<Tuple<Type, string>>() != null);
            Contract.Ensures(typeof(UnaryPipe<TKey, TPayload, TPayload>).GetTypeInfo().IsAssignableFrom(Contract.Result<Tuple<Type, string>>().Item1));

            var keyType = typeof(TKey);
            var payloadType = typeof(TPayload);

            var generatedClassName = $"RowToColumnUnaryPipeGeneratedFrom_{keyType.GetValidIdentifier()}_{payloadType.GetValidIdentifier()}_{RowToColumnSequenceNumber++}";
            var template = new RowToColumnTemplate(generatedClassName, keyType, payloadType);
            var expandedCode = template.TransformText();

            var assemblyReferences = Transformer.AssemblyReferencesNeededFor(keyType, payloadType);
            assemblyReferences.Add(typeof(IStreamable<,>).GetTypeInfo().Assembly);
            assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TPayload>());
            assemblyReferences.Add(Transformer.GeneratedMemoryPoolAssembly<TKey, TPayload>());

            generatedClassName = generatedClassName.AddNumberOfNecessaryGenericArguments(keyType, payloadType);

            var a = Transformer.CompileSourceCode(expandedCode, assemblyReferences, out string errorMessages);

            var t = a.GetType(generatedClassName);
            return Tuple.Create(t.InstantiateAsNecessary(typeof(TKey), typeof(TPayload)), errorMessages);
        }
    }
}
