// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.Reflection;

namespace Microsoft.StreamProcessing
{
    internal partial class ColumnToRowTemplate
    {
        private static int ColumnToRowSequenceNumber = 0;
        private bool rowMajor = true;

        public ColumnToRowTemplate(string className, Type keyType, Type payloadType)
            : base(className, keyType, payloadType, payloadType) { }

        internal static Tuple<Type, string> Generate<TKey, TPayload>(ColumnToRowStreamable<TKey, TPayload> stream)
        {
            Contract.Requires(stream != null);
            Contract.Ensures(Contract.Result<Tuple<Type, string>>() != null);
            Contract.Ensures(typeof(UnaryPipe<TKey, TPayload, TPayload>).GetTypeInfo().IsAssignableFrom(Contract.Result<Tuple<Type, string>>().Item1));

            var keyType = typeof(TKey);
            var payloadType = typeof(TPayload);

            var assemblyReferences = new List<Assembly>();
            assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(keyType));
            assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(payloadType));

            var generatedClassName = $"ColumnToRowUnaryPipeGeneratedFrom_{keyType.GetValidIdentifier()}_{payloadType.GetValidIdentifier()}_{ColumnToRowSequenceNumber++}";
            var template = new ColumnToRowTemplate(generatedClassName, keyType, payloadType);

            var expandedCode = template.TransformText();

            assemblyReferences.Add(typeof(IStreamable<,>).GetTypeInfo().Assembly);
            assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TPayload>());

            generatedClassName = generatedClassName.AddNumberOfNecessaryGenericArguments(keyType, payloadType);

            var a = Transformer.CompileSourceCode(expandedCode, assemblyReferences, out string errorMessages);
            if (payloadType.IsAnonymousTypeName())
            {
                if (errorMessages == null) errorMessages = string.Empty;
                errorMessages += "\nCodegen Warning: The payload type for ColumnToRow is anonymous, causing the use of Activator.CreateInstance in an inner loop. This will lead to poor performance.\n";
            }

            var t = a.GetType(generatedClassName);
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
    }
}
