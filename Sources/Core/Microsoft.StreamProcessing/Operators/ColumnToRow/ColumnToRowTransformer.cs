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
    internal partial class ColumnToRowTemplate(string className, Type keyType, Type payloadType)
        : CommonUnaryTemplate(className, keyType, payloadType, payloadType)
    {
        private static int ColumnToRowSequenceNumber = 0;
        private bool rowMajor = true;

        internal static Tuple<Type, string> Generate<TKey, TPayload>(ColumnToRowStreamable<TKey, TPayload> stream)
        {
            ArgumentNullException.ThrowIfNull(stream);
            Contract.Ensures(Contract.Result<Tuple<Type, string>>() != null);
            Contract.Ensures(typeof(UnaryPipe<TKey, TPayload, TPayload>).IsAssignableFrom(Contract.Result<Tuple<Type, string>>().Item1));

            var keyType = typeof(TKey);
            var payloadType = typeof(TPayload);

            var assemblyReferences = new List<Assembly>();
            assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(keyType));
            assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(payloadType));

            var generatedClassName = $"ColumnToRowUnaryPipeGeneratedFrom_{keyType.GetValidIdentifier()}_{payloadType.GetValidIdentifier()}_{ColumnToRowSequenceNumber++}";
            var template = new ColumnToRowTemplate(generatedClassName, keyType, payloadType);

            var expandedCode = template.TransformText();

            assemblyReferences.Add(typeof(IStreamable<,>).Assembly);
            assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TPayload>());

            generatedClassName = generatedClassName.AddNumberOfNecessaryGenericArguments(keyType, payloadType);

            var t = Transformer.CompileSourceCode(expandedCode, assemblyReferences, a => a.GetType(generatedClassName), out var errorMessages);
            if (payloadType.IsAnonymousTypeName())
            {
                errorMessages ??= string.Empty;
                errorMessages += "\nCodegen Warning: The payload type for ColumnToRow is anonymous, causing the use of Activator.CreateInstance in an inner loop. This will lead to poor performance.\n";
            }
            if (t.IsGenericType)
            {
                var list = keyType.GetAnonymousTypes();
                list.AddRange(payloadType.GetAnonymousTypes());
                return Tuple.Create(t.MakeGenericType([.. list]), errorMessages);
            }
            else
            {
                return Tuple.Create(t, errorMessages);
            }
        }
    }
}
