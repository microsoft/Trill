// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Reflection;

namespace Microsoft.StreamProcessing
{
    internal partial class PointAtEndTemplate
    {
        private static int PointAtEndSequenceNumber = 0;

        private PointAtEndTemplate(string className, Type keyType, Type payloadType)
            : base(className, keyType, payloadType, payloadType) { }

        /// <summary>
        /// Generate a batch class definition to be used as a Beat pipe.
        /// Compile the definition, dynamically load the assembly containing it, and return the Type representing the
        /// union pipe class.
        /// </summary>
        /// <typeparam name="TKey">The key type for both sides.</typeparam>
        /// <typeparam name="TPayload">The payload type.</typeparam>
        /// <returns>
        /// A type that is defined to be a subtype of UnaryPipe&lt;<typeparamref name="TKey"/>,<typeparamref name="TPayload"/>, <typeparamref name="TPayload"/>&gt;.
        /// </returns>
        internal static Tuple<Type, string> Generate<TKey, TPayload>(PointAtEndStreamable<TKey, TPayload> stream)
        {
            Contract.Requires(stream != null);
            Contract.Ensures(Contract.Result<Tuple<Type, string>>() == null || typeof(UnaryPipe<TKey, TPayload, TPayload>).GetTypeInfo().IsAssignableFrom(Contract.Result<Tuple<Type, string>>().Item1));

#if CODEGEN_TIMING
          Stopwatch sw = new Stopwatch();
          sw.Start();
#endif
            var template = new PointAtEndTemplate(
                $"GeneratedPointAtEnd_{PointAtEndSequenceNumber++}",
                typeof(TKey), typeof(TPayload));

            return template.Generate<TKey, TPayload>(typeof(IStreamable<,>));
        }

    }
}
