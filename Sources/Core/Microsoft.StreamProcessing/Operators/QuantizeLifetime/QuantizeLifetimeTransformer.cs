// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
#if CODEGEN_TIMING
using System.Diagnostics;
#endif
using System.Diagnostics.Contracts;
using System.Reflection;

namespace Microsoft.StreamProcessing
{
    internal partial class QuantizeLifetimeTemplate
    {
        private static int QuantizeLifetimeSequenceNumber = 0;

        private QuantizeLifetimeTemplate(string className, Type keyType, Type payloadType)
            : base(className, keyType, payloadType, payloadType) { }

        internal static Tuple<Type, string> Generate<TKey, TPayload>(QuantizeLifetimeStreamable<TKey, TPayload> stream)
        {
            ArgumentNullException.ThrowIfNull(stream);
            Contract.Ensures(Contract.Result<Tuple<Type, string>>() == null || typeof(UnaryPipe<TKey, TPayload, TPayload>).IsAssignableFrom(Contract.Result<Tuple<Type, string>>().Item1));

#if CODEGEN_TIMING
          Stopwatch sw = new Stopwatch();
          sw.Start();
#endif
            var template = new QuantizeLifetimeTemplate(
                $"GeneratedQuantizeLifetime_{QuantizeLifetimeSequenceNumber++}",
                typeof(TKey), typeof(TPayload));

            return template.Generate<TKey, TPayload>(typeof(IStreamable<,>));
        }
    }
}
