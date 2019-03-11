// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Reflection;

namespace Microsoft.StreamProcessing
{
    internal partial class BeatTemplate
    {
        private static int BeatSequenceNumber = 0;
        private Func<string, string, string> keyComparer;
        private Func<string, string, string> payloadComparer;
        private string ActiveEventType;

        private BeatTemplate(string className, Type keyType, Type payloadType)
            : base(className, keyType, payloadType, payloadType) { }

        /// <summary>
        /// Generate a batch class definition to be used as a Beat pipe.
        /// Compile the definition, dynamically load the assembly containing it, and return the Type representing the
        /// Beat pipe class.
        /// </summary>
        /// <typeparam name="TKey">The key type for both sides.</typeparam>
        /// <typeparam name="TPayload">The payload type.</typeparam>
        /// <returns>
        /// A type that is defined to be a subtype of UnaryPipe&lt;<typeparamref name="TKey"/>,<typeparamref name="TPayload"/>, <typeparamref name="TPayload"/>&gt;.
        /// </returns>
        internal static Tuple<Type, string> Generate<TKey, TPayload>(BeatStreamable<TKey, TPayload> stream)
        {
            Contract.Requires(stream != null);
            Contract.Ensures(Contract.Result<Tuple<Type, string>>() == null || typeof(UnaryPipe<TKey, TPayload, TPayload>).GetTypeInfo().IsAssignableFrom(Contract.Result<Tuple<Type, string>>().Item1));

#if CODEGEN_TIMING
          Stopwatch sw = new Stopwatch();
          sw.Start();
#endif
            var template = new BeatTemplate(
                $"GeneratedBeat_{BeatSequenceNumber++}",
                typeof(TKey), typeof(TPayload));

            template.ActiveEventType = typeof(TPayload).GetTypeInfo().IsValueType ? template.TPayload : "Active_Event";

            #region Key Comparer
            var keyComparer = stream.Properties.KeyEqualityComparer.GetEqualsExpr();
            template.keyComparer =
                (left, right) =>
                    keyComparer.Inline(left, right);
            #endregion

            #region Payload Comparer
            var payloadComparer = stream.Properties.PayloadEqualityComparer.GetEqualsExpr();
            var newLambda = Extensions.TransformFunction<TKey, TPayload>(payloadComparer, "index");
            template.payloadComparer = (left, right) => newLambda.Inline(left, right);
            #endregion

            return template.Generate<TKey, TPayload>(typeof(IStreamable<,>));
        }
    }
}
