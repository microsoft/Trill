// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Reflection;

namespace Microsoft.StreamProcessing
{
    internal partial class ExtendLifetimeBaseTemplate
    {
        protected static int ExtendLifetimeSequenceNumber = 0;
        protected bool useCompiledKeyComparer;
        protected Func<string, string, string> keyComparer;
        protected bool useCompiledPayloadComparer;
        protected Func<string, string, string> payloadComparer;
        protected string ActiveEventType;

        protected ExtendLifetimeBaseTemplate(string className, Type keyType, Type payloadType)
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
        internal static Tuple<Type, string> Generate<TKey, TPayload>(ExtendLifetimeStreamable<TKey, TPayload> stream, long duration)
        {
            Contract.Requires(stream != null);
            Contract.Ensures(Contract.Result<Tuple<Type, string>>() == null || typeof(UnaryPipe<TKey, TPayload, TPayload>).GetTypeInfo().IsAssignableFrom(Contract.Result<Tuple<Type, string>>().Item1));

            var result = Generate(stream, duration, false, false);
            if (duration >= 0) return result; // only negative pipe uses comparers
            if (result.Item1 != null) return result;

            result = Generate(stream, duration, true, false);
            if (result.Item1 != null) return result;

            result = Generate(stream, duration, true, true);
            return result;
        }

        internal static Tuple<Type, string> Generate<TKey, TPayload>(ExtendLifetimeStreamable<TKey, TPayload> stream, long duration, bool useCompiledKeyComparer, bool useCompiledPayloadComparer)
        {
            Contract.Requires(stream != null);
            Contract.Ensures(Contract.Result<Tuple<Type, string>>() == null || typeof(UnaryPipe<TKey, TPayload, TPayload>).GetTypeInfo().IsAssignableFrom(Contract.Result<Tuple<Type, string>>().Item1));

#if CODEGEN_TIMING
          Stopwatch sw = new Stopwatch();
          sw.Start();
#endif
            ExtendLifetimeBaseTemplate template;
            var className = $"GeneratedExtendLifetime_{ExtendLifetimeSequenceNumber++}";

            template = duration < 0
                ? new ExtendLifetimeNegativeTemplate(className, typeof(TKey), typeof(TPayload))
                : (ExtendLifetimeBaseTemplate)new ExtendLifetimeTemplate(className, typeof(TKey), typeof(TPayload));

            template.ActiveEventType = typeof(TPayload).GetTypeInfo().IsValueType ? template.TPayload : "Active_Event";

            #region Key Comparer
            template.useCompiledKeyComparer = useCompiledKeyComparer;
            if (useCompiledKeyComparer)
            {
                template.keyComparer = (left, right) => string.Format("keyComparer({0}, {1}", left, right);
            }
            else
            {
                var keyComparer = stream.Properties.KeyEqualityComparer.GetEqualsExpr();
                template.keyComparer =
                    (left, right) =>
                        keyComparer.Inline(left, right);
            }
            #endregion

            #region Payload Comparer
            template.useCompiledPayloadComparer = useCompiledPayloadComparer;
            if (useCompiledPayloadComparer)
            {
                template.payloadComparer = (left, right) => $"payloadComparer({left}.Payload, {right}.Payload";
            }
            else
            {
                var payloadComparer = stream.Properties.PayloadEqualityComparer.GetEqualsExpr();
                var newLambda = Extensions.TransformFunction<TKey, TPayload>(payloadComparer, "index");
                template.payloadComparer = (left, right) => newLambda.Inline(left, right);
            }
            #endregion

            return template.Generate<TKey, TPayload>(typeof(IStreamable<,>));
        }
    }

    internal partial class ExtendLifetimeTemplate
    {
        public ExtendLifetimeTemplate(string className, Type keyType, Type payloadType)
            : base(className, keyType, payloadType) { }
    }

    internal partial class ExtendLifetimeNegativeTemplate
    {
        public ExtendLifetimeNegativeTemplate(string className, Type keyType, Type payloadType)
            : base(className, keyType, payloadType) { }
    }
}
