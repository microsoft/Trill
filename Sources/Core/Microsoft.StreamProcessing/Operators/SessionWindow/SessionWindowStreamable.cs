// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    internal sealed class SessionWindowStreamable<TKey, TPayload> : UnaryStreamable<TKey, TPayload, TPayload>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        private readonly long sessionTimeout;
        private readonly long maximumDuration;

        public SessionWindowStreamable(IStreamable<TKey, TPayload> source, long sessionTimeout, long maximumDuration)
            : base(source, source.Properties)
        {
            Contract.Assert(sessionTimeout > 0);
            Contract.Assert(maximumDuration > sessionTimeout);
            this.sessionTimeout = sessionTimeout;
            this.maximumDuration = maximumDuration;

            Initialize();
        }

        internal override IStreamObserver<TKey, TPayload> CreatePipe(IStreamObserver<TKey, TPayload> observer)
        {
            var t = typeof(TKey).GetPartitionType();
            if (t == null)
            {
                if (this.Source.Properties.IsColumnar) return GetPipe(observer);
                if (typeof(TKey) == typeof(Empty))
                    return new SessionWindowPipeStateless<TKey, TPayload>(this, observer, this.sessionTimeout, this.maximumDuration);
                else
                    return new SessionWindowPipe<TKey, TPayload>(this, observer, this.sessionTimeout, this.maximumDuration);
            }
            var outputType = typeof(PartitionedSessionWindowPipe<,,>).MakeGenericType(
                typeof(TKey),
                typeof(TPayload),
                t);
            return (IStreamObserver<TKey, TPayload>)Activator.CreateInstance(outputType, this, observer, this.sessionTimeout, this.maximumDuration);
        }

        protected override bool CanGenerateColumnar()
        {
            var lookupKey = CacheKey.Create();

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => SessionWindowTemplate.Generate(this));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private UnaryPipe<TKey, TPayload, TPayload> GetPipe(IStreamObserver<TKey, TPayload> observer)
        {
            var lookupKey = CacheKey.Create();

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => SessionWindowTemplate.Generate(this));
            Func<PlanNode, IQueryObject, PlanNode> planNode = ((PlanNode p, IQueryObject o) => new SessionWindowPlanNode(p, o, typeof(TKey), typeof(TPayload), this.sessionTimeout, this.maximumDuration, true, generatedPipeType.Item2));

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, planNode, this.sessionTimeout, this.maximumDuration);
            var returnValue = (UnaryPipe<TKey, TPayload, TPayload>)instance;
            return returnValue;
        }
    }
}
