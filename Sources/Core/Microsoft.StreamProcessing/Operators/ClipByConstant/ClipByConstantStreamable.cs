// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    internal sealed class ClipByConstantStreamable<TKey, TPayload> : UnaryStreamable<TKey, TPayload, TPayload>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        private readonly long limit;

        public ClipByConstantStreamable(IStreamable<TKey, TPayload> source, long limit)
            : base(source, source.Properties)
        {
            Contract.Assert(limit > 0);
            this.limit = limit;
        }

        internal override IStreamObserver<TKey, TPayload> CreatePipe(IStreamObserver<TKey, TPayload> observer)
        {
            var t = typeof(TKey).GetPartitionType();
            if (t == null)
            {
                return this.Source.Properties.IsColumnar
                    ? GetPipe(observer)
                    : new ClipByConstantPipe<TKey, TPayload>(this, observer, this.limit);
            }
            var outputType = typeof(PartitionedClipByConstantPipe<,,>).MakeGenericType(
                typeof(TKey),
                typeof(TPayload),
                t);
            return (IStreamObserver<TKey, TPayload>)Activator.CreateInstance(outputType, this, observer, this.limit);
        }

        protected override bool CanGenerateColumnar()
        {
            var lookupKey = CacheKey.Create();

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => ClipByConstantTemplate.Generate(this));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private UnaryPipe<TKey, TPayload, TPayload> GetPipe(IStreamObserver<TKey, TPayload> observer)
        {
            var lookupKey = CacheKey.Create();

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => ClipByConstantTemplate.Generate(this));
            Func<PlanNode, IQueryObject, PlanNode> planNode = (PlanNode p, IQueryObject o) => new ClipByConstantPlanNode(p, o, typeof(TKey), typeof(TPayload), true, generatedPipeType.Item2);

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, planNode, this.limit);
            var returnValue = (UnaryPipe<TKey, TPayload, TPayload>)instance;
            return returnValue;
        }
    }

}