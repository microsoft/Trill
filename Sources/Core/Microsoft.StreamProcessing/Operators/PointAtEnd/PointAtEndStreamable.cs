// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    internal sealed class PointAtEndStreamable<TKey, TPayload> : UnaryStreamable<TKey, TPayload, TPayload>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        public PointAtEndStreamable(IStreamable<TKey, TPayload> source)
            : base(source, source.Properties.PointAtEnd())
        {
            Contract.Requires(source != null);

            Initialize();
        }

        internal override IStreamObserver<TKey, TPayload> CreatePipe(IStreamObserver<TKey, TPayload> observer)
        {
            if (this.Source.Properties.IsIntervalFree || this.Source.Properties.IsConstantDuration)
            {
                return new StatelessPointAtEndPipe<TKey, TPayload>(this, observer, this.Source.Properties.ConstantDurationLength ?? 0);
            }

            var t = typeof(TKey).GetPartitionType();
            if (t == null)
            {
                return this.Source.Properties.IsColumnar
                    ? GetPipe(observer)
                    : new PointAtEndPipe<TKey, TPayload>(this, observer);
            }
            var outputType = typeof(PartitionedPointAtEndPipe<,,>).MakeGenericType(
                typeof(TKey),
                typeof(TPayload),
                t);
            return (IStreamObserver<TKey, TPayload>)Activator.CreateInstance(outputType, this, observer);
        }

        protected override bool CanGenerateColumnar()
        {
            var typeOfTKey = typeof(TKey);
            var typeOfTPayload = typeof(TPayload);

            if (typeOfTKey.GetPartitionType() != null) return false;
            if (!typeOfTPayload.CanRepresentAsColumnar()) return false;

            var lookupKey = CacheKey.Create();

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => PointAtEndTemplate.Generate(this));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private UnaryPipe<TKey, TPayload, TPayload> GetPipe(IStreamObserver<TKey, TPayload> observer)
        {
            var lookupKey = CacheKey.Create();

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => PointAtEndTemplate.Generate(this));
            Func<PlanNode, IQueryObject, PlanNode> planNode = ((PlanNode p, IQueryObject o) => new PointAtEndPlanNode(p, o, typeof(TKey), typeof(TPayload), true, generatedPipeType.Item2));

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, planNode);
            var returnValue = (UnaryPipe<TKey, TPayload, TPayload>)instance;
            return returnValue;
        }
    }
}