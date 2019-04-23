// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    internal sealed class QuantizeLifetimeStreamable<TKey, TPayload> : UnaryStreamable<TKey, TPayload, TPayload>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        private readonly long width;
        private readonly long skip;
        private readonly long progress;
        private readonly long offset;

        public QuantizeLifetimeStreamable(IStreamable<TKey, TPayload> source, long width, long skip, long progress, long offset)
            : base(source, source.Properties.ToConstantDuration(
                source.Properties.IsConstantDuration && source.Properties.ConstantDurationLength == 1 && skip == progress,
                !source.Properties.IsConstantDuration || source.Properties.ConstantDurationLength != 1 || skip != progress
                ? source.Properties.ConstantDurationLength
                : width).ToConstantHop(true, skip, offset))
        {
            this.width = width;
            this.skip = skip;
            this.progress = progress;
            this.offset = offset;

            Initialize();
        }

        protected override bool CanGenerateColumnar() => typeof(TPayload).CanRepresentAsColumnar();

        internal override IStreamObserver<TKey, TPayload> CreatePipe(IStreamObserver<TKey, TPayload> observer)
        {
            if (this.Source.Properties.IsConstantDuration)
                return new StatelessQuantizeLifetimePipe<TKey, TPayload>(this, observer, this.width, this.skip, this.progress, this.offset);

            var t = typeof(TKey).GetPartitionType();
            if (t == null)
            {
                return this.Source.Properties.IsColumnar
                    ? GetPipe(observer)
                    : new QuantizeLifetimePipe<TKey, TPayload>(this, observer, this.width, this.skip, this.progress, this.offset);
            }
            var outputType = typeof(PartitionedQuantizeLifetimePipe<,,>).MakeGenericType(
                typeof(TKey),
                typeof(TPayload),
                t);
            return (IStreamObserver<TKey, TPayload>)Activator.CreateInstance(outputType, this, observer, this.width, this.skip, this.progress, this.offset);
        }

        private UnaryPipe<TKey, TPayload, TPayload> GetPipe(IStreamObserver<TKey, TPayload> observer)
        {
            var lookupKey = CacheKey.Create();

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => QuantizeLifetimeTemplate.Generate(this));
            Func<PlanNode, IQueryObject, PlanNode> planNode = ((PlanNode p, IQueryObject o) => new PointAtEndPlanNode(p, o, typeof(TKey), typeof(TPayload), true, generatedPipeType.Item2));

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, this.width, this.skip, this.progress, this.offset, planNode);
            var returnValue = (UnaryPipe<TKey, TPayload, TPayload>)instance;
            return returnValue;
        }
    }
}
