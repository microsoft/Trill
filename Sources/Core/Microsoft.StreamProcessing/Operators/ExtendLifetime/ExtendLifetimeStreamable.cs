// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    internal sealed class ExtendLifetimeStreamable<TKey, TPayload> : UnaryStreamable<TKey, TPayload, TPayload>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();
        private readonly long duration;

        public ExtendLifetimeStreamable(IStreamable<TKey, TPayload> source, long duration)
            : base(source, source.Properties)
        {
            this.duration = duration;

            // This operator uses the equality method on payloads
            if (duration < 0 && this.Properties.IsColumnar && !this.Properties.PayloadEqualityComparer.CanUsePayloadEquality())
            {
                throw new InvalidOperationException($"Type of payload, '{typeof(TPayload).FullName}', to ExtendLifetime does not have a valid equality operator for columnar mode.");
            }

            Initialize();
        }

        internal override IStreamObserver<TKey, TPayload> CreatePipe(IStreamObserver<TKey, TPayload> observer)
        {
            if (this.duration == 0) return observer;
            if (this.duration < 0)
            {
                var t = typeof(TKey).GetPartitionType();
                if (t == null)
                {
                    return this.Source.Properties.IsColumnar
                        ? GetPipe(this, observer)
                        : new ExtendLifetimeNegativePipe<TKey, TPayload>(this, observer, -this.duration);
                }
                var outputType = typeof(PartitionedExtendLifetimeNegativePipe<,,>).MakeGenericType(
                    typeof(TKey),
                    typeof(TPayload),
                    t);
                return (IStreamObserver<TKey, TPayload>)Activator.CreateInstance(outputType, this, observer, -this.duration);
            }
            else
            {
                var t = typeof(TKey).GetPartitionType();
                if (t == null)
                {
                    return this.Source.Properties.IsColumnar
                        ? GetPipe(this, observer)
                        : new ExtendLifetimePipe<TKey, TPayload>(this, observer, this.duration);
                }
                var outputType = typeof(PartitionedExtendLifetimePipe<,,>).MakeGenericType(
                    typeof(TKey),
                    typeof(TPayload),
                    t);
                return (IStreamObserver<TKey, TPayload>)Activator.CreateInstance(outputType, this, observer, this.duration);
            }
        }

        protected override bool CanGenerateColumnar()
        {
            var typeOfTKey = typeof(TKey);
            var typeOfTPayload = typeof(TPayload);

            if (typeOfTKey.GetPartitionType() != null) return false;
            if (!typeOfTPayload.CanRepresentAsColumnar()) return false;

            var lookupKey = CacheKey.Create(this.duration);

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => ExtendLifetimeBaseTemplate.Generate(this, this.duration));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private UnaryPipe<TKey, TPayload, TPayload> GetPipe(ExtendLifetimeStreamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer)
        {
            var lookupKey = CacheKey.Create(this.duration);

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => ExtendLifetimeBaseTemplate.Generate(stream, this.duration));
            Func<PlanNode, IQueryObject, PlanNode> planNode = (PlanNode p, IQueryObject o) => new ExtendLifetimePlanNode(p, o, typeof(TKey), typeof(TPayload), true, generatedPipeType.Item2, this.duration < 0);

            var instance = Activator.CreateInstance(generatedPipeType.Item1, stream, observer, planNode, (this.duration < 0) ? -this.duration : this.duration);
            var returnValue = (UnaryPipe<TKey, TPayload, TPayload>)instance;
            return returnValue;
        }
    }
}