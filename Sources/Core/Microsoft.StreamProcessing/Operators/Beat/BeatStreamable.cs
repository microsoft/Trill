// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    internal sealed class BeatStreamable<TKey, TPayload> : UnaryStreamable<TKey, TPayload, TPayload>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        public readonly long Offset;
        public readonly long Period;

        public BeatStreamable(IStreamable<TKey, TPayload> source, long offset, long period)
            : base(source, source.Properties)
        {
            Contract.Requires(source != null);
            Contract.Requires(period > 0);

            // This operator uses the equality method on payloads
            if (this.Properties.IsColumnar && !this.Properties.PayloadEqualityComparer.CanUsePayloadEquality())
            {
                throw new InvalidOperationException($"Type of payload, '{typeof(TPayload).FullName}', to Beat does not have a valid equality operator for columnar mode.");
            }

            this.Offset = offset;
            this.Period = period;

            Initialize();
        }

        internal override IStreamObserver<TKey, TPayload> CreatePipe(IStreamObserver<TKey, TPayload> observer)
        {
            var p = typeof(TKey).GetPartitionType();
            if (p == null)
            {
                return this.Source.Properties.IsColumnar
                    ? GetPipe(observer)
                    : new BeatPipe<TKey, TPayload>(this, observer);
            }

            var outputType = typeof(PartitionedBeatPipe<,,>).MakeGenericType(typeof(TKey), typeof(TPayload), p);
            return (IStreamObserver<TKey, TPayload>)Activator.CreateInstance(outputType, this, observer);
        }

        protected override bool CanGenerateColumnar()
        {
            var typeOfTKey = typeof(TKey);

            if (typeOfTKey.GetPartitionType() != null) return false;

            // This operator can handle a pseudo-columnar payload type, e.g., string.
            var lookupKey = CacheKey.Create(this.Properties.KeyEqualityComparer.GetEqualsExpr().ExpressionToCSharp(), this.Properties.PayloadEqualityComparer.GetEqualsExpr().ExpressionToCSharp());

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => BeatTemplate.Generate(this));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private UnaryPipe<TKey, TPayload, TPayload> GetPipe(IStreamObserver<TKey, TPayload> observer)
        {
            var lookupKey = CacheKey.Create(this.Properties.KeyEqualityComparer.GetEqualsExpr().ExpressionToCSharp(), this.Properties.PayloadEqualityComparer.GetEqualsExpr().ExpressionToCSharp());

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => BeatTemplate.Generate(this));
            Func<PlanNode, IQueryObject, PlanNode> planNode = ((PlanNode p, IQueryObject o) => new BeatPlanNode(p, o, typeof(TKey), typeof(TPayload), this.Offset, this.Period, true, generatedPipeType.Item2));

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, planNode, this.Offset, this.Period);
            var returnValue = (UnaryPipe<TKey, TPayload, TPayload>)instance;
            return returnValue;
        }
    }
}
