// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Globalization;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    internal sealed class EndEdgeFreeOutputStreamable<TKey, TPayload> : UnaryStreamable<TKey, TPayload, TPayload>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();
        public EndEdgeFreeOutputStreamable(IStreamable<TKey, TPayload> source)
            : base(source, source.Properties.ToIntervalFree(false))
        {
            Contract.Requires(source != null);

            // This operator uses the equality method on payloads
            if (this.Properties.IsColumnar && !this.Properties.PayloadEqualityComparer.CanUsePayloadEquality())
            {
                throw new InvalidOperationException($"Type of payload, '{typeof(TPayload).FullName}', to EndEdgeFreeOutputStreamable does not have a valid equality operator for columnar mode.");
            }

            Initialize();
        }

        public override sealed IDisposable Subscribe(IStreamObserver<TKey, TPayload> observer)
            => this.Source.Properties.IsConstantDuration
                ? this.Source.Subscribe(observer)
                : this.Source.Subscribe(CreatePipe(observer));

        internal override IStreamObserver<TKey, TPayload> CreatePipe(IStreamObserver<TKey, TPayload> observer)
        {
            var part = typeof(TKey).GetPartitionType();
            if (part == null)
            {
                return this.Source.Properties.IsColumnar
                    ? GetPipe(observer)
                    : new EndEdgeFreeOutputPipe<TKey, TPayload>(this, observer);
            }

            var outputType = typeof(PartitionedEndEdgeFreeOutputPipe<,,>).MakeGenericType(
                typeof(TKey),
                typeof(TPayload),
                part);
            return (IStreamObserver<TKey, TPayload>)Activator.CreateInstance(outputType, this, observer);
        }

        protected override bool CanGenerateColumnar()
        {
            var typeOfTKey = typeof(TKey);
            var typeOfTPayload = typeof(TPayload);

            if (typeOfTKey.GetPartitionType() != null) return false;
            if (!typeOfTPayload.CanRepresentAsColumnar()) return false;

            var lookupKey = CacheKey.Create(this.Properties.KeyEqualityComparer.GetEqualsExpr().ExpressionToCSharp(), this.Properties.PayloadEqualityComparer.GetEqualsExpr().ExpressionToCSharp());

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => EndEdgeFreeOutputTemplate.Generate(this));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private UnaryPipe<TKey, TPayload, TPayload> GetPipe(IStreamObserver<TKey, TPayload> observer)
        {
            var lookupKey = CacheKey.Create(this.Properties.KeyEqualityComparer.GetEqualsExpr().ExpressionToCSharp(), this.Properties.PayloadEqualityComparer.GetEqualsExpr().ExpressionToCSharp());

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => EndEdgeFreeOutputTemplate.Generate(this));
            Func<PlanNode, IQueryObject, PlanNode> planNode = ((PlanNode p, IQueryObject o) => new EndEdgeFreeOutputPlanNode(p, o, typeof(TKey), typeof(TPayload), true, generatedPipeType.Item2));

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, planNode);
            var returnValue = (UnaryPipe<TKey, TPayload, TPayload>)instance;
            return returnValue;
        }
    }
}