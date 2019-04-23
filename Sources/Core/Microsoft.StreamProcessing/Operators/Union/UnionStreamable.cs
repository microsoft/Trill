// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    internal sealed class UnionStreamable<TKey, TPayload> : BinaryStreamable<TKey, TPayload, TPayload, TPayload>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        public UnionStreamable(IStreamable<TKey, TPayload> left, IStreamable<TKey, TPayload> right, bool registerInputs = false)
            : base(left.Properties.Union(right.Properties), left, right, registerInputs)
            => Initialize();

        protected override IBinaryObserver<TKey, TPayload, TPayload, TPayload> CreatePipe(IStreamObserver<TKey, TPayload> observer)
        {
            var part = typeof(TKey).GetPartitionType();
            if (part == null)
            {
                if (this.Left.Properties.IsColumnar && this.Right.Properties.IsColumnar) return GetPipe(observer);
                else return new UnionPipe<TKey, TPayload>(this, observer);
            }

            var outputType = typeof(PartitionedUnionPipe<,,>).MakeGenericType(
                typeof(TKey),
                typeof(TPayload),
                part);
            return (BinaryPipe<TKey, TPayload, TPayload, TPayload>)Activator.CreateInstance(outputType, this, observer);
        }

        protected override bool CanGenerateColumnar() => true;

        private BinaryPipe<TKey, TPayload, TPayload, TPayload> GetPipe(IStreamObserver<TKey, TPayload> observer)
        {
            var lookupKey = CacheKey.Create();

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => UnionTemplate.GenerateUnionPipeClass(this));
            Func<PlanNode, PlanNode, IBinaryObserver, BinaryPlanNode> planNode = ((PlanNode left, PlanNode right, IBinaryObserver o) =>
            {
                var node = new UnionPlanNode(
                        left, right, o, typeof(TKey), typeof(TPayload), false, true, this.ErrorMessages);
                return node;
            });

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, planNode);
            var returnValue = (BinaryPipe<TKey, TPayload, TPayload, TPayload>)instance;
            return returnValue;
        }
    }
}