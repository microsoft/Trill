// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    internal sealed class ClipJoinStreamable<TKey, TLeft, TRight> : BinaryStreamable<TKey, TLeft, TRight, TLeft>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Expressions are immutable")]
        public readonly IEqualityComparerExpression<TLeft> LeftComparer;

        public ClipJoinStreamable(IStreamable<TKey, TLeft> left, IStreamable<TKey, TRight> right)
            : base(left.Properties.Clip(right.Properties), left, right)
        {
            Contract.Requires(left != null);
            Contract.Requires(right != null);

            this.LeftComparer = left.Properties.PayloadEqualityComparer;

            // This operator uses the equality method on payloads from the left side
            if (left.Properties.IsColumnar && !this.LeftComparer.CanUsePayloadEquality())
            {
                throw new InvalidOperationException($"Type of payload, '{typeof(TLeft).FullName}', to Clip does not have a valid equality operator for columnar mode.");
            }

            Initialize();
        }

        protected override IBinaryObserver<TKey, TLeft, TRight, TLeft> CreatePipe(IStreamObserver<TKey, TLeft> observer)
        {
            if (this.properties.IsColumnar) return GetPipe(observer);
            else
            {
                var part = typeof(TKey).GetPartitionType();
                if (part == null) return new ClipJoinPipe<TKey, TLeft, TRight>(this, observer);

                var outputType = typeof(PartitionedClipJoinPipe<,,,>).MakeGenericType(
                    typeof(TKey),
                    typeof(TLeft),
                    typeof(TRight),
                    part);
                return (BinaryPipe<TKey, TLeft, TRight, TLeft>)Activator.CreateInstance(outputType, this, observer);
            }
        }

        protected override bool CanGenerateColumnar()
        {
            var typeOfTKey = typeof(TKey);
            var typeOfTLeft = typeof(TLeft);
            var typeOfTRight = typeof(TRight);

            if (!typeOfTLeft.CanRepresentAsColumnar()) return false;
            if (!typeOfTRight.CanRepresentAsColumnar()) return false;
            if (typeOfTKey.GetPartitionType() != null) return false;

            var lookupKey = CacheKey.Create(this.Properties.KeyEqualityComparer.GetEqualsExpr().ToString(), this.LeftComparer.GetEqualsExpr().ToString());

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => ClipJoinTemplate.Generate(this));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private BinaryPipe<TKey, TLeft, TRight, TLeft> GetPipe(IStreamObserver<TKey, TLeft> observer)
        {
            var lookupKey = CacheKey.Create(this.Properties.KeyEqualityComparer.GetEqualsExpr().ToString(), this.LeftComparer.GetEqualsExpr().ToString());

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => ClipJoinTemplate.Generate(this));
            Func<PlanNode, PlanNode, IBinaryObserver, BinaryPlanNode> planNode = ((PlanNode left, PlanNode right, IBinaryObserver o) =>
            {
                var node = new JoinPlanNode(
                        left, right, o,
                    typeof(TLeft), typeof(TRight), typeof(TLeft), typeof(TKey),
                    JoinKind.Clip, true, generatedPipeType.Item2);
                node.AddJoinExpression("left comparer", this.LeftComparer.GetEqualsExpr());
                node.AddJoinExpression("key comparer", this.Properties.KeyComparer.GetCompareExpr());
                return node;
            });

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, this.Properties.KeyEqualityComparer, this.LeftComparer, planNode);
            var returnValue = (BinaryPipe<TKey, TLeft, TRight, TLeft>)instance;
            return returnValue;
        }
    }
}
