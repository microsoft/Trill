// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    internal sealed class LeftAntiSemiJoinStreamable<TKey, TLeft, TRight> : BinaryStreamable<TKey, TLeft, TRight, TLeft>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Expressions are immutable")]
        public readonly IEqualityComparerExpression<TLeft> LeftComparer;

        public LeftAntiSemiJoinStreamable(IStreamable<TKey, TLeft> left, IStreamable<TKey, TRight> right)
            : base(left.Properties.LASJ(right.Properties), left, right)
        {
            Contract.Requires(left != null);
            Contract.Requires(right != null);

            this.LeftComparer = left.Properties.PayloadEqualityComparer;

            Initialize();
        }

        protected override IBinaryObserver<TKey, TLeft, TRight, TLeft> CreatePipe(IStreamObserver<TKey, TLeft> observer)
        {
            var part = typeof(TKey).GetPartitionType();
            if (part == null)
            {
                return this.properties.IsColumnar
                    ? GetPipe(observer, this.Left.Properties.IsConstantDuration, this.Right.Properties.IsConstantDuration)
                    : new LeftAntiSemiJoinPipe<TKey, TLeft, TRight>(this, observer);
            }

            var outputType = typeof(PartitionedLeftAntiSemiJoinPipe<,,,>).MakeGenericType(
                typeof(TKey),
                typeof(TLeft),
                typeof(TRight),
                part);
            return (BinaryPipe<TKey, TLeft, TRight, TLeft>)Activator.CreateInstance(outputType, this, observer);
        }

        protected override bool CanGenerateColumnar()
        {
            // This operator uses the equality method on payloads
            if (this.Properties.IsColumnar && !this.LeftComparer.CanUsePayloadEquality())
            {
                this.errorMessages = $"The payload type, '{typeof(TLeft).FullName}', to Left Antisemijoin does not implement the interface {nameof(IEqualityComparerExpression<TLeft>)}. This interface is needed for code generation of this operator for columnar mode. Furthermore, the equality expression in the interface can only refer to input variables if used in field or property references.";
                if (Config.CodegenOptions.DontFallBackToRowBasedExecution)
                    throw new StreamProcessingException(this.errorMessages);
                return false;
            }

            var typeOfTKey = typeof(TKey);
            var typeOfTLeft = typeof(TLeft);
            var typeOfTRight = typeof(TRight);

            if (!typeOfTLeft.CanRepresentAsColumnar()) return false;
            if (!typeOfTRight.CanRepresentAsColumnar()) return false;
            if (typeOfTKey.GetPartitionType() != null) return false;

            var lookupKey = CacheKey.Create(this.Properties.KeyEqualityComparer.GetEqualsExpr().ToString(), this.LeftComparer.GetEqualsExpr().ToString());

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => LeftAntiSemiJoinTemplate.Generate(this));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private BinaryPipe<TKey, TLeft, TRight, TLeft> GetPipe(IStreamObserver<TKey, TLeft> observer, bool leftIsConstantDuration, bool rightIsConstantDuration)
        {
            var lookupKey = CacheKey.Create(
                leftIsConstantDuration, this.Properties.KeyEqualityComparer.GetEqualsExpr().ToString(), this.LeftComparer.GetEqualsExpr().ToString());

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => LeftAntiSemiJoinTemplate.Generate(this));
            Func<PlanNode, PlanNode, IBinaryObserver, BinaryPlanNode> planNode = ((PlanNode left, PlanNode right, IBinaryObserver o) =>
            {
                var node = new JoinPlanNode(
                        left, right, o,
                    typeof(TLeft), typeof(TRight), typeof(TLeft), typeof(TKey),
                    JoinKind.LeftAntiSemiJoin, true, generatedPipeType.Item2);
                node.AddJoinExpression("key comparer", this.Properties.KeyEqualityComparer.GetEqualsExpr());
                node.AddJoinExpression("left key comparer", this.LeftComparer.GetEqualsExpr());
                return node;
            });

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, this.Properties.KeyEqualityComparer, this.LeftComparer, planNode, leftIsConstantDuration, rightIsConstantDuration);
            var returnValue = (BinaryPipe<TKey, TLeft, TRight, TLeft>)instance;
            return returnValue;
        }
    }
}
