// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.Linq.Expressions;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    internal sealed class EquiJoinStreamable<TKey, TLeft, TRight, TResult> : BinaryStreamable<TKey, TLeft, TRight, TResult>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        private readonly JoinKind joinKind;
        private readonly Func<CacheKey, Tuple<Type, string>> columnarGenerator;
        private readonly Func<BinaryStreamable<TKey, TLeft, TRight, TResult>, Expression<Func<TLeft, TRight, TResult>>, IStreamObserver<TKey, TResult>, BinaryPipe<TKey, TLeft, TRight, TResult>> partitionedGenerator;
        private readonly Func<BinaryStreamable<TKey, TLeft, TRight, TResult>, Expression<Func<TLeft, TRight, TResult>>, IStreamObserver<TKey, TResult>, IBinaryObserver<TKey, TLeft, TRight, TResult>> fallbackGenerator;

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Reviewed.")]
        private readonly Expression<Func<TLeft, TRight, TResult>> Selector;

        public EquiJoinStreamable(IStreamable<TKey, TLeft> left, IStreamable<TKey, TRight> right, Expression<Func<TLeft, TRight, TResult>> selector)
            : base(left.Properties.Join(right.Properties, selector), left, right)
        {
            Contract.Requires(selector != null);

            this.Selector = selector;

            // This operator uses the equality method on payloads
            if (left.Properties.IsColumnar && !left.Properties.IsStartEdgeOnly && !left.Properties.PayloadEqualityComparer.CanUsePayloadEquality())
            {
                throw new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, "Type of left side of join, '{0}', does not have a valid equality operator for columnar mode.", typeof(TLeft).FullName));
            }
            // This operator uses the equality method on payloads
            if (right.Properties.IsColumnar && !right.Properties.IsStartEdgeOnly && !right.Properties.PayloadEqualityComparer.CanUsePayloadEquality())
            {
                throw new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, "Type of right side of join, '{0}', does not have a valid equality operator for columnar mode.", typeof(TRight).FullName));
            }

            if (left.Properties.IsStartEdgeOnly && right.Properties.IsStartEdgeOnly)
            {
                if ((left.Properties.KeyComparer != null) && (right.Properties.KeyComparer != null) &&
                    (left.Properties.KeyComparer.ExpressionEquals(right.Properties.KeyComparer) &&
                    (typeof(TKey).GetPartitionType() == null)))
                {
                    this.joinKind = JoinKind.IncreasingOrderEquiJoin;
                    this.fallbackGenerator = (s, e, o) => new IncreasingOrderEquiJoinPipe<TKey, TLeft, TRight, TResult>(s, e, o);
                    this.partitionedGenerator = null;
                    this.columnarGenerator = (k => IncreasingOrderEquiJoinTemplate.Generate(this, this.Selector));
                }
                else
                {
                    this.joinKind = JoinKind.StartEdgeEquijoin;
                    this.fallbackGenerator = (s, e, o) => new StartEdgeEquiJoinPipe<TKey, TLeft, TRight, TResult>(s, e, o);
                    this.partitionedGenerator = (s, e, o) => (BinaryPipe<TKey, TLeft, TRight, TResult>)Activator.CreateInstance(
                        typeof(PartitionedStartEdgeEquiJoinPipe<,,,,>).MakeGenericType(
                             typeof(TKey),
                             typeof(TLeft),
                             typeof(TRight),
                             typeof(TResult),
                             typeof(TKey).GetPartitionType()), s, e, o);
                    this.columnarGenerator = (k => StartEdgeEquiJoinTemplate.Generate(this, this.Selector));
                }
            }
            else
            {
                this.joinKind = JoinKind.EquiJoin;
                this.fallbackGenerator = (s, e, o) => new EquiJoinPipe<TKey, TLeft, TRight, TResult>(s, e, o);
                this.partitionedGenerator = (s, e, o) => (BinaryPipe<TKey, TLeft, TRight, TResult>)Activator.CreateInstance(
                    CreatePartitionedEquiJoinType(), s, e, o);
                this.columnarGenerator = (k => EquiJoinTemplate.Generate(this, this.Selector));
            }

            Initialize();
        }

        private static Type CreatePartitionedEquiJoinType()
        {
            // Simple case: key type is a simple partition key
            if (typeof(TKey).GetGenericTypeDefinition() == typeof(PartitionKey<>))
                return typeof(PartitionedEquiJoinPipeSimple<,,,>).MakeGenericType(
                                         typeof(TLeft),
                                         typeof(TRight),
                                         typeof(TResult),
                                         typeof(TKey).GetPartitionType());
            // Middle case: type is one level of grouping, e.g., TKey = CompoundGroupKey<PartitionKey<TP>, TG>
            if (typeof(TKey).GenericTypeArguments[0].GetGenericTypeDefinition() == typeof(PartitionKey<>))
                return typeof(PartitionedEquiJoinPipeCompound<,,,,>).MakeGenericType(
                                         typeof(TKey).GenericTypeArguments[1],
                                         typeof(TLeft),
                                         typeof(TRight),
                                         typeof(TResult),
                                         typeof(TKey).GetPartitionType());
            // Generic case
            return typeof(PartitionedEquiJoinPipe<,,,,>).MakeGenericType(
                                     typeof(TKey),
                                     typeof(TLeft),
                                     typeof(TRight),
                                     typeof(TResult),
                                     typeof(TKey).GetPartitionType());
        }

        protected override IBinaryObserver<TKey, TLeft, TRight, TResult> CreatePipe(IStreamObserver<TKey, TResult> observer)
        {
            var part = typeof(TKey).GetPartitionType();
            if (part == null)
            {
                if (this.properties.IsColumnar) return GetPipe(observer);
                else return this.fallbackGenerator(this, this.Selector, observer);
            }
            return this.partitionedGenerator(this, this.Selector, observer);
        }

        protected override bool CanGenerateColumnar()
        {
            if (!typeof(TResult).CanRepresentAsColumnar()) return false;

            var lookupKey = CacheKey.Create(this.joinKind, this.Properties.KeyEqualityComparer.GetEqualsExpr().ExpressionToCSharp(), this.Left.Properties.PayloadEqualityComparer.GetEqualsExpr().ExpressionToCSharp(), this.Right.Properties.PayloadEqualityComparer.GetEqualsExpr().ExpressionToCSharp(), this.Selector.ExpressionToCSharp());

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, this.columnarGenerator);

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private BinaryPipe<TKey, TLeft, TRight, TResult> GetPipe(IStreamObserver<TKey, TResult> observer)
        {
            var lookupKey = CacheKey.Create(this.joinKind, this.Properties.KeyEqualityComparer.GetEqualsExpr().ExpressionToCSharp(), this.Left.Properties.PayloadEqualityComparer.GetEqualsExpr().ExpressionToCSharp(), this.Right.Properties.PayloadEqualityComparer.GetEqualsExpr().ExpressionToCSharp(), this.Selector.ExpressionToCSharp());

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, this.columnarGenerator);
            Func<PlanNode, PlanNode, IBinaryObserver, BinaryPlanNode> planNode = ((PlanNode left, PlanNode right, IBinaryObserver o) =>
            {
                var node = new JoinPlanNode(
                        left, right, o,
                        typeof(TLeft), typeof(TRight), typeof(TLeft), typeof(TKey), this.joinKind, true, generatedPipeType.Item2, false);
                node.AddJoinExpression("key comparer", this.Properties.KeyEqualityComparer.GetEqualsExpr());
                node.AddJoinExpression("left payload comparer", this.Left.Properties.PayloadEqualityComparer.GetEqualsExpr());
                node.AddJoinExpression("right payload comparer", this.Right.Properties.PayloadEqualityComparer.GetEqualsExpr());
                node.AddJoinExpression("left key comparer", this.Left.Properties.KeyComparer.GetCompareExpr());
                node.AddJoinExpression("right key comparer", this.Right.Properties.KeyComparer.GetCompareExpr());
                return node;
            });

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, planNode);
            var returnValue = (BinaryPipe<TKey, TLeft, TRight, TResult>)instance;
            return returnValue;
        }
    }
}
