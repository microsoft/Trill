// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
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

            if (left.Properties.IsStartEdgeOnly && right.Properties.IsStartEdgeOnly)
            {
                if ((left.Properties.KeyComparer != null) && (right.Properties.KeyComparer != null) &&
                     left.Properties.KeyComparer.ExpressionEquals(right.Properties.KeyComparer) &&
                    (typeof(TKey).GetPartitionType() == null))
                {
                    this.joinKind = JoinKind.IncreasingOrderEquiJoin;
                    this.fallbackGenerator = (s, e, o) => new IncreasingOrderEquiJoinPipe<TKey, TLeft, TRight, TResult>(s, e, o);
                    this.partitionedGenerator = null;
                    this.columnarGenerator = k => IncreasingOrderEquiJoinTemplate.Generate(this, this.Selector);
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
                    this.columnarGenerator = k => StartEdgeEquiJoinTemplate.Generate(this, this.Selector);
                }
            }
            else if (left.Properties.IsConstantDuration && right.Properties.IsConstantDuration)
            {
                this.joinKind = JoinKind.FixedIntervalEquiJoin;
                this.fallbackGenerator = (s, e, o) => new FixedIntervalEquiJoinPipe<TKey, TLeft, TRight, TResult>(s, e, o);
                this.partitionedGenerator = (s, e, o) => (BinaryPipe<TKey, TLeft, TRight, TResult>)Activator.CreateInstance(
                    CreatePartitionedFixedIntervalEquiJoinType(), s, e, o);
                this.columnarGenerator = k => FixedIntervalEquiJoinTemplate.Generate(this, this.Selector);
            }
            else
            {
                this.joinKind = JoinKind.EquiJoin;
                this.fallbackGenerator = (s, e, o) => new EquiJoinPipe<TKey, TLeft, TRight, TResult>(s, e, o);
                this.partitionedGenerator = (s, e, o) => (BinaryPipe<TKey, TLeft, TRight, TResult>)Activator.CreateInstance(
                    CreatePartitionedEquiJoinType(), s, e, o);
                this.columnarGenerator = k => EquiJoinTemplate.Generate(this, this.Selector);
            }

            Initialize();
        }

        private static Type CreatePartitionedEquiJoinType()
        {
            // Simple case: key type is a simple partition key
            if (typeof(TKey).GetGenericTypeDefinition() == typeof(PartitionKey<>))
            {
                return typeof(PartitionedEquiJoinPipeSimple<,,,>).MakeGenericType(
                                         typeof(TLeft),
                                         typeof(TRight),
                                         typeof(TResult),
                                         typeof(TKey).GetPartitionType());
            }

            // Middle case: type is one level of grouping, e.g., TKey = CompoundGroupKey<PartitionKey<TP>, TG>
            if (typeof(TKey).GenericTypeArguments[0].GetGenericTypeDefinition() == typeof(PartitionKey<>))
            {
                return typeof(PartitionedEquiJoinPipeCompound<,,,,>).MakeGenericType(
                                         typeof(TKey).GenericTypeArguments[1],
                                         typeof(TLeft),
                                         typeof(TRight),
                                         typeof(TResult),
                                         typeof(TKey).GetPartitionType());
            }

            // Generic case
            return typeof(PartitionedEquiJoinPipe<,,,,>).MakeGenericType(
                                     typeof(TKey),
                                     typeof(TLeft),
                                     typeof(TRight),
                                     typeof(TResult),
                                     typeof(TKey).GetPartitionType());
        }

        private static Type CreatePartitionedFixedIntervalEquiJoinType()
        {
            // Simple case: key type is a simple partition key
            if (typeof(TKey).GetGenericTypeDefinition() == typeof(PartitionKey<>))
            {
                return typeof(PartitionedFixedIntervalEquiJoinPipeSimple<,,,>).MakeGenericType(
                                         typeof(TLeft),
                                         typeof(TRight),
                                         typeof(TResult),
                                         typeof(TKey).GetPartitionType());
            }

            // Middle case: type is one level of grouping, e.g., TKey = CompoundGroupKey<PartitionKey<TP>, TG>
            if (typeof(TKey).GenericTypeArguments[0].GetGenericTypeDefinition() == typeof(PartitionKey<>))
            {
                return typeof(PartitionedFixedIntervalEquiJoinPipeCompound<,,,,>).MakeGenericType(
                                         typeof(TKey).GenericTypeArguments[1],
                                         typeof(TLeft),
                                         typeof(TRight),
                                         typeof(TResult),
                                         typeof(TKey).GetPartitionType());
            }

            // Generic case
            return typeof(PartitionedFixedIntervalEquiJoinPipe<,,,,>).MakeGenericType(
                                     typeof(TKey),
                                     typeof(TLeft),
                                     typeof(TRight),
                                     typeof(TResult),
                                     typeof(TKey).GetPartitionType());
        }

        protected override IBinaryObserver<TKey, TLeft, TRight, TResult> CreatePipe(IStreamObserver<TKey, TResult> observer)
            => typeof(TKey).GetPartitionType() != null
                ? this.partitionedGenerator(this, this.Selector, observer)
                : this.properties.IsColumnar
                    ? GetPipe(observer)
                    : this.fallbackGenerator(this, this.Selector, observer);

        protected override bool CanGenerateColumnar()
        {
            // This operator uses the equality method on payloads
            if (this.Left.Properties.IsColumnar && !this.Left.Properties.IsStartEdgeOnly && !this.Left.Properties.PayloadEqualityComparer.CanUsePayloadEquality())
            {
                this.errorMessages = $"The left input payload type, '{typeof(TLeft).FullName}', to Equijoin does not implement the interface {nameof(IEqualityComparerExpression<TLeft>)}. This interface is needed for code generation of this operator for columnar mode. Furthermore, the equality expression in the interface can only refer to input variables if used in field or property references.";
                if (Config.CodegenOptions.DontFallBackToRowBasedExecution)
                    throw new StreamProcessingException(this.errorMessages);
                return false;
            }

            // This operator uses the equality method on payloads
            if (this.Right.Properties.IsColumnar && !this.Right.Properties.IsStartEdgeOnly && !this.Right.Properties.PayloadEqualityComparer.CanUsePayloadEquality())
            {
                this.errorMessages = $"The right input payload type, '{typeof(TRight).FullName}', to Equijoin does not implement the interface {nameof(IEqualityComparerExpression<TRight>)}. This interface is needed for code generation of this operator for columnar mode. Furthermore, the equality expression in the interface can only refer to input variables if used in field or property references.";
                if (Config.CodegenOptions.DontFallBackToRowBasedExecution)
                    throw new StreamProcessingException(this.errorMessages);
                return false;
            }

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
            Func<PlanNode, PlanNode, IBinaryObserver, BinaryPlanNode> planNode = (PlanNode left, PlanNode right, IBinaryObserver o) =>
            {
                var node = new JoinPlanNode(
                    left, right, o,
                    typeof(TLeft), typeof(TRight), typeof(TLeft), typeof(TKey), this.joinKind, true, generatedPipeType.Item2);
                node.AddJoinExpression("key comparer", this.Properties.KeyEqualityComparer.GetEqualsExpr());
                node.AddJoinExpression("left payload comparer", this.Left.Properties.PayloadEqualityComparer.GetEqualsExpr());
                node.AddJoinExpression("right payload comparer", this.Right.Properties.PayloadEqualityComparer.GetEqualsExpr());
                node.AddJoinExpression("left key comparer", this.Left.Properties.KeyComparer.GetCompareExpr());
                node.AddJoinExpression("right key comparer", this.Right.Properties.KeyComparer.GetCompareExpr());
                return node;
            };

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, planNode);
            var returnValue = (BinaryPipe<TKey, TLeft, TRight, TResult>)instance;
            return returnValue;
        }
    }
}
