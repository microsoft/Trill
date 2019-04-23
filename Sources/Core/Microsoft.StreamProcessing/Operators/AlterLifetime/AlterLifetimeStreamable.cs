// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    internal sealed class AlterLifetimeStreamable<TKey, TPayload> : UnaryStreamable<TKey, TPayload, TPayload>
    {
        public readonly LambdaExpression DurationSelector;
        public readonly LambdaExpression StartTimeSelector;

        public AlterLifetimeStreamable(
            IStreamable<TKey, TPayload> source,
            LambdaExpression startTimeSelector,
            LambdaExpression durationSelector)
            : base(source, source.Properties.AlterLifetime(durationSelector))
        {
            Contract.Requires(source != null);

            this.StartTimeSelector = startTimeSelector;
            this.DurationSelector = durationSelector;
        }

        internal static IStreamObserver<TKey, TPayload> CreatePartitionedPayload(
            Type t,
            AlterLifetimeStreamable<TKey, TPayload> stream,
            IStreamObserver<TKey, TPayload> observer)
        {
            var outputType = typeof(PartitionedAlterLifetimeVariableDurationByKeyPipe<,>).MakeGenericType(
                t,
                typeof(TPayload));
            return (IStreamObserver<TKey, TPayload>)Activator.CreateInstance(outputType, stream, observer);
        }

        internal static IStreamObserver<TKey, TPayload> CreatePartitionedVariable(
            Type t,
            AlterLifetimeStreamable<TKey, TPayload> stream,
            IStreamObserver<TKey, TPayload> observer)
        {
            var outputType = typeof(PartitionedAlterLifetimeVariableDurationPipe<,,>).MakeGenericType(
                typeof(TKey),
                typeof(TPayload),
                t);
            return (IStreamObserver<TKey, TPayload>)Activator.CreateInstance(outputType, stream, observer);
        }

        internal static IStreamObserver<TKey, TPayload> CreatePartitionedConstant(
            Type t,
            AlterLifetimeStreamable<TKey, TPayload> stream,
            IStreamObserver<TKey, TPayload> observer)
        {
            var outputType = typeof(PartitionedAlterLifetimeConstantDurationPipe<,,>).MakeGenericType(
                typeof(TKey),
                typeof(TPayload),
                t);
            return (IStreamObserver<TKey, TPayload>)Activator.CreateInstance(outputType, stream, observer);
        }

        internal static IStreamObserver<TKey, TPayload> CreatePartitionedStartDependent(
            Type t,
            AlterLifetimeStreamable<TKey, TPayload> stream,
            IStreamObserver<TKey, TPayload> observer)
        {
            var outputType = typeof(PartitionedAlterLifetimeStartDependentDurationPipe<,,>).MakeGenericType(
                typeof(TKey),
                typeof(TPayload),
                t);
            return (IStreamObserver<TKey, TPayload>)Activator.CreateInstance(outputType, stream, observer);
        }

        internal override IStreamObserver<TKey, TPayload> CreatePipe(IStreamObserver<TKey, TPayload> observer)
        {
            var part = typeof(TKey).GetPartitionType();
            if (this.DurationSelector.Body is ConstantExpression)
            {
                if (this.StartTimeSelector == null)
                {
                    return new AlterLifetimeConstantDurationStatelessPipe<TKey, TPayload>(this, observer);
                }
                else if (part != null)
                {
                    return CreatePartitionedConstant(part, this, observer);
                }
                else
                {
                    return new AlterLifetimeConstantDurationPipe<TKey, TPayload>(this, observer);
                }
            }
            else if (this.DurationSelector is Expression<Func<long, long>>)
            {
                if (this.StartTimeSelector == null)
                {
                    return new AlterLifetimeStartDependentDurationStatelessPipe<TKey, TPayload>(this, observer);
                }
                else if (part != null)
                {
                    return CreatePartitionedStartDependent(part, this, observer);
                }
                else
                {
                    return new AlterLifetimeStartDependentDurationPipe<TKey, TPayload>(this, observer);
                }
            }
            else if (this.DurationSelector is Expression<Func<long, long, long>>)
            {
                return part != null
                    ? CreatePartitionedVariable(part, this, observer)
                    : new AlterLifetimeVariableDurationPipe<TKey, TPayload>(this, observer);
            }
            else if (part != null && typeof(TKey).GetGenericTypeDefinition().Equals(typeof(PartitionKey<>)))
            {
                return CreatePartitionedPayload(part, this, observer);
            }
            else
            {
                throw new InvalidOperationException("Unknown alter duration expression type: " + this.DurationSelector.GetType().ToString());
            }
        }

        protected override bool CanGenerateColumnar() => false;
    }
}