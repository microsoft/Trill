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
    internal sealed class ShuffleNestedStreamable<TOuterKey, TSource, TInnerKey> : Streamable<CompoundGroupKey<TOuterKey, TInnerKey>, TSource>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Used to avoid creating redundant readonly property.")]
        public readonly Expression<Func<TSource, TInnerKey>> KeySelector;
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Used to avoid creating redundant readonly property.")]
        public readonly IStreamable<TOuterKey, TSource> Source;
        public readonly int totalBranchesL2;
        public readonly int shuffleId;
        private readonly GroupNestedStreamable<TOuterKey, TSource, TInnerKey> singleThreadedShuffler;
        public readonly bool powerOf2;

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2233:OperationsShouldNotOverflow", MessageId = "_totalBranchesL2-1", Justification = "Enforced with code contract.")]
        public ShuffleNestedStreamable(
            IStreamable<TOuterKey, TSource> source,
            Expression<Func<TSource, TInnerKey>> keySelector,            int totalBranchesL2,
            int shuffleId)
            : base(source.Properties.GroupNested(keySelector))
        {
            Contract.Requires(source != null);
            Contract.Requires(keySelector != null);
            Contract.Requires(totalBranchesL2 > 0);

            Source = source;
            KeySelector = keySelector;
            this.totalBranchesL2 = totalBranchesL2;
            this.shuffleId = shuffleId;
            powerOf2 = ((totalBranchesL2 & (totalBranchesL2 - 1)) == 0);

            if (totalBranchesL2 <= 1)
            {
                singleThreadedShuffler = new GroupNestedStreamable<TOuterKey, TSource, TInnerKey>(source, keySelector);
                this.properties = singleThreadedShuffler.Properties;
            }
        }

        private IStreamObserverAndNestedGroupedStreamObservable<TOuterKey, TSource, TInnerKey> pipe = null;
        private int numBranches = 0;

        public override IDisposable Subscribe(IStreamObserver<CompoundGroupKey<TOuterKey, TInnerKey>, TSource> observer)
        {
            if (totalBranchesL2 <= 1)
            {
                return singleThreadedShuffler.Subscribe(observer);
            }

            numBranches++;
            if (pipe == null)
            {
                if (this.Properties.IsColumnar && CanGenerateColumnar()) pipe = GetPipe(observer, totalBranchesL2, shuffleId);
                else pipe = CreatePipe(observer);
            }
            var o = observer;
            pipe.AddObserver(o);

            var d = o as IDisposable;
            if (numBranches < totalBranchesL2)
            {
                return d ?? Utility.EmptyDisposable;
            }
            else
            {
                // Reset status for next set of subscribe calls
                var oldpipe = pipe;
                pipe = null;
                numBranches = 0;

                return d == null
                    ? Source.Subscribe(oldpipe)
                    : Utility.CreateDisposable(Source.Subscribe(oldpipe), d);
            }
        }

        private IStreamObserverAndNestedGroupedStreamObservable<TOuterKey, TSource, TInnerKey> CreatePipe(
            IStreamObserver<CompoundGroupKey<TOuterKey, TInnerKey>, TSource> observer)
        {
            if (typeof(TOuterKey).GetPartitionType() == null)
                return new ShuffleNestedPipe<TOuterKey, TSource, TInnerKey>(this, observer, totalBranchesL2, shuffleId);
            return new PartitionedShuffleNestedPipe<TOuterKey, TSource, TInnerKey>(this, observer, totalBranchesL2, shuffleId);
        }

        private bool CanGenerateColumnar()
        {
            var typeOfTOuterKey = typeof(TOuterKey);
            var typeOfTSource = typeof(TSource);
            var typeOfTInnerKey = typeof(TInnerKey);

            if (!typeOfTSource.CanRepresentAsColumnar()) return false;
            if (typeOfTOuterKey.GetPartitionType() != null) return false;
            if (typeOfTInnerKey.GetPartitionType() != null) return false;

            var keyEqComparer = Properties.KeyEqualityComparer;
            string inlinedHashCodeComputation;
            if (keyEqComparer is CompoundGroupKeyEqualityComparer<TOuterKey, TInnerKey> comparer)
            {
                var y = comparer.innerComparer.GetGetHashCodeExpr();
                inlinedHashCodeComputation = y.Inline("key");
            }
            else
            {
                inlinedHashCodeComputation = keyEqComparer.GetGetHashCodeExpr().Inline("key");
            }

            var lookupKey = CacheKey.Create(KeySelector.ToString(), inlinedHashCodeComputation, powerOf2);
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => ShuffleTemplate.Generate<TOuterKey, TSource, TInnerKey>(this.KeySelector, inlinedHashCodeComputation, true, this.powerOf2));

            errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private IStreamObserverAndNestedGroupedStreamObservable<TOuterKey, TSource, TInnerKey> GetPipe(IStreamObserver<CompoundGroupKey<TOuterKey, TInnerKey>, TSource> observer, int totalBranchesL2, int shuffleId)
        {
            var keyEqComparer = Properties.KeyEqualityComparer;
            string inlinedHashCodeComputation;
            if (keyEqComparer is CompoundGroupKeyEqualityComparer<TOuterKey, TInnerKey> comparer)
            {
                var y = comparer.innerComparer.GetGetHashCodeExpr();
                inlinedHashCodeComputation = y.Inline("key");
            }
            else
            {
                inlinedHashCodeComputation = keyEqComparer.GetGetHashCodeExpr().Inline("key");
            }

            var lookupKey = CacheKey.Create(KeySelector.ToString(), inlinedHashCodeComputation, powerOf2);
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => ShuffleTemplate.Generate<TOuterKey, TSource, TInnerKey>(this.KeySelector, inlinedHashCodeComputation, true, this.powerOf2));

            Func<PlanNode, IQueryObject, PlanNode> planNode = ((PlanNode p, IQueryObject o) => new GroupPlanNode(
                    p,
                    o,
                    typeof(TOuterKey),
                    typeof(CompoundGroupKey<TOuterKey, TInnerKey>),
                    typeof(TSource),
                    this.KeySelector,
                    this.shuffleId,
                    this.totalBranchesL2,
                    true,
                    true,
                    generatedPipeType.Item2));

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, totalBranchesL2, shuffleId, planNode);
            var returnValue = (IStreamObserverAndNestedGroupedStreamObservable<TOuterKey, TSource, TInnerKey>)instance;
            return returnValue;
        }
    }

    internal sealed class ShuffleStreamable<TOuterKey, TSource, TInnerKey> : Streamable<TInnerKey, TSource>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Used to avoid creating redundant readonly property.")]
        public readonly Expression<Func<TSource, TInnerKey>> KeySelector;
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Used to avoid creating redundant readonly property.")]
        public readonly IStreamable<TOuterKey, TSource> Source;
        public readonly int totalBranchesL2;
        public readonly int shuffleId;
        private readonly GroupStreamable<TOuterKey, TSource, TInnerKey> singleThreadedShuffler;
        public readonly bool powerOf2;

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2233:OperationsShouldNotOverflow", MessageId = "_totalBranchesL2-1", Justification = "Enforced with code contract.")]
        public ShuffleStreamable(
            IStreamable<TOuterKey, TSource> source,
            Expression<Func<TSource, TInnerKey>> keySelector,            int totalBranchesL2,
            int shuffleId)
            : base(source.Properties.Group(keySelector))
        {
            Contract.Requires(source != null);
            Contract.Requires(keySelector != null);
            Contract.Requires(totalBranchesL2 > 0);

            Source = source;
            KeySelector = keySelector;
            this.totalBranchesL2 = totalBranchesL2;
            this.shuffleId = shuffleId;
            powerOf2 = ((totalBranchesL2 & (totalBranchesL2 - 1)) == 0);

            if (totalBranchesL2 <= 1)
            {
                singleThreadedShuffler = new GroupStreamable<TOuterKey, TSource, TInnerKey>(source, keySelector);
                this.properties = singleThreadedShuffler.Properties;
            }
        }

        private IStreamObserverAndGroupedStreamObservable<TOuterKey, TSource, TInnerKey> pipe = null;
        private int numBranches = 0;

        public override IDisposable Subscribe(IStreamObserver<TInnerKey, TSource> observer)
        {
            if (totalBranchesL2 <= 1)
            {
                return singleThreadedShuffler.Subscribe(observer);
            }

            numBranches++;
            if (pipe == null)
            {
                if (this.Properties.IsColumnar && CanGenerateColumnar()) pipe = GetPipe(observer, totalBranchesL2, shuffleId);
                else pipe = CreatePipe(observer);
            }
            var o = observer;
            pipe.AddObserver(o);

            var d = o as IDisposable;
            if (numBranches < totalBranchesL2)
            {
                return d ?? Utility.EmptyDisposable;
            }
            else
            {
                // Reset status for next set of subscribe calls
                var oldpipe = pipe;
                pipe = null;
                numBranches = 0;

                return d == null
                    ? Source.Subscribe(oldpipe)
                    : Utility.CreateDisposable(Source.Subscribe(oldpipe), d);
            }
        }

        private IStreamObserverAndGroupedStreamObservable<TOuterKey, TSource, TInnerKey> CreatePipe(
            IStreamObserver<TInnerKey, TSource> observer)
        {
            return new ShufflePipe<TOuterKey, TSource, TInnerKey>(this, observer, totalBranchesL2, shuffleId);
        }

        private bool CanGenerateColumnar()
        {
            var typeOfTOuterKey = typeof(TOuterKey);
            var typeOfTSource = typeof(TSource);
            var typeOfTInnerKey = typeof(TInnerKey);

            if (!typeOfTSource.CanRepresentAsColumnar()) return false;
            if (typeOfTOuterKey.GetPartitionType() != null) return false;
            if (typeOfTInnerKey.GetPartitionType() != null) return false;

            var keyEqComparer = Properties.KeyEqualityComparer;
            string inlinedHashCodeComputation;
            if (keyEqComparer is CompoundGroupKeyEqualityComparer<TOuterKey, TInnerKey> comparer)
            {
                var y = comparer.innerComparer.GetGetHashCodeExpr();
                inlinedHashCodeComputation = y.Inline("key");
            }
            else
            {
                inlinedHashCodeComputation = keyEqComparer.GetGetHashCodeExpr().Inline("key");
            }

            var lookupKey = CacheKey.Create(KeySelector.ToString(), inlinedHashCodeComputation, powerOf2);
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => ShuffleTemplate.Generate<TOuterKey, TSource, TInnerKey>(this.KeySelector, inlinedHashCodeComputation, false, this.powerOf2));

            errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private IStreamObserverAndGroupedStreamObservable<TOuterKey, TSource, TInnerKey> GetPipe(IStreamObserver<TInnerKey, TSource> observer, int totalBranchesL2, int shuffleId)
        {
            var keyEqComparer = Properties.KeyEqualityComparer;
            string inlinedHashCodeComputation;
            if (keyEqComparer is CompoundGroupKeyEqualityComparer<TOuterKey, TInnerKey> comparer)
            {
                var y = comparer.innerComparer.GetGetHashCodeExpr();
                inlinedHashCodeComputation = y.Inline("key");
            }
            else
            {
                inlinedHashCodeComputation = keyEqComparer.GetGetHashCodeExpr().Inline("key");
            }

            var lookupKey = CacheKey.Create(KeySelector.ToString(), inlinedHashCodeComputation, powerOf2);
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => ShuffleTemplate.Generate<TOuterKey, TSource, TInnerKey>(this.KeySelector, inlinedHashCodeComputation, false, this.powerOf2));

            Func<PlanNode, IQueryObject, PlanNode> planNode = ((PlanNode p, IQueryObject o) => new GroupPlanNode(
                    p,
                    o,
                    typeof(TOuterKey),
                    typeof(CompoundGroupKey<TOuterKey, TInnerKey>),
                    typeof(TSource),
                    this.KeySelector,
                    this.shuffleId,
                    this.totalBranchesL2,
                    true,
                    true,
                    generatedPipeType.Item2));

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, totalBranchesL2, shuffleId, planNode);
            var returnValue = (IStreamObserverAndGroupedStreamObservable<TOuterKey, TSource, TInnerKey>)instance;
            return returnValue;
        }
    }

    internal sealed class ShuffleSameKeyStreamable<TOuterKey, TSource, TInnerKey> : Streamable<TOuterKey, TSource>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Used to avoid creating redundant readonly property.")]
        public readonly IStreamable<TOuterKey, TSource> Source;
        public readonly int totalBranchesL2;
        public readonly int shuffleId;
        public readonly bool powerOf2;

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2233:OperationsShouldNotOverflow", MessageId = "_totalBranchesL2-1", Justification = "Enforced with code contract.")]
        public ShuffleSameKeyStreamable(
            IStreamable<TOuterKey, TSource> source,
                        int totalBranchesL2,
            int shuffleId)
            : base(source.Properties)
        {
            Contract.Requires(source != null);
            Contract.Requires(totalBranchesL2 > 0);

            Source = source;
            this.totalBranchesL2 = totalBranchesL2;
            this.shuffleId = shuffleId;
            powerOf2 = ((totalBranchesL2 & (totalBranchesL2 - 1)) == 0);

        }

        private IStreamObserverAndSameKeyGroupedStreamObservable<TOuterKey, TSource, TOuterKey> pipe = null;
        private int numBranches = 0;

        public override IDisposable Subscribe(IStreamObserver<TOuterKey, TSource> observer)
        {
            if (totalBranchesL2 <= 1)
            {
                return Source.Subscribe(observer);
            }

            numBranches++;
            if (pipe == null)
            {
                if (this.Properties.IsColumnar && CanGenerateColumnar()) pipe = GetPipe(observer, totalBranchesL2, shuffleId);
                else pipe = CreatePipe(observer);
            }
            var o = observer;
            pipe.AddObserver(o);

            var d = o as IDisposable;
            if (numBranches < totalBranchesL2)
            {
                return d ?? Utility.EmptyDisposable;
            }
            else
            {
                // Reset status for next set of subscribe calls
                var oldpipe = pipe;
                pipe = null;
                numBranches = 0;

                return d == null
                    ? Source.Subscribe(oldpipe)
                    : Utility.CreateDisposable(Source.Subscribe(oldpipe), d);
            }
        }

        private IStreamObserverAndSameKeyGroupedStreamObservable<TOuterKey, TSource, TOuterKey> CreatePipe(
            IStreamObserver<TOuterKey, TSource> observer)
        {
            return new ShuffleSameKeyPipe<TOuterKey, TSource, TInnerKey>(this, observer, totalBranchesL2, shuffleId);
        }

        private bool CanGenerateColumnar()
        {
            var typeOfTOuterKey = typeof(TOuterKey);
            var typeOfTSource = typeof(TSource);
            var typeOfTInnerKey = typeof(TInnerKey);

            if (!typeOfTSource.CanRepresentAsColumnar()) return false;
            if (typeOfTOuterKey.GetPartitionType() != null) return false;
            if (typeOfTInnerKey.GetPartitionType() != null) return false;

            var keyEqComparer = Properties.KeyEqualityComparer;
            string inlinedHashCodeComputation;
            if (keyEqComparer is CompoundGroupKeyEqualityComparer<TOuterKey, TInnerKey> comparer)
            {
                var y = comparer.innerComparer.GetGetHashCodeExpr();
                inlinedHashCodeComputation = y.Inline("key");
            }
            else
            {
                inlinedHashCodeComputation = keyEqComparer.GetGetHashCodeExpr().Inline("key");
            }

            var lookupKey = CacheKey.Create(inlinedHashCodeComputation, powerOf2);
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => ShuffleTemplate.Generate<TOuterKey, TSource, TInnerKey>(null, inlinedHashCodeComputation, false, this.powerOf2));

            errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private IStreamObserverAndSameKeyGroupedStreamObservable<TOuterKey, TSource, TOuterKey> GetPipe(IStreamObserver<TOuterKey, TSource> observer, int totalBranchesL2, int shuffleId)
        {
            var keyEqComparer = Properties.KeyEqualityComparer;
            string inlinedHashCodeComputation;
            if (keyEqComparer is CompoundGroupKeyEqualityComparer<TOuterKey, TInnerKey> comparer)
            {
                var y = comparer.innerComparer.GetGetHashCodeExpr();
                inlinedHashCodeComputation = y.Inline("key");
            }
            else
            {
                inlinedHashCodeComputation = keyEqComparer.GetGetHashCodeExpr().Inline("key");
            }

            var lookupKey = CacheKey.Create(inlinedHashCodeComputation, powerOf2);
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => ShuffleTemplate.Generate<TOuterKey, TSource, TInnerKey>(null, inlinedHashCodeComputation, false, this.powerOf2));

            Func<PlanNode, IQueryObject, PlanNode> planNode = ((PlanNode p, IQueryObject o) => new GroupPlanNode(
                    p,
                    o,
                    typeof(TOuterKey),
                    typeof(CompoundGroupKey<TOuterKey, TInnerKey>),
                    typeof(TSource),
                    null,
                    this.shuffleId,
                    this.totalBranchesL2,
                    true,
                    true,
                    generatedPipeType.Item2));

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, totalBranchesL2, shuffleId, planNode);
            var returnValue = (IStreamObserverAndSameKeyGroupedStreamObservable<TOuterKey, TSource, TOuterKey>)instance;
            return returnValue;
        }
    }

}