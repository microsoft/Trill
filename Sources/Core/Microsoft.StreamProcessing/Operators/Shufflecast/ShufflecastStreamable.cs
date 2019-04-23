// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    internal sealed class ShufflecastStreamable<TSource, TInnerKey> : Streamable<TInnerKey, TSource>
    {
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Used to avoid creating redundant readonly property.")]
        public readonly IStreamable<TInnerKey, TSource> Source;
        public readonly int totalBranchesL2;
        public readonly bool powerOf2;

        private readonly Func<TInnerKey, int, TSource, int[]> destinationSelectorCompiled = null;

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2233:OperationsShouldNotOverflow", MessageId = "_totalBranchesL2-1", Justification = "Enforced with code contract.")]
        public ShufflecastStreamable(
            IStreamable<TInnerKey, TSource> source,
            int totalBranchesL2,
            Expression<Func<TInnerKey, int, TSource, int[]>> destinationSelector = null)
            : base(source.Properties)
        {
            Contract.Requires(source != null);
            Contract.Requires(totalBranchesL2 > 0);

            this.Source = source;
            this.totalBranchesL2 = totalBranchesL2;
            this.powerOf2 = ((totalBranchesL2 & (totalBranchesL2 - 1)) == 0);

            if (destinationSelector != null)
            {
                this.destinationSelectorCompiled = destinationSelector.Compile();
            }
        }

        private IStreamObserverAndGroupedStreamObservable<TInnerKey, TSource, TInnerKey> pipe = null;
        private int numBranches = 0;

        public override IDisposable Subscribe(IStreamObserver<TInnerKey, TSource> observer)
        {
            if (this.totalBranchesL2 <= 1)
            {
                return this.Source.Subscribe(observer);
            }

            this.numBranches++;
            if (this.pipe == null)
            {
                this.pipe = CreatePipe(observer);
            }
            var o = observer;
            this.pipe.AddObserver(o);

            var d = o as IDisposable;
            if (this.numBranches < this.totalBranchesL2) return d ?? Utility.EmptyDisposable;
            else
            {
                // Reset status for next set of subscribe calls
                var oldpipe = this.pipe;
                this.pipe = null;
                this.numBranches = 0;

                return d == null
                    ? this.Source.Subscribe(oldpipe)
                    : Utility.CreateDisposable(this.Source.Subscribe(oldpipe), d);
            }
        }

        private IStreamObserverAndGroupedStreamObservable<TInnerKey, TSource, TInnerKey> CreatePipe(
            IStreamObserver<TInnerKey, TSource> observer)
            => new ShufflecastPipe<TSource, TInnerKey>(this, observer, this.totalBranchesL2, this.destinationSelectorCompiled);
    }

}