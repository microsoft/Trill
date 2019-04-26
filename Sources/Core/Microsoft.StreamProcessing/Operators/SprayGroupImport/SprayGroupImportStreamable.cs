// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;

namespace Microsoft.StreamProcessing
{
    internal sealed class SprayGroupImportStreamable<TKey, TSpray> : Streamable<TKey, TSpray>
    {
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Used to avoid creating redundant readonly property.")]
        public readonly IStreamable<TKey, TSpray> Source;
        private IBothStreamObserverAndStreamObservable<TKey, TSpray> pipe = null;
        private int numBranches = 0;
        private readonly int totalBranches = 0;
        private readonly IComparerExpression<TSpray> spraySortOrderComparer;
        public readonly bool IsColumnar;
        public bool multicast;

        private readonly Guid observerClassId;

        public SprayGroupImportStreamable(
            IStreamable<TKey, TSpray> source,
            int totalBranches,
            bool multicast = false,
            IComparerExpression<TSpray> sprayComparer = null)
            : base(source.Properties.ToMulticore(true))
        {
            Contract.Requires(source != null);

            this.totalBranches = totalBranches;
            this.Source = source;
            this.spraySortOrderComparer = sprayComparer; // source.Properties.PayloadComparer;
            this.IsColumnar = source.Properties.IsColumnar;
            this.multicast = multicast;

            this.observerClassId = Guid.NewGuid();
        }

        public override IDisposable Subscribe(IStreamObserver<TKey, TSpray> observer)
        {
            if (this.totalBranches <= 1)
            {
                // completely elide this operator
                return this.Source.Subscribe(observer);
            }

            this.numBranches++;
            if (this.pipe == null)
            {
                this.pipe = CreatePipe(observer);

            }
            var o = (!this.multicast) && (this.spraySortOrderComparer == null)
                ? Config.StreamScheduler.RegisterStreamObserver(observer, this.observerClassId)
                : Config.StreamScheduler.RegisterStreamObserver(observer);
            this.pipe.AddObserver(o);

            var d = o as IDisposable;
            if (this.numBranches < this.totalBranches) return d ?? Utility.EmptyDisposable;
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

        private IBothStreamObserverAndStreamObservable<TKey, TSpray> CreatePipe(IStreamObserver<TKey, TSpray> observer)
            => new SynchronousGAPipe<TKey, TSpray>(this, observer, this.spraySortOrderComparer, this.totalBranches, this.multicast);
    }
}