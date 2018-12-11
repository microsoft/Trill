// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    internal sealed class PartitionStreamable<TPartitionKey, TPayload> : Streamable<PartitionKey<TPartitionKey>, TPayload>
    {
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification="Used to avoid creating redundant readonly property.")]
        public readonly Expression<Func<TPayload, TPartitionKey>> KeySelector;
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification="Used to avoid creating redundant readonly property.")]
        public readonly IStreamable<Empty, TPayload> Source;
        public readonly long PartitionLag;

        public PartitionStreamable(
            IStreamable<Empty, TPayload> source, Expression<Func<TPayload, TPartitionKey>> keySelector, long partitionLag)
            : base(source.Properties.Partition(keySelector))
        {
            Contract.Requires(source != null);
            Contract.Requires(keySelector != null);

            this.Source = source;
            this.KeySelector = keySelector;
            this.PartitionLag = partitionLag;

            if (source.Properties.IsColumnar)
            {
                // No current support for partitioned columnar
                this.properties = this.properties.ToRowBased();
                this.Source = this.Source.ColumnToRow();
            }
        }

        public override IDisposable Subscribe(IStreamObserver<PartitionKey<TPartitionKey>, TPayload> observer)
        {
            var pipe = new PartitionPipe<TPartitionKey, TPayload>(this, observer);
            return this.Source.Subscribe(pipe);
        }
    }

}