// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;

namespace Microsoft.StreamProcessing
{
    public static partial class Streamable
    {
        /// <summary>
        /// Imports an observable of grouped batch data and produces an IStreamable source. Assumes that the data within and between batches
        /// is sync time ordered.
        /// </summary>
        internal static IStreamable<Empty, TPayload> CreateStreamable<TPayload>(this IObservable<StreamMessage<Empty, TPayload>> source)
        {
            Contract.Requires(source != null);

            var p = StreamProperties<Empty, TPayload>.Default;
            if (Config.ForceRowBasedExecution || !typeof(TPayload).CanRepresentAsColumnar())
                p = p.ToRowBased();
            return new StreamMessageIngressStreamable<TPayload>(source, p, null, null);
        }

        /// <summary>
        /// Imports an observable of grouped batch data and produces an IStreamable source. Assumes that the data within and between batches
        /// is sync time ordered.
        /// </summary>
        internal static IStreamable<Empty, TPayload> RegisterInput<TPayload>(this QueryContainer container, IObservable<StreamMessage<Empty, TPayload>> source, string identifier = null)
        {
            Contract.Requires(source != null);

            var p = StreamProperties<Empty, TPayload>.Default;
            if (Config.ForceRowBasedExecution || !typeof(TPayload).CanRepresentAsColumnar())
                p = p.ToRowBased();
            return new StreamMessageIngressStreamable<TPayload>(source, p, container, identifier ?? Guid.NewGuid().ToString());
        }
    }
}
