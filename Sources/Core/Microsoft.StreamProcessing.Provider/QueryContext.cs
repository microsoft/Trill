// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Provider
{
    /// <summary>
    ///
    /// </summary>
    public sealed class QueryContext
    {
        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="observable"></param>
        /// <param name="startEdgeSelector"></param>
        /// <param name="endEdgeSelector"></param>
        /// <returns></returns>
        public IQStreamable<TPayload> RegisterStream<TPayload>(
            IObservable<TPayload> observable,
            Expression<Func<TPayload, long>> startEdgeSelector,
            Expression<Func<TPayload, long>> endEdgeSelector)
            => throw new NotImplementedException();

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TPayload"></typeparam>
        /// <param name="observable"></param>
        /// <param name="startEdgeSelector"></param>
        /// <param name="endEdgeSelector"></param>
        /// <param name="disorderPolicy"></param>
        /// <param name="partitionPolicy"></param>
        /// <returns></returns>
        public IQStreamable<TPayload> RegisterStream<TPayload>(
            IObservable<TPayload> observable,
            Expression<Func<TPayload, long>> startEdgeSelector,
            Expression<Func<TPayload, long>> endEdgeSelector,
            DisorderPolicy disorderPolicy,
            PartitionPolicy partitionPolicy)
            => throw new NotImplementedException();

        /// <summary>
        /// Stub
        /// </summary>
        public sealed class DisorderPolicy
        {
            internal static DisorderPolicy Throw() => throw new NotImplementedException();
            internal static DisorderPolicy Drop() => throw new NotImplementedException();
            internal static DisorderPolicy Adjust() => throw new NotImplementedException();
        }

        /// <summary>
        /// Stub
        /// </summary>
        public class PartitionPolicy
        {
            internal static PartitionPolicy Create<TPayload, TPartitionKey>(Expression<Func<TPayload, TPartitionKey>> extractor)
                => throw new NotImplementedException();

            internal static PartitionPolicy Create<TPayload, TPartitionKey>(
                Expression<Func<TPayload, TPartitionKey>> extractor,
                long watermarkLag)
                => throw new NotImplementedException();
        }
    }
}
