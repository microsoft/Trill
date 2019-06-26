// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
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

        private void TestMethod()
        {
            IQStreamable<Tuple<string, int>> test0 = null;
            IQStreamable<Tuple<string, int>> test1 = null;
            IQStreamable<Tuple<string, int>> test2 = null;
            var test3 = from t in test1
                        join t1 in test0 on t.Item1 equals t1.Item1
                        join t2 in test2 on t.Item1 equals t2.Item1

                        // join t0 in test0 on t.Item1 equals t0.Item1
                        where t.Item2 < 10
                        group t by t.Item1 into g
                        select g.Window.Count();
        }

        private void TestMethod2()
        {
            IQStreamable<Tuple<string, int, IEnumerable<object>>> test1 = null;
            var test3 = from t in test1
                        from o in t.Item3

                        // join t0 in test0 on t.Item1 equals t0.Item1
                        where t.Item2 < 10
                        group o by t.Item1 into g
                        select g.Window.Count();
        }

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
