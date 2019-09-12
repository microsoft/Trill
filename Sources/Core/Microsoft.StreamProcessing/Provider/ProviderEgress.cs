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
    public static class ProviderEgress
    {
        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TPayload"></typeparam>
        /// <typeparam name="TOutput"></typeparam>
        /// <param name="streamable"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public static IObservable<TOutput> ToTemporalObservable<TPayload, TOutput>(
            this IQStreamable<TPayload> streamable,
            Expression<Func<long, long, TPayload, TOutput>> selector)
            => streamable.Provider.Execute<IStreamable<Empty, TPayload>>(streamable.Expression)
                .ToTemporalObservable(selector);
    }
}
