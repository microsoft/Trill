// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Provider
{
    internal static class ProviderEgress
    {
        public static IObservable<TOutput> ToTemporalObservable<TPayload, TOutput>(
            this IQStreamable<TPayload> streamable,
            Expression<Func<long, long, TPayload, TOutput>> selector)
            => streamable.Provider.Execute<IStreamable<Empty, TPayload>>(streamable.Expression)
                .ToTemporalObservable(selector);
    }
}
