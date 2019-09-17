// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Aggregates
{
    internal sealed class NaiveAggregate<TInput, TOutput> : ListAggregateBase<TInput, TOutput>
    {
        private readonly Expression<Func<List<TInput>, TOutput>> createResult;

        public NaiveAggregate(Expression<Func<List<TInput>, TOutput>> resultConstructor)
            => this.createResult = resultConstructor;

        public override Expression<Func<List<TInput>, TOutput>> ComputeResult() => createResult;
    }

    internal sealed class NaiveGrouping<TKey, TPayload> : IGrouping<TKey, TPayload>
    {
        public NaiveGrouping(TKey groupingKey, IEnumerable<TPayload> collection)
        {
            this.Key = groupingKey;
            this.Collection = collection;
        }

        public TKey Key { get; }

        private IEnumerable<TPayload> Collection { get; }

        public IEnumerator<TPayload> GetEnumerator() => this.Collection.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => this.Collection.GetEnumerator();
    }
}
