// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Aggregates
{
    internal abstract class ListAggregateBase<T, R> : IAggregate<T, List<T>, R>
    {
        private static readonly Expression<Func<List<T>>> init = () => new List<T>();
        public Expression<Func<List<T>>> InitialState() => init;

        public Expression<Func<List<T>, long, T, List<T>>> Accumulate()
        {
            Expression<Action<List<T>, long, T>> temp = (set, timestamp, input) => set.Add(input);
            var block = Expression.Block(temp.Body, temp.Parameters[0]);
            return Expression.Lambda<Func<List<T>, long, T, List<T>>>(block, temp.Parameters);
        }

        public Expression<Func<List<T>, long, T, List<T>>> Deaccumulate()
        {
            Expression<Action<List<T>, long, T>> temp = (set, timestamp, input) => set.Remove(input);
            var block = Expression.Block(temp.Body, temp.Parameters[0]);
            return Expression.Lambda<Func<List<T>, long, T, List<T>>>(block, temp.Parameters);
        }

        public Expression<Func<List<T>, List<T>, List<T>>> Difference() => (leftSet, rightSet) => SetExcept(leftSet, rightSet);

        private static List<T> SetExcept(List<T> left, List<T> right)
        {
            var newList = new List<T>(left);
            foreach (var t in right) newList.Remove(t);
            return newList;
        }

        public abstract Expression<Func<List<T>, R>> ComputeResult();
    }
}
