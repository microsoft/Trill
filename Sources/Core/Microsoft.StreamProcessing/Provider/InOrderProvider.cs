// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Provider
{
    internal class InOrderProvider : IQStreamableProvider
    {
        private readonly InOrderProviderVisitor visitor = new InOrderProviderVisitor();

        public IQStreamable<TElement> CreateQuery<TElement>(Expression expression) => throw new NotImplementedException();

        public TResult Execute<TResult>(Expression expression) => throw new NotImplementedException();
    }
}
