// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
namespace Microsoft.StreamProcessing
{
    internal sealed class DisjointUnionStreamable<TKey, TPayload> : BinaryStreamable<TKey, TPayload, TPayload, TPayload>
    {
        public DisjointUnionStreamable(IStreamable<TKey, TPayload> left, IStreamable<TKey, TPayload> right, bool registerInputs = false)
            : base(left.Properties.Union(right.Properties), left, right, registerInputs) { }

        protected override IBinaryObserver<TKey, TPayload, TPayload, TPayload> CreatePipe(IStreamObserver<TKey, TPayload> observer)
            => new DisjointUnionPipe<TKey, TPayload>(this, observer);

        protected override bool CanGenerateColumnar() => true;
    }
}
