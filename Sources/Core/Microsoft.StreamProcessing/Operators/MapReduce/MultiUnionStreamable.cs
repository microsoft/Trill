// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq;

namespace Microsoft.StreamProcessing
{
    internal sealed class MultiUnionStreamable<TKey, TSource> : Streamable<TKey, TSource>
    {
        public static int l2index = 0;
        public IStreamable<TKey, TSource>[] Sources;
        private readonly bool registerScheduler;
        private readonly Func<IStreamable<TKey, TSource>, IStreamable<TKey, TSource>, bool, IStreamable<TKey, TSource>> constructor;

        public MultiUnionStreamable(IStreamable<TKey, TSource>[] sources, bool register = true, bool guaranteedDisjoint = false)
            : base(sources.Skip(1).Select(o => o.Properties).Aggregate(sources[0].Properties, (r, p) => r.Union(p)))
        {
            Invariant.IsNotNull(sources, "sources");
            Invariant.IsPositive(sources.Length, "sources.Length");
            Invariant.IsNotNull(sources[0], "sources[0]");

            this.Sources = sources;
            this.registerScheduler = register;

            if (guaranteedDisjoint)
            {
                this.constructor = (l, r, b) => new DisjointUnionStreamable<TKey, TSource>(l, r, b);
            }
            else
            {
                this.constructor = (l, r, b) => new UnionStreamable<TKey, TSource>(l, r, b);
            }
        }

        public override IDisposable Subscribe(IStreamObserver<TKey, TSource> observer)
        {
            if (this.Sources.Length == 1)
            {
                return this.Sources[0].Subscribe(observer);
            }

            var obs = observer;
            if (this.registerScheduler)
                obs = Config.StreamScheduler.RegisterStreamObserver(observer);

            // use a binary tree of binary-union operators
            var currentSources = new IStreamable<TKey, TSource>[this.Sources.Length];
            for (int j = 0; j < this.Sources.Length; j++)
                currentSources[j] = this.Sources[j];

            int currentLength = this.Sources.Length;
            bool level1 = true;

            while (currentLength > 1)
            {
                int i = 0, j = 0;
                while (j < currentLength - 1)
                {
                    currentSources[i++] = this.constructor(currentSources[j], currentSources[j + 1], level1);
                    j += 2;
                }
                level1 = false;

                // since we advance by 2, j < length when length is odd.
                if (j < currentLength)
                {
                    // move the last odd source and adjust length.
                    currentSources[i++] = currentSources[j];
                }

                currentLength = i;
            }

            if (!(obs is IDisposable d))
                return currentSources[0].Subscribe(obs);
            else
                return Utility.CreateDisposable(currentSources[0].Subscribe(obs), d);
        }
    }
}