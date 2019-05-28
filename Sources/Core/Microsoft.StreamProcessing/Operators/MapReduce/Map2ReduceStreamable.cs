// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    internal sealed class Map2ReduceStreamable<TMapKey, TMapInputLeft1, TMapInputRight1, TMapInputLeft2, TMapInputRight2, TReduceKey, TReduceInput1, TReduceInput2, TBind, TOutput> : Streamable<TMapKey, TOutput>
    {
        private readonly IStreamable<TMapKey, TMapInputLeft1> sourceLeft1;
        private readonly IStreamable<TMapKey, TMapInputRight1> sourceRight1;
        private readonly IStreamable<TMapKey, TMapInputLeft2> sourceLeft2;
        private readonly IStreamable<TMapKey, TMapInputRight2> sourceRight2;

        private readonly Func<IStreamable<TMapKey, TMapInputLeft1>, IStreamable<TMapKey, TMapInputRight1>, IStreamable<TMapKey, TReduceInput1>> mapper1;
        private readonly Func<IStreamable<TMapKey, TMapInputLeft2>, IStreamable<TMapKey, TMapInputRight2>, IStreamable<TMapKey, TReduceInput2>> mapper2;

        private readonly Expression<Func<TReduceInput1, TReduceKey>> keySelector1;
        private readonly Expression<Func<TReduceInput2, TReduceKey>> keySelector2;

        private readonly Func<IStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput1>, IStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput2>, IStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TBind>> reducer;

        private readonly Expression<Func<TReduceKey, TBind, TOutput>> resultSelector;

        private readonly bool leftAsymmetric1;
        private readonly bool leftAsymmetric2;

        private readonly OperationalHint reduceOptions;

        private readonly bool reduceInMap;
        private readonly IComparerExpression<TMapInputLeft1> sprayComparer1 = null;

        internal Map2ReduceStreamable(
            IStreamable<TMapKey, TMapInputLeft1> sourceLeft1,
            IStreamable<TMapKey, TMapInputRight1> sourceRight1,
            Func<IStreamable<TMapKey, TMapInputLeft1>, IStreamable<TMapKey, TMapInputRight1>, IStreamable<TMapKey, TReduceInput1>> mapper1,
            Expression<Func<TReduceInput1, TReduceKey>> keySelector1,

            IStreamable<TMapKey, TMapInputLeft2> sourceLeft2,
            IStreamable<TMapKey, TMapInputRight2> sourceRight2,
            Func<IStreamable<TMapKey, TMapInputLeft2>, IStreamable<TMapKey, TMapInputRight2>, IStreamable<TMapKey, TReduceInput2>> mapper2,
            Expression<Func<TReduceInput2, TReduceKey>> keySelector2,

            Func<IStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput1>, IStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput2>, IStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TBind>> reducer,

            Expression<Func<TReduceKey, TBind, TOutput>> resultSelector,

            bool leftAsymmetric1,
            bool leftAsymmetric2,

            OperationalHint reduceOptions)
            : base(
                sourceLeft1.Properties.Map2Reduce(
                    sourceRight1?.Properties,
                    sourceLeft2?.Properties,
                    sourceRight2?.Properties,
                    mapper1,
                    mapper2,
                    reducer,
                    keySelector1,
                    keySelector2,
                    resultSelector,
                    reduceOptions))
        {
            Contract.Requires(sourceLeft1 != null);
            Contract.Requires(sourceLeft2 != null);
            Contract.Requires(reducer != null);

            this.sourceLeft1 = sourceLeft1;
            this.sourceLeft2 = sourceLeft2;
            this.sourceRight1 = sourceRight1;
            this.sourceRight2 = sourceRight2;
            this.mapper1 = mapper1;
            this.mapper2 = mapper2;
            this.keySelector1 = keySelector1;
            this.keySelector2 = keySelector2;
            this.reducer = reducer;
            this.resultSelector = resultSelector;
            this.leftAsymmetric1 = leftAsymmetric1;
            this.leftAsymmetric2 = leftAsymmetric2;
            this.reduceOptions = reduceOptions;

            if (this.sourceLeft1.Properties.CanSpray(this.sourceLeft2.Properties, this.keySelector1, this.keySelector2) &&
                this.sourceLeft1.Properties.Derive(a => this.mapper1(a, null)).CanSpray(
                    this.sourceLeft2.Properties.Derive(a => this.mapper2(a, null)),
                    this.keySelector1, this.keySelector2))
            {
                this.reduceInMap = true;
                this.sprayComparer1 = this.sourceLeft1.Properties.GetSprayComparerExpression(this.keySelector1);
            }
        }

        public override IDisposable Subscribe(IStreamObserver<TMapKey, TOutput> observer)
        {
            // asymmetric mapper implies that we have to have a 2-input mapper
            Contract.Assert((!this.leftAsymmetric1) || (this.sourceRight1 != null));
            Contract.Assert((!this.leftAsymmetric2) || (this.sourceRight2 != null));

            var mapArity = Config.MapArity;
            var reduceArity = Config.ReduceArity;

            // process mapper #1
            Streamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput1>[] shuffleL2Results1;

            if (this.sourceRight1 != null) // two-input mapper
            {
                // DEAD
                shuffleL2Results1 = new MultiUnionStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput1>[reduceArity];

                // [1] spray batches into L1 physical cores
                var importLeft1 = new SprayGroupImportStreamable<TMapKey, TMapInputLeft1>(this.sourceLeft1, mapArity, this.leftAsymmetric1);
                var importRight1 = new SprayGroupImportStreamable<TMapKey, TMapInputRight1>(this.sourceRight1, mapArity);

                // [2] perform the spray lambda on each L1 core
                var sprayResults1 = new BinaryMulticastStreamable<TMapKey, TMapInputLeft1, TMapInputRight1, TReduceInput1>[mapArity];
                for (int i = 0; i < mapArity; i++)
                    sprayResults1[i] = new BinaryMulticastStreamable<TMapKey, TMapInputLeft1, TMapInputRight1, TReduceInput1>(importLeft1, importRight1, this.mapper1);

                // [3] apply shuffle on the result of each spray
                Streamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput1>[] shuffleL1Results1 = new ShuffleNestedStreamable<TMapKey, TReduceInput1, TReduceKey>[mapArity];
                for (int i = 0; i < mapArity; i++)
                    shuffleL1Results1[i] = new ShuffleNestedStreamable<TMapKey, TReduceInput1, TReduceKey>(sprayResults1[i], this.keySelector1, reduceArity, i);

                // [4] Union the shuffled data by group key
                MultiUnionStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput1>.l2index = 0;
                for (int i = 0; i < reduceArity; i++)
                    shuffleL2Results1[i] = new MultiUnionStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput1>(shuffleL1Results1);

            }
            else // single-input mapper
            {
                // [1] spray batches into L1 physical cores
                var importLeft = new SprayGroupImportStreamable<TMapKey, TMapInputLeft1>
                    (this.sourceLeft1, mapArity, false, this.sprayComparer1);

                // [2] perform the spray lambda on each L1 core
                var sprayResults1 = new MulticastStreamable<TMapKey, TMapInputLeft1, TReduceInput1>[mapArity];
                for (int i = 0; i < mapArity; i++)
                    sprayResults1[i] = new MulticastStreamable<TMapKey, TMapInputLeft1, TReduceInput1>(importLeft, a => this.mapper1(a, null));

                if (this.reduceInMap || (this.reduceOptions == OperationalHint.Asymmetric))
                {
                    shuffleL2Results1 = new Streamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput1>[mapArity];
                    for (int i = 0; i < mapArity; i++)
                        shuffleL2Results1[i] = new GroupNestedStreamable<TMapKey, TReduceInput1, TReduceKey>(sprayResults1[i], this.keySelector1);
                }
                else
                {
                    shuffleL2Results1 = new MultiUnionStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput1>[reduceArity];

                    // [3] apply shuffle on the result of each spray
                    Streamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput1>[] shuffleL1Results1 = new ShuffleNestedStreamable<TMapKey, TReduceInput1, TReduceKey>[mapArity];
                    for (int i = 0; i < mapArity; i++)
                        shuffleL1Results1[i] = new ShuffleNestedStreamable<TMapKey, TReduceInput1, TReduceKey>(sprayResults1[i], this.keySelector1, reduceArity, i);

                    // [4] Union the shuffled data by group key
                    MultiUnionStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput1>.l2index = 0;
                    for (int i = 0; i < reduceArity; i++)
                        shuffleL2Results1[i] = new MultiUnionStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput1>(shuffleL1Results1);
                }
            }

            // process mapper #2
            Streamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput2>[] shuffleL2Results2;
            if (this.sourceRight2 != null) // two-input mapper
            {
                // DEAD
                shuffleL2Results2 = new MultiUnionStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput2>[reduceArity];

                // [1] spray batches into L1 physical cores
                var importLeft2 = new SprayGroupImportStreamable<TMapKey, TMapInputLeft2>(this.sourceLeft2, mapArity, this.leftAsymmetric2);
                var importRight2 = new SprayGroupImportStreamable<TMapKey, TMapInputRight2>(this.sourceRight2, mapArity);

                // [2] perform the spray lambda on each L1 core
                var sprayResults2 = new BinaryMulticastStreamable<TMapKey, TMapInputLeft2, TMapInputRight2, TReduceInput2>[mapArity];
                for (int i = 0; i < mapArity; i++)
                    sprayResults2[i] = new BinaryMulticastStreamable<TMapKey, TMapInputLeft2, TMapInputRight2, TReduceInput2>(importLeft2, importRight2, this.mapper2);

                // [3] apply shuffle on the result of each spray
                Streamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput2>[] shuffleL1Results2 = new ShuffleNestedStreamable<TMapKey, TReduceInput2, TReduceKey>[mapArity];
                for (int i = 0; i < mapArity; i++)
                    shuffleL1Results2[i] = new ShuffleNestedStreamable<TMapKey, TReduceInput2, TReduceKey>(sprayResults2[i], this.keySelector2, reduceArity, i);

                // [4] Union the shuffled data by group key
                MultiUnionStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput2>.l2index = 0;
                for (int i = 0; i < reduceArity; i++)
                    shuffleL2Results2[i] = new MultiUnionStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput2>(shuffleL1Results2);
            }
            else // single-input mapper
            {
                // [1] spray batches into L1 physical cores
                var importLeft = new SprayGroupImportStreamable<TMapKey, TMapInputLeft2>(this.sourceLeft2, mapArity, this.reduceOptions == OperationalHint.Asymmetric);

                // [2] perform the spray lambda on each L1 core
                var sprayResults2 = new MulticastStreamable<TMapKey, TMapInputLeft2, TReduceInput2>[mapArity];
                for (int i = 0; i < mapArity; i++)
                    sprayResults2[i] = new MulticastStreamable<TMapKey, TMapInputLeft2, TReduceInput2>(importLeft, a => this.mapper2(a, null));

                if (this.reduceInMap || (this.reduceOptions == OperationalHint.Asymmetric))
                {
                    shuffleL2Results2 = new Streamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput2>[mapArity];
                    for (int i = 0; i < mapArity; i++)
                        shuffleL2Results2[i] = new GroupNestedStreamable<TMapKey, TReduceInput2, TReduceKey>(sprayResults2[i], this.keySelector2);
                }
                else
                {
                    shuffleL2Results2 = new MultiUnionStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput2>[reduceArity];

                    // [3] apply shuffle on the result of each spray
                    var shuffleL1Results2 = new ShuffleNestedStreamable<TMapKey, TReduceInput2, TReduceKey>[mapArity];
                    for (int i = 0; i < mapArity; i++)
                        shuffleL1Results2[i] = new ShuffleNestedStreamable<TMapKey, TReduceInput2, TReduceKey>(sprayResults2[i], this.keySelector2, reduceArity, i);

                    // [4] Union the shuffled data by group key
                    MultiUnionStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput2>.l2index = 0;
                    for (int i = 0; i < reduceArity; i++)
                        shuffleL2Results2[i] = new MultiUnionStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput2>(shuffleL1Results2);
                }
            }

            // process 2-input reducer
            // [5] perform the apply lambda on each L2 core
            var innerResults
                = new BinaryMulticastStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput1, TReduceInput2, TBind>[shuffleL2Results1.Length];
            var ungroupInnerResults
                = new UngroupStreamable<TMapKey, TReduceKey, TBind, TOutput>[shuffleL2Results1.Length];

            for (int i = 0; i < shuffleL2Results1.Length; i++)
            {
                innerResults[i] = new BinaryMulticastStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput1, TReduceInput2, TBind>(shuffleL2Results1[i], shuffleL2Results2[i], this.reducer);
                ungroupInnerResults[i] = new UngroupStreamable<TMapKey, TReduceKey, TBind, TOutput>(this.sourceLeft1.Properties.KeyEqualityComparer, innerResults[i], this.resultSelector);
            }

            // [6] final single merging union
            var union = new MultiUnionStreamable<TMapKey, TOutput>(ungroupInnerResults, false);

            return union.Subscribe(observer);
        }
    }

    internal sealed class Map2ReduceStreamable<TMapInputLeft1, TMapInputRight1, TMapInputLeft2, TMapInputRight2, TReduceKey, TReduceInput1, TReduceInput2, TBind, TOutput> : Streamable<Empty, TOutput>
    {
        private readonly IStreamable<Empty, TMapInputLeft1> sourceLeft1;
        private readonly IStreamable<Empty, TMapInputRight1> sourceRight1;
        private readonly IStreamable<Empty, TMapInputLeft2> sourceLeft2;
        private readonly IStreamable<Empty, TMapInputRight2> sourceRight2;

        private readonly Func<IStreamable<Empty, TMapInputLeft1>, IStreamable<Empty, TMapInputRight1>, IStreamable<Empty, TReduceInput1>> mapper1;
        private readonly Func<IStreamable<Empty, TMapInputLeft2>, IStreamable<Empty, TMapInputRight2>, IStreamable<Empty, TReduceInput2>> mapper2;

        private readonly Expression<Func<TReduceInput1, TReduceKey>> keySelector1;
        private readonly Expression<Func<TReduceInput2, TReduceKey>> keySelector2;

        private readonly Func<IStreamable<TReduceKey, TReduceInput1>, IStreamable<TReduceKey, TReduceInput2>, IStreamable<TReduceKey, TBind>> reducer;

        private readonly Expression<Func<TReduceKey, TBind, TOutput>> resultSelector;

        private readonly bool leftAsymmetric1;
        private readonly bool leftAsymmetric2;

        private readonly OperationalHint reduceOptions;

        private readonly bool reduceInMap;
        private readonly IComparerExpression<TMapInputLeft1> sprayComparer1 = null;

        internal Map2ReduceStreamable(
            IStreamable<Empty, TMapInputLeft1> sourceLeft1,
            IStreamable<Empty, TMapInputRight1> sourceRight1,
            Func<IStreamable<Empty, TMapInputLeft1>, IStreamable<Empty, TMapInputRight1>, IStreamable<Empty, TReduceInput1>> mapper1,
            Expression<Func<TReduceInput1, TReduceKey>> keySelector1,

            IStreamable<Empty, TMapInputLeft2> sourceLeft2,
            IStreamable<Empty, TMapInputRight2> sourceRight2,
            Func<IStreamable<Empty, TMapInputLeft2>, IStreamable<Empty, TMapInputRight2>, IStreamable<Empty, TReduceInput2>> mapper2,
            Expression<Func<TReduceInput2, TReduceKey>> keySelector2,

            Func<IStreamable<TReduceKey, TReduceInput1>, IStreamable<TReduceKey, TReduceInput2>, IStreamable<TReduceKey, TBind>> reducer,

            Expression<Func<TReduceKey, TBind, TOutput>> resultSelector,

            bool leftAsymmetric1,
            bool leftAsymmetric2,

            OperationalHint reduceOptions)
            : base(
                sourceLeft1.Properties.Map2Reduce(
                    sourceRight1?.Properties,
                    sourceLeft2?.Properties,
                    sourceRight2?.Properties,
                    mapper1,
                    mapper2,
                    reducer,
                    keySelector1,
                    keySelector2,
                    resultSelector,
                    reduceOptions))
        {
            Contract.Requires(sourceLeft1 != null);
            Contract.Requires(sourceLeft2 != null);
            Contract.Requires(reducer != null);

            this.sourceLeft1 = sourceLeft1;
            this.sourceLeft2 = sourceLeft2;
            this.sourceRight1 = sourceRight1;
            this.sourceRight2 = sourceRight2;
            this.mapper1 = mapper1;
            this.mapper2 = mapper2;
            this.keySelector1 = keySelector1;
            this.keySelector2 = keySelector2;
            this.reducer = reducer;
            this.resultSelector = resultSelector;
            this.leftAsymmetric1 = leftAsymmetric1;
            this.leftAsymmetric2 = leftAsymmetric2;
            this.reduceOptions = reduceOptions;

            if (this.sourceLeft1.Properties.CanSpray(this.sourceLeft2.Properties, this.keySelector1, this.keySelector2) &&
                this.sourceLeft1.Properties.Derive(a => this.mapper1(a, null)).CanSpray(
                    this.sourceLeft2.Properties.Derive(a => this.mapper2(a, null)),
                    this.keySelector1, this.keySelector2))
            {
                this.reduceInMap = true;
                this.sprayComparer1 = this.sourceLeft1.Properties.GetSprayComparerExpression(this.keySelector1);
            }
        }

        public override IDisposable Subscribe(IStreamObserver<Empty, TOutput> observer)
        {
            // asymmetric mapper implies that we have to have a 2-input mapper
            Contract.Assert((!this.leftAsymmetric1) || (this.sourceRight1 != null));
            Contract.Assert((!this.leftAsymmetric2) || (this.sourceRight2 != null));

            var mapArity = Config.MapArity;
            var reduceArity = Config.ReduceArity;

            // process mapper #1
            Streamable<TReduceKey, TReduceInput1>[] shuffleL2Results1;

            if (this.sourceRight1 != null) // two-input mapper
            {
                // DEAD
                shuffleL2Results1 = new MultiUnionStreamable<TReduceKey, TReduceInput1>[reduceArity];

                // [1] spray batches into L1 physical cores
                var importLeft1 = new SprayGroupImportStreamable<Empty, TMapInputLeft1>(this.sourceLeft1, mapArity, this.leftAsymmetric1);
                var importRight1 = new SprayGroupImportStreamable<Empty, TMapInputRight1>(this.sourceRight1, mapArity);

                // [2] perform the spray lambda on each L1 core
                var sprayResults1 = new BinaryMulticastStreamable<Empty, TMapInputLeft1, TMapInputRight1, TReduceInput1>[mapArity];
                for (int i = 0; i < mapArity; i++)
                    sprayResults1[i] = new BinaryMulticastStreamable<Empty, TMapInputLeft1, TMapInputRight1, TReduceInput1>(importLeft1, importRight1, this.mapper1);

                // [3] apply shuffle on the result of each spray
                Streamable<TReduceKey, TReduceInput1>[] shuffleL1Results1 = new ShuffleStreamable<Empty, TReduceInput1, TReduceKey>[mapArity];
                for (int i = 0; i < mapArity; i++)
                    shuffleL1Results1[i] = new ShuffleStreamable<Empty, TReduceInput1, TReduceKey>(sprayResults1[i], this.keySelector1, reduceArity, i);

                // [4] Union the shuffled data by group key
                MultiUnionStreamable<TReduceKey, TReduceInput1>.l2index = 0;
                for (int i = 0; i < reduceArity; i++)
                    shuffleL2Results1[i] = new MultiUnionStreamable<TReduceKey, TReduceInput1>(shuffleL1Results1);

            }
            else // single-input mapper
            {
                // [1] spray batches into L1 physical cores
                var importLeft = new SprayGroupImportStreamable<Empty, TMapInputLeft1>
                    (this.sourceLeft1, mapArity, false, this.sprayComparer1);

                // [2] perform the spray lambda on each L1 core
                var sprayResults1 = new MulticastStreamable<Empty, TMapInputLeft1, TReduceInput1>[mapArity];
                for (int i = 0; i < mapArity; i++)
                    sprayResults1[i] = new MulticastStreamable<Empty, TMapInputLeft1, TReduceInput1>(importLeft, a => this.mapper1(a, null));

                if (this.reduceInMap || (this.reduceOptions == OperationalHint.Asymmetric))
                {
                    shuffleL2Results1 = new Streamable<TReduceKey, TReduceInput1>[mapArity];
                    for (int i = 0; i < mapArity; i++)
                        shuffleL2Results1[i] = new GroupStreamable<Empty, TReduceInput1, TReduceKey>(sprayResults1[i], this.keySelector1);
                }
                else
                {
                    shuffleL2Results1 = new MultiUnionStreamable<TReduceKey, TReduceInput1>[reduceArity];

                    // [3] apply shuffle on the result of each spray
                    Streamable<TReduceKey, TReduceInput1>[] shuffleL1Results1 = new ShuffleStreamable<Empty, TReduceInput1, TReduceKey>[mapArity];
                    for (int i = 0; i < mapArity; i++)
                        shuffleL1Results1[i] = new ShuffleStreamable<Empty, TReduceInput1, TReduceKey>(sprayResults1[i], this.keySelector1, reduceArity, i);

                    // [4] Union the shuffled data by group key
                    MultiUnionStreamable<TReduceKey, TReduceInput1>.l2index = 0;
                    for (int i = 0; i < reduceArity; i++)
                        shuffleL2Results1[i] = new MultiUnionStreamable<TReduceKey, TReduceInput1>(shuffleL1Results1);
                }
            }

            // process mapper #2
            Streamable<TReduceKey, TReduceInput2>[] shuffleL2Results2;
            if (this.sourceRight2 != null) // two-input mapper
            {
                // DEAD
                shuffleL2Results2 = new MultiUnionStreamable<TReduceKey, TReduceInput2>[reduceArity];

                // [1] spray batches into L1 physical cores
                var importLeft2 = new SprayGroupImportStreamable<Empty, TMapInputLeft2>(this.sourceLeft2, mapArity, this.leftAsymmetric2);
                var importRight2 = new SprayGroupImportStreamable<Empty, TMapInputRight2>(this.sourceRight2, mapArity);

                // [2] perform the spray lambda on each L1 core
                var sprayResults2 = new BinaryMulticastStreamable<Empty, TMapInputLeft2, TMapInputRight2, TReduceInput2>[mapArity];
                for (int i = 0; i < mapArity; i++)
                    sprayResults2[i] = new BinaryMulticastStreamable<Empty, TMapInputLeft2, TMapInputRight2, TReduceInput2>(importLeft2, importRight2, this.mapper2);

                // [3] apply shuffle on the result of each spray
                Streamable<TReduceKey, TReduceInput2>[] shuffleL1Results2 = new ShuffleStreamable<Empty, TReduceInput2, TReduceKey>[mapArity];
                for (int i = 0; i < mapArity; i++)
                    shuffleL1Results2[i] = new ShuffleStreamable<Empty, TReduceInput2, TReduceKey>(sprayResults2[i], this.keySelector2, reduceArity, i);

                // [4] Union the shuffled data by group key
                MultiUnionStreamable<TReduceKey, TReduceInput2>.l2index = 0;
                for (int i = 0; i < reduceArity; i++)
                    shuffleL2Results2[i] = new MultiUnionStreamable<TReduceKey, TReduceInput2>(shuffleL1Results2);
            }
            else // single-input mapper
            {
                // [1] spray batches into L1 physical cores
                var importLeft = new SprayGroupImportStreamable<Empty, TMapInputLeft2>(this.sourceLeft2, mapArity, this.reduceOptions == OperationalHint.Asymmetric);

                // [2] perform the spray lambda on each L1 core
                var sprayResults2 = new MulticastStreamable<Empty, TMapInputLeft2, TReduceInput2>[mapArity];
                for (int i = 0; i < mapArity; i++)
                    sprayResults2[i] = new MulticastStreamable<Empty, TMapInputLeft2, TReduceInput2>(importLeft, a => this.mapper2(a, null));

                if (this.reduceInMap || (this.reduceOptions == OperationalHint.Asymmetric))
                {
                    shuffleL2Results2 = new Streamable<TReduceKey, TReduceInput2>[mapArity];
                    for (int i = 0; i < mapArity; i++)
                        shuffleL2Results2[i] = new GroupStreamable<Empty, TReduceInput2, TReduceKey>(sprayResults2[i], this.keySelector2);
                }
                else
                {
                    shuffleL2Results2 = new MultiUnionStreamable<TReduceKey, TReduceInput2>[reduceArity];

                    // [3] apply shuffle on the result of each spray
                    var shuffleL1Results2 = new ShuffleStreamable<Empty, TReduceInput2, TReduceKey>[mapArity];
                    for (int i = 0; i < mapArity; i++)
                        shuffleL1Results2[i] = new ShuffleStreamable<Empty, TReduceInput2, TReduceKey>(sprayResults2[i], this.keySelector2, reduceArity, i);

                    // [4] Union the shuffled data by group key
                    MultiUnionStreamable<TReduceKey, TReduceInput2>.l2index = 0;
                    for (int i = 0; i < reduceArity; i++)
                        shuffleL2Results2[i] = new MultiUnionStreamable<TReduceKey, TReduceInput2>(shuffleL1Results2);
                }
            }

            // process 2-input reducer
            // [5] perform the apply lambda on each L2 core
            var innerResults
                = new BinaryMulticastStreamable<TReduceKey, TReduceInput1, TReduceInput2, TBind>[shuffleL2Results1.Length];
            var ungroupInnerResults
                = new UngroupStreamable<TReduceKey, TBind, TOutput>[shuffleL2Results1.Length];

            for (int i = 0; i < shuffleL2Results1.Length; i++)
            {
                innerResults[i] = new BinaryMulticastStreamable<TReduceKey, TReduceInput1, TReduceInput2, TBind>(shuffleL2Results1[i], shuffleL2Results2[i], this.reducer);
                ungroupInnerResults[i] = new UngroupStreamable<TReduceKey, TBind, TOutput>(innerResults[i], this.resultSelector);
            }

            // [6] final single merging union
            var union = new MultiUnionStreamable<Empty, TOutput>(ungroupInnerResults, false);

            return union.Subscribe(observer);
        }
    }

}