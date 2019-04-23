// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    internal sealed class MapReduceStreamable<TMapKey, TMapInputLeft, TMapInputRight, TReduceKey, TReduceInput, TBind, TOutput> : Streamable<TMapKey, TOutput>
    {
        private readonly IStreamable<TMapKey, TMapInputLeft> sourceLeft;
        private readonly IStreamable<TMapKey, TMapInputRight> sourceRight;
        private readonly Func<IStreamable<TMapKey, TMapInputLeft>, IStreamable<TMapKey, TMapInputRight>, IStreamable<TMapKey, TReduceInput>> mapper;
        private readonly Expression<Func<TReduceInput, TReduceKey>> keySelector;
        private readonly Func<IStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput>, IStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TBind>> reducer;
        private readonly Expression<Func<TReduceKey, TBind, TOutput>> resultSelector;
        private readonly bool leftAsymmetric;
        private bool isMulticore;

        internal MapReduceStreamable(
            IStreamable<TMapKey, TMapInputLeft> sourceLeft,
            IStreamable<TMapKey, TMapInputRight> sourceRight,
            Func<IStreamable<TMapKey, TMapInputLeft>, IStreamable<TMapKey, TMapInputRight>, IStreamable<TMapKey, TReduceInput>> mapper,
            Expression<Func<TReduceInput, TReduceKey>> keySelector,
            Func<IStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput>, IStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TBind>> reducer,
            Expression<Func<TReduceKey, TBind, TOutput>> resultSelector,
            bool leftAsymmetric = false)
            : base(
            sourceLeft.Properties.MapReduce
            (sourceRight?.Properties, mapper, keySelector, reducer, resultSelector))
        {
            Contract.Requires(sourceLeft != null);

            this.sourceLeft = sourceLeft;
            this.sourceRight = sourceRight;
            this.mapper = mapper;
            this.keySelector = keySelector;
            this.reducer = reducer;
            this.resultSelector = resultSelector;
            this.leftAsymmetric = leftAsymmetric;

            ProcessProperties();
        }

        internal MapReduceStreamable(
            IStreamable<TMapKey, TMapInputLeft> sourceLeft,
            IStreamable<TMapKey, TMapInputRight> sourceRight,
            Func<IStreamable<TMapKey, TMapInputLeft>, IStreamable<TMapKey, TMapInputRight>, IStreamable<TMapKey, TReduceInput>> mapper,
            Expression<Func<TReduceInput, TReduceKey>> keySelector,
            bool leftAsymmetric = false)
            : this(sourceLeft, sourceRight, mapper, keySelector, null, null, leftAsymmetric)
        { }

        private bool reduceInMap;
        private IComparerExpression<TMapInputLeft> sprayComparer = null;

        internal void ProcessProperties()
        {
            reduceInMap = sourceLeft.Properties.CanSpray(keySelector) && sourceLeft.Properties.Derive(a => mapper(a, null)).CanSpray(keySelector);
            if (reduceInMap)
            {
                sprayComparer = sourceLeft.Properties.GetSprayComparerExpression(keySelector);
            }
            isMulticore = sourceLeft.Properties.IsMulticore;
        }

        public override IDisposable Subscribe(IStreamObserver<TMapKey, TOutput> observer)
        {
            // asymmetric mapper implies that we have to have a 2-input mapper
            Contract.Assert((!leftAsymmetric) || (sourceRight != null));

            var mapArity = isMulticore ? 1 : Config.MapArity;
            var reduceArity = isMulticore ? 1 : Config.ReduceArity;

            if (keySelector != null)
            {
                if (sourceRight != null) // two-input mapper
                {
                    // [1] spray batches into L1 physical cores
                    var importLeft = new SprayGroupImportStreamable<TMapKey, TMapInputLeft>(sourceLeft, mapArity, leftAsymmetric);
                    var importRight = new SprayGroupImportStreamable<TMapKey, TMapInputRight>(sourceRight, mapArity);

                    // [2] perform the spray lambda on each L1 core
                    var sprayResults = new BinaryMulticastStreamable<TMapKey, TMapInputLeft, TMapInputRight, TReduceInput>[mapArity];
                    for (int i = 0; i < mapArity; i++)
                        sprayResults[i] = new BinaryMulticastStreamable<TMapKey, TMapInputLeft, TMapInputRight, TReduceInput>(importLeft, importRight, mapper);

                    // [3] apply shuffle on the result of each spray
                    Streamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput>[] shuffleL1Results = new ShuffleNestedStreamable<TMapKey, TReduceInput, TReduceKey>[mapArity];
                    for (int i = 0; i < mapArity; i++)
                        shuffleL1Results[i] = new ShuffleNestedStreamable<TMapKey, TReduceInput, TReduceKey>(sprayResults[i], keySelector, reduceArity, i);

                    // [4] Union the shuffled data by group key
                    MultiUnionStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput>.l2index = 0;
                    var shuffleL2Results = new MultiUnionStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput>[reduceArity];
                    for (int i = 0; i < reduceArity; i++)
                        shuffleL2Results[i] = new MultiUnionStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput>(shuffleL1Results);

                    // [5] perform the apply lambda on each L2 core
                    var innerResults = new MulticastStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput, TBind>[reduceArity];
                    var ungroupInnerResults = new UngroupStreamable<TMapKey, TReduceKey, TBind, TOutput>[reduceArity];

                    for (int i = 0; i < reduceArity; i++)
                    {
                        innerResults[i] = new MulticastStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput, TBind>(shuffleL2Results[i], reducer);
                        ungroupInnerResults[i] = new UngroupStreamable<TMapKey, TReduceKey, TBind, TOutput>(sourceLeft.Properties.KeyEqualityComparer, innerResults[i], resultSelector);
                    }
                    // [6] final single merging union
                    var union = new MultiUnionStreamable<TMapKey, TOutput>(ungroupInnerResults, false);

                    return union.Subscribe(observer);
                }
                else // single-input mapper
                {
                    // [1] spray batches into L1 physical cores
                    var importLeft = new SprayGroupImportStreamable<TMapKey, TMapInputLeft>(sourceLeft, mapArity, leftAsymmetric, sprayComparer);

                    // [2] perform the spray lambda on each L1 core
                    var sprayResults = new MulticastStreamable<TMapKey, TMapInputLeft, TReduceInput>[mapArity];
                    for (int i = 0; i < mapArity; i++)
                        sprayResults[i] = new MulticastStreamable<TMapKey, TMapInputLeft, TReduceInput>(importLeft, a => mapper(a, null));

                    Streamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput>[] mergeInputs;
                    if (reduceInMap) // apply reducer in map phase itself
                    {
                        // [3] apply shuffle on the result of each spray
                        mergeInputs = new Streamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput>[mapArity];
                        for (int i = 0; i < mapArity; i++)
                            mergeInputs[i] = new GroupNestedStreamable<TMapKey, TReduceInput, TReduceKey>(sprayResults[i], keySelector);
                    }
                    else
                    {
                        // [3] apply shuffle on the result of each spray
                        Streamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput>[] shuffleL1Results = new ShuffleNestedStreamable<TMapKey, TReduceInput, TReduceKey>[mapArity];
                        for (int i = 0; i < mapArity; i++)
                            shuffleL1Results[i] = new ShuffleNestedStreamable<TMapKey, TReduceInput, TReduceKey>(sprayResults[i], keySelector, reduceArity, i);

                        // [4] Union the shuffled data by group key
                        MultiUnionStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput>.l2index = 0;

                        mergeInputs = new Streamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput>[reduceArity];
                        mergeInputs = new MultiUnionStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput>[reduceArity];
                        for (int i = 0; i < reduceArity; i++)
                            mergeInputs[i] = new MultiUnionStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput>(shuffleL1Results);
                    }

                    // [5] perform the apply lambda on each L2 core
                    var innerResults
                        = new MulticastStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput, TBind>[mergeInputs.Length];
                    var ungroupInnerResults
                        = new UngroupStreamable<TMapKey, TReduceKey, TBind, TOutput>[mergeInputs.Length];

                    for (int i = 0; i < mergeInputs.Length; i++)
                    {
                        innerResults[i] = new MulticastStreamable<CompoundGroupKey<TMapKey, TReduceKey>, TReduceInput, TBind>(mergeInputs[i], reducer);
                        ungroupInnerResults[i] = new UngroupStreamable<TMapKey, TReduceKey, TBind, TOutput>(sourceLeft.Properties.KeyEqualityComparer, innerResults[i], resultSelector);
                    }

                    // [6] final single merging union
                    var union = new MultiUnionStreamable<TMapKey, TOutput>(ungroupInnerResults, false);

                    return union.Subscribe(observer);
                }
            }
            else
            {
                if (sourceRight != null) // two-input mapper
                {
                    // [1] spray batches into L1 physical cores
                    var importLeft = new SprayGroupImportStreamable<TMapKey, TMapInputLeft>(sourceLeft, mapArity, leftAsymmetric);
                    var importRight = new SprayGroupImportStreamable<TMapKey, TMapInputRight>(sourceRight, mapArity);

                    // [2] perform the spray lambda on each L1 core
                    var sprayResults = new BinaryMulticastStreamable<TMapKey, TMapInputLeft, TMapInputRight, TReduceInput>[mapArity];
                    for (int i = 0; i < mapArity; i++)
                        sprayResults[i] = new BinaryMulticastStreamable<TMapKey, TMapInputLeft, TMapInputRight, TReduceInput>(importLeft, importRight, mapper);

                    // [4] Union the shuffled data by group key
                    MultiUnionStreamable<TMapKey, TReduceInput>.l2index = 0;
                    var shuffleL2Result
                        = new MultiUnionStreamable<TMapKey, TReduceInput>(sprayResults, false) as MultiUnionStreamable<TMapKey, TOutput>;

                    return shuffleL2Result.Subscribe(observer);
                }
                else // single-input mapper
                {
                    // [1] spray batches into L1 physical cores
                    var importLeft = new SprayGroupImportStreamable<TMapKey, TMapInputLeft>(sourceLeft, mapArity);

                    // [2] perform the spray lambda on each L1 core
                    var sprayResults = new MulticastStreamable<TMapKey, TMapInputLeft, TReduceInput>[mapArity];
                    for (int i = 0; i < mapArity; i++)
                        sprayResults[i] = new MulticastStreamable<TMapKey, TMapInputLeft, TReduceInput>(importLeft, a => mapper(a, null));

                    // [4] Union the shuffled data by group key
                    MultiUnionStreamable<TMapKey, TReduceInput>.l2index = 0;
                    var shuffleL2Result
                        = new MultiUnionStreamable<TMapKey, TReduceInput>(sprayResults, false) as MultiUnionStreamable<TMapKey, TOutput>;

                    return shuffleL2Result.Subscribe(observer);
                }
            }
        }
    }

    internal sealed class MapReduceStreamable<TMapInputLeft, TMapInputRight, TReduceKey, TReduceInput, TBind, TOutput> : Streamable<Empty, TOutput>
    {
        private readonly IStreamable<Empty, TMapInputLeft> sourceLeft;
        private readonly IStreamable<Empty, TMapInputRight> sourceRight;
        private readonly Func<IStreamable<Empty, TMapInputLeft>, IStreamable<Empty, TMapInputRight>, IStreamable<Empty, TReduceInput>> mapper;
        private readonly Expression<Func<TReduceInput, TReduceKey>> keySelector;
        private readonly Func<IStreamable<TReduceKey, TReduceInput>, IStreamable<TReduceKey, TBind>> reducer;
        private readonly Expression<Func<TReduceKey, TBind, TOutput>> resultSelector;
        private readonly bool leftAsymmetric;
        private bool isMulticore;

        internal MapReduceStreamable(
            IStreamable<Empty, TMapInputLeft> sourceLeft,
            IStreamable<Empty, TMapInputRight> sourceRight,
            Func<IStreamable<Empty, TMapInputLeft>, IStreamable<Empty, TMapInputRight>, IStreamable<Empty, TReduceInput>> mapper,
            Expression<Func<TReduceInput, TReduceKey>> keySelector,
            Func<IStreamable<TReduceKey, TReduceInput>, IStreamable<TReduceKey, TBind>> reducer,
            Expression<Func<TReduceKey, TBind, TOutput>> resultSelector,
            bool leftAsymmetric = false)
            : base(
            sourceLeft.Properties.MapReduce
            (sourceRight?.Properties, mapper, keySelector, reducer, resultSelector))
        {
            Contract.Requires(sourceLeft != null);

            this.sourceLeft = sourceLeft;
            this.sourceRight = sourceRight;
            this.mapper = mapper;
            this.keySelector = keySelector;
            this.reducer = reducer;
            this.resultSelector = resultSelector;
            this.leftAsymmetric = leftAsymmetric;

            ProcessProperties();
        }

        internal MapReduceStreamable(
            IStreamable<Empty, TMapInputLeft> sourceLeft,
            IStreamable<Empty, TMapInputRight> sourceRight,
            Func<IStreamable<Empty, TMapInputLeft>, IStreamable<Empty, TMapInputRight>, IStreamable<Empty, TReduceInput>> mapper,
            Expression<Func<TReduceInput, TReduceKey>> keySelector,
            bool leftAsymmetric = false)
            : this(sourceLeft, sourceRight, mapper, keySelector, null, null, leftAsymmetric)
        { }

        private bool reduceInMap;
        private IComparerExpression<TMapInputLeft> sprayComparer = null;

        internal void ProcessProperties()
        {
            reduceInMap = sourceLeft.Properties.CanSpray(keySelector) && sourceLeft.Properties.Derive(a => mapper(a, null)).CanSpray(keySelector);
            if (reduceInMap)
            {
                sprayComparer = sourceLeft.Properties.GetSprayComparerExpression(keySelector);
            }
            isMulticore = sourceLeft.Properties.IsMulticore;
        }

        public override IDisposable Subscribe(IStreamObserver<Empty, TOutput> observer)
        {
            // asymmetric mapper implies that we have to have a 2-input mapper
            Contract.Assert((!leftAsymmetric) || (sourceRight != null));

            var mapArity = isMulticore ? 1 : Config.MapArity;
            var reduceArity = isMulticore ? 1 : Config.ReduceArity;

            if (keySelector != null)
            {
                if (sourceRight != null) // two-input mapper
                {
                    // [1] spray batches into L1 physical cores
                    var importLeft = new SprayGroupImportStreamable<Empty, TMapInputLeft>(sourceLeft, mapArity, leftAsymmetric);
                    var importRight = new SprayGroupImportStreamable<Empty, TMapInputRight>(sourceRight, mapArity);

                    // [2] perform the spray lambda on each L1 core
                    var sprayResults = new BinaryMulticastStreamable<Empty, TMapInputLeft, TMapInputRight, TReduceInput>[mapArity];
                    for (int i = 0; i < mapArity; i++)
                        sprayResults[i] = new BinaryMulticastStreamable<Empty, TMapInputLeft, TMapInputRight, TReduceInput>(importLeft, importRight, mapper);

                    // [3] apply shuffle on the result of each spray
                    Streamable<TReduceKey, TReduceInput>[] shuffleL1Results = new ShuffleStreamable<Empty, TReduceInput, TReduceKey>[mapArity];
                    for (int i = 0; i < mapArity; i++)
                        shuffleL1Results[i] = new ShuffleStreamable<Empty, TReduceInput, TReduceKey>(sprayResults[i], keySelector, reduceArity, i);

                    // [4] Union the shuffled data by group key
                    MultiUnionStreamable<TReduceKey, TReduceInput>.l2index = 0;
                    var shuffleL2Results = new MultiUnionStreamable<TReduceKey, TReduceInput>[reduceArity];
                    for (int i = 0; i < reduceArity; i++)
                        shuffleL2Results[i] = new MultiUnionStreamable<TReduceKey, TReduceInput>(shuffleL1Results);

                    // [5] perform the apply lambda on each L2 core
                    var innerResults = new MulticastStreamable<TReduceKey, TReduceInput, TBind>[reduceArity];
                    var ungroupInnerResults = new UngroupStreamable<TReduceKey, TBind, TOutput>[reduceArity];

                    for (int i = 0; i < reduceArity; i++)
                    {
                        innerResults[i] = new MulticastStreamable<TReduceKey, TReduceInput, TBind>(shuffleL2Results[i], reducer);
                        ungroupInnerResults[i] = new UngroupStreamable<TReduceKey, TBind, TOutput>(innerResults[i], resultSelector);
                    }
                    // [6] final single merging union
                    var union = new MultiUnionStreamable<Empty, TOutput>(ungroupInnerResults, false);

                    return union.Subscribe(observer);
                }
                else // single-input mapper
                {
                    // [1] spray batches into L1 physical cores
                    var importLeft = new SprayGroupImportStreamable<Empty, TMapInputLeft>(sourceLeft, mapArity, leftAsymmetric, sprayComparer);

                    // [2] perform the spray lambda on each L1 core
                    var sprayResults = new MulticastStreamable<Empty, TMapInputLeft, TReduceInput>[mapArity];
                    for (int i = 0; i < mapArity; i++)
                        sprayResults[i] = new MulticastStreamable<Empty, TMapInputLeft, TReduceInput>(importLeft, a => mapper(a, null));

                    Streamable<TReduceKey, TReduceInput>[] mergeInputs;
                    if (reduceInMap) // apply reducer in map phase itself
                    {
                        // [3] apply shuffle on the result of each spray
                        mergeInputs = new Streamable<TReduceKey, TReduceInput>[mapArity];
                        for (int i = 0; i < mapArity; i++)
                            mergeInputs[i] = new GroupStreamable<Empty, TReduceInput, TReduceKey>(sprayResults[i], keySelector);
                    }
                    else
                    {
                        // [3] apply shuffle on the result of each spray
                        Streamable<TReduceKey, TReduceInput>[] shuffleL1Results = new ShuffleStreamable<Empty, TReduceInput, TReduceKey>[mapArity];
                        for (int i = 0; i < mapArity; i++)
                            shuffleL1Results[i] = new ShuffleStreamable<Empty, TReduceInput, TReduceKey>(sprayResults[i], keySelector, reduceArity, i);

                        // [4] Union the shuffled data by group key
                        MultiUnionStreamable<TReduceKey, TReduceInput>.l2index = 0;

                        mergeInputs = new Streamable<TReduceKey, TReduceInput>[reduceArity];
                        mergeInputs = new MultiUnionStreamable<TReduceKey, TReduceInput>[reduceArity];
                        for (int i = 0; i < reduceArity; i++)
                            mergeInputs[i] = new MultiUnionStreamable<TReduceKey, TReduceInput>(shuffleL1Results);
                    }

                    // [5] perform the apply lambda on each L2 core
                    var innerResults
                        = new MulticastStreamable<TReduceKey, TReduceInput, TBind>[mergeInputs.Length];
                    var ungroupInnerResults
                        = new UngroupStreamable<TReduceKey, TBind, TOutput>[mergeInputs.Length];

                    for (int i = 0; i < mergeInputs.Length; i++)
                    {
                        innerResults[i] = new MulticastStreamable<TReduceKey, TReduceInput, TBind>(mergeInputs[i], reducer);
                        ungroupInnerResults[i] = new UngroupStreamable<TReduceKey, TBind, TOutput>(innerResults[i], resultSelector);
                    }

                    // [6] final single merging union
                    var union = new MultiUnionStreamable<Empty, TOutput>(ungroupInnerResults, false);

                    return union.Subscribe(observer);
                }
            }
            else
            {
                if (sourceRight != null) // two-input mapper
                {
                    // [1] spray batches into L1 physical cores
                    var importLeft = new SprayGroupImportStreamable<Empty, TMapInputLeft>(sourceLeft, mapArity, leftAsymmetric);
                    var importRight = new SprayGroupImportStreamable<Empty, TMapInputRight>(sourceRight, mapArity);

                    // [2] perform the spray lambda on each L1 core
                    var sprayResults = new BinaryMulticastStreamable<Empty, TMapInputLeft, TMapInputRight, TReduceInput>[mapArity];
                    for (int i = 0; i < mapArity; i++)
                        sprayResults[i] = new BinaryMulticastStreamable<Empty, TMapInputLeft, TMapInputRight, TReduceInput>(importLeft, importRight, mapper);

                    // [4] Union the shuffled data by group key
                    MultiUnionStreamable<Empty, TReduceInput>.l2index = 0;
                    var shuffleL2Result
                        = new MultiUnionStreamable<Empty, TReduceInput>(sprayResults, false) as MultiUnionStreamable<Empty, TOutput>;

                    return shuffleL2Result.Subscribe(observer);
                }
                else // single-input mapper
                {
                    // [1] spray batches into L1 physical cores
                    var importLeft = new SprayGroupImportStreamable<Empty, TMapInputLeft>(sourceLeft, mapArity);

                    // [2] perform the spray lambda on each L1 core
                    var sprayResults = new MulticastStreamable<Empty, TMapInputLeft, TReduceInput>[mapArity];
                    for (int i = 0; i < mapArity; i++)
                        sprayResults[i] = new MulticastStreamable<Empty, TMapInputLeft, TReduceInput>(importLeft, a => mapper(a, null));

                    // [4] Union the shuffled data by group key
                    MultiUnionStreamable<Empty, TReduceInput>.l2index = 0;
                    var shuffleL2Result
                        = new MultiUnionStreamable<Empty, TReduceInput>(sprayResults, false) as MultiUnionStreamable<Empty, TOutput>;

                    return shuffleL2Result.Subscribe(observer);
                }
            }
        }
    }

}