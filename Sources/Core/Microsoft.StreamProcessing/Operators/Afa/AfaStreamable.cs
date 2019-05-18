// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    internal sealed class AfaStreamable<TKey, TPayload, TRegister, TAccumulator> : UnaryStreamable<TKey, TPayload, TRegister>
    {
        private static readonly SafeConcurrentDictionary<Tuple<Type, string>> cachedPipes
                          = new SafeConcurrentDictionary<Tuple<Type, string>>();

        internal CompiledAfa<TPayload, TRegister, TAccumulator> afa;
        internal long MaxDuration;

        public AfaStreamable(IStreamable<TKey, TPayload> source, Afa<TPayload, TRegister, TAccumulator> afa, long maxDuration)
            : base(source, source.Properties.Afa<TKey, TPayload, TRegister>())
        {
            Contract.Requires(source != null);

            afa.Seal();
            this.afa = afa.Compile();
            this.MaxDuration = maxDuration;

            Initialize();
        }

        internal override IStreamObserver<TKey, TPayload> CreatePipe(IStreamObserver<TKey, TRegister> observer)
        {
            if ((this.afa.eventListStateMap != null) && (this.afa.multiEventStateMap == null))
            {
                if (this.Source.Properties.IsColumnar)
                    return GetGroupedAFAEventListPipe(observer);
                else if (typeof(TKey) == typeof(Empty))
                {
                    var downcast = this as AfaStreamable<Empty, TPayload, TRegister, TAccumulator>;
                    var emptyObserver = observer as IStreamObserver<Empty, TRegister>;
                    return new CompiledUngroupedAfaPipe_EventList<TPayload, TRegister, TAccumulator>(downcast, emptyObserver, this.afa, this.MaxDuration) as IStreamObserver<TKey, TPayload>;
                }
                else
                {
                    var partitionType = typeof(TKey).GetPartitionType();
                    if (partitionType == null)
                        return new CompiledGroupedAfaPipe_EventList<TKey, TPayload, TRegister, TAccumulator>(this, observer, this.afa, this.MaxDuration);
                    else
                    {
                        var outputType = typeof(CompiledPartitionedAfaPipe_EventList<,,,,>).MakeGenericType(
                            typeof(TKey),
                            typeof(TPayload),
                            typeof(TRegister),
                            typeof(TAccumulator),
                            partitionType);
                        return (IStreamObserver<TKey, TPayload>)Activator.CreateInstance(outputType, this, observer, this.afa, this.MaxDuration);
                    }
                }
            }

            if ((this.afa.eventListStateMap == null) && (this.afa.multiEventStateMap != null) && (this.afa.singleEventStateMap == null))
            {
                if (this.Source.Properties.IsColumnar)
                    return GetGroupedAFAMultiEventPipe(observer);
                else
                {
                    var partitionType = typeof(TKey).GetPartitionType();
                    if (partitionType == null)
                        return new CompiledGroupedAfaPipe_MultiEvent<TKey, TPayload, TRegister, TAccumulator>(this, observer, this.afa, this.MaxDuration);
                    else
                    {
                        var outputType = typeof(CompiledPartitionedAfaPipe_MultiEvent<,,,,>).MakeGenericType(
                            typeof(TKey),
                            typeof(TPayload),
                            typeof(TRegister),
                            typeof(TAccumulator),
                            partitionType);
                        return (IStreamObserver<TKey, TPayload>)Activator.CreateInstance(outputType, this, observer, this.afa, this.MaxDuration);
                    }
                }
            }

            if ((this.afa.eventListStateMap == null) && (this.afa.multiEventStateMap == null) && (this.afa.singleEventStateMap != null))
            {
                if (typeof(TKey) == typeof(Empty))
                {
                    var downcast = this as AfaStreamable<Empty, TPayload, TRegister, TAccumulator>;
                    var emptyObserver = observer as IStreamObserver<Empty, TRegister>;
                    if (this.afa.uncompiledAfa.IsDeterministic)
                    {
                        return this.Source.Properties.IsColumnar
                            ? GetUngroupedDAfaPipe(emptyObserver) as IStreamObserver<TKey, TPayload>
                            : new CompiledUngroupedDAfaPipe<TPayload, TRegister, TAccumulator>(downcast, observer as IStreamObserver<Empty, TRegister>, this.afa, this.MaxDuration) as IStreamObserver<TKey, TPayload>;
                    }
                    else
                    {
                        return this.Source.Properties.IsColumnar
                            ? GetUngroupedAFAPipe(emptyObserver) as IStreamObserver<TKey, TPayload>
                            : new CompiledUngroupedAfaPipe<TPayload, TRegister, TAccumulator>(downcast, observer as IStreamObserver<Empty, TRegister>, this.afa, this.MaxDuration) as IStreamObserver<TKey, TPayload>;
                    }
                }
                else
                {
                    if (this.Source.Properties.IsColumnar)
                        return GetGroupedAFAPipe(observer);
                    else
                    {
                        var partitionType = typeof(TKey).GetPartitionType();
                        if (partitionType == null)
                            return new CompiledGroupedAfaPipe_SingleEvent<TKey, TPayload, TRegister, TAccumulator>(this, observer, this.afa, this.MaxDuration);
                        else
                        {
                            var outputType = typeof(CompiledPartitionedAfaPipe_SingleEvent<,,,,>).MakeGenericType(
                                typeof(TKey),
                                typeof(TPayload),
                                typeof(TRegister),
                                typeof(TAccumulator),
                                partitionType);
                            return (IStreamObserver<TKey, TPayload>)Activator.CreateInstance(outputType, this, observer, this.afa, this.MaxDuration);
                        }
                    }
                }
            }

            if (this.Source.Properties.IsColumnar)
                return GetAFAMultiEventListPipe(observer);
            else if (typeof(TKey) == typeof(Empty))
            {
                var downcast = this as AfaStreamable<Empty, TPayload, TRegister, TAccumulator>;
                var emptyObserver = observer as IStreamObserver<Empty, TRegister>;
                return new CompiledUngroupedAfaPipe_MultiEventList<TPayload, TRegister, TAccumulator>(downcast, emptyObserver, this.afa, this.MaxDuration) as IStreamObserver<TKey, TPayload>;
            }
            else
            {
                var partitionType = typeof(TKey).GetPartitionType();
                if (partitionType == null)
                    return new CompiledGroupedAfaPipe_MultiEventList<TKey, TPayload, TRegister, TAccumulator>(this, observer, this.afa, this.MaxDuration);
                else
                {
                    var outputType = typeof(CompiledPartitionedAfaPipe_MultiEventList<,,,,>).MakeGenericType(
                        typeof(TKey),
                        typeof(TPayload),
                        typeof(TRegister),
                        typeof(TAccumulator),
                        partitionType);
                    return (IStreamObserver<TKey, TPayload>)Activator.CreateInstance(outputType, this, observer, this.afa, this.MaxDuration);
                }
            }
        }

        protected override bool CanGenerateColumnar()
        {
            if ((this.afa.eventListStateMap != null) && (this.afa.multiEventStateMap == null))
                return Config.CodegenOptions.CodeGenAfa && CanGenerateGroupedAFAEventListPipe();
            else if ((this.afa.eventListStateMap == null) && (this.afa.multiEventStateMap != null) && (this.afa.singleEventStateMap == null))
                return Config.CodegenOptions.CodeGenAfa && CanGenerateGroupedAFAMultiEventPipe();
            else if ((this.afa.eventListStateMap == null) && (this.afa.multiEventStateMap == null) && (this.afa.singleEventStateMap != null))
            {
                if (typeof(TKey) == typeof(Empty))
                {
                    return this.afa.uncompiledAfa.IsDeterministic
                        ? Config.CodegenOptions.CodeGenAfa && CanGenerateUngroupedDAfaPipe()
                        : Config.CodegenOptions.CodeGenAfa && CanGenerateUngroupedAFAPipe();
                }
                else
                    return Config.CodegenOptions.CodeGenAfa && CanGenerateGroupedAFAPipe();
            }
            else return Config.CodegenOptions.CodeGenAfa && CanGenerateAFAMultiEventListPipe();
        }

        private bool CanGenerateUngroupedAFAPipe()
        {
            if (!typeof(TPayload).CanRepresentAsColumnar()) return false;
            if (!typeof(TRegister).CanRepresentAsColumnar()) return false;

            var lookupKey = CacheKey.Create((object)this.afa);
            var downcast = this as AfaStreamable<Empty, TPayload, TRegister, TAccumulator>;
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => UngroupedAfaTemplate.GenerateAFA(downcast));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private UnaryPipe<Empty, TPayload, TRegister> GetUngroupedAFAPipe(IStreamObserver<Empty, TRegister> observer)
        {
            var lookupKey = CacheKey.Create((object)this.afa);
            var downcast = this as AfaStreamable<Empty, TPayload, TRegister, TAccumulator>;
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => UngroupedAfaTemplate.GenerateAFA(downcast));

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, this.afa, this.MaxDuration);
            var returnValue = (UnaryPipe<Empty, TPayload, TRegister>)instance;
            return returnValue;
        }

        private bool CanGenerateGroupedAFAPipe()
        {
            if (!typeof(TPayload).CanRepresentAsColumnar()) return false;
            if (!typeof(TRegister).CanRepresentAsColumnar()) return false;
            if (typeof(TKey).GetPartitionType() != null) return false;

            var lookupKey = CacheKey.Create((object)this.afa);

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => GroupedAfaTemplate.GenerateAFA(this));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private UnaryPipe<TKey, TPayload, TRegister> GetGroupedAFAPipe(IStreamObserver<TKey, TRegister> observer)
        {
            var lookupKey = CacheKey.Create((object)this.afa);
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => GroupedAfaTemplate.GenerateAFA(this));

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, this.afa, this.MaxDuration);
            var returnValue = (UnaryPipe<TKey, TPayload, TRegister>)instance;
            return returnValue;
        }

        private bool CanGenerateUngroupedDAfaPipe()
        {
            if (!typeof(TPayload).CanRepresentAsColumnar()) return false;
            if (!typeof(TRegister).CanRepresentAsColumnar()) return false;

            var lookupKey = CacheKey.Create((object)this.afa);

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => UngroupedDAfaTemplate.GenerateAFA(this));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private UnaryPipe<Empty, TPayload, TRegister> GetUngroupedDAfaPipe(IStreamObserver<Empty, TRegister> observer)
        {
            var lookupKey = CacheKey.Create((object)this.afa);
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => UngroupedDAfaTemplate.GenerateAFA(this));

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, this.afa, this.MaxDuration);
            var returnValue = (UnaryPipe<Empty, TPayload, TRegister>)instance;
            return returnValue;
        }

        private bool CanGenerateGroupedAFAEventListPipe()
        {
            if (!typeof(TPayload).CanRepresentAsColumnar()) return false;
            if (!typeof(TRegister).CanRepresentAsColumnar()) return false;
            if (typeof(TKey).GetPartitionType() != null) return false;

            var lookupKey = CacheKey.Create((object)this.afa);

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => GroupedAfaEventListTemplate.GenerateAFA(this));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private UnaryPipe<TKey, TPayload, TRegister> GetGroupedAFAEventListPipe(IStreamObserver<TKey, TRegister> observer)
        {
            var lookupKey = CacheKey.Create((object)this.afa);
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => GroupedAfaEventListTemplate.GenerateAFA(this));

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, this.afa, this.MaxDuration);
            var returnValue = (UnaryPipe<TKey, TPayload, TRegister>)instance;
            return returnValue;
        }

        private bool CanGenerateGroupedAFAMultiEventPipe()
        {
            if (!typeof(TPayload).CanRepresentAsColumnar()) return false;
            if (!typeof(TRegister).CanRepresentAsColumnar()) return false;
            if (typeof(TKey).GetPartitionType() != null) return false;

            var lookupKey = CacheKey.Create((object)this.afa);

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => GroupedAfaMultiEventTemplate.GenerateAFA(this));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private UnaryPipe<TKey, TPayload, TRegister> GetGroupedAFAMultiEventPipe(IStreamObserver<TKey, TRegister> observer)
        {
            var lookupKey = CacheKey.Create((object)this.afa);
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => GroupedAfaMultiEventTemplate.GenerateAFA(this));

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, this.afa, this.MaxDuration);
            var returnValue = (UnaryPipe<TKey, TPayload, TRegister>)instance;
            return returnValue;
        }

        private bool CanGenerateAFAMultiEventListPipe()
        {
            if (!typeof(TPayload).CanRepresentAsColumnar()) return false;
            if (!typeof(TRegister).CanRepresentAsColumnar()) return false;
            if (typeof(TKey).GetPartitionType() != null) return false;

            var lookupKey = CacheKey.Create((object)this.afa);

            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => AfaMultiEventListTemplate.GenerateAFA(this));

            this.errorMessages = generatedPipeType.Item2;
            return generatedPipeType.Item1 != null;
        }

        private UnaryPipe<TKey, TPayload, TRegister> GetAFAMultiEventListPipe(IStreamObserver<TKey, TRegister> observer)
        {
            var lookupKey = CacheKey.Create((object)this.afa);
            var generatedPipeType = cachedPipes.GetOrAdd(lookupKey, key => AfaMultiEventListTemplate.GenerateAFA(this));

            var instance = Activator.CreateInstance(generatedPipeType.Item1, this, observer, this.afa, this.MaxDuration);
            var returnValue = (UnaryPipe<TKey, TPayload, TRegister>)instance;
            return returnValue;
        }
    }
}
