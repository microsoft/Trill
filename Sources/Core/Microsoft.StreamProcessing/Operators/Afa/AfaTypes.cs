// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing
{
    internal enum ArcType { SingleElement, ListElement, MultiElement, Epsilon };

    /// <summary>
    /// Currently for internal use only - do not use directly
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    /// <typeparam name="TRegister"></typeparam>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct SingleEventArcInfo<TPayload, TRegister>
    {
        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int toState;
        internal ArcType arcType;

        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public Func<long, TPayload, TRegister, bool> Fence;

        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public Func<long, TPayload, TRegister, TRegister> Transfer;
    }

    /// <summary>
    /// Currently for internal use only - do not use directly
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    /// <typeparam name="TRegister"></typeparam>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct EventListArcInfo<TPayload, TRegister>
    {
        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int toState;
        internal ArcType arcType;

        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public Func<long, List<TPayload>, TRegister, bool> Fence;

        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public Func<long, List<TPayload>, TRegister, TRegister> Transfer;
    }

    /// <summary>
    /// Currently for internal use only - do not use directly
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    /// <typeparam name="TRegister"></typeparam>
    /// <typeparam name="TAccumulator"></typeparam>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public sealed class MultiEventArcInfo<TPayload, TRegister, TAccumulator>
    {
        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool fromStartState;

        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int toState;
        internal ArcType arcType;

        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public Func<long, TRegister, TAccumulator> Initialize;

        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public Func<long, TPayload, TRegister, TAccumulator, TAccumulator> Accumulate;

        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public Func<long, TPayload, TAccumulator, bool> SkipToEnd;

        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public Func<long, TAccumulator, TRegister, bool> Fence;

        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public Func<long, TAccumulator, TRegister, TRegister> Transfer;

        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public Action<TAccumulator> Dispose;
    }

    /// <summary>
    /// Currently for internal use only - do not use directly
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TRegister"></typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct GroupedActiveState<TKey, TRegister>
    {
        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public TKey key;

        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int state;

        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public TRegister register;

        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public long PatternStartTimestamp;
    }

    /// <summary>
    /// Currently for internal use only - do not use directly
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TPayload"></typeparam>
    /// <typeparam name="TRegister"></typeparam>
    /// <typeparam name="TAccumulator"></typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct GroupedActiveStateAccumulator<TKey, TPayload, TRegister, TAccumulator>
    {
        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public TKey key;

        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int fromState;

        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public int toState;

        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public TRegister register;

        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public TAccumulator accumulator;

        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public MultiEventArcInfo<TPayload, TRegister, TAccumulator> arcinfo;

        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public long PatternStartTimestamp;
    }

    /// <summary>
    /// Currently for internal use only - do not use directly
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TPayload"></typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct OutputEvent<TKey, TPayload>
    {
        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public long other;

        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public TKey key;

        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public TPayload payload;
    }

    /// <summary>
    /// Currently for internal use only - do not use directly
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TPayload"></typeparam>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public struct SavedEventList<TKey, TPayload>
    {
        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public TKey key;

        /// <summary>
        /// Currently for internal use only - do not use directly
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public List<TPayload> payloads;
    }
}
