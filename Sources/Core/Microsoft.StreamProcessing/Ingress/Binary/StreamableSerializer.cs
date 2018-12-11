// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Serializer
{
    internal sealed class SerializedProperties
    {
        public SerializedProperties() { }

        public static SerializedProperties FromStreamProperties<TKey, TPayload>(StreamProperties<TKey, TPayload> props)
            => new SerializedProperties
            {
                IsColumnar = props.IsColumnar,
                IsConstantDuration = props.IsConstantDuration,
                ConstantDurationLength = props.ConstantDurationLength,
                IsConstantHop = props.IsConstantHop,
                ConstantHopLength = props.ConstantHopLength,
                ConstantHopOffset = props.ConstantHopOffset,
                IsIntervalFree = props.IsIntervalFree,
                IsSyncTimeSimultaneityFree = props.IsSyncTimeSimultaneityFree,
                IsEventOverlappingFree = props.IsEventOverlappingFree
            };

        public StreamProperties<TKey, TPayload> ToStreamProperties<TKey, TPayload>()
            => new StreamProperties<TKey, TPayload>(this.IsColumnar, this.IsConstantDuration, this.ConstantDurationLength, this.IsConstantHop, this.ConstantHopLength, this.ConstantHopOffset, this.IsIntervalFree, this.IsSyncTimeSimultaneityFree,
                false, this.IsEventOverlappingFree,
                EqualityComparerExpression<TKey>.Default,
                EqualityComparerExpression<TPayload>.Default,
                null, null,
                new Dictionary<Expression, object>(),
                new Dictionary<Expression, Guid?>(),
                null);

        public bool IsColumnar;
        public bool IsConstantDuration;
        public long? ConstantDurationLength;
        public bool IsConstantHop;
        public long? ConstantHopLength;
        public long? ConstantHopOffset;
        public bool IsIntervalFree;
        public bool IsSyncTimeSimultaneityFree;
        public bool IsEventOverlappingFree;
    }

    /// <summary>
    /// Interface wrapping a scheduler
    /// </summary>
    public interface IIngressScheduler
    {
        /// <summary>
        /// Schedule an action to be handled according to the scheduler
        /// </summary>
        /// <param name="action">The action to be scheduled</param>
        /// <returns>An object that, when disposed, shuts down the action scheduling</returns>
        IDisposable Schedule(Action action);
    }
}
