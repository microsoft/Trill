// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing
{
    internal sealed class DiagnosticObservable<TPayload> : IObservable<OutOfOrderStreamEvent<TPayload>>, IObserver<OutOfOrderStreamEvent<TPayload>>, IDisposable
    {
        private List<IObserver<OutOfOrderStreamEvent<TPayload>>> observers = new List<IObserver<OutOfOrderStreamEvent<TPayload>>>();

        public IDisposable Subscribe(IObserver<OutOfOrderStreamEvent<TPayload>> observer)
        {
            this.observers.Add(observer);
            return this;
        }

        public void OnCompleted()
        {
            foreach (var observer in this.observers)
            {
                observer.OnCompleted();
            }
        }

        public void OnError(Exception error)
        {
            foreach (var observer in this.observers)
            {
                observer.OnError(error);
            }
        }

        public void OnNext(OutOfOrderStreamEvent<TPayload> value)
        {
            foreach (var observer in this.observers)
            {
                observer.OnNext(value);
            }
        }

        public void Dispose() { }
    }

    /// <summary>
    /// A structure for representing when a tuple had to be adjusted or dropped from a source of data ingress.
    /// </summary>
    /// <typeparam name="TPayload">The type of the stream event payload.</typeparam>
    [DataContract]
    public struct OutOfOrderStreamEvent<TPayload>
    {
        /// <summary>
        /// The event that was adjusted or dropped in its original version.
        /// </summary>
        [DataMember]
        public StreamEvent<TPayload> Event;

        /// <summary>
        /// The amount of time that the event was adjusted, or null if the event was dropped.
        /// </summary>
        [DataMember]
        public long? TimeAdjustment;
    }

    /// <summary>
    /// Static class containing generator methods for OutOfOrderStreamEvent objects.
    /// </summary>
    public static class OutOfOrderStreamEvent
    {
        /// <summary>
        /// Create an OutOfOrderStreamEvent object from its constituent parts.
        /// </summary>
        /// <typeparam name="TPayload">The type of the underlying event.</typeparam>
        /// <param name="e">The event that has arrived in the stream out of order.</param>
        /// <param name="delta">The amount of time that the event was out of order.</param>
        /// <returns></returns>
        public static OutOfOrderStreamEvent<TPayload> Create<TPayload>(StreamEvent<TPayload> e, long? delta)
            => new OutOfOrderStreamEvent<TPayload> { Event = e, TimeAdjustment = delta };
    }

    internal sealed class PartitionedDiagnosticObservable<TKey, TPayload> : IObservable<OutOfOrderPartitionedStreamEvent<TKey, TPayload>>, IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>>, IDisposable
    {
        private List<IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>>> observers = new List<IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>>>();

        public IDisposable Subscribe(IObserver<OutOfOrderPartitionedStreamEvent<TKey, TPayload>> observer)
        {
            this.observers.Add(observer);
            return this;
        }

        public void OnCompleted()
        {
            foreach (var observer in this.observers)
            {
                observer.OnCompleted();
            }
        }

        public void OnError(Exception error)
        {
            foreach (var observer in this.observers)
            {
                observer.OnError(error);
            }
        }

        public void OnNext(OutOfOrderPartitionedStreamEvent<TKey, TPayload> value)
        {
            foreach (var observer in this.observers)
            {
                observer.OnNext(value);
            }
        }

        public void Dispose() { }
    }

    /// <summary>
    /// A structure for representing when a tuple had to be adjusted or dropped from a source of data ingress.
    /// </summary>
    /// <typeparam name="TKey">The partition key type.</typeparam>
    /// <typeparam name="TPayload">The type of the stream event payload.</typeparam>
    [DataContract]
    public struct OutOfOrderPartitionedStreamEvent<TKey, TPayload>
    {
        /// <summary>
        /// The event that was adjusted or dropped in its original version.
        /// </summary>
        [DataMember]
        public PartitionedStreamEvent<TKey, TPayload> Event;

        /// <summary>
        /// The amount of time that the event was adjusted, or null if the event was dropped.
        /// </summary>
        [DataMember]
        public long? TimeAdjustment;
    }

    /// <summary>
    /// Static class containing generator methods for OutOfOrderPartitionedStreamEvent objects.
    /// </summary>
    public static class OutOfOrderPartitionedStreamEvent
    {
        /// <summary>
        /// Create an OutOfOrderPartitionedStreamEvent object from its constituent parts.
        /// </summary>
        /// <typeparam name="TKey">The partition key type.</typeparam>
        /// <typeparam name="TPayload">The type of the underlying event.</typeparam>
        /// <param name="e">The event that has arrived in the stream out of order.</param>
        /// <param name="delta">The amount of time that the event was out of order.</param>
        /// <returns></returns>
        public static OutOfOrderPartitionedStreamEvent<TKey, TPayload> Create<TKey, TPayload>(PartitionedStreamEvent<TKey, TPayload> e, long? delta)
            => new OutOfOrderPartitionedStreamEvent<TKey, TPayload> { Event = e, TimeAdjustment = delta };
    }

}