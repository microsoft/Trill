// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    public static partial class Streamable
    {
        /// <summary>
        /// Caches the complete results computed by the streamable upon an immediate call to subscribe. This
        /// call will block until the underlying query has fully executed and the result cached.
        /// </summary>
        /// <typeparam name="TKey">Type of key for stream</typeparam>
        /// <typeparam name="TPayload">Type of payload for stream</typeparam>
        /// <param name="stream">Instance of the stream to be cached</param>
        /// <param name="limit">Limit on number of events to store in the cached stream</param>
        /// <param name="inferProperties">Specifies whether each stream event in the incoming stream should be checked to
        /// infer the properties of no intervals and constant duration
        /// </param>
        /// <returns>A streamable cache instance</returns>
        /// <param name="coalesceEndEdges">Whether or not we coalesce end edges with their starts into interval events</param>
        /// <returns></returns>
        public static StreamCache<TKey, TPayload> Cache<TKey, TPayload>(this IStreamable<TKey, TPayload> stream, ulong limit = 0, bool inferProperties = false, bool coalesceEndEdges = false)
        {
            var elements = new List<QueuedMessage<StreamMessage<TKey, TPayload>>>();
            if (coalesceEndEdges) stream = stream.ToEndEdgeFreeStream();

            var observable = new QueuedMessageObservable<TKey, TPayload>(stream);
            var limitSpecified = limit > 0;
            var memoryPool = MemoryManager.GetMemoryPool<TKey, TPayload>(stream.Properties.IsColumnar);
            long lastSync = -1;

            try
            {
                observable.SynchronousForEach(t =>
                {
                    if (t.Kind == MessageKind.DataBatch)
                    {
                        var mt = t.Message.MinTimestamp;
                        if (mt < lastSync) throw new StreamProcessingOutOfOrderException("Out-of-order event received during Cache() call");

                        lastSync = mt;

                        // REVIEW: What if there are a *lot* of data batches after we want to stop?
                        // But we need to wait to see all of the remaining punctuations?
                        if (limitSpecified)
                        {
                            if (limit > 0)
                            {
                                ulong count = (ulong)t.Message.Count;
                                if (count <= limit)
                                {
                                    // whole batch fits
                                    limit -= count;
                                    elements.Add(t);
                                }
                                else
                                {
                                    memoryPool.Get(out StreamMessage<TKey, TPayload> newMessage);
                                    newMessage.Allocate();

                                    for (ulong i = 0; i < limit; i++)
                                    {
                                        newMessage.Add(t.Message.vsync.col[i], t.Message.vother.col[i], t.Message.key.col[i], t.Message[(int)i]);
                                    }
                                    elements.Add(new QueuedMessage<StreamMessage<TKey, TPayload>> { Kind = MessageKind.DataBatch, Message = newMessage });
                                    limit = 0;
                                    t.Message.Free();
                                }
                            }
                            else t.Message.Free();
                        }
                        else elements.Add(t);
                    }
                    else elements.Add(t);
                });
            }
            catch (Exception)
            {
                for (int i = 0; i < elements.Count; i++)
                {
                    elements[i].Message?.Free();
                }
                throw;
            }

            var p = stream.Properties;
            if (inferProperties)
            {
                InferProperties(elements, out bool noIntervals, out bool constantDuration, out long duration);
                if (noIntervals)
                {
                    ; // Intentional nop until internal variable is available to set property
                }
                if (constantDuration)
                    p = p.ToConstantDuration(true, duration);
            }
            return new StreamCache<TKey, TPayload>(p, elements);
        }

        private static void InferProperties<TKey, TPayload>(
            List<QueuedMessage<StreamMessage<TKey, TPayload>>> messages,
            out bool noIntervals,
            out bool constantDuration,
            out long duration)
        {
            noIntervals = true;
            constantDuration = true;
            duration = 0;
            var first = true;
            for (int i = 0; i < messages.Count; i++)
            {
                var t = messages[i];
                if (!noIntervals && !constantDuration) return; // no point continuing

                // Need to find first duration.
                if (first && t.Message.Count > 0)
                {
                    // BUG? Can we depend on the first stream event *not* being an end edge?
                    duration = t.Message.vother.col[0] == StreamEvent.InfinitySyncTime
                        ? StreamEvent.InfinitySyncTime
                        : t.Message.vother.col[0] - t.Message.vsync.col[0];
                    first = false;
                }
                for (int r = 0; r < t.Message.Count; r++)
                {
                    var start = t.Message.vsync.col[r];
                    var end = t.Message.vother.col[r];
                    var isStartEdge = end == StreamEvent.InfinitySyncTime;
                    var isInterval = !isStartEdge && start < end;
                    var isEndEdge = !isStartEdge && end < start;
                    constantDuration &= !isEndEdge; // end edge => give up constant duration
                    noIntervals &= !isInterval;
                    if (constantDuration)
                    {
                        if (isInterval)
                            constantDuration = (end - start) == duration;
                        else if (isStartEdge)
                            constantDuration = duration == StreamEvent.InfinitySyncTime;
                    }
                }
            }
            return;
        }
    }

    internal sealed class QueuedMessageObservable<TKey, TPayload> : IObservable<QueuedMessage<StreamMessage<TKey, TPayload>>>
    {
        private readonly IStreamable<TKey, TPayload> streamable;

        public QueuedMessageObservable(IStreamable<TKey, TPayload> stream) => this.streamable = stream;

        public IDisposable Subscribe(IObserver<QueuedMessage<StreamMessage<TKey, TPayload>>> observer)
            => this.streamable.Subscribe(new QueuedMessageObserver<TKey, TPayload>(observer));
    }

    internal sealed class QueuedMessageObserver<TKey, TPayload> : IStreamObserver<TKey, TPayload>, IDisposable
    {
        private readonly IObserver<QueuedMessage<StreamMessage<TKey, TPayload>>> observer;

        public QueuedMessageObserver(IObserver<QueuedMessage<StreamMessage<TKey, TPayload>>> observer)
        {
            this.observer = observer;
            this.ClassId = Guid.NewGuid();
        }

        public Guid ClassId { get; }

        public int CurrentlyBufferedOutputCount => 0;

        public int CurrentlyBufferedInputCount => 0;

        public void Checkpoint(Stream stream) { }
        public void Dispose() => throw new NotImplementedException();
        public void OnCompleted() => this.observer.OnCompleted();
        public void OnError(Exception error) => throw error;
        public void OnFlush() => this.observer.OnNext(new QueuedMessage<StreamMessage<TKey, TPayload>> { Kind = MessageKind.Flush });
        public void OnNext(StreamMessage<TKey, TPayload> value) => this.observer.OnNext(new QueuedMessage<StreamMessage<TKey, TPayload>> { Kind = MessageKind.DataBatch, Message = value });
        public void ProduceQueryPlan(PlanNode previous) { }
        public void Reset() { }
        public void Restore(Stream stream) { }
    }

    /// <summary>
    /// Stores a complete stream in a highly efficient, stream friendly manner for subsequent iteration over
    /// the data.
    /// </summary>
    public sealed class StreamCache<TKey, TPayload> : Streamable<TKey, TPayload>, IDisposable
    {
        internal List<QueuedMessage<StreamMessage<TKey, TPayload>>> messages;
        internal MemoryPool<TKey, TPayload> pool;

        internal StreamCache(StreamProperties<TKey, TPayload> properties, List<QueuedMessage<StreamMessage<TKey, TPayload>>> messages)
            : base(properties)
        {
            this.messages = messages;
            this.pool = MemoryManager.GetMemoryPool<TKey, TPayload>(properties.IsColumnar);
        }

        internal StreamCache() : base(StreamProperties<TKey, TPayload>.Default)
            => this.pool = MemoryManager.GetMemoryPool<TKey, TPayload>(false);

        internal void Update(StreamProperties<TKey, TPayload> properties, List<QueuedMessage<StreamMessage<TKey, TPayload>>> messages)
        {
            this.properties = properties;
            this.pool = MemoryManager.GetMemoryPool<TKey, TPayload>(properties.IsColumnar);
            this.messages = messages;
        }

        /// <summary>
        /// Returns the number of elements of type T contained in this cache.
        /// </summary>
        public long ComputeSize()
        {
            long size = 0;
            foreach (var e in this.messages)
            {
                if (e.Message.Count > 0)
                {
                    var col_bv = e.Message.bitvector.col;
                    size += e.Message.Count;
                    for (int i = 0; i < e.Message.Count; i++)
                    {
                        if ((col_bv[i >> 6] & (1L << (i & 0x3f))) != 0) size--;
                    }
                }
            }
            return size;
        }

        /// <summary>
        /// Callback method for downstream operators to register subscription to the output of the current operator
        /// </summary>
        /// <param name="observer">Pointer to the next operator downstream</param>
        /// <returns>Disposable object to clean up resources after stream is taken down</returns>
        public override IDisposable Subscribe(IStreamObserver<TKey, TPayload> observer)
        {
            observer = Config.StreamScheduler.RegisterStreamObserver(observer);
            for (int i = 0; i < this.messages.Count; i++)
            {
                var m = this.messages[i];
                switch (m.Kind)
                {
                    case MessageKind.Flush:
                        observer.OnFlush();
                        break;
                    case MessageKind.DataBatch:
                        // Next two lines are idiom for incrementing the ref count of m
                        this.pool.Get(out StreamMessage<TKey, TPayload> batch);
                        batch.CloneFrom(m.Message);
                        observer.OnNext(batch);
                        break;
                }
            }
            observer.OnCompleted();
            return Utility.EmptyDisposable;
        }

        /// <summary>
        /// Frees up all of the elements in the cache.
        /// </summary>
        public void Dispose()
        {
            for (int i = 0; i < this.messages.Count; i++)
            {
                this.messages[i].Message?.Free();
            }
        }
    }
}