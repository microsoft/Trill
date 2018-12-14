// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;

namespace Microsoft.StreamProcessing
{

        [DataContract]
        internal sealed class StreamEventArrayEgressPipe<TPayload> : EgressBoundary<Empty, TPayload, ArraySegment<StreamEvent<TPayload>>>
        {
            private readonly Func<StreamEvent<TPayload>[]> generator;
            [DataMember]
            private StreamEvent<TPayload>[] array;
            [DataMember]
            private int arrayLength;
            [DataMember]
            private int populationCount = 0;

            [Obsolete("Used only by serialization. Do not call directly.")]
            public StreamEventArrayEgressPipe() { }

            public StreamEventArrayEgressPipe(
                Func<StreamEvent<TPayload>[]> generator,
                IObserver<ArraySegment<StreamEvent<TPayload>>> observer,
                QueryContainer container)
                : base(observer, container)
            {
                this.generator = generator;
                this.array = this.generator();
                this.arrayLength = this.array.Length;
            }

            public override void OnNext(StreamMessage<Empty, TPayload> batch)
            {
                var col_bv = batch.bitvector.col;
                var col_vsync = batch.vsync.col;
                var col_vother = batch.vother.col;

                for (int i = 0; i < batch.Count; i++)
                {
                    if ((col_bv[i >> 6] & (1L << (i & 0x3f))) != 0 && col_vother[i] >= 0)
                        continue;
                    else if (col_vother[i] == StreamEvent.PunctuationOtherTime)
                        this.array[this.populationCount++] = StreamEvent.CreatePunctuation<TPayload>(col_vsync[i]);
                    else this.array[this.populationCount++] = new StreamEvent<TPayload>(col_vsync[i], col_vother[i], batch[i]);

                    if (this.populationCount == this.arrayLength)
                    {
                        this.observer.OnNext(new ArraySegment<StreamEvent<TPayload>>(this.array, 0, this.arrayLength));
                        this.populationCount = 0;
                        this.array = this.generator();
                        this.arrayLength = this.array.Length;
                    }
                }
                batch.Free();
            }

            public override void OnCompleted()
            {
                OnFlush();
                base.OnCompleted();
            }

            public override void OnFlush()
            {
                if (this.populationCount > 0)
                {
                    this.observer.OnNext(new ArraySegment<StreamEvent<TPayload>>(this.array, 0, this.populationCount));
                    this.populationCount = 0;
                    this.array = this.generator();
                    this.arrayLength = this.array.Length;
                }
            }

            public override int CurrentlyBufferedOutputCount => this.populationCount;

            public override int CurrentlyBufferedInputCount => 0;
        }

        [DataContract]
        internal sealed class StartEdgeArrayEgressPipe<TPayload, TResult> : EgressBoundary<Empty, TPayload, ArraySegment<TResult>>
        {
            private readonly Func<TResult[]> generator;
            [DataMember]
            private TResult[] array;
            [DataMember]
            private int arrayLength;
            [DataMember]
            private int populationCount = 0;
            private readonly Func<long, TPayload, TResult> constructor;

            [Obsolete("Used only by serialization. Do not call directly.")]
            public StartEdgeArrayEgressPipe() { }

            public StartEdgeArrayEgressPipe(
                Func<TResult[]> generator,
                Expression<Func<long, TPayload, TResult>> constructor,
                IObserver<ArraySegment<TResult>> observer,
                QueryContainer container)
                : base(observer, container)
            {
                this.generator = generator;
                this.array = this.generator();
                this.arrayLength = this.array.Length;
                this.constructor = constructor.Compile();
            }

            public override void OnNext(StreamMessage<Empty, TPayload> batch)
            {
                var col_bv = batch.bitvector.col;
                var col_vsync = batch.vsync.col;
                var col_vother = batch.vother.col;

                for (int i = 0; i < batch.Count; i++)
                {
                    if ((col_bv[i >> 6] & (1L << (i & 0x3f))) != 0)
                        continue;
                    else if (col_vother[i] == StreamEvent.InfinitySyncTime)
                        this.array[this.populationCount++] = this.constructor(col_vsync[i], batch[i]);
                    else throw new StreamProcessingException("The query has encountered either an end edge or an interval, while the egress point expects only start edges.");

                    if (this.populationCount == this.arrayLength)
                    {
                        this.observer.OnNext(new ArraySegment<TResult>(this.array, 0, this.arrayLength));
                        this.populationCount = 0;
                        this.array = this.generator();
                        this.arrayLength = this.array.Length;
                    }
                }
                batch.Free();
            }

            public override void OnCompleted()
            {
                OnFlush();
                base.OnCompleted();
            }

            public override void OnFlush()
            {
                if (this.populationCount > 0)
                {
                    this.observer.OnNext(new ArraySegment<TResult>(this.array, 0, this.populationCount));
                    this.populationCount = 0;
                    this.array = this.generator();
                    this.arrayLength = this.array.Length;
                }
            }

            public override int CurrentlyBufferedOutputCount => this.populationCount;

            public override int CurrentlyBufferedInputCount => 0;
        }

        [DataContract]
        internal sealed class IntervalArrayEgressPipe<TPayload, TResult> : EgressBoundary<Empty, TPayload, ArraySegment<TResult>>
        {
            private readonly Func<TResult[]> generator;
            [DataMember]
            private TResult[] array;
            [DataMember]
            private int arrayLength;
            [DataMember]
            private int populationCount = 0;
            private readonly Func<long, long, TPayload, TResult> constructor;

            [Obsolete("Used only by serialization. Do not call directly.")]
            public IntervalArrayEgressPipe() { }

            public IntervalArrayEgressPipe(
                Func<TResult[]> generator,
                Expression<Func<long, long, TPayload, TResult>> constructor,
                IObserver<ArraySegment<TResult>> observer,
                QueryContainer container)
                : base(observer, container)
            {
                this.generator = generator;
                this.array = this.generator();
                this.arrayLength = this.array.Length;
                this.constructor = constructor.Compile();
            }

            public override void OnNext(StreamMessage<Empty, TPayload> batch)
            {
                var col_bv = batch.bitvector.col;
                var col_vsync = batch.vsync.col;
                var col_vother = batch.vother.col;

                for (int i = 0; i < batch.Count; i++)
                {
                    if ((col_bv[i >> 6] & (1L << (i & 0x3f))) != 0)
                        continue;
                    else this.array[this.populationCount++] = this.constructor(col_vsync[i], col_vother[i], batch[i]);

                    if (this.populationCount == this.arrayLength)
                    {
                        this.observer.OnNext(new ArraySegment<TResult>(this.array, 0, this.arrayLength));
                        this.populationCount = 0;
                        this.array = this.generator();
                        this.arrayLength = this.array.Length;
                    }
                }
                batch.Free();
            }

            public override void OnCompleted()
            {
                OnFlush();
                base.OnCompleted();
            }

            public override void OnFlush()
            {
                if (this.populationCount > 0)
                {
                    this.observer.OnNext(new ArraySegment<TResult>(this.array, 0, this.populationCount));
                    this.populationCount = 0;
                    this.array = this.generator();
                    this.arrayLength = this.array.Length;
                }
            }

            public override int CurrentlyBufferedOutputCount => this.populationCount;

            public override int CurrentlyBufferedInputCount => 0;
        }

        [DataContract]
        internal sealed class PartitionedStreamEventArrayEgressPipe<TKey, TPayload> : EgressBoundary<PartitionKey<TKey>, TPayload, ArraySegment<PartitionedStreamEvent<TKey, TPayload>>>
        {
            private readonly Func<PartitionedStreamEvent<TKey, TPayload>[]> generator;
            [DataMember]
            private PartitionedStreamEvent<TKey, TPayload>[] array;
            [DataMember]
            private int arrayLength;
            [DataMember]
            private int populationCount = 0;

            [Obsolete("Used only by serialization. Do not call directly.")]
            public PartitionedStreamEventArrayEgressPipe() { }

            public PartitionedStreamEventArrayEgressPipe(
                Func<PartitionedStreamEvent<TKey, TPayload>[]> generator,
                IObserver<ArraySegment<PartitionedStreamEvent<TKey, TPayload>>> observer,
                QueryContainer container)
                : base(observer, container)
            {
                this.generator = generator;
                this.array = this.generator();
                this.arrayLength = this.array.Length;
            }

            public override void OnNext(StreamMessage<PartitionKey<TKey>, TPayload> batch)
            {
                var colkey = batch.key.col;
                var col_bv = batch.bitvector.col;
                var col_vsync = batch.vsync.col;
                var col_vother = batch.vother.col;

                for (int i = 0; i < batch.Count; i++)
                {
                    if ((col_bv[i >> 6] & (1L << (i & 0x3f))) != 0 && col_vother[i] >= 0)
                        continue;
                    else if (col_vother[i] == StreamEvent.PunctuationOtherTime)
                        this.array[this.populationCount++] = PartitionedStreamEvent.CreatePunctuation<TKey, TPayload>(colkey[i].Key, col_vsync[i]);
                    else if (col_vother[i] == PartitionedStreamEvent.LowWatermarkOtherTime)
                        this.array[this.populationCount++] = PartitionedStreamEvent.CreateLowWatermark<TKey, TPayload>(col_vsync[i]);
                    else this.array[this.populationCount++] = new PartitionedStreamEvent<TKey, TPayload>(colkey[i].Key, col_vsync[i], col_vother[i], batch[i]);

                    if (this.populationCount == this.arrayLength)
                    {
                        this.observer.OnNext(new ArraySegment<PartitionedStreamEvent<TKey, TPayload>>(this.array, 0, this.arrayLength));
                        this.populationCount = 0;
                        this.array = this.generator();
                        this.arrayLength = this.array.Length;
                    }
                }
                batch.Free();
            }

            public override void OnCompleted()
            {
                OnFlush();
                base.OnCompleted();
            }

            public override void OnFlush()
            {
                if (this.populationCount > 0)
                {
                    this.observer.OnNext(new ArraySegment<PartitionedStreamEvent<TKey, TPayload>>(this.array, 0, this.populationCount));
                    this.populationCount = 0;
                    this.array = this.generator();
                    this.arrayLength = this.array.Length;
                }
            }

            public override int CurrentlyBufferedOutputCount => this.populationCount;

            public override int CurrentlyBufferedInputCount => 0;
        }

        [DataContract]
        internal sealed class PartitionedStartEdgeArrayEgressPipe<TKey, TPayload, TResult> : EgressBoundary<PartitionKey<TKey>, TPayload, ArraySegment<TResult>>
        {
            private readonly Func<TResult[]> generator;
            [DataMember]
            private TResult[] array;
            [DataMember]
            private int arrayLength;
            [DataMember]
            private int populationCount = 0;
            private readonly Func<TKey, long, TPayload, TResult> constructor;

            [Obsolete("Used only by serialization. Do not call directly.")]
            public PartitionedStartEdgeArrayEgressPipe() { }

            public PartitionedStartEdgeArrayEgressPipe(
                Func<TResult[]> generator,
                Expression<Func<TKey, long, TPayload, TResult>> constructor,
                IObserver<ArraySegment<TResult>> observer,
                QueryContainer container)
                : base(observer, container)
            {
                this.generator = generator;
                this.array = this.generator();
                this.arrayLength = this.array.Length;
                this.constructor = constructor.Compile();
            }

            public override void OnNext(StreamMessage<PartitionKey<TKey>, TPayload> batch)
            {
                var colkey = batch.key.col;
                var col_bv = batch.bitvector.col;
                var col_vsync = batch.vsync.col;
                var col_vother = batch.vother.col;

                for (int i = 0; i < batch.Count; i++)
                {
                    if ((col_bv[i >> 6] & (1L << (i & 0x3f))) != 0)
                        continue;
                    else if (col_vother[i] == StreamEvent.InfinitySyncTime)
                        this.array[this.populationCount++] = this.constructor(colkey[i].Key, col_vsync[i], batch[i]);
                    else throw new StreamProcessingException("The query has encountered either an end edge or an interval, while the egress point expects only start edges.");

                    if (this.populationCount == this.arrayLength)
                    {
                        this.observer.OnNext(new ArraySegment<TResult>(this.array, 0, this.arrayLength));
                        this.populationCount = 0;
                        this.array = this.generator();
                        this.arrayLength = this.array.Length;
                    }
                }
                batch.Free();
            }

            public override void OnCompleted()
            {
                OnFlush();
                base.OnCompleted();
            }

            public override void OnFlush()
            {
                if (this.populationCount > 0)
                {
                    this.observer.OnNext(new ArraySegment<TResult>(this.array, 0, this.populationCount));
                    this.populationCount = 0;
                    this.array = this.generator();
                    this.arrayLength = this.array.Length;
                }
            }

            public override int CurrentlyBufferedOutputCount => this.populationCount;

            public override int CurrentlyBufferedInputCount => 0;
        }

        [DataContract]
        internal sealed class PartitionedIntervalArrayEgressPipe<TKey, TPayload, TResult> : EgressBoundary<PartitionKey<TKey>, TPayload, ArraySegment<TResult>>
        {
            private readonly Func<TResult[]> generator;
            [DataMember]
            private TResult[] array;
            [DataMember]
            private int arrayLength;
            [DataMember]
            private int populationCount = 0;
            private readonly Func<TKey, long, long, TPayload, TResult> constructor;

            [Obsolete("Used only by serialization. Do not call directly.")]
            public PartitionedIntervalArrayEgressPipe() { }

            public PartitionedIntervalArrayEgressPipe(
                Func<TResult[]> generator,
                Expression<Func<TKey, long, long, TPayload, TResult>> constructor,
                IObserver<ArraySegment<TResult>> observer,
                QueryContainer container)
                : base(observer, container)
            {
                this.generator = generator;
                this.array = this.generator();
                this.arrayLength = this.array.Length;
                this.constructor = constructor.Compile();
            }

            public override void OnNext(StreamMessage<PartitionKey<TKey>, TPayload> batch)
            {
                var colkey = batch.key.col;
                var col_bv = batch.bitvector.col;
                var col_vsync = batch.vsync.col;
                var col_vother = batch.vother.col;

                for (int i = 0; i < batch.Count; i++)
                {
                    if ((col_bv[i >> 6] & (1L << (i & 0x3f))) != 0)
                        continue;
                    else this.array[this.populationCount++] = this.constructor(colkey[i].Key, col_vsync[i], col_vother[i], batch[i]);

                    if (this.populationCount == this.arrayLength)
                    {
                        this.observer.OnNext(new ArraySegment<TResult>(this.array, 0, this.arrayLength));
                        this.populationCount = 0;
                        this.array = this.generator();
                        this.arrayLength = this.array.Length;
                    }
                }
                batch.Free();
            }

            public override void OnCompleted()
            {
                OnFlush();
                base.OnCompleted();
            }

            public override void OnFlush()
            {
                if (this.populationCount > 0)
                {
                    this.observer.OnNext(new ArraySegment<TResult>(this.array, 0, this.populationCount));
                    this.populationCount = 0;
                    this.array = this.generator();
                    this.arrayLength = this.array.Length;
                }
            }

            public override int CurrentlyBufferedOutputCount => this.populationCount;

            public override int CurrentlyBufferedInputCount => 0;
        }
}