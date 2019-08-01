// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class TracePipe<TKey, TPayload> : UnaryPipe<TKey, TPayload, TPayload>, ITraceMetrics
    {
        [SchemaSerialization]
        private readonly string traceId;
        [SchemaSerialization]
        private readonly bool isPartitioned = typeof(TKey).GetPartitionType() != null;

        [DataMember]
        private long totalEventCount = 0;
        [DataMember]
        private long maxSyncTime = 0;
        [DataMember]
        private long maxPunctuationTime = 0;
        [DataMember]
        private long lowWatermark = 0;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public TracePipe() { }

        public TracePipe(TraceStreamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer)
            : base(stream, observer)
        {
            this.traceId = stream.TraceId;
        }

        public string TraceId => this.traceId;

        public long EventCount => this.totalEventCount;

        /// <summary>
        /// Gets the value of the latest event seen in the stream.
        /// For the case of sub-streams this is the timestamp of the latestevent across all sub-streams.
        /// Note that this value tracks timestamps of events only and not the high watermark of the stream.
        /// High watermark is Max(MaxPunctuationTime, MaxSyncTime).
        /// </summary>
        public long MaxSyncTime => this.maxSyncTime;

        /// <summary>
        /// Gets the value of the latest punctuation seen in the stream.
        /// For the case of sub-streams this is the latest punctuation all across sub-streams.
        /// Note that this value tracks punctuations only and not the high watermark of the stream.
        /// High watermark is Max(MaxPunctuationTime, MaxSyncTime).
        /// </summary>
        public long MaxPunctuationTime => this.maxPunctuationTime;

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new TracePlanNode(
                previous,
                this,
                this,
                typeof(TKey),
                typeof(TPayload)));

        public override unsafe void OnNext(StreamMessage<TKey, TPayload> batch)
        {
            var vsync = batch.vsync.col;
            var vother = batch.vother.col;
            fixed (long* bv = batch.bitvector.col)
            {
                for (int i = 0; i < batch.Count; i++)
                {
                    if ((bv[i >> 6] & (1L << (i & 0x3f))) != 0)
                    {
                        if (vother[i] == StreamEvent.PunctuationOtherTime)
                        {
                            this.maxPunctuationTime = Math.Max(this.maxPunctuationTime, vsync[i]);
                        }
                        else if (vother[i] == PartitionedStreamEvent.LowWatermarkOtherTime)
                        {
                            if (this.isPartitioned)
                            {
                                this.lowWatermark = Math.Max(vsync[i], this.lowWatermark);
                            }

                            this.maxPunctuationTime = Math.Max(this.maxPunctuationTime, vsync[i]);
                        }

                        continue;
                    }

                    // In order to avoid double counting, exclude end edges
                    if (vother[i] == StreamEvent.InfinitySyncTime || vsync[i] < vother[i])
                    {
                        this.totalEventCount++;
                    }

                    // Perform out-of-order detection
                    // For partitioned streams, just check against low watermark, since tracking
                    // the individual partition sync times could be exensive.
                    var outOfOrderThreshold = this.isPartitioned ? this.lowWatermark : this.maxSyncTime;
                    if (vsync[i] < outOfOrderThreshold)
                    {
                        throw new StreamProcessingOutOfOrderException(
                            $"TraceId: {this.TraceId} " +
                            $"Out of order output detected! " +
                            $"Current/previous sync time: {outOfOrderThreshold}, " +
                            $"Event sync time: {vsync[i]}");
                    }

                    this.maxSyncTime = Math.Max(this.maxSyncTime, vsync[i]);
                }
            }

            this.Observer.OnNext(batch);
        }
    }
}