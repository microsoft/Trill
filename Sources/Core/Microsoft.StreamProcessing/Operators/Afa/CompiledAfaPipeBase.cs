// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.ComponentModel;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing.Internal
{
    /// <summary>
    /// Currently for internal use only - do not use directly.
    /// </summary>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class CompiledAfaPipeBase<TKey, TPayload, TRegister, TAccumulator> : UnaryPipe<TKey, TPayload, TRegister>
    {
        /* Local copy of automaton */

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly", Justification="Used to avoid creating redundant readonly property.")]
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly bool[] isFinal;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly", Justification = "Used to avoid creating redundant readonly property.")]
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly bool[] hasOutgoingArcs;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly", Justification = "Used to avoid creating redundant readonly property.")]
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly SingleEventArcInfo<TPayload, TRegister>[][] singleEventStateMap;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly", Justification = "Used to avoid creating redundant readonly property.")]
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly EventListArcInfo<TPayload, TRegister>[][] eventListStateMap;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly", Justification = "Used to avoid creating redundant readonly property.")]
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly MultiEventArcInfo<TPayload, TRegister, TAccumulator>[][] multiEventStateMap;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly", Justification = "Used to avoid creating redundant readonly property.")]
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly int[][] epsilonStateMap;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly", Justification = "Used to avoid creating redundant readonly property.")]
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly int[] startStates;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly int numStartStates;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Used to avoid creating redundant readonly property.")]
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly TRegister defaultRegister;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly bool AllowOverlappingInstances;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly bool IsDeterministic;
        /* End local copy of automaton */

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [SchemaSerialization]
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly long MaxDuration;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly bool IsSyncTimeSimultaneityFree;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly bool IsGenerated;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "It is immutable.")]
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly MemoryPool<TKey, TRegister> pool;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly string errorMessages;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Used to avoid creating redundant readonly property.")]
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected readonly Func<TKey, TKey, bool> keyEqualityComparer;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected int iter;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected StreamMessage<TKey, TRegister> batch;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [Obsolete("Used only by serialization. Do not call directly.")]
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected CompiledAfaPipeBase() { }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="observer"></param>
        /// <param name="afa"></param>
        /// <param name="maxDuration"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected CompiledAfaPipeBase(IStreamable<TKey, TRegister> stream, IStreamObserver<TKey, TRegister> observer, object afa, long maxDuration)
            : base(stream, observer)
        {
            var compiledAfa = (CompiledAfa<TPayload, TRegister, TAccumulator>)afa;

            this.isFinal = compiledAfa.isFinal;
            this.hasOutgoingArcs = compiledAfa.hasOutgoingArcs;
            this.singleEventStateMap = compiledAfa.singleEventStateMap;
            this.eventListStateMap = compiledAfa.eventListStateMap;
            this.multiEventStateMap = compiledAfa.multiEventStateMap;
            this.epsilonStateMap = compiledAfa.epsilonStateMap;
            this.startStates = compiledAfa.startStates;
            this.numStartStates = compiledAfa.numStartStates;
            this.defaultRegister = compiledAfa.defaultRegister;
            this.AllowOverlappingInstances = compiledAfa.uncompiledAfa.AllowOverlappingInstances;
            this.IsDeterministic = compiledAfa.uncompiledAfa.IsDeterministic;

            this.MaxDuration = maxDuration;
            this.IsSyncTimeSimultaneityFree = stream.Properties.IsSyncTimeSimultaneityFree;

            this.pool = MemoryManager.GetMemoryPool<TKey, TRegister>(stream.Properties.IsColumnar);
            this.pool.Get(out this.batch);
            this.batch.Allocate();
            this.batch.iter = 0;
            this.iter = 0;
            this.errorMessages = stream.ErrorMessages;
            this.keyEqualityComparer = stream.Properties.KeyEqualityComparer.GetEqualsExpr().Compile();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="timestamp"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected void OnPunctuation(long timestamp)
        {
            this.batch.vsync.col[this.iter] = timestamp;
            this.batch.vother.col[this.iter] = StreamEvent.PunctuationOtherTime;
            this.batch.bitvector.col[this.iter >> 6] |= (1L << (this.iter & 0x3f));

            if (this.batch.payload != null) this.batch.payload.col[this.iter] = default;
            if (this.batch.key != null) this.batch.key.col[this.iter] = default;
            this.batch.hash.col[this.iter] = 0;
            this.iter++;
            if (this.iter == Config.DataBatchSize) FlushContents();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="timestamp"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected void OnLowWatermark(long timestamp)
        {
            this.batch.vsync.col[this.iter] = timestamp;
            this.batch.vother.col[this.iter] = PartitionedStreamEvent.LowWatermarkOtherTime;
            this.batch.bitvector.col[this.iter >> 6] |= (1L << (this.iter & 0x3f));

            if (this.batch.payload != null) this.batch.payload.col[this.iter] = default;
            if (this.batch.key != null) this.batch.key.col[this.iter] = default;
            this.batch.hash.col[this.iter] = 0;
            this.iter++;
            if (this.iter == Config.DataBatchSize) FlushContents();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected override void DisposeState() => this.batch.Free();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected override void FlushContents()
        {
            if (this.iter > 0)
            {
                this.batch.Count = this.iter;
                this.batch.Seal();
                this.Observer.OnNext(this.batch);
                this.pool.Get(out this.batch);
                this.batch.Allocate();
                this.batch.iter = 0;
                this.iter = 0;
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override int CurrentlyBufferedOutputCount => this.iter;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="previous"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new AfaPlanNode(
                previous, this, typeof(TKey), typeof(TPayload), this.IsGenerated, this.errorMessages));
    }
}