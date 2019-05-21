// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Threading;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Supports implementation of binary pipes that consume inputs in sync time order.
    /// Implements streaming merge sort. The only distinction between a conventional merge sort and a streaming
    /// merge sort is that you can't always peek at the next element in one of the inputs. However, we can
    /// keep track of the last consumed input and assume that whatever the next element is, it will not
    /// have a lower sync time.
    /// </summary>
    /// <remarks>
    /// It is important in demonstrating correctness that at least one buffer is empty at any point. Intuitively,
    /// this is because the existence of buffered elements implies that one input is more advanced than the other.
    /// It is not possible for each to be ahead of the other. Demonstration is trivial by induction (both queues
    /// are initially empty).
    /// </remarks>
    [DataContract]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class BinaryPipe<TKey, TLeft, TRight, TResult> : Pipe<TKey, TResult>, IBinaryObserver<TKey, TLeft, TRight, TResult>
    {
        /// <summary>
        /// Gets the observer the binary pipe uses to listen to input from the LHS.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Used to avoid creating redundant readonly property.")]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public IStreamObserver<TKey, TLeft> Left { get; }

        /// <summary>
        /// Gets the observer the binary pipe uses to listen to input from the RHS.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Used to avoid creating redundant readonly property.")]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public IStreamObserver<TKey, TRight> Right { get; }

        // TODO_PERFORMANCE:: special case when input events are already serialized because they are emitted from a common multicast
        // source.
        private readonly object binarySync;
        private readonly object sync = new object();

        private SerializationState serializationState = SerializationState.Open;
        private readonly Queue<PlanNode> leftPlans = new Queue<PlanNode>();
        private readonly Queue<PlanNode> rightPlans = new Queue<PlanNode>();

        [DataMember]
        private int completedCount;
        [DataMember]
        private ConcurrentQueue<StreamMessage<TKey, TLeft>> leftQueue = new ConcurrentQueue<StreamMessage<TKey, TLeft>>();
        [DataMember]
        private ConcurrentQueue<StreamMessage<TKey, TRight>> rightQueue = new ConcurrentQueue<StreamMessage<TKey, TRight>>();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [DataMember]
        protected long lastCTI = long.MinValue;
        [DataMember]
        private volatile ProcessState state = ProcessState.WaitingForAny;

        [DataContract]
        private enum ProcessState
        {
            // Implies there is no current processing of batches and both the left and right
            // queues are empty. Start processing batches whenever a new batch arrives,
            // regardless of whether it comes in on the left or right.
            WaitingForAny,

            // Implies there is no current processing of batches and the left queue is empty.
            // The right queue is not empty but the top batch cannot be further processed
            // until a batch on the left input arrives (so time can advance).
            WaitingForLeft,

            // Implies there is no current processing of batches and the right queue is empty.
            // The left queue is not empty but the top batch cannot be further processed
            // until a batch on the right input arrives (so time can advance).
            WaitingForRight,

            // A thread is currently processing batches. The thread could either be the thread
            // that called OnLeft or OnRight. Only one thread can process batches at a time.
            Processing
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected BinaryPipe() { }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="observer"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected BinaryPipe(IStreamable<TKey, TResult> stream, IStreamObserver<TKey, TResult> observer)
            : base(stream, observer)
        {
            var left = new LeftObserver(this, out ObserverBase<TRight, TLeft> right);

            this.Left = left;
            this.Right = right;

            this.binarySync = new object();
        }

        // Specializations of BinaryPipe override these methods. It is the responsibility of the BinaryPipe
        // merge sort algorithm to ensure that Consume* methods are called in sync time order
        private void OnLeft(StreamMessage<TKey, TLeft> batch)
        {
            batch.iter = 0;
            batch.RefreshCount();
            if (batch.Count == 0)
            {
                batch.Free();
                return;
            }

            this.leftQueue.Enqueue(batch);
            ProcessPendingBatches();
        }

        private void OnRight(StreamMessage<TKey, TRight> batch)
        {
            batch.iter = 0;
            batch.RefreshCount();
            if (batch.Count == 0)
            {
                batch.Free();
                return;
            }

            this.rightQueue.Enqueue(batch);
            ProcessPendingBatches();
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        public override void OnFlush()
        {
            // This will not deadlock, since it we can only be waiting another thread further along in the
            // query, and there cannot be cycles in the query.
            Monitor.Enter(this.sync);
            try
            {
                FlushContents();
                this.Observer.OnFlush();
            }
            finally
            {
                Monitor.Exit(this.sync);
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        public override void OnCompleted()
        {
            int count = Interlocked.Increment(ref this.completedCount);
            if (count == 2)
            {
                // Ensure we have no pending batches in case we cut off another thread that was about to do so.
                // TODO: add parameter to so implementers can finish processing data knowing there will be no
                // future batches, then the cleanup below should only catch bugs in implementers.
                ProcessPendingBatches();

                // Not all binary pipes will have processed all batches, so make sure we don't leak.
                // This will not deadlock, since it we can only be waiting another thread further along in the
                // query, and there cannot be cycles in the query.
                Monitor.Enter(this.sync);
                try
                {
                    base.OnCompleted();

                    while (this.leftQueue.TryDequeue(out var leftBatch))
                    {
                        leftBatch.Free();
                    }
                    while (this.rightQueue.TryDequeue(out var rightBatch))
                    {
                        rightBatch.Free();
                    }
                }
                finally
                {
                    Monitor.Exit(this.sync);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessPendingBatches()
        {
            // Exit if another thread is already processing the pending batches
            if (!Monitor.TryEnter(this.sync))
            {
                return;
            }

            // Upon acquiring and releasing the lock, state should be one of WaitingFor*, not Processing, since if
            // there is still processing to be done, we shouldn't release the lock.
            try
            {
                while (true)
                {
                    bool process;
                    switch (this.state)
                    {
                        case ProcessState.WaitingForLeft:
                            process = !this.leftQueue.IsEmpty;
                            break;
                        case ProcessState.WaitingForRight:
                            process = !this.rightQueue.IsEmpty;
                            break;
                        case ProcessState.WaitingForAny:
                            process = !this.leftQueue.IsEmpty || !this.rightQueue.IsEmpty;
                            break;
                        default:
                            throw new InvalidOperationException();
                    }

                    if (!process) break;

                    this.state = ProcessState.Processing;

                    while (true)
                    {
                        bool hasLeftBatch = this.leftQueue.TryPeek(out var leftBatch);
                        bool hasRightBatch = this.rightQueue.TryPeek(out var rightBatch);
                        bool leftBatchDone = false;
                        bool rightBatchDone = false;
                        bool leftBatchFree = true;
                        bool rightBatchFree = true;

                        if (hasLeftBatch && hasRightBatch)
                        {
                            ProcessBothBatches(leftBatch, rightBatch, out leftBatchDone, out rightBatchDone, out leftBatchFree, out rightBatchFree);
                        }
                        else if (hasLeftBatch)
                        {
                            ProcessLeftBatch(leftBatch, out leftBatchDone, out leftBatchFree);
                        }
                        else if (hasRightBatch)
                        {
                            ProcessRightBatch(rightBatch, out rightBatchDone, out rightBatchFree);
                        }
                        else
                        {
                            this.state = ProcessState.WaitingForAny;
                            break;
                        }

                        if (leftBatchDone)
                        {
                            this.leftQueue.TryDequeue(out leftBatch);
                            if (leftBatchFree) leftBatch.Free();
                        }

                        if (rightBatchDone)
                        {
                            this.rightQueue.TryDequeue(out rightBatch);
                            if (rightBatchFree) rightBatch.Free();
                        }

                        if (!leftBatchDone && !rightBatchDone)
                        {
                            this.state = hasLeftBatch ? ProcessState.WaitingForRight : ProcessState.WaitingForLeft;
                            break;
                        }
                    }
                }
            }
            finally
            {
                Monitor.Exit(this.sync);
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual bool LeftInputHasState => !this.leftQueue.IsEmpty;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual bool RightInputHasState => !this.rightQueue.IsEmpty;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="leftBatch"></param>
        /// <param name="leftBatchDone"></param>
        /// <param name="leftBatchFree"></param>
        /// <param name="rightBatch"></param>
        /// <param name="rightBatchDone"></param>
        /// <param name="rightBatchFree"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected abstract void ProcessBothBatches(StreamMessage<TKey, TLeft> leftBatch, StreamMessage<TKey, TRight> rightBatch, out bool leftBatchDone, out bool rightBatchDone, out bool leftBatchFree, out bool rightBatchFree);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="leftBatch"></param>
        /// <param name="leftBatchDone"></param>
        /// <param name="leftBatchFree"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected abstract void ProcessLeftBatch(StreamMessage<TKey, TLeft> leftBatch, out bool leftBatchDone, out bool leftBatchFree);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="rightBatch"></param>
        /// <param name="rightBatchDone"></param>
        /// <param name="rightBatchFree"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected abstract void ProcessRightBatch(StreamMessage<TKey, TRight> rightBatch, out bool rightBatchDone, out bool rightBatchFree);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override int CurrentlyBufferedInputCount => this.CurrentlyBufferedLeftInputCount + this.CurrentlyBufferedRightInputCount;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual int CurrentlyBufferedLeftInputCount => this.leftQueue.Select(q => q.Count).Sum();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual int CurrentlyBufferedRightInputCount => this.rightQueue.Select(q => q.Count).Sum();

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual int CurrentlyBufferedLeftKeyCount => this.LeftInputHasState ? 1 : 0;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public virtual int CurrentlyBufferedRightKeyCount => this.RightInputHasState ? 1 : 0;

        private void CheckpointLeft(Stream stream)
        {
            try
            {
                switch (this.serializationState)
                {
                    case SerializationState.Open:
                        this.serializationState = SerializationState.CheckpointLeft;
                        Checkpoint(stream);
                        break;

                    case SerializationState.CheckpointRight:
                        this.serializationState = SerializationState.Open;
                        break;

                    default:
                        throw new InvalidOperationException();
                }
            }
            catch (Exception)
            {
                this.serializationState = SerializationState.Open;
                throw;
            }
        }

        private void CheckpointRight(Stream stream)
        {
            try
            {
                switch (this.serializationState)
                {
                    case SerializationState.Open:
                        this.serializationState = SerializationState.CheckpointRight;
                        Checkpoint(stream);
                        break;

                    case SerializationState.CheckpointLeft:
                        this.serializationState = SerializationState.Open;
                        break;

                    default:
                        throw new InvalidOperationException();
                }
            }
            catch (Exception)
            {
                this.serializationState = SerializationState.Open;
                throw;
            }
        }

        private void RestoreLeft(Stream stream)
        {
            try
            {
                switch (this.serializationState)
                {
                    case SerializationState.Open:
                        this.serializationState = SerializationState.RestoreLeft;
                        Restore(stream);
                        break;

                    case SerializationState.RestoreRight:
                        this.serializationState = SerializationState.Open;
                        break;

                    default:
                        throw new InvalidOperationException();
                }
            }
            catch (Exception)
            {
                this.serializationState = SerializationState.Open;
                throw;
            }
        }

        private void RestoreRight(Stream stream)
        {
            try
            {
                switch (this.serializationState)
                {
                    case SerializationState.Open:
                        this.serializationState = SerializationState.RestoreRight;
                        Restore(stream);
                        break;

                    case SerializationState.RestoreLeft:
                        this.serializationState = SerializationState.Open;
                        break;

                    default:
                        throw new InvalidOperationException();
                }
            }
            catch (Exception)
            {
                this.serializationState = SerializationState.Open;
                throw;
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void Reset()
        {
            this.serializationState = SerializationState.Open;
            this.Observer.Reset();
        }

        private void ReceiveLeftQueryPlan(PlanNode left)
        {
            this.leftPlans.Enqueue(left);
            lock (this.rightPlans)
            {
                if (this.rightPlans.Count > 0)
                {
                    ProduceBinaryQueryPlan(this.leftPlans.Dequeue(), this.rightPlans.Dequeue());
                }
            }
        }

        private void ReceiveRightQueryPlan(PlanNode right)
        {
            this.rightPlans.Enqueue(right);
            lock (this.leftPlans)
            {
                if (this.leftPlans.Count > 0)
                {
                    ProduceBinaryQueryPlan(this.leftPlans.Dequeue(), this.rightPlans.Dequeue());
                }
            }
        }

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected abstract void ProduceBinaryQueryPlan(PlanNode left, PlanNode right);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="previous"></param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override void ProduceQueryPlan(PlanNode previous) =>
            throw new InvalidOperationException(); // Remove this method when moving implementation from Pipe to UnaryPipe.

        private enum SerializationState { Open, CheckpointLeft, CheckpointRight, RestoreLeft, RestoreRight }

        // Implementations of the input observers for the pipe.
        private abstract class ObserverBase<TThis, TOther> : IStreamObserver<TKey, TThis>
        {
            /// <summary>
            /// Gets or sets a value indicating whether or not we're done with this input (received OnCompleted from this input
            /// or received OnError from either input).
            /// </summary>
            private bool isCompleted;
            protected readonly BinaryPipe<TKey, TLeft, TRight, TResult> Parent;

            /// <summary>
            /// Gets the 'other' observer (the 'left' observer if this is 'right', the 'right' observer if this is 'left').
            /// </summary>
            private readonly ObserverBase<TOther, TThis> other;

            public Guid ClassId { get; private set; }

            public abstract void OnNext(StreamMessage<TKey, TThis> batch);
            public abstract void Checkpoint(Stream stream);
            public abstract void Restore(Stream stream);
            public abstract void ProduceQueryPlan(PlanNode previous);
            public abstract void Reset();
            public abstract void OnFlush();
            public abstract void OnCompleted();

            public int CurrentlyBufferedOutputCount => 0;
            public int CurrentlyBufferedInputCount => 0;

            protected ObserverBase(BinaryPipe<TKey, TLeft, TRight, TResult> parent, Func<ObserverBase<TThis, TOther>, ObserverBase<TOther, TThis>> createOther, out ObserverBase<TOther, TThis> other)
            {
                this.Parent = parent;
                other = this.other = createOther(this);
                this.ClassId = Guid.NewGuid();
            }

            protected ObserverBase(BinaryPipe<TKey, TLeft, TRight, TResult> parent, ObserverBase<TOther, TThis> other)
            {
                this.Parent = parent;
                this.other = other;
                this.ClassId = Guid.NewGuid();
            }

            public void OnError(Exception error)
            {
                lock (this.Parent.binarySync)
                {
                    if (!this.isCompleted)
                    {
                        this.Parent.OnError(error);
                    }

                    this.isCompleted = true;
                    this.other.isCompleted = true;
                }
            }
        }

        private sealed class LeftObserver : ObserverBase<TLeft, TRight>
        {
            public LeftObserver(BinaryPipe<TKey, TLeft, TRight, TResult> parent, out ObserverBase<TRight, TLeft> right)
                : base(parent, l => new RightObserver(parent, l), out right)
            { }

            public override void OnNext(StreamMessage<TKey, TLeft> batch) => this.Parent.OnLeft(batch);

            public override void Checkpoint(Stream stream) => this.Parent.CheckpointLeft(stream);

            public override void Restore(Stream stream) => this.Parent.RestoreLeft(stream);

            public override void Reset() => this.Parent.Reset();

            public override void OnFlush() => this.Parent.OnFlush();

            public override void OnCompleted() => this.Parent.OnCompleted();

            public override void ProduceQueryPlan(PlanNode previous) => this.Parent.ReceiveLeftQueryPlan(previous);

            private sealed class RightObserver : ObserverBase<TRight, TLeft>
            {
                public RightObserver(BinaryPipe<TKey, TLeft, TRight, TResult> parent, ObserverBase<TLeft, TRight> left)
                    : base(parent, left)
                { }

                public override void OnNext(StreamMessage<TKey, TRight> batch) => this.Parent.OnRight(batch);

                public override void Checkpoint(Stream stream) => this.Parent.CheckpointRight(stream);

                public override void Restore(Stream stream) => this.Parent.RestoreRight(stream);

                public override void Reset() => this.Parent.Reset();

                public override void OnFlush() => this.Parent.OnFlush();

                public override void OnCompleted() => this.Parent.OnCompleted();

                public override void ProduceQueryPlan(PlanNode previous) => this.Parent.ReceiveRightQueryPlan(previous);
            }
        }
    }
}
