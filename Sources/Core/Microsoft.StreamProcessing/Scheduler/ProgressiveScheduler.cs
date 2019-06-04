// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;

namespace Microsoft.StreamProcessing
{
    internal enum TaskEntryStatus
    {
        Inactive,
        HasWork,
        Processing
    }

    internal enum MessageKind
    {
        DataBatch,
        Completed,
        Flush
    }

    internal struct QueuedMessage<T> where T : StreamMessage
    {
        public MessageKind Kind;
        public T Message;
    }

    internal sealed class TaskEntry
    {
        public TaskEntry(Guid classId, Action<StreamMessage> onNext, Action onCompleted, Action onFlush, Action<Exception> onError)
        {
            this.ClassId = classId;
            this.Priority = -1;
            this.TaskCount = 0;
            this.onNext = onNext;
            this.onCompleted = onCompleted;
            this.onFlush = onFlush;
            this.onError = onError;
            this.tasks = new ConcurrentQueue<QueuedMessage<StreamMessage>>();
            this.Status = TaskEntryStatus.Inactive;
            this.Disposed = false;
            this.Completed = false;
        }

        public Guid ClassId;
        public long Priority;
        public int TaskCount;
        public bool Disposed;
        public bool Completed;
        public TaskEntryStatus Status;
        public Action<StreamMessage> onNext;
        private Action onCompleted;
        public Action onFlush;
        public Action<Exception> onError;
        public ConcurrentQueue<QueuedMessage<StreamMessage>> tasks;

        public void OnCompleted()
        {
            this.Completed = true;
            this.onCompleted();
            this.onNext = null;
            this.onCompleted = null;
            this.onFlush = null;
            this.onError = null;
        }
    }

    internal sealed class TaskEntryComparer : IComparer<TaskEntry>
    {
        public int Compare(TaskEntry x, TaskEntry y)
            => x.Priority < y.Priority ? -1 : (x.Priority == y.Priority ? x.ClassId.CompareTo(y.ClassId) : 1);
    }

    internal sealed class OwnedThreadsScheduler : IInternalScheduler
    {
        internal SortedSet<TaskEntry> pendingTasks;
        internal ConcurrentDictionary<Guid, TaskEntry> taskTable;
        internal ConcurrentDictionary<int, ScheduledUnit> activeThreads;
        internal int pendingTaskCount;
        internal bool useCommonSprayPool = false;
        internal object global = new object();

        internal bool affinitize;

        public OwnedThreadsScheduler(int numThreads, bool affinitize = false)
        {
            this.pendingTaskCount = 0;
            this.affinitize = affinitize;
            this.pendingTasks = new SortedSet<TaskEntry>(new TaskEntryComparer());
            this.activeThreads = new ConcurrentDictionary<int, ScheduledUnit>();
            this.taskTable = new ConcurrentDictionary<Guid, TaskEntry>();

            for (int i = 0; i < numThreads; i++) this.activeThreads.TryAdd(i, new ScheduledUnit(this, i));

            this.MapArity = numThreads;
        }

        public IStreamObserver<TK, TP> RegisterStreamObserver<TK, TP>(IStreamObserver<TK, TP> o, Guid? classId = null)
        {
            // Check if already wrapped
            if (o as WrapperStreamObserver<TK, TP> != null)
                return o;

            var cid = o.ClassId;
            if (this.useCommonSprayPool && (classId != null)) cid = classId.Value;

            var t = new TaskEntry(cid, m => o.OnNext((StreamMessage<TK, TP>)m), o.OnCompleted, o.OnFlush, o.OnError);

            this.taskTable.TryAdd(cid, t);

            return new WrapperStreamObserver<TK, TP>(o, this, t);
        }

        public void Stop()
        {
            foreach (var x in this.activeThreads) x.Value.Stop();
            foreach (var x in this.activeThreads) x.Value.Join();
        }

        public int MapArity { get; private set; }

        public int ReduceArity => this.MapArity;
    }

    internal class ScheduledUnit
    {
        private readonly Thread thread;
        private readonly OwnedThreadsScheduler scheduler;
        private bool stopped;

        public ScheduledUnit(OwnedThreadsScheduler scheduler, int id)
        {
            this.scheduler = scheduler;
            this.stopped = false;
            this.thread = new Thread(Run);
            this.thread.Start(id);
        }

        public void Stop()
        {
            this.stopped = true;
            lock (this.scheduler.global)
            {
                Monitor.PulseAll(this.scheduler.global);
            }
        }

        public void Join() => this.thread.Join();

        public void Run(object obj)
        {
            int id = (int)obj;

            if (this.scheduler.affinitize) NativeMethods.AffinitizeThread(id);

            TaskEntry te = null;

            while (!this.stopped)
            {
                lock (this.scheduler.global)
                {
                    if (te != null)
                    {
                        if (te.TaskCount > 0)
                        {
                            // Add operator back to pending list with corrected priority
                            te.Status = TaskEntryStatus.HasWork;
                            this.scheduler.pendingTasks.Add(te);
                        }
                        else
                            te.Status = TaskEntryStatus.Inactive;
                    }

                    te = null;
                    while (!this.stopped && (this.scheduler.pendingTasks.Count == 0))
                    {
                        Monitor.Wait(this.scheduler.global);
                    }

                    if (this.stopped) return;

                    this.scheduler.pendingTasks.TryGetFirst(out te);
                    this.scheduler.pendingTasks.Remove(te);
                    te.Status = TaskEntryStatus.Processing;

                    if (this.scheduler.pendingTasks.Count > 0) Monitor.Pulse(this.scheduler.global);
                }

                try
                {
                    long now = -1;
                    long mt = -1;
                    int par = 0;
                    while (te.tasks.TryPeek(out var message))
                    {
                        mt = message.Kind == MessageKind.DataBatch ? message.Message.MinTimestamp : StreamEvent.InfinitySyncTime;
                        if ((now == -1) || (now == mt))
                        {
                            now = mt;
                            if (!te.tasks.TryDequeue(out message))
                                throw new InvalidOperationException("Could not dequeue task from task entry");

                            var newCount = Interlocked.Decrement(ref te.TaskCount);

                            switch (message.Kind)
                            {
                                case MessageKind.DataBatch:
                                    if (te.Disposed) message.Message.Free();
                                    else te.onNext(message.Message);
                                    break;
                                case MessageKind.Completed:
                                    if (!te.Completed)
                                    {
                                        te.OnCompleted();
                                        this.scheduler.taskTable.TryRemove(te.ClassId, out var tmp);
                                    }
                                    break;
                                case MessageKind.Flush:
                                    te.onFlush();
                                    break;
                            }

                            par++;
                            if (newCount == 0) break;
                        }
                        else
                        {
                            te.Priority = mt;
                            if (this.scheduler.pendingTasks.Count > 0) break;
                            now = mt;
                        }
                    }
                }
                catch (Exception e)
                {
                    te.onError(e);
                }
            }
        }
    }

    internal sealed class WrapperStreamObserver<TK, TP> : IStreamObserver<TK, TP>, IDisposable
    {
        private readonly IStreamObserver<TK, TP> o;
        private readonly OwnedThreadsScheduler scheduler;
        private readonly TaskEntry te;
        private bool onCompletedSeen = false;

        public WrapperStreamObserver(IStreamObserver<TK, TP> o, OwnedThreadsScheduler scheduler, TaskEntry t)
        {
            this.o = o;
            this.scheduler = scheduler;
            this.te = t;
        }

        public void OnError(Exception error) => this.o.OnError(error);

        public void Checkpoint(System.IO.Stream stream) => this.o.Checkpoint(stream);

        public void Restore(System.IO.Stream stream) => this.o.Restore(stream);

        public void Reset() => this.o.Reset();

        public void ProduceQueryPlan(PlanNode previous) => throw new NotImplementedException();

        public void OnNext(StreamMessage<TK, TP> message)
        {
            if (this.te.Disposed)
            {
                message.Free();
                return;
            }

            EnqueueMessage(MessageKind.DataBatch, message);
        }

        private void EnqueueMessage(MessageKind kind, StreamMessage<TK, TP> message = null)
        {
            this.te.tasks.Enqueue(new QueuedMessage<StreamMessage> { Kind = kind, Message = message });
            var newCount = Interlocked.Increment(ref this.te.TaskCount);
            if (newCount == 1)
            {
                lock (this.scheduler.global)
                {
                    if (this.te.Status == TaskEntryStatus.Inactive)
                    {
                        // It's possible that a scheduler thread dequeues this event before this happens,
                        // and hence does not see the correct priority for the TaskEvent. This is benign.
                        this.te.Priority = message?.MinTimestamp ?? StreamEvent.MaxSyncTime;
                        this.te.Status = TaskEntryStatus.HasWork;
                        this.scheduler.pendingTasks.Add(this.te);
                        if (this.scheduler.pendingTasks.Count == 1) Monitor.Pulse(this.scheduler.global);
                    }
                }
            }
        }

        public void OnCompleted()
        {
            this.onCompletedSeen = true;
            EnqueueMessage(MessageKind.Completed);
        }

        public void OnFlush() => EnqueueMessage(MessageKind.Flush);

        public Guid ClassId => throw new NotImplementedException();

        public int CurrentlyBufferedOutputCount => this.o.CurrentlyBufferedOutputCount;

        public int CurrentlyBufferedInputCount => this.o.CurrentlyBufferedInputCount;

        public void Dispose()
        {
            lock (this.scheduler.global)
            {
                this.te.Disposed = true;
            }

            if (!this.onCompletedSeen)
            {
                this.onCompletedSeen = true;
                OnCompleted();
            }
        }
    }

}
