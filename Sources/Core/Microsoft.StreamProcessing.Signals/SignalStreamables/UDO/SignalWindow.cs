// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing.Signal.UDO
{
    /// <summary>
    /// Base class representing a signal stream window
    /// </summary>
    /// <typeparam name="T">Data/event payload type for the signal window</typeparam>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1063:ImplementIDisposableCorrectly"), DataContract]
    public class BaseWindow<T> : IDisposable
    {
        /// <summary>
        /// For internal use only - do not use externally
        /// </summary>
        [DataMember]
        protected int BaseWindowSize;
        /// <summary>
        /// For internal use only - do not use externally
        /// </summary>
        [DataMember]
        protected int HistoryWindowSize;
        /// <summary>
        /// For internal use only - do not use externally
        /// </summary>
        [DataMember]
        protected int Size;
        [DataMember]
        internal int Capacity;
        /// <summary>
        /// For internal use only - do not use externally
        /// </summary>
        [DataMember]
        protected int IndexMask;
        [DataMember]
        internal T[] Items;
        [DataMember]
        internal int tail;

        internal BaseWindow() { }

        internal BaseWindow(int baseSize, int historySize)
        {
            BaseWindowSize = baseSize;
            HistoryWindowSize = historySize;
            Size = baseSize + historySize;
            Capacity = Utility.Power2Ceil(Size + 1);
            IndexMask = Capacity - 1;
            Items = new T[Capacity];
            tail = 0;
        }

        internal BaseWindow(T[] items, int baseSize) : this() => SetItems(items, baseSize);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal CircularArrayRange CurrentRange() => new CircularArrayRange(Capacity, (tail - BaseWindowSize) & IndexMask, tail);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal CircularArrayRange OldRange() => new CircularArrayRange(Capacity, (tail - Size) & IndexMask, (tail - BaseWindowSize) & IndexMask);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal CircularArrayRange NewRange() => new CircularArrayRange(Capacity, (tail - HistoryWindowSize) & IndexMask, tail);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Linearize(T[] output)
        {
            var range = CurrentRange();

            if (output.Length < range.Length)
            {
                throw new ArgumentException("Output array is too small");
            }

            Array.Copy(Items, range.First.Head, output, 0, range.First.Length);
            Array.Copy(Items, range.Second.Head, output, range.First.Length, range.Second.Length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetItems(T[] items, int baseSize)
        {
            if (items == null || items.Length <= baseSize)
            {
                throw new ArgumentException("Array size has to be greater than base size");
            }

            if (!Utility.IsPowerOfTwo(items.Length))
            {
                throw new ArgumentException("Initial capacity has to be zero or a power of two");
            }

            BaseWindowSize = baseSize;
            HistoryWindowSize = 0;
            Size = items.Length;
            Capacity = items.Length;
            IndexMask = Capacity - 1;
            Items = items;
            tail = baseSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Enqueue(ref T item)
        {
            Items[tail] = item;
            tail = (tail + 1) & IndexMask;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Clear()
        {
            Array.Clear(Items, 0, Capacity);
            tail = 0;
        }

        /// <summary>
        /// Disposal method to implement IDisposable
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1063:ImplementIDisposableCorrectly")]
        public void Dispose()
        {
            BaseWindowSize = 0;
            HistoryWindowSize = 0;
            Size = 0;
            Capacity = 0;
            IndexMask = 0;
            Items = null;
            tail = 0;
        }
    }

    [DataContract]
    internal abstract class SignalWindow<T> : BaseWindow<T>
    {
        [DataMember]
        protected readonly ISignalWindowObserver<T> observer;

        [DataMember]
        protected readonly long PeriodTicks;
        [DataMember]
        protected readonly long OffsetTicks;
        [DataMember]
        protected readonly long WindowSizeTicks;
        [DataMember]
        protected readonly long HopSizeTicks;

        [DataMember]
        protected long windowStartTime = long.MinValue;
        [DataMember]
        protected long windowEndTime = long.MinValue + 1;
        [DataMember]
        protected long nextSampleTime = long.MinValue;

        [DataMember]
        public int numberOfActiveItems;

        protected SignalWindow(int baseSize, int historySize, ISignalWindowObserver<T> observer, long period, long offset, int sampleWindowSize, int sampleHopSize)
            : base(baseSize, historySize)
        {
            this.observer = observer;
            PeriodTicks = period;
            OffsetTicks = offset;
            WindowSizeTicks = sampleWindowSize * PeriodTicks;
            HopSizeTicks = sampleHopSize * PeriodTicks;
            numberOfActiveItems = 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static SignalWindow<T> GetInstance(ISignalWindowObserver<T> observer, long period, long offset, int windowSize, int hopSize, bool setMissingDataToNull)
        {
            if (hopSize < windowSize)
            {
                return setMissingDataToNull
                    ? new HoppingSignalWindow<T>(observer, period, offset, windowSize, hopSize)
                    : (SignalWindow<T>)new PaddedHoppingSignalWindow<T>(observer, period, offset, windowSize, hopSize);
            }
            else
            {
                return setMissingDataToNull
                    ? new TumblingSignalWindow<T>(observer, period, offset, windowSize, hopSize)
                    : (SignalWindow<T>)new PaddedTumblingSignalWindow<T>(observer, period, offset, windowSize, hopSize);
            }
        }

        public abstract void AdvanceTime(long time);

        public abstract void Enqueue(long time, ref T item);
    }

    [DataContract]
    internal sealed class HoppingSignalWindow<T> : SignalWindow<T>
    {
        [DataMember]
        public bool isHistoryValid;

        public HoppingSignalWindow(ISignalWindowObserver<T> observer, long period, long offset, int sampleWindowSize, int sampleHopSize)
            : base(sampleWindowSize, sampleHopSize, observer, period, offset, sampleWindowSize, sampleHopSize)
        {
            Contract.Requires(sampleWindowSize > sampleHopSize);

            isHistoryValid = false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public sealed override void AdvanceTime(long time)
        {
            /* Move window, if necessary, to ensure
             *    time <= nextSampleTime
             * and
             *    windowStartTime <= nextSampleTime < windowEndTime
             */
            if (time > nextSampleTime)
            {
                // Drop current window
                Clear();
                numberOfActiveItems = 0;
                isHistoryValid = false;

                // Compute timestamps of next window
                windowEndTime = (time + WindowSizeTicks - OffsetTicks + HopSizeTicks - 1) / HopSizeTicks * HopSizeTicks + OffsetTicks;
                windowStartTime = windowEndTime - WindowSizeTicks;
                nextSampleTime = windowStartTime;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public sealed override void Enqueue(long time, ref T item)
        {
            // Advance time to ensure:
            //   1) time <= nextSampleTime
            //   2) windowStartTime <= nextSampleTime < windowEndTime
            AdvanceTime(time);

            if (time < nextSampleTime) { return; }

            // Ensured: windowStartTime <= nextSampleTime = time < windowEndTime

            Items[tail] = item;
            tail = (tail + 1) & IndexMask;
            numberOfActiveItems++;
            nextSampleTime += PeriodTicks;

            // If window is full, then move it by hopSize
            if (nextSampleTime == windowEndTime)
            {
                if (isHistoryValid)
                {
                    observer.OnHop(windowEndTime, this);
                }
                else
                {
                    observer.OnInit(windowEndTime, this);
                    isHistoryValid = true;
                }
                numberOfActiveItems -= HistoryWindowSize;

                windowStartTime += HopSizeTicks;
                windowEndTime += HopSizeTicks;
            }
        }
    }

    [DataContract]
    internal sealed class TumblingSignalWindow<T> : SignalWindow<T>
    {
        public TumblingSignalWindow(ISignalWindowObserver<T> observer, long period, long offset, int sampleWindowSize, int sampleHopSize)
            : base(sampleWindowSize, 0, observer, period, offset, sampleWindowSize, sampleHopSize)
            => Contract.Requires(sampleWindowSize <= sampleHopSize);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public sealed override void AdvanceTime(long time)
        {
            /* Move window, if necessary, to ensure
             *    time <= nextSampleTime
             * and
             *    windowStartTime <= nextSampleTime < windowEndTime
             */
            if (time <= nextSampleTime) { return; }

            Clear();
            numberOfActiveItems = 0;

            if (time < windowEndTime)
            {
                windowStartTime += HopSizeTicks;
                windowEndTime += HopSizeTicks;
            }
            else
            {
                // Compute timestamps of next window
                windowEndTime = (time + WindowSizeTicks - OffsetTicks + HopSizeTicks - 1) / HopSizeTicks * HopSizeTicks + OffsetTicks;
                windowStartTime = windowEndTime - WindowSizeTicks;
            }
            nextSampleTime = windowStartTime;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public sealed override void Enqueue(long time, ref T item)
        {
            // Advance time to ensure:
            //   1) time <= nextSampleTime
            //   2) windowStartTime <= nextSampleTime < windowEndTime

            AdvanceTime(time);

            if (time < nextSampleTime) { return; }

            // Ensured: windowStartTime <= nextSampleTime = time < windowEndTime

            Items[tail] = item;
            tail = (tail + 1) & IndexMask;
            numberOfActiveItems++;
            nextSampleTime += PeriodTicks;

            // If window is full, then move it by hopSize
            if (nextSampleTime == windowEndTime)
            {
                observer.OnInit(windowEndTime, this);
                numberOfActiveItems = 0;

                windowStartTime += HopSizeTicks;
                windowEndTime += HopSizeTicks;
                nextSampleTime = windowStartTime;
            }
        }
    }

    [DataContract]
    internal abstract class PaddedSignalWindow<T> : SignalWindow<T>
    {
        [DataMember]
        protected T DefaultValue => default;

        public PaddedSignalWindow(int baseSize, int historySize, ISignalWindowObserver<T> observer, long period, long offset, int sampleWindowSize, int sampleHopSize)
            : base(baseSize, historySize, observer, period, offset, sampleWindowSize, sampleHopSize) { }
    }

    [DataContract]
    internal sealed class PaddedHoppingSignalWindow<T> : PaddedSignalWindow<T>
    {
        [DataMember]
        private readonly bool[] IsActiveItems;
        [DataMember]
        private int numberOfItems;
        [DataMember]
        private bool isHistoryValid;

        public PaddedHoppingSignalWindow(ISignalWindowObserver<T> observer, long period, long offset, int sampleWindowSize, int sampleHopSize)
            : base(sampleWindowSize, sampleHopSize, observer, period, offset, sampleWindowSize, sampleHopSize)
        {
            Contract.Requires(sampleWindowSize > sampleHopSize);

            IsActiveItems = new bool[Capacity];
            numberOfItems = 0;
            isHistoryValid = false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public sealed override void AdvanceTime(long time)
        {
            /* Move window, if necessary, to ensure
             *    (windowEndTime - hopSize) <= time < windowEndTime
             * and
             *    windowStartTime <= nextSampleTime < windowEndTime
             */
            while (time >= windowEndTime)
            {
                if (numberOfActiveItems > 0)
                {
                    // Fill out window with null values between [index, windowBeginIndex + WindowSize)
                    for (; numberOfItems < BaseWindowSize; numberOfItems++)
                    {
                        IsActiveItems[tail] = false;
                        Items[tail] = DefaultValue;
                        tail = (tail + 1) & IndexMask;
                        nextSampleTime += PeriodTicks;
                    }

                    // Window is full and has some active items --> notify observer
                    if (isHistoryValid)
                    {
                        observer.OnHop(windowEndTime, this);
                    }
                    else
                    {
                        observer.OnInit(windowEndTime, this);
                        isHistoryValid = true;
                    }

                    // Deactivate HistoryWindowSize oldest samples
                    int head = (tail - BaseWindowSize) & IndexMask;
                    for (int i = 0; i < HistoryWindowSize; i++)
                    {
                        if (IsActiveItems[head])
                        {
                            IsActiveItems[head] = false;
                            numberOfActiveItems--;
                        }
                        head = (head + 1) & IndexMask;
                    }
                    numberOfItems -= HistoryWindowSize;

                    // Adjust active window timestamps
                    windowStartTime += HopSizeTicks;
                    windowEndTime += HopSizeTicks;
                }
                else
                {
                    // Move window since it has no active events
                    windowEndTime = (time - OffsetTicks + HopSizeTicks) / HopSizeTicks * HopSizeTicks + OffsetTicks;
                    windowStartTime = windowEndTime - WindowSizeTicks;
                    nextSampleTime = windowStartTime;

                    numberOfItems = 0;
                    isHistoryValid = false;
                    return;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public sealed override void Enqueue(long time, ref T item)
        {
            // Advance time to ensure:
            //   1) (windowEndTime - hopSize) <= time < windowEndTime
            //   2) windowStartTime <= nextSampleTime < windowEndTime
            AdvanceTime(time);

            if (time < nextSampleTime) { return; }

            // Ensured: windowStartTime <= nextSampleTime <= time < windowEndTime

            // Replace missing values with nulls and ensure (time == nextSampleTime)
            while (time > nextSampleTime)
            {
                IsActiveItems[tail] = false;
                Items[tail] = DefaultValue;
                tail = (tail + 1) & IndexMask;
                numberOfItems++;
                nextSampleTime += PeriodTicks;
            }

            // Enqueue item
            IsActiveItems[tail] = true;
            Items[tail] = item;
            tail = (tail + 1) & IndexMask;
            numberOfItems++;
            numberOfActiveItems++;
            nextSampleTime += PeriodTicks;

            // Restore if violated: windowStartTime <= nextSampleTime < windowEndTime
            if (nextSampleTime == windowEndTime)
            {
                // Window is full and has some active items --> notify observer
                if (isHistoryValid)
                {
                    observer.OnHop(windowEndTime, this);
                }
                else
                {
                    observer.OnInit(windowEndTime, this);
                    isHistoryValid = true;
                }

                // Deactivate HistorySize oldest samples
                int head = (tail - BaseWindowSize) & IndexMask;
                for (int i = 0; i < HistoryWindowSize; i++)
                {
                    if (IsActiveItems[head])
                    {
                        IsActiveItems[head] = false;
                        numberOfActiveItems--;
                    }
                    head = (head + 1) & IndexMask;
                }
                numberOfItems -= HistoryWindowSize;

                // Adjust active window timestamps
                windowStartTime += HopSizeTicks;
                windowEndTime += HopSizeTicks;
            }
        }
    }

    [DataContract]
    internal sealed class PaddedTumblingSignalWindow<T> : PaddedSignalWindow<T>
    {
        public PaddedTumblingSignalWindow(ISignalWindowObserver<T> observer, long period, long offset, int sampleWindowSize, int sampleHopSize)
            : base(sampleWindowSize, 0, observer, period, offset, sampleWindowSize, sampleHopSize)
        {
            Contract.Requires(sampleWindowSize <= sampleHopSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public sealed override void AdvanceTime(long time)
        {
            /* Move window, if necessary, to ensure
             *    time < windowEndTime
             * and
             *    windowStartTime <= nextSampleTime < windowEndTime
             */
            if (time >= windowEndTime)
            {
                if (numberOfActiveItems > 0)
                {
                    // Fill out window with null values
                    for (; tail < BaseWindowSize; tail++)
                    {
                        Items[tail] = DefaultValue;
                    }

                    // Window is full and has some active items
                    observer.OnInit(windowEndTime, this);
                    tail = 0;
                    numberOfActiveItems = 0;
                }

                // Move window since it has no active events
                windowEndTime = (time - OffsetTicks + HopSizeTicks) / HopSizeTicks * HopSizeTicks + OffsetTicks;
                windowStartTime = windowEndTime - WindowSizeTicks;
                nextSampleTime = windowStartTime;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public sealed override void Enqueue(long time, ref T item)
        {
            // Advance time to ensure:
            //   1) time < windowEndTime
            //   2) windowStartTime <= nextSampleTime < windowEndTime
            AdvanceTime(time);

            if (time < nextSampleTime) { return; }

            // Ensured: windowStartTime <= nextSampleTime <= time < windowEndTime

            // Replace missing values with nulls and ensure (time == nextSampleTime)
            while (time > nextSampleTime)
            {
                Items[tail] = DefaultValue;
                tail++;
                nextSampleTime += PeriodTicks;
            }

            // Enqueue item
            Items[tail] = item;
            tail++;
            numberOfActiveItems++;
            nextSampleTime += PeriodTicks;

            // Restore if violated: windowStartTime <= nextSampleTime < windowEndTime
            if (nextSampleTime == windowEndTime)
            {
                // Window is full and has some active items
                observer.OnInit(windowEndTime, this);
                tail = 0;
                numberOfActiveItems = 0;

                // Adjust active window timestamps
                windowStartTime += HopSizeTicks;
                windowEndTime += HopSizeTicks;
                nextSampleTime = windowStartTime;
            }
        }
    }
}