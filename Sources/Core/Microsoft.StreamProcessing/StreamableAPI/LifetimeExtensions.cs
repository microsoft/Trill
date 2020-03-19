// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// This static class contains helper macros to make Trill query authoring easier.
    /// </summary>
    public static partial class Streamable
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long AdjustStartTime(long startTime, long offset, long period)
        {
            if ((startTime > StreamEvent.MaxSyncTime - period) && // Cheap check which fails the vast majority of the time
                (startTime > (StreamEvent.MaxSyncTime / period) * period)) // More expensive precise check
            {
                throw new InvalidOperationException("Window start out of range");
            }
            return (startTime + period - 1).SnapToLeftBoundary(period, offset);
        }

        /// <summary>
        /// Adjusts the lifetime of incoming events to implement, when used in combination with aggregates, tumbling windows. In this implementation each incoming
        /// event results in a single outgoing event, which means that subsequent aggregates only produce output when the input changes. For instance, if a single
        /// point event is received and the tumbleDuration and offset are 100 and 0 respectively, a single aggregate output is produced with a lifetime of
        /// 100 ticks.
        /// </summary>
        /// <typeparam name="TKey">Type of (mapping) key in the stream</typeparam>
        /// <typeparam name="TPayload">Type of payload in the stream</typeparam>
        /// <param name="source">Input stream</param>
        /// <param name="tumbleDuration">Duration of the tumble</param>
        /// <param name="offset">Offset from the start of time</param>
        /// <returns>Result (output) stream</returns>
        public static IStreamable<TKey, TPayload> TumblingWindowLifetime<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source,
            long tumbleDuration,
            long offset = 0)
        {
            Invariant.IsNotNull(source, nameof(source));
            return source.HoppingWindowLifetime(tumbleDuration, tumbleDuration, offset);
        }

        /// <summary>
        /// Adjusts the lifetime of incoming events to implement, when used in combination with aggregates, hopping windows. In this implementation each incoming
        /// event results in a single outgoing event, which means that subsequent aggregates only produce output when the input changes. For instance, if a single
        /// point event is received and the windowSize, period, and offset are 100, 2, and 0 respectively, a single aggregate output is produced with a lifetime of
        /// 100 ticks.
        /// </summary>
        /// <typeparam name="TKey">Type of (mapping) key in the stream</typeparam>
        /// <typeparam name="TPayload">Type of payload in the stream</typeparam>
        /// <param name="source">Input stream</param>
        /// <param name="windowSize">Window size</param>
        /// <param name="period">Period (or hop size)</param>
        /// <param name="offset">Offset from the start of time</param>
        /// <returns>Result (output) stream</returns>
        public static IStreamable<TKey, TPayload> HoppingWindowLifetime<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source,
            long windowSize,
            long period,
            long offset = 0)
        {
            if (period > windowSize)
            {
                return source
                    .Select((vs, e) => new { StartTime = vs, Payload = e })
                    .Where(x => (((x.StartTime + period - offset) % period) > (period - windowSize)) || ((x.StartTime + period - offset) % period == 0))
                    .Select(x => x.Payload)
                    .AlterEventLifetime(vs => AdjustStartTime(vs, offset, period), windowSize)
                    .SetProperty().IsConstantHop(true, period, offset);
            }

            if (windowSize % period == 0)
            {
                return source.AlterEventLifetime(vs => AdjustStartTime(vs, offset, period), windowSize)
                    .SetProperty().IsConstantHop(true, period, offset);
            }
            else
            {
                return source.AlterEventLifetime(
                        vs => AdjustStartTime(vs, offset, period),
                        vs => AdjustStartTime(vs + windowSize, offset, period) - AdjustStartTime(vs, offset, period))
                    .SetProperty().IsConstantHop(true, period, offset);
            }
        }

        /// <summary>
        /// Adjusts the lifetime of incoming events to snap the start and end time of each event to quantized boundaries.
        /// The function is similar to a hopping lifetime expression, except that all start edges are either moved
        /// earlier or stay the same, and all end edges either move later or stay the same.
        /// </summary>
        /// <typeparam name="TKey">Type of (mapping) key in the stream</typeparam>
        /// <typeparam name="TPayload">Type of payload in the stream</typeparam>
        /// <param name="source">Input stream</param>
        /// <param name="windowSize">Window size</param>
        /// <param name="period">Period (or hop size)</param>
        /// <param name="offset">Offset from the start of time</param>
        /// <returns>Result (output) stream</returns>
        public static IStreamable<TKey, TPayload> QuantizeLifetime<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source,
            long windowSize,
            long period,
            long offset = 0)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsPositive(windowSize, nameof(windowSize));
            Invariant.IsPositive(period, nameof(period));

            return new QuantizeLifetimeStreamable<TKey, TPayload>(source, windowSize, period, period, offset);
        }

        /// <summary>
        /// Adjusts the lifetime of incoming events to snap the start and end time of each event to quantized boundaries,
        /// except that the start times are progressively spaced through the window.
        /// The function is similar to a hopping lifetime expression, except that all start edges are either moved
        /// earlier or stay the same, and all end edges either move later or stay the same.
        /// </summary>
        /// <typeparam name="TKey">Type of (mapping) key in the stream</typeparam>
        /// <typeparam name="TPayload">Type of payload in the stream</typeparam>
        /// <param name="source">Input stream</param>
        /// <param name="windowSize">Window size</param>
        /// <param name="period">Period (or hop size)</param>
        /// <param name="progress">Interval at which progressive results within a window are requested</param>
        /// <param name="offset">Offset from the start of time</param>
        /// <returns>Result (output) stream</returns>
        public static IStreamable<TKey, TPayload> ProgressiveQuantizeLifetime<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source,
            long windowSize,
            long period,
            long progress,
            long offset = 0)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsPositive(windowSize, nameof(windowSize));
            Invariant.IsPositive(period, nameof(period));
            Invariant.IsPositive(progress, nameof(progress));
            if (period % progress != 0) throw new ArgumentException("Progress interval must be a proper divisor of the period.");
            if (period <= progress) throw new ArgumentException("Progress interval must be strictly smaller than the period.");

            return new QuantizeLifetimeStreamable<TKey, TPayload>(source, windowSize, period, progress, offset);
        }

        /// <summary>
        /// Adjusts the lifetime of incoming events to implement, when used in combination with aggregates, repetitive hopping windows. In this implementation each
        /// incoming event results in an arbitrary number of outgoing events. For instance, if a single point event is received and the windowSize, period, and
        /// offset are 100, 2, and 0 respectively, 50 identical aggregate outputs are produced, each with a lifetime of 2 ticks.
        /// </summary>
        /// <typeparam name="TKey">Type of (mapping) key in the stream</typeparam>
        /// <typeparam name="TPayload">Type of payload in the stream</typeparam>
        /// <param name="source">Input stream</param>
        /// <param name="windowSize">Window size</param>
        /// <param name="period">Period (or hop size)</param>
        /// <param name="offset">Offset from the start of time</param>
        /// <returns>The result (output) stream</returns>
        public static IStreamable<TKey, TPayload> RepetitiveHoppingWindowLifetime<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source, long windowSize, long period, long offset = 0)
        {
            Invariant.IsNotNull(source, nameof(source));

            return source.HoppingWindowLifetime(windowSize, period, offset).Chop(offset, period);
        }

        /// <summary>
        /// The window type implements sessions with timeout in Trill. The current window is extended as long as new events
        /// arrive within a specified timeout period. Once a timeout period elapses with no data (for a given grouping key),
        /// the window is closed.
        /// </summary>
        /// <typeparam name="TKey">Type of (mapping) key in the stream</typeparam>
        /// <typeparam name="TPayload">Type of payload in the stream</typeparam>
        /// <param name="source">Input stream</param>
        /// <param name="timeout">Timeout for closing the current session</param>
        /// <param name="maxDuration">The max duration of a session timeout window</param>
        /// <returns>Result (output) stream</returns>
        public static IStreamable<TKey, TPayload> SessionTimeoutWindow<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source,
            long timeout,
            long maxDuration = 0L)
        {
            Invariant.IsNotNull(source, nameof(source));

            if (maxDuration <= 0L) maxDuration = StreamEvent.InfinitySyncTime;
            return new SessionWindowStreamable<TKey, TPayload>(source, timeout, maxDuration);
        }

        /// <summary>
        /// Changes the Ve of each event according to the durationSelector, which is a function of start time
        /// </summary>
        /// <param name="source">Source stream</param>
        /// <param name="durationSelector">Function which recomputes the event duration</param>
        public static IStreamable<TKey, TPayload> AlterEventDuration<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source,
            Expression<Func<long, long>> durationSelector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(durationSelector, nameof(durationSelector));

            return new AlterLifetimeStreamable<TKey, TPayload>(source, null, durationSelector);
        }

        /// <summary>
        /// Changes the Ve of each event according to the durationSelector, which is a function of start and end times
        /// </summary>
        /// <param name="source">Source stream</param>
        /// <param name="durationSelector">Function which recomputes the event duration</param>
        public static IStreamable<TKey, TPayload> AlterEventDuration<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source,
            Expression<Func<long, long, long>> durationSelector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(durationSelector, nameof(durationSelector));

            return new AlterLifetimeStreamable<TKey, TPayload>(source, null, durationSelector);
        }

        /// <summary>
        /// Changes the Ve of each event according to the duration provided
        /// </summary>
        /// <param name="source">Source stream</param>
        /// <param name="duration"></param>
        public static IStreamable<TKey, TPayload> AlterEventDuration<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source,
            long duration)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsPositive(duration, nameof(duration));

            return source is IFusibleStreamable<TKey, TPayload> s
                ? s.FuseSetDurationConstant(duration)
                : (IStreamable<TKey, TPayload>)new AlterLifetimeStreamable<TKey, TPayload>(source, null, Expression.Lambda<Func<long>>(Expression.Constant(duration)));
        }

        /// <summary>
        /// Shifts the lifetime by the shiftSelector, which is a function of the event start time
        /// </summary>
        /// <param name="source">Source stream</param>
        /// <param name="shiftSelector">Function which computes the amount to shift the Vs by</param>
        public static IStreamable<TKey, TPayload> ShiftEventLifetime<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source,
            Expression<Func<long, long>> shiftSelector)
        {
            Invariant.IsNotNull(source, nameof(source));

            if (shiftSelector == null) return source;

            var startTimeSelector
                = Expression.Lambda<Func<long, long>>(Expression.AddChecked(shiftSelector.Body, shiftSelector.Parameters[0]), shiftSelector.Parameters);

            if (source.Properties.IsConstantDuration)
                return new AlterLifetimeStreamable<TKey, TPayload>(source, startTimeSelector, Expression.Lambda<Func<long>>(Expression.Constant(source.Properties.ConstantDurationLength.Value)));

            var vs = Expression.Parameter(typeof(long), "vs");
            var ve = Expression.Parameter(typeof(long), "ve");
            return new AlterLifetimeStreamable<TKey, TPayload>(source, startTimeSelector,
                Expression.Lambda(Expression.Subtract(ve, vs), vs, ve));
        }

        /// <summary>
        /// Shifts the lifetime by the specified amount of time
        /// </summary>
        /// <param name="source">Source stream</param>
        /// <param name="shiftAmount">The amount to shift the Vs by</param>
        public static IStreamable<TKey, TPayload> ShiftEventLifetime<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source,
            long shiftAmount)
        {
            Invariant.IsNotNull(source, nameof(source));
            return source.ShiftEventLifetime(s => shiftAmount);
        }

        /// <summary>
        /// Changes the Vs and Ve of each event according to the startTimeSelector, and the durationSelector, which is a function of start time
        /// </summary>
        /// <param name="source">Source stream</param>
        /// <param name="startTimeSelector">Function which recomputes the event start time</param>
        /// <param name="durationSelector">Function which recomputes the event duration</param>
        public static IStreamable<TKey, TPayload> AlterEventLifetime<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source,
            Expression<Func<long, long>> startTimeSelector,
            Expression<Func<long, long>> durationSelector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(startTimeSelector, nameof(startTimeSelector));
            Invariant.IsNotNull(durationSelector, nameof(durationSelector));

            return new AlterLifetimeStreamable<TKey, TPayload>(source, startTimeSelector, durationSelector);
        }

        /// <summary>
        /// Changes the Vs and Ve of each event according to the startTimeSelector, and the durationSelector, which is a function of start and end times
        /// </summary>
        /// <param name="source">Source stream</param>
        /// <param name="startTimeSelector">Function which recomputes the event start time</param>
        /// <param name="durationSelector">Function which recomputes the event duration</param>
        public static IStreamable<TKey, TPayload> AlterEventLifetime<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source,
            Expression<Func<long, long>> startTimeSelector,
            Expression<Func<long, long, long>> durationSelector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(startTimeSelector, nameof(startTimeSelector));
            Invariant.IsNotNull(durationSelector, nameof(durationSelector));

            return new AlterLifetimeStreamable<TKey, TPayload>(source, startTimeSelector, durationSelector);
        }

        /// <summary>
        /// Changes the Vs and Ve of each event according to the startTimeSelector, and the durationSelector, which is a function of start and end times as well as the partition key
        /// Note: This overload is primarily for power users and should be used exceedingly sparingly
        /// </summary>
        /// <param name="source">Source stream</param>
        /// <param name="startTimeSelector">Function which recomputes the event start time</param>
        /// <param name="durationSelector">Function which recomputes the event duration</param>
        public static IStreamable<PartitionKey<TPartition>, TPayload> AlterEventLifetime<TPartition, TPayload>(
            this IStreamable<PartitionKey<TPartition>, TPayload> source,
            Expression<Func<TPartition, long, long>> startTimeSelector,
            Expression<Func<TPartition, long, long, long>> durationSelector)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(startTimeSelector, nameof(startTimeSelector));
            Invariant.IsNotNull(durationSelector, nameof(durationSelector));

            return new AlterLifetimeStreamable<PartitionKey<TPartition>, TPayload>(source, startTimeSelector, durationSelector);
        }

        /// <summary>
        /// Changes the Vs and Ve of each event.
        /// </summary>
        /// <typeparam name="TKey">Key type of the stream</typeparam>
        /// <typeparam name="TPayload">Data type of the stream</typeparam>
        /// <param name="source">source stream</param>
        /// <param name="startTimeSelector">Function which recomputes the event start time</param>
        /// <param name="duration">Amount to alter the durations by</param>
        /// <returns>The altered stream</returns>
        public static IStreamable<TKey, TPayload> AlterEventLifetime<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source,
            Expression<Func<long, long>> startTimeSelector,
            long duration)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsNotNull(startTimeSelector, nameof(startTimeSelector));
            Invariant.IsPositive(duration, nameof(duration));

            return new AlterLifetimeStreamable<TKey, TPayload>(source, startTimeSelector, Expression.Lambda<Func<long>>(Expression.Constant(duration)));
        }

        /// <summary>
        /// Passes a truncated version of each event, where the event is truncated by a maximum event length.
        /// </summary>
        public static IStreamable<TKey, TPayload> ClipEventDuration<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source,
            long limit)
        {
            Invariant.IsNotNull(source, nameof(source));
            Invariant.IsPositive(limit, nameof(limit));

            return source.Properties.IsConstantDuration && limit < source.Properties.ConstantDurationLength.Value
                ? AlterEventDuration(source, limit)
                : new ClipByConstantStreamable<TKey, TPayload>(source, limit);
        }
    }
}