// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing.Signal
{
    /// <summary>
    /// Builder static class for creating interpolation policies
    /// </summary>
    public static class InterpolationPolicy
    {
        /// <summary>
        /// Replace missing samples with a fixed value.
        /// </summary>
        /// <returns>An instance of the interpolation policy</returns>
        public static InterpolationPolicy<T> FixedValue<T>(long windowSize, T defaultValue)
            => new TwoPointInterpolationPolicy<T>(windowSize, (v1, t1, v2, t2, t) => defaultValue);

        /// <summary>
        /// Replace missing samples with the last known value.
        /// </summary>
        /// <returns>An instance of the interpolation policy</returns>
        public static InterpolationPolicy<T> LastValue<T>(long windowSize)
            => new TwoPointInterpolationPolicy<T>(windowSize, (v1, t1, v2, t2, t) => (t == t2 ? v2 : v1));

        /// <summary>
        /// Interpolate missing samples using a step function between two last known values.
        /// </summary>
        /// <returns>An instance of the interpolation policy</returns>
        public static InterpolationPolicy<T> ZeroOrder<T>(long windowSize)
            => new TwoPointInterpolationPolicy<T>(windowSize, (v1, t1, v2, t2, t) => t < ((t1 + t2) >> 1) ? v1 : v2);

        /// <summary>
        /// Interpolate missing samples using a linear function between two last known values.
        /// </summary>
        /// <returns>An instance of the interpolation policy</returns>
        public static InterpolationPolicy<T> FirstOrder<T>(long windowSize)
        {
            Invariant.IsTrue(typeof(T).SupportsOperator("+"), "Type " + typeof(T) + " does not support the addition operator with second argument " + typeof(T) + ".");
            Invariant.IsTrue(typeof(T).SupportsOperator("-"), "Type " + typeof(T) + " does not support the subtraction operator with second argument " + typeof(T) + ".");
            Invariant.IsTrue(typeof(T).SupportsOperator("*"), "Type " + typeof(T) + " does not support the multiplication operator with second argument " + typeof(double) + ".");

            Expression<Func<double, long, double, long, long, double>> exp = (v1, t1, v2, t2, t) =>
                    t == t1 ? v1 : (t == t2 ? v2 : (v2 - v1) * ((double)(t - t1) / (t2 - t1)) + v1);
            var parameters = exp.Parameters;
            var var1 = Expression.Parameter(typeof(T));
            var var2 = Expression.Parameter(typeof(T));
            var body = ParameterSubstituter.Replace(parameters[0], var1, parameters[2], var2, exp);
            return new TwoPointInterpolationPolicy<T>(
                windowSize,
                Expression.Lambda<Func<T, long, T, long, long, T>>(
                    body, var1, parameters[1], var2, parameters[3], parameters[4]));
        }

        /// <summary>
        /// Interpolate missing samples using a second-order polynomial over three last known values.
        /// </summary>
        /// <returns>An instance of the interpolation policy</returns>
        public static InterpolationPolicy<T> SecondOrder<T>(long windowSize)
        {
            Invariant.IsTrue(typeof(T).SupportsOperator("+"), "Type " + typeof(T) + " does not support the addition operator with second argument " + typeof(T) + ".");
            Invariant.IsTrue(typeof(T).SupportsOperator("-"), "Type " + typeof(T) + " does not support the subtraction operator with second argument " + typeof(T) + ".");
            Invariant.IsTrue(typeof(T).SupportsOperator("*"), "Type " + typeof(T) + " does not support the multiplication operator with second argument " + typeof(double) + ".");

            Expression<Func<double, long, double, long, long, double>> exp1 = (v1, t1, v2, t2, t) =>
                    (t == t1 ? v1 : (t == t2 ? v2 : (v2 - v1) * ((double)(t - t1) / (t2 - t1)) + v1));
            var parameters1 = exp1.Parameters;
            var var1 = Expression.Parameter(typeof(T));
            var var2 = Expression.Parameter(typeof(T));
            var body1 = ParameterSubstituter.Replace(parameters1[0], var1, parameters1[2], var2, exp1);

            Expression<Func<double, long, double, long, double, long, long, double>> exp2 = (v1, t1, v2, t2, v3, t3, t) =>
                    t == t1 ? v1 : (t == t2 ? v2 : t == t3 ? v3 :
                        v1 * ((double)(t - t2) * (t - t3) / (t1 - t2) / (t1 - t3)) +
                        v2 * ((double)(t - t1) * (t - t3) / (t2 - t1) / (t2 - t3)) +
                        v3 * ((double)(t - t1) * (t - t2) / (t3 - t1) / (t3 - t2)));
            var parameters2 = exp2.Parameters;
            var var3 = Expression.Parameter(typeof(T));
            var body2 = ParameterSubstituter.Replace(parameters2[0], var1, parameters2[2], var2, parameters2[4], var3, exp2);

            return new ThreePointInterpolationPolicy<T>(
                windowSize,
                Expression.Lambda<Func<T, long, T, long, long, T>>(
                    body1, var1, parameters1[1], var2, parameters1[3], parameters1[4]),
                Expression.Lambda<Func<T, long, T, long, T, long, long, T>>(
                    body2, var1, parameters2[1], var2, parameters2[3], var3, parameters2[5], parameters2[6]));
        }

        /// <summary>
        /// Interpolate missing samples using the provided function over two last known values.
        /// </summary>
        /// <returns>An instance of the interpolation policy</returns>
        public static InterpolationPolicy<T> CustomTwoPoint<T>(long windowSize, Expression<Func<T, long, T, long, long, T>> func2)
            => new TwoPointInterpolationPolicy<T>(windowSize, func2);

        /// <summary>
        /// Interpolate missing samples using the provided functions over two or three last known values.
        /// </summary>
        /// <returns>An instance of the interpolation policy</returns>
        public static InterpolationPolicy<T> CustomThreePoint<T>(long windowSize, Expression<Func<T, long, T, long, long, T>> func2,
            Expression<Func<T, long, T, long, T, long, long, T>> func3)
            => new ThreePointInterpolationPolicy<T>(windowSize, func2, func3);

        [DataContract]
        private class TwoPointInterpolationPolicy<T> : InterpolationPolicy<T>
        {
            [DataMember]
            private Expression<Func<T, long, T, long, long, T>> ExpFunc2 { get; }
            [DataMember]
            private readonly Func<T, long, T, long, long, T> func2;

            public TwoPointInterpolationPolicy(
                long windowSize,
                Expression<Func<T, long, T, long, long, T>> expFunc2)
                : base(windowSize)
            {
                this.ExpFunc2 = expFunc2;
                this.func2 = this.ExpFunc2.Compile();
            }

            internal override Expression<Func<Interpolator<T>>> NewInterpolator() => () => new TwoPointInterpolator(windowSize, func2);

            public override bool Equals(object obj) => !(obj is TwoPointInterpolationPolicy<T> that)
                    ? false
                    : that != null && base.Equals(that) && this.ExpFunc2.Equals(that.ExpFunc2);

            public bool Equals(TwoPointInterpolationPolicy<T> that) => that != null && base.Equals(that) && this.ExpFunc2.Equals(that.ExpFunc2);

            public override int GetHashCode() => base.GetHashCode() ^ this.ExpFunc2.GetHashCode();

            [DataContract]
            private sealed class TwoPointInterpolator : Interpolator<T>
            {
                [DataMember]
                private readonly Func<T, long, T, long, long, T> func2;

                public TwoPointInterpolator(long windowSize, Func<T, long, T, long, long, T> func2)
                    : base(windowSize) => this.func2 = func2;

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public override void AdvanceTime(long time)
                {
                    if (time <= this.rightTime) return;

                    // Check if leftValue or rightValue or both should expire
                    long watermark = time - this.windowSize;
                    if (watermark > this.rightTime)
                    {
                        this.leftTime = this.rightTime = long.MinValue;
                        this.leftValue = this.rightValue = default;
                        this.numActiveSamples = 0;
                    }
                    else if (watermark > this.leftTime)
                    {
                        this.leftTime = this.rightTime;
                        this.leftValue = this.rightValue;
                        this.numActiveSamples = 1;
                    }
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public override void AddPoint(long time, ref T value)
                {
                    if (time <= this.rightTime) { return; }

                    long watermark = time - this.windowSize;

                    // Check if interpolator is uninitialized or new sample is too far away from right point
                    if (this.numActiveSamples == 0 || watermark > this.rightTime)
                    {
                        this.leftTime = this.rightTime = time;
                        this.leftValue = this.rightValue = value;
                        this.numActiveSamples = 1;
                    }
                    else
                    {
                        // numActiveSamples == 1 or 2
                        this.leftTime = this.rightTime;
                        this.leftValue = this.rightValue;
                        this.rightTime = time;
                        this.rightValue = value;
                        this.numActiveSamples = 2;
                    }
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public override bool CanInterpolate(long time) => this.leftTime <= time && time <= this.rightTime && this.numActiveSamples == 2 && this.func2 != null;

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public override void Interpolate(long time, out T value) => value = this.func2(this.leftValue, this.leftTime, this.rightValue, this.rightTime, time);
            }
        }

        [DataContract]
        private class ThreePointInterpolationPolicy<T> : InterpolationPolicy<T>
        {
            [DataMember]
            private Expression<Func<T, long, T, long, long, T>> ExpFunc2 { get; }
            [DataMember]
            private readonly Func<T, long, T, long, long, T> func2;
            [DataMember]
            private Expression<Func<T, long, T, long, T, long, long, T>> ExpFunc3 { get; }
            [DataMember]
            private readonly Func<T, long, T, long, T, long, long, T> func3;

            public ThreePointInterpolationPolicy(
                long windowSize,
                Expression<Func<T, long, T, long, long, T>> expFunc2,
                Expression<Func<T, long, T, long, T, long, long, T>> expFunc3)
                : base(windowSize)
            {
                this.ExpFunc2 = expFunc2;
                this.func2 = expFunc2.Compile();
                this.ExpFunc3 = expFunc3;
                this.func3 = expFunc3.Compile();
            }

            internal override Expression<Func<Interpolator<T>>> NewInterpolator()
                => () => new ThreePointInterpolator(windowSize, func2, func3);

            public override bool Equals(object obj) => !(obj is ThreePointInterpolationPolicy<T> that)
                    ? false
                    : that != null && base.Equals(that) && this.ExpFunc2.Equals(that.ExpFunc2) && this.ExpFunc3.Equals(that.ExpFunc3);

            public bool Equals(ThreePointInterpolationPolicy<T> that) => that != null && base.Equals(that) && this.ExpFunc2.Equals(that.ExpFunc2) && this.ExpFunc3.Equals(that.ExpFunc3);

            public override int GetHashCode() => base.GetHashCode() ^ this.ExpFunc2.GetHashCode() ^ this.ExpFunc3.GetHashCode();

            [DataContract]
            private sealed class ThreePointInterpolator : Interpolator<T>
            {
                [DataMember]
                private readonly Func<T, long, T, long, long, T> func2;
                [DataMember]
                private readonly Func<T, long, T, long, T, long, long, T> func3;

                [DataMember]
                private long middleTime;
                [DataMember]
                private T middleValue;

                public ThreePointInterpolator(
                    long windowSize,
                    Func<T, long, T, long, long, T> func2,
                    Func<T, long, T, long, T, long, long, T> func3)
                    : base(windowSize)
                {
                    this.func2 = func2;
                    this.func3 = func3;
                    this.middleTime = long.MinValue;
                    this.middleValue = default;
                }


                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public override void AdvanceTime(long time)
                {
                    if (time <= this.rightTime) { return; }

                    // Check if leftValue, middleValue and rightValue should expire
                    long watermark = time - this.windowSize;
                    if (watermark > this.rightTime)
                    {
                        this.leftTime = this.middleTime = this.rightTime = long.MinValue;
                        this.leftValue = this.middleValue = this.rightValue = default;
                        this.numActiveSamples = 0;
                    }
                    else if (watermark > this.middleTime)
                    {
                        this.leftTime = this.middleTime = this.rightTime;
                        this.leftValue = this.middleValue = this.rightValue;
                        this.numActiveSamples = 1;
                    }
                    else if (watermark > leftTime)
                    {
                        this.leftTime = this.middleTime;
                        this.leftValue = this.middleValue;
                        this.numActiveSamples = 2;
                    }
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public override void AddPoint(long time, ref T value)
                {
                    if (time <= this.rightTime) { return; }

                    long watermark = time - this.windowSize;

                    // Check if interpolator is uninitialized or new sample is too far away from right point
                    if (this.numActiveSamples == 0 || watermark > this.rightTime)
                    {
                        this.leftTime = this.middleTime = this.rightTime = time;
                        this.leftValue = this.middleValue = this.rightValue = value;
                        this.numActiveSamples = 1;
                    }
                    // Check if interpolator has one sample or new sample is too far away from middle point
                    else if (numActiveSamples == 1 || watermark > this.middleTime)
                    {
                        this.leftTime = this.middleTime = this.rightTime;
                        this.leftValue = this.middleValue = this.rightValue;
                        this.rightTime = time;
                        this.rightValue = value;
                        this.numActiveSamples = 2;
                    }
                    else
                    {
                        // numActiveSamples == 2 or 3
                        this.leftTime = this.middleTime;
                        this.leftValue = this.middleValue;
                        this.middleTime = this.rightTime;
                        this.middleValue = this.rightValue;
                        this.rightTime = time;
                        this.rightValue = value;
                        this.numActiveSamples = 3;
                    }
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public override bool CanInterpolate(long time)
                    => (this.numActiveSamples == 3 && this.func3 != null ||
                        this.numActiveSamples == 2 && this.func2 != null) &&
                        this.leftTime <= time && time <= this.rightTime;

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public override void Interpolate(long time, out T value)
                    => value = this.numActiveSamples == 3
                        ? func3(this.leftValue, this.leftTime, this.middleValue, this.middleTime, this.rightValue, this.rightTime, time)
                        : func2(this.middleValue, this.middleTime, this.rightValue, this.rightTime, time);
            }
        }
    }

    /// <summary>
    /// Specifies how to interpolate missing samples
    /// </summary>
    [DataContract]
    public abstract class InterpolationPolicy<T>
    {
        /// <summary>
        /// The window size associated with the interpolation policy
        /// </summary>
        [DataMember]
        public readonly long windowSize;

        /// <summary>
        /// Empty constructor required for Data Contract serializability - do not use directly
        /// </summary>
        [Obsolete("Empty constructor required for Data Contract serializability.")]
        protected InterpolationPolicy() { }

        internal InterpolationPolicy(long windowSize)
        {
            Contract.Requires(windowSize > 0);

            this.windowSize = windowSize;
        }

        /// <summary>
        /// Generator for a factory method for creating new interpolators
        /// </summary>
        /// <returns>Factory method for creating new interpolators</returns>
        internal abstract Expression<Func<Interpolator<T>>> NewInterpolator();

        /// <summary>
        /// Override for equals
        /// </summary>
        /// <param name="obj">The other object against which to compare</param>
        /// <returns>True if the two objects are equal</returns>
        public override bool Equals(object obj) => !(obj is InterpolationPolicy<T> that) ? false : windowSize == that.windowSize;

        /// <summary>
        /// Determines whether two interpolation policies are equal
        /// </summary>
        /// <param name="that">The other interpolation policy against which to compare</param>
        /// <returns>True if the two policies are equal</returns>
        public bool Equals(InterpolationPolicy<T> that) => that != null && windowSize == that.windowSize;

        /// <summary>
        /// Override for hash code computation
        /// </summary>
        /// <returns>Hash code</returns>
        public override int GetHashCode() => base.GetHashCode() ^ windowSize.GetHashCode();
    }
}